"""Frozen, network-free synthetic Section 16 parser oracles."""

from __future__ import annotations

import copy
import hashlib
import json
import unittest
from xml.etree import ElementTree
from pathlib import Path
from typing import cast
from unittest.mock import patch

from insider_contract import (
    InsiderContractError,
    absolute_decimal_product,
    canonical_insider_json_bytes,
    validate_insider_filing,
)
from insider_schema import OWNERSHIP_NAMESPACE, derive_unknown_element_records
from insider_parser import (
    MAX_UNKNOWN_RECORDS,
    InsiderParseError,
    UnsafeOwnershipXML,
    parse_ownership_xml,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "insider_filings"
ORACLE = json.loads((FIXTURE_ROOT / "expectations.json").read_text())


class InsiderParserTests(unittest.TestCase):
    def parse_case(self, case_name: str) -> dict[str, object]:
        case = ORACLE["filings"][case_name]
        return parse_ownership_xml(
            (FIXTURE_ROOT / case["filename"]).read_bytes(),
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

    def test_fixture_hashes_are_frozen_and_explicitly_synthetic(self) -> None:
        self.assertEqual(
            "synthetic_test_only_section16_oracle",
            ORACLE["fixture_kind"],
        )
        self.assertIsNone(ORACLE["network_source"])
        for case_name, case in ORACLE["filings"].items():
            with self.subTest(case=case_name):
                self.assertEqual(
                    case["sha256"],
                    hashlib.sha256(
                        (FIXTURE_ROOT / case["filename"]).read_bytes()
                    ).hexdigest(),
                )

    def test_unsafe_fixture_hashes_are_also_frozen(self) -> None:
        for case_name, case in ORACLE["unsafe_filings"].items():
            with self.subTest(case=case_name):
                self.assertEqual(
                    case["sha256"],
                    hashlib.sha256(
                        (FIXTURE_ROOT / case["filename"]).read_bytes()
                    ).hexdigest(),
                )

    def test_simple_form4_purchase_is_exact_and_traceable(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        expected = case["expected"]
        self.assertEqual(1, filing["insider_contract_version"])
        self.assertEqual("1.0.0", filing["parser_version"])
        self.assertEqual("X0306", filing["schema_version"])
        self.assertEqual(hashlib.sha256(raw_xml).hexdigest(), filing["raw_sha256"])
        self.assertEqual(case["source_index_url"], filing["source"]["index_url"])
        self.assertEqual(
            case["source_document_url"],
            filing["source"]["document_url"],
        )
        self.assertEqual(expected["form_type"], filing["form_type"])
        self.assertEqual(expected["base_form_type"], filing["base_form_type"])
        self.assertEqual(expected["issuer_cik"], filing["issuer"]["cik"])
        self.assertEqual(1, len(filing["owners"]))
        self.assertEqual(expected["owner_cik"], filing["owners"][0]["cik"])
        self.assertEqual("TEST DATA ONLY", filing["owners"][0]["restricted_address"]["street1"])
        self.assertEqual(1, len(filing["transactions"]))
        row = filing["transactions"][0]
        self.assertEqual(case["accession_number"], row["accession_number"])
        self.assertEqual(
            "b60bff110a398604cc437b81324aa84ac4fbdb93e77eb0a2d3d78f5013171d3c",
            row["row_key"],
        )
        self.assertEqual("non_derivative", row["source_table"])
        self.assertEqual(0, row["source_row_index"])
        for field in (
            "transaction_code",
            "shares",
            "price_per_share",
            "calculated_value",
            "post_transaction_shares",
            "post_transaction_value",
        ):
            self.assertEqual(expected[field], row[field])
        self.assertEqual("purchase", row["normalized_category"])
        self.assertTrue(row["is_meaningful_ps"])
        self.assertEqual("unknown", row["plan_status"])
        self.assertEqual(
            "/ownershipDocument/nonDerivativeTable/"
            "nonDerivativeTransaction[1]/transactionAmounts/"
            "transactionShares/value",
            row["field_sources"]["shares"]["source_path"],
        )
        self.assertEqual(1, len(filing["signatures"]))
        signature = filing["signatures"][0]
        self.assertEqual(0, signature["signature_order"])
        self.assertEqual("/s/ SYNTHETIC SIGNER", signature["name"])
        self.assertEqual("2026-01-16", signature["date"])
        self.assertEqual(
            "/ownershipDocument/ownerSignature[1]",
            signature["source_path"],
        )

    def test_schema_faithful_owner_address_and_country_are_private(self) -> None:
        filing = self.parse_case("form4_simple_purchase")

        owner = filing["owners"][0]
        self.assertEqual("TEST DATA ONLY", owner["restricted_address"]["street1"])
        self.assertEqual("SYNTHETIC CITY", owner["restricted_address"]["city"])
        self.assertEqual("ZZ", owner["restricted_address"]["state"])
        self.assertEqual("00000", owner["restricted_address"]["zip_code"])
        self.assertEqual("TEST REGION", owner["restricted_address"]["state_description"])
        self.assertIsNone(owner["country"])
        self.assertTrue(filing["privacy"]["contains_restricted_owner_addresses"])

    def test_joint_form4_preserves_rows_missing_price_and_all_footnotes(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_joint_sale_derivative"]
        expected = case["expected"]
        filing = self.parse_case("form4_joint_sale_derivative")

        self.assertEqual(expected["owner_ciks"], [owner["cik"] for owner in filing["owners"]])
        self.assertEqual([0, 1], [owner["owner_order"] for owner in filing["owners"]])
        self.assertTrue(filing["owners"][0]["is_director"])
        self.assertTrue(filing["owners"][0]["is_officer"])
        self.assertFalse(filing["owners"][1]["is_director"])
        self.assertTrue(filing["owners"][1]["is_ten_percent_owner"])
        self.assertEqual("GB", filing["owners"][0]["country"])
        self.assertEqual(
            expected["foreign_trading_symbol"],
            filing["issuer"]["foreign_trading_symbol_as_filed"],
        )
        self.assertTrue(filing["aff10b5_one"])

        rows = [
            row for row in filing["transactions"]
            if row["source_table"] == "non_derivative"
        ]
        self.assertEqual(2, len(rows))
        self.assertEqual(2, len({row["row_key"] for row in rows}))
        self.assertTrue(all(row["owner_group_key"] == filing["owner_group_key"] for row in rows))
        self.assertTrue(all(row["plan_status"] == "filing_marked" for row in rows))
        self.assertTrue(all(row["transaction_code"] == "S" for row in rows))
        self.assertTrue(all(row["normalized_category"] == "sale" for row in rows))
        self.assertTrue(all(row["is_meaningful_ps"] is True for row in rows))
        self.assertEqual(expected["priced_sale_value"], rows[0]["calculated_value"])
        self.assertEqual(expected["weighted_average_refs"], rows[0]["field_footnotes"]["price_per_share"])
        self.assertEqual("I", rows[0]["direct_indirect_ownership"])
        self.assertEqual(expected["missing_price_shares"], rows[1]["shares"])
        self.assertIsNone(rows[1]["price_per_share"])
        self.assertIsNone(rows[1]["calculated_value"])
        self.assertEqual("D", rows[1]["direct_indirect_ownership"])

        self.assertEqual(expected["footnote_ids"], [note["id"] for note in filing["footnotes"]])
        linked = [
            (
                link["source_table"],
                link["source_row_index"],
                link["field_name"],
                link["footnote_id"],
                link["reference_order"],
            )
            for link in filing["field_footnote_links"]
        ]
        self.assertCountEqual(
            [
                ("non_derivative", 0, "security_title_as_filed", "F3", 0),
                ("non_derivative", 0, "transaction_date", "F2", 0),
                ("non_derivative", 0, "price_per_share", "F1", 0),
                ("non_derivative", 0, "price_per_share", "F2", 1),
                ("non_derivative", 0, "nature_of_ownership", "F3", 0),
            ],
            linked,
        )
        self.assertEqual(2, len(filing["signatures"]))
        self.assertEqual("SYNTHETIC TEST-ONLY REMARK; NOT A REAL FILING.", filing["remarks"])

    def test_duplicate_reporting_owner_ciks_fail_parser_contract(self) -> None:
        case = ORACLE["filings"]["form4_joint_sale_derivative"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes().replace(
            b"<rptOwnerCik>0000000004</rptOwnerCik>",
            b"<rptOwnerCik>3</rptOwnerCik>",
        )

        with self.assertRaisesRegex(
            InsiderParseError,
            "owner CIKs must be unique",
        ):
            parse_ownership_xml(
                raw_xml,
                accession_number=case["accession_number"],
                filing_date=case["filing_date"],
                accepted_at=case["accepted_at"],
                source_index_url=case["source_index_url"],
                source_document_url=case["source_document_url"],
            )

    def test_transaction_coding_scalars_follow_ownership_schema_shape(self) -> None:
        case = ORACLE["filings"]["form4_joint_sale_derivative"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        self.assertIn(
            b"<equitySwapInvolved>false</equitySwapInvolved>",
            raw_xml,
        )
        self.assertIn(
            b"</transactionCoding>\n      <transactionTimeliness>\n"
            b"        <value>L</value>\n      </transactionTimeliness>",
            raw_xml,
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        first_sale = filing["transactions"][0]
        self.assertIs(False, first_sale["equity_swap_involved"])
        self.assertEqual("L", first_sale["transaction_timeliness"])
        self.assertEqual(
            "false",
            first_sale["field_sources"]["equity_swap_involved"]["raw_value"],
        )
        self.assertTrue(
            first_sale["field_sources"]["transaction_timeliness"][
                "source_path"
            ].endswith("/transactionTimeliness/value")
        )

    def test_derivative_transaction_and_holding_retain_full_terms(self) -> None:
        filing = self.parse_case("form4_joint_sale_derivative")

        derivative_rows = [
            row for row in filing["transactions"]
            if row["source_table"] == "derivative"
        ]
        self.assertEqual(1, len(derivative_rows))
        row = derivative_rows[0]
        self.assertEqual(0, row["source_row_index"])
        self.assertEqual("Synthetic Test Option", row["security_title_as_filed"])
        self.assertEqual("M", row["transaction_code"])
        self.assertEqual("derivative_exercise", row["normalized_category"])
        self.assertFalse(row["is_meaningful_ps"])
        self.assertEqual("5.125", row["conversion_or_exercise_price"])
        self.assertEqual("28.875", row["reported_total_value"])
        self.assertEqual("28.875", row["transaction_value"])
        self.assertEqual("reported_total", row["value_method"])
        self.assertEqual("2026-03-01", row["exercise_date"])
        self.assertEqual("2031-03-01", row["expiration_date"])
        self.assertEqual("Class A Common Stock", row["underlying_security_title"])
        self.assertEqual("10.5", row["underlying_shares"])
        self.assertEqual("211.3125", row["underlying_value"])

        holdings = [
            holding for holding in filing["holdings"]
            if holding["source_table"] == "derivative"
        ]
        self.assertEqual(1, len(holdings))
        holding = holdings[0]
        self.assertEqual(0, holding["source_row_index"])
        self.assertEqual("4.25", holding["conversion_or_exercise_price"])
        self.assertEqual("42.125", holding["shares_owned"])
        self.assertEqual("Class A Common Stock", holding["underlying_security_title"])
        self.assertEqual("42.125", holding["underlying_shares"])
        self.assertEqual("847.765625", holding["underlying_value"])
        self.assertEqual(filing["accession_number"], holding["accession_number"])
        self.assertNotEqual(row["row_key"], holding["row_key"])

    def test_transaction_products_do_not_round_large_exact_decimals(self) -> None:
        case = ORACLE["filings"]["form4_joint_sale_derivative"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        exact_shares = b"12345678901234567890123456789"
        exact_price = b"9.876543210987654321"
        raw_xml = raw_xml.replace(
            b"<transactionShares><value>250.000</value>",
            b"<transactionShares><value>" + exact_shares + b"</value>",
        ).replace(
            b"<transactionPricePerShare>\n          <value>20.125000</value>",
            b"<transactionPricePerShare>\n          <value>"
            + exact_price
            + b"</value>",
        ).replace(
            b"<transactionShares><value>10.500</value>",
            b"<transactionShares><value>" + exact_shares + b"</value>",
        ).replace(
            b"<transactionPricePerShare><value>2.750</value>",
            b"<transactionPricePerShare><value>"
            + exact_price
            + b"</value>",
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        expected = (
            "121932631137021795224965706422.374638011112635269"
        )
        self.assertEqual(
            [expected, expected],
            [row["calculated_value"] for row in filing["transactions"] if row["price_per_share"] is not None],
        )

    def test_calculated_transaction_value_is_absolute(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes().replace(
            b"<transactionShares><value>00123.4500</value>",
            b"<transactionShares><value>-00123.4500</value>",
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        row = filing["transactions"][0]
        self.assertEqual("-123.45", row["shares"])
        self.assertEqual("1265.3625", row["calculated_value"])
        self.assertEqual("1265.3625", row["transaction_value"])

    def test_form3_holdings_only_preserves_both_tables_and_null_plan(self) -> None:
        case = ORACLE["filings"]["form3_holdings_only"]
        expected = case["expected"]
        filing = self.parse_case("form3_holdings_only")

        self.assertEqual("3", filing["form_type"])
        self.assertEqual([], filing["transactions"])
        self.assertIsNone(filing["aff10b5_one"])
        self.assertFalse(filing["no_securities_owned"])
        self.assertTrue(filing["form3_holdings_reported"])
        self.assertEqual(2, len(filing["holdings"]))
        non_derivative = next(
            row for row in filing["holdings"]
            if row["source_table"] == "non_derivative"
        )
        derivative = next(
            row for row in filing["holdings"]
            if row["source_table"] == "derivative"
        )
        self.assertEqual(expected["non_derivative_shares"], non_derivative["shares_owned"])
        self.assertEqual(expected["non_derivative_value"], non_derivative["value_owned"])
        self.assertEqual("I", non_derivative["direct_indirect_ownership"])
        self.assertEqual([expected["footnote_id"]], non_derivative["field_footnotes"]["security_title_as_filed"])
        self.assertEqual(expected["derivative_shares"], derivative["shares_owned"])
        self.assertEqual("FORM 3 TEST DATA ONLY", filing["owners"][0]["restricted_address"]["street1"])

    def test_form5_flags_and_reported_value_remain_source_distinct(self) -> None:
        case = ORACLE["filings"]["form5_annual"]
        expected = case["expected"]
        filing = self.parse_case("form5_annual")

        self.assertEqual("5", filing["form_type"])
        self.assertTrue(filing["not_subject_to_section16"])
        self.assertFalse(filing["no_securities_owned"])
        self.assertTrue(filing["form3_holdings_reported"])
        self.assertFalse(filing["form4_transactions_reported"])
        self.assertIsNone(filing["aff10b5_one"])
        self.assertEqual(1, len(filing["transactions"]))
        row = filing["transactions"][0]
        self.assertEqual(expected["transaction_code"], row["transaction_code"])
        self.assertEqual("gift", row["normalized_category"])
        self.assertFalse(row["is_meaningful_ps"])
        self.assertEqual(expected["reported_total_value"], row["reported_total_value"])
        self.assertEqual(expected["reported_total_value"], row["transaction_value"])
        self.assertEqual("reported_total", row["value_method"])
        self.assertIsNone(row["price_per_share"])
        self.assertEqual(expected["holding_shares"], filing["holdings"][0]["shares_owned"])
        self.assertEqual("SYNTHETIC FORM 5 TEST-ONLY REMARK.", filing["remarks"])

    def test_form4_amendment_is_separate_and_unresolved_without_phase3_matching(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_amendment"]
        expected = case["expected"]
        filing = self.parse_case("form4_amendment")

        self.assertEqual("4/A", filing["form_type"])
        self.assertEqual("4", filing["base_form_type"])
        self.assertTrue(filing["is_amendment"])
        self.assertEqual(expected["original_submission_date"], filing["original_submission_date"])
        self.assertNotIn("declared_in_document", filing["amendment"])
        self.assertEqual(expected["original_submission_date"], filing["amendment"]["original_submission_date"])
        self.assertIsNone(filing["amendment"]["amends_accession_number"])
        self.assertEqual("unresolved", filing["amendment"]["match_confidence"])
        self.assertEqual("unresolved_phase2", filing["amendment"]["resolution_status"])
        self.assertNotIn("is_current_effective_version", filing)
        self.assertEqual(case["accession_number"], filing["transactions"][0]["accession_number"])
        self.assertEqual(expected["transaction_value"], filing["transactions"][0]["transaction_value"])

    def test_unknown_code_elements_and_lexemes_are_preserved_for_review(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_unknown_extension"]
        expected = case["expected"]
        filing = self.parse_case("form4_unknown_extension")

        row = filing["transactions"][0]
        self.assertEqual(expected["unknown_code"], row["transaction_code"])
        self.assertEqual("unknown", row["normalized_category"])
        self.assertFalse(row["is_meaningful_ps"])
        self.assertTrue(row["requires_review"])
        self.assertEqual(expected["shares"], row["shares"])
        self.assertEqual(expected["price"], row["price_per_share"])
        self.assertIsNone(row["equity_swap_involved"])
        self.assertEqual(
            [expected["unresolved_footnote_id"]],
            row["field_footnotes"]["price_per_share"],
        )

        unknown_names = [item["local_name"] for item in filing["unknown_elements"]]
        for name in expected["unknown_names"]:
            self.assertIn(name, unknown_names)
        row_extension = next(
            item for item in filing["unknown_elements"]
            if item["local_name"] == "rowExtension"
        )
        self.assertEqual(
            "urn:synthetic:test-only:future-ownership",
            row_extension["namespace_uri"],
        )
        self.assertIn("SYNTHETIC UNKNOWN ROW VALUE", row_extension["raw_fragment"])
        self.assertTrue(
            any(
                item["kind"] == "unknown_attributes"
                and item["local_name"] == "securityTitle"
                for item in filing["unknown_elements"]
            )
        )
        warning_codes = [warning["code"] for warning in filing["warnings"]]
        self.assertIn("unknown_transaction_code", warning_codes)
        self.assertIn("unknown_element", warning_codes)
        self.assertIn("invalid_boolean", warning_codes)
        self.assertIn("unresolved_footnote_reference", warning_codes)
        unresolved = next(
            warning
            for warning in filing["warnings"]
            if warning["code"] == "unresolved_footnote_reference"
        )
        self.assertTrue(
            unresolved["source_path"].endswith(
                "/transactionPricePerShare/value"
            )
        )

    def test_unknown_control_codes_are_preserved_with_required_warnings(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        raw_xml = raw_xml.replace(
            b"      </transactionCoding>\n      <transactionAmounts>",
            b"      </transactionCoding>\n"
            b"      <transactionTimeliness><value>X</value>"
            b"</transactionTimeliness>\n"
            b"      <transactionAmounts>",
        ).replace(
            b"<transactionAcquiredDisposedCode><value>A</value>",
            b"<transactionAcquiredDisposedCode><value>Z</value>",
        ).replace(
            b"<directOrIndirectOwnership><value>D</value>",
            b"<directOrIndirectOwnership><value>X</value>",
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        transactions = cast(list[dict[str, object]], filing["transactions"])
        row = transactions[0]
        self.assertEqual("Z", row["acquired_disposed_code"])
        self.assertEqual("X", row["direct_indirect_ownership"])
        self.assertEqual("X", row["transaction_timeliness"])
        parser_warnings = cast(list[dict[str, object]], filing["warnings"])
        warnings = {
            (
                warning["field_name"],
                warning["raw_code"],
                warning["source_path"],
            )
            for warning in parser_warnings
            if warning["code"] == "unknown_control_code"
        }
        field_sources = cast(
            dict[str, dict[str, object]],
            row["field_sources"],
        )
        self.assertEqual(
            {
                (
                    field_name,
                    raw_code,
                    field_sources[field_name]["source_path"],
                )
                for field_name, raw_code in (
                    ("acquired_disposed_code", "Z"),
                    ("direct_indirect_ownership", "X"),
                    ("transaction_timeliness", "X"),
                )
            },
            warnings,
        )

        invalid = copy.deepcopy(filing)
        invalid_warnings = cast(list[dict[str, object]], invalid["warnings"])
        invalid["warnings"] = [
            warning
            for warning in invalid_warnings
            if warning["code"] != "unknown_control_code"
        ]
        with self.assertRaisesRegex(
            InsiderContractError,
            "missing required parser warning",
        ):
            validate_insider_filing(invalid)

    def test_unknown_holding_control_code_has_required_warning(self) -> None:
        case = ORACLE["filings"]["form3_holdings_only"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes().replace(
            b"<directOrIndirectOwnership><value>I</value>",
            b"<directOrIndirectOwnership><value>X</value>",
            1,
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        holdings = cast(list[dict[str, object]], filing["holdings"])
        holding = holdings[0]
        field_sources = cast(
            dict[str, dict[str, object]],
            holding["field_sources"],
        )
        expected_warning = {
            "code": "unknown_control_code",
            "source_path": field_sources["direct_indirect_ownership"]["source_path"],
            "field_name": "direct_indirect_ownership",
            "raw_code": "X",
        }
        warnings = cast(list[dict[str, object]], filing["warnings"])
        self.assertIn(expected_warning, warnings)

        invalid = copy.deepcopy(filing)
        invalid_warnings = cast(list[dict[str, object]], invalid["warnings"])
        invalid["warnings"] = [
            warning for warning in invalid_warnings if warning != expected_warning
        ]
        with self.assertRaisesRegex(
            InsiderContractError,
            "missing required parser warning",
        ):
            validate_insider_filing(invalid)

    def test_parser_warning_telemetry_is_explicitly_bounded(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes().replace(
            b"<isDirector>1</isDirector>",
            b"<isDirector>X</isDirector>",
            1,
        )

        with patch("insider_parser.MAX_WARNING_RECORDS", 0):
            with self.assertRaisesRegex(
                InsiderParseError,
                "too many parser warnings",
            ):
                parse_ownership_xml(
                    raw_xml,
                    accession_number=case["accession_number"],
                    filing_date=case["filing_date"],
                    accepted_at=case["accepted_at"],
                    source_index_url=case["source_index_url"],
                    source_document_url=case["source_document_url"],
                )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )
        with patch("insider_contract.MAX_WARNING_RECORDS", 0):
            with self.assertRaisesRegex(
                InsiderContractError,
                "too many parser warnings",
            ):
                validate_insider_filing(filing)

    def test_foreign_namespace_lookalike_cannot_override_sec_fields(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        raw_xml = raw_xml.replace(
            b'<ownershipDocument xmlns="http://www.sec.gov/edgar/document/ownership">',
            b'<ownershipDocument xmlns="http://www.sec.gov/edgar/document/ownership" '
            b'xmlns:future="urn:synthetic:test-only:future">',
        ).replace(
            b"  <documentType>4</documentType>",
            b"  <documentType>4</documentType>\n"
            b"  <future:documentType>5</future:documentType>",
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        self.assertEqual("4", filing["form_type"])
        self.assertTrue(any(
            record["local_name"] == "documentType"
            and record["namespace_uri"] == "urn:synthetic:test-only:future"
            for record in filing["unknown_elements"]
        ))

    def test_misplaced_known_name_is_retained_as_unknown_for_review(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes().replace(
            b"  <ownerSignature>",
            b"  <value>SYNTHETIC MISPLACED TEST VALUE</value>\n"
            b"  <ownerSignature>",
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        misplaced = [
            record
            for record in filing["unknown_elements"]
            if record["local_name"] == "value"
            and "SYNTHETIC MISPLACED TEST VALUE" in record["raw_fragment"]
        ]
        self.assertEqual(1, len(misplaced))
        self.assertTrue(any(
            warning["code"] == "unknown_element"
            and warning["source_path"] == misplaced[0]["source_path"]
            for warning in filing["warnings"]
        ))

    def test_foreign_descendant_cannot_contaminate_known_scalar_value(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        raw_xml = raw_xml.replace(
            b'<ownershipDocument xmlns="http://www.sec.gov/edgar/document/ownership">',
            b'<ownershipDocument xmlns="http://www.sec.gov/edgar/document/ownership" '
            b'xmlns:future="urn:synthetic:test-only:scalar">',
        ).replace(
            b"<value>00123.4500</value>",
            b"<value>00123.<future:digits>999</future:digits>4500</value>",
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        self.assertEqual("123.45", filing["transactions"][0]["shares"])
        self.assertTrue(any(
            record["local_name"] == "digits"
            for record in filing["unknown_elements"]
        ))

    def test_nested_unknown_elements_are_preserved_without_quadratic_copying(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_unknown_extension"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        nested = (
            b"<future:outer>"
            + b"<future:layer>" * 100
            + b"SYNTHETIC DEEPEST UNKNOWN"
            + b"</future:layer>" * 100
            + b"</future:outer>"
        )
        raw_xml = raw_xml.replace(
            b"  <ownerSignature>",
            b"  " + nested + b"\n  <ownerSignature>",
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        outer_records = [
            record
            for record in filing["unknown_elements"]
            if record["local_name"] in {"outer", "layer"}
        ]
        self.assertEqual(1, len(outer_records))
        self.assertIn(
            "SYNTHETIC DEEPEST UNKNOWN",
            outer_records[0]["raw_fragment"],
        )
        self.assertLess(
            len(canonical_insider_json_bytes(filing)),
            len(raw_xml) * 10,
        )

    def test_unknown_attributes_do_not_copy_nested_subtrees_quadratically(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        nested = (
            b'<value synthetic="test-only">' * 100
            + b"SYNTHETIC DEEPEST ATTRIBUTE VALUE"
            + b"</value>" * 100
        )
        raw_xml = raw_xml.replace(
            b"  <ownerSignature>",
            b"  " + nested + b"\n  <ownerSignature>",
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        captured_records = [
            record
            for record in filing["unknown_elements"]
            if "synthetic" in record["attributes"]
        ]
        self.assertEqual(1, len(captured_records))
        self.assertIn(
            "SYNTHETIC DEEPEST ATTRIBUTE VALUE",
            captured_records[0]["raw_fragment"],
        )
        self.assertLess(
            len(canonical_insider_json_bytes(filing)),
            len(raw_xml) * 10,
        )

    def test_unknown_attribute_fragment_round_trips_unqualified_descendant_namespace(
        self,
    ) -> None:
        raw_root = {
            "local_name": "ownershipDocument",
            "namespace_uri": OWNERSHIP_NAMESPACE,
            "attributes": {},
            "text": None,
            "tail": None,
            "children": [{
                "local_name": "issuer",
                "namespace_uri": OWNERSHIP_NAMESPACE,
                "attributes": {"synthetic": "test-only"},
                "text": None,
                "tail": None,
                "children": [{
                    "local_name": "issuerName",
                    "namespace_uri": None,
                    "attributes": {},
                    "text": "SYNTHETIC UNQUALIFIED DESCENDANT",
                    "tail": None,
                    "children": [],
                }],
            }],
        }

        records = derive_unknown_element_records(raw_root)

        self.assertEqual(1, len(records))
        self.assertEqual("unknown_attributes", records[0]["kind"])
        self.assertEqual("issuer", records[0]["local_name"])
        fragment = cast(str, records[0]["raw_fragment"])
        round_tripped = ElementTree.fromstring(fragment)
        self.assertEqual(f"{{{OWNERSHIP_NAMESPACE}}}issuer", round_tripped.tag)
        self.assertEqual("issuerName", round_tripped[0].tag)
        self.assertEqual(
            "SYNTHETIC UNQUALIFIED DESCENDANT",
            round_tripped[0].text,
        )

    def test_excessive_unknown_siblings_fail_closed_at_documented_bound(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_unknown_extension"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        siblings = b"".join(
            f"<future:item{i}/>".encode("ascii")
            for i in range(MAX_UNKNOWN_RECORDS + 1)
        )
        raw_xml = raw_xml.replace(
            b"  <ownerSignature>",
            b"  " + siblings + b"\n  <ownerSignature>",
        )

        with self.assertRaisesRegex(
            InsiderParseError,
            "too many unknown",
        ):
            parse_ownership_xml(
                raw_xml,
                accession_number=case["accession_number"],
                filing_date=case["filing_date"],
                accepted_at=case["accepted_at"],
                source_index_url=case["source_index_url"],
                source_document_url=case["source_document_url"],
            )

    def test_unknown_owner_address_shape_is_still_classified_private(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        raw_xml = raw_xml.replace(
            b'<ownershipDocument xmlns="http://www.sec.gov/edgar/document/ownership">',
            b'<ownershipDocument xmlns="http://www.sec.gov/edgar/document/ownership" '
            b'xmlns:future="urn:synthetic:test-only:address">',
        )
        address_start = raw_xml.index(b"    <reportingOwnerAddress>")
        address_end = (
            raw_xml.index(b"    </reportingOwnerAddress>", address_start)
            + len(b"    </reportingOwnerAddress>")
        )
        raw_xml = (
            raw_xml[:address_start]
            + b"    <reportingOwnerAddress>\n"
            + b"      <future:privateAddress>SYNTHETIC PRIVATE TEST ONLY"
            + b"</future:privateAddress>\n"
            + b"    </reportingOwnerAddress>"
            + raw_xml[address_end:]
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        self.assertEqual({}, filing["owners"][0]["restricted_address"])
        self.assertTrue(
            filing["privacy"]["contains_restricted_owner_addresses"]
        )

    def test_element_limit_rejects_before_full_dom_materialization(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()

        with patch("insider_parser.MAX_XML_ELEMENTS", 2), patch(
            "insider_parser.etree.fromstring",
            side_effect=AssertionError("full DOM materialization must not run"),
        ):
            with self.assertRaisesRegex(
                InsiderParseError,
                "too many elements",
            ):
                parse_ownership_xml(
                    raw_xml,
                    accession_number=case["accession_number"],
                    filing_date=case["filing_date"],
                    accepted_at=case["accepted_at"],
                    source_index_url=case["source_index_url"],
                    source_document_url=case["source_document_url"],
                )

    def test_dtd_and_entities_are_rejected_without_expansion(self) -> None:
        metadata = ORACLE["filings"]["form4_simple_purchase"]
        unsafe_fixtures = (
            "unsafe_internal_entity.xml",
            "unsafe_file_entity.xml",
            "unsafe_network_dtd.xml",
        )
        for filename in unsafe_fixtures:
            with self.subTest(filename=filename):
                with self.assertRaises(UnsafeOwnershipXML) as caught:
                    parse_ownership_xml(
                        (FIXTURE_ROOT / filename).read_bytes(),
                        accession_number="0000000011-26-000007",
                        filing_date="2026-07-02",
                        accepted_at="2026-07-02T20:05:06Z",
                        source_index_url=metadata["source_index_url"],
                        source_document_url=metadata["source_document_url"],
                    )
                message = str(caught.exception)
                self.assertNotIn("SYNTHETIC ENTITY EXPANSION", message)
                self.assertNotIn("synthetic-test-only-never-read", message)
                self.assertNotIn("example.invalid", message)

    def test_header_issuer_owner_and_signature_scalars_have_raw_lineage(self) -> None:
        filing = self.parse_case("form4_simple_purchase")

        self.assertEqual("ownershipDocument", filing["raw_document"]["local_name"])
        self.assertEqual(
            "/ownershipDocument/documentType",
            filing["field_sources"]["form_type"]["source_path"],
        )
        self.assertEqual(
            filing["form_type"],
            filing["field_sources"]["form_type"]["raw_value"],
        )
        self.assertEqual(
            "issuer",
            filing["issuer"]["raw_issuer"]["local_name"],
        )
        self.assertEqual(
            "/ownershipDocument/issuer/issuerName",
            filing["issuer"]["field_sources"]["name_as_filed"]["source_path"],
        )
        owner = filing["owners"][0]
        self.assertEqual(
            "/ownershipDocument/reportingOwner[1]/reportingOwnerId/rptOwnerName",
            owner["field_sources"]["name_as_filed"]["source_path"],
        )
        self.assertEqual(
            "/ownershipDocument/reportingOwner[1]/reportingOwnerAddress/rptOwnerStreet1",
            owner["field_sources"]["restricted_address.street1"]["source_path"],
        )
        signature = filing["signatures"][0]
        self.assertEqual(
            "/ownershipDocument/ownerSignature[1]/signatureName",
            signature["field_sources"]["name"]["source_path"],
        )
        self.assertEqual(
            "external_call_metadata",
            filing["source"]["field_sources"]["accession_number"]["provenance"],
        )

    def test_contract_rejects_coherent_reassignment_of_root_owner_and_signature_sources(
        self,
    ) -> None:
        filing = self.parse_case("form4_simple_purchase")

        mutations = (
            lambda value: value["field_sources"]["remarks"].update({
                "source_path": "/ownershipDocument/schemaVersion",
                "raw_value": value["schema_version"],
            }) or value.__setitem__("remarks", value["schema_version"]),
            lambda value: value["owners"][0]["field_sources"]["name_as_filed"].update({
                "source_path": "/ownershipDocument/reportingOwner[1]/reportingOwnerId/rptOwnerCik",
                "raw_value": value["owners"][0]["field_sources"]["cik"]["raw_value"],
            }) or value["owners"][0].__setitem__(
                "name_as_filed", value["owners"][0]["cik"]
            ),
            lambda value: value["signatures"][0]["field_sources"]["name"].update({
                "source_path": "/ownershipDocument/ownerSignature[1]/signatureDate",
                "raw_value": value["signatures"][0]["date"],
            }) or value["signatures"][0].__setitem__(
                "name", value["signatures"][0]["date"]
            ),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                invalid = copy.deepcopy(filing)
                mutate(invalid)
                with self.assertRaisesRegex(InsiderContractError, "field source"):
                    validate_insider_filing(invalid)

    def test_transaction_coding_and_scalar_footnotes_are_preserved_for_both_tables(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_joint_sale_derivative"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        raw_xml = raw_xml.replace(
            b"<transactionCode>S</transactionCode>",
            b'<transactionCode>S<footnoteId id="F1"/></transactionCode>'
            b'<footnoteId id="F2"/>',
        ).replace(
            b"<transactionFormType>4</transactionFormType>",
            b'<transactionFormType>4<footnoteId id="F3"/></transactionFormType>',
        ).replace(
            b"<transactionCode>M</transactionCode>",
            b'<transactionCode>M<footnoteId id="F1"/></transactionCode>'
            b'<footnoteId id="F2"/>',
        )

        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )
        rows = [
            row for row in filing["transactions"]
            if row["source_table"] in {"non_derivative", "derivative"}
        ]
        self.assertTrue(rows)
        for row in rows:
            with self.subTest(source_table=row["source_table"]):
                self.assertEqual(["F2"], row["field_footnotes"]["transaction_coding"])
                self.assertEqual(["F3"], row["field_footnotes"]["transaction_form_type"])
                self.assertEqual(["F1"], row["field_footnotes"]["transaction_code"])
                self.assertEqual(
                    row["field_footnotes"]["transaction_coding"],
                    row["field_sources"]["transaction_coding"]["footnote_ids"],
                )
        coding_links = [
            link for link in filing["field_footnote_links"]
            if link["field_name"] == "transaction_coding"
        ]
        self.assertEqual(len(rows), len(coding_links))
        self.assertTrue(all(
            link["source_path"].endswith("/transactionCoding")
            for link in coding_links
        ))

    def test_public_metadata_wrong_types_are_parse_errors(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        metadata = {
            "accession_number": case["accession_number"],
            "filing_date": case["filing_date"],
            "accepted_at": case["accepted_at"],
            "source_index_url": case["source_index_url"],
            "source_document_url": case["source_document_url"],
        }
        for field_name in metadata:
            with self.subTest(field_name=field_name):
                invalid = dict(metadata)
                invalid[field_name] = 7
                with self.assertRaises(InsiderParseError):
                    parse_ownership_xml(raw_xml, **invalid)

    def test_malformed_sec_source_url_fails_as_a_parse_error(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]

        with self.assertRaisesRegex(
            InsiderParseError,
            "allowlisted SEC URL",
        ):
            parse_ownership_xml(
                (FIXTURE_ROOT / case["filename"]).read_bytes(),
                accession_number=case["accession_number"],
                filing_date=case["filing_date"],
                accepted_at=case["accepted_at"],
                source_index_url=(
                    "https://www.sec.gov:notaport/Archives/test-only"
                ),
                source_document_url=case["source_document_url"],
            )

    def test_sec_source_urls_must_match_accession(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        compact_accession = case["accession_number"].replace("-", "")
        invalid_urls = (
            (
                "https://www.sec.gov/Archives/edgar/data/1/"
                "000000000126999999/form4-test-only.xml"
            ),
            (
                "https://www.sec.gov/Archives/not-edgar/"
                f"{compact_accession}/form4-test-only.xml"
            ),
        )

        for invalid_url in invalid_urls:
            with self.subTest(url=invalid_url):
                with self.assertRaisesRegex(
                    InsiderParseError,
                    "accession",
                ):
                    parse_ownership_xml(
                        (FIXTURE_ROOT / case["filename"]).read_bytes(),
                        accession_number=case["accession_number"],
                        filing_date=case["filing_date"],
                        accepted_at=case["accepted_at"],
                        source_index_url=case["source_index_url"],
                        source_document_url=invalid_url,
                    )

    def test_accepts_index_under_issuer_and_document_under_reporting_owner(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        reporting_owner_document_url = case["source_document_url"].replace(
            "/data/1/",
            "/data/2/",
        )

        filing = parse_ownership_xml(
            (FIXTURE_ROOT / case["filename"]).read_bytes(),
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=reporting_owner_document_url,
        )

        self.assertEqual(case["source_index_url"], filing["source"]["index_url"])
        self.assertEqual(
            reporting_owner_document_url,
            filing["source"]["document_url"],
        )

    def test_rejects_index_archive_cik_outside_issuer(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]

        for archive_cik in ("9999999999", "0000000000"):
            with self.subTest(archive_cik=archive_cik):
                with self.assertRaisesRegex(InsiderParseError, "issuer CIK"):
                    parse_ownership_xml(
                        (FIXTURE_ROOT / case["filename"]).read_bytes(),
                        accession_number=case["accession_number"],
                        filing_date=case["filing_date"],
                        accepted_at=case["accepted_at"],
                        source_index_url=case["source_index_url"].replace(
                            "/data/1/",
                            f"/data/{archive_cik}/",
                        ),
                        source_document_url=case["source_document_url"],
                    )

    def test_rejects_document_archive_cik_outside_issuer_and_reporting_owners(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]

        with self.assertRaisesRegex(InsiderParseError, "issuer CIK"):
            parse_ownership_xml(
                (FIXTURE_ROOT / case["filename"]).read_bytes(),
                accession_number=case["accession_number"],
                filing_date=case["filing_date"],
                accepted_at=case["accepted_at"],
                source_index_url=case["source_index_url"],
                source_document_url=case["source_document_url"].replace(
                    "/data/1/",
                    "/data/9999999999/",
                ),
            )

    def test_rejects_document_with_wrong_accession_directory(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        compact_accession = case["accession_number"].replace("-", "")

        with self.assertRaisesRegex(InsiderParseError, "accession"):
            parse_ownership_xml(
                (FIXTURE_ROOT / case["filename"]).read_bytes(),
                accession_number=case["accession_number"],
                filing_date=case["filing_date"],
                accepted_at=case["accepted_at"],
                source_index_url=case["source_index_url"],
                source_document_url=case["source_document_url"].replace(
                    compact_accession,
                    "000000000126999999",
                ),
            )

    def test_nested_document_archive_path_requires_safe_xml_filename(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        document_directory = case["source_document_url"].rsplit("/", 1)[0]
        invalid_document_urls = (
            f"{document_directory}/xslF345X05",
            f"{document_directory}/xslF345X05/document.txt",
            f"{document_directory}/xslF345X05//document.xml",
            f"{document_directory}/xslF345X05/../document.xml",
            f"{document_directory}/xslF345X05/%2e%2e/document.xml",
            f"{document_directory}/xslF345X05\\document.xml",
        )
        for document_url in invalid_document_urls:
            with self.subTest(document_url=document_url):
                with self.assertRaisesRegex(InsiderParseError, "accession"):
                    parse_ownership_xml(
                        raw_xml,
                        accession_number=case["accession_number"],
                        filing_date=case["filing_date"],
                        accepted_at=case["accepted_at"],
                        source_index_url=case["source_index_url"],
                        source_document_url=document_url,
                    )

        nested_index_url = case["source_index_url"].replace(
            f"/{case['accession_number']}-index.html",
            f"/nested/{case['accession_number']}-index.html",
        )
        with self.assertRaisesRegex(InsiderParseError, "accession"):
            parse_ownership_xml(
                raw_xml,
                accession_number=case["accession_number"],
                filing_date=case["filing_date"],
                accepted_at=case["accepted_at"],
                source_index_url=nested_index_url,
                source_document_url=case["source_document_url"],
            )

    def test_invalid_normalized_xml_values_fail_as_parse_errors(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        source = (FIXTURE_ROOT / case["filename"]).read_bytes()
        mutations = {
            "issuer CIK": source.replace(
                b"<issuerCik>0000000001</issuerCik>",
                b"<issuerCik>NOT-A-CIK</issuerCik>",
            ),
            "decimal": source.replace(
                b"<value>00123.4500</value>",
                b"<value>NOT-A-DECIMAL</value>",
                1,
            ),
        }

        for label, xml_bytes in mutations.items():
            with self.subTest(case=label):
                self.assertNotEqual(source, xml_bytes)
                with self.assertRaisesRegex(
                    InsiderParseError,
                    "invalid normalized values",
                ):
                    parse_ownership_xml(
                        xml_bytes,
                        accession_number=case["accession_number"],
                        filing_date=case["filing_date"],
                        accepted_at=case["accepted_at"],
                        source_index_url=case["source_index_url"],
                        source_document_url=case["source_document_url"],
                    )

    def test_reparse_and_canonical_json_round_trip_are_deterministic(self) -> None:
        for case_name in ORACLE["filings"]:
            with self.subTest(case=case_name):
                first = self.parse_case(case_name)
                second = self.parse_case(case_name)

                self.assertEqual(first, second)
                first_bytes = canonical_insider_json_bytes(first)
                self.assertEqual(first_bytes, canonical_insider_json_bytes(second))
                self.assertTrue(first_bytes.endswith(b"\n"))
                self.assertEqual(first, json.loads(first_bytes))

        round_tripped = json.loads(
            canonical_insider_json_bytes(
                self.parse_case("form4_joint_sale_derivative")
            )
        )
        missing_price_row = round_tripped["transactions"][1]
        self.assertEqual("5.00000001", missing_price_row["shares"])
        self.assertIsNone(missing_price_row["price_per_share"])

    def test_canonical_joint_filing_revalidates_after_json_round_trip(self) -> None:
        rendered = canonical_insider_json_bytes(
            self.parse_case("form4_joint_sale_derivative")
        )
        round_tripped = json.loads(rendered)

        validated = validate_insider_filing(round_tripped)

        self.assertEqual(rendered, canonical_insider_json_bytes(validated))

    def test_contract_rejects_noncanonical_or_nonstring_decimal_fields(
        self,
    ) -> None:
        filing = self.parse_case("form4_simple_purchase")
        invalid_values = (123, 123.45, "00123.4500")

        for invalid_value in invalid_values:
            with self.subTest(invalid_value=invalid_value):
                invalid = copy.deepcopy(filing)
                invalid["transactions"][0]["shares"] = invalid_value
                with self.assertRaisesRegex(
                    InsiderContractError,
                    "canonical decimal string",
                ):
                    validate_insider_filing(invalid)

    def test_contract_rejects_malformed_temporal_fields(self) -> None:
        invalid_date = "2026-02-30"
        invalid_timestamp = "2026-02-30T25:61:61Z"

        def set_external_metadata(
            value: dict[str, object],
            field_name: str,
            invalid_value: str,
        ) -> None:
            value[field_name] = invalid_value
            source = cast(dict[str, object], value["source"])
            field_sources = cast(dict[str, object], source["field_sources"])
            field_source = cast(dict[str, object], field_sources[field_name])
            field_source["raw_value"] = invalid_value
            field_source["normalized_value"] = invalid_value

        cases = (
            (
                "form4_simple_purchase",
                "accepted_at",
                "ISO timestamp",
                lambda value: set_external_metadata(
                    value,
                    "accepted_at",
                    invalid_timestamp,
                ),
            ),
            (
                "form4_simple_purchase",
                "filing_date",
                "ISO date",
                lambda value: set_external_metadata(
                    value,
                    "filing_date",
                    invalid_date,
                ),
            ),
            (
                "form4_simple_purchase",
                "period_of_report",
                "ISO date",
                lambda value: value.__setitem__("period_of_report", invalid_date),
            ),
            (
                "form4_simple_purchase",
                "original_submission_date",
                "ISO date",
                lambda value: value.__setitem__(
                    "original_submission_date",
                    invalid_date,
                ),
            ),
            (
                "form4_simple_purchase",
                "signature.date",
                "ISO date",
                lambda value: value["signatures"][0].__setitem__(
                    "date",
                    invalid_date,
                ),
            ),
            (
                "form4_simple_purchase",
                "transaction_date",
                "ISO date",
                lambda value: value["transactions"][0].__setitem__(
                    "transaction_date",
                    invalid_date,
                ),
            ),
            (
                "form4_simple_purchase",
                "deemed_execution_date",
                "ISO date",
                lambda value: value["transactions"][0].__setitem__(
                    "deemed_execution_date",
                    invalid_date,
                ),
            ),
            (
                "form4_simple_purchase",
                "transaction.exercise_date",
                "ISO date",
                lambda value: value["transactions"][0].__setitem__(
                    "exercise_date",
                    invalid_date,
                ),
            ),
            (
                "form4_simple_purchase",
                "transaction.expiration_date",
                "ISO date",
                lambda value: value["transactions"][0].__setitem__(
                    "expiration_date",
                    invalid_date,
                ),
            ),
            (
                "form3_holdings_only",
                "holding.exercise_date",
                "ISO date",
                lambda value: value["holdings"][0].__setitem__(
                    "exercise_date",
                    invalid_date,
                ),
            ),
            (
                "form3_holdings_only",
                "holding.expiration_date",
                "ISO date",
                lambda value: value["holdings"][0].__setitem__(
                    "expiration_date",
                    invalid_date,
                ),
            ),
        )

        for case_name, label, error_pattern, mutate in cases:
            with self.subTest(field=label):
                invalid = self.parse_case(case_name)
                mutate(invalid)
                with self.assertRaisesRegex(
                    InsiderContractError,
                    error_pattern,
                ):
                    validate_insider_filing(invalid)

    def test_contract_rejects_duplicate_source_rows(self) -> None:
        filing = self.parse_case("form4_simple_purchase")
        filing["transactions"].append(
            copy.deepcopy(filing["transactions"][0])
        )

        with self.assertRaisesRegex(
            InsiderContractError,
            "duplicate transaction source row",
        ):
            validate_insider_filing(filing)

    def test_contract_binds_row_keys_to_source_identity(self) -> None:
        filing = self.parse_case("form4_simple_purchase")
        transactions = cast(list[dict[str, object]], filing["transactions"])
        transactions[0]["row_key"] = "f" * 64

        with self.assertRaisesRegex(
            InsiderContractError,
            "row_key does not match source identity",
        ):
            validate_insider_filing(filing)

    def test_contract_rejects_missing_or_malformed_root_lineage(self) -> None:
        filing = self.parse_case("form4_simple_purchase")
        mutations = (
            ("boolean contract version", lambda value: value.__setitem__("insider_contract_version", True)),
            ("missing accession", lambda value: value.pop("accession_number")),
            ("invalid raw hash", lambda value: value.__setitem__("raw_sha256", "not-a-hash")),
            ("missing source", lambda value: value.pop("source")),
            ("invalid source", lambda value: value["source"].__setitem__("document_url", 7)),
            ("missing issuer", lambda value: value.pop("issuer")),
            ("zero issuer cik", lambda value: value["issuer"].__setitem__("cik", "0000000000")),
            ("invalid owners", lambda value: value.__setitem__("owners", {})),
            ("zero owner cik", lambda value: value["owners"][0].__setitem__("cik", "0000000000")),
            ("missing signatures", lambda value: value.pop("signatures")),
            ("invalid schema version", lambda value: value.__setitem__("schema_version", [])),
            ("blank parser version", lambda value: value.__setitem__("parser_version", "")),
        )

        for label, mutate in mutations:
            with self.subTest(case=label):
                invalid = copy.deepcopy(filing)
                mutate(invalid)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

    def test_contract_rejects_malformed_row_lineage_and_scalar_containers(
        self,
    ) -> None:
        filing = self.parse_case("form4_simple_purchase")
        mutations = (
            lambda value: value["transactions"][0].__setitem__("shares", {}),
            lambda value: value.__setitem__("aff10b5_one", []),
            lambda value: value["transactions"][0].pop("row_key"),
            lambda value: value["transactions"][0].__setitem__("row_key", ""),
            lambda value: value["transactions"][0].__setitem__(
                "source_row_index",
                "zero",
            ),
            lambda value: value["transactions"][0].__setitem__(
                "source_row_index",
                -1,
            ),
            lambda value: value["transactions"][0].__setitem__(
                "source_table",
                "future_table",
            ),
        )

        for index, mutate in enumerate(mutations):
            with self.subTest(case=index):
                invalid = copy.deepcopy(filing)
                mutate(invalid)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

    def test_contract_requires_structured_raw_and_absolute_source_lineage(
        self,
    ) -> None:
        filing = self.parse_case("form4_simple_purchase")
        cases = (
            lambda value: value["transactions"][0].__setitem__("raw_row", {}),
            lambda value: value["owners"][0].__setitem__("raw_owner", {}),
            lambda value: value["transactions"][0].__setitem__("source_path", "relative"),
            lambda value: value["transactions"][0]["field_sources"][
                "shares"
            ].__setitem__("source_path", "relative"),
            lambda value: value["transactions"][0].__setitem__(
                "source_path",
                "/ownershipDocument/SYNTHETIC",
            ),
            lambda value: value["transactions"][0]["field_sources"][
                "shares"
            ].__setitem__(
                "source_path",
                "/ownershipDocument/SYNTHETIC",
            ),
        )

        for index, mutate in enumerate(cases):
            with self.subTest(case=index):
                invalid = copy.deepcopy(filing)
                mutate(invalid)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

    def test_contract_requires_every_parser_defined_field_source(self) -> None:
        simple = self.parse_case("form4_simple_purchase")
        form3 = self.parse_case("form3_holdings_only")
        joint = self.parse_case("form4_joint_sale_derivative")
        cases = (
            (simple, "transactions", 0, "transaction_code"),
            (simple, "transactions", 0, "post_transaction_value"),
            (form3, "holdings", 0, "shares_owned"),
            (joint, "transactions", 2, "conversion_or_exercise_price"),
            (joint, "holdings", 0, "underlying_security_title"),
        )

        for source, collection, row_index, field_name in cases:
            with self.subTest(collection=collection, field=field_name):
                invalid = copy.deepcopy(source)
                invalid[collection][row_index]["field_sources"].pop(field_name)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

    def test_contract_binds_normalized_values_to_raw_field_lineage(self) -> None:
        filing = self.parse_case("form4_simple_purchase")

        def change_normalized_values(value: dict[str, object]) -> None:
            transactions = cast(list[object], value["transactions"])
            row = cast(dict[str, object], transactions[0])
            row["shares"] = "999"
            row["calculated_value"] = "10239.75"
            row["transaction_value"] = "10239.75"

        def change_source_and_normalized_values(value: dict[str, object]) -> None:
            change_normalized_values(value)
            transactions = cast(list[object], value["transactions"])
            row = cast(dict[str, object], transactions[0])
            field_sources = cast(dict[str, object], row["field_sources"])
            shares_source = cast(dict[str, object], field_sources["shares"])
            shares_source["raw_value"] = "999"

        def reassign_field_to_another_raw_source(
            value: dict[str, object],
        ) -> None:
            transactions = cast(list[object], value["transactions"])
            row = cast(dict[str, object], transactions[0])
            field_sources = cast(dict[str, object], row["field_sources"])
            shares_source = cast(dict[str, object], field_sources["shares"])
            price_source = cast(
                dict[str, object],
                field_sources["price_per_share"],
            )
            shares_source.update(price_source)
            price = cast(str, row["price_per_share"])
            row["shares"] = price
            calculated_value = absolute_decimal_product(price, price)
            row["calculated_value"] = calculated_value
            row["transaction_value"] = calculated_value

        for mutate in (
            change_normalized_values,
            change_source_and_normalized_values,
            reassign_field_to_another_raw_source,
        ):
            with self.subTest(mutation=mutate.__name__):
                invalid = copy.deepcopy(filing)
                mutate(invalid)
                with self.assertRaisesRegex(
                    InsiderContractError,
                    "field source",
                ):
                    validate_insider_filing(invalid)

    def test_contract_enforces_transaction_fields_code_and_plan_semantics(
        self,
    ) -> None:
        simple = self.parse_case("form4_simple_purchase")
        unknown = self.parse_case("form4_unknown_extension")
        cases = (
            (simple, lambda value: value["transactions"][0].pop("transaction_code")),
            (simple, lambda value: value["transactions"][0].pop("security_title_as_filed")),
            (simple, lambda value: value["transactions"][0].pop("plan_status")),
            (simple, lambda value: value["transactions"][0].pop("direct_indirect_ownership")),
            (unknown, lambda value: value["transactions"][0].__setitem__("normalized_category", "purchase")),
            (unknown, lambda value: value["transactions"][0].__setitem__("is_meaningful_ps", True)),
            (simple, lambda value: value["transactions"][0].__setitem__("plan_status", "filing_marked")),
        )

        for index, (source, mutate) in enumerate(cases):
            with self.subTest(case=index):
                invalid = copy.deepcopy(source)
                mutate(invalid)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

    def test_contract_recomputes_exact_transaction_value_semantics(self) -> None:
        filing = self.parse_case("form4_simple_purchase")
        mutations = (
            lambda value: value["transactions"][0].__setitem__(
                "calculated_value",
                "999",
            ),
            lambda value: value["transactions"][0].__setitem__(
                "transaction_value",
                "888",
            ),
            lambda value: value["transactions"][0].__setitem__(
                "value_method",
                "reported_total",
            ),
        )

        for index, mutate in enumerate(mutations):
            with self.subTest(case=index):
                invalid = copy.deepcopy(filing)
                mutate(invalid)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

    def test_contract_validates_owner_signature_and_footnote_shapes(self) -> None:
        simple = self.parse_case("form4_simple_purchase")
        joint = self.parse_case("form4_joint_sale_derivative")
        cases = (
            (simple, lambda value: value.__setitem__("owners", ["invalid"])),
            (simple, lambda value: value["owners"][0].__setitem__("cik", "2")),
            (simple, lambda value: value["owners"][0].__setitem__("owner_order", True)),
            (simple, lambda value: value["owners"][0].__setitem__("restricted_address", [])),
            (simple, lambda value: value["owners"][0].pop("raw_owner")),
            (simple, lambda value: value["owners"][0].pop("is_director")),
            (simple, lambda value: value["owners"][0].pop("country")),
            (simple, lambda value: value["issuer"].pop("foreign_trading_symbol_as_filed")),
            (simple, lambda value: value.__setitem__("signatures", [7])),
            (simple, lambda value: value["signatures"][0].pop("date")),
            (
                simple,
                lambda value: value["signatures"][0].__setitem__(
                    "source_path",
                    "relative/signature",
                ),
            ),
            (joint, lambda value: value["footnotes"][0].pop("text")),
        )

        for index, (source, mutate) in enumerate(cases):
            with self.subTest(case=index):
                invalid = copy.deepcopy(source)
                mutate(invalid)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

    def test_contract_keeps_amendment_metadata_phase2_only(self) -> None:
        amendment = self.parse_case("form4_amendment")
        for field in (
            "original_submission_date",
            "amends_accession_number",
            "match_confidence",
            "resolution_status",
        ):
            with self.subTest(missing=field):
                invalid = copy.deepcopy(amendment)
                invalid["amendment"].pop(field)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

        invalid = copy.deepcopy(amendment)
        invalid["amendment"]["is_current_effective_version"] = True
        with self.assertRaises(InsiderContractError):
            validate_insider_filing(invalid)

        original = self.parse_case("form4_simple_purchase")
        original["amendment"]["resolution_status"] = "unresolved_phase2"
        with self.assertRaises(InsiderContractError):
            validate_insider_filing(original)

    def test_contract_enforces_exact_tristate_booleans(self) -> None:
        filing = self.parse_case("form4_simple_purchase")
        mutations = (
            ("filing", "aff10b5_one", "false"),
            ("owner", "is_director", 1),
            ("transaction", "equity_swap_involved", "0"),
        )

        for target, field_name, invalid_value in mutations:
            with self.subTest(target=target, field=field_name):
                invalid = copy.deepcopy(filing)
                container = (
                    invalid
                    if target == "filing"
                    else invalid["owners"][0]
                    if target == "owner"
                    else invalid["transactions"][0]
                )
                container[field_name] = invalid_value
                with self.assertRaisesRegex(
                    InsiderContractError,
                    "tri-state boolean",
                ):
                    validate_insider_filing(invalid)

    def test_contract_rejects_incomplete_flattened_footnote_linkage(self) -> None:
        filing = self.parse_case("form4_joint_sale_derivative")
        filing["field_footnote_links"].pop()

        with self.assertRaisesRegex(
            InsiderContractError,
            "field footnote links do not match row references",
        ):
            validate_insider_filing(filing)

    def test_footnote_definitions_retain_exact_raw_source_lineage(self) -> None:
        filing = self.parse_case("form4_joint_sale_derivative")
        footnotes = cast(list[dict[str, object]], filing["footnotes"])

        for index, footnote in enumerate(footnotes, start=1):
            with self.subTest(footnote_id=footnote["id"]):
                raw_footnote = cast(
                    dict[str, object],
                    footnote["raw_footnote"],
                )
                raw_attributes = cast(dict[str, str], raw_footnote["attributes"])
                self.assertEqual(
                    f"/ownershipDocument/footnotes/footnote[{index}]",
                    footnote["source_path"],
                )
                self.assertEqual("footnote", raw_footnote["local_name"])
                self.assertEqual(footnote["id"], raw_attributes["id"])

    def test_contract_rejects_footnote_definition_text_divergence(self) -> None:
        filing = self.parse_case("form4_joint_sale_derivative")
        footnotes = cast(list[dict[str, object]], filing["footnotes"])
        footnotes[0]["text"] = "FORGED FOOTNOTE TEXT"

        with self.assertRaisesRegex(
            InsiderContractError,
            "footnote definition does not match raw lineage",
        ):
            validate_insider_filing(filing)

    def test_contract_derives_field_footnotes_from_raw_row(self) -> None:
        filing = self.parse_case("form4_joint_sale_derivative")
        transactions = cast(list[dict[str, object]], filing["transactions"])
        row = next(
            candidate
            for candidate in transactions
            if any(
                cast(dict[str, list[str]], candidate["field_footnotes"]).values()
            )
        )
        field_footnotes = cast(dict[str, list[str]], row["field_footnotes"])
        field_name = next(
            name
            for name, footnote_ids in field_footnotes.items()
            if footnote_ids
        )
        field_footnotes[field_name] = []
        field_sources = cast(dict[str, dict[str, object]], row["field_sources"])
        field_sources[field_name]["footnote_ids"] = []
        links = cast(list[dict[str, object]], filing["field_footnote_links"])
        filing["field_footnote_links"] = [
            link
            for link in links
            if not (
                link["row_key"] == row["row_key"]
                and link["field_name"] == field_name
            )
        ]

        with self.assertRaisesRegex(
            InsiderContractError,
            "field footnotes do not match raw lineage",
        ):
            validate_insider_filing(filing)

    def test_contract_binds_duplicate_raw_subtrees_to_raw_document(self) -> None:
        filing = self.parse_case("form4_simple_purchase")
        owners = cast(list[dict[str, object]], filing["owners"])
        owner = owners[0]
        raw_owner = cast(dict[str, object], owner["raw_owner"])
        raw_owner_children = cast(
            list[dict[str, object]],
            raw_owner["children"],
        )
        raw_owner_id = next(
            child
            for child in raw_owner_children
            if child["local_name"] == "reportingOwnerId"
        )
        raw_owner_id_children = cast(
            list[dict[str, object]],
            raw_owner_id["children"],
        )
        raw_owner_name = next(
            child
            for child in raw_owner_id_children
            if child["local_name"] == "rptOwnerName"
        )
        raw_owner_name["text"] = "FORGED OWNER NAME"
        owner["name_as_filed"] = "FORGED OWNER NAME"
        owner_field_sources = cast(
            dict[str, dict[str, object]],
            owner["field_sources"],
        )
        owner_field_sources["name_as_filed"]["raw_value"] = (
            "FORGED OWNER NAME"
        )

        with self.assertRaisesRegex(
            InsiderContractError,
            "raw lineage does not match raw_document",
        ):
            validate_insider_filing(filing)

    def test_contract_rejects_inconsistent_footnote_source_lineage(self) -> None:
        filing = self.parse_case("form4_joint_sale_derivative")
        cases = (
            lambda value: value["transactions"][0]["field_sources"][
                "price_per_share"
            ].__setitem__("footnote_ids", []),
            lambda value: value["field_footnote_links"][0].__setitem__(
                "source_path",
                "/ownershipDocument/SYNTHETIC-WRONG-PATH",
            ),
        )

        for index, mutate in enumerate(cases):
            with self.subTest(case=index):
                invalid = copy.deepcopy(filing)
                mutate(invalid)
                with self.assertRaisesRegex(
                    InsiderContractError,
                    "footnote",
                ):
                    validate_insider_filing(invalid)

    def test_contract_requires_unknown_and_warning_traceability(self) -> None:
        filing = self.parse_case("form4_unknown_extension")
        for field in (
            "kind",
            "source_path",
            "namespace_uri",
            "local_name",
            "attributes",
            "text",
            "raw_fragment",
        ):
            with self.subTest(unknown_field=field):
                invalid = copy.deepcopy(filing)
                invalid["unknown_elements"][0].pop(field)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

        for field in ("code", "source_path"):
            with self.subTest(warning_field=field):
                invalid = copy.deepcopy(filing)
                invalid["warnings"][0].pop(field)
                with self.assertRaises(InsiderContractError):
                    validate_insider_filing(invalid)

        for code in (
            "unknown_element",
            "unknown_attribute",
            "invalid_boolean",
            "unknown_transaction_code",
            "unresolved_footnote_reference",
        ):
            with self.subTest(missing_warning=code):
                invalid = copy.deepcopy(filing)
                invalid["warnings"] = [
                    warning
                    for warning in invalid["warnings"]
                    if warning["code"] != code
                ]
                with self.assertRaisesRegex(
                    InsiderContractError,
                    "missing required parser warning",
                ):
                    validate_insider_filing(invalid)

    def test_contract_rejects_removed_unknown_telemetry_from_raw_document(
        self,
    ) -> None:
        invalid = copy.deepcopy(self.parse_case("form4_unknown_extension"))
        invalid["unknown_elements"] = []
        invalid["warnings"] = [
            warning
            for warning in invalid["warnings"]
            if warning["code"] not in {"unknown_element", "unknown_attribute"}
        ]

        with self.assertRaisesRegex(
            InsiderContractError,
            "unknown element telemetry",
        ):
            validate_insider_filing(invalid)

    def test_contract_rejects_coherently_mutated_unknown_telemetry(
        self,
    ) -> None:
        invalid = copy.deepcopy(self.parse_case("form4_unknown_extension"))
        record = cast(
            dict[str, object],
            cast(list[object], invalid["unknown_elements"])[0],
        )
        original_name = record["local_name"]
        record["local_name"] = "syntheticCoherentMutation"
        for warning in invalid["warnings"]:
            if (
                warning["code"] in {"unknown_element", "unknown_attribute"}
                and warning["source_path"] == record["source_path"]
                and warning["local_name"] == original_name
            ):
                warning["local_name"] = record["local_name"]

        with self.assertRaisesRegex(
            InsiderContractError,
            "unknown element telemetry",
        ):
            validate_insider_filing(invalid)

    def test_contract_rejects_owner_address_source_flag_divergence(self) -> None:
        filing = self.parse_case("form4_simple_purchase")
        filing["owners"][0]["has_restricted_address_source"] = False

        with self.assertRaisesRegex(
            InsiderContractError,
            "address-source classification",
        ):
            validate_insider_filing(filing)

    def test_private_contract_cannot_be_marked_for_public_projection(self) -> None:
        filing = self.parse_case("form4_simple_purchase")
        filing["privacy"]["public_projection_allowed"] = True

        with self.assertRaisesRegex(
            InsiderContractError,
            "private filing cannot be public",
        ):
            validate_insider_filing(filing)

    def test_private_contract_cannot_hide_present_owner_address_data(self) -> None:
        filing = self.parse_case("form4_simple_purchase")
        filing["privacy"]["contains_restricted_owner_addresses"] = False

        with self.assertRaisesRegex(
            InsiderContractError,
            "address classification",
        ):
            validate_insider_filing(filing)


if __name__ == "__main__":
    unittest.main()
