"""Offline parser checks against sanitized, source-derived SEC XML."""

from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from typing import Any, cast

from insider_contract import canonical_insider_json_bytes
from insider_parser import parse_ownership_xml


FIXTURE_ROOT = (
    Path(__file__).parent
    / "fixtures"
    / "insider_filings"
    / "sec_derived"
)


class SanitizedSecOwnershipFixtureTests(unittest.TestCase):
    def assert_rows_have_sanitized_private_text(
        self,
        rows: list[dict[str, Any]],
    ) -> None:
        for row in rows:
            for private_text_field in (
                "security_title_as_filed",
                "underlying_security_title",
                "nature_of_ownership",
            ):
                private_text = row.get(private_text_field)
                if private_text is not None:
                    self.assertIsInstance(private_text, str)
                    assert isinstance(private_text, str)
                    self.assertIn("SYNTHETIC", private_text)

    def test_private_text_check_rejects_unsanitized_security_title(self) -> None:
        with self.assertRaises(AssertionError):
            self.assert_rows_have_sanitized_private_text(
                [
                    {
                        "security_title_as_filed": "Real retained security title",
                        "underlying_security_title": None,
                        "nature_of_ownership": None,
                    }
                ]
            )

    def test_all_supported_forms_have_provenanced_source_derived_fixtures(
        self,
    ) -> None:
        manifest = cast(
            dict[str, Any],
            json.loads((FIXTURE_ROOT / "manifest.json").read_text()),
        )
        self.assertEqual(
            "sanitized_source_derived_sec_ownership_xml",
            manifest["fixture_kind"],
        )
        self.assertEqual("section16-sec-sanitize-v1", manifest["sanitization_profile"])
        manifest_privacy = cast(dict[str, Any], manifest["privacy_metadata"])
        self.assertFalse(manifest_privacy["raw_source_retained"])
        self.assertFalse(manifest_privacy["network_required_for_tests"])
        self.assertTrue(
            manifest_privacy["parser_source_urls_sanitized_to_fixture_issuer"]
        )
        filings = cast(dict[str, dict[str, Any]], manifest["filings"])
        self.assertEqual(
            {"3", "3/A", "4", "4/A", "5", "5/A"},
            {case["form_type"] for case in filings.values()},
        )

        for case_name, case in filings.items():
            with self.subTest(case=case_name):
                privacy = cast(dict[str, Any], case["privacy_metadata"])
                self.assertEqual(
                    "section16-sec-sanitize-v1",
                    case["sanitization_profile"],
                )
                self.assertEqual(
                    "SEC filing index page",
                    privacy["filing_date_source"],
                )
                self.assertFalse(privacy["raw_source_retained"])
                self.assertEqual("passed", privacy["source_text_replacement_check"])
                raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
                self.assertEqual(
                    case["sanitized_sha256"],
                    hashlib.sha256(raw_xml).hexdigest(),
                )
                self.assertRegex(case["original_sha256"], r"^[0-9a-f]{64}$")
                self.assertNotEqual(
                    case["original_sha256"],
                    case["sanitized_sha256"],
                )
                self.assertTrue(case["source_index_url"].startswith("https://www.sec.gov/Archives/"))
                self.assertTrue(case["source_document_url"].startswith("https://www.sec.gov/Archives/"))
                self.assertIn("/Archives/edgar/data/1/", case["parser_source_index_url"])
                self.assertIn(
                    "/Archives/edgar/data/1/",
                    case["parser_source_document_url"],
                )
                self.assertNotEqual(
                    case["source_index_url"],
                    case["parser_source_index_url"],
                )
                self.assertNotEqual(
                    case["source_document_url"],
                    case["parser_source_document_url"],
                )

                kwargs = {
                    "accession_number": case["accession_number"],
                    "filing_date": case["filing_date"],
                    "accepted_at": None,
                    "source_index_url": case["parser_source_index_url"],
                    "source_document_url": case["parser_source_document_url"],
                }
                first = parse_ownership_xml(raw_xml, **kwargs)
                second = parse_ownership_xml(raw_xml, **kwargs)

                self.assertEqual(case["form_type"], first["form_type"])
                self.assertEqual(case["schema_version"], first["schema_version"])
                self.assertEqual(first, second)
                self.assertEqual(
                    canonical_insider_json_bytes(first),
                    canonical_insider_json_bytes(second),
                )
                expected = cast(dict[str, Any], case["expected"])
                owners = cast(list[dict[str, Any]], first["owners"])
                transactions = cast(
                    list[dict[str, Any]],
                    first["transactions"],
                )
                holdings = cast(list[dict[str, Any]], first["holdings"])
                footnotes = cast(list[dict[str, Any]], first["footnotes"])
                signatures = cast(list[dict[str, Any]], first["signatures"])
                self.assertEqual(expected["owner_count"], len(owners))
                self.assertEqual(expected["transaction_count"], len(transactions))
                self.assertEqual(expected["holding_count"], len(holdings))
                self.assertEqual(expected["signature_count"], len(signatures))
                self.assertEqual(
                    expected["transaction_codes"],
                    [row["transaction_code"] for row in transactions],
                )
                self.assertEqual(
                    expected["transaction_timeliness"],
                    [row["transaction_timeliness"] for row in transactions],
                )
                self.assertEqual(
                    expected["footnote_ids"],
                    [footnote["id"] for footnote in footnotes],
                )
                self.assertEqual(
                    expected["transaction_source_rows"],
                    [
                        [row["source_table"], row["source_row_index"]]
                        for row in transactions
                    ],
                )
                self.assertEqual(
                    expected["holding_source_rows"],
                    [
                        [row["source_table"], row["source_row_index"]]
                        for row in holdings
                    ],
                )
                if "transaction_timeliness_source_paths" in expected:
                    self.assertEqual(
                        expected["transaction_timeliness_source_paths"],
                        [
                            cast(
                                dict[str, Any],
                                row["field_sources"],
                            )["transaction_timeliness"]["source_path"]
                            for row in transactions
                        ],
                    )
                if "unknown_timeliness_warning_paths" in expected:
                    warnings = cast(list[dict[str, Any]], first["warnings"])
                    self.assertEqual(
                        expected["unknown_timeliness_warning_paths"],
                        [
                            warning["source_path"]
                            for warning in warnings
                            if warning["code"] == "unknown_control_code"
                            and warning["field_name"] == "transaction_timeliness"
                        ],
                    )
                self.assertTrue(
                    all(
                        owner["name_as_filed"].startswith("SYNTHETIC SOURCE OWNER")
                        for owner in owners
                    )
                )
                issuer = cast(dict[str, Any], first["issuer"])
                self.assertEqual(
                    "SYNTHETIC SOURCE ISSUER",
                    issuer["name_as_filed"],
                )
                self.assertEqual("0000000001", issuer["cik"])
                self.assertEqual("SYNTHETIC", issuer["trading_symbol_as_filed"])
                for owner in owners:
                    self.assertTrue(str(owner["cik"]).startswith("0000001"))
                    self.assertIn(owner["country"], {None, "ZZ"})
                    for private_text_field in ("officer_title", "other_text"):
                        private_text = owner[private_text_field]
                        if private_text is not None:
                            self.assertIn("SYNTHETIC", private_text)
                    restricted_address = cast(
                        dict[str, str],
                        owner["restricted_address"],
                    )
                    self.assertNotIn(
                        "@",
                        json.dumps(restricted_address),
                    )
                    self.assertTrue(
                        all(
                            "SYNTHETIC" in value or value in {"ZZ", "00000"}
                            for value in restricted_address.values()
                        )
                    )

                remarks = first["remarks"]
                if remarks is not None:
                    self.assertIsInstance(remarks, str)
                    assert isinstance(remarks, str)
                    self.assertIn("SYNTHETIC", remarks)
                self.assertTrue(
                    all(
                        signature["name"].startswith("SYNTHETIC SOURCE SIGNATORY")
                        for signature in signatures
                    )
                )
                rows = transactions + holdings
                self.assert_rows_have_sanitized_private_text(rows)
                self.assertTrue(
                    all("SYNTHETIC" in footnote["text"] for footnote in footnotes)
                )


if __name__ == "__main__":
    unittest.main()
