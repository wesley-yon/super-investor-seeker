"""Frozen amendment-variant coverage for SEC ownership forms."""

from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

from insider_contract import canonical_insider_json_bytes
from insider_parser import parse_ownership_xml


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "insider_filings"
ORACLE = json.loads((FIXTURE_ROOT / "expectations.json").read_text())


class InsiderAmendmentVariantTests(unittest.TestCase):
    def test_forms_3a_4a_and_5a_are_frozen_and_parse_deterministically(self) -> None:
        cases = {
            "form3_amendment": ("3/A", 0, 2),
            "form4_amendment": ("4/A", 1, 0),
            "form5_amendment": ("5/A", 1, 1),
        }

        for case_name, (
            expected_form_type,
            expected_transactions,
            expected_holdings,
        ) in cases.items():
            with self.subTest(case=case_name):
                case = ORACLE["filings"][case_name]
                raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
                self.assertEqual(
                    case["sha256"],
                    hashlib.sha256(raw_xml).hexdigest(),
                )

                kwargs = {
                    "accession_number": case["accession_number"],
                    "filing_date": case["filing_date"],
                    "accepted_at": case["accepted_at"],
                    "source_index_url": case["source_index_url"],
                    "source_document_url": case["source_document_url"],
                }
                first = parse_ownership_xml(raw_xml, **kwargs)
                second = parse_ownership_xml(raw_xml, **kwargs)

                self.assertEqual(expected_form_type, first["form_type"])
                self.assertEqual(expected_form_type[0], first["base_form_type"])
                self.assertIs(True, first["is_amendment"])
                transactions = first["transactions"]
                holdings = first["holdings"]
                self.assertIsInstance(transactions, list)
                self.assertIsInstance(holdings, list)
                assert isinstance(transactions, list)
                assert isinstance(holdings, list)
                self.assertEqual(expected_transactions, len(transactions))
                self.assertEqual(expected_holdings, len(holdings))
                self.assertEqual(
                    case["expected"]["original_submission_date"],
                    first["original_submission_date"],
                )
                self.assertEqual(
                    first["original_submission_date"],
                    first["amendment"]["original_submission_date"],
                )
                self.assertIsNone(first["amendment"]["amends_accession_number"])
                self.assertEqual(
                    "unresolved",
                    first["amendment"]["match_confidence"],
                )
                self.assertEqual(
                    "unresolved_phase2",
                    first["amendment"]["resolution_status"],
                )
                self.assertEqual(first, second)
                self.assertEqual(
                    canonical_insider_json_bytes(first),
                    canonical_insider_json_bytes(second),
                )


if __name__ == "__main__":
    unittest.main()
