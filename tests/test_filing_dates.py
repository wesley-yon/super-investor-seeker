from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import validate_data


class FilingDateValidationTests(unittest.TestCase):
    def validate_quarter(
        self,
        filing_date: object,
        *,
        source_filings: object = None,
    ) -> tuple[list[str], dict[str, object]]:
        with tempfile.TemporaryDirectory() as temporary:
            funds_dir = Path(temporary)
            quarter = {
                "report_date": "2026-03-31",
                "filing_date": filing_date,
                "holdings": [],
                "num_holdings": 0,
                "total_value": 0,
            }
            if source_filings is not None:
                quarter["source_filings"] = source_filings
            (funds_dir / "123.json").write_text(json.dumps({
                "cik": 123,
                "name": "Example Adviser, LLC",
                "quarters": [quarter],
            }))
            errors: list[str] = []
            quality_summary: dict[str, object] = {
                "legacy_month_precision_filing_dates": 0,
            }
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                validate_data.validate_funds(
                    errors,
                    {},
                    quality_summary,
                )
            return errors, quality_summary

    def test_precision_parser_rejects_impossible_or_ambiguous_shapes(self) -> None:
        self.assertEqual(
            "DAY",
            validate_data.filing_date_precision("2026-05-14"),
        )
        self.assertEqual(
            "MONTH",
            validate_data.filing_date_precision("2026-05"),
        )
        for value in (
            "2026-02-31",
            "2026-13",
            "0000-02-29",
            "0000-02",
            "2026",
            "May 2026",
            "",
            None,
        ):
            with self.subTest(value=value):
                self.assertIsNone(validate_data.filing_date_precision(value))

    def test_legacy_month_precision_is_counted_without_inventing_a_day(self) -> None:
        errors, quality_summary = self.validate_quarter("2026-05")

        self.assertFalse(any("filing_date" in error for error in errors))
        self.assertEqual(
            1,
            quality_summary["legacy_month_precision_filing_dates"],
        )

    def test_malformed_filing_date_fails_validation(self) -> None:
        errors, _quality_summary = self.validate_quarter("2026-02-31")

        self.assertTrue(any(
            "has invalid filing_date '2026-02-31'" in error
            for error in errors
        ))

    def test_month_precision_is_legacy_only_when_provenance_is_absent(self) -> None:
        errors, quality_summary = self.validate_quarter(
            "2026-05",
            source_filings=[],
        )

        self.assertTrue(any(
            "has month-only filing_date '2026-05' despite structured provenance"
            in error
            for error in errors
        ))
        self.assertEqual(
            0,
            quality_summary["legacy_month_precision_filing_dates"],
        )


if __name__ == "__main__":
    unittest.main()
