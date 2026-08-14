from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import validate_data


class FilerIdentityValidationTests(unittest.TestCase):
    def test_fund_name_is_required_and_carried_into_identity_calendar(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            funds_dir = Path(temporary)
            fund_path = funds_dir / "123.json"
            fund_path.write_text(json.dumps({
                "cik": 123,
                "name": "",
                "quarters": [],
            }))
            errors: list[str] = []
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                _files, _groups, _cusips, calendars, _stats = (
                    validate_data.validate_funds(errors, {})
                )

            self.assertTrue(any(
                "fund file 123.json has invalid filer name" in error
                for error in errors
            ))
            self.assertNotIn("name", calendars["123"])

            fund_path.write_text(json.dumps({
                "cik": 123,
                "name": "Correct Adviser, LLC",
                "quarters": [],
            }))
            errors.clear()
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                _files, _groups, _cusips, calendars, _stats = (
                    validate_data.validate_funds(errors, {})
                )

            self.assertEqual([], errors)
            self.assertEqual("Correct Adviser, LLC", calendars["123"]["name"])

    def test_index_name_must_match_its_cik_fund_file(self) -> None:
        index = {
            "funds": [{
                "cik": 123,
                "name": "Wrong Adviser, LLC",
                "q": [],
            }],
            "tickers": [],
            "total_filers": 1,
            "total_tickers": 0,
        }
        calendars = {
            "123": {
                "name": "Correct Adviser, LLC",
                "report_dates": (),
                "report_date_set": frozenset(),
                "q": (),
            }
        }
        errors: list[str] = []

        validate_data.validate_index(
            index,
            {"123": Path("123.json")},
            {},
            {},
            errors,
            [],
            calendars,
            {},
        )

        self.assertTrue(any(
            "index.json fund cik 123 name" in error
            and "does not match fund file" in error
            for error in errors
        ))

    def test_stock_holder_name_must_match_its_cik_fund_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            stocks_dir = Path(temporary)
            (stocks_dir / "111111111.json").write_text(json.dumps({
                "stock_id": "111111111",
                "cusip": "111111111",
                "ticker": "TEST",
                "issuer": "Test Issuer",
                "instrument_type": "EQUITY",
                "holders": [{
                    "cik": 123,
                    "name": "Wrong Adviser, LLC",
                    "history": [],
                }],
            }))
            calendars = {
                "123": {
                    "name": "Correct Adviser, LLC",
                    "report_dates": (),
                    "report_date_set": frozenset(),
                    "q": (),
                }
            }
            errors: list[str] = []

            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(
                    errors,
                    calendars,
                    registry={},
                )

            self.assertTrue(any(
                "stock file 111111111.json holder 0 name" in error
                and "does not match fund 123" in error
                for error in errors
            ))

    def test_normalized_legal_name_collisions_are_reported_by_cik(self) -> None:
        collisions = validate_data.filer_name_collision_groups({
            "1765681": {"name": "Thrive Capital Management, LLC"},
            "1845943": {"name": "THRIVE CAPITAL MANAGEMENT LLC"},
            "1000000": {"name": "Different Adviser"},
        })

        self.assertEqual(
            [{
                "name": "Thrive Capital Management, LLC",
                "ciks": ("1765681", "1845943"),
            }],
            collisions,
        )


if __name__ == "__main__":
    unittest.main()
