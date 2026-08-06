import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pipeline


class FundReportCalendarTests(unittest.TestCase):
    def test_report_quarter_code_accepts_only_canonical_quarter_ends(self) -> None:
        expected = {
            "2025-03-31": 20251,
            "2025-06-30": 20252,
            "2025-09-30": 20253,
            "2025-12-31": 20254,
        }
        for report_date, code in expected.items():
            with self.subTest(report_date=report_date):
                self.assertEqual(code, pipeline.report_quarter_code(report_date))

        invalid = [
            None,
            20250331,
            "",
            "03/31/2025",
            "20250331",
            "2025-6-30",
            "2025-02-29",
            "2025-03-30",
            "2025-12-32",
        ]
        for report_date in invalid:
            with self.subTest(report_date=report_date):
                self.assertIsNone(pipeline.report_quarter_code(report_date))

    def test_fund_report_quarter_codes_sort_deduplicate_and_limit(self) -> None:
        quarters = [
            {"report_date": "2024-12-31"},
            {"report_date": "2026-03-31"},
            {"report_date": "2025-06-30"},
            {"report_date": "2025-12-31"},
            {"report_date": "2025-09-30"},
            {"report_date": "2025-12-31"},
            {"report_date": "2026-01-15"},
            {"report_date": None},
            "not a quarter",
        ]

        self.assertEqual(
            [20261, 20254, 20253, 20252],
            pipeline.fund_report_quarter_codes(quarters),
        )
        self.assertEqual([], pipeline.fund_report_quarter_codes(None))

    def test_generator_writes_exact_calendar_to_both_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir()
            stocks_dir.mkdir()
            (funds_dir / "123456.json").write_text(json.dumps({
                "cik": 123456,
                "name": "Example Fund",
                "quarters": [
                    {"report_date": "2025-09-30", "holdings": []},
                    {"report_date": "2026-03-31", "holdings": []},
                    {
                        "report_date": "2025-12-31",
                        "total_value": 150,
                        "holdings": [
                            {
                                "ticker": "AAPL",
                                "issuer": "APPLE INC",
                                "cusip": "037833100",
                                "value": 100,
                                "shares": 10,
                                "holding_type": "EQUITY",
                            },
                            {
                                "ticker": "AAPL",
                                "issuer": "APPLE INC",
                                "cusip": "037833100",
                                "value": 50,
                                "shares": 5,
                                "holding_type": "CALL",
                            },
                        ],
                    },
                    {"report_date": "2025-06-30", "holdings": []},
                    {"report_date": "2025-03-31", "holdings": []},
                    {"report_date": "2025-12-31", "holdings": []},
                    {"report_date": "2026-01-15", "holdings": []},
                ],
            }))

            with (
                mock.patch.object(pipeline, "DATA_DIR", data_dir),
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(pipeline, "STOCKS_DIR", stocks_dir),
                mock.patch.object(pipeline, "INDEX_PATH", data_dir / "index.json"),
                mock.patch.object(
                    pipeline,
                    "FUNDS_INDEX_PATH",
                    data_dir / "funds-index.json",
                ),
                mock.patch.object(pipeline, "load_cusip_registry", return_value={}),
            ):
                pipeline.regenerate_stock_files_and_index()

            index = json.loads((data_dir / "index.json").read_text())
            funds_index = json.loads((data_dir / "funds-index.json").read_text())
            expected_fund = {
                "cik": 123456,
                "name": "Example Fund",
                "q": [20261, 20254, 20253, 20252],
            }
            self.assertEqual([expected_fund], index["funds"])
            self.assertEqual([expected_fund], funds_index["funds"])

            equity_stock = json.loads(
                (stocks_dir / "037833100.json").read_text()
            )
            call_stock = json.loads(
                (stocks_dir / "037833100__CALL.json").read_text()
            )
            self.assertEqual("EQUITY", equity_stock["instrument_type"])
            self.assertEqual("CALL", call_stock["instrument_type"])
            self.assertEqual(
                ["2025-12-31"],
                [row["date"] for row in equity_stock["holders"][0]["history"]],
            )
            self.assertEqual(
                ["2025-12-31"],
                [row["date"] for row in call_stock["holders"][0]["history"]],
            )


if __name__ == "__main__":
    unittest.main()
