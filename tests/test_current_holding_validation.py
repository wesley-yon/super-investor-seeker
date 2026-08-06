from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import validate_data


def holding(
    cusip: str,
    value: int,
    shares: int,
    holding_type: str = "EQUITY",
) -> dict:
    return {
        "ticker": "AAPL",
        "issuer": "APPLE INC",
        "cusip": cusip,
        "value": value,
        "shares": shares,
        "holding_type": holding_type,
    }


def quarter(report_date: str, holdings: list[dict]) -> dict:
    return {
        "report_date": report_date,
        "filing_date": "2026-05-15",
        "total_value": sum(row["value"] for row in holdings),
        "num_holdings": len(holdings),
        "holdings": holdings,
    }


def history_entry(
    report_date: str,
    value: int,
    shares: int,
    pct_of_fund: float,
) -> dict:
    return {
        "date": report_date,
        "value": value,
        "shares": shares,
        "pct_of_fund": pct_of_fund,
    }


class SparseHistoryClassificationTests(unittest.TestCase):
    def test_classification_uses_presence_on_each_funds_actual_calendar(self) -> None:
        exit_history = [
            history_entry("2025-12-31", 100, 10, 100.0),
        ]
        self.assertEqual(
            "EXIT",
            validate_data.classify_sparse_history(
                exit_history,
                ("2026-03-31", "2025-12-31"),
            )[0],
        )

        reentry_history = [
            history_entry("2026-03-31", 120, 12, 100.0),
            history_entry("2025-09-30", 80, 8, 100.0),
        ]
        self.assertEqual(
            "NEW",
            validate_data.classify_sparse_history(
                reentry_history,
                ("2026-03-31", "2025-12-31", "2025-09-30"),
            )[0],
        )

        same_observation = [
            history_entry("2025-12-31", 50, 5, 100.0),
        ]
        self.assertEqual(
            "NEW",
            validate_data.classify_sparse_history(
                same_observation,
                ("2025-12-31", "2025-09-30"),
            )[0],
        )
        self.assertEqual(
            "HISTORICAL",
            validate_data.classify_sparse_history(
                same_observation,
                ("2026-03-31", "2025-09-30"),
            )[0],
        )

        zero_record = history_entry("2026-03-31", 0, 0, 0.0)
        status, current, _previous = validate_data.classify_sparse_history(
            [zero_record],
            ("2026-03-31", "2025-12-31"),
        )
        self.assertEqual("NEW", status)
        self.assertIs(current, zero_record)


class CurrentHoldingCorpusValidationTests(unittest.TestCase):
    def _write_fixture(self, data_dir: Path) -> None:
        funds_dir = data_dir / "funds"
        stocks_dir = data_dir / "stocks"
        funds_dir.mkdir()
        stocks_dir.mkdir()

        funds = {
            1: [
                quarter(
                    "2026-03-31",
                    [
                        holding("037833100", 100, 10),
                        holding("037833100", 25, 2, "CALL"),
                    ],
                ),
                quarter(
                    "2025-12-31",
                    [
                        holding("037833100", 90, 9),
                        holding("037833100", 20, 2, "CALL"),
                    ],
                ),
            ],
            # Staggered filer: its own latest report is still 2025-12-31.
            2: [
                quarter("2025-12-31", [holding("037833100", 50, 5)]),
                quarter("2025-09-30", [holding("037833100", 40, 4)]),
            ],
            # Re-entry after an absent immediately preceding report.
            3: [
                quarter("2026-03-31", [holding("037833100", 30, 3)]),
                quarter("2025-12-31", []),
                quarter("2025-09-30", [holding("037833100", 20, 2)]),
            ],
            # A persisted zero-valued row is present and therefore current.
            4: [
                quarter("2026-03-31", [holding("037833100", 0, 0)]),
                quarter("2025-12-31", []),
            ],
            # Empty latest portfolio: the prior holding is an exit, not current.
            5: [
                quarter("2026-03-31", []),
                quarter("2025-12-31", [holding("037833100", 999, 99)]),
            ],
        }
        for cik, quarters in funds.items():
            (funds_dir / f"{cik}.json").write_text(json.dumps({
                "cik": cik,
                "name": f"Fund {cik}",
                "quarters": quarters,
            }))

        equity_holders = [
            {
                "cik": 1,
                "name": "Fund 1",
                "history": [
                    history_entry("2026-03-31", 100, 10, 80.0),
                    history_entry("2025-12-31", 90, 9, 81.818),
                ],
            },
            {
                "cik": 2,
                "name": "Fund 2",
                "history": [
                    history_entry("2025-12-31", 50, 5, 100.0),
                    history_entry("2025-09-30", 40, 4, 100.0),
                ],
            },
            {
                "cik": 3,
                "name": "Fund 3",
                "history": [
                    history_entry("2026-03-31", 30, 3, 100.0),
                    history_entry("2025-09-30", 20, 2, 100.0),
                ],
            },
            {
                "cik": 4,
                "name": "Fund 4",
                "history": [
                    history_entry("2026-03-31", 0, 0, 0.0),
                ],
            },
            {
                "cik": 5,
                "name": "Fund 5",
                "history": [
                    history_entry("2025-12-31", 999, 99, 100.0),
                ],
            },
        ]
        (stocks_dir / "037833100.json").write_text(json.dumps({
            "stock_id": "037833100",
            "cusip": "037833100",
            "ticker": "AAPL",
            "issuer": "APPLE INC",
            "instrument_type": "EQUITY",
            "holders": equity_holders,
        }))
        (stocks_dir / "037833100__CALL.json").write_text(json.dumps({
            "stock_id": "037833100|CALL",
            "cusip": "037833100",
            "ticker": "AAPL",
            "issuer": "APPLE INC",
            "instrument_type": "CALL",
            "holders": [{
                "cik": 1,
                "name": "Fund 1",
                "history": [
                    history_entry("2026-03-31", 25, 2, 20.0),
                    history_entry("2025-12-31", 20, 2, 18.182),
                ],
            }],
        }))

    def test_current_aggregates_ignore_exits_and_keep_instruments_separate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            self._write_fixture(data_dir)
            errors: list[str] = []
            with mock.patch.object(
                validate_data,
                "FUNDS_DIR",
                data_dir / "funds",
            ):
                (
                    _fund_files,
                    _stock_groups,
                    _fund_cusips,
                    calendars,
                    expected,
                ) = validate_data.validate_funds(errors, {})

            self.assertEqual([], errors)
            self.assertEqual(4, expected["037833100"]["holder_count"])
            self.assertEqual(180, expected["037833100"]["total_value"])
            self.assertEqual(100, expected["037833100"]["largest_value"])
            self.assertEqual(1, expected["037833100|CALL"]["holder_count"])

            with mock.patch.object(
                validate_data,
                "STOCKS_DIR",
                data_dir / "stocks",
            ):
                validate_data.validate_stocks(errors, calendars, expected)
            self.assertEqual([], errors)

    def test_reconciliation_rejects_stale_holder_as_current_largest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            self._write_fixture(data_dir)
            errors: list[str] = []
            with mock.patch.object(
                validate_data,
                "FUNDS_DIR",
                data_dir / "funds",
            ):
                (
                    _fund_files,
                    _stock_groups,
                    _fund_cusips,
                    calendars,
                    expected,
                ) = validate_data.validate_funds(errors, {})
            self.assertEqual([], errors)

            stock_path = data_dir / "stocks" / "037833100.json"
            stock = json.loads(stock_path.read_text())
            stock["holders"][-1]["history"].insert(
                0,
                history_entry("2026-03-31", 999, 99, 100.0),
            )
            stock_path.write_text(json.dumps(stock))

            with mock.patch.object(
                validate_data,
                "STOCKS_DIR",
                data_dir / "stocks",
            ):
                validate_data.validate_stocks(errors, calendars, expected)
            self.assertTrue(any(
                "largest_value=999 expected 100" in error
                for error in errors
            ))

    def test_reconciliation_rejects_missing_prior_exit_observation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            self._write_fixture(data_dir)
            errors: list[str] = []
            with mock.patch.object(
                validate_data,
                "FUNDS_DIR",
                data_dir / "funds",
            ):
                (
                    _fund_files,
                    _stock_groups,
                    _fund_cusips,
                    calendars,
                    expected,
                ) = validate_data.validate_funds(errors, {})
            self.assertEqual([], errors)

            stock_path = data_dir / "stocks" / "037833100.json"
            stock = json.loads(stock_path.read_text())
            stock["holders"][-1]["history"] = []
            stock_path.write_text(json.dumps(stock))

            with mock.patch.object(
                validate_data,
                "STOCKS_DIR",
                data_dir / "stocks",
            ):
                validate_data.validate_stocks(errors, calendars, expected)
            self.assertTrue(any(
                "latest-two observations differ" in error
                for error in errors
            ))

    def test_reconciliation_rejects_missing_older_retained_observation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            self._write_fixture(data_dir)
            errors: list[str] = []
            with mock.patch.object(
                validate_data,
                "FUNDS_DIR",
                data_dir / "funds",
            ):
                (
                    _fund_files,
                    _stock_groups,
                    _fund_cusips,
                    calendars,
                    expected,
                ) = validate_data.validate_funds(errors, {})
            self.assertEqual([], errors)

            stock_path = data_dir / "stocks" / "037833100.json"
            stock = json.loads(stock_path.read_text())
            reentry_holder = next(
                holder for holder in stock["holders"] if holder["cik"] == 3
            )
            reentry_holder["history"] = [
                entry
                for entry in reentry_holder["history"]
                if entry["date"] != "2025-09-30"
            ]
            stock_path.write_text(json.dumps(stock))

            with mock.patch.object(
                validate_data,
                "STOCKS_DIR",
                data_dir / "stocks",
            ):
                validate_data.validate_stocks(errors, calendars, expected)
            self.assertTrue(any(
                "retained-quarter observations differ" in error
                for error in errors
            ))


class FundCalendarIndexValidationTests(unittest.TestCase):
    def test_fund_calendar_rejects_malformed_and_duplicate_dates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            funds_dir = Path(temporary) / "funds"
            funds_dir.mkdir()
            (funds_dir / "1.json").write_text(json.dumps({
                "cik": 1,
                "name": "Fund 1",
                "quarters": [
                    quarter("2025-12-31", []),
                    quarter("2026-03-30", []),
                    quarter("2025-12-31", []),
                ],
            }))
            errors: list[str] = []
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                validate_data.validate_funds(errors, {})

        self.assertTrue(any(
            "non-canonical report_date" in error
            for error in errors
        ))
        self.assertTrue(any("unsorted quarters" in error for error in errors))
        self.assertTrue(any(
            "duplicate report_date" in error
            for error in errors
        ))

    def test_index_calendar_must_exactly_match_persisted_quarters(self) -> None:
        calendar = {
            "1": {
                "report_dates": (
                    "2026-03-31",
                    "2025-12-31",
                    "2025-09-30",
                    "2025-06-30",
                    "2025-03-31",
                ),
                "report_date_set": frozenset({
                    "2026-03-31",
                    "2025-12-31",
                    "2025-09-30",
                    "2025-06-30",
                    "2025-03-31",
                }),
                "q": (20261, 20254, 20253, 20252),
            },
        }
        index = {
            "funds": [{
                "cik": 1,
                "name": "Fund 1",
                "q": [20261, 20254, 20253, 20252],
            }],
            "tickers": [],
            "total_filers": 1,
            "total_tickers": 0,
        }
        errors: list[str] = []
        validate_data.validate_index(
            index,
            {"1": Path("1.json")},
            {},
            {},
            errors,
            [],
            calendar,
        )
        self.assertEqual([], errors)

        index["funds"][0]["q"] = [20261, 20253, 20254]
        validate_data.validate_index(
            index,
            {"1": Path("1.json")},
            {},
            {},
            errors,
            [],
            calendar,
        )
        self.assertTrue(any(
            "does not match persisted fund quarters" in error
            for error in errors
        ))
        self.assertTrue(any(
            "not newest-first and de-duplicated" in error
            for error in errors
        ))

        errors.clear()
        index["funds"][0]["q"] = [20261, "20254"]
        validate_data.validate_index(
            index,
            {"1": Path("1.json")},
            {},
            {},
            errors,
            [],
            calendar,
        )
        self.assertTrue(any(
            "invalid YYYYQ code" in error
            for error in errors
        ))

    def test_report_date_encoder_rejects_non_quarter_dates(self) -> None:
        self.assertEqual(20261, validate_data.report_quarter_code("2026-03-31"))
        for invalid in (
            "2026-03-30",
            "2026-02-31",
            "03/31/2026",
            20260331,
            None,
        ):
            with self.subTest(invalid=invalid):
                self.assertIsNone(validate_data.report_quarter_code(invalid))

    def test_array_dates_are_reported_without_crashing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir()
            stocks_dir.mkdir()
            (funds_dir / "1.json").write_text(json.dumps({
                "cik": 1,
                "name": "Fund 1",
                "quarters": [{
                    "report_date": [],
                    "filing_date": "2026-05-15",
                    "total_value": 0,
                    "num_holdings": 0,
                    "holdings": [],
                }],
            }))
            (stocks_dir / "037833100.json").write_text(json.dumps({
                "stock_id": "037833100",
                "cusip": "037833100",
                "ticker": "AAPL",
                "issuer": "APPLE INC",
                "instrument_type": "EQUITY",
                "holders": [{
                    "cik": 1,
                    "name": "Fund 1",
                    "history": [{
                        "date": [],
                        "value": 0,
                        "shares": 0,
                        "pct_of_fund": 0,
                    }],
                }],
            }))

            errors: list[str] = []
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                (
                    _fund_files,
                    _stock_groups,
                    _fund_cusips,
                    calendars,
                    _expected,
                ) = validate_data.validate_funds(errors, {})
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(errors, calendars)

        self.assertTrue(any(
            "non-canonical report_date" in error
            for error in errors
        ))
        self.assertTrue(any(
            "history 0 has invalid date" in error
            for error in errors
        ))

    def test_ciks_must_be_positive_integers_in_every_artifact(self) -> None:
        self.assertEqual("1", validate_data.canonical_cik(1))
        for invalid in ("001", "1", 0, -1, True, None):
            with self.subTest(invalid=invalid):
                self.assertIsNone(validate_data.canonical_cik(invalid))

        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir()
            stocks_dir.mkdir()
            (funds_dir / "1.json").write_text(json.dumps({
                "cik": "001",
                "name": "Fund 1",
                "quarters": [quarter("2026-03-31", [])],
            }))
            (stocks_dir / "037833100.json").write_text(json.dumps({
                "stock_id": "037833100",
                "cusip": "037833100",
                "ticker": "AAPL",
                "issuer": "APPLE INC",
                "instrument_type": "EQUITY",
                "holders": [{
                    "cik": "001",
                    "name": "Fund 1",
                    "history": [],
                }],
            }))

            errors: list[str] = []
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                validate_data.validate_funds(errors, {})
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(errors, {})
            validate_data.validate_index(
                {
                    "funds": [{"cik": "001", "name": "Fund 1", "q": []}],
                    "tickers": [],
                    "total_filers": 1,
                    "total_tickers": 0,
                },
                {"1": funds_dir / "1.json"},
                {"037833100": stocks_dir / "037833100.json"},
                {},
                errors,
                [],
                {},
            )

        self.assertTrue(any(
            error.startswith("fund file") and "invalid cik" in error
            for error in errors
        ))
        self.assertTrue(any(
            error.startswith("stock file") and "invalid cik" in error
            for error in errors
        ))
        self.assertTrue(any(
            error.startswith("index.json") and "invalid cik" in error
            for error in errors
        ))


if __name__ == "__main__":
    unittest.main()
