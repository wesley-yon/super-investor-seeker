from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import validate_data


def holding(
    position: int,
    *,
    value: int,
    shares: int,
    holding_type: str = "EQUITY",
    amount_type: str | None = None,
) -> dict:
    row = {
        "cusip": f"{position:09d}",
        "issuer": f"Issuer {position}",
        "value": value,
        "shares": shares,
        "holding_type": holding_type,
    }
    if amount_type is not None:
        row["share_amount_type"] = amount_type
    return row


def quarter(report_date: str, holdings: list[dict]) -> dict:
    return {
        "report_date": report_date,
        "holdings": holdings,
        "total_value": sum(row["value"] for row in holdings),
    }


def validate(quarters: list[dict]) -> list[str]:
    errors: list[str] = []
    validate_data.validate_adjacent_quarter_value_units(
        {"quarters": quarters},
        "test fund",
        errors,
    )
    return errors


class AdjacentQuarterValueUnitValidationTests(unittest.TestCase):
    def test_rejects_missing_provenance_prn_quarter_at_1000x(self) -> None:
        older = [
            holding(
                position,
                value=1_000_000 + position * 10_000,
                shares=1_000_000,
                holding_type="NOTE",
                amount_type="PRN",
            )
            for position in range(12)
        ]
        newer = [
            {
                **row,
                "value": row["value"] * 1000,
            }
            for row in older
        ]

        errors = validate([
            quarter("2026-03-31", newer),
            quarter("2025-12-31", older),
        ])

        self.assertEqual(1, len(errors))
        self.assertIn("value per share/principal is about 1000x higher", errors[0])
        self.assertIn("12/12 shared positions", errors[0])

    def test_rejects_mixed_unit_quarter_despite_one_large_normal_row(self) -> None:
        older = [
            holding(
                position,
                value=(20_000_000_000 if position == 0 else 200_000),
                shares=1_000_000,
            )
            for position in range(12)
        ]
        newer = [
            {
                **row,
                "value": (
                    int(row["value"] * 1.02)
                    if position == 0
                    else row["value"] * 1000
                ),
            }
            for position, row in enumerate(older)
        ]

        errors = validate([
            quarter("2025-09-30", newer),
            quarter("2025-06-30", older),
        ])

        self.assertEqual(1, len(errors))
        self.assertIn("mixed value-unit clusters", errors[0])
        self.assertIn("matched=12", errors[0])
        self.assertIn("inflated count/value=0.917", errors[0])

    def test_allows_turnover_and_large_position_size_changes(self) -> None:
        older = [
            holding(
                position,
                value=100 * (position + 1),
                shares=10 * (position + 1),
            )
            for position in range(20)
        ]
        newer = [
            holding(
                position,
                value=110 * (position + 1) * 1000,
                shares=10 * (position + 1) * 1000,
            )
            for position in range(10, 30)
        ]

        self.assertEqual(
            [],
            validate([
                quarter("2026-03-31", newer),
                quarter("2025-12-31", older),
            ]),
        )

    def test_requires_ten_shared_cluster_positions(self) -> None:
        older = [
            holding(position, value=100_000, shares=10)
            for position in range(9)
        ]
        newer = [
            {
                **row,
                "value": row["value"] // 1000,
            }
            for row in older
        ]

        self.assertEqual(
            [],
            validate([
                quarter("2026-03-31", newer),
                quarter("2025-12-31", older),
            ]),
        )

    def test_rejects_1000x_lower_values(self) -> None:
        older = [
            holding(position, value=100_000, shares=10)
            for position in range(10)
        ]
        newer = [
            {
                **row,
                "value": row["value"] // 1000,
            }
            for row in older
        ]

        errors = validate([
            quarter("2026-03-31", newer),
            quarter("2025-12-31", older),
        ])

        self.assertEqual(1, len(errors))
        self.assertIn("value per share/principal is about 1000x lower", errors[0])
        self.assertIn("10/10 shared positions", errors[0])

    def test_rejects_eight_of_ten_scale_cluster_boundary(self) -> None:
        older = [
            holding(position, value=100_000, shares=10)
            for position in range(10)
        ]
        newer = [
            {
                **row,
                "value": row["value"] * (1000 if position < 8 else 10),
            }
            for position, row in enumerate(older)
        ]

        errors = validate([
            quarter("2026-03-31", newer),
            quarter("2025-12-31", older),
        ])

        self.assertEqual(1, len(errors))
        self.assertIn("8/10 shared positions", errors[0])
        self.assertIn("count support=0.800", errors[0])

    def test_ignores_non_adjacent_retained_quarters(self) -> None:
        older = [
            holding(position, value=100, shares=10)
            for position in range(12)
        ]
        newer = [
            {
                **row,
                "value": row["value"] * 1000,
            }
            for row in older
        ]

        self.assertEqual(
            [],
            validate([
                quarter("2026-03-31", newer),
                quarter("2025-09-30", older),
            ]),
        )

    def test_peer_validator_describes_mixed_count_value_clusters(self) -> None:
        report_date = "2025-12-31"
        holdings = [
            holding(position, value=1_000, shares=1_000)
            for position in range(10)
        ]
        holdings.append(
            holding(10, value=200_000, shares=1_000)
        )
        references = {
            (report_date, row["cusip"]): (
                1_000.0 if index < 10 else 200.0,
                4,
            )
            for index, row in enumerate(holdings)
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            (funds_dir / "999.json").write_text(json.dumps({
                "cik": 999,
                "quarters": [quarter(report_date, holdings)],
            }))
            errors: list[str] = []
            with mock.patch.object(
                validate_data,
                "FUNDS_DIR",
                funds_dir,
            ):
                validate_data.validate_value_unit_peer_consistency(
                    references,
                    errors,
                )

        self.assertEqual(1, len(errors))
        self.assertIn("mixed_scale_clusters", errors[0])
        self.assertIn("aligned 0.091/0.952", errors[0])
        self.assertIn("understated 0.909/0.048", errors[0])

    def test_peer_validator_checks_intrinsic_shape_without_references(
        self,
    ) -> None:
        report_date = "2025-12-31"
        holdings = [
            holding(position, value=100, shares=1_000)
            for position in range(10)
        ]
        holdings.append(
            holding(10, value=1_000_000, shares=10_000)
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            (funds_dir / "999.json").write_text(json.dumps({
                "cik": 999,
                "quarters": [quarter(report_date, holdings)],
            }))
            errors: list[str] = []
            with mock.patch.object(
                validate_data,
                "FUNDS_DIR",
                funds_dir,
            ):
                validate_data.validate_value_unit_peer_consistency(
                    {},
                    errors,
                )

        self.assertEqual(1, len(errors))
        self.assertIn("mixed_scale_clusters", errors[0])
        self.assertIn(
            "intrinsic low-price count/value support=0.909/0.001",
            errors[0],
        )
        self.assertIn("peer count coverage=0.000", errors[0])


if __name__ == "__main__":
    unittest.main()
