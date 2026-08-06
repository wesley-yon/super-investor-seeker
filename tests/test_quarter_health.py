from __future__ import annotations

import json
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path
from unittest import mock

import quarter_health
import validate_data


REPORT_DATE = "2025-12-31"


def holding(
    position: int,
    *,
    value: int | float,
    shares: int | float,
    holding_type: str = "EQUITY",
    **extra: object,
) -> dict:
    return {
        "cusip": f"{position:09d}",
        "issuer": f"Issuer {position}",
        "ticker": f"T{position}",
        "holding_type": holding_type,
        "value": value,
        "shares": shares,
        **extra,
    }


def quarter(holdings: list[dict], *, num_holdings: int | None = None) -> dict:
    return {
        "report_date": REPORT_DATE,
        "filing_date": "2026-02-14",
        "total_value": sum(row["value"] for row in holdings),
        "num_holdings": (
            len(holdings) if num_holdings is None else num_holdings
        ),
        "holdings": holdings,
    }


def clean_holdings(count: int = 12) -> list[dict]:
    return [
        holding(
            position,
            value=(100 + position) * (100_000 + position),
            shares=100_000 + position,
        )
        for position in range(count)
    ]


def peer_index_for(
    target_quarter: dict,
    *,
    peer_count: int = 4,
) -> dict:
    index = defaultdict(lambda: defaultdict(list))
    for peer_number in range(peer_count):
        rows = [
            {
                **row,
                "value": (
                    row["shares"]
                    * (100 + position)
                    * (1 + (peer_number - 1.5) / 100)
                ),
            }
            for position, row in enumerate(clean_holdings())
        ]
        quarter_health.add_quarter_peer_observations(
            index,
            filer_id=f"peer-{peer_number}",
            quarter=quarter(rows),
        )
    quarter_health.add_quarter_peer_observations(
        index,
        filer_id="target",
        quarter=target_quarter,
    )
    return index


class StructuralQuarterHealthTests(unittest.TestCase):
    def test_non_list_holdings_fail_closed(self) -> None:
        issues = quarter_health.quarter_health_issues({
            "num_holdings": 0,
            "holdings": None,
        })
        self.assertEqual(["invalid_holdings"], [issue.code for issue in issues])

    def test_legacy_empty_quarter_cannot_bypass_holding_count_check(self) -> None:
        malformed = quarter([], num_holdings=1)
        self.assertNotIn("composition_version", malformed)

        with tempfile.TemporaryDirectory() as temporary:
            funds_dir = Path(temporary)
            (funds_dir / "1.json").write_text(json.dumps({
                "cik": 1,
                "name": "Legacy Fund",
                "quarters": [malformed],
            }))
            errors: list[str] = []
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                validate_data.validate_funds(errors, {})

        self.assertTrue(any(
            "failed quarter health [holding_count_mismatch]" in error
            and "num_holdings=1 does not match holdings length 0" in error
            for error in errors
        ))

    def test_rejects_filing_wide_duplicated_value_and_share_columns(
        self,
    ) -> None:
        malformed = quarter([
            holding(position, value=10_000 + position, shares=10_000 + position)
            for position in range(10)
        ])

        issues = quarter_health.quarter_health_issues(malformed)

        self.assertEqual(
            ["duplicated_value_share_columns"],
            [issue.code for issue in issues],
        )
        self.assertIn("10/10 eligible positions", issues[0].detail)

    def test_options_imputed_rows_and_principal_amounts_do_not_vote(
        self,
    ) -> None:
        rows = clean_holdings(10)
        rows.extend(
            holding(
                100 + position,
                value=5_000 + position,
                shares=5_000 + position,
                holding_type="CALL",
            )
            for position in range(10)
        )
        rows.extend(
            holding(
                200 + position,
                value=6_000 + position,
                shares=6_000 + position,
                shares_imputed=True,
            )
            for position in range(10)
        )
        rows.extend(
            holding(
                300 + position,
                value=7_000 + position,
                shares=7_000 + position,
                holding_type="NOTE",
                share_amount_type="PRN",
            )
            for position in range(10)
        )

        self.assertEqual(
            [],
            quarter_health.structural_quarter_health_issues(quarter(rows)),
        )


class PeerPriceQuarterHealthTests(unittest.TestCase):
    def test_compiler_can_release_raw_corpus_index(self) -> None:
        target = quarter(clean_holdings())
        raw_index = peer_index_for(target)

        compiled = quarter_health.compile_peer_price_index(
            raw_index,
            consume=True,
        )

        self.assertEqual({}, raw_index)
        self.assertTrue(compiled)

    def test_rejects_lm_style_value_share_swap(self) -> None:
        swapped_rows = [
            holding(
                position,
                value=100_000 + position,
                shares=(100 + position) * (100_000 + position),
            )
            for position in range(12)
        ]
        target = quarter(swapped_rows)
        index = peer_index_for(target)
        references = quarter_health.same_date_peer_price_references(
            quarter_health.compile_peer_price_index(index),
            filer_id="target",
            quarter=target,
        )

        self.assertTrue(all(count == 4 for _price, count in references.values()))
        issue = quarter_health.peer_price_quarter_health_issue(
            target,
            references,
        )

        self.assertIsNotNone(issue)
        assert issue is not None
        self.assertEqual("peer_price_distortion", issue.code)
        self.assertIn("aligned=0/12", issue.detail)
        self.assertIn("coverage=12/12", issue.detail)

    def test_allows_clean_quarter_with_broad_peer_coverage(self) -> None:
        target = quarter(clean_holdings())
        index = peer_index_for(target)
        references = quarter_health.same_date_peer_price_references(
            quarter_health.compile_peer_price_index(index),
            filer_id="target",
            quarter=target,
        )

        self.assertEqual(12, len(references))
        self.assertIsNone(
            quarter_health.peer_price_quarter_health_issue(
                target,
                references,
            )
        )

    def test_low_evidence_does_not_create_false_positive(self) -> None:
        swapped = quarter([
            holding(
                position,
                value=100_000 + position,
                shares=(100 + position) * (100_000 + position),
            )
            for position in range(12)
        ])
        only_three_peers = peer_index_for(swapped, peer_count=3)
        references = quarter_health.same_date_peer_price_references(
            quarter_health.compile_peer_price_index(only_three_peers),
            filer_id="target",
            quarter=swapped,
        )
        self.assertEqual({}, references)
        self.assertIsNone(
            quarter_health.peer_price_quarter_health_issue(
                swapped,
                references,
            )
        )

        nine_references = {
            (row["cusip"], "EQUITY"): (100.0, 4)
            for row in swapped["holdings"][:9]
        }
        self.assertIsNone(
            quarter_health.peer_price_quarter_health_issue(
                swapped,
                nine_references,
            )
        )


if __name__ == "__main__":
    unittest.main()
