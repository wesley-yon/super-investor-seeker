from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from security_master_migration import (
    POSITION_DIGEST_ALGORITHM,
    SecurityMasterMigrationError,
    _position_projection,
    build_cutover_difference_report,
    capture_cutover_projection,
    write_cutover_difference_report,
)


class SecurityMasterMigrationReportTests(unittest.TestCase):
    @staticmethod
    def write_fund(root: Path, *, second_value: int = 50) -> None:
        funds = root / "funds"
        funds.mkdir(exist_ok=True)
        (funds / "1.json").write_text(
            json.dumps(
                {
                    "cik": 1,
                    "quarters": [
                        {
                            "report_date": "2026-06-30",
                            "holdings": [
                                {
                                    "cusip": "037833100",
                                    "class": "COM",
                                    "ticker": "OLD-AAPL",
                                    "issuer": "Old display name",
                                    "holding_type": "EQUITY",
                                    "value": 100,
                                    "shares": 10,
                                },
                                {
                                    "cusip": "76954AAD5",
                                    "class": "NOTE 3.625% 10/15/30",
                                    "ticker": "RIVN",
                                    "issuer": "Old display name",
                                    "holding_type": "NOTE",
                                    "value": second_value,
                                    "shares": 999,
                                    "shares_imputed": True,
                                },
                            ],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

    def test_parallel_projection_matches_serial_and_propagates_invalid_fund(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.write_fund(root)
            funds = root / "funds"
            original = json.loads((funds / "1.json").read_text())
            for cik in range(2, 10):
                original["cik"] = cik
                original["quarters"][0]["holdings"][0]["value"] = cik / 8
                (funds / f"{cik}.json").write_text(json.dumps(original))
            self.assertEqual(
                _position_projection(funds, workers=1),
                _position_projection(funds, workers=2),
            )
            (funds / "4.json").write_text('{broken')
            with self.assertRaisesRegex(SecurityMasterMigrationError, "4.json"):
                _position_projection(funds, workers=2)

    def test_report_preserves_positions_and_exposes_mapping_differences(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.write_fund(root)
            before = capture_cutover_projection(
                root / "funds",
                {
                    "037833100": {"ticker": "AAPL"},
                    "76954AAD5": {"ticker": "RIVN"},
                },
            )

            # Display-only rewrites and an imputed share value must not trip
            # the position-preservation gate.
            fund_path = root / "funds" / "1.json"
            fund = json.loads(fund_path.read_text(encoding="utf-8"))
            for holding in fund["quarters"][0]["holdings"]:
                holding["reported_cusip"] = holding["cusip"]
                holding["reported_class"] = holding["class"]
            fund["quarters"][0]["holdings"][0]["ticker"] = "AAPL"
            fund["quarters"][0]["holdings"][0]["issuer"] = "Apple Inc."
            fund["quarters"][0]["holdings"][0]["class"] = "COMMON STOCK"
            fund["quarters"][0]["holdings"][0]["cusip"] = "display-AAPL"
            fund["quarters"][0]["holdings"][1]["ticker"] = None
            fund["quarters"][0]["holdings"][1]["shares"] = 12345
            fund_path.write_text(json.dumps(fund), encoding="utf-8")
            after = capture_cutover_projection(
                root / "funds",
                {
                    "037833100": {
                        "ticker": "AAPL",
                        "mapping_status": "resolved",
                        "ticker_source": "sec_ftd",
                        "ticker_as_of": "2026-08-14",
                    },
                    "76954AAD5": {
                        "ticker": None,
                        "mapping_status": "no_listed_symbol",
                        "ticker_source": None,
                        "ticker_as_of": None,
                    },
                },
            )

            report = build_cutover_difference_report(
                before,
                after,
                generated_at="2026-09-01T00:00:00Z",
            )

            self.assertTrue(report["corpus_invariants_ok"])
            self.assertEqual(1, report["mapping_summary"]["differences"])
            self.assertEqual(
                "now_tickerless",
                report["mapping_differences"][0]["outcome"],
            )
            self.assertEqual("76954AAD5", report["mapping_differences"][0]["cusip"])

    def test_position_change_fails_shadow_invariants(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.write_fund(root)
            before = capture_cutover_projection(root / "funds", {})
            self.write_fund(root, second_value=51)
            after = capture_cutover_projection(root / "funds", {})

            report = build_cutover_difference_report(
                before,
                after,
                generated_at=None,
            )

            self.assertFalse(report["corpus_invariants_ok"])
            self.assertFalse(report["corpus_invariants"]["total_value"]["ok"])
            self.assertFalse(
                report["corpus_invariants"]["position_sha256"]["ok"]
            )

    def test_lossless_reported_identity_row_split_preserves_invariants(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.write_fund(root)
            before = capture_cutover_projection(root / "funds", {})

            fund_path = root / "funds" / "1.json"
            fund = json.loads(fund_path.read_text(encoding="utf-8"))
            original = fund["quarters"][0]["holdings"][0]
            first = {
                **original,
                "reported_issuer": "APPLE INC",
                "reported_class": "COM",
                "reported_cusip": original["cusip"],
                "share_amount_type": "SH",
                "value": 40,
                "shares": 4,
            }
            second = {
                **original,
                "reported_issuer": "APPLE COMPUTER INC",
                "reported_class": "COMMON STOCK",
                "reported_cusip": original["cusip"],
                "share_amount_type": "SH",
                "value": 60,
                "shares": 6,
            }
            fund["quarters"][0]["holdings"][:1] = [first, second]
            fund_path.write_text(json.dumps(fund), encoding="utf-8")
            after = capture_cutover_projection(root / "funds", {})

            report = build_cutover_difference_report(
                before,
                after,
                generated_at=None,
            )

            self.assertTrue(report["corpus_invariants_ok"])
            self.assertEqual(
                before["corpus"]["holding_count"],
                after["corpus"]["holding_count"],
            )
            self.assertEqual(
                before["corpus"]["position_sha256"],
                after["corpus"]["position_sha256"],
            )
            self.assertEqual(
                before["corpus"]["source_holding_count"] + 1,
                after["corpus"]["source_holding_count"],
            )

    def test_economic_identity_and_amount_changes_fail_invariants(self) -> None:
        mutations = {
            "cusip": lambda holding: holding.update(cusip="594918104"),
            "option side": lambda holding: holding.update(put_call="CALL"),
            "instrument type": lambda holding: holding.update(
                holding_type="NOTE"
            ),
            "value": lambda holding: holding.update(value=101),
            "shares": lambda holding: holding.update(shares=11),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                self.write_fund(root)
                before = capture_cutover_projection(root / "funds", {})

                fund_path = root / "funds" / "1.json"
                fund = json.loads(fund_path.read_text(encoding="utf-8"))
                mutate(fund["quarters"][0]["holdings"][0])
                fund_path.write_text(json.dumps(fund), encoding="utf-8")
                after = capture_cutover_projection(root / "funds", {})

                report = build_cutover_difference_report(
                    before,
                    after,
                    generated_at=None,
                )

                self.assertFalse(report["corpus_invariants_ok"])
                self.assertFalse(
                    report["corpus_invariants"]["position_sha256"]["ok"]
                )

    def test_position_digest_is_order_independent_and_preserves_duplicate_value(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.write_fund(root)
            before = capture_cutover_projection(root / "funds", {})

            fund_path = root / "funds" / "1.json"
            fund = json.loads(fund_path.read_text(encoding="utf-8"))
            fund["quarters"][0]["holdings"].reverse()
            fund_path.write_text(json.dumps(fund), encoding="utf-8")
            reordered = capture_cutover_projection(root / "funds", {})

            self.assertEqual(
                POSITION_DIGEST_ALGORITHM,
                before["corpus"]["position_digest_algorithm"],
            )
            self.assertEqual(
                before["corpus"]["position_sha256"],
                reordered["corpus"]["position_sha256"],
            )

            duplicate = dict(fund["quarters"][0]["holdings"][0])
            fund["quarters"][0]["holdings"].append(duplicate)
            fund_path.write_text(json.dumps(fund), encoding="utf-8")
            with_duplicate = capture_cutover_projection(root / "funds", {})

            self.assertNotEqual(
                before["corpus"]["position_sha256"],
                with_duplicate["corpus"]["position_sha256"],
            )
            self.assertEqual(
                before["corpus"]["holding_count"],
                with_duplicate["corpus"]["holding_count"],
            )
            self.assertEqual(
                before["corpus"]["source_holding_count"] + 1,
                with_duplicate["corpus"]["source_holding_count"],
            )

    def test_private_report_write_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.write_fund(root)
            projection = capture_cutover_projection(root / "funds", {})
            report = build_cutover_difference_report(
                projection,
                projection,
                generated_at="2026-09-01T00:00:00Z",
            )
            destination = root / ".cache" / "migration.json"

            write_cutover_difference_report(report, destination)
            first = destination.read_bytes()
            write_cutover_difference_report(report, destination)

            self.assertEqual(first, destination.read_bytes())
            self.assertEqual(report, json.loads(first))


if __name__ == "__main__":
    unittest.main()
