"""Frozen, network-free SEC filing fixtures with independent expectations.

The XML files deliberately retain SEC namespace and element shapes while
remaining small enough to audit by eye.  Expected parser rows, cover totals,
normalization decisions, and composition outcomes live in a separate JSON
manifest and are never generated from the implementation under test.
"""

from __future__ import annotations

import copy
import hashlib
import json
import unittest
from pathlib import Path

import pipeline
import security_identity
import value_units
from scripts import repair_value_units


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "sec_filing_oracle"
ORACLE = json.loads((FIXTURE_ROOT / "expectations.json").read_text())


def fixture_bytes(filename: str) -> bytes:
    return (FIXTURE_ROOT / filename).read_bytes()


def parsed_filing(case_name: str) -> tuple[dict, list[dict]]:
    case = ORACLE["filings"][case_name]
    metadata = pipeline.parse_primary_document(
        fixture_bytes(case["primary"]),
        form_type=case["form_type"],
    )
    holdings = pipeline.parse_information_table(
        fixture_bytes(case["information_table"])
    )
    if holdings is None:
        raise AssertionError(f"{case_name} did not parse as an information table")
    return metadata, holdings


def immutable_component(case_name: str) -> dict:
    case = ORACLE["filings"][case_name]
    metadata, holdings = parsed_filing(case_name)
    raw_total = sum(row["value"] for row in holdings)
    source = (
        fixture_bytes(case["primary"])
        + b"\0"
        + fixture_bytes(case["information_table"])
    )
    return {
        "cik": 123456,
        "report_date": metadata["report_date"],
        "filing_date": case["accepted_at"][:10],
        "accepted_at": case["accepted_at"],
        "accession": case["accession"],
        "form_type": case["form_type"],
        "amendment_number": metadata["amendment_number"],
        "amendment_kind": metadata["amendment_kind"],
        "reported_entry_total": metadata["reported_entry_total"],
        "reported_value_total": metadata["reported_value_total"],
        "normalized_value_total": raw_total,
        "value_unit_policy_version": value_units.VALUE_UNIT_POLICY_VERSION,
        "value_multiplier": 1,
        "value_unit_method": "frozen_sec_fixture_oracle",
        "value_unit_confidence": "high",
        "value_unit_evidence": {},
        "security_identity_version": pipeline.SECURITY_IDENTITY_VERSION,
        "source_hash": hashlib.sha256(source).hexdigest(),
        "holdings": holdings,
    }


class FrozenSecFixtureOracleTests(unittest.TestCase):
    def test_primary_and_information_tables_match_declared_expectations(
        self,
    ) -> None:
        for case_name, case in ORACLE["filings"].items():
            with self.subTest(case=case_name):
                metadata, holdings = parsed_filing(case_name)
                information_xml = fixture_bytes(case["information_table"])

                self.assertEqual(case["expected_metadata"], metadata)
                self.assertEqual(case["expected_rows"], holdings)
                self.assertEqual(
                    (
                        metadata["reported_entry_total"],
                        metadata["reported_value_total"],
                    ),
                    pipeline._information_table_totals(information_xml),
                )
                self.assertEqual(
                    case["expected_raw_total"],
                    sum(row["value"] for row in holdings),
                )

    def test_dollar_and_thousands_cases_normalize_to_expected_totals(
        self,
    ) -> None:
        for case_name, case in ORACLE["filings"].items():
            with self.subTest(case=case_name):
                _, raw_holdings = parsed_filing(case_name)
                holdings = copy.deepcopy(raw_holdings)
                peer_prices = {
                    cusip: (price, 4, 0)
                    for cusip, price in case.get("peer_prices", {}).items()
                }

                decision = value_units.normalize_value_units(
                    holdings,
                    peer_prices=peer_prices,
                )

                self.assertEqual(
                    case["expected_multiplier"],
                    decision["value_multiplier"],
                )
                self.assertEqual(
                    case["expected_unit_method"],
                    decision["value_unit_method"],
                )
                self.assertEqual(
                    case["expected_normalized_values"],
                    [row["value"] for row in holdings],
                )
                self.assertEqual(
                    case["expected_normalized_total"],
                    sum(row["value"] for row in holdings),
                )

    def test_sec_option_side_and_principal_type_define_distinct_identities(
        self,
    ) -> None:
        case = ORACLE["filings"]["post_2023_dollars"]
        _, holdings = parsed_filing("post_2023_dollars")
        identities = [
            security_identity.stock_lookup_id(
                holding["cusip"],
                security_identity.holding_instrument_type(holding),
            )
            for holding in holdings
        ]

        self.assertEqual(case["expected_security_ids"], identities)
        self.assertEqual(
            ["EQUITY", "CALL", "PUT", "NOTE"],
            [holding["holding_type"] for holding in holdings],
        )
        self.assertEqual("PRN", holdings[-1]["share_amount_type"])

    def test_new_holdings_amendment_appends_disjoint_rows_once(self) -> None:
        amendment = ORACLE["filings"]["new_holdings"]
        composed = pipeline.compose_quarter_filings([
            immutable_component(amendment["base_case"]),
            immutable_component("new_holdings"),
        ])

        self.assertEqual(
            amendment["expected_composed_total"],
            composed["total_value"],
        )
        self.assertEqual(
            amendment["expected_composed_holdings"],
            composed["num_holdings"],
        )
        self.assertEqual(
            amendment["expected_applied_accessions"],
            composed["applied_accessions"],
        )
        self.assertEqual(
            amendment["expected_composition_actions"],
            [
                source["composition_action"]
                for source in composed["source_filings"]
            ],
        )

    def test_mixed_unit_temporal_shape_is_detected_and_fails_closed(
        self,
    ) -> None:
        anomaly = ORACLE["temporal_anomaly"]
        current = pipeline.parse_information_table(
            fixture_bytes(anomaly["current_information_table"])
        )
        adjacent = pipeline.parse_information_table(
            fixture_bytes(anomaly["adjacent_information_table"])
        )
        self.assertIsNotNone(current)
        self.assertIsNotNone(adjacent)

        current_rows = [
            [row["cusip"], row["value"], row["shares"]]
            for row in current
        ]
        adjacent_rows = [
            [row["cusip"], row["value"], row["shares"]]
            for row in adjacent
        ]
        self.assertEqual(anomaly["expected_current_rows"], current_rows)
        self.assertEqual(anomaly["expected_adjacent_rows"], adjacent_rows)
        self.assertEqual(
            anomaly["expected_current_total"],
            sum(row["value"] for row in current),
        )
        self.assertEqual(
            anomaly["expected_adjacent_total"],
            sum(row["value"] for row in adjacent),
        )

        evidence = value_units.adjacent_quarter_scale_evidence(
            current,
            adjacent,
        )
        for key, expected in anomaly["expected_evidence"].items():
            with self.subTest(evidence=key):
                self.assertEqual(expected, evidence[key])

        with self.assertRaisesRegex(
            value_units.AmbiguousValueUnits,
            "mixed value-unit clusters|different unit scales",
        ):
            value_units.normalize_value_units(
                copy.deepcopy(current),
                prior_multiplier=1,
                adjacent_holdings=adjacent,
            )

    def test_real_ccla_snapshot_matches_and_exercises_explicit_repair(
        self,
    ) -> None:
        snapshot = json.loads(
            (
                FIXTURE_ROOT
                / "ccla_0001631562_25_000005_rows.json"
            ).read_text()
        )
        source = snapshot["source"]
        oracle = snapshot["oracle"]
        rows = snapshot["rows"]
        holdings = [
            {"cusip": cusip, "shares": shares, "value": raw_value}
            for cusip, shares, raw_value in rows
        ]
        spec = repair_value_units.EXPLICIT_HISTORICAL_REPAIRS[
            (source["cik"], source["report_date"])
        ]

        self.assertEqual("sec_derived_row_snapshot", snapshot["fixture_kind"])
        self.assertFalse(snapshot["is_byte_for_byte_source_document"])
        self.assertEqual(53, len(rows))
        self.assertEqual(source["reported_entry_total"], len(rows))
        self.assertEqual(
            source["reported_raw_value_total"],
            sum(holding["value"] for holding in holdings),
        )
        self.assertEqual(source["accession"], spec["accession"])
        self.assertEqual(source["reported_entry_total"], spec["holding_count"])
        self.assertEqual(source["reported_raw_value_total"], spec["bad_total"])
        self.assertEqual(
            spec["bad_signature"],
            repair_value_units.holding_value_signature(holdings),
        )
        with self.assertRaisesRegex(
            value_units.AmbiguousValueUnits,
            "position count and raw-value evidence support "
            "different unit scales",
        ):
            value_units.classify_value_units(copy.deepcopy(holdings))

        before = {
            holding["cusip"]: holding["value"]
            for holding in holdings
        }
        quarter = {
            "total_value": source["reported_raw_value_total"],
            "holdings": holdings,
        }
        self.assertTrue(
            repair_value_units.repair_explicit_historical_quarter(
                quarter,
                spec,
            )
        )

        unscaled = set(oracle["unscaled_cusips"])
        self.assertEqual({"002824100"}, unscaled)
        self.assertEqual(118_808_336, before["002824100"])
        scaled_rows = 0
        for holding in quarter["holdings"]:
            cusip = holding["cusip"]
            if cusip in unscaled:
                self.assertEqual(before[cusip], holding["value"])
            else:
                scaled_rows += 1
                self.assertEqual(
                    before[cusip] * oracle["other_row_multiplier"],
                    holding["value"],
                )
        self.assertEqual(52, scaled_rows)
        self.assertEqual(
            oracle["corrected_value_total"],
            quarter["total_value"],
        )
        self.assertEqual(spec["correct_total"], quarter["total_value"])
        self.assertEqual(
            spec["correct_signature"],
            repair_value_units.holding_value_signature(
                quarter["holdings"]
            ),
        )


if __name__ == "__main__":
    unittest.main()
