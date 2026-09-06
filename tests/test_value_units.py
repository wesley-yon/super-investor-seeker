from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pipeline
import value_units


FIXTURE_CORPUS_DIR = Path(__file__).resolve().parent / "fixtures/corpus"


def equity(
    cusip: str,
    value: int,
    shares: int,
    *,
    title: str = "COM",
    amount_type: str = "SH",
) -> dict:
    return {
        "ticker": None,
        "issuer": f"ISSUER {cusip}",
        "cusip": cusip,
        "class": title,
        "value": value,
        "shares": shares,
        "holding_type": "EQUITY",
        "share_amount_type": amount_type,
    }


def principal(cusip: str, value: int, amount: int) -> dict:
    holding = equity(
        cusip,
        value,
        amount,
        title="NOTE",
        amount_type="PRN",
    )
    holding["holding_type"] = "NOTE"
    return holding


def component(
    accession: str,
    kind: str,
    accepted_at: str,
    holdings: list[dict],
    unit_metadata: dict,
    *,
    amendment_number: int | None = None,
) -> dict:
    multiplier = unit_metadata["value_multiplier"]
    normalized_total = sum(row["value"] for row in holdings)
    return {
        "cik": 123456,
        "report_date": "2025-12-31",
        "filing_date": accepted_at[:10],
        "accepted_at": accepted_at,
        "accession": accession,
        "form_type": "13F-HR" if kind == "ORIGINAL" else "13F-HR/A",
        "amendment_number": amendment_number,
        "amendment_kind": kind,
        "reported_entry_total": len(holdings),
        "reported_value_total": normalized_total // multiplier,
        "normalized_value_total": normalized_total,
        **unit_metadata,
        "source_hash": hashlib.sha256(accession.encode()).hexdigest(),
        "holdings": holdings,
    }


class ValueUnitClassifierTests(unittest.TestCase):
    def test_policy_version_is_two(self) -> None:
        self.assertEqual(2, value_units.VALUE_UNIT_POLICY_VERSION)

    def test_policy_free_peer_evidence_helpers_are_shared(self) -> None:
        holding = equity("037833100", 150, 10)

        self.assertEqual(
            ("037833100", 150.0, 10.0),
            value_units._unit_evidence_row(holding),
        )
        self.assertEqual("aligned_1x", value_units._scale_ratio_cluster(1.0))
        self.assertEqual(
            "inflated_1000x",
            value_units._scale_ratio_cluster(1000.0),
        )
        self.assertEqual(
            "understated_1000x",
            value_units._scale_ratio_cluster(0.001),
        )
        self.assertIsNone(value_units._scale_ratio_cluster(25.0))

    def test_broad_value_weighted_evidence_scales_entire_component(self) -> None:
        holdings = [
            equity(f"10000000{i}", 100 + i, 1000 + i)
            for i in range(5)
        ]
        holdings.append({
            "ticker": None,
            "issuer": "EXCLUDED NOTE",
            "cusip": "200000001",
            "class": "NOTE 1.5%",
            "value": 7,
            "shares": 10,
            "holding_type": "NOTE",
            "share_amount_type": "PRN",
        })

        decision = value_units.normalize_value_units(
            holdings,
            prior_multiplier=1000,
        )

        self.assertEqual(1000, decision["value_multiplier"])
        self.assertEqual(
            "prior_unit_convention",
            decision["value_unit_method"],
        )
        self.assertEqual("low", decision["value_unit_confidence"])
        self.assertIsNone(
            pipeline._trusted_value_unit_multiplier(decision)
        )
        self.assertEqual(7_000, holdings[-1]["value"])

    def test_material_dollar_positions_outweigh_many_tiny_warrants(self) -> None:
        holdings = [
            equity("78462F103", 70_702_363, 108_716),
            equity("922908363", 45_526_139, 76_188),
            equity("464287465", 17_797_130, 183_230),
        ]
        holdings.extend(
            equity(
                f"30000000{i}",
                20_000,
                100_000,
                title="*W EXP 01/01/2030",
            )
            for i in range(10)
        )

        decision = value_units.normalize_value_units(holdings)

        self.assertEqual(1, decision["value_multiplier"])
        self.assertEqual("weighted_equity_dollars", decision["value_unit_method"])
        self.assertEqual(70_702_363, holdings[0]["value"])

    def test_principal_rows_do_not_vote_on_share_price(self) -> None:
        holdings = [equity("111111111", 10_000, 100)]
        holdings.extend(
            equity(
                f"40000000{i}",
                100_000,
                200_000,
                title="CONVERTIBLE NOTE",
                amount_type="PRN",
            )
            for i in range(10)
        )

        decision = value_units.classify_value_units(holdings)

        self.assertEqual(1, decision["value_multiplier"])
        self.assertEqual(1, decision["value_unit_evidence"]["eligible_positions"])

    def test_imputed_shares_do_not_vote_on_value_units(self) -> None:
        holding = equity("111111111", 100, 100)
        holding["shares_imputed"] = True

        self.assertFalse(value_units.is_unit_evidence_holding(holding))

    def test_concentrated_penny_stock_without_confirmation_is_ambiguous(
        self,
    ) -> None:
        holdings = [equity("00857U107", 4_333_573, 6_292_396)]

        with self.assertRaises(value_units.AmbiguousValueUnits):
            value_units.classify_value_units(holdings)

    def test_concentrated_penny_stock_reuses_trusted_dollar_convention(
        self,
    ) -> None:
        holdings = [equity("00857U107", 4_333_573, 6_292_396)]

        decision = value_units.classify_value_units(
            holdings,
            prior_multiplier=1,
        )

        self.assertEqual(1, decision["value_multiplier"])
        self.assertEqual(
            "prior_unit_convention",
            decision["value_unit_method"],
        )

    def test_broad_sub_dollar_portfolio_without_confirmation_is_ambiguous(
        self,
    ) -> None:
        holdings = [
            equity(f"00999999{i}", 100, 1000)
            for i in range(5)
        ]

        with self.assertRaises(value_units.AmbiguousValueUnits):
            value_units.classify_value_units(holdings)

    def test_peer_consensus_vetoes_penny_stock_inflation(self) -> None:
        holdings = [equity("00857U107", 4_333_573, 6_292_396)]
        peers = {"00857U107": (0.68870017, 200)}

        decision = value_units.classify_value_units(holdings, peers)

        self.assertEqual(1, decision["value_multiplier"])
        self.assertEqual("same_quarter_peer_dollars", decision["value_unit_method"])

    def test_peer_consensus_recovers_concentrated_thousands_filing(self) -> None:
        holdings = [
            equity("084670108", 2_873, 4),
            equity("46434V464", 2_228, 10_000),
        ]
        peers = {
            "084670108": (718_250.0, 50),
            "46434V464": (222.8, 100),
        }

        decision = value_units.normalize_value_units(holdings, peers)

        self.assertEqual(1000, decision["value_multiplier"])
        self.assertEqual(
            "same_quarter_peer_thousands",
            decision["value_unit_method"],
        )
        self.assertEqual(2_873_000, holdings[0]["value"])
        self.assertEqual(2_228_000, holdings[1]["value"])

    def test_peer_count_value_scale_conflict_fails_closed(self) -> None:
        for distance_days in (0, 90):
            with self.subTest(distance_days=distance_days):
                holdings = [
                    equity(f"08467010{i}", 1_000, 1_000)
                    for i in range(10)
                ]
                holdings.append(equity("084670199", 200_000, 1_000))
                peers = {
                    holding["cusip"]: (
                        1_000.0 if index < 10 else 200.0,
                        50,
                        distance_days,
                    )
                    for index, holding in enumerate(holdings)
                }

                with self.assertRaisesRegex(
                    value_units.AmbiguousValueUnits,
                    "count and raw-value evidence support different unit scales",
                ):
                    value_units.classify_value_units(holdings, peers)

    def test_sparse_peer_cannot_mask_intrinsic_mixed_unit_shape(
        self,
    ) -> None:
        holdings = [
            equity(f"08467200{index}", 100, 1_000)
            for index in range(10)
        ]
        holdings.append(equity("084672099", 1_000_000, 10_000))

        for peers in (
            {},
            {"084672099": (100.0, 50)},
        ):
            with self.subTest(peer_cusips=sorted(peers)):
                with self.assertRaisesRegex(
                    value_units.AmbiguousValueUnits,
                    "position count and raw-value evidence support "
                    "different unit scales",
                ):
                    value_units.classify_value_units(holdings, peers)

    def test_broad_exact_peers_can_prove_low_price_rows_are_dollars(
        self,
    ) -> None:
        holdings = [
            equity(f"08467300{index}", 100, 1_000)
            for index in range(10)
        ]
        holdings.append(equity("084673099", 1_000_000, 10_000))
        peers = {
            holding["cusip"]: (0.1, 50)
            for holding in holdings[:10]
        }

        decision = value_units.classify_value_units(holdings, peers)

        self.assertEqual(1, decision["value_multiplier"])
        self.assertEqual(
            "same_quarter_peer_dollars",
            decision["value_unit_method"],
        )

    def test_peer_validator_detects_one_large_aligned_row_masking_bad_rows(
        self,
    ) -> None:
        holdings = [
            equity(f"08467010{i}", 1_000, 1_000)
            for i in range(10)
        ]
        holdings.append(equity("084670199", 200_000, 1_000))
        peers = {
            holding["cusip"]: (
                1_000.0 if index < 10 else 200.0,
                50,
            )
            for index, holding in enumerate(holdings)
        }

        evidence = value_units.peer_scale_evidence(holdings, peers)

        self.assertEqual("mixed_scale_clusters", evidence["status"])
        self.assertGreater(evidence["understated_count_support"], 0.80)
        self.assertGreater(evidence["aligned_value_support"], 0.80)

    def test_peer_validator_detects_sparse_peer_mixed_unit_shape(
        self,
    ) -> None:
        holdings = [
            equity(f"08467400{index}", 100, 1_000)
            for index in range(10)
        ]
        holdings.append(equity("084674099", 1_000_000, 10_000))
        peers = {"084674099": (100.0, 50)}

        evidence = value_units.peer_scale_evidence(holdings, peers)

        self.assertEqual("mixed_scale_clusters", evidence["status"])
        self.assertTrue(evidence["intrinsic_count_value_conflict"])
        self.assertAlmostEqual(
            1 / 11,
            evidence["matched_count_coverage"],
            places=6,
        )
        self.assertGreater(evidence["low_price_count_support"], 0.80)
        self.assertLess(evidence["low_price_value_support"], 0.01)

    def test_peer_value_dominance_without_count_support_is_inconclusive(
        self,
    ) -> None:
        holdings = [
            equity(
                f"08467100{index}",
                100_000 if index < 3 else 1_000,
                1_000,
            )
            for index in range(10)
        ]
        peers = {
            holding["cusip"]: (
                100_000.0 if index < 3 else 0.1,
                50,
            )
            for index, holding in enumerate(holdings)
        }

        evidence = value_units.peer_scale_evidence(holdings, peers)

        self.assertIsNone(evidence["status"])
        self.assertGreater(evidence["understated_value_support"], 0.90)
        self.assertEqual(0.3, evidence["understated_count_support"])
        with self.assertRaises(value_units.AmbiguousValueUnits):
            value_units.classify_value_units(holdings, peers)

    def test_one_stale_peer_position_cannot_choose_units(self) -> None:
        holdings = [equity("084670108", 2_873, 4)]
        peers = {"084670108": (718_250.0, 50, 90)}

        with self.assertRaises(value_units.AmbiguousValueUnits):
            value_units.classify_value_units(holdings, peers)

    def test_duplicate_rows_do_not_satisfy_nearby_peer_breadth(self) -> None:
        holdings = [
            equity("084670108", 2_873, 4)
            for _ in range(3)
        ]
        peers = {"084670108": (718_250.0, 50, 90)}

        with self.assertRaises(value_units.AmbiguousValueUnits):
            value_units.classify_value_units(holdings, peers)

    def test_concentrated_thousands_filing_reuses_trusted_convention(
        self,
    ) -> None:
        holdings = [equity("084670108", 2_873, 4)]

        decision = value_units.normalize_value_units(
            holdings,
            prior_multiplier=1000,
        )

        self.assertEqual(1000, decision["value_multiplier"])
        self.assertEqual(2_873_000, holdings[0]["value"])

    def test_split_weighted_evidence_fails_closed(self) -> None:
        holdings = [
            equity(f"50000000{i}", 12, 120)
            for i in range(5)
        ]
        holdings.append(equity("599999999", 40, 1))

        with self.assertRaises(value_units.AmbiguousValueUnits):
            value_units.classify_value_units(holdings)

    def test_empty_component_is_deterministic(self) -> None:
        decision = value_units.normalize_value_units([])

        self.assertEqual(1, decision["value_multiplier"])
        self.assertEqual(
            "zero_value_component",
            decision["value_unit_method"],
        )

    def test_positive_non_equity_component_requires_a_trusted_prior(
        self,
    ) -> None:
        holdings = [{
            "issuer": "EXAMPLE NOTE",
            "cusip": "111111111",
            "class": "NOTE",
            "value": 100,
            "shares": 100,
            "holding_type": "NOTE",
            "share_amount_type": "PRN",
        }]

        with self.assertRaises(value_units.AmbiguousValueUnits):
            value_units.classify_value_units(holdings)

        decision = value_units.classify_value_units(
            holdings,
            prior_multiplier=1,
        )
        self.assertEqual(1, decision["value_multiplier"])

    def test_adjacent_principal_rows_veto_wrong_prior_multiplier(self) -> None:
        holdings = [
            principal(f"60000000{i}", 1_000_000 + i, 10_000 + i)
            for i in range(10)
        ]
        adjacent = copy.deepcopy(holdings)

        with self.assertRaisesRegex(
            value_units.AmbiguousValueUnits,
            "contradicts proposed multiplier 1000",
        ):
            value_units.normalize_value_units(
                holdings,
                prior_multiplier=1000,
                adjacent_holdings=adjacent,
            )

        self.assertEqual(1_000_000, holdings[0]["value"])

    def test_adjacent_uniform_thousands_evidence_accepts_scaling(self) -> None:
        holdings = [
            principal(f"61000000{i}", 1_000 + i, 10_000 + i)
            for i in range(10)
        ]
        adjacent = [
            principal(
                holding["cusip"],
                holding["value"] * 1000,
                holding["shares"],
            )
            for holding in holdings
        ]

        decision = value_units.normalize_value_units(
            holdings,
            prior_multiplier=1000,
            adjacent_holdings=adjacent,
        )

        self.assertEqual(1_000_000, holdings[0]["value"])
        self.assertEqual(
            "understated_1000x",
            decision["value_unit_evidence"]["adjacent_quarter"]["status"],
        )
        self.assertEqual(
            "adjacent_quarter_thousands",
            decision["value_unit_method"],
        )
        self.assertEqual("high", decision["value_unit_confidence"])

    def test_adjacent_uniform_thousands_evidence_vetoes_dollars(self) -> None:
        holdings = [
            principal(f"62000000{i}", 1_000 + i, 10_000 + i)
            for i in range(10)
        ]
        adjacent = [
            principal(
                holding["cusip"],
                holding["value"] * 1000,
                holding["shares"],
            )
            for holding in holdings
        ]

        with self.assertRaisesRegex(
            value_units.AmbiguousValueUnits,
            "contradicts proposed multiplier 1",
        ):
            value_units.normalize_value_units(
                holdings,
                prior_multiplier=1,
                adjacent_holdings=adjacent,
            )

    def test_adjacent_aligned_principal_rows_create_current_dollar_proof(
        self,
    ) -> None:
        holdings = [
            principal(f"62500000{i}", 1_000_000 + i, 10_000 + i)
            for i in range(10)
        ]

        decision = value_units.normalize_value_units(
            holdings,
            prior_multiplier=1,
            adjacent_holdings=copy.deepcopy(holdings),
        )

        self.assertEqual(
            "adjacent_quarter_dollars",
            decision["value_unit_method"],
        )
        self.assertEqual("high", decision["value_unit_confidence"])

    def test_adjacent_mixed_units_fail_closed_despite_count_majority(
        self,
    ) -> None:
        holdings = [
            principal(f"63000000{i}", 1_000, 10_000)
            for i in range(10)
        ]
        adjacent = [
            principal(holding["cusip"], 1_000_000, holding["shares"])
            for holding in holdings
        ]
        holdings.append(principal("639999999", 200_000, 10_000))
        adjacent.append(principal("639999999", 200_000, 10_000))

        evidence = value_units.adjacent_quarter_scale_evidence(
            holdings,
            adjacent,
        )

        self.assertEqual("mixed_scale_clusters", evidence["status"])
        self.assertGreater(evidence["understated_count_support"], 0.80)
        self.assertLess(evidence["understated_raw_value_support"], 0.05)
        with self.assertRaisesRegex(
            value_units.AmbiguousValueUnits,
            "mixed value-unit clusters",
        ):
            value_units.normalize_value_units(
                holdings,
                prior_multiplier=1000,
                adjacent_holdings=adjacent,
            )

    def test_adjacent_scale_evidence_requires_ten_shared_positions(
        self,
    ) -> None:
        holdings = [
            principal(f"64000000{i}", 1_000, 10_000)
            for i in range(9)
        ]
        adjacent = [
            principal(holding["cusip"], 1_000_000, holding["shares"])
            for holding in holdings
        ]

        evidence = value_units.adjacent_quarter_scale_evidence(
            holdings,
            adjacent,
        )

        self.assertIsNone(evidence["status"])
        self.assertEqual(9, evidence["matched_positions"])

    def test_adjacent_scale_evidence_accepts_eight_of_ten_boundary(
        self,
    ) -> None:
        adjacent = [
            principal(f"65000000{i}", 1_000, 1_000)
            for i in range(10)
        ]
        holdings = [
            {
                **holding,
                "value": holding["value"] * (1000 if index < 8 else 10),
            }
            for index, holding in enumerate(adjacent)
        ]

        evidence = value_units.adjacent_quarter_scale_evidence(
            holdings,
            adjacent,
        )

        self.assertEqual("inflated_1000x", evidence["status"])
        self.assertEqual(0.8, evidence["inflated_count_support"])
        self.assertGreater(evidence["inflated_raw_value_support"], 0.99)

    def test_adjacent_aligned_count_with_unclustered_value_is_inconclusive(
        self,
    ) -> None:
        adjacent = [
            principal(f"66000000{i}", 1_000, 1_000)
            for i in range(10)
        ]
        holdings = [
            {
                **holding,
                "value": holding["value"] * (1 if index < 8 else 10),
            }
            for index, holding in enumerate(adjacent)
        ]

        evidence = value_units.adjacent_quarter_scale_evidence(
            holdings,
            adjacent,
        )

        self.assertIsNone(evidence["status"])
        self.assertEqual(0.8, evidence["aligned_count_support"])
        self.assertLess(evidence["aligned_raw_value_support"], 0.30)

    def test_adjacent_aligned_count_vs_scaled_value_fails_closed(
        self,
    ) -> None:
        adjacent = [
            principal(f"67000000{i}", 1_000, 1_000)
            for i in range(10)
        ]
        holdings = [
            {
                **holding,
                "value": holding["value"] * (1 if index < 8 else 1000),
            }
            for index, holding in enumerate(adjacent)
        ]

        evidence = value_units.adjacent_quarter_scale_evidence(
            holdings,
            adjacent,
        )

        self.assertEqual("mixed_scale_clusters", evidence["status"])
        self.assertEqual(0.8, evidence["aligned_count_support"])
        self.assertGreater(evidence["inflated_raw_value_support"], 0.99)


class ValueUnitPipelineTests(unittest.TestCase):
    def test_parser_preserves_shares_or_principal_type(self) -> None:
        xml = b"""<informationTable>
          <infoTable>
            <nameOfIssuer>EXAMPLE NOTE</nameOfIssuer>
            <titleOfClass>NOTE</titleOfClass>
            <cusip>111111111</cusip>
            <value>100</value>
            <shrsOrPrnAmt>
              <sshPrnamt>1000</sshPrnamt>
              <sshPrnamtType>PRN</sshPrnamtType>
            </shrsOrPrnAmt>
          </infoTable>
        </informationTable>"""

        holdings = pipeline.parse_information_table(xml)

        self.assertIsNotNone(holdings)
        self.assertEqual("PRN", holdings[0]["share_amount_type"])

    def test_peer_loader_excludes_current_filer_and_uses_nearest_quarter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stocks_dir = Path(tmpdir)
            stock = {
                "instrument_type": "EQUITY",
                "holders": [
                    {
                        "cik": cik,
                        "history": [{
                            "date": "2025-09-30",
                            "value": value,
                            "shares": 10,
                        }],
                    }
                    for cik, value in (
                        (1, 1_000),
                        (2, 1_100),
                        (3, 900),
                        (999, 9_999_000),
                    )
                ],
            }
            (stocks_dir / "111111111.json").write_text(json.dumps(stock))
            holdings = [equity("111111111", 100, 1)]

            with mock.patch.object(pipeline, "STOCKS_DIR", stocks_dir):
                pipeline._PEER_PRICE_HISTORY_CACHE.clear()
                references = pipeline.load_peer_value_unit_prices(
                    999,
                    "2025-12-31",
                    holdings,
                )

        self.assertEqual((100.0, 3, 92), references["111111111"])

    def test_peer_loader_excludes_imputed_share_prices(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stocks_dir = Path(tmpdir)
            stock = {
                "instrument_type": "EQUITY",
                "holders": [
                    {
                        "cik": cik,
                        "history": [{
                            "date": "2025-12-31",
                            "value": 1_000,
                            "shares": 10,
                            **(
                                {"shares_imputed": True}
                                if cik == 3
                                else {}
                            ),
                        }],
                    }
                    for cik in (1, 2, 3)
                ],
            }
            (stocks_dir / "111111111.json").write_text(json.dumps(stock))
            holdings = [equity("111111111", 100, 1)]

            with mock.patch.object(pipeline, "STOCKS_DIR", stocks_dir):
                pipeline._PEER_PRICE_HISTORY_CACHE.clear()
                references = pipeline.load_peer_value_unit_prices(
                    999,
                    "2025-12-31",
                    holdings,
                )

        self.assertNotIn("111111111", references)

    def test_prior_loader_requires_complete_high_confidence_provenance(
        self,
    ) -> None:
        prior_holdings = [principal("111111111", 1_000_000, 900_000)]
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            fund = {
                "quarters": [{
                    "report_date": "2025-09-30",
                    "holdings": prior_holdings,
                    "total_value": 1_000_000,
                    "source_filings": [{
                        "accession": "0000000001-25-000001",
                        "applied": True,
                        "value_unit_policy_version": (
                            value_units.VALUE_UNIT_POLICY_VERSION
                        ),
                        "value_multiplier": 1000,
                        "value_unit_confidence": "high",
                    }],
                }],
            }
            (funds_dir / "999.json").write_text(json.dumps(fund))

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                multiplier, holdings = pipeline.load_prior_value_unit_context(
                    999,
                    "2025-12-31",
                )

        self.assertEqual(1000, multiplier)
        self.assertEqual(prior_holdings, holdings)

    def test_prior_loader_accepts_migrated_quarter_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            fund = {
                "quarters": [{
                    "report_date": "2025-09-30",
                    "holdings": [],
                    "total_value": 0,
                    "value_unit_policy_version": (
                        value_units.VALUE_UNIT_POLICY_VERSION
                    ),
                    "value_multiplier": 1,
                    "value_unit_confidence": "high",
                }],
            }
            (funds_dir / "999.json").write_text(json.dumps(fund))

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                multiplier, holdings = pipeline.load_prior_value_unit_context(
                    999,
                    "2025-12-31",
                )

        self.assertEqual(1, multiplier)
        self.assertEqual([], holdings)

    def test_prior_loader_does_not_skip_an_untrusted_adjacent_quarter(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            fund = {
                "quarters": [
                    {
                        "report_date": "2025-12-31",
                    },
                    {
                        "report_date": "2025-09-30",
                        "value_unit_policy_version": (
                            value_units.VALUE_UNIT_POLICY_VERSION
                        ),
                        "value_multiplier": 1000,
                        "value_unit_confidence": "high",
                    },
                ],
            }
            (funds_dir / "999.json").write_text(json.dumps(fund))

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                multiplier, holdings = pipeline.load_prior_value_unit_context(
                    999,
                    "2026-03-31",
                )

        self.assertIsNone(multiplier)
        self.assertIsNone(holdings)

    def test_prior_context_rejects_untrusted_or_inconsistent_inputs(
        self,
    ) -> None:
        trusted_source = {
            "accession": "0000000001-25-000001",
            "applied": True,
            "value_unit_policy_version": (
                value_units.VALUE_UNIT_POLICY_VERSION
            ),
            "value_multiplier": 1,
            "value_unit_confidence": "high",
        }
        base_quarter = {
            "report_date": "2025-09-30",
            "holdings": [equity("111111111", 1_000, 10)],
            "total_value": 1_000,
            "source_filings": [trusted_source],
        }
        cases = {
            "duplicate exact prior": {
                "quarters": [base_quarter, copy.deepcopy(base_quarter)],
            },
            "malformed source filings": {
                "quarters": [{
                    **base_quarter,
                    "source_filings": "invalid",
                    "value_unit_policy_version": (
                        value_units.VALUE_UNIT_POLICY_VERSION
                    ),
                    "value_multiplier": 1,
                    "value_unit_confidence": "high",
                }],
            },
            "non-object source": {
                "quarters": [{
                    **base_quarter,
                    "source_filings": [trusted_source, "invalid"],
                }],
            },
            "mixed applied multipliers": {
                "quarters": [{
                    **base_quarter,
                    "source_filings": [
                        trusted_source,
                        {
                            **trusted_source,
                            "accession": "0000000001-25-000002",
                            "value_multiplier": 1000,
                        },
                    ],
                }],
            },
            "holdings total mismatch": {
                "quarters": [{
                    **base_quarter,
                    "total_value": 999,
                }],
            },
        }

        for name, fund in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                funds_dir = Path(tmpdir)
                (funds_dir / "999.json").write_text(json.dumps(fund))
                with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                    self.assertEqual(
                        (None, None),
                        pipeline.load_prior_value_unit_context(
                            999,
                            "2025-12-31",
                        ),
                    )

        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            (funds_dir / "999.json").write_text(json.dumps({
                "quarters": [base_quarter],
            }))
            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                self.assertEqual(
                    (None, None),
                    pipeline.load_prior_value_unit_context(
                        999,
                        "2025-12-15",
                    ),
                )

    def test_components_keep_independent_unit_provenance(self) -> None:
        original_holdings = [equity("111111111", 100, 1)]
        original_units = value_units.normalize_value_units(original_holdings)
        supplement_holdings = [
            equity(f"20000000{i}", 10, 100)
            for i in range(5)
        ]
        supplement_units = value_units.normalize_value_units(
            supplement_holdings,
            prior_multiplier=1000,
        )
        original = component(
            "0000000001-26-000001",
            "ORIGINAL",
            "2026-02-01T10:00:00Z",
            original_holdings,
            original_units,
        )
        supplement = component(
            "0000000001-26-000002",
            "NEW_HOLDINGS",
            "2026-02-02T10:00:00Z",
            supplement_holdings,
            supplement_units,
            amendment_number=1,
        )

        quarter = pipeline.compose_quarter_filings([original, supplement])

        self.assertEqual(1, quarter["source_filings"][0]["value_multiplier"])
        self.assertEqual(1000, quarter["source_filings"][1]["value_multiplier"])
        self.assertEqual(
            50_100,
            quarter["total_value"],
        )
        self.assertEqual(
            quarter["total_value"],
            sum(
                source["normalized_value_total"]
                for source in quarter["source_filings"]
                if source["applied"]
            ),
        )

    def test_known_corpus_boundary_and_fixed_failures(self) -> None:
        funds_dir = FIXTURE_CORPUS_DIR / "funds"

        lakehouse = json.loads((funds_dir / "1844830.json").read_text())
        lakehouse_quarter = next(
            quarter
            for quarter in lakehouse["quarters"]
            if quarter["report_date"] == "2026-06-30"
        )
        raw_lakehouse = copy.deepcopy(lakehouse_quarter["holdings"])
        for holding in raw_lakehouse:
            holding["value"] //= 1000
        with mock.patch.object(
            pipeline,
            "STOCKS_DIR",
            FIXTURE_CORPUS_DIR / "stocks",
        ):
            pipeline._PEER_PRICE_HISTORY_CACHE.clear()
            lakehouse_peers = pipeline.load_peer_value_unit_prices(
                1844830,
                "2026-06-30",
                raw_lakehouse,
            )
            pipeline._PEER_PRICE_HISTORY_CACHE.clear()
        self.assertEqual(
            1000,
            value_units.classify_value_units(
                raw_lakehouse,
                lakehouse_peers,
            )["value_multiplier"],
        )

        lepercq = json.loads((funds_dir / "1854423.json").read_text())
        lepercq_quarter = next(
            quarter
            for quarter in lepercq["quarters"]
            if quarter["report_date"] == "2026-03-31"
        )
        self.assertEqual(137_977_850, lepercq_quarter["total_value"])
        self.assertEqual(
            1,
            value_units.classify_value_units(
                lepercq_quarter["holdings"]
            )["value_multiplier"],
        )

        rubicon = json.loads((funds_dir / "2032489.json").read_text())
        rubicon_quarter = next(
            quarter
            for quarter in rubicon["quarters"]
            if quarter["report_date"] == "2025-12-31"
        )
        self.assertEqual(164_488_272, rubicon_quarter["total_value"])
        agilon = next(
            holding
            for holding in rubicon_quarter["holdings"]
            if holding["cusip"] == "00857U107"
        )
        self.assertEqual(4_333_573, agilon["value"])

        wellesley = json.loads((funds_dir / "1533551.json").read_text())
        wellesley_quarter = next(
            quarter
            for quarter in wellesley["quarters"]
            if quarter["report_date"] == "2026-03-31"
        )
        self.assertEqual(952_526_299, wellesley_quarter["total_value"])
        self.assertEqual(1, wellesley_quarter["value_multiplier"])

        maryland = json.loads((funds_dir / "1624050.json").read_text())
        maryland_quarter = next(
            quarter
            for quarter in maryland["quarters"]
            if quarter["report_date"] == "2026-03-31"
        )
        self.assertEqual(5_101_000, maryland_quarter["total_value"])
        self.assertEqual(1000, maryland_quarter["value_multiplier"])


if __name__ == "__main__":
    unittest.main()
