from __future__ import annotations

import copy
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

import pipeline
from scripts import repair_value_units


FIXTURE_FUNDS_DIR = Path(__file__).resolve().parent / "fixtures/corpus/funds"


class ExplicitHistoricalRepairTests(unittest.TestCase):
    def load_fund(self, cik: int) -> dict:
        return json.loads((FIXTURE_FUNDS_DIR / f"{cik}.json").read_text())

    def source_quarter(self, cik: int, report_date: str) -> dict:
        quarter = copy.deepcopy(next(
            quarter
            for quarter in self.load_fund(cik)["quarters"]
            if quarter["report_date"] == report_date
        ))
        spec = repair_value_units.EXPLICIT_HISTORICAL_REPAIRS[
            (cik, report_date)
        ]
        if quarter["total_value"] == spec["correct_total"]:
            unscaled_cusips = set(spec.get("unscaled_cusips", ()))
            for holding in quarter["holdings"]:
                cusip = str(holding.get("cusip") or "").strip().upper()
                if (
                    spec["operation"] == "multiply_except_cusips"
                    and cusip in unscaled_cusips
                ):
                    continue
                if spec["operation"] != "divide_all":
                    holding["value"] //= 1000
                else:
                    holding["value"] *= 1000
            quarter["total_value"] = sum(
                holding["value"] for holding in quarter["holdings"]
            )
        self.assertEqual(spec["bad_total"], quarter["total_value"])
        self.assertEqual(
            spec["bad_signature"],
            repair_value_units.holding_value_signature(quarter["holdings"]),
        )
        return quarter

    def test_exact_manifest_repairs_are_idempotent(self) -> None:
        for key, spec in (
            repair_value_units.EXPLICIT_HISTORICAL_REPAIRS.items()
        ):
            with self.subTest(cik=key[0], report_date=key[1]):
                quarter = self.source_quarter(*key)

                self.assertTrue(
                    repair_value_units.repair_explicit_historical_quarter(
                        quarter,
                        spec,
                    )
                )

                self.assertEqual(spec["correct_total"], quarter["total_value"])
                self.assertEqual(
                    spec["correct_signature"],
                    repair_value_units.holding_value_signature(
                        quarter["holdings"]
                    ),
                )
                if spec["value_multiplier"] is None:
                    self.assertNotIn("value_multiplier", quarter)
                    self.assertNotIn("value_unit_policy_version", quarter)
                    self.assertEqual(
                        "sec_verified_historical_migration",
                        quarter["value_unit_repair"]["method"],
                    )
                    self.assertEqual(
                        spec["accession"],
                        quarter["value_unit_repair"]["evidence"][
                            "sec_accession"
                        ],
                    )
                else:
                    self.assertEqual(
                        "sec_verified_historical_migration",
                        quarter["value_unit_method"],
                    )
                    self.assertEqual(
                        spec["accession"],
                        quarter["value_unit_evidence"]["sec_accession"],
                    )
                    self.assertEqual(
                        spec["value_multiplier"],
                        quarter["value_multiplier"],
                    )
                self.assertFalse(
                    repair_value_units.repair_explicit_historical_quarter(
                        quarter,
                        spec,
                    )
                )

    def test_ccla_scales_only_verified_thousands_rows(self) -> None:
        key = (1631562, "2025-06-30")
        spec = repair_value_units.EXPLICIT_HISTORICAL_REPAIRS[key]
        quarter = self.source_quarter(*key)
        before = {
            holding["cusip"]: holding["value"]
            for holding in quarter["holdings"]
        }

        repair_value_units.repair_explicit_historical_quarter(quarter, spec)
        after = {
            holding["cusip"]: holding["value"]
            for holding in quarter["holdings"]
        }

        self.assertEqual(before["002824100"], after["002824100"])
        self.assertEqual(before["594918104"] * 1000, after["594918104"])
        self.assertEqual(6_102_754_336, quarter["total_value"])
        self.assertEqual(
            {"default": 1000, "002824100": 1},
            quarter["value_unit_repair"]["evidence"][
                "row_value_multipliers"
            ],
        )
        self.assertNotIn("value_unit_policy_version", quarter)
        self.assertNotIn(1631562, repair_value_units.KNOWN_REPAIRS[1])

    def test_manifest_fails_closed_if_rows_change(self) -> None:
        key = (1629996, "2025-12-31")
        spec = repair_value_units.EXPLICIT_HISTORICAL_REPAIRS[key]
        quarter = self.source_quarter(*key)
        quarter["holdings"][0]["value"] += 1
        quarter["holdings"][1]["value"] -= 1

        with self.assertRaisesRegex(ValueError, "source row signature changed"):
            repair_value_units.repair_explicit_historical_quarter(
                quarter,
                spec,
            )

    def test_apply_validates_all_targets_before_writing(self) -> None:
        funds = {
            cik: self.load_fund(cik)
            for cik in {key[0] for key in (
                repair_value_units.EXPLICIT_HISTORICAL_REPAIRS
            )}
        }
        for key in repair_value_units.EXPLICIT_HISTORICAL_REPAIRS:
            cik, report_date = key
            source = self.source_quarter(cik, report_date)
            index = next(
                index
                for index, quarter in enumerate(funds[cik]["quarters"])
                if quarter["report_date"] == report_date
            )
            funds[cik]["quarters"][index] = source

        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            for cik, fund in funds.items():
                (funds_dir / f"{cik}.json").write_text(json.dumps(fund))

            with mock.patch.object(
                repair_value_units,
                "FUNDS_DIR",
                funds_dir,
            ):
                self.assertEqual(
                    6,
                    repair_value_units.apply_explicit_historical_repairs(),
                )
                self.assertEqual(
                    0,
                    repair_value_units.apply_explicit_historical_repairs(),
                )

    def test_backfill_skips_current_composed_source_provenance(self) -> None:
        fund = {
            "cik": 999,
            "quarters": [{
                "report_date": "2025-12-31",
                "source_filings": [{
                    "applied": True,
                    "value_unit_policy_version": (
                        repair_value_units.VALUE_UNIT_POLICY_VERSION
                    ),
                    "value_multiplier": 1,
                    "value_unit_confidence": "high",
                }],
            }],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            path = funds_dir / "999.json"
            path.write_text(json.dumps(fund))
            with (
                mock.patch.object(
                    repair_value_units,
                    "FUNDS_DIR",
                    funds_dir,
                ),
                mock.patch.object(
                    repair_value_units,
                    "KNOWN_REPAIRS",
                    {1: {999: ("2025-12-31",)}},
                ),
            ):
                self.assertEqual(
                    0,
                    repair_value_units.backfill_known_repair_provenance(),
                )
            self.assertEqual(fund, json.loads(path.read_text()))

    def test_arithmetic_source_assignment_is_not_high_confidence(self) -> None:
        quarter = {
            "total_value": 2_003,
            "source_filings": [
                {
                    "accession": "0000000001-25-000001",
                    "applied": True,
                    "reported_value_total": 2,
                },
                {
                    "accession": "0000000001-25-000002",
                    "applied": True,
                    "reported_value_total": 3,
                },
            ],
        }

        repair_value_units.backfill_unit_provenance(quarter)

        sources = quarter["source_filings"]
        self.assertEqual([1_000, 1], [
            source["value_multiplier"] for source in sources
        ])
        self.assertTrue(all(
            source["value_unit_policy_version"]
            == repair_value_units.VALUE_UNIT_POLICY_VERSION
            for source in sources
        ))
        self.assertTrue(all(
            source["value_unit_confidence"] == "low"
            and source["value_unit_method"] == "arithmetic_only_migration"
            and source["value_unit_evidence"]["independent_unit_proof"] is False
            for source in sources
        ))

    def test_policy_cutover_is_marked_only_after_a_clean_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "pipeline_state.json"
            state_path.write_text(json.dumps({"processed": []}))
            inventory = Counter({
                "quarters": 10,
                "legacy_or_low_confidence": 10,
            })
            with (
                mock.patch.object(
                    repair_value_units,
                    "STATE_PATH",
                    state_path,
                ),
                mock.patch.object(
                    repair_value_units,
                    "apply_explicit_historical_repairs",
                    return_value=0,
                ),
                mock.patch.object(
                    repair_value_units,
                    "audit_retained_value_unit_policy",
                    return_value=(inventory, []),
                ),
            ):
                result, explicit = (
                    repair_value_units.migrate_value_unit_policy()
                )

            self.assertEqual(inventory, result)
            self.assertEqual(0, explicit)
            self.assertEqual(
                repair_value_units.VALUE_UNIT_POLICY_VERSION,
                json.loads(state_path.read_text())[
                    "value_unit_migration_version"
                ],
            )

    def test_policy_cutover_fails_closed_on_a_corpus_anomaly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "pipeline_state.json"
            state_path.write_text(json.dumps({"processed": []}))
            with (
                mock.patch.object(
                    repair_value_units,
                    "STATE_PATH",
                    state_path,
                ),
                mock.patch.object(
                    repair_value_units,
                    "apply_explicit_historical_repairs",
                    return_value=0,
                ),
                mock.patch.object(
                    repair_value_units,
                    "audit_retained_value_unit_policy",
                    return_value=(
                        Counter({"quarters": 10}),
                        ["999 2025-12-31 mixed_scale_clusters"],
                    ),
                ),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "found 1 anomaly",
                ):
                    repair_value_units.migrate_value_unit_policy()

            self.assertNotIn(
                "value_unit_migration_version",
                json.loads(state_path.read_text()),
            )

    def test_pipeline_state_preserves_value_unit_cutover_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "pipeline_state.json"
            legacy_state_path = Path(tmpdir) / "legacy-state.json"
            state = {
                "processed": [],
                "_processed_set": set(),
                "quarantined": {},
                "_quarantined": {},
                "amendment_migration_pending": {},
                "amendment_reducer_version": (
                    pipeline.AMENDMENT_REDUCER_VERSION
                ),
                "security_identity_migration_pending": {},
                "security_identity_migration_version": (
                    pipeline.SECURITY_IDENTITY_VERSION
                ),
                "value_unit_migration_version": (
                    pipeline.VALUE_UNIT_MIGRATION_VERSION
                ),
            }
            with mock.patch.multiple(
                pipeline,
                STATE_PATH=state_path,
                LEGACY_STATE_PATH=legacy_state_path,
            ):
                pipeline.save_state(state)
                loaded = pipeline.load_state()

            self.assertEqual(
                pipeline.VALUE_UNIT_MIGRATION_VERSION,
                loaded["value_unit_migration_version"],
            )


if __name__ == "__main__":
    unittest.main()
