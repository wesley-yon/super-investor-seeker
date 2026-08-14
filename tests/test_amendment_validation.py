import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import validate_data


BASE_ACCESSION = "0000000001-25-000001"
ADD_ACCESSION = "0000000001-25-000002"
EARLY_ADD_ACCESSION = "0000000001-25-000003"
RESTATEMENT_ACCESSION = "0000000001-25-000004"


def source_filing(
    accession: str,
    kind: str,
    accepted_at: str,
    *,
    applied: bool = True,
) -> dict:
    is_original = kind == "ORIGINAL"
    return {
        "accession": accession,
        "form_type": "13F-HR" if is_original else "13F-HR/A",
        "filing_date": accepted_at[:10],
        "accepted_at": accepted_at,
        "amendment_number": None if is_original else 1,
        "amendment_kind": kind,
        "source_hash": "b" * 64,
        "reported_entry_total": 1,
        "reported_value_total": 100,
        "applied": applied,
    }


def valid_quarter() -> dict:
    quarter = {
        "composition_version": 1,
        "is_complete": True,
        "report_date": "2025-03-31",
        "accession": ADD_ACCESSION,
        "filing_date": "2025-05-20",
        "base_accession": BASE_ACCESSION,
        "applied_accessions": [BASE_ACCESSION, ADD_ACCESSION],
        "composition_hash": "",
        "num_holdings": 2,
        "total_value": 150,
        "holdings": [{"value": 100}, {"value": 50}],
        "source_filings": [
            source_filing(
                BASE_ACCESSION,
                "ORIGINAL",
                "2025-05-15T12:00:00Z",
            ),
            source_filing(
                ADD_ACCESSION,
                "NEW_HOLDINGS",
                "2025-05-20T12:00:00Z",
            ),
        ],
    }
    quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)
    return quarter


def overlap_evidence(
    *,
    matched_rows: int,
    prior_rows: int,
    amendment_rows: int,
    exact_positions: bool = False,
) -> dict:
    return {
        "identity_version": 1,
        "matched_rows": matched_rows,
        "prior_rows": prior_rows,
        "amendment_rows": amendment_rows,
        "exact_positions": exact_positions,
    }


def valid_v2_append_quarter() -> dict:
    quarter = valid_quarter()
    quarter["composition_version"] = 2
    quarter["source_filings"][0]["composition_action"] = "BASE"
    quarter["source_filings"][1].update({
        "composition_action": "APPEND",
        "new_holdings_overlap": overlap_evidence(
            matched_rows=0,
            prior_rows=1,
            amendment_rows=1,
        ),
    })
    quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)
    return quarter


def valid_v2_replace_quarter() -> dict:
    quarter = valid_quarter()
    quarter["composition_version"] = 2
    quarter["base_accession"] = ADD_ACCESSION
    quarter["applied_accessions"] = [ADD_ACCESSION]
    quarter["holdings"] = [
        {"cusip": f"{index:09d}", "value": 10}
        for index in range(10)
    ]
    quarter["num_holdings"] = 10
    quarter["total_value"] = 100
    quarter["source_filings"][0].update({
        "reported_entry_total": 10,
        "composition_action": "SUPERSEDED",
        "applied": False,
    })
    quarter["source_filings"][1].update({
        "reported_entry_total": 10,
        "composition_action": "REPLACE",
        "new_holdings_overlap": overlap_evidence(
            matched_rows=9,
            prior_rows=10,
            amendment_rows=10,
        ),
    })
    quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)
    return quarter


def valid_v2_replacement_after_append_quarter() -> dict:
    quarter = valid_v2_replace_quarter()
    earlier_append = source_filing(
        EARLY_ADD_ACCESSION,
        "NEW_HOLDINGS",
        "2025-05-18T12:00:00Z",
        applied=False,
    )
    earlier_append.update({
        "composition_action": "SUPERSEDED",
        "new_holdings_overlap": overlap_evidence(
            matched_rows=0,
            prior_rows=10,
            amendment_rows=1,
        ),
    })
    quarter["source_filings"].insert(1, earlier_append)
    quarter["source_filings"][2]["amendment_number"] = 2
    quarter["source_filings"][2]["new_holdings_overlap"] = overlap_evidence(
        matched_rows=10,
        prior_rows=11,
        amendment_rows=10,
    )
    quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)
    return quarter


def valid_v2_restatement_after_unmeasured_new_quarter() -> dict:
    quarter = valid_v2_append_quarter()
    for source in quarter["source_filings"]:
        source["applied"] = False
        source["composition_action"] = "SUPERSEDED"
    quarter["source_filings"][1].pop("new_holdings_overlap")

    restatement = source_filing(
        RESTATEMENT_ACCESSION,
        "RESTATEMENT",
        "2025-05-25T12:00:00Z",
    )
    restatement.update({
        "amendment_number": 2,
        "reported_value_total": 75,
        "composition_action": "BASE",
    })
    quarter["source_filings"].append(restatement)
    quarter["base_accession"] = RESTATEMENT_ACCESSION
    quarter["applied_accessions"] = [RESTATEMENT_ACCESSION]
    quarter["accession"] = RESTATEMENT_ACCESSION
    quarter["filing_date"] = "2025-05-25"
    quarter["holdings"] = [{"value": 75}]
    quarter["num_holdings"] = 1
    quarter["total_value"] = 75
    quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)
    return quarter


class AmendmentCompositionValidationTests(unittest.TestCase):
    def validate(self, quarter: dict) -> list[str]:
        errors: list[str] = []
        validate_data.validate_amendment_composition(quarter, "test quarter", errors)
        return errors

    def test_structured_sources_require_exact_valid_filing_dates(self) -> None:
        for filing_date in ("2025-05", "2025-02-31", "May 2025", None):
            with self.subTest(filing_date=filing_date):
                quarter = valid_quarter()
                quarter["source_filings"][0]["filing_date"] = filing_date
                errors = self.validate(quarter)
                self.assertTrue(any(
                    "structured provenance requires a valid YYYY-MM-DD date"
                    in error
                    for error in errors
                ))

    def test_valid_original_plus_new_holdings_chain(self):
        self.assertEqual(self.validate(valid_quarter()), [])

    def test_cover_reconciliation_metadata_is_complete_and_truthful(self):
        quarter = valid_quarter()
        for source in quarter["source_filings"]:
            source.update({
                "cover_reported_entry_total": source["reported_entry_total"],
                "cover_reported_value_total": source["reported_value_total"],
                "cover_reconciliation_status": "EXACT",
            })
        self.assertEqual(self.validate(quarter), [])

        incomplete = copy.deepcopy(quarter)
        incomplete["source_filings"][0].pop("cover_reported_value_total")
        self.assertTrue(any(
            "incomplete cover reconciliation metadata" in error
            for error in self.validate(incomplete)
        ))

        false_exact = copy.deepcopy(quarter)
        false_exact["source_filings"][0]["cover_reported_value_total"] += 1
        self.assertTrue(any(
            "marks unequal cover and table totals as EXACT" in error
            for error in self.validate(false_exact)
        ))

        false_mismatch = copy.deepcopy(quarter)
        false_mismatch["source_filings"][0][
            "cover_reconciliation_status"
        ] = "MISMATCH_UNIQUE_TABLE"
        self.assertTrue(any(
            "marks equal cover and table totals as mismatched" in error
            for error in self.validate(false_mismatch)
        ))

    def test_unit_provenance_reconciles_applied_source_totals(self):
        quarter = valid_quarter()
        quarter["source_filings"][1]["reported_value_total"] = 50
        for source in quarter["source_filings"]:
            source.update({
                "value_unit_policy_version": (
                    validate_data.VALUE_UNIT_POLICY_VERSION
                ),
                "value_multiplier": 1,
                "normalized_value_total": source["reported_value_total"],
                "value_unit_method": "weighted_equity_dollars",
                "value_unit_confidence": "high",
                "value_unit_evidence": {},
            })

        self.assertEqual(self.validate(quarter), [])

        quarter["source_filings"][1]["normalized_value_total"] = 50000
        errors = self.validate(quarter)
        self.assertTrue(
            any("normalized_value_total" in error for error in errors)
        )
        self.assertTrue(
            any("applied sources" in error for error in errors)
        )

    def test_legacy_v1_unit_provenance_is_valid_but_not_current_proof(self):
        quarter = valid_quarter()
        quarter["source_filings"][1]["reported_value_total"] = 50
        for source in quarter["source_filings"]:
            source.update({
                "value_unit_policy_version": 1,
                "value_multiplier": 1,
                "normalized_value_total": source["reported_value_total"],
                "value_unit_method": "weighted_equity_dollars",
                "value_unit_confidence": "high",
                "value_unit_evidence": {},
            })

        self.assertEqual(self.validate(quarter), [])

    def test_legacy_v1_unit_totals_still_reconcile(self):
        quarter = valid_quarter()
        for source in quarter["source_filings"]:
            source.update({
                "value_unit_policy_version": 1,
                "value_multiplier": 1,
                "normalized_value_total": source["reported_value_total"],
                "value_unit_method": "weighted_equity_dollars",
                "value_unit_confidence": "high",
                "value_unit_evidence": {},
            })
        quarter["source_filings"][1]["normalized_value_total"] += 1

        errors = self.validate(quarter)

        self.assertTrue(any(
            "does not match applied sources" in error
            for error in errors
        ))

    def test_valid_restatement_base_can_supersede_earlier_sources(self):
        quarter = valid_quarter()
        restatement_accession = "0000000001-25-000003"
        quarter["base_accession"] = restatement_accession
        quarter["applied_accessions"] = [restatement_accession, ADD_ACCESSION]
        quarter["source_filings"] = [
            source_filing(
                BASE_ACCESSION,
                "ORIGINAL",
                "2025-05-15T12:00:00Z",
                applied=False,
            ),
            source_filing(
                restatement_accession,
                "RESTATEMENT",
                "2025-05-18T12:00:00Z",
            ),
            source_filing(
                ADD_ACCESSION,
                "NEW_HOLDINGS",
                "2025-05-20T12:00:00Z",
            ),
        ]
        quarter["source_filings"][2]["amendment_number"] = 2
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        self.assertEqual(self.validate(quarter), [])

    def test_single_source_does_not_require_an_ordering_timestamp(self):
        quarter = valid_quarter()
        quarter["applied_accessions"] = [BASE_ACCESSION]
        quarter["accession"] = BASE_ACCESSION
        quarter["filing_date"] = "2025-05-15"
        quarter["source_filings"] = [quarter["source_filings"][0]]
        quarter["source_filings"][0]["accepted_at"] = None
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        self.assertEqual(self.validate(quarter), [])

    def test_legacy_quarter_is_not_subject_to_composition_validation(self):
        legacy = {
            "holdings": [{"value": 100}],
            "num_holdings": 99,
            "total_value": 0,
        }
        self.assertEqual(self.validate(legacy), [])

    def test_explicit_unknown_composition_version_fails_closed(self):
        quarter = valid_quarter()
        quarter["composition_version"] = 3

        errors = self.validate(quarter)

        self.assertTrue(any("unsupported composition_version" in error for error in errors))

    def test_valid_v2_base_plus_append_chain(self):
        self.assertEqual(self.validate(valid_v2_append_quarter()), [])

    def test_valid_v2_inferred_replace_becomes_the_active_base(self):
        self.assertEqual(self.validate(valid_v2_replace_quarter()), [])

    def test_composition_requires_list_holdings(self):
        cases = {
            "missing": lambda quarter: quarter.pop("holdings"),
            "null": lambda quarter: quarter.__setitem__("holdings", None),
            "object": lambda quarter: quarter.__setitem__("holdings", {}),
        }
        for label, mutate in cases.items():
            with self.subTest(label):
                quarter = valid_v2_append_quarter()
                mutate(quarter)

                errors = self.validate(quarter)

                self.assertTrue(
                    any("non-list holdings" in error for error in errors)
                )

    def test_v2_composition_hash_binds_source_decision_metadata(self):
        cases = {
            "amendment_kind": ("amendment_kind", "RESTATEMENT"),
            "form_type": ("form_type", "13F-HR"),
            "amendment_number": ("amendment_number", 2),
            "accepted_at": ("accepted_at", "2025-05-21T12:00:00Z"),
        }
        for label, (field, value) in cases.items():
            with self.subTest(label):
                quarter = valid_v2_append_quarter()
                quarter["source_filings"][1][field] = value

                errors = self.validate(quarter)

                self.assertTrue(
                    any(
                        "does not match composition content" in error
                        for error in errors
                    )
                )

    def test_v2_rejects_multiple_original_sources(self):
        quarter = valid_v2_replace_quarter()
        second_original = quarter["source_filings"][1]
        second_original.update({
            "form_type": "13F-HR",
            "amendment_number": None,
            "amendment_kind": "ORIGINAL",
            "composition_action": "BASE",
        })
        second_original.pop("new_holdings_overlap")
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        errors = self.validate(quarter)

        self.assertTrue(
            any("multiple original sources" in error for error in errors)
        )

    def test_v2_evaluated_superseded_new_holdings_requires_overlap_evidence(self):
        quarter = valid_v2_replacement_after_append_quarter()
        quarter["source_filings"][1].pop("new_holdings_overlap")
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        errors = self.validate(quarter)

        self.assertTrue(
            any(
                "evaluated NEW_HOLDINGS source" in error
                and "missing overlap evidence" in error
                for error in errors
            )
        )

    def test_v2_new_holdings_before_restatement_may_omit_overlap_evidence(self):
        quarter = valid_v2_restatement_after_unmeasured_new_quarter()

        self.assertEqual(self.validate(quarter), [])

    def test_v2_append_rejects_any_active_portfolio_overlap(self):
        quarter = valid_v2_append_quarter()
        quarter["source_filings"][1]["new_holdings_overlap"] = overlap_evidence(
            matched_rows=1,
            prior_rows=1,
            amendment_rows=1,
            exact_positions=True,
        )
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        errors = self.validate(quarter)

        self.assertTrue(
            any("APPEND source" in error and "overlaps" in error for error in errors)
        )

    def test_v2_rejects_empty_new_holdings_table(self):
        quarter = valid_v2_append_quarter()
        quarter["source_filings"][1]["new_holdings_overlap"] = overlap_evidence(
            matched_rows=0,
            prior_rows=1,
            amendment_rows=0,
        )
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        errors = self.validate(quarter)

        self.assertTrue(
            any("empty NEW_HOLDINGS table" in error for error in errors)
        )

    def test_v2_replace_rejects_overlap_below_replacement_threshold(self):
        quarter = valid_v2_replace_quarter()
        quarter["source_filings"][1]["new_holdings_overlap"] = overlap_evidence(
            matched_rows=8,
            prior_rows=10,
            amendment_rows=10,
        )
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        errors = self.validate(quarter)

        self.assertTrue(
            any("lacks clear replacement overlap" in error for error in errors)
        )

    def test_v2_rejects_unknown_composition_action(self):
        quarter = valid_v2_append_quarter()
        quarter["source_filings"][1]["composition_action"] = "MERGE"
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        errors = self.validate(quarter)

        self.assertTrue(
            any("invalid composition_action" in error for error in errors)
        )

    def test_v2_applied_complete_base_must_use_base_action(self):
        quarter = valid_v2_append_quarter()
        quarter["source_filings"][0]["composition_action"] = "SUPERSEDED"
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        errors = self.validate(quarter)

        self.assertTrue(
            any("applied complete base source must use BASE action" in error for error in errors)
        )

    def test_v2_non_applied_source_must_use_superseded_action(self):
        quarter = valid_v2_replace_quarter()
        quarter["source_filings"][0]["composition_action"] = "BASE"
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        errors = self.validate(quarter)

        self.assertTrue(
            any("non-applied source" in error and "SUPERSEDED action" in error for error in errors)
        )

    def test_v2_composition_hash_covers_source_action(self):
        quarter = valid_v2_append_quarter()
        quarter["source_filings"][1]["composition_action"] = "REPLACE"

        errors = self.validate(quarter)

        self.assertTrue(
            any("does not match composition content" in error for error in errors)
        )

    def test_v2_composition_hash_covers_overlap_evidence(self):
        quarter = valid_v2_append_quarter()
        quarter["source_filings"][1]["new_holdings_overlap"]["amendment_rows"] = 2

        errors = self.validate(quarter)

        self.assertTrue(
            any("does not match composition content" in error for error in errors)
        )

    def test_completion_totals_and_hash_are_reconciled(self):
        quarter = valid_quarter()
        quarter["is_complete"] = False
        quarter["num_holdings"] = 1
        quarter["total_value"] = 149
        quarter["composition_hash"] = "A" * 64

        errors = self.validate(quarter)

        self.assertTrue(any("not complete" in error for error in errors))
        self.assertTrue(any("num_holdings" in error for error in errors))
        self.assertTrue(any("total_value" in error for error in errors))
        self.assertTrue(any("64 lowercase hexadecimal" in error for error in errors))

    def test_accession_lists_must_match_unique_applied_sources(self):
        quarter = valid_quarter()
        quarter["applied_accessions"] = [BASE_ACCESSION, BASE_ACCESSION]
        quarter["source_filings"].append(copy.deepcopy(quarter["source_filings"][0]))

        errors = self.validate(quarter)

        self.assertTrue(any("duplicate applied accessions" in error for error in errors))
        self.assertTrue(any("duplicate source accession" in error for error in errors))
        self.assertTrue(any("do not match applied source_filings" in error for error in errors))

    def test_base_and_later_source_kinds_are_constrained(self):
        quarter = valid_quarter()
        quarter["source_filings"][0]["amendment_kind"] = "NEW_HOLDINGS"
        quarter["source_filings"][1]["amendment_kind"] = "RESTATEMENT"

        errors = self.validate(quarter)

        self.assertTrue(any("applied base source" in error for error in errors))
        self.assertTrue(any("later applied source" in error for error in errors))

    def test_form_type_must_match_original_or_amendment_kind(self):
        quarter = valid_quarter()
        quarter["source_filings"][0]["form_type"] = "13F-HR/A"
        quarter["source_filings"][1]["form_type"] = "13F-HR"

        errors = self.validate(quarter)

        self.assertTrue(any("original source" in error for error in errors))
        self.assertTrue(any("amendment source" in error for error in errors))

    def test_top_level_provenance_must_match_latest_applied_source(self):
        quarter = valid_quarter()
        quarter["accession"] = BASE_ACCESSION
        quarter["filing_date"] = "2025-05-15"

        errors = self.validate(quarter)

        self.assertTrue(any("top-level accession" in error for error in errors))
        self.assertTrue(any("top-level filing_date" in error for error in errors))

    def test_applied_sources_must_be_in_acceptance_order(self):
        quarter = valid_quarter()
        quarter["source_filings"][0]["accepted_at"] = "2025-05-21T12:00:00Z"

        errors = self.validate(quarter)

        self.assertTrue(any("acceptance order" in error for error in errors))

    def test_non_applied_source_after_base_is_not_a_complete_active_chain(self):
        quarter = valid_quarter()
        hidden_accession = "0000000001-25-000003"
        quarter["source_filings"].append(source_filing(
            hidden_accession,
            "UNKNOWN",
            "2025-05-25T12:00:00Z",
            applied=False,
        ))

        errors = self.validate(quarter)

        self.assertTrue(any("active source tail" in error for error in errors))
        self.assertTrue(any("later applied source" in error for error in errors))

    def test_valid_looking_but_stale_composition_hash_is_rejected(self):
        quarter = valid_quarter()
        quarter["holdings"][0]["value"] = 99
        quarter["holdings"][1]["value"] = 51

        errors = self.validate(quarter)

        self.assertTrue(any("does not match composition content" in error for error in errors))

    def test_display_issuer_rewrite_does_not_invalidate_composition_hash(self):
        quarter = valid_quarter()
        quarter["holdings"][0]["issuer"] = "RAW SEC ISSUER"
        quarter["composition_hash"] = validate_data.calculate_composition_hash(quarter)

        # Registry-backed regeneration canonicalizes display names after the
        # immutable filing chain is composed. Display-only rewrites must not
        # alter the source composition identity.
        quarter["holdings"][0]["issuer"] = "CANONICAL ISSUER NAME"

        self.assertEqual(self.validate(quarter), [])

    def test_malformed_source_reports_error_instead_of_crashing_hash_check(self):
        quarter = valid_quarter()
        quarter["source_filings"].append(None)

        errors = self.validate(quarter)

        self.assertTrue(any("is not an object" in error for error in errors))


class AmendmentMigrationStateValidationTests(unittest.TestCase):
    def validate_state(
        self,
        state: dict,
        fund: dict,
    ) -> tuple[list[str], list[str], dict[str, object]]:
        state.setdefault(
            "security_identity_migration_version",
            validate_data._SECURITY_IDENTITY_VERSION,
        )
        state.setdefault("security_identity_migration_pending", {})
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            state_path = root / "pipeline_state.json"
            fund_path = funds_dir / "1393818.json"
            state_path.write_text(json.dumps(state))
            fund_path.write_text(json.dumps(fund))
            errors: list[str] = []
            warnings: list[str] = []
            quality_summary: dict[str, object] = {}
            with mock.patch.object(validate_data, "STATE_PATH", state_path):
                validate_data.validate_pipeline_state(
                    {"1393818": fund_path},
                    errors,
                    warnings,
                    quality_summary,
                )
            return errors, warnings, quality_summary

    def test_pending_target_may_reference_withheld_report_date(self):
        state = {
            "amendment_reducer_version": validate_data._AMENDMENT_REDUCER_VERSION,
            "processed": [],
            "quarantined": {ADD_ACCESSION: {"reason": "parse failure"}},
            "amendment_migration_pending": {
                ADD_ACCESSION: {"cik": 1393818, "report_date": "2025-03-31"}
            },
        }
        fund = {
            "cik": 1393818,
            "quarters": [],
        }

        errors, warnings, quality_summary = self.validate_state(state, fund)

        self.assertEqual([], errors)
        self.assertTrue(any("retry automatically" in warning for warning in warnings))
        self.assertEqual(
            1,
            quality_summary["amendment_migration_pending"],
        )
        self.assertEqual(
            0,
            quality_summary["security_identity_migration_pending"],
        )
        self.assertTrue(any(
            "value-unit corpus migration remains pending" in warning
            for warning in warnings
        ))

    def test_global_v2_state_rejects_published_v1_new_holdings(self):
        state = {
            "amendment_reducer_version": validate_data._AMENDMENT_REDUCER_VERSION,
            "processed": [],
            "quarantined": {},
            "amendment_migration_pending": {},
        }
        fund = {
            "cik": 1393818,
            "quarters": [valid_quarter()],
        }

        errors, _warnings, _summary = self.validate_state(state, fund)

        self.assertTrue(
            any(
                "still publishes a v1 NEW_HOLDINGS composition" in error
                for error in errors
            )
        )

    def test_pending_target_cannot_be_processed_or_already_published(self):
        state = {
            "amendment_reducer_version": validate_data._AMENDMENT_REDUCER_VERSION,
            "processed": [ADD_ACCESSION],
            "quarantined": {},
            "amendment_migration_pending": {
                ADD_ACCESSION: {"cik": 1393818, "report_date": "2025-03-31"}
            },
        }
        fund = {
            "cik": 1393818,
            "quarters": [{
                "report_date": "2025-03-31",
                "composition_version": validate_data._AMENDMENT_REDUCER_VERSION,
            }],
        }

        errors, _warnings, _summary = self.validate_state(state, fund)

        self.assertTrue(any("marked processed" in error for error in errors))
        self.assertTrue(any("missing quarantine" in error for error in errors))
        self.assertTrue(any("remains queued" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
