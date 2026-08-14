from __future__ import annotations

import unittest

import composition_integrity
import pipeline
import validate_data


class CompositionIntegrityCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sources = [
            {
                "accession": "0000000000-26-000001",
                "source_hash": "a" * 64,
                "form_type": "13F-HR",
                "accepted_at": "2026-05-15T12:00:00Z",
                "amendment_number": 0,
                "amendment_kind": "ORIGINAL",
                "composition_action": "BASE",
                "new_holdings_overlap": None,
                "security_identity_version": 1,
            },
            {
                "accession": "0000000000-26-000002",
                "source_hash": "b" * 64,
                "form_type": "13F-HR/A",
                "accepted_at": "2026-05-16T12:00:00Z",
                "amendment_number": 1,
                "amendment_kind": "NEW_HOLDINGS",
                "composition_action": "APPEND",
                "new_holdings_overlap": {"matched_rows": 0},
                "security_identity_version": 1,
            },
        ]
        self.holdings = [
            {
                "cusip": "222222222",
                "class": "COM",
                "value": 250,
                "shares": 25,
                "put_call": None,
                "holding_type": "EQUITY",
            },
            {
                "cusip": "111111111",
                "class": "COM",
                "value": 100,
                "shares": 10,
                "shares_imputed": True,
                "put_call": "CALL",
                "holding_type": "CALL",
            },
        ]
        self.applied = [source["accession"] for source in self.sources]

    def quarter(self, hash_version: int) -> dict:
        return {
            "composition_version": 2,
            "composition_hash_version": hash_version,
            "security_identity_version": 1,
            "report_date": "2026-03-31",
            "base_accession": self.applied[0],
            "applied_accessions": self.applied,
            "source_filings": self.sources,
            "holdings": self.holdings,
        }

    def test_frozen_v1_and_v2_hashes_are_byte_compatible(self) -> None:
        expected = {
            1: "1bff89888b689e529d0b87235a7970bb437e95b561c5f5d597e4b32135add562",
            2: "d47ff67d58601554b6af6623b8a2fccaa7179c2f6e77e284d83200b100a5d400",
        }
        for hash_version, digest in expected.items():
            with self.subTest(hash_version=hash_version):
                quarter = self.quarter(hash_version)
                self.assertEqual(
                    digest,
                    composition_integrity.calculate_quarter_composition_hash(
                        quarter,
                        current_hash_version=2,
                    ),
                )
                self.assertEqual(
                    digest,
                    validate_data.calculate_composition_hash(quarter),
                )
                self.assertEqual(
                    digest,
                    pipeline.calculate_composition_hash(
                        quarter["report_date"],
                        quarter["base_accession"],
                        quarter["applied_accessions"],
                        [
                            source["source_hash"]
                            for source in quarter["source_filings"]
                        ],
                        quarter["holdings"],
                        composition_version=quarter["composition_version"],
                        source_filings=quarter["source_filings"],
                        security_identity_version=(
                            quarter["security_identity_version"]
                        ),
                        composition_hash_version=hash_version,
                    ),
                )

    def test_imputed_share_value_does_not_change_source_composition(self) -> None:
        quarter = self.quarter(2)
        original = composition_integrity.calculate_quarter_composition_hash(
            quarter,
            current_hash_version=2,
        )
        quarter["holdings"][1]["shares"] = 999_999

        self.assertEqual(
            original,
            composition_integrity.calculate_quarter_composition_hash(
                quarter,
                current_hash_version=2,
            ),
        )


if __name__ == "__main__":
    unittest.main()
