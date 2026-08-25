from __future__ import annotations

import copy
import hashlib
import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from insider_parser import INSIDER_PARSER_VERSION, parse_ownership_xml
from insider_pipeline import (
    NormalizedIssuerRecord,
    issuer_record_from_normalized,
    reduce_issuer_state,
)
from insider_publication import validate_insider_public_tree
from insider_storage import (
    InsiderStateStore,
    InsiderStorage,
    InsiderStorageError,
    canonical_insider_state_json_bytes,
    issuer_generation_digest,
)
from security_identity import stock_file_stem
from tests.test_insider_storage import store_source_prerequisites


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "insider_filings"
ORACLE = json.loads((FIXTURE_ROOT / "expectations.json").read_text())
AS_OF = "2026-06-30T20:45:00Z"
SYNC_AT = "2026-06-30T20:40:00Z"


def materializer_module():
    return importlib.import_module("scripts.publish_insider_activity")


def policy_mapping(
    issuer_state: dict[str, object],
    *,
    stock_id: str,
    ticker: str,
    company_name: str,
) -> dict[str, dict[str, object]]:
    security_classes = issuer_state["security_classes"]
    assert isinstance(security_classes, list)
    return {
        entry["security_class_key"]: {
            "stockId": stock_id,
            "fileStem": stock_file_stem(stock_id),
            "ticker": ticker,
            "companyName": company_name,
            "securityType": "Common Stock",
            "securityTypeLabel": "COMMON STOCK",
            "cusip": stock_id,
            "primary": True,
        }
        for entry in security_classes
        if isinstance(entry, dict)
    }


def publication_policy(
    issuer_states: dict[str, dict[str, object]],
) -> dict[str, object]:
    identities = {
        "0000000001": ("03770N101", "ONE", "Synthetic Issuer One"),
        "0000000005": ("084670702", "FIVE", "Synthetic Issuer Five"),
        "0000000007": ("594918104", "SEVEN", "Synthetic Issuer Seven"),
    }
    return {
        "contract_version": 1,
        "issuers": [
            {
                "issuer_cik": issuer_cik,
                "security_mappings": policy_mapping(
                    issuer_states[issuer_cik],
                    stock_id=identities[issuer_cik][0],
                    ticker=identities[issuer_cik][1],
                    company_name=identities[issuer_cik][2],
                ),
            }
            for issuer_cik in sorted(issuer_states)
        ],
    }


def completed_reparse_checkpoint(
    *,
    issuer_cik: str,
    accession_number: str,
    maximum: int = 1,
) -> dict[str, object]:
    return {
        "contract_version": 1,
        "status": "completed",
        "parser_version": INSIDER_PARSER_VERSION,
        "scope": "issuer",
        "scope_identifier": issuer_cik,
        "max_accessions": maximum,
        "queue": [
            {
                "accession_number": accession_number,
                "issuer_cik": issuer_cik,
            }
        ],
        "completed_accessions": [accession_number],
    }


def completed_incremental_checkpoint(
    *,
    case: dict[str, object],
    issuer_cik: str,
) -> dict[str, object]:
    accession_number = case["accession_number"]
    accepted_at = case["accepted_at"]
    index_url = case["source_index_url"]
    expected = case["expected"]
    assert isinstance(accession_number, str)
    assert isinstance(accepted_at, str)
    assert isinstance(index_url, str)
    assert isinstance(expected, dict)
    form_type = expected["form_type"]
    assert isinstance(form_type, str)
    queue_entry = {
        "accession_number": accession_number,
        "issuer_cik": issuer_cik,
        "form_type": form_type,
        "index_url": index_url,
        "accepted_at": accepted_at,
        "observed_at": accepted_at,
    }
    return {
        "contract_version": 1,
        "status": "completed",
        "lookback_seconds": 3600,
        "first_observed_at": accepted_at,
        "last_observed_at": accepted_at,
        "queue": [queue_entry],
        "completed_accessions": [accession_number],
        "source_entries": [
            {
                "accession_number": accession_number,
                "form_type": form_type,
                "entity_role": "issuer",
                "entity_cik": issuer_cik,
                "entry_url": index_url,
                "accepted_at": accepted_at,
                "observed_at": accepted_at,
            }
        ],
    }


def completed_backfill_checkpoint(
    *,
    quarter: str,
    issuer_cik: str,
    accession_number: str,
) -> dict[str, object]:
    return {
        "contract_version": 1,
        "quarter": quarter,
        "issuer_cik": issuer_cik,
        "status": "completed",
        "catalog_url": (
            "https://www.sec.gov/data-research/sec-markets-data/"
            "insider-transactions-data-sets"
        ),
        "zip_url": (
            "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/"
            f"{quarter.lower()}.zip"
        ),
        "zip_sha256": "0" * 64,
        "zip_byte_count": 1,
        "etag": None,
        "last_modified": None,
        "table_evidence": [
            {
                "table_name": "SUBMISSION",
                "headers": ["ACCESSION_NUMBER"],
                "row_count": 1,
            }
        ],
        "missing_optional_tables": [
            "DERIV_HOLDING",
            "DERIV_TRANS",
            "FOOTNOTES",
            "NONDERIV_HOLDING",
            "NONDERIV_TRANS",
            "OWNER_SIGNATURE",
            "REPORTINGOWNER",
        ],
        "selected_accessions": [accession_number],
        "completed_accessions": [accession_number],
        "reconciliation": [],
    }


def seed_repository(
    root: Path,
    case_names: tuple[str, ...],
    *,
    maintenance_case: str,
    maintenance_mode: str = "reparse",
    maintenance_quarter: str | None = None,
) -> tuple[InsiderStateStore, dict[str, dict[str, object]]]:
    storage = InsiderStorage(root)
    state_store = InsiderStateStore(root)
    prepared: list[tuple[str, dict[str, object], bytes, dict[str, object]]] = []
    for case_name in case_names:
        case = ORACLE["filings"][case_name]
        raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
        normalized = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )
        prepared.append((case_name, case, raw_xml, normalized))
    issuer_ciks = sorted(
        {
            normalized["issuer"]["cik"]
            for _, _, _, normalized in prepared
            if isinstance(normalized["issuer"], dict)
        }
    )
    state_store.write(
        "approved-issuers-v1",
        {"contract_version": 1, "issuer_ciks": issuer_ciks},
    )

    records_by_issuer: dict[str, list[NormalizedIssuerRecord]] = {}
    issuer_by_case: dict[str, str] = {}
    for case_name, case, raw_xml, normalized in prepared:
        accession = case["accession_number"]
        assert isinstance(accession, str)
        store_source_prerequisites(storage, accession, raw_xml, case)
        storage.store_normalized(
            accession,
            INSIDER_PARSER_VERSION,
            normalized,
        )
        record = issuer_record_from_normalized(
            normalized,
            parser_version=INSIDER_PARSER_VERSION,
        )
        issuer_by_case[case_name] = record.issuer_cik
        records_by_issuer.setdefault(record.issuer_cik, []).append(record)

    issuer_states: dict[str, dict[str, object]] = {}
    for issuer_cik in sorted(records_by_issuer):
        reduced = reduce_issuer_state(
            issuer_cik=issuer_cik,
            records=records_by_issuer[issuer_cik],
        )
        issuer_states[issuer_cik] = reduced.issuer_state
        state_store.write_issuer_if_approved(issuer_cik, reduced.issuer_state)

    state_store.write("publication-policy-v1", publication_policy(issuer_states))
    maintenance = ORACLE["filings"][maintenance_case]
    maintenance_accession = maintenance["accession_number"]
    assert isinstance(maintenance_accession, str)
    maintenance_issuer_cik = issuer_by_case[maintenance_case]
    if maintenance_mode == "incremental":
        state_store.write_incremental_if_issuers_approved(
            completed_incremental_checkpoint(
                case=maintenance,
                issuer_cik=maintenance_issuer_cik,
            )
        )
    elif maintenance_mode == "backfill":
        assert maintenance_quarter is not None
        state_store.write_backfill_if_issuer_approved(
            maintenance_quarter,
            maintenance_issuer_cik,
            completed_backfill_checkpoint(
                quarter=maintenance_quarter,
                issuer_cik=maintenance_issuer_cik,
                accession_number=maintenance_accession,
            ),
        )
    elif maintenance_mode == "reparse":
        assert maintenance_quarter is None
        state_store.write_reparse_if_issuers_approved(
            completed_reparse_checkpoint(
                issuer_cik=maintenance_issuer_cik,
                accession_number=maintenance_accession,
            )
        )
    else:
        raise AssertionError(
            f"unsupported synthetic maintenance mode: {maintenance_mode}"
        )
    return state_store, issuer_states


def materialize(
    root: Path,
    *,
    maintenance_mode: str = "reparse",
    issuer_cik: str = "0000000001",
    maintenance_quarter: str | None = None,
) -> dict[str, object]:
    module = materializer_module()
    return module.materialize_insider_publication(
        repository_root=root,
        maintenance_mode=maintenance_mode,
        maintenance_issuer_cik=issuer_cik,
        maintenance_quarter=maintenance_quarter,
        maintenance_max_accessions=1,
        as_of=AS_OF,
        latest_successful_sync_at=SYNC_AT,
    )


class InsiderPublicationMaterializerTests(unittest.TestCase):
    def test_policy_state_is_exact_canonical_and_duplicate_key_safe(self) -> None:
        policy = {
            "contract_version": 1,
            "issuers": [
                {
                    "issuer_cik": "0000000001",
                    "security_mappings": {
                        "a" * 64: {
                            "stockId": "03770N101",
                            "fileStem": "03770N101",
                            "ticker": "ONE",
                            "companyName": "Synthetic Issuer One",
                            "securityType": "Common Stock",
                            "securityTypeLabel": "COMMON STOCK",
                            "cusip": "03770N101",
                            "primary": True,
                        }
                    },
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_store = InsiderStateStore(root)
            artifact = state_store.write("publication-policy-v1", policy)
            self.assertTrue(artifact.created)
            self.assertEqual(policy, state_store.read("publication-policy-v1"))

        invalid_bytes = (
            b'{"contract_version":1,"contract_version":1,"issuers":[]}\n',
            json.dumps(policy, indent=2, sort_keys=True).encode() + b"\n",
        )
        for rendered in invalid_bytes:
            with (
                self.subTest(rendered=rendered[:40]),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                root = Path(tmpdir)
                state_store = InsiderStateStore(root)
                state_store.write(
                    "approved-issuers-v1",
                    {"contract_version": 1, "issuer_ciks": ["0000000001"]},
                )
                policy_path = (
                    root / "data/insiders/private/state/publication-policy-v1.json"
                )
                policy_path.write_bytes(rendered)
                with self.assertRaises(InsiderStorageError):
                    state_store.read("publication-policy-v1")

    def test_two_issuer_policy_materializes_one_complete_atomic_publication(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            seed_repository(
                root,
                ("form4_simple_purchase", "form5_annual"),
                maintenance_case="form4_simple_purchase",
            )
            private_sentinel = root / "data/insiders/private/private-sentinel.txt"
            private_sentinel.write_text("123 Main St private@example.test")
            module = materializer_module()

            with mock.patch.object(
                module,
                "write_insider_publication",
                wraps=module.write_insider_publication,
            ) as writer:
                result = materialize(root)

            writer.assert_called_once()
            self.assertEqual(
                ["0000000001", "0000000007"],
                result["issuerCiks"],
            )
            public_root = root / "data/insiders/public"
            self.assertEqual([], validate_insider_public_tree(public_root))
            public_bytes = b"".join(
                path.read_bytes() for path in sorted(public_root.rglob("*.json"))
            )
            self.assertNotIn(b"123 Main St", public_bytes)
            self.assertNotIn(b"private@example.test", public_bytes)
            self.assertEqual(
                ["0000000001", "0000000007"],
                json.loads((public_root / "manifest.json").read_text())["issuerCiks"],
            )

    def test_materializer_does_not_reparse_verified_private_sources(self) -> None:
        parser_targets = (
            "insider_storage.raw_ownership_document",
            "insider_storage.parse_insider_filing_index",
            "insider_storage.parse_ownership_xml",
        )
        for parser_target in parser_targets:
            with (
                self.subTest(parser_target=parser_target),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                root = Path(tmpdir)
                seed_repository(
                    root,
                    ("form4_simple_purchase",),
                    maintenance_case="form4_simple_purchase",
                )

                with mock.patch(
                    parser_target,
                    side_effect=AssertionError(
                        "materializer reparsed a private source"
                    ),
                ) as parser:
                    result = materialize(root)

                parser.assert_not_called()
                self.assertEqual(["0000000001"], result["issuerCiks"])
                self.assertEqual(
                    [], validate_insider_public_tree(root / "data/insiders/public")
                )

    def test_all_checkpoint_modes_materialize_the_exact_bound_scope(self) -> None:
        tree_sha256_values: set[str] = set()
        for maintenance_mode, maintenance_quarter in (
            ("incremental", None),
            ("backfill", "2026Q1"),
            ("reparse", None),
        ):
            with (
                self.subTest(maintenance_mode=maintenance_mode),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                root = Path(tmpdir)
                seed_repository(
                    root,
                    ("form4_simple_purchase",),
                    maintenance_case="form4_simple_purchase",
                    maintenance_mode=maintenance_mode,
                    maintenance_quarter=maintenance_quarter,
                )

                result = materialize(
                    root,
                    maintenance_mode=maintenance_mode,
                    maintenance_quarter=maintenance_quarter,
                )

                self.assertEqual(["0000000001"], result["issuerCiks"])
                self.assertEqual(
                    [], validate_insider_public_tree(root / "data/insiders/public")
                )
                tree_sha256 = result["treeSha256"]
                assert isinstance(tree_sha256, str)
                tree_sha256_values.add(tree_sha256)
        self.assertEqual(1, len(tree_sha256_values))

    def test_all_checkpoint_modes_reject_mismatched_scope_before_public_write(
        self,
    ) -> None:
        for maintenance_mode, maintenance_quarter in (
            ("incremental", None),
            ("backfill", "2026Q1"),
            ("reparse", None),
        ):
            with (
                self.subTest(maintenance_mode=maintenance_mode),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                root = Path(tmpdir)
                seed_repository(
                    root,
                    ("form4_simple_purchase",),
                    maintenance_case="form4_simple_purchase",
                    maintenance_mode=maintenance_mode,
                    maintenance_quarter=maintenance_quarter,
                )
                module = materializer_module()

                with mock.patch.object(module, "write_insider_publication") as writer:
                    with self.assertRaises(
                        module.InsiderPublicationMaterializationError
                    ):
                        materialize(
                            root,
                            maintenance_mode=maintenance_mode,
                            issuer_cik="0000000007",
                            maintenance_quarter=maintenance_quarter,
                        )
                writer.assert_not_called()

    def test_empty_incremental_checkpoint_is_rejected_before_public_write(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_store, _ = seed_repository(
                root,
                ("form4_simple_purchase", "form5_annual"),
                maintenance_case="form4_simple_purchase",
                maintenance_mode="incremental",
            )
            state_store.update(
                "incremental-v1",
                lambda _: {
                    "contract_version": 1,
                    "status": "completed",
                    "lookback_seconds": 3600,
                    "first_observed_at": None,
                    "last_observed_at": None,
                    "queue": [],
                    "completed_accessions": [],
                    "source_entries": [],
                },
            )
            module = materializer_module()

            with mock.patch.object(
                module,
                "write_insider_publication",
                return_value={},
            ) as writer:
                with self.assertRaisesRegex(
                    module.InsiderPublicationMaterializationError,
                    "maintenance checkpoint is not completed and exact",
                ):
                    materialize(
                        root,
                        maintenance_mode="incremental",
                        issuer_cik="0000000007",
                    )
            writer.assert_not_called()

    def test_aggregate_normalized_accession_budget_fails_before_public_write(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            seed_repository(
                root,
                ("form4_simple_purchase", "form5_annual"),
                maintenance_case="form4_simple_purchase",
            )
            module = materializer_module()

            with (
                mock.patch.object(module, "_MAX_MATERIALIZATION_ACCESSIONS", 1),
                mock.patch.object(module, "write_insider_publication") as writer,
            ):
                with self.assertRaisesRegex(
                    module.InsiderPublicationMaterializationError,
                    "normalized accession budget",
                ):
                    materialize(root)
            writer.assert_not_called()

    def test_aggregate_normalized_byte_budget_fails_before_public_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            seed_repository(
                root,
                ("form4_simple_purchase",),
                maintenance_case="form4_simple_purchase",
            )
            module = materializer_module()

            with (
                mock.patch.object(module, "_MAX_MATERIALIZATION_NORMALIZED_BYTES", 1),
                mock.patch.object(module, "write_insider_publication") as writer,
            ):
                with self.assertRaisesRegex(
                    module.InsiderPublicationMaterializationError,
                    "normalized byte budget",
                ):
                    materialize(root)
            writer.assert_not_called()

    def test_normalized_hash_mismatch_fails_before_public_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_store, _ = seed_repository(
                root,
                ("form4_simple_purchase",),
                maintenance_case="form4_simple_purchase",
            )
            current = state_store.read("issuers/0000000001")
            invalid = copy.deepcopy(current)
            accessions = invalid["accessions"]
            amendments = invalid["amendments"]
            assert isinstance(accessions, list) and isinstance(amendments, list)
            accessions[0]["normalized_sha256"] = "0" * 64
            resolutions = {
                item["accession_number"]: {
                    "amends_accession": item["amends_accession"],
                    "confidence": item["confidence"],
                    "reason_code": item["reason_code"],
                    "candidates": item["candidates"],
                }
                for item in amendments
            }
            invalid["generation_digest"] = issuer_generation_digest(
                [
                    {
                        **entry,
                        "amendment_resolution": resolutions.get(
                            entry["accession_number"]
                        ),
                    }
                    for entry in accessions
                ]
            )
            state_store.write_issuer_if_approved(
                "0000000001",
                invalid,
                expected_sha256=hashlib.sha256(
                    canonical_insider_state_json_bytes(current)
                ).hexdigest(),
            )
            module = materializer_module()

            with mock.patch.object(module, "write_insider_publication") as writer:
                with self.assertRaisesRegex(
                    module.InsiderPublicationMaterializationError,
                    "normalized filing binding",
                ):
                    materialize(root)
            writer.assert_not_called()

    def test_incomplete_checkpoint_fails_before_public_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_store, _ = seed_repository(
                root,
                ("form4_simple_purchase",),
                maintenance_case="form4_simple_purchase",
            )
            current = state_store.read("reparse-v1")
            incomplete = {**current, "status": "incomplete", "completed_accessions": []}
            state_store.write_reparse_if_issuers_approved(
                incomplete,
                expected_sha256=hashlib.sha256(
                    canonical_insider_state_json_bytes(current)
                ).hexdigest(),
            )
            module = materializer_module()

            with mock.patch.object(module, "write_insider_publication") as writer:
                with self.assertRaisesRegex(
                    module.InsiderPublicationMaterializationError,
                    "maintenance checkpoint",
                ):
                    materialize(root)
            writer.assert_not_called()

    def test_incomplete_security_mapping_fails_before_public_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_store, issuer_states = seed_repository(
                root,
                ("form3_holdings_only",),
                maintenance_case="form3_holdings_only",
            )
            policy = publication_policy(issuer_states)
            issuers = policy["issuers"]
            assert isinstance(issuers, list)
            issuer = issuers[0]
            assert isinstance(issuer, dict)
            mappings = issuer["security_mappings"]
            assert isinstance(mappings, dict) and len(mappings) > 1
            issuer["security_mappings"] = {
                next(iter(sorted(mappings))): mappings[next(iter(sorted(mappings)))]
            }
            current = state_store.read("publication-policy-v1")
            state_store.write(
                "publication-policy-v1",
                policy,
                expected_sha256=hashlib.sha256(
                    canonical_insider_state_json_bytes(current)
                ).hexdigest(),
            )
            module = materializer_module()

            with mock.patch.object(module, "write_insider_publication") as writer:
                with self.assertRaisesRegex(
                    module.InsiderPublicationMaterializationError,
                    "security mapping",
                ):
                    materialize(root, issuer_cik="0000000005")
            writer.assert_not_called()

    def test_policy_outside_approved_scope_fails_before_public_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_store, _ = seed_repository(
                root,
                ("form4_simple_purchase", "form5_annual"),
                maintenance_case="form4_simple_purchase",
            )
            approved = state_store.read("approved-issuers-v1")
            state_store.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
                expected_sha256=hashlib.sha256(
                    canonical_insider_state_json_bytes(approved)
                ).hexdigest(),
            )
            module = materializer_module()

            with mock.patch.object(module, "write_insider_publication") as writer:
                with self.assertRaisesRegex(
                    module.InsiderPublicationMaterializationError,
                    "approved issuer scope",
                ):
                    materialize(root)
            writer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
