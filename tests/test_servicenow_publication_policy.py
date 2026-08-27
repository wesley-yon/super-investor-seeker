from __future__ import annotations

from copy import deepcopy
import hashlib
import unittest

from insider_storage import (
    canonical_insider_state_json_bytes,
    issuer_generation_digest,
    validate_issuer_state_payload,
)
from insider_publication_policy import (
    ServiceNowPublicationPolicyError,
    build_servicenow_publication_policy,
    publication_policy_sha256,
)
from security_identity import section16_security_class_key, stock_file_stem


SERVICENOW_CIK = "0001373715"
CLASS_TITLE = "COMMON STOCK"
CLASS_KEY = section16_security_class_key(
    SERVICENOW_CIK,
    CLASS_TITLE,
    is_derivative=False,
)


def public_identity_fixture(*, stock_id: str = "81762P102") -> dict[str, object]:
    return {
        "stockId": stock_id,
        "fileStem": stock_id,
        "ticker": "SYN",
        "companyName": "Synthetic ServiceNow",
        "securityType": "Common Stock",
        "securityTypeLabel": "COMMON STOCK",
        "cusip": stock_id,
        "primary": True,
    }


def public_index_fixture(*, stock_id: str = "81762P102") -> dict[str, object]:
    identity = public_identity_fixture(stock_id=stock_id)
    return {stock_id: identity}


def issuer_state_fixture(
    *,
    issuer_cik: str = SERVICENOW_CIK,
    class_keys: tuple[str, ...] = (CLASS_KEY,),
    security_classes: tuple[tuple[str, bool], ...] | None = None,
    unresolved_ambiguities: list[object] | None = None,
) -> dict[str, object]:
    accessions = [
        {
            "accession_number": "0001373715-26-000001",
            "parser_version": "test-v1",
            "normalized_sha256": hashlib.sha256(b"canonical test filing").hexdigest(),
        }
    ]
    classes = (
        [
            {
                "security_class_key": section16_security_class_key(
                    issuer_cik,
                    title,
                    is_derivative=derivative,
                ),
                "derivative": derivative,
                "title": title,
            }
            for title, derivative in security_classes
        ]
        if security_classes is not None
        else [
            {
                "security_class_key": class_key,
                "derivative": False,
                "title": CLASS_TITLE,
            }
            for class_key in class_keys
        ]
    )
    classes.sort(key=lambda item: str(item["security_class_key"]))
    state = {
        "contract_version": 1,
        "issuer_cik": issuer_cik,
        "accessions": accessions,
        "owner_groups": [],
        "security_classes": classes,
        "amendments": [],
        "unresolved_ambiguities": (
            [] if unresolved_ambiguities is None else unresolved_ambiguities
        ),
        "generation_digest": issuer_generation_digest(
            [
                {
                    **accession,
                    "amendment_resolution": None,
                }
                for accession in accessions
            ]
        ),
    }
    return state


class ServiceNowPublicationPolicyTests(unittest.TestCase):
    def test_builds_exact_servicenow_candidate_for_complete_explicit_mapping(
        self,
    ) -> None:
        issuer_state = issuer_state_fixture()
        mapping_spec = {CLASS_KEY: public_identity_fixture()}

        candidate = build_servicenow_publication_policy(
            issuer_state=issuer_state,
            mapping_spec=mapping_spec,
            public_index=public_index_fixture(),
        )

        self.assertEqual(
            {
                "contract_version": 1,
                "issuers": [
                    {
                        "issuer_cik": SERVICENOW_CIK,
                        "security_mappings": mapping_spec,
                    }
                ],
            },
            candidate,
        )

    def test_rejects_foreign_or_noncanonical_issuer_cik(self) -> None:
        for issuer_cik in ("0001067983", "1373715", " 0001373715", True):
            with self.subTest(issuer_cik=issuer_cik):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state_fixture(issuer_cik=issuer_cik),
                        mapping_spec={CLASS_KEY: public_identity_fixture()},
                        public_index=public_index_fixture(),
                    )

    def test_rejects_missing_duplicate_malformed_or_extra_class_keys(self) -> None:
        second_key = "d" * 64
        cases = (
            (
                issuer_state_fixture(),
                {},
                public_index_fixture(),
            ),
            (
                issuer_state_fixture(class_keys=(CLASS_KEY, CLASS_KEY)),
                {CLASS_KEY: public_identity_fixture()},
                public_index_fixture(),
            ),
            (
                issuer_state_fixture(class_keys=("not-a-class-key",)),
                {"not-a-class-key": public_identity_fixture()},
                public_index_fixture(),
            ),
            (
                issuer_state_fixture(),
                {
                    CLASS_KEY: public_identity_fixture(),
                    second_key: public_identity_fixture(),
                },
                public_index_fixture(),
            ),
        )
        for issuer_state, mapping_spec, public_index in cases:
            with self.subTest(mapping_spec=mapping_spec):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state,
                        mapping_spec=mapping_spec,
                        public_index=public_index,
                    )

    def test_rejects_nonempty_unresolved_ambiguities(self) -> None:
        issuer_state = issuer_state_fixture()
        accessions = issuer_state["accessions"]
        assert isinstance(accessions, list)
        unresolved_accession = {
            "accession_number": "0001373715-26-000002",
            "parser_version": "test-v1",
            "normalized_sha256": hashlib.sha256(
                b"unresolved canonical filing"
            ).hexdigest(),
        }
        accessions.append(unresolved_accession)
        unresolved_resolution = {
            "amends_accession": None,
            "confidence": "unresolved",
            "reason_code": "no_candidate",
            "candidates": [],
        }
        issuer_state["amendments"] = [
            {
                "accession_number": unresolved_accession["accession_number"],
                **unresolved_resolution,
            }
        ]
        issuer_state["unresolved_ambiguities"] = [
            {
                "accession_number": unresolved_accession["accession_number"],
                "reason_code": "no_candidate",
                "candidates": [],
            }
        ]
        issuer_state["generation_digest"] = issuer_generation_digest(
            [
                {
                    **accession,
                    "amendment_resolution": (
                        unresolved_resolution
                        if accession["accession_number"]
                        == unresolved_accession["accession_number"]
                        else None
                    ),
                }
                for accession in accessions
            ]
        )

        self.assertEqual(issuer_state, validate_issuer_state_payload(issuer_state))
        with self.assertRaises(ServiceNowPublicationPolicyError):
            build_servicenow_publication_policy(
                issuer_state=issuer_state,
                mapping_spec={CLASS_KEY: public_identity_fixture()},
                public_index=public_index_fixture(),
            )

    def test_rejects_empty_or_malformed_issuer_generation_state(self) -> None:
        complete = issuer_state_fixture()
        malformed = {
            key: value for key, value in complete.items() if key != "generation_digest"
        }
        for issuer_state in ({**complete, "accessions": []}, malformed):
            with self.subTest(issuer_state=issuer_state):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state,
                        mapping_spec={CLASS_KEY: public_identity_fixture()},
                        public_index=public_index_fixture(),
                    )

    def test_requires_an_exact_public_index_target_identity(self) -> None:
        expected = public_identity_fixture()
        wrong_ticker = {**expected, "ticker": "WRONG"}
        cases = (
            ({CLASS_KEY: wrong_ticker}, {expected["stockId"]: expected}),
            ({CLASS_KEY: expected}, {}),
        )
        for mapping_spec, public_index in cases:
            with self.subTest(mapping_spec=mapping_spec, public_index=public_index):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state_fixture(),
                        mapping_spec=mapping_spec,
                        public_index=public_index,
                    )

    def test_rejects_distinct_classes_mapped_to_one_public_identity(self) -> None:
        issuer_state = issuer_state_fixture(
            security_classes=(("COMMON STOCK", False), ("RESTRICTED STOCK", False))
        )
        classes = issuer_state["security_classes"]
        assert isinstance(classes, list) and len(classes) == 2
        class_keys = [entry["security_class_key"] for entry in classes]
        self.assertEqual(2, len(set(class_keys)))
        metadata = public_identity_fixture()

        with self.assertRaises(ServiceNowPublicationPolicyError):
            build_servicenow_publication_policy(
                issuer_state=issuer_state,
                mapping_spec={class_key: metadata for class_key in class_keys},
                public_index={metadata["stockId"]: metadata},
            )

    def test_policy_digest_rejects_non_servicenow_candidate_contracts(self) -> None:
        cases = (
            {"contract_version": 1, "issuers": []},
            {"contract_version": 1, "issuers": [], "unexpected": None},
            {
                "contract_version": 1,
                "issuers": [
                    {
                        "issuer_cik": "0001067983",
                        "security_mappings": {CLASS_KEY: public_identity_fixture()},
                    }
                ],
            },
        )
        for policy in cases:
            with self.subTest(policy=policy):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    publication_policy_sha256(policy)

    def test_rejects_corrupt_or_incomplete_canonical_issuer_state(self) -> None:
        wrong_digest = issuer_state_fixture()
        wrong_digest["generation_digest"] = "d" * 64

        duplicate_accession = issuer_state_fixture()
        accessions = duplicate_accession["accessions"]
        assert isinstance(accessions, list)
        accessions.append(deepcopy(accessions[0]))

        incomplete_class = issuer_state_fixture()
        incomplete_class["security_classes"] = [{"security_class_key": CLASS_KEY}]

        for issuer_state in (wrong_digest, duplicate_accession, incomplete_class):
            with self.subTest(issuer_state=issuer_state):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state,
                        mapping_spec={CLASS_KEY: public_identity_fixture()},
                        public_index=public_index_fixture(),
                    )

    def test_rejects_unsafe_noncanonical_stock_identity(self) -> None:
        metadata = public_identity_fixture(stock_id="../81762P102")
        metadata["fileStem"] = stock_file_stem(metadata["stockId"])

        with self.assertRaises(ServiceNowPublicationPolicyError):
            build_servicenow_publication_policy(
                issuer_state=issuer_state_fixture(),
                mapping_spec={CLASS_KEY: metadata},
                public_index={metadata["stockId"]: metadata},
            )

    def test_rejects_cik_or_private_correlator_in_public_identity_fields(
        self,
    ) -> None:
        issuer_state = issuer_state_fixture()
        accessions = issuer_state["accessions"]
        assert isinstance(accessions, list) and len(accessions) == 1
        accession = accessions[0]
        assert isinstance(accession, dict)
        normalized_sha256 = accession["normalized_sha256"]
        assert isinstance(normalized_sha256, str)
        private_correlator = normalized_sha256.upper()
        private_stock_identity = public_identity_fixture(stock_id=private_correlator)
        private_stock_identity["fileStem"] = stock_file_stem(private_correlator)
        private_stock_identity["cusip"] = "81762P102"

        cases = (
            ("CIK stock identity", public_identity_fixture(stock_id=SERVICENOW_CIK)),
            ("private correlator stock identity", private_stock_identity),
            ("CIK CUSIP", {**public_identity_fixture(), "cusip": SERVICENOW_CIK}),
        )
        for label, metadata in cases:
            stock_id = metadata["stockId"]
            assert isinstance(stock_id, str)
            with self.subTest(label=label, metadata=metadata):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state,
                        mapping_spec={CLASS_KEY: metadata},
                        public_index={stock_id: metadata},
                    )

    def test_allows_ordinary_public_company_words(self) -> None:
        metadata = public_identity_fixture()
        metadata["companyName"] = "Private Bancorp"

        candidate = build_servicenow_publication_policy(
            issuer_state=issuer_state_fixture(),
            mapping_spec={CLASS_KEY: metadata},
            public_index={metadata["stockId"]: metadata},
        )

        self.assertEqual(
            metadata,
            candidate["issuers"][0]["security_mappings"][CLASS_KEY],
        )

    def test_rejects_malformed_owner_groups_or_amendments(self) -> None:
        malformed_owner_groups = issuer_state_fixture()
        malformed_owner_groups["owner_groups"] = [{"owner_group_key": CLASS_KEY}]
        malformed_amendments = issuer_state_fixture()
        malformed_amendments["amendments"] = [
            {"accession_number": "0001373715-26-000001"}
        ]

        for issuer_state in (malformed_owner_groups, malformed_amendments):
            with self.subTest(issuer_state=issuer_state):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state,
                        mapping_spec={CLASS_KEY: public_identity_fixture()},
                        public_index=public_index_fixture(),
                    )

    def test_rejects_forged_derived_security_class_key(self) -> None:
        issuer_state = issuer_state_fixture()
        issuer_state["security_classes"] = [
            {
                "security_class_key": "a" * 64,
                "derivative": False,
                "title": CLASS_TITLE,
            }
        ]

        with self.assertRaises(ServiceNowPublicationPolicyError):
            build_servicenow_publication_policy(
                issuer_state=issuer_state,
                mapping_spec={"a" * 64: public_identity_fixture()},
                public_index=public_index_fixture(),
            )

    def test_rejects_reversed_duplicate_or_invalid_accessions(self) -> None:
        reversed_accessions = issuer_state_fixture()
        accessions = reversed_accessions["accessions"]
        assert isinstance(accessions, list)
        accessions.append(
            {
                "accession_number": "0001373715-26-000002",
                "parser_version": "test-v1",
                "normalized_sha256": hashlib.sha256(
                    b"second canonical filing"
                ).hexdigest(),
            }
        )
        reversed_accessions["generation_digest"] = issuer_generation_digest(
            [{**accession, "amendment_resolution": None} for accession in accessions]
        )
        accessions.reverse()

        duplicate_accessions = issuer_state_fixture()
        duplicate = duplicate_accessions["accessions"]
        assert isinstance(duplicate, list)
        duplicate.append(deepcopy(duplicate[0]))
        duplicate_accessions["generation_digest"] = issuer_generation_digest(
            [{**accession, "amendment_resolution": None} for accession in duplicate]
        )

        invalid_parser = issuer_state_fixture()
        invalid_accessions = invalid_parser["accessions"]
        assert isinstance(invalid_accessions, list)
        invalid_accessions[0]["parser_version"] = "not a parser version!"
        invalid_parser["generation_digest"] = issuer_generation_digest(
            [
                {**accession, "amendment_resolution": None}
                for accession in invalid_accessions
            ]
        )

        for issuer_state in (reversed_accessions, duplicate_accessions, invalid_parser):
            with self.subTest(issuer_state=issuer_state):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state,
                        mapping_spec={CLASS_KEY: public_identity_fixture()},
                        public_index=public_index_fixture(),
                    )

    def test_rejects_invalid_public_security_metadata_grammar(self) -> None:
        invalid_metadata = (
            ("stockId", "not a canonical stock id!"),
            ("ticker", "also not a ticker?"),
            ("cusip", "definitely not a CUSIP"),
            ("companyName", "CIK 0001373715"),
            ("securityType", "CIK 0001373715"),
            ("securityTypeLabel", "CIK 0001373715"),
        )
        for field, value in invalid_metadata:
            with self.subTest(field=field):
                metadata = public_identity_fixture()
                metadata[field] = value
                if field == "stockId":
                    assert isinstance(metadata["stockId"], str)
                    metadata["fileStem"] = stock_file_stem(metadata["stockId"])
                stock_id = metadata["stockId"]
                assert isinstance(stock_id, str)
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state_fixture(),
                        mapping_spec={CLASS_KEY: metadata},
                        public_index={stock_id: metadata},
                    )

    def test_validates_indexed_target_before_exact_comparison(self) -> None:
        invalid_indexed_target = public_identity_fixture()
        invalid_indexed_target["ticker"] = "not a ticker?"
        stock_id = invalid_indexed_target["stockId"]
        assert isinstance(stock_id, str)

        with self.assertRaises(ServiceNowPublicationPolicyError):
            build_servicenow_publication_policy(
                issuer_state=issuer_state_fixture(),
                mapping_spec={CLASS_KEY: public_identity_fixture()},
                public_index={stock_id: invalid_indexed_target},
            )

    def test_rejects_collection_bounds_non_json_scalars_and_unknown_keys(self) -> None:
        no_security_classes = issuer_state_fixture()
        no_security_classes["security_classes"] = []
        too_many_accessions = issuer_state_fixture()
        many_accessions = too_many_accessions["accessions"]
        assert isinstance(many_accessions, list)
        many_accessions *= 1_001
        unknown_issuer_key = {**issuer_state_fixture(), "unexpected": None}
        boolean_contract_version = {**issuer_state_fixture(), "contract_version": True}
        nan_metadata = public_identity_fixture()
        nan_metadata["companyName"] = float("nan")
        integer_primary = public_identity_fixture()
        integer_primary["primary"] = 1
        unknown_metadata_key = {**public_identity_fixture(), "unexpected": None}

        cases = (
            (
                no_security_classes,
                {CLASS_KEY: public_identity_fixture()},
                public_index_fixture(),
            ),
            (
                too_many_accessions,
                {CLASS_KEY: public_identity_fixture()},
                public_index_fixture(),
            ),
            (
                unknown_issuer_key,
                {CLASS_KEY: public_identity_fixture()},
                public_index_fixture(),
            ),
            (
                boolean_contract_version,
                {CLASS_KEY: public_identity_fixture()},
                public_index_fixture(),
            ),
            (
                issuer_state_fixture(),
                {CLASS_KEY: nan_metadata},
                {"81762P102": nan_metadata},
            ),
            (
                issuer_state_fixture(),
                {CLASS_KEY: integer_primary},
                {"81762P102": integer_primary},
            ),
            (
                issuer_state_fixture(),
                {CLASS_KEY: unknown_metadata_key},
                {"81762P102": unknown_metadata_key},
            ),
        )
        for issuer_state, mapping_spec, public_index in cases:
            with self.subTest(mapping_spec=mapping_spec):
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state,
                        mapping_spec=mapping_spec,
                        public_index=public_index,
                    )

    def test_rejects_noncanonical_or_control_public_text_and_private_correlator(
        self,
    ) -> None:
        for value in ("Service\u200bNow", "Service\x01Now", "Service\u212a", "a" * 64):
            with self.subTest(value=value):
                metadata = public_identity_fixture()
                metadata["companyName"] = value
                stock_id = metadata["stockId"]
                assert isinstance(stock_id, str)
                with self.assertRaises(ServiceNowPublicationPolicyError):
                    build_servicenow_publication_policy(
                        issuer_state=issuer_state_fixture(),
                        mapping_spec={CLASS_KEY: metadata},
                        public_index={stock_id: metadata},
                    )

    def test_canonical_policy_bytes_and_digest_are_insertion_order_independent(
        self,
    ) -> None:
        issuer_state = issuer_state_fixture(
            security_classes=(("COMMON STOCK", False), ("RESTRICTED STOCK", False))
        )
        classes = issuer_state["security_classes"]
        assert isinstance(classes, list) and len(classes) == 2
        first_class, second_class = classes
        assert isinstance(first_class, dict) and isinstance(second_class, dict)
        first_key = first_class["security_class_key"]
        second_key = second_class["security_class_key"]
        assert isinstance(first_key, str) and isinstance(second_key, str)
        first_metadata = public_identity_fixture(stock_id="81762P102")
        second_metadata = public_identity_fixture(stock_id="81762P103")
        first_order = {first_key: first_metadata, second_key: second_metadata}
        second_order = {second_key: second_metadata, first_key: first_metadata}
        public_index = {
            "81762P103": second_metadata,
            "81762P102": first_metadata,
        }

        first_policy = build_servicenow_publication_policy(
            issuer_state=issuer_state,
            mapping_spec=first_order,
            public_index=public_index,
        )
        second_policy = build_servicenow_publication_policy(
            issuer_state=issuer_state,
            mapping_spec=second_order,
            public_index=public_index,
        )
        expected_mappings = b",".join(
            b'"'
            + class_key.encode()
            + b'":{"companyName":"Synthetic ServiceNow","cusip":"'
            + stock_id.encode()
            + b'","fileStem":"'
            + stock_id.encode()
            + b'","primary":true,"securityType":"Common Stock",'
            b'"securityTypeLabel":"COMMON STOCK","stockId":"'
            + stock_id.encode()
            + b'","ticker":"SYN"}'
            for class_key, metadata in sorted(first_order.items())
            for stock_id in (metadata["stockId"],)
            if isinstance(stock_id, str)
        )
        expected_bytes = (
            b'{"contract_version":1,"issuers":[{"issuer_cik":"0001373715",'
            b'"security_mappings":{' + expected_mappings + b"}}]}\n"
        )
        self.assertEqual(
            expected_bytes, canonical_insider_state_json_bytes(first_policy)
        )
        self.assertEqual(first_policy, second_policy)
        self.assertEqual(
            hashlib.sha256(expected_bytes).hexdigest(),
            publication_policy_sha256(first_policy),
        )
        self.assertEqual(
            publication_policy_sha256(first_policy),
            publication_policy_sha256(second_policy),
        )

        changed_metadata = {**second_metadata, "companyName": "Different ServiceNow"}
        changed_policy = build_servicenow_publication_policy(
            issuer_state=issuer_state,
            mapping_spec={first_key: first_metadata, second_key: changed_metadata},
            public_index={"81762P102": first_metadata, "81762P103": changed_metadata},
        )
        self.assertNotEqual(
            publication_policy_sha256(first_policy),
            publication_policy_sha256(changed_policy),
        )


if __name__ == "__main__":
    unittest.main()
