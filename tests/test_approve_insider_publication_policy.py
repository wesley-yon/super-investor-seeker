from __future__ import annotations

from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from insider_publication_policy import ServiceNowPublicationPolicyError
from insider_storage import (
    InsiderStateRevisionError,
    InsiderStateStore,
    StoredArtifact,
    canonical_insider_state_json_bytes,
    issuer_generation_digest,
)
from security_identity import section16_security_class_key

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "approve_insider_publication_policy.py"
SERVICENOW_CIK = "0001373715"
ACCESSION = "0001373715-26-000001"
CLASS_TITLE = "COMMON STOCK"
CLASS_KEY = section16_security_class_key(
    SERVICENOW_CIK,
    CLASS_TITLE,
    is_derivative=False,
)


def approval_module():
    spec = importlib.util.spec_from_file_location(
        "approve_insider_publication_policy",
        SCRIPT,
    )
    if spec is None or spec.loader is None:
        raise AssertionError("publication-policy approval script is not importable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def issuer_state_payload() -> dict[str, object]:
    normalized_sha256 = hashlib.sha256(b"ServiceNow approval CLI fixture").hexdigest()
    accessions = [
        {
            "accession_number": ACCESSION,
            "parser_version": "test-v1",
            "normalized_sha256": normalized_sha256,
        }
    ]
    generation_material = [
        {
            **accessions[0],
            "amendment_resolution": None,
        }
    ]
    return {
        "contract_version": 1,
        "issuer_cik": SERVICENOW_CIK,
        "accessions": accessions,
        "owner_groups": [],
        "security_classes": [
            {
                "security_class_key": CLASS_KEY,
                "derivative": False,
                "title": CLASS_TITLE,
            }
        ],
        "amendments": [],
        "unresolved_ambiguities": [],
        "generation_digest": issuer_generation_digest(generation_material),
    }


def candidate_policy() -> dict[str, object]:
    return {
        "contract_version": 1,
        "issuers": [
            {
                "issuer_cik": SERVICENOW_CIK,
                "security_mappings": {
                    CLASS_KEY: {
                        "stockId": "81762P102",
                        "fileStem": "81762P102",
                        "ticker": "NOW",
                        "companyName": "Synthetic ServiceNow",
                        "securityType": "Common Stock",
                        "securityTypeLabel": CLASS_TITLE,
                        "cusip": "81762P102",
                        "primary": True,
                    }
                },
            }
        ],
    }


def policy_sha256(policy: object) -> str:
    return hashlib.sha256(canonical_insider_state_json_bytes(policy)).hexdigest()


class ApproveInsiderPublicationPolicyTests(unittest.TestCase):
    def test_approves_exact_candidate_from_exact_empty_policy(self) -> None:
        self.assertTrue(
            SCRIPT.is_file(), "publication-policy approval script is missing"
        )
        module = approval_module()
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        candidate = candidate_policy()
        candidate_sha256 = policy_sha256(candidate)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [SERVICENOW_CIK]},
            )
            state.write_issuer_if_approved(SERVICENOW_CIK, issuer_state)
            empty_policy = state.write(
                "publication-policy-v1",
                {"contract_version": 1, "issuers": []},
            )

            result = module.approve_servicenow_publication_policy(
                repository_root=root,
                candidate_policy=candidate,
                expected_current_policy_sha256=empty_policy.sha256,
                expected_issuer_generation_digest=generation_digest,
                expected_candidate_policy_sha256=candidate_sha256,
            )

            self.assertTrue(result["changed"])
            self.assertEqual(SERVICENOW_CIK, result["issuer_cik"])
            self.assertEqual(1, result["security_class_count"])
            self.assertEqual(candidate_sha256, result["publication_policy_sha256"])
            self.assertEqual(candidate, state.read("publication-policy-v1"))

    def test_wrong_candidate_digest_is_rejected_before_store_construction(self) -> None:
        module = approval_module()
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        with (
            mock.patch.object(
                module,
                "InsiderStateStore",
                side_effect=AssertionError("store must not be constructed"),
            ),
            self.assertRaises(InsiderStateRevisionError),
        ):
            module.approve_servicenow_publication_policy(
                repository_root=Path("/not-used"),
                candidate_policy=candidate_policy(),
                expected_current_policy_sha256="0" * 64,
                expected_issuer_generation_digest=generation_digest,
                expected_candidate_policy_sha256="f" * 64,
            )

    def test_rejects_foreign_or_noncanonical_candidate_before_state_mutation(
        self,
    ) -> None:
        module = approval_module()
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        cases = []
        foreign = json.loads(json.dumps(candidate_policy()))
        foreign_issuers = foreign["issuers"]
        assert isinstance(foreign_issuers, list) and len(foreign_issuers) == 1
        foreign_row = foreign_issuers[0]
        assert isinstance(foreign_row, dict)
        foreign_row["issuer_cik"] = "0001652044"
        cases.append(foreign)
        missing = json.loads(json.dumps(candidate_policy()))
        missing_issuers = missing["issuers"]
        assert isinstance(missing_issuers, list) and len(missing_issuers) == 1
        missing_row = missing_issuers[0]
        assert isinstance(missing_row, dict)
        missing_row["security_mappings"] = {}
        cases.append(missing)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [SERVICENOW_CIK]},
            )
            state.write_issuer_if_approved(SERVICENOW_CIK, issuer_state)
            empty_policy = state.write(
                "publication-policy-v1",
                {"contract_version": 1, "issuers": []},
            )
            rendered = empty_policy.path.read_bytes()
            for candidate in cases:
                with (
                    self.subTest(candidate=candidate),
                    self.assertRaises(ServiceNowPublicationPolicyError),
                ):
                    module.approve_servicenow_publication_policy(
                        repository_root=root,
                        candidate_policy=candidate,
                        expected_current_policy_sha256=empty_policy.sha256,
                        expected_issuer_generation_digest=generation_digest,
                        expected_candidate_policy_sha256=policy_sha256(candidate),
                    )
                self.assertEqual(rendered, empty_policy.path.read_bytes())

    def test_invalid_digest_grammar_is_rejected_before_store_construction(self) -> None:
        module = approval_module()
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        candidate = candidate_policy()
        candidate_sha256 = policy_sha256(candidate)
        cases = (
            ("A" * 64, generation_digest, candidate_sha256),
            ("0" * 64, "0" * 63, candidate_sha256),
            ("0" * 64, generation_digest, True),
        )
        with mock.patch.object(
            module,
            "InsiderStateStore",
            side_effect=AssertionError("store must not be constructed"),
        ):
            for current_sha, generation_sha, candidate_sha in cases:
                with self.subTest(case=(current_sha, generation_sha, candidate_sha)):
                    with self.assertRaises(
                        module.InsiderPublicationPolicyApprovalError
                    ):
                        module.approve_servicenow_publication_policy(
                            repository_root=Path("/not-used"),
                            candidate_policy=candidate,
                            expected_current_policy_sha256=current_sha,
                            expected_issuer_generation_digest=generation_sha,
                            expected_candidate_policy_sha256=candidate_sha,
                        )

    def test_delegates_to_exactly_one_lock_policy_primitive(self) -> None:
        module = approval_module()
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        candidate = candidate_policy()
        candidate_sha256 = policy_sha256(candidate)
        proxy = mock.Mock()
        for method in (
            "read",
            "write",
            "update",
            "publish_if_issuer_approved",
        ):
            getattr(proxy, method).side_effect = AssertionError(
                f"split state operation is forbidden: {method}"
            )
        proxy.approve_publication_policy_for_approved_issuer.return_value = (
            StoredArtifact(
                path=Path("/private/not-exposed.json"),
                sha256=candidate_sha256,
                byte_count=1,
                created=True,
            )
        )
        with mock.patch.object(module, "InsiderStateStore", return_value=proxy):
            result = module.approve_servicenow_publication_policy(
                repository_root=Path("/not-used"),
                candidate_policy=candidate,
                expected_current_policy_sha256="0" * 64,
                expected_issuer_generation_digest=generation_digest,
                expected_candidate_policy_sha256=candidate_sha256,
            )

        proxy.approve_publication_policy_for_approved_issuer.assert_called_once()
        for method in (
            "read",
            "write",
            "update",
            "publish_if_issuer_approved",
        ):
            getattr(proxy, method).assert_not_called()
        self.assertEqual(
            {
                "changed": True,
                "issuer_cik": SERVICENOW_CIK,
                "issuer_generation_digest": generation_digest,
                "publication_policy_sha256": candidate_sha256,
                "security_class_count": 1,
            },
            result,
        )

    def test_exact_current_candidate_is_idempotent_and_preserves_bytes(self) -> None:
        module = approval_module()
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        candidate = candidate_policy()
        candidate_sha256 = policy_sha256(candidate)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [SERVICENOW_CIK]},
            )
            state.write_issuer_if_approved(SERVICENOW_CIK, issuer_state)
            empty_policy = state.write(
                "publication-policy-v1",
                {"contract_version": 1, "issuers": []},
            )
            module.approve_servicenow_publication_policy(
                repository_root=root,
                candidate_policy=candidate,
                expected_current_policy_sha256=empty_policy.sha256,
                expected_issuer_generation_digest=generation_digest,
                expected_candidate_policy_sha256=candidate_sha256,
            )
            rendered = empty_policy.path.read_bytes()

            result = module.approve_servicenow_publication_policy(
                repository_root=root,
                candidate_policy=candidate,
                expected_current_policy_sha256=candidate_sha256,
                expected_issuer_generation_digest=generation_digest,
                expected_candidate_policy_sha256=candidate_sha256,
            )

            self.assertFalse(result["changed"])
            self.assertEqual(rendered, empty_policy.path.read_bytes())

    def test_cli_is_serialized_and_emits_only_bounded_metadata(self) -> None:
        module = approval_module()
        self.assertTrue(hasattr(module.main, "__wrapped__"))
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        candidate = candidate_policy()
        candidate_sha256 = policy_sha256(candidate)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [SERVICENOW_CIK]},
            )
            state.write_issuer_if_approved(SERVICENOW_CIK, issuer_state)
            empty_policy = state.write(
                "publication-policy-v1",
                {"contract_version": 1, "issuers": []},
            )
            candidate_path = root / "candidate.json"
            candidate_path.write_bytes(canonical_insider_state_json_bytes(candidate))
            candidate_path.chmod(0o600)
            output = io.StringIO()

            with redirect_stdout(output):
                status = module.main.__wrapped__(
                    [
                        "--repository-root",
                        os.fspath(root),
                        "--candidate-policy",
                        os.fspath(candidate_path),
                        "--expected-current-policy-sha256",
                        empty_policy.sha256,
                        "--expected-issuer-generation-digest",
                        generation_digest,
                        "--expected-candidate-policy-sha256",
                        candidate_sha256,
                    ]
                )

            self.assertEqual(0, status)
            result = json.loads(output.getvalue())
            self.assertEqual(
                {
                    "changed",
                    "issuer_cik",
                    "issuer_generation_digest",
                    "publication_policy_sha256",
                    "security_class_count",
                },
                set(result),
            )
            self.assertEqual(SERVICENOW_CIK, result["issuer_cik"])
            self.assertNotIn(CLASS_KEY, output.getvalue())
            self.assertNotIn("Synthetic ServiceNow", output.getvalue())
            self.assertNotIn(os.fspath(candidate_path), output.getvalue())
            self.assertEqual(candidate, state.read("publication-policy-v1"))

    def test_cli_rejects_invalid_configuration_without_state_mutation(self) -> None:
        module = approval_module()
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        candidate = candidate_policy()
        candidate_sha256 = policy_sha256(candidate)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [SERVICENOW_CIK]},
            )
            state.write_issuer_if_approved(SERVICENOW_CIK, issuer_state)
            empty_policy = state.write(
                "publication-policy-v1",
                {"contract_version": 1, "issuers": []},
            )
            candidate_path = root / "candidate.json"
            candidate_path.write_bytes(canonical_insider_state_json_bytes(candidate))
            candidate_path.chmod(0o600)
            rendered = empty_policy.path.read_bytes()
            cases = (
                [],
                ["--repository-root", os.fspath(root)],
                [
                    "--repository-root",
                    os.fspath(root),
                    "--candidate-policy",
                    os.fspath(candidate_path),
                    "--expected-current-policy-sha256",
                    "A" * 64,
                    "--expected-issuer-generation-digest",
                    generation_digest,
                    "--expected-candidate-policy-sha256",
                    candidate_sha256,
                ],
                "--repository-root",
            )

            for arguments in cases:
                with self.subTest(arguments=arguments):
                    output = io.StringIO()
                    with redirect_stdout(output):
                        status = module.main.__wrapped__(arguments)
                    self.assertEqual(2, status)
                    self.assertEqual("", output.getvalue())
                    self.assertEqual(rendered, empty_policy.path.read_bytes())

    def test_cli_rejects_unsafe_candidate_files_without_state_mutation(self) -> None:
        module = approval_module()
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        candidate = candidate_policy()
        candidate_sha256 = policy_sha256(candidate)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [SERVICENOW_CIK]},
            )
            state.write_issuer_if_approved(SERVICENOW_CIK, issuer_state)
            empty_policy = state.write(
                "publication-policy-v1",
                {"contract_version": 1, "issuers": []},
            )
            rendered = empty_policy.path.read_bytes()
            canonical = canonical_insider_state_json_bytes(candidate)
            valid = root / "valid.json"
            valid.write_bytes(canonical)
            valid.chmod(0o600)
            unsafe_paths: list[Path] = []

            symlink = root / "symlink.json"
            symlink.symlink_to(valid)
            unsafe_paths.append(symlink)

            permissive = root / "permissive.json"
            permissive.write_bytes(canonical)
            permissive.chmod(0o644)
            unsafe_paths.append(permissive)

            hardlink = root / "hardlink.json"
            os.link(valid, hardlink)
            unsafe_paths.extend((valid, hardlink))

            noncanonical = root / "noncanonical.json"
            noncanonical.write_text(json.dumps(candidate, indent=2))
            noncanonical.chmod(0o600)
            unsafe_paths.append(noncanonical)

            duplicate = root / "duplicate.json"
            duplicate.write_bytes(
                b'{"contract_version":1,"contract_version":1,"issuers":[]}\n'
            )
            duplicate.chmod(0o600)
            unsafe_paths.append(duplicate)

            for candidate_path in unsafe_paths:
                with self.subTest(candidate_path=candidate_path.name):
                    output = io.StringIO()
                    with redirect_stdout(output):
                        status = module.main.__wrapped__(
                            [
                                "--repository-root",
                                os.fspath(root),
                                "--candidate-policy",
                                os.fspath(candidate_path),
                                "--expected-current-policy-sha256",
                                empty_policy.sha256,
                                "--expected-issuer-generation-digest",
                                generation_digest,
                                "--expected-candidate-policy-sha256",
                                candidate_sha256,
                            ]
                        )
                    self.assertEqual(1, status)
                    self.assertEqual("", output.getvalue())
                    self.assertEqual(rendered, empty_policy.path.read_bytes())

    def test_cli_runtime_failure_is_generic_and_omits_sensitive_values(self) -> None:
        module = approval_module()
        issuer_state = issuer_state_payload()
        generation_digest = issuer_state["generation_digest"]
        assert isinstance(generation_digest, str)
        candidate = candidate_policy()
        candidate_sha256 = policy_sha256(candidate)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [SERVICENOW_CIK]},
            )
            state.write_issuer_if_approved(SERVICENOW_CIK, issuer_state)
            empty_policy = state.write(
                "publication-policy-v1",
                {"contract_version": 1, "issuers": []},
            )
            rendered = empty_policy.path.read_bytes()
            candidate_path = root / "private-candidate.json"
            candidate_path.write_bytes(
                b'{"private":"Synthetic ServiceNow /private/path '
                + CLASS_KEY.encode()
                + b'"}\n'
            )
            candidate_path.chmod(0o600)
            output = io.StringIO()

            with (
                mock.patch.object(module.pipeline.log, "error") as error_log,
                redirect_stdout(output),
            ):
                status = module.main.__wrapped__(
                    [
                        "--repository-root",
                        os.fspath(root),
                        "--candidate-policy",
                        os.fspath(candidate_path),
                        "--expected-current-policy-sha256",
                        empty_policy.sha256,
                        "--expected-issuer-generation-digest",
                        generation_digest,
                        "--expected-candidate-policy-sha256",
                        candidate_sha256,
                    ]
                )

            self.assertEqual(1, status)
            self.assertEqual("", output.getvalue())
            error_log.assert_called_once_with(
                "private publication-policy approval failed closed"
            )
            log_text = repr(error_log.call_args)
            self.assertNotIn(CLASS_KEY, log_text)
            self.assertNotIn("Synthetic ServiceNow", log_text)
            self.assertNotIn(os.fspath(candidate_path), log_text)
            self.assertEqual(rendered, empty_policy.path.read_bytes())


if __name__ == "__main__":
    unittest.main()
