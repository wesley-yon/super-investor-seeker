from __future__ import annotations

from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from insider_storage import InsiderStateStore, canonical_insider_state_json_bytes


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/bootstrap_insider_authority_state.py"
EMPTY_APPROVED = {"contract_version": 1, "issuer_ciks": []}
EMPTY_POLICY = {"contract_version": 1, "issuers": []}
CONFIRMATION = "INITIALIZE_EMPTY_PRIVATE_INSIDER_AUTHORITY_ONLY"
NONEMPTY_POLICY = {
    "contract_version": 1,
    "issuers": [
        {
            "issuer_cik": "0001067983",
            "security_mappings": {
                "a" * 64: {
                    "stockId": "084670702",
                    "fileStem": "084670702",
                    "ticker": "BRK.B",
                    "companyName": "Synthetic Berkshire",
                    "securityType": "Common Stock",
                    "securityTypeLabel": "COMMON STOCK",
                    "cusip": "084670702",
                    "primary": True,
                }
            },
        }
    ],
}


def bootstrap_module():
    spec = importlib.util.spec_from_file_location(
        "bootstrap_insider_authority_state", SCRIPT
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load private authority bootstrap script")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BootstrapInsiderAuthorityStateTests(unittest.TestCase):
    def test_creates_exact_empty_authority_state_and_is_idempotent(self) -> None:
        self.assertTrue(
            SCRIPT.is_file(), "private authority bootstrap script is missing"
        )
        module = bootstrap_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)

            created = module.bootstrap_insider_authority_state(repository_root=root)

            self.assertEqual(EMPTY_APPROVED, state.read("approved-issuers-v1"))
            self.assertEqual(EMPTY_POLICY, state.read("publication-policy-v1"))
            self.assertEqual(
                {
                    "changed": True,
                    "created_keys": ["approved-issuers-v1", "publication-policy-v1"],
                    "approved_state_sha256": hashlib.sha256(
                        canonical_insider_state_json_bytes(EMPTY_APPROVED)
                    ).hexdigest(),
                    "publication_policy_sha256": hashlib.sha256(
                        canonical_insider_state_json_bytes(EMPTY_POLICY)
                    ).hexdigest(),
                },
                created,
            )
            approved_path = (
                root / "data/insiders/private/state/approved-issuers-v1.json"
            )
            policy_path = (
                root / "data/insiders/private/state/publication-policy-v1.json"
            )
            approved_bytes = approved_path.read_bytes()
            policy_bytes = policy_path.read_bytes()

            unchanged = module.bootstrap_insider_authority_state(repository_root=root)

            self.assertFalse(unchanged["changed"])
            self.assertEqual([], unchanged["created_keys"])
            self.assertEqual(approved_bytes, approved_path.read_bytes())
            self.assertEqual(policy_bytes, policy_path.read_bytes())

    def test_rejects_nonempty_policy_before_creating_missing_approved_root(
        self,
    ) -> None:
        module = bootstrap_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored_policy = state.write("publication-policy-v1", NONEMPTY_POLICY)
            policy_bytes = stored_policy.path.read_bytes()

            with self.assertRaises(module.InsiderAuthorityBootstrapError):
                module.bootstrap_insider_authority_state(repository_root=root)

            with self.assertRaises(FileNotFoundError):
                state.read("approved-issuers-v1")
            self.assertEqual(policy_bytes, stored_policy.path.read_bytes())

    def test_rejects_nonempty_approved_root_before_creating_missing_policy(
        self,
    ) -> None:
        module = bootstrap_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored_approved = state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0001373715"]},
            )
            approved_bytes = stored_approved.path.read_bytes()

            with self.assertRaises(module.InsiderAuthorityBootstrapError):
                module.bootstrap_insider_authority_state(repository_root=root)

            self.assertEqual(approved_bytes, stored_approved.path.read_bytes())
            with self.assertRaises(FileNotFoundError):
                state.read("publication-policy-v1")

    def test_recovers_only_an_exact_empty_partial_genesis(self) -> None:
        module = bootstrap_module()
        partial_states = (
            ("approved-issuers-v1", EMPTY_APPROVED, "publication-policy-v1"),
            ("publication-policy-v1", EMPTY_POLICY, "approved-issuers-v1"),
        )
        for existing_key, existing_payload, missing_key in partial_states:
            with self.subTest(existing_key=existing_key):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    state = InsiderStateStore(root)
                    stored = state.write(existing_key, existing_payload)
                    stored_bytes = stored.path.read_bytes()

                    result = module.bootstrap_insider_authority_state(
                        repository_root=root
                    )

                    self.assertTrue(result["changed"])
                    self.assertEqual([missing_key], result["created_keys"])
                    self.assertEqual(stored_bytes, stored.path.read_bytes())
                    self.assertEqual(EMPTY_APPROVED, state.read("approved-issuers-v1"))
                    self.assertEqual(EMPTY_POLICY, state.read("publication-policy-v1"))

    def test_retry_repairs_an_interruption_between_the_two_genesis_writes(
        self,
    ) -> None:
        module = bootstrap_module()
        real_write = InsiderStateStore.write
        write_count = 0

        def interrupted_write(
            store: InsiderStateStore,
            key: str,
            payload: object,
            *,
            expected_sha256: str | None = None,
        ):
            nonlocal write_count
            write_count += 1
            if write_count == 2:
                raise OSError("simulated interruption")
            return real_write(
                store,
                key,
                payload,
                expected_sha256=expected_sha256,
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            with mock.patch.object(
                InsiderStateStore,
                "write",
                autospec=True,
                side_effect=interrupted_write,
            ):
                with self.assertRaisesRegex(OSError, "simulated interruption"):
                    module.bootstrap_insider_authority_state(repository_root=root)

            self.assertEqual(EMPTY_APPROVED, state.read("approved-issuers-v1"))
            with self.assertRaises(FileNotFoundError):
                state.read("publication-policy-v1")

            recovered = module.bootstrap_insider_authority_state(repository_root=root)

            self.assertTrue(recovered["changed"])
            self.assertEqual(["publication-policy-v1"], recovered["created_keys"])
            self.assertEqual(EMPTY_APPROVED, state.read("approved-issuers-v1"))
            self.assertEqual(EMPTY_POLICY, state.read("publication-policy-v1"))

    def test_rejects_malformed_existing_root_without_creating_counterpart(self) -> None:
        module = bootstrap_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored = state.write("approved-issuers-v1", EMPTY_APPROVED)
            stored.path.write_bytes(b"{\n")

            with self.assertRaises(module.InsiderAuthorityBootstrapError):
                module.bootstrap_insider_authority_state(repository_root=root)

            self.assertEqual(b"{\n", stored.path.read_bytes())
            with self.assertRaises(FileNotFoundError):
                state.read("publication-policy-v1")

    def test_empty_policy_remains_non_materializable(self) -> None:
        materializer = importlib.import_module("scripts.publish_insider_activity")
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("approved-issuers-v1", EMPTY_APPROVED)
            state.write("publication-policy-v1", EMPTY_POLICY)

            with self.assertRaisesRegex(
                materializer.InsiderPublicationMaterializationError,
                "publication policy",
            ):
                materializer._policy_issuer_rows(state)

    def test_cli_requires_exact_confirmation_and_emits_bounded_metadata(self) -> None:
        module = bootstrap_module()
        self.assertTrue(hasattr(module.main, "__wrapped__"))
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output = io.StringIO()

            with redirect_stdout(output):
                invalid_status = module.main.__wrapped__(
                    [
                        "--repository-root",
                        str(root),
                        "--confirmation",
                        "WRONG",
                    ]
                )

            self.assertEqual(2, invalid_status)
            self.assertEqual("", output.getvalue())
            self.assertFalse((root / "data").exists())

            with redirect_stdout(output):
                status = module.main.__wrapped__(
                    [
                        "--repository-root",
                        str(root),
                        "--confirmation",
                        CONFIRMATION,
                    ]
                )

            self.assertEqual(0, status)
            result = json.loads(output.getvalue())
            self.assertEqual(
                {
                    "approved_state_sha256",
                    "changed",
                    "created_keys",
                    "publication_policy_sha256",
                },
                set(result),
            )
            self.assertTrue(result["changed"])
            self.assertEqual(
                ["approved-issuers-v1", "publication-policy-v1"],
                result["created_keys"],
            )


if __name__ == "__main__":
    unittest.main()
