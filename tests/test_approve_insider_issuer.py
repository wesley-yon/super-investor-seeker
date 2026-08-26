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

from insider_storage import (
    InsiderStateRevisionError,
    InsiderStateStore,
    canonical_insider_state_json_bytes,
)

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "approve_insider_issuer.py"
BERKSHIRE_CIK = "0001067983"
SERVICENOW_CIK = "0001373715"
OTHER_CIK = "0001652044"


def approval_module():
    spec = importlib.util.spec_from_file_location("approve_insider_issuer", SCRIPT)
    if spec is None or spec.loader is None:
        raise AssertionError("manual insider approval script is not importable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ApproveInsiderIssuerTests(unittest.TestCase):
    def test_adds_exact_service_now_cik_to_existing_durable_scope(self) -> None:
        self.assertTrue(SCRIPT.is_file(), "manual insider approval script is missing")
        module = approval_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            initial = {
                "contract_version": 1,
                "issuer_ciks": [BERKSHIRE_CIK],
            }
            stored = state.write("approved-issuers-v1", initial)

            result = module.approve_insider_issuer(
                repository_root=root,
                issuer_cik=SERVICENOW_CIK,
                expected_current_sha256=stored.sha256,
            )

            expected = {
                "contract_version": 1,
                "issuer_ciks": [BERKSHIRE_CIK, SERVICENOW_CIK],
            }
            self.assertEqual(expected, state.read("approved-issuers-v1"))
            self.assertEqual(SERVICENOW_CIK, result["issuer_cik"])
            self.assertTrue(result["changed"])
            self.assertEqual(1, result["previous_issuer_count"])
            self.assertEqual(2, result["approved_issuer_count"])
            self.assertEqual(
                hashlib.sha256(
                    canonical_insider_state_json_bytes(expected)
                ).hexdigest(),
                result["approved_state_sha256"],
            )

    def test_already_approved_service_now_cik_is_idempotent(self) -> None:
        module = approval_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            initial = {
                "contract_version": 1,
                "issuer_ciks": [BERKSHIRE_CIK, SERVICENOW_CIK],
            }
            stored = state.write("approved-issuers-v1", initial)
            state_bytes_before = stored.path.read_bytes()

            result = module.approve_insider_issuer(
                repository_root=root,
                issuer_cik=SERVICENOW_CIK,
                expected_current_sha256=stored.sha256,
            )

            self.assertEqual(state_bytes_before, stored.path.read_bytes())
            self.assertFalse(result["changed"])
            self.assertEqual(2, result["previous_issuer_count"])
            self.assertEqual(2, result["approved_issuer_count"])
            self.assertEqual(stored.sha256, result["approved_state_sha256"])

    def test_rejects_a_different_canonical_issuer_without_mutation(self) -> None:
        module = approval_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored = state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [BERKSHIRE_CIK]},
            )
            state_bytes_before = stored.path.read_bytes()

            with self.assertRaises(module.InsiderIssuerApprovalError):
                module.approve_insider_issuer(
                    repository_root=root,
                    issuer_cik=OTHER_CIK,
                    expected_current_sha256=stored.sha256,
                )

            self.assertEqual(state_bytes_before, stored.path.read_bytes())

    def test_rejects_noncanonical_service_now_inputs_without_mutation(self) -> None:
        module = approval_module()
        invalid_inputs = (
            "1373715",
            f" {SERVICENOW_CIK}",
            f"{SERVICENOW_CIK} ",
            True,
            1373715,
            None,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored = state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [BERKSHIRE_CIK]},
            )
            state_bytes_before = stored.path.read_bytes()

            for invalid_input in invalid_inputs:
                with self.subTest(issuer_cik=invalid_input):
                    with self.assertRaises(module.InsiderIssuerApprovalError):
                        module.approve_insider_issuer(
                            repository_root=root,
                            issuer_cik=invalid_input,
                            expected_current_sha256=stored.sha256,
                        )
                    self.assertEqual(state_bytes_before, stored.path.read_bytes())

    def test_rejects_stale_state_revision_without_mutation(self) -> None:
        module = approval_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored = state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [BERKSHIRE_CIK]},
            )
            state_bytes_before = stored.path.read_bytes()

            with self.assertRaisesRegex(
                InsiderStateRevisionError,
                "private state revision is stale",
            ):
                module.approve_insider_issuer(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    expected_current_sha256="0" * 64,
                )

            self.assertEqual(state_bytes_before, stored.path.read_bytes())

    def test_missing_approved_state_is_not_bootstrapped(self) -> None:
        module = approval_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "data/insiders/private/state/approved-issuers-v1.json"

            with self.assertRaises(FileNotFoundError):
                module.approve_insider_issuer(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    expected_current_sha256="0" * 64,
                )

            self.assertFalse(state_path.exists())

    def test_private_approval_does_not_change_publication_policy(self) -> None:
        module = approval_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            approved = state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [BERKSHIRE_CIK]},
            )
            policy = {
                "contract_version": 1,
                "issuers": [
                    {
                        "issuer_cik": BERKSHIRE_CIK,
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
            stored_policy = state.write("publication-policy-v1", policy)
            policy_bytes_before = stored_policy.path.read_bytes()

            module.approve_insider_issuer(
                repository_root=root,
                issuer_cik=SERVICENOW_CIK,
                expected_current_sha256=approved.sha256,
            )

            self.assertEqual(policy_bytes_before, stored_policy.path.read_bytes())
            self.assertEqual(policy, state.read("publication-policy-v1"))

    def test_cli_is_serialized_and_emits_only_bounded_approval_metadata(self) -> None:
        module = approval_module()
        self.assertTrue(hasattr(module.main, "__wrapped__"))
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored = state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [BERKSHIRE_CIK]},
            )
            output = io.StringIO()

            with redirect_stdout(output):
                status = module.main.__wrapped__(
                    [
                        "--repository-root",
                        str(root),
                        "--issuer-cik",
                        SERVICENOW_CIK,
                        "--expected-current-sha256",
                        stored.sha256,
                    ]
                )

            self.assertEqual(0, status)
            result = json.loads(output.getvalue())
            self.assertEqual(
                {
                    "approved_issuer_count",
                    "approved_state_sha256",
                    "changed",
                    "issuer_cik",
                    "previous_issuer_count",
                },
                set(result),
            )
            self.assertEqual(SERVICENOW_CIK, result["issuer_cik"])
            self.assertNotIn(BERKSHIRE_CIK, output.getvalue())
            self.assertEqual(
                [BERKSHIRE_CIK, SERVICENOW_CIK],
                state.read("approved-issuers-v1")["issuer_ciks"],
            )

    def test_cli_rejects_invalid_scope_before_state_mutation(self) -> None:
        module = approval_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored = state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [BERKSHIRE_CIK]},
            )
            state_bytes_before = stored.path.read_bytes()
            cases = (
                [],
                ["--issuer-cik", SERVICENOW_CIK],
                [
                    "--issuer-cik",
                    "1373715",
                    "--expected-current-sha256",
                    stored.sha256,
                ],
                [
                    "--issuer-cik",
                    OTHER_CIK,
                    "--expected-current-sha256",
                    stored.sha256,
                ],
                [
                    "--issuer-cik",
                    SERVICENOW_CIK,
                    "--expected-current-sha256",
                    "A" * 64,
                ],
                "--issuer-cik",
            )

            for arguments in cases:
                with self.subTest(arguments=arguments):
                    output = io.StringIO()
                    with redirect_stdout(output):
                        status = module.main.__wrapped__(arguments)
                    self.assertEqual(2, status)
                    self.assertEqual("", output.getvalue())
                    self.assertEqual(state_bytes_before, stored.path.read_bytes())

    def test_cli_rejects_stale_revision_without_state_mutation(self) -> None:
        module = approval_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored = state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [BERKSHIRE_CIK]},
            )
            state_bytes_before = stored.path.read_bytes()
            output = io.StringIO()

            with redirect_stdout(output):
                status = module.main.__wrapped__(
                    [
                        "--repository-root",
                        str(root),
                        "--issuer-cik",
                        SERVICENOW_CIK,
                        "--expected-current-sha256",
                        "0" * 64,
                    ]
                )

            self.assertEqual(1, status)
            self.assertEqual("", output.getvalue())
            self.assertEqual(state_bytes_before, stored.path.read_bytes())

    def test_compare_and_swap_rejects_a_racing_state_change(self) -> None:
        module = approval_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored = state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": [BERKSHIRE_CIK]},
            )
            proxy = mock.Mock()
            proxy.read.side_effect = state.read

            def racing_write(key, payload, *, expected_sha256=None):
                state.write(
                    "approved-issuers-v1",
                    {
                        "contract_version": 1,
                        "issuer_ciks": [BERKSHIRE_CIK, OTHER_CIK],
                    },
                    expected_sha256=stored.sha256,
                )
                return state.write(
                    key,
                    payload,
                    expected_sha256=expected_sha256,
                )

            proxy.write.side_effect = racing_write
            with (
                mock.patch.object(module, "InsiderStateStore", return_value=proxy),
                self.assertRaises(InsiderStateRevisionError),
            ):
                module.approve_insider_issuer(
                    repository_root=root,
                    issuer_cik=SERVICENOW_CIK,
                    expected_current_sha256=stored.sha256,
                )

            self.assertEqual(
                [BERKSHIRE_CIK, OTHER_CIK],
                state.read("approved-issuers-v1")["issuer_ciks"],
            )


if __name__ == "__main__":
    unittest.main()
