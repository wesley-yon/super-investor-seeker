from __future__ import annotations

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/initialize-empty-private-insider-authority.yml"


class BootstrapInsiderAuthorityWorkflowTests(unittest.TestCase):
    def test_genesis_boundary_is_documented_as_empty_and_private_only(self) -> None:
        for path in ("README.md", "ARCHITECTURE.md"):
            with self.subTest(path=path):
                documentation = (ROOT / path).read_text(encoding="utf-8")
                self.assertIn(
                    "initialize-empty-private-insider-authority.yml",
                    documentation,
                )
                self.assertIn("empty", documentation)
                self.assertIn("private", documentation)
                self.assertIn("public", documentation)
                self.assertIn("materializer", documentation)

    def test_manual_genesis_is_exact_private_only_and_fail_closed(self) -> None:
        self.assertTrue(
            WORKFLOW.is_file(), "private authority genesis workflow is missing"
        )
        workflow = WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("\n  push:", workflow)
        self.assertNotIn("\n  schedule:", workflow)
        self.assertNotIn("\n  pull_request:", workflow)
        self.assertIn("Type INITIALIZE_EMPTY_PRIVATE_INSIDER_AUTHORITY_ONLY", workflow)
        self.assertIn(
            'if [ "$CONFIRMATION" != "INITIALIZE_EMPTY_PRIVATE_INSIDER_AUTHORITY_ONLY" ]; then',
            workflow,
        )
        self.assertIn('if [ "$REQUESTED_REF" != "refs/heads/main" ]; then', workflow)
        self.assertIn(
            "REQUESTED_REPOSITORY: ${{ github.repository }}",
            workflow,
        )
        self.assertIn(
            'if [ "$REQUESTED_REPOSITORY" != "wesley-yon/super-investor-seeker" ]; then',
            workflow,
        )
        self.assertIn(
            "DATA_REPOSITORY: wesley-yon/super-investor-seeker-data",
            workflow,
        )
        self.assertIn("owner: wesley-yon", workflow)
        self.assertNotIn("${{ github.repository_owner }}", workflow)
        self.assertIn('[[ ! "$EXPECTED_DATASET_ID" =~ ^[0-9a-f]{64}$ ]]', workflow)
        self.assertIn("ref: ${{ github.sha }}", workflow)
        self.assertIn(
            "git fetch --no-tags origin main:refs/remotes/origin/main", workflow
        )
        self.assertIn("Dispatch code is not the exact current main revision", workflow)

        checkout = workflow.split("- name: Checkout repository", 1)[1].split(
            "\n      - name:", 1
        )[0]
        self.assertIn("persist-credentials: false", checkout)

        request_guard = workflow.index(
            "Validate explicit empty-authority genesis request"
        )
        checkout_at = workflow.index("- name: Checkout repository")
        checkout_guard = workflow.index("Verify exact dispatch code checkout")
        read_credential = workflow.index("Authenticate to the private data repository")
        restore_guard = workflow.index(
            "Revalidate current main before private snapshot restore"
        )
        restore = workflow.index("Restore exact validated private snapshot")
        mutation_guard = workflow.index(
            "Revalidate current main before empty-authority mutation"
        )
        bootstrap = workflow.index("Initialize exact empty private authority state")
        self.assertLess(request_guard, checkout_at)
        self.assertLess(checkout_guard, read_credential)
        self.assertLess(read_credential, restore_guard)
        self.assertLess(restore_guard, restore)
        self.assertLess(restore, mutation_guard)
        self.assertLess(mutation_guard, bootstrap)
        self.assertEqual(
            3,
            workflow.count("git fetch --no-tags origin main:refs/remotes/origin/main"),
        )
        self.assertEqual(
            3,
            workflow.count("Dispatch code is not the exact current main revision"),
        )
        self.assertIn("permission-contents: read", workflow)
        self.assertIn("permission-contents: write", workflow)
        self.assertIn("if: steps.bootstrap.outputs.changed == 'true'", workflow)

        self.assertIn("python scripts/data_snapshot.py pull", workflow)
        self.assertIn('if [ "$dataset_id" != "$EXPECTED_DATASET_ID" ]; then', workflow)
        self.assertIn("python scripts/bootstrap_insider_authority_state.py", workflow)
        self.assertIn(
            '--confirmation "INITIALIZE_EMPTY_PRIVATE_INSIDER_AUTHORITY_ONLY"',
            workflow,
        )
        self.assertIn(
            'keys == ["approved_state_sha256", "changed", "created_keys", "publication_policy_sha256"]',
            workflow,
        )
        self.assertIn('state_store.read("approved-issuers-v1")', workflow)
        self.assertIn('state_store.read("publication-policy-v1")', workflow)
        self.assertIn(
            'approved == {"contract_version": 1, "issuer_ciks": []}', workflow
        )
        self.assertIn('policy == {"contract_version": 1, "issuers": []}', workflow)
        self.assertIn(
            '.approved_state_sha256 == "f4333a9e0b5f83ed25ed0b25ded529cae101a7ad12b1586a8d4f755e3f7a7122"',
            workflow,
        )
        self.assertIn(
            '.publication_policy_sha256 == "f77a13b72ad36a0543a5330911b831afb434d175f3998808b450580f58f10ea2"',
            workflow,
        )
        self.assertIn(
            "Public artifact changed during empty authority genesis", workflow
        )
        self.assertIn("REQUIRE_PUBLIC_TREE_UNCHANGED: 'true'", workflow)
        self.assertIn(
            "BASE_RELEASE_TAG: ${{ steps.restore_snapshot.outputs.release_tag }}",
            workflow,
        )
        self.assertIn("bash scripts/publish_private_snapshot.sh", workflow)
        self.assertIn(
            "Private authority genesis must not change the public site", workflow
        )
        self.assertIn(
            "Idempotent genesis attempted a private snapshot mutation", workflow
        )

        self.assertNotIn("publish_insider_activity.py", workflow)
        self.assertNotIn("deploy-pages", workflow)
        self.assertNotIn("SEC_USER_AGENT", workflow)
        self.assertNotIn("pages: write", workflow)
        self.assertNotIn("git push", workflow)
        self.assertNotIn("gh pr", workflow)
        self.assertRegex(workflow, r"(?m)^permissions:\n  contents: read$")

        uses = re.findall(r"(?m)^\s*uses:\s*([^\s#]+)", workflow)
        self.assertGreaterEqual(len(uses), 3)
        for action in uses:
            with self.subTest(action=action):
                self.assertRegex(action, r"^[^@]+@[0-9a-f]{40}$")


if __name__ == "__main__":
    unittest.main()
