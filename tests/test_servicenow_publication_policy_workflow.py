from __future__ import annotations

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github/workflows/approve-servicenow-insider-publication.yml"


class ServiceNowPublicationPolicyWorkflowTests(unittest.TestCase):
    def _workflow(self) -> str:
        self.assertTrue(
            WORKFLOW_PATH.is_file(), "publication-policy workflow is missing"
        )
        return WORKFLOW_PATH.read_text(encoding="utf-8")

    def test_manual_dispatch_has_exact_ten_required_string_inputs(self) -> None:
        workflow = self._workflow()
        trigger = workflow.split("on:\n", 1)[1].split("\nconcurrency:", 1)[0]
        expected = (
            "source_sha",
            "private_release_id",
            "private_release_identity_sha256",
            "dataset_id",
            "archive_sha256",
            "manifest_sha256",
            "current_policy_sha256",
            "issuer_generation_digest",
            "candidate_policy_sha256",
            "confirmation",
        )

        self.assertIn("  workflow_dispatch:", trigger)
        for prohibited in ("push:", "schedule:", "pull_request:", "workflow_call:"):
            self.assertNotIn(prohibited, trigger)
        self.assertEqual(
            10,
            len(re.findall(r"(?m)^      [a-z0-9_]+:\n        description:", trigger)),
        )
        for name in expected:
            with self.subTest(name=name):
                block = re.split(
                    r"(?m)^      [a-z0-9_]+:\n",
                    trigger.split(f"      {name}:\n", 1)[1],
                    maxsplit=1,
                )[0]
                self.assertIn("required: true", block)
                self.assertIn("type: string", block)
        self.assertIn("APPROVE_SERVICENOW_PUBLIC_INSIDER_POLICY", workflow)
        self.assertIn("^[1-9][0-9]*$", workflow)
        self.assertGreaterEqual(workflow.count("^[0-9a-f]{64}$"), 2)
        self.assertIn("^[0-9a-f]{40}$", workflow)

    def test_workflow_has_fixed_private_only_authority_and_permissions(self) -> None:
        workflow = self._workflow()
        self.assertIn(
            "DATA_REPOSITORY: wesley-yon/super-investor-seeker-data", workflow
        )
        self.assertIn("SERVICENOW_CIK: '0001373715'", workflow)
        self.assertIn('"wesley-yon/super-investor-seeker"', workflow)
        self.assertIn("owner: wesley-yon", workflow)
        self.assertNotIn("github.repository_owner", workflow)
        self.assertRegex(workflow, r"(?m)^permissions:\n  contents: read$")
        self.assertEqual(
            2,
            len(
                re.findall(
                    r"(?m)^  [a-z][a-z-]+:\n(?:    [^\n]*\n)*?    runs-on:", workflow
                )
            ),
        )
        for forbidden in (
            "pages:",
            "deployments:",
            "actions:",
            "id-token:",
            "pages-deploy",
            "deploy-pages",
            "publish_insider_activity.py",
            "SEC_USER_AGENT",
            "git commit",
            "git push",
            "release delete",
            "tag delete",
        ):
            self.assertNotIn(forbidden, workflow)

    def test_actions_checkouts_secret_order_and_private_public_separation(self) -> None:
        workflow = self._workflow()
        actions = re.findall(r"(?m)^\s*uses:\s*([^\s#]+)", workflow)
        self.assertGreaterEqual(len(actions), 5)
        for action in actions:
            with self.subTest(action=action):
                self.assertRegex(action, r"^[^@]+@[0-9a-f]{40}$")
        checkout_blocks = workflow.split("uses: actions/checkout@")[1:]
        self.assertEqual(2, len(checkout_blocks))
        for checkout in checkout_blocks:
            self.assertIn(
                "ref: ${{ inputs.source_sha }}", checkout.split("\n      - name:", 1)[0]
            )
            self.assertIn(
                "persist-credentials: false", checkout.split("\n      - name:", 1)[0]
            )

        secret = "secrets.SERVICENOW_INSIDER_PUBLICATION_POLICY_JSON"
        self.assertEqual(1, workflow.count(secret))
        self.assertIn(
            "environment:\n      name: insider-publication-approval", workflow
        )
        self.assertIn("umask 077", workflow)
        self.assertIn("mktemp", workflow)
        self.assertIn("trap", workflow)
        self.assertIn("candidate_policy_sha256", workflow)
        self.assertNotIn('echo "$SERVICENOW_INSIDER_PUBLICATION_POLICY_JSON"', workflow)

        for before_secret in (
            "Validate exact publication request",
            "Authenticate to the private data repository",
            "Restore exact immutable private snapshot",
            "Revalidate immutable private release before candidate use",
        ):
            self.assertLess(workflow.index(before_secret), workflow.index(secret))
        self.assertLess(
            workflow.index(secret),
            workflow.index("approve_insider_publication_policy.py"),
        )
        self.assertLess(
            workflow.index("candidate_policy_sha256"),
            workflow.index("approve_insider_publication_policy.py"),
        )
        self.assertIn("REQUIRE_PUBLIC_TREE_UNCHANGED: 'true'", workflow)
        self.assertIn(
            "Public artifact changed during private publication-policy approval",
            workflow,
        )
        self.assertIn("steps.approve_policy.outputs.changed == 'true'", workflow)
        self.assertIn("steps.approve_policy.outputs.changed == 'false'", workflow)
        self.assertIn("BASE_RELEASE_ID:", workflow)
        self.assertIn("BASE_RELEASE_IDENTITY_SHA256:", workflow)
        self.assertIn('store.read_canonical(f"issuers/{cik}")', workflow)
        self.assertIn('issuer["generation_digest"]', workflow)
        self.assertNotIn('store.read("issuer-state-v1")', workflow)
        self.assertNotIn("release upload $release_tag", workflow)
        self.assertNotIn("release upload.*--clobber", workflow)

    def test_first_approval_hashes_canonical_empty_policy_with_storage_contract(
        self,
    ) -> None:
        workflow = self._workflow()
        transition = workflow.split(
            "- name: Validate restored policy transition origin", 1
        )[1].split("- name: Revalidate immutable private release", 1)[0]

        self.assertIn("canonical_insider_state_json_bytes", transition)
        self.assertIn("hashlib.sha256", transition)
        self.assertIn("store.read_canonical(", transition)
        self.assertNotIn("store.read(", transition)
        self.assertNotIn("publication_policy_sha256", transition)


if __name__ == "__main__":
    unittest.main()
