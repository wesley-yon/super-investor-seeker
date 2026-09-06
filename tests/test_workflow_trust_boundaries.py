"""Regression checks for branch-scoped production and candidate credentials."""

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def job_header(workflow: str, job: str) -> str:
    """Return only job policy, excluding step guards and checkout settings."""
    match = re.search(
        rf"(?ms)^  {re.escape(job)}:\n(.*?)(?=^    steps:|^  [\w-]+:|\Z)",
        workflow,
    )
    if match is None:
        raise AssertionError(f"missing job: {job}")
    return match.group(1)


class WorkflowTrustBoundaryTests(unittest.TestCase):
    def test_maintenance_requires_main_before_accessing_production_environment(self):
        for filename, job in (
            ("update-data.yml", "update"),
            ("refresh-cusip-registry.yml", "rebuild_security_master"),
        ):
            with self.subTest(workflow=filename):
                workflow = (WORKFLOWS / filename).read_text(encoding="utf-8")
                policy = job_header(workflow, job)
                self.assertRegex(policy, r"(?m)^    environment: private-data$")
                guard = re.search(
                    r"(?ms)^    if: (.*?)(?=^    [a-z][\w-]*:|\Z)", policy
                )
                self.assertIsNotNone(guard)
                normalized = " ".join(guard.group(1).split())
                normalized = normalized.removeprefix(">- ")
                expected = "github.ref == 'refs/heads/main'"
                if job == "update":
                    expected += " && needs.filing-window.outputs.run_update == 'true'"
                self.assertEqual(expected, normalized)

    def test_candidate_never_receives_the_production_app_identity(self):
        workflow = (WORKFLOWS / "verify-sec-candidate.yml").read_text(
            encoding="utf-8"
        )
        policy = job_header(workflow, "verify")
        self.assertRegex(policy, r"(?m)^    environment: private-data-readonly$")
        self.assertNotIn("DATA_ARCHIVE_APP_PRIVATE_KEY", workflow)
        self.assertNotIn("DATA_ARCHIVE_APP_CLIENT_ID", workflow)
        self.assertNotIn("secrets: inherit", workflow)
        self.assertIn("app-id: ${{ vars.SIS_READER_APP_ID }}", workflow)
        self.assertIn(
            "private-key: ${{ secrets.SIS_READER_APP_PRIVATE_KEY }}", workflow
        )
        self.assertIn("repositories: super-investor-seeker-data", workflow)
        self.assertEqual(
            ["read"], re.findall(r"permission-contents: (\w+)", workflow)
        )
        self.assertNotRegex(workflow, r"(?m)^\s+(?:contents|actions|pages): write$")

    def test_actions_are_immutable_and_checkout_credentials_are_explicit(self):
        for path in WORKFLOWS.glob("*.yml"):
            text = path.read_text()
            for action, ref in re.findall(r"uses: (actions/[^@\s]+)@([^\s]+)", text):
                with self.subTest(workflow=path.name, action=action):
                    self.assertRegex(ref, r"^[0-9a-f]{40}$")
            for block in re.split(r"(?m)(?=^      - name:)", text):
                if "uses: actions/checkout@" in block:
                    expected = "true" if path.name == "keepalive.yml" else "false"
                    self.assertIn(f"persist-credentials: {expected}", block)

    def test_pages_credential_jobs_require_protected_environments(self):
        workflow = (WORKFLOWS / "deploy-pages.yml").read_text()
        for job in ("resolve", "build", "finalize-private-snapshots"):
            policy = job_header(workflow, job)
            self.assertIn("environment: private-data", policy)
            self.assertIn("github.ref == 'refs/heads/main'", policy)
        policy = job_header(workflow, "deploy")
        self.assertIn("name: github-pages", policy)
        self.assertIn("github.ref == 'refs/heads/main'", policy)

    def test_keepalive_write_job_is_only_dispatched_from_main(self):
        workflow = (WORKFLOWS / "keepalive.yml").read_text(encoding="utf-8")
        policy = job_header(workflow, "keepalive")
        self.assertRegex(policy, r"(?m)^    if: github.ref == 'refs/heads/main'$")


if __name__ == "__main__":
    unittest.main()
