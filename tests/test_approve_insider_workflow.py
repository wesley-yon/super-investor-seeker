import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ".github/workflows/approve-servicenow-insider-ingestion.yml"
PUBLISHER_SCRIPT = "scripts/publish_private_snapshot.sh"


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


class ServiceNowApprovalWorkflowTests(unittest.TestCase):
    def test_private_ingestion_approval_is_manual_and_private_only(self) -> None:
        workflow = read(WORKFLOW_PATH)
        trigger = workflow.split("on:\n", 1)[1].split("\nconcurrency:", 1)[0]

        self.assertIn("  workflow_dispatch:", trigger)
        self.assertNotIn("  push:", trigger)
        self.assertNotIn("  schedule:", trigger)
        for field in ("confirmation", "expected_dataset_id"):
            block = trigger.split(f"      {field}:", 1)[1]
            self.assertIn("required: true", block)
            self.assertIn("type: string", block)

        self.assertIn("group: data-maintenance", workflow)
        self.assertIn("group: private-release-publication", workflow)
        self.assertIn("SERVICENOW_CIK: '0001373715'", workflow)
        self.assertIn(
            'if [ "$CONFIRMATION" != "APPROVE_PRIVATE_SERVICENOW_INGESTION_ONLY" ]; then',
            workflow,
        )
        self.assertIn('[[ ! "$EXPECTED_DATASET_ID" =~ ^[0-9a-f]{64}$ ]]', workflow)
        self.assertIn('if [ "$dataset_id" != "$EXPECTED_DATASET_ID" ]; then', workflow)
        self.assertIn("python scripts/data_snapshot.py pull", workflow)
        self.assertIn('state_store.read("approved-issuers-v1")', workflow)
        self.assertIn('state_store.read("publication-policy-v1")', workflow)
        self.assertIn("canonical_insider_state_json_bytes", workflow)
        self.assertIn("python scripts/approve_insider_issuer.py", workflow)
        self.assertIn('--issuer-cik "$SERVICENOW_CIK"', workflow)
        self.assertIn('--expected-current-sha256 "$APPROVED_STATE_SHA256"', workflow)
        self.assertIn("Publication policy changed during private approval", workflow)
        self.assertIn("Public artifact changed during private approval", workflow)
        self.assertIn("python validate_data.py", workflow)
        self.assertIn(
            "python -m unittest discover -s tests -p 'test_data_contract.py' -v",
            workflow,
        )
        self.assertIn("python -m unittest discover -s tests -v", workflow)
        self.assertIn("node --test tests/test_site_data_loader.mjs", workflow)
        self.assertIn(f"bash {PUBLISHER_SCRIPT}", workflow)
        self.assertIn("permission-contents: read", workflow)
        self.assertIn("permission-contents: write", workflow)
        self.assertIn("BASE_PUBLIC_TREE_SHA256:", workflow)
        self.assertIn("site_changed", workflow)
        self.assertIn("must not change the public site", workflow)
        self.assertEqual(1, workflow.count(f"bash {PUBLISHER_SCRIPT}"))

        publish_at = workflow.index(
            "- name: Publish validated private-only approval snapshot"
        )
        self.assertLess(
            workflow.index("- name: Refresh private data publication credential"),
            publish_at,
        )
        for gate in (
            "python validate_data.py",
            "python -m unittest discover -s tests -p 'test_data_contract.py' -v",
            "python -m unittest discover -s tests -v",
            "node --test tests/test_site_data_loader.mjs",
        ):
            self.assertLess(workflow.index(gate), publish_at)

        self.assertNotIn("publish_insider_activity.py", workflow)
        self.assertNotIn("Materialize approved public insider publication", workflow)
        self.assertNotIn("deploy-pages:", workflow)
        self.assertNotIn("pages: write", workflow)
        self.assertNotIn("SEC_USER_AGENT", workflow)
        self.assertNotIn("git commit", workflow)
        self.assertNotIn("git push", workflow)

    def test_workflow_binds_bounded_approval_result_to_durable_state(self) -> None:
        workflow = read(WORKFLOW_PATH)
        approval_step = workflow.split(
            "- name: Approve ServiceNow for private ingestion",
            1,
        )[1].split("\n      - name:", 1)[0]
        boundary_step = workflow.split(
            "- name: Verify private-only approval boundaries",
            1,
        )[1].split("\n      - name:", 1)[0]

        self.assertIn(
            '(.previous_issuer_count | type == "number" and . >= 0 and . == floor)',
            approval_step,
        )
        self.assertIn(
            '(.approved_issuer_count | type == "number" and . >= 0 and . == floor)',
            approval_step,
        )
        self.assertIn(
            "if .changed then",
            approval_step,
        )
        self.assertIn(
            ".approved_issuer_count == (.previous_issuer_count + 1)",
            approval_step,
        )
        self.assertIn(
            ".approved_issuer_count == .previous_issuer_count",
            approval_step,
        )
        self.assertIn(
            "EXPECTED_APPROVED_STATE_SHA256: ${{ steps.approve_issuer.outputs.approved_state_sha256 }}",
            boundary_step,
        )
        self.assertIn(
            "Approved state digest does not match approval output",
            boundary_step,
        )

    def test_idempotent_approval_skips_private_snapshot_mutation(self) -> None:
        workflow = read(WORKFLOW_PATH)
        refresh_credential = workflow.split(
            "- name: Refresh private data publication credential",
            1,
        )[1].split("\n      - name:", 1)[0]
        publish_snapshot = workflow.split(
            "- name: Publish validated private-only approval snapshot",
            1,
        )[1].split("\n      - name:", 1)[0]

        for step in (refresh_credential, publish_snapshot):
            self.assertIn(
                "if: steps.approve_issuer.outputs.changed == 'true'",
                step,
            )

        no_op = workflow.split(
            "- name: Verify idempotent private-only approval no-op",
            1,
        )[1]
        self.assertIn("if: steps.approve_issuer.outputs.changed == 'false'", no_op)
        self.assertIn(
            "PUBLISH_STEP_OUTCOME: ${{ steps.publish_snapshot.outcome }}",
            no_op,
        )
        self.assertIn(
            '[ "$PUBLISH_STEP_OUTCOME" != "skipped" ]; then',
            no_op,
        )
        self.assertNotIn("DATA_ARCHIVE_TOKEN", no_op)
        self.assertNotIn("publish_private_snapshot.sh", no_op)

    def test_dispatch_binds_exact_reviewed_code_before_credentials(self) -> None:
        workflow = read(WORKFLOW_PATH)
        checkout = workflow.split("- name: Checkout repository", 1)[1].split(
            "\n      - name:", 1
        )[0]
        exact_checkout = workflow.split(
            "- name: Verify exact dispatch code checkout", 1
        )[1].split("\n      - name:", 1)[0]

        self.assertIn("ref: ${{ github.sha }}", checkout)
        self.assertNotIn("ref: main", checkout)
        self.assertIn("EXPECTED_CODE_SHA: ${{ github.sha }}", exact_checkout)
        self.assertIn("actual_code_sha=$(git rev-parse HEAD)", exact_checkout)
        self.assertIn(
            "git fetch --no-tags origin main:refs/remotes/origin/main",
            exact_checkout,
        )
        self.assertIn("current_main_sha=$(git rev-parse origin/main)", exact_checkout)
        self.assertIn('[ "$actual_code_sha" != "$EXPECTED_CODE_SHA" ]', exact_checkout)
        self.assertIn('[ "$current_main_sha" != "$EXPECTED_CODE_SHA" ]', exact_checkout)
        self.assertLess(
            workflow.index("- name: Verify exact dispatch code checkout"),
            workflow.index("- name: Authenticate to the private data repository"),
        )

    def test_private_snapshot_publisher_fails_before_public_tree_drift_mutation(
        self,
    ) -> None:
        workflow = read(WORKFLOW_PATH)
        publisher = read(PUBLISHER_SCRIPT)
        publish_step = workflow.split(
            "- name: Publish validated private-only approval snapshot", 1
        )[1].split("\n      - name:", 1)[0]

        self.assertIn("REQUIRE_PUBLIC_TREE_UNCHANGED: 'true'", publish_step)
        self.assertIn(
            "require_public_tree_unchanged=${REQUIRE_PUBLIC_TREE_UNCHANGED:-false}",
            publisher,
        )
        guard = (
            'if [ "$require_public_tree_unchanged" = true ] &&\n'
            '   [ "$public_tree_unchanged" != true ]; then'
        )
        self.assertIn(guard, publisher)
        self.assertIn(
            "Private-only publication requires an unchanged public artifact",
            publisher,
        )
        guard_at = publisher.index(guard)
        self.assertLess(guard_at, publisher.index('release_tag="dataset-'))
        self.assertLess(guard_at, publisher.index("gh_mutate_once release create"))
        self.assertLess(guard_at, publisher.index("gh_mutate_once release edit"))

    def test_publisher_rechecks_current_main_immediately_before_each_mutation(
        self,
    ) -> None:
        publisher = read(PUBLISHER_SCRIPT)

        self.assertIn("verify_current_main() {", publisher)
        for mutation in (
            "gh_mutate_once release create",
            "gh_mutate_once release upload",
            "gh_mutate_once release edit",
        ):
            mutation_at = publisher.index(mutation)
            preceding_guard = publisher.rfind("verify_current_main", 0, mutation_at)
            preceding_loop = publisher.rfind("for attempt in 0 1 2; do", 0, mutation_at)
            self.assertGreater(preceding_guard, preceding_loop)
            self.assertLess(preceding_guard, mutation_at)

    def test_publisher_rejects_main_advance_after_generation_without_mutation(
        self,
    ) -> None:
        initial_sha = "a" * 40
        advanced_sha = "b" * 40
        dataset_id = "1" * 64
        archive_sha256 = "2" * 64
        public_tree_sha256 = "3" * 64

        with tempfile.TemporaryDirectory(prefix="publisher-main-race-") as raw_temp:
            temp = Path(raw_temp)
            fake_bin = temp / "bin"
            fake_bin.mkdir()
            fetch_count = temp / "fetch-count"
            mutation_log = temp / "mutation.log"
            output_path = temp / "github-output"

            fake_git = fake_bin / "git"
            fake_git.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    if [ "$1" = rev-parse ] && [ "$2" = HEAD ]; then
                      printf '%s\\n' "$INITIAL_CODE_SHA"
                    elif [ "$1" = fetch ] && [ "$2" = --no-tags ]; then
                      count=0
                      if [ -f "$FETCH_COUNT_FILE" ]; then
                        count=$(<"$FETCH_COUNT_FILE")
                      fi
                      printf '%s\\n' "$((count + 1))" > "$FETCH_COUNT_FILE"
                    elif [ "$1" = rev-parse ] && [ "$2" = origin/main ]; then
                      count=$(<"$FETCH_COUNT_FILE")
                      if [ "$count" -ge 2 ]; then
                        printf '%s\\n' "$ADVANCED_CODE_SHA"
                      else
                        printf '%s\\n' "$INITIAL_CODE_SHA"
                      fi
                    else
                      printf 'unexpected fake git command: %s\\n' "$*" >&2
                      exit 97
                    fi
                    """
                ),
                encoding="utf-8",
            )
            fake_git.chmod(0o755)

            fake_python = fake_bin / "python"
            fake_python.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    if [ "$1" = scripts/data_snapshot.py ] && [ "$2" = pack ]; then
                      archive_path="$RUNNER_TEMP/payload.tar.zst"
                      manifest_path="$RUNNER_TEMP/payload.manifest.json"
                      : > "$archive_path"
                      : > "$manifest_path"
                      printf '{"dataset_id":"%s","archive_path":"%s","manifest_path":"%s","archive_sha256":"%s"}\\n' \
                        "$DATASET_ID" "$archive_path" "$manifest_path" "$ARCHIVE_SHA256"
                    elif [ "$1" = scripts/build_pages_artifact.py ]; then
                      printf '{"tree_sha256":"%s"}\\n' "$PUBLIC_TREE_SHA256"
                    elif [ "$1" = scripts/github_cli_retry.py ]; then
                      if [[ " $* " == *" release create "* ]] ||
                         [[ " $* " == *" release upload "* ]] ||
                         [[ " $* " == *" release edit "* ]]; then
                        printf '%s\\n' "$*" >> "$MUTATION_LOG"
                        exit 99
                      fi
                      if [[ " $* " == *" --jq .tag_name "* ]]; then
                        printf '%s\\n' dataset-active
                      else
                        printf '%s\\n' '{"tag_name":"dataset-active","draft":false,"prerelease":false}'
                      fi
                    else
                      printf 'unexpected fake python command: %s\\n' "$*" >&2
                      exit 98
                    fi
                    """
                ),
                encoding="utf-8",
            )
            fake_python.chmod(0o755)

            environment = {
                **os.environ,
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "INITIAL_CODE_SHA": initial_sha,
                "ADVANCED_CODE_SHA": advanced_sha,
                "FETCH_COUNT_FILE": str(fetch_count),
                "MUTATION_LOG": str(mutation_log),
                "RUNNER_TEMP": str(temp),
                "GITHUB_WORKSPACE": str(ROOT),
                "GITHUB_OUTPUT": str(output_path),
                "DATASET_ID": dataset_id,
                "ARCHIVE_SHA256": archive_sha256,
                "PUBLIC_TREE_SHA256": public_tree_sha256,
                "BASE_DATASET_ID": "0" * 64,
                "BASE_PUBLIC_TREE_SHA256": public_tree_sha256,
                "REQUIRE_PUBLIC_TREE_UNCHANGED": "true",
                "DATA_REPOSITORY": "example/private-data",
                "DATA_ARCHIVE_TOKEN": "test-only",
                "PUBLIC_GITHUB_TOKEN": "test-only",
            }
            result = subprocess.run(
                ["bash", PUBLISHER_SCRIPT],
                cwd=ROOT,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(
                "main moved during generation; aborting stale publication",
                result.stdout,
            )
            self.assertEqual(fetch_count.read_text(encoding="utf-8").strip(), "2")
            self.assertFalse(mutation_log.exists(), result.stdout)

    def test_private_approval_is_documented_without_public_authority(self) -> None:
        for path in ("README.md", "ARCHITECTURE.md"):
            with self.subTest(path=path):
                documentation = read(path)
                self.assertIn(
                    "approve-servicenow-insider-ingestion.yml",
                    documentation,
                )
                self.assertIn("scripts/approve_insider_issuer.py", documentation)
                self.assertIn("0001373715", documentation)
                self.assertIn("private-only", documentation)
                self.assertIn("publication-policy-v1", documentation)
                self.assertIn("unchanged", documentation)


if __name__ == "__main__":
    unittest.main()
