import json
import os
import re
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAINTENANCE_WORKFLOWS = (
    ".github/workflows/update-data.yml",
    ".github/workflows/refresh-cusip-registry.yml",
)
PRIVATE_STATE_WORKFLOWS = (
    ".github/workflows/approve-servicenow-insider-ingestion.yml",
    ".github/workflows/initialize-empty-private-insider-authority.yml",
)
PUBLISHING_WORKFLOWS = MAINTENANCE_WORKFLOWS + PRIVATE_STATE_WORKFLOWS
PUBLISHER_SCRIPT = "scripts/publish_private_snapshot.sh"
GH_RETRY_SCRIPT = "scripts/github_cli_retry.py"


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


SHELL_SEQUENCE_HELPER = r"""
next_sequence_value() {
  local sequence=$1
  local counter_file=$2
  local index
  local last_index
  local -a values

  index=$(<"$counter_file")
  IFS=',' read -r -a values <<<"$sequence"
  last_index=$((${#values[@]} - 1))
  if [ "$index" -gt "$last_index" ]; then
    index=$last_index
  fi
  printf '%s\n' "${values[$index]}"
  printf '%s\n' "$((index + 1))" > "$counter_file"
}
"""


class WorkflowResilienceTests(unittest.TestCase):
    @staticmethod
    def _finalization_shell() -> str:
        pages = read(".github/workflows/deploy-pages.yml")
        return pages.split("\n  finalize-private-snapshots:", 1)[1].split(
            "\n  cleanup-public-pages-artifacts:", 1
        )[0]

    def _run_publication_reconciliation(
        self,
        *,
        draft_sequence: str,
        latest_sequence: str,
        mutation_sequence: str,
    ) -> tuple[subprocess.CompletedProcess[str], int]:
        publisher = read(PUBLISHER_SCRIPT)
        function_start = publisher.index("observe_publication() {")
        loop_start = publisher.index("publication_verified=false", function_start)
        function_source = publisher[function_start:loop_start]
        loop_end = publisher.index('\necho "code_sha=', loop_start)
        loop_source = publisher[loop_start:loop_end]

        with tempfile.TemporaryDirectory() as tmpdir:
            counters = Path(tmpdir)
            for name in ("draft", "latest", "mutation"):
                (counters / name).write_text("0\n", encoding="utf-8")
            script = "\n".join(
                (
                    "set -euo pipefail",
                    SHELL_SEQUENCE_HELPER,
                    "readonly TRANSIENT_MUTATION_EXIT_CODE=75",
                    'release_tag="dataset-new"',
                    'expected_latest_release_tag="dataset-old"',
                    'DATA_REPOSITORY="owner/private-data"',
                    "owned_draft=false",
                    "sleep_before_retry() { return 0; }",
                    "verify_current_main() { return 0; }",
                    "verify_base_snapshot_current() { return 0; }",
                    "verify_owned_draft_current() { return 0; }",
                    "verify_remote_snapshot() { return 0; }",
                    'abort_with_owned_draft_cleanup() { echo "::error::$1"; exit 1; }',
                    r"""
gh_read_retry() {
  if [ "$1" = release ] && [ "$2" = view ]; then
    next_sequence_value "$DRAFT_SEQUENCE" "$COUNTER_DIR/draft"
    return 0
  fi
  if [ "$1" = api ]; then
    next_sequence_value "$LATEST_SEQUENCE" "$COUNTER_DIR/latest"
    return 0
  fi
  return 99
}
gh_mutate_once() {
  local status
  status=$(next_sequence_value "$MUTATION_SEQUENCE" "$COUNTER_DIR/mutation")
  return "$status"
}
""",
                    function_source,
                    loop_source,
                    'printf "verified=%s\\n" "$publication_verified"',
                )
            )
            env = os.environ.copy()
            env.update(
                {
                    "COUNTER_DIR": tmpdir,
                    "DRAFT_SEQUENCE": draft_sequence,
                    "LATEST_SEQUENCE": latest_sequence,
                    "MUTATION_SEQUENCE": mutation_sequence,
                }
            )
            result = subprocess.run(
                ["bash"],
                cwd=ROOT,
                env=env,
                input=script,
                text=True,
                capture_output=True,
                check=False,
            )
            mutation_count = int(
                (counters / "mutation").read_text(encoding="utf-8").strip()
            )
        return result, mutation_count

    def _run_marker_reconciliation(
        self,
        *,
        marker_sequence: str,
        mutation_sequence: str,
    ) -> tuple[subprocess.CompletedProcess[str], int]:
        finalization = self._finalization_shell()
        function_start = finalization.index("          wait_for_marker_match() {")
        function_end = finalization.index(
            "          observe_latest_release() {", function_start
        )
        function_source = textwrap.dedent(
            finalization[function_start:function_end]
        )
        loop_start = finalization.index("          marker_verified=false")
        loop_end = finalization.index("\n\n          active_release_tag=", loop_start)
        loop_source = textwrap.dedent(finalization[loop_start:loop_end])

        with tempfile.TemporaryDirectory() as tmpdir:
            counters = Path(tmpdir)
            for name in ("marker", "mutation"):
                (counters / name).write_text("0\n", encoding="utf-8")
            script = "\n".join(
                (
                    "set -euo pipefail",
                    SHELL_SEQUENCE_HELPER,
                    "readonly TRANSIENT_MUTATION_EXIT_CODE=75",
                    'EXPECTED_RELEASE_TAG="dataset-new"',
                    'DATA_REPOSITORY="owner/private-data"',
                    'marker_path="/tmp/pages-deployment.json"',
                    "sleep_before_retry() { return 0; }",
                    "verify_target_snapshot_current() { return 0; }",
                    r"""
marker_matches_remote() {
  local status
  status=$(next_sequence_value "$MARKER_SEQUENCE" "$COUNTER_DIR/marker")
  return "$status"
}
gh_mutate_once() {
  local status
  status=$(next_sequence_value "$MUTATION_SEQUENCE" "$COUNTER_DIR/mutation")
  return "$status"
}
""",
                    function_source,
                    loop_source,
                    'printf "verified=%s\\n" "$marker_verified"',
                )
            )
            env = os.environ.copy()
            env.update(
                {
                    "COUNTER_DIR": tmpdir,
                    "MARKER_SEQUENCE": marker_sequence,
                    "MUTATION_SEQUENCE": mutation_sequence,
                }
            )
            result = subprocess.run(
                ["bash"],
                cwd=ROOT,
                env=env,
                input=script,
                text=True,
                capture_output=True,
                check=False,
            )
            mutation_count = int(
                (counters / "mutation").read_text(encoding="utf-8").strip()
            )
        return result, mutation_count

    def _run_rollback_reconciliation(
        self,
        *,
        latest_sequence: str,
        mutation_sequence: str,
    ) -> tuple[subprocess.CompletedProcess[str], int]:
        finalization = self._finalization_shell()
        function_start = finalization.index("          observe_latest_release() {")
        function_end = finalization.index(
            "\n\n          if [[ ! \"$EXPECTED_PREVIOUS_LATEST_RELEASE_TAG\"",
            function_start,
        )
        function_source = textwrap.dedent(
            finalization[function_start:function_end]
        )
        loop_start = finalization.index("            rollback_verified=false")
        loop_end = finalization.index(
            "\n          elif [ \"$active_release_tag\"", loop_start
        )
        loop_source = textwrap.dedent(finalization[loop_start:loop_end])

        with tempfile.TemporaryDirectory() as tmpdir:
            counters = Path(tmpdir)
            for name in ("latest", "mutation"):
                (counters / name).write_text("0\n", encoding="utf-8")
            script = "\n".join(
                (
                    "set -euo pipefail",
                    SHELL_SEQUENCE_HELPER,
                    "readonly TRANSIENT_MUTATION_EXIT_CODE=75",
                    'EXPECTED_RELEASE_TAG="dataset-new"',
                    'EXPECTED_PREVIOUS_LATEST_RELEASE_TAG="dataset-old"',
                    'DATA_REPOSITORY="owner/private-data"',
                    "sleep_before_retry() { return 0; }",
                    "verify_target_snapshot_current() { return 0; }",
                    r"""
gh_read_retry() {
  next_sequence_value "$LATEST_SEQUENCE" "$COUNTER_DIR/latest"
}
gh_mutate_once() {
  local status
  status=$(next_sequence_value "$MUTATION_SEQUENCE" "$COUNTER_DIR/mutation")
  return "$status"
}
""",
                    function_source,
                    loop_source,
                    'printf "verified=%s active=%s\\n" "$rollback_verified" "$active_release_tag"',
                )
            )
            env = os.environ.copy()
            env.update(
                {
                    "COUNTER_DIR": tmpdir,
                    "LATEST_SEQUENCE": latest_sequence,
                    "MUTATION_SEQUENCE": mutation_sequence,
                }
            )
            result = subprocess.run(
                ["bash"],
                cwd=ROOT,
                env=env,
                input=script,
                text=True,
                capture_output=True,
                check=False,
            )
            mutation_count = int(
                (counters / "mutation").read_text(encoding="utf-8").strip()
            )
        return result, mutation_count

    def _run_nonrollback_finalization_until_cleanup(
        self,
        *,
        active_release_json: str,
        expected_previous_latest_release_tag: str = "dataset-expected",
    ) -> tuple[subprocess.CompletedProcess[str], int]:
        finalization = self._finalization_shell()
        block_start = finalization.index(
            '          release_json=$(gh_read_retry api "/repos/$DATA_REPOSITORY/releases/tags/$EXPECTED_RELEASE_TAG")'
        )
        block_end = finalization.index(
            "\n          # GitHub exposes no conditional/versioned delete", block_start
        )
        block_source = textwrap.dedent(finalization[block_start:block_end])

        expected_release_json = (
            '{"tag_name":"dataset-expected","draft":false,'
            '"prerelease":false,"published_at":"2026-08-21T20:09:07Z"}'
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            counters = Path(tmpdir)
            mutation_counter = counters / "mutation"
            mutation_counter.write_text("0\n", encoding="utf-8")
            script = "\n".join(
                (
                    "set -euo pipefail",
                    "readonly TRANSIENT_MUTATION_EXIT_CODE=75",
                    'ALLOW_OLDER_RELEASE="false"',
                    'EXPECTED_CODE_SHA="d3da4c385b1235e0aacb50982ddf454a6f182d55"',
                    'EXPECTED_DATASET_ID="5e8b2383befa7ad2d6cb2109e93cf9ec38c3fd7c725b5da0d47a25478d624d67"',
                    'EXPECTED_RELEASE_TAG="dataset-expected"',
                    (
                        'EXPECTED_PREVIOUS_LATEST_RELEASE_TAG="'
                        f'{expected_previous_latest_release_tag}"'
                    ),
                    'DATA_REPOSITORY="owner/private-data"',
                    'GITHUB_REPOSITORY="owner/public"',
                    'PUBLIC_GITHUB_TOKEN="public-token"',
                    'RUNNER_TEMP="$COUNTER_DIR"',
                    r"""
gh_read_retry() {
  if [ "$1" = api ] && [[ "$2" == */releases/tags/* ]]; then
    printf '%s\n' "$EXPECTED_RELEASE_JSON"
    return 0
  fi
  if [ "$1" = release ] && [ "$2" = download ]; then
    local output_dir=""
    while [ "$#" -gt 0 ]; do
      if [ "$1" = --dir ]; then
        output_dir=$2
        break
      fi
      shift
    done
    mkdir -p "$output_dir"
    printf '{"dataset_id":"%s"}\n' "$EXPECTED_DATASET_ID" > "$output_dir/snapshot.manifest.json"
    return 0
  fi
  if [ "$1" = api ] && [[ "$2" == */deployments\?* ]]; then
    printf '%s\n' '[{"id":101}]'
    return 0
  fi
  if [ "$1" = api ] && [[ "$2" == */deployments/101/statuses\?* ]]; then
    printf '%s\n' '[{"state":"success","created_at":"2026-08-21T21:10:00Z"}]'
    return 0
  fi
  if [ "$1" = api ] && [[ "$2" == */releases/latest ]]; then
    if [ "${3:-}" = --jq ]; then
      jq -r "${4:-.}" <<<"$ACTIVE_RELEASE_JSON"
    else
      printf '%s\n' "$ACTIVE_RELEASE_JSON"
    fi
    return 0
  fi
  return 99
}
gh_mutate_once() {
  local count
  count=$(<"$COUNTER_DIR/mutation")
  printf '%s\n' "$((count + 1))" > "$COUNTER_DIR/mutation"
  return 0
}
wait_for_marker_match() { return 0; }
sleep_before_retry() { return 0; }
verify_target_snapshot_current() { return 0; }
observe_latest_release() { return 99; }
mapfile() {
  local value
  deployment_ids=()
  while IFS= read -r value; do
    deployment_ids[${#deployment_ids[@]}]=$value
  done
}
""",
                    block_source,
                    'printf "reached-cleanup=true\\n"',
                )
            )
            env = os.environ.copy()
            env.update(
                {
                    "ACTIVE_RELEASE_JSON": active_release_json,
                    "COUNTER_DIR": tmpdir,
                    "EXPECTED_RELEASE_JSON": expected_release_json,
                }
            )
            result = subprocess.run(
                ["bash"],
                cwd=ROOT,
                env=env,
                input=script,
                text=True,
                capture_output=True,
                check=False,
            )
            mutation_count = int(mutation_counter.read_text(encoding="utf-8").strip())
        return result, mutation_count

    def test_lxml_dependency_requires_patched_release(self):
        requirement = next(
            line
            for line in read("requirements.txt").splitlines()
            if line.startswith("lxml")
        )
        match = re.fullmatch(
            r"lxml>=(\d+)\.(\d+)\.(\d+)(?:,.*)?",
            requirement,
        )

        if match is None:
            self.fail(f"unexpected lxml requirement format: {requirement}")
        self.assertGreaterEqual(
            tuple(int(part) for part in match.groups()),
            (6, 1, 0),
        )

    def test_critical_schedules_avoid_top_of_hour_without_changing_windows(self):
        update = read(".github/workflows/update-data.yml")
        refresh = read(".github/workflows/refresh-cusip-registry.yml")

        self.assertIn("cron: '23 11-23 * * 1-5'", update)
        self.assertIn("cron: '23 4 * * 0'", refresh)
        self.assertNotRegex(update, r"(?m)^\s*- cron: '0 ")
        self.assertNotRegex(refresh, r"(?m)^\s*- cron: '0 ")

    def test_manual_update_can_force_replay_one_cik(self):
        workflow = read(".github/workflows/update-data.yml")
        dispatch = workflow.split("  workflow_dispatch:", 1)[1].split(
            "\nconcurrency:", 1
        )[0]

        self.assertRegex(
            dispatch,
            r"(?ms)^      filing_cik:\n.*?^        required: false$",
        )
        self.assertIn("FILING_CIK: ${{ inputs.filing_cik || '' }}", workflow)
        self.assertIn('[[ ! "$FILING_CIK" =~ ^[1-9][0-9]*$ ]]', workflow)
        self.assertIn('mode=(--cik "$FILING_CIK")', workflow)
        self.assertIn('echo "targeted_cik=$targeted_cik" >> "$GITHUB_OUTPUT"', workflow)
        self.assertIn("steps.pipeline.outputs.targeted_cik != 'true'", workflow)

    def test_manual_insider_inputs_are_explicit_and_default_off(self):
        workflow = read(".github/workflows/update-data.yml")
        dispatch = workflow.split("  workflow_dispatch:", 1)[1].split(
            "\nconcurrency:", 1
        )[0]

        mode = dispatch.split("      insider_mode:", 1)[1].split(
            "\n      insider_issuer_cik:", 1
        )[0]
        self.assertIn("default: 'off'", mode)
        self.assertIn("type: choice", mode)
        for choice in ("off", "incremental", "backfill", "reparse"):
            self.assertIn(f"- {choice}", mode)

        for field, default, input_type in (
            ("insider_issuer_cik", "''", "string"),
            ("insider_quarter", "''", "string"),
            ("insider_max_accessions", "25", "number"),
            ("insider_deadline_seconds", "600", "number"),
        ):
            with self.subTest(field=field):
                field_block = dispatch.split(f"      {field}:", 1)[1]
                self.assertIn(f"default: {default}", field_block)
                self.assertIn(f"type: {input_type}", field_block)

    def test_public_insider_materialization_is_manual_explicit_and_default_off(self):
        workflow = read(".github/workflows/update-data.yml")
        dispatch = workflow.split("  workflow_dispatch:", 1)[1].split(
            "\nconcurrency:", 1
        )[0]

        publish = dispatch.split("      publish_insider_publication:", 1)[1].split(
            "\n      insider_publication_as_of:", 1
        )[0]
        self.assertIn("required: false", publish)
        self.assertIn("default: false", publish)
        self.assertIn("type: boolean", publish)
        for field, default in (
            ("insider_publication_as_of", "''"),
            ("insider_publication_latest_successful_sync_at", "'none'"),
        ):
            with self.subTest(field=field):
                field_block = dispatch.split(f"      {field}:", 1)[1]
                self.assertIn("required: false", field_block)
                self.assertIn(f"default: {default}", field_block)
                self.assertIn("type: string", field_block)

        resolver = workflow.split(
            "- name: Resolve bounded insider maintenance plan", 1
        )[1].split("\n      - name:", 1)[0]
        self.assertIn(
            "REQUESTED_PUBLISH: ${{ inputs.publish_insider_publication || 'false' }}",
            resolver,
        )
        self.assertIn(
            "REQUESTED_PUBLICATION_AS_OF: ${{ inputs.insider_publication_as_of || '' }}",
            resolver,
        )
        self.assertIn(
            "REQUESTED_PUBLICATION_LATEST_SUCCESSFUL_SYNC_AT: ${{ inputs.insider_publication_latest_successful_sync_at || 'none' }}",
            resolver,
        )
        self.assertIn("publish=false", resolver)
        dispatch_branch = resolver.split(
            'elif [ "$EVENT_NAME" = "workflow_dispatch" ]; then', 1
        )[1].split("\n          fi", 1)[0]
        self.assertIn('publish="$REQUESTED_PUBLISH"', dispatch_branch)
        schedule_branch = resolver.split('if [ "$EVENT_NAME" = "schedule" ]; then', 1)[
            1
        ].split('elif [ "$EVENT_NAME" = "workflow_dispatch" ]; then', 1)[0]
        self.assertNotIn("REQUESTED_PUBLISH", schedule_branch)
        self.assertIn(
            'if [ "$publish" = "true" ] && [ "$mode" = "off" ]; then', resolver
        )
        self.assertIn(
            "Public insider publication requires a maintenance mode", resolver
        )
        for output in (
            "publish",
            "publication_as_of",
            "publication_latest_successful_sync_at",
        ):
            self.assertIn(f'echo "{output}=', resolver)

        materialize = workflow.split(
            "- name: Materialize approved public insider publication", 1
        )[1].split("\n      - name:", 1)[0]
        self.assertIn(
            "if: ${{ steps.insider_plan.outputs.publish == 'true' }}",
            materialize,
        )
        self.assertIn("timeout-minutes: 15", materialize)
        for environment_name in (
            "INSIDER_MODE",
            "INSIDER_ISSUER_CIK",
            "INSIDER_QUARTER",
            "INSIDER_MAX_ACCESSIONS",
            "INSIDER_PUBLICATION_AS_OF",
            "INSIDER_PUBLICATION_LATEST_SUCCESSFUL_SYNC_AT",
        ):
            self.assertIn(f"{environment_name}:", materialize)
        self.assertIn("python scripts/publish_insider_activity.py", materialize)
        self.assertIn('--maintenance-mode "$INSIDER_MODE"', materialize)
        self.assertIn('--maintenance-issuer-cik "$INSIDER_ISSUER_CIK"', materialize)
        self.assertIn(
            '--maintenance-max-accessions "$INSIDER_MAX_ACCESSIONS"', materialize
        )
        self.assertIn('--as-of "$INSIDER_PUBLICATION_AS_OF"', materialize)
        self.assertIn(
            '--latest-successful-sync-at "$INSIDER_PUBLICATION_LATEST_SUCCESSFUL_SYNC_AT"',
            materialize,
        )
        self.assertIn('--maintenance-quarter "$INSIDER_QUARTER"', materialize)
        self.assertNotIn("SEC_USER_AGENT", materialize)
        self.assertNotIn("OPENFIGI_API_KEY", materialize)

        self.assertLess(
            workflow.index("- name: Validate private insider checkpoint state"),
            workflow.index("- name: Materialize approved public insider publication"),
        )
        self.assertLess(
            workflow.index("- name: Materialize approved public insider publication"),
            workflow.index("- name: Validate generated data"),
        )

    def test_insider_plan_is_bounded_and_scheduled_execution_is_opt_in(self):
        workflow = read(".github/workflows/update-data.yml")
        resolver = workflow.split(
            "- name: Resolve bounded insider maintenance plan", 1
        )[1].split("\n      - name:", 1)[0]

        self.assertIn(
            "SCHEDULED_ENABLED: ${{ vars.ENABLE_SCHEDULED_INSIDER_INGESTION }}",
            resolver,
        )
        self.assertIn(
            "SCHEDULED_ISSUER_CIK: ${{ vars.SCHEDULED_INSIDER_ISSUER_CIK }}",
            resolver,
        )
        self.assertIn('[ "$EVENT_NAME" = "schedule" ]', resolver)
        self.assertIn('[ "$SCHEDULED_ENABLED" = "true" ]', resolver)
        self.assertNotIn('[ -n "$SCHEDULED_ENABLED" ]', resolver)
        self.assertIn("mode=off", resolver)
        self.assertIn("mode=incremental", resolver)
        self.assertIn("Backfill and reparse are manual-only", resolver)
        self.assertIn('[[ ! "$issuer_cik" =~ ^[0-9]{1,10}$ ]]', resolver)
        self.assertIn('[[ "$issuer_cik" =~ ^0+$ ]]', resolver)
        self.assertIn('[[ ! "$max_accessions" =~ ^[1-9][0-9]*$ ]]', resolver)
        self.assertIn('"$max_accessions" -gt 100', resolver)
        self.assertIn('[[ ! "$deadline_seconds" =~ ^[1-9][0-9]*$ ]]', resolver)
        self.assertIn('"$deadline_seconds" -lt 60', resolver)
        self.assertIn('"$deadline_seconds" -gt 840', resolver)
        self.assertIn('[[ ! "$quarter" =~ ^[0-9]{4}Q[1-4]$ ]]', resolver)
        self.assertIn('if [ -n "$quarter" ]; then', resolver)
        for output in (
            "mode",
            "issuer_cik",
            "quarter",
            "max_accessions",
            "deadline_seconds",
        ):
            self.assertIn(f'echo "{output}=', resolver)

    def test_bounded_insider_step_is_sequential_and_checkpoint_aware(self):
        workflow = read(".github/workflows/update-data.yml")
        preflight = workflow.split(
            "- name: Resolve validated insider resume state", 1
        )[1].split("\n      - name:", 1)[0]
        insider = workflow.split("- name: Run bounded insider maintenance", 1)[
            1
        ].split("- name: Validate private insider checkpoint state", 1)[0]
        validation = workflow.split(
            "- name: Validate private insider checkpoint state", 1
        )[1].split("\n      - name:", 1)[0]

        self.assertIn("InsiderStateStore", preflight)
        self.assertIn("resolve_incremental_checkpoint_action", preflight)
        self.assertIn('state_store.read("incremental-v1")', preflight)
        self.assertIn('state_store.read(f"backfill/{quarter}")', preflight)
        self.assertIn('state_store.read("reparse-v1")', preflight)
        self.assertIn("INSIDER_PARSER_VERSION", preflight)
        self.assertIn('{"running", "incomplete"}', preflight)
        self.assertIn("existing backfill checkpoint is not resumable", preflight)
        self.assertIn("existing reparse checkpoint is not resumable", preflight)
        self.assertNotIn("json.load", preflight)
        self.assertNotIn("Path.exists", preflight)

        self.assertIn(
            "if: ${{ steps.insider_plan.outputs.mode != 'off' }}",
            insider,
        )
        self.assertIn("timeout-minutes: 15", insider)
        self.assertIn("SEC_MAX_REQUESTS_PER_SECOND: '5'", insider)
        self.assertIn("pipeline.require_declared_sec_user_agent()", insider)
        self.assertIn("python scripts/refresh_recent_insider_filings.py", insider)
        self.assertIn("python scripts/backfill_insider_transactions.py", insider)
        self.assertIn("python scripts/reparse_insider_filings.py", insider)
        self.assertIn('if [ "$INSIDER_ACTION" = "resume" ]; then', insider)
        incremental_branch = insider.split("incremental)", 1)[1].split(";;", 1)[0]
        self.assertIn("set +e", incremental_branch)
        self.assertIn("incremental_status=$?", incremental_branch)
        self.assertIn('if [ "$incremental_status" -eq 75 ]; then', incremental_branch)
        self.assertIn('elif [ "$incremental_status" -ne 0 ]; then', incremental_branch)
        self.assertIn('if [ "$reparse_status" -eq 75 ]; then', insider)
        self.assertIn('elif [ "$reparse_status" -ne 0 ]; then', insider)
        self.assertNotIn("reparse-telemetry-before.json", insider)
        self.assertNotIn("--all", insider)
        self.assertNotIn("--refetch", insider)
        self.assertNotIn("continue-on-error", insider)
        self.assertNotIn("strategy:", insider)
        self.assertNotIn("xargs -P", insider)

        self.assertIn('state_store.read("incremental-v1")', validation)
        self.assertIn("validate_incremental_checkpoint_scope", validation)
        incremental_validation = validation.split(
            'if mode == "incremental":', 1
        )[1].split('elif mode == "backfill":', 1)[0]
        self.assertIn(
            'expected_statuses = {"running", "incomplete"} if cooperative_checkpoint else {"completed"}',
            incremental_validation,
        )
        self.assertIn(
            'checkpoint["status"] not in expected_statuses',
            incremental_validation,
        )
        self.assertIn('state_store.read(f"backfill/{quarter}")', validation)
        self.assertIn('state_store.read("reparse-v1")', validation)
        self.assertIn('state_store.read("telemetry-v1")', validation)
        self.assertIn("cooperative_checkpoint", validation)
        self.assertIn("reparse checkpoint did not validate", validation)

        ordered_steps = (
            "- name: Restore latest validated private snapshot",
            "- name: Run pipeline",
            "- name: Refresh recently accepted 13F filings",
            "- name: Regenerate registry-backed site data",
            "- name: Resolve bounded insider maintenance plan",
            "- name: Resolve validated insider resume state",
            "- name: Run bounded insider maintenance",
            "- name: Validate private insider checkpoint state",
            "- name: Materialize approved public insider publication",
            "- name: Validate generated data",
            "- name: Run full Python regression suite",
            "- name: Publish validated private snapshot",
        )
        offsets = tuple(workflow.index(step) for step in ordered_steps)
        self.assertEqual(tuple(sorted(offsets)), offsets)

    def test_private_only_snapshot_preserves_active_pages_target(self):
        workflow = read(".github/workflows/update-data.yml")
        publisher = read(PUBLISHER_SCRIPT)

        baseline = workflow.split(
            "- name: Capture restored public artifact identity", 1
        )[1].split("\n      - name:", 1)[0]
        self.assertIn("python scripts/build_pages_artifact.py", baseline)
        self.assertIn('echo "tree_sha256=$base_public_tree_sha256"', baseline)
        self.assertLess(
            workflow.index("- name: Capture restored public artifact identity"),
            workflow.index("- name: Run pipeline"),
        )
        publish = workflow.split("- name: Publish validated private snapshot", 1)[
            1
        ].split("\n  deploy-pages:", 1)[0]
        self.assertIn(
            "BASE_PUBLIC_TREE_SHA256: ${{ steps.base_public.outputs.tree_sha256 }}",
            publish,
        )

        for fragment in (
            'if [ -n "${BASE_PUBLIC_TREE_SHA256:-}" ]; then',
            "python scripts/build_pages_artifact.py",
            'if [ "$current_public_tree_sha256" = "$BASE_PUBLIC_TREE_SHA256" ]; then',
            "--latest=false",
            'echo "site_changed=false"',
        ):
            self.assertIn(fragment, publisher)
        self.assertNotIn("data/insiders", baseline)

    def test_keepalive_is_twice_monthly_empty_and_strictly_off_main(self):
        workflow = read(".github/workflows/keepalive.yml")

        self.assertIn("cron: '37 5 1,15 * *'", workflow)
        self.assertNotRegex(workflow, r"(?m)^  push:")
        self.assertIn("permissions:\n  contents: read", workflow)
        keepalive_job = workflow.split("\n  keepalive:", 1)[1]
        self.assertIn("permissions:\n      contents: write", keepalive_job)
        self.assertIn("token: ${{ github.token }}", keepalive_job)
        self.assertIn("KEEPALIVE_BRANCH: automation-keepalive", keepalive_job)
        self.assertIn('git switch --orphan "$KEEPALIVE_BRANCH"', keepalive_job)
        self.assertIn("git commit --allow-empty", keepalive_job)
        self.assertIn(
            'git push origin "HEAD:refs/heads/$KEEPALIVE_BRANCH"',
            keepalive_job,
        )
        self.assertNotIn("HEAD:refs/heads/main", keepalive_job)

    def test_pages_has_reusable_recovery_and_code_only_push_triggers(self):
        workflow = read(".github/workflows/deploy-pages.yml")

        for trigger in ("workflow_call", "workflow_dispatch", "push", "schedule"):
            self.assertRegex(workflow, rf"(?m)^  {trigger}:$")
        self.assertIn("cron: '17 */6 * * *'", workflow)
        self.assertIn("release_tag:", workflow.split("  workflow_dispatch:", 1)[1])

        for public_code_path in (
            "index.html",
            "site-data-loader.js",
            "scripts/build_pages_artifact.py",
            "scripts/data_snapshot.py",
            "scripts/pages_deploy_needed.sh",
        ):
            self.assertIn(f"- '{public_code_path}'", workflow)
        self.assertNotIn("- 'data/funds/**'", workflow)
        self.assertNotIn("- 'data/stocks/**'", workflow)

    def test_reusable_pages_contract_is_code_release_and_dataset_exact(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        workflow_call = workflow.split("  workflow_call:", 1)[1].split(
            "\n  workflow_dispatch:", 1
        )[0]

        for field in ("code_sha", "release_tag", "dataset_id"):
            with self.subTest(field=field):
                self.assertRegex(
                    workflow_call,
                    rf"(?ms)^      {field}:\n.*?^        required: true$",
                )
        self.assertIn(
            "code_sha: ${{ steps.target.outputs.code_sha }}", workflow
        )
        self.assertIn(
            "release_tag: ${{ steps.target.outputs.release_tag }}", workflow
        )
        self.assertIn(
            "dataset_id: ${{ steps.target.outputs.dataset_id }}", workflow
        )

    def test_pages_resolve_target_checkouts_are_sparse_and_blobless(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        resolve = workflow.split("  resolve:", 1)[1].split("\n  build:", 1)[0]
        target_checkouts = resolve.split(
            "- name: Checkout trusted GitHub retry and snapshot identity helpers", 1
        )[0]
        checkouts = re.findall(
            r"(?ms)^      - name: Checkout .*?(?=^      - name: |\Z)",
            target_checkouts,
        )

        self.assertEqual(3, len(checkouts))
        for checkout in checkouts:
            with self.subTest(checkout=checkout.splitlines()[0]):
                self.assertIn("filter: blob:none", checkout)
                self.assertIn("sparse-checkout: |", checkout)
                self.assertIn("CNAME", checkout)
                self.assertIn("scripts/pages_deploy_needed.sh", checkout)

    def test_every_private_data_job_uses_the_scoped_github_app(self):
        expected_action = (
            "uses: actions/create-github-app-token@"
            "bcd2ba49218906704ab6c1aa796996da409d3eb1 # v3"
        )
        expected_repository = "repositories: super-investor-seeker-data"
        for path, minimum in (
            (".github/workflows/update-data.yml", 1),
            (".github/workflows/refresh-cusip-registry.yml", 1),
            (".github/workflows/deploy-pages.yml", 4),
        ):
            with self.subTest(path=path):
                workflow = read(path)
                self.assertGreaterEqual(workflow.count(expected_action), minimum)
                self.assertEqual(
                    workflow.count(expected_action),
                    workflow.count(expected_repository),
                )
                self.assertIn(
                    "client-id: ${{ vars.DATA_ARCHIVE_APP_CLIENT_ID }}", workflow
                )
                self.assertIn(
                    "private-key: ${{ secrets.DATA_ARCHIVE_APP_PRIVATE_KEY }}",
                    workflow,
                )
                self.assertNotIn("secrets.DATA_ARCHIVE_TOKEN", workflow)

    def test_long_maintenance_jobs_refresh_their_app_token_before_publish(self):
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                refresh_at = workflow.index(
                    "- name: Refresh private data publication credential"
                )
                publish_at = workflow.index("- name: Publish ", refresh_at)
                self.assertLess(refresh_at, publish_at)
                publish = workflow[publish_at:]
                self.assertIn(
                    "steps.publish-data-app-token.outputs.token", publish
                )
                self.assertNotIn("steps.data-app-token.outputs.token", publish)

    def test_private_publisher_workflows_pin_owner_and_checkout_credentials(self):
        workflow_paths = MAINTENANCE_WORKFLOWS + (".github/workflows/deploy-pages.yml",)
        for path in workflow_paths:
            with self.subTest(path=path):
                workflow = read(path)
                self.assertIn(
                    "DATA_REPOSITORY: wesley-yon/super-investor-seeker-data",
                    workflow,
                )
                self.assertNotIn("${{ github.repository_owner }}", workflow)

                lines = workflow.splitlines()
                checkout_count = 0
                for index, line in enumerate(lines):
                    if "uses: actions/checkout@" not in line:
                        continue
                    checkout_count += 1
                    uses_indent = len(line) - len(line.lstrip())
                    step_indent = uses_indent - 2
                    block = [line]
                    for candidate in lines[index + 1 :]:
                        candidate_indent = len(candidate) - len(candidate.lstrip())
                        if (
                            candidate_indent == step_indent
                            and candidate.lstrip().startswith("- name:")
                        ):
                            break
                        if candidate.strip() and candidate_indent < step_indent:
                            break
                        block.append(candidate)
                    self.assertIn(
                        "persist-credentials: false",
                        "\n".join(block),
                        f"{path}:{index + 1} persists checkout credentials",
                    )
                self.assertGreater(checkout_count, 0)

    def test_maintenance_workflows_use_one_shared_snapshot_publisher(self):
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                self.assertEqual(
                    1,
                    workflow.count(f"bash {PUBLISHER_SCRIPT}"),
                )
                self.assertNotIn("python scripts/data_snapshot.py pack", workflow)

        publisher = read(PUBLISHER_SCRIPT)
        for fragment in (
            "python scripts/data_snapshot.py pack",
            "git fetch --no-tags origin main:refs/remotes/origin/main",
            'gh_mutate_once release create "$release_tag"',
            "python scripts/data_snapshot.py verify",
            'gh_mutate_once release edit "$release_tag"',
            'echo "site_changed=true"',
        ):
            self.assertIn(fragment, publisher)

    def test_pages_resolve_uses_bounded_retries_for_private_release_reads(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        resolve = workflow.split("\n  resolve:", 1)[1].split("\n  build:", 1)[0]

        trusted_helper_checkout = resolve.split(
            "- name: Checkout trusted GitHub retry and snapshot identity helpers", 1
        )[1].split("\n      - name:", 1)[0]
        self.assertIn(
            "uses: actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09 # v5.1.0",
            trusted_helper_checkout,
        )
        self.assertIn("repository: ${{ job.workflow_repository }}", trusted_helper_checkout)
        self.assertIn("ref: ${{ job.workflow_sha }}", trusted_helper_checkout)
        self.assertIn("path: .workflow-tools", trusted_helper_checkout)
        self.assertIn("sparse-checkout: |", trusted_helper_checkout)
        self.assertIn("scripts/data_snapshot.py", trusted_helper_checkout)
        self.assertIn(GH_RETRY_SCRIPT, trusted_helper_checkout)
        target_checkouts = resolve.split(
            "- name: Checkout trusted GitHub retry and snapshot identity helpers", 1
        )[0]
        self.assertEqual(3, target_checkouts.count(f"{GH_RETRY_SCRIPT}\n"))
        self.assertIn(
            f'python "$GITHUB_WORKSPACE/.workflow-tools/{GH_RETRY_SCRIPT}" \\\n'
            '              --retry-forbidden-read -- "$@"',
            resolve,
        )
        self.assertIn(
            'latest_release_tag=$(\n            gh_read_retry api '
            '"/repos/$DATA_REPOSITORY/releases/latest"',
            resolve,
        )
        self.assertIn('release_tag="$REQUESTED_RELEASE_TAG"', resolve)
        self.assertIn(
            'release_json=$(gh_read_retry api "/repos/$DATA_REPOSITORY/releases/tags/$release_tag")',
            resolve,
        )
        self.assertIn(
            'echo "resolved_latest_release_tag=$latest_release_tag" >> "$GITHUB_OUTPUT"',
            resolve,
        )
        download = resolve.split(
            'gh_read_retry release download "$release_tag"', 1
        )[1].split('if [ "$(find "$manifest_dir"', 1)[0]
        self.assertIn("--clobber", download)

    def test_private_release_mutations_share_a_lock_and_rollback_precondition(self):
        update = read(".github/workflows/update-data.yml")
        refresh = read(".github/workflows/refresh-cusip-registry.yml")
        pages = read(".github/workflows/deploy-pages.yml")
        private_lock = (
            "concurrency:\n"
            "      group: private-release-publication\n"
            "      cancel-in-progress: false\n"
            "      queue: max"
        )
        update_job = update.split("\n  update:", 1)[1].split(
            "\n  deploy-pages:", 1
        )[0]
        refresh_job = refresh.split("\n  refresh-cusips:", 1)[1].split(
            "\n  deploy-pages:", 1
        )[0]
        finalization = pages.split(
            "\n  finalize-private-snapshots:", 1
        )[1].split("\n  cleanup-public-pages-artifacts:", 1)[0]

        for job in (update_job, refresh_job, finalization):
            self.assertIn(private_lock, job)
        self.assertIn(
            "resolved_latest_release_tag: "
            "${{ steps.target.outputs.resolved_latest_release_tag }}",
            pages,
        )
        self.assertIn(
            "EXPECTED_PREVIOUS_LATEST_RELEASE_TAG: "
            "${{ needs.resolve.outputs.resolved_latest_release_tag }}",
            finalization,
        )
        self.assertIn(
            'observe_latest_release \\\n                "$EXPECTED_RELEASE_TAG" \\\n                "$EXPECTED_PREVIOUS_LATEST_RELEASE_TAG"',
            finalization,
        )
        self.assertIn("Refusing stale rollback", finalization)
        self.assertIn(
            "uses: actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09 # v5.1.0",
            finalization,
        )
        self.assertIn("repository: ${{ job.workflow_repository }}", finalization)
        self.assertIn("ref: ${{ job.workflow_sha }}", finalization)
        self.assertNotIn("ref: ${{ needs.resolve.outputs.code_sha }}", finalization)
        self.assertNotIn("uses: actions/checkout@v5", finalization)

    def test_confirmed_publication_mutation_is_not_replayed_while_reads_converge(
        self,
    ):
        result, mutation_count = self._run_publication_reconciliation(
            draft_sequence="true,true,false",
            latest_sequence="dataset-old,dataset-old,dataset-new",
            mutation_sequence="0",
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("verified=true", result.stdout)
        self.assertEqual(1, mutation_count)

    def test_uncertain_publication_mutation_reconciles_without_replay(self):
        result, mutation_count = self._run_publication_reconciliation(
            draft_sequence="true,false",
            latest_sequence="dataset-old,dataset-new",
            mutation_sequence="75",
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("verified=true", result.stdout)
        self.assertEqual(1, mutation_count)

    def test_publication_refuses_changed_latest_pointer_after_mutation(self):
        for mutation_status in ("0", "75"):
            with self.subTest(mutation_status=mutation_status):
                result, mutation_count = self._run_publication_reconciliation(
                    draft_sequence="true,false",
                    latest_sequence="dataset-old,dataset-unexpected",
                    mutation_sequence=mutation_status,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn("latest release pointer changed during", result.stdout)
                self.assertEqual(1, mutation_count)

    def test_publication_refuses_changed_latest_pointer_without_mutating(self):
        result, mutation_count = self._run_publication_reconciliation(
            draft_sequence="true",
            latest_sequence="dataset-unexpected",
            mutation_sequence="0",
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("latest release pointer changed", result.stdout)
        self.assertEqual(0, mutation_count)

    def test_marker_upload_observes_stale_reads_without_replaying_mutation(self):
        for mutation_status in ("0", "75"):
            with self.subTest(mutation_status=mutation_status):
                result, mutation_count = self._run_marker_reconciliation(
                    marker_sequence="1,1,0",
                    mutation_sequence=mutation_status,
                )

                self.assertEqual(
                    0,
                    result.returncode,
                    result.stdout + result.stderr,
                )
                self.assertIn("verified=true", result.stdout)
                self.assertEqual(1, mutation_count)

    def test_rollback_observes_stale_reads_without_replaying_mutation(self):
        for mutation_status in ("0", "75"):
            with self.subTest(mutation_status=mutation_status):
                result, mutation_count = self._run_rollback_reconciliation(
                    latest_sequence="dataset-old,dataset-old,dataset-new",
                    mutation_sequence=mutation_status,
                )

                self.assertEqual(
                    0,
                    result.returncode,
                    result.stdout + result.stderr,
                )
                self.assertIn("verified=true active=dataset-new", result.stdout)
                self.assertEqual(1, mutation_count)

    def test_rollback_refuses_changed_latest_pointer_after_mutation(self):
        for mutation_status in ("0", "75"):
            with self.subTest(mutation_status=mutation_status):
                result, mutation_count = self._run_rollback_reconciliation(
                    latest_sequence="dataset-old,dataset-unexpected",
                    mutation_sequence=mutation_status,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn("a different release became latest", result.stdout)
                self.assertEqual(1, mutation_count)

    def test_rollback_refuses_changed_latest_pointer_without_mutating(self):
        result, mutation_count = self._run_rollback_reconciliation(
            latest_sequence="dataset-unexpected",
            mutation_sequence="0",
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("Refusing stale rollback", result.stdout)
        self.assertEqual(0, mutation_count)

    def test_nonrollback_finalizer_noops_when_newer_release_supersedes_deploy(self):
        result, mutation_count = self._run_nonrollback_finalization_until_cleanup(
            active_release_json=(
                '{"tag_name":"dataset-newer","draft":false,'
                '"prerelease":false,"published_at":"2026-08-21T21:13:28Z"}'
            )
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("superseded", result.stdout.lower())
        self.assertNotIn("reached-cleanup=true", result.stdout)
        self.assertEqual(0, mutation_count)

    def test_nonrollback_finalizer_continues_when_target_remains_active(self):
        result, mutation_count = self._run_nonrollback_finalization_until_cleanup(
            active_release_json=(
                '{"tag_name":"dataset-expected","draft":false,'
                '"prerelease":false,"published_at":"2026-08-21T20:09:07Z"}'
            )
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("reached-cleanup=true", result.stdout)
        self.assertEqual(1, mutation_count)

    def test_nonrollback_finalizer_fails_closed_for_untrusted_pointer_change(self):
        cases = {
            "older_release": (
                '{"tag_name":"dataset-unexpected","draft":false,'
                '"prerelease":false,"published_at":"2026-08-21T19:00:00Z"}'
            ),
            "unexpected_tag": (
                '{"tag_name":"manual-release","draft":false,'
                '"prerelease":false,"published_at":"2026-08-21T21:13:28Z"}'
            ),
            "draft_release": (
                '{"tag_name":"dataset-unexpected","draft":true,'
                '"prerelease":false,"published_at":"2026-08-21T21:13:28Z"}'
            ),
            "prerelease_release": (
                '{"tag_name":"dataset-unexpected","draft":false,'
                '"prerelease":true,"published_at":"2026-08-21T21:13:28Z"}'
            ),
            "missing_draft": (
                '{"tag_name":"dataset-unexpected","prerelease":false,'
                '"published_at":"2026-08-21T21:13:28Z"}'
            ),
            "null_draft": (
                '{"tag_name":"dataset-unexpected","draft":null,'
                '"prerelease":false,"published_at":"2026-08-21T21:13:28Z"}'
            ),
            "missing_prerelease": (
                '{"tag_name":"dataset-unexpected","draft":false,'
                '"published_at":"2026-08-21T21:13:28Z"}'
            ),
            "null_prerelease": (
                '{"tag_name":"dataset-unexpected","draft":false,'
                '"prerelease":null,"published_at":"2026-08-21T21:13:28Z"}'
            ),
            "invalid_timestamp": (
                '{"tag_name":"dataset-unexpected","draft":false,'
                '"prerelease":false,"published_at":"not-a-timestamp"}'
            ),
        }
        for name, active_release_json in cases.items():
            with self.subTest(name=name):
                result, mutation_count = (
                    self._run_nonrollback_finalization_until_cleanup(
                        active_release_json=active_release_json
                    )
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn(
                    "active release changed unexpectedly", result.stdout.lower()
                )
                self.assertNotIn("reached-cleanup=true", result.stdout)
                self.assertEqual(0, mutation_count)

    def test_nonrollback_finalizer_requires_target_was_latest_at_resolve(self):
        result, mutation_count = self._run_nonrollback_finalization_until_cleanup(
            active_release_json=(
                '{"tag_name":"dataset-newer","draft":false,'
                '"prerelease":false,"published_at":"2026-08-21T21:13:28Z"}'
            ),
            expected_previous_latest_release_tag="dataset-other",
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("active release changed unexpectedly", result.stdout.lower())
        self.assertNotIn("reached-cleanup=true", result.stdout)
        self.assertEqual(0, mutation_count)

    def test_pages_finalizer_never_deletes_private_releases_or_dataset_tags(self):
        finalization = self._finalization_shell()

        for forbidden in (
            "gh_mutate_once release delete",
            'gh_mutate_once api --method DELETE \\\n                "/repos/$DATA_REPOSITORY/releases/',
            'gh_mutate_once api --method DELETE \\\n                "/repos/$DATA_REPOSITORY/git/refs/tags/',
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, finalization)
        self.assertIn(
            "Automatic private snapshot retention cleanup is disabled",
            finalization,
        )

    def test_failed_private_publication_preserves_draft_for_manual_reconciliation(self):
        publisher = read(PUBLISHER_SCRIPT)

        self.assertNotIn("cleanup_owned_draft() {", publisher)
        self.assertNotIn('gh_mutate_once release delete "$release_tag"', publisher)
        self.assertIn(
            "Owned unpublished snapshot draft requires manual reconciliation",
            publisher,
        )

    def test_github_cli_retries_are_bounded_and_replay_safe(self):
        publisher = read(PUBLISHER_SCRIPT)
        pages = read(".github/workflows/deploy-pages.yml")
        finalization = pages.split(
            "\n  finalize-private-snapshots:", 1
        )[1].split("\n  cleanup-public-pages-artifacts:", 1)[0]

        for script in (publisher, finalization):
            self.assertIn("RETRY_DELAYS_SECONDS=(1 3)", script)
            self.assertIn("TRANSIENT_MUTATION_EXIT_CODE=75", script)
            self.assertIn(
                f'python {GH_RETRY_SCRIPT} --retry-forbidden-read -- "$@"',
                script,
            )
            self.assertIn(f'python {GH_RETRY_SCRIPT} -- "$@"', script)
            self.assertNotIn("gh_read_retry release create", script)
            self.assertNotIn("gh_read_retry release upload", script)
            self.assertNotIn("gh_read_retry release edit", script)
            self.assertNotIn("gh_read_retry release delete", script)

        self.assertIn("release_state()", publisher)
        self.assertIn('gh_mutate_once release create "$release_tag"', publisher)
        self.assertIn("wait_for_draft_release()", publisher)
        self.assertIn('wait_for_draft_release "$release_tag"', publisher)
        self.assertIn("verify_remote_snapshot()", publisher)
        self.assertIn("wait_for_remote_snapshot()", publisher)
        self.assertIn('gh_mutate_once release upload "$release_tag"', publisher)
        self.assertIn("wait_for_remote_snapshot", publisher)
        self.assertIn('gh_mutate_once release edit "$release_tag"', publisher)
        self.assertIn("wait_for_publication()", publisher)
        self.assertIn("Snapshot draft could not be reconciled", publisher)
        self.assertIn("Remote snapshot could not be reconciled", publisher)
        self.assertIn("Snapshot publication could not be reconciled", publisher)
        self.assertIn("observed_latest_tag=$(", publisher)
        self.assertIn(
            'gh_read_retry api "/repos/$DATA_REPOSITORY/releases/latest" '
            "--jq '.tag_name'",
            publisher,
        )

        self.assertIn("sparse-checkout: |", finalization)
        self.assertIn("scripts/data_snapshot.py", finalization)
        self.assertIn(GH_RETRY_SCRIPT, finalization)
        self.assertNotIn("--allow-release-not-found", finalization)
        self.assertIn("marker_matches_remote()", finalization)
        self.assertIn("wait_for_marker_match()", finalization)
        self.assertIn(
            'gh_mutate_once release upload "$EXPECTED_RELEASE_TAG"', finalization
        )
        self.assertIn("wait_for_marker_match", finalization)
        self.assertIn("wait_for_latest_release()", finalization)
        self.assertIn(
            'gh_mutate_once release edit "$EXPECTED_RELEASE_TAG"', finalization
        )
        self.assertNotIn("gh_mutate_once release delete", publisher)
        self.assertNotIn("gh_mutate_once release delete", finalization)
        self.assertNotIn("gh_mutate_once api --method DELETE", finalization)
        self.assertIn(
            "Automatic private snapshot retention cleanup is disabled",
            finalization,
        )

        for script in (publisher, finalization):
            lines = script.splitlines()
            downloads = []
            for index, line in enumerate(lines):
                if "gh_read_retry release download" not in line:
                    continue
                command = [line]
                while command[-1].rstrip().endswith("\\"):
                    index += 1
                    command.append(lines[index])
                downloads.append("\n".join(command))
            self.assertTrue(downloads)
            for download in downloads:
                self.assertIn(
                    "--clobber",
                    download,
                    "every retried release download must overwrite a partial prior attempt",
                )

    def test_maintenance_restores_private_snapshot_before_mutation(self):
        mutation_steps = {
            ".github/workflows/update-data.yml": "- name: Run pipeline",
            ".github/workflows/refresh-cusip-registry.yml": (
                "- name: Fully refresh private CUSIP cache"
            ),
        }
        for path, mutation in mutation_steps.items():
            with self.subTest(path=path):
                workflow = read(path)
                restore_at = workflow.index(
                    "- name: Restore latest validated private snapshot"
                )
                mutate_at = workflow.index(mutation)
                self.assertLess(restore_at, mutate_at)
                self.assertIn("python scripts/data_snapshot.py pull", workflow)
                self.assertIn('--repository "$DATA_REPOSITORY"', workflow)
                self.assertIn('--root "$GITHUB_WORKSPACE"', workflow)
                self.assertIn("--replace", workflow)

    def test_publisher_requires_exact_restored_release_freshness_before_mutations(self):
        publisher = read(PUBLISHER_SCRIPT)
        self.assertIn("verify_base_snapshot_current() {", publisher)
        self.assertIn("python scripts/data_snapshot.py resolve", publisher)
        self.assertIn(
            'if [ "$current_base_release_tag" != "$BASE_RELEASE_TAG" ]; then', publisher
        )
        self.assertIn(
            'if [ "$current_base_dataset_id" != "$BASE_DATASET_ID" ] ||', publisher
        )
        self.assertIn(
            '[ "$current_base_archive_sha256" != "$BASE_ARCHIVE_SHA256" ] ||',
            publisher,
        )
        self.assertIn(
            '[ "$current_base_manifest_sha256" != "$BASE_MANIFEST_SHA256" ]; then',
            publisher,
        )

        for path in PUBLISHING_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                publish = workflow.split(
                    "run: bash scripts/publish_private_snapshot.sh", 1
                )[0]
                self.assertIn(
                    "BASE_RELEASE_TAG: ${{ steps.restore_snapshot.outputs.release_tag }}",
                    publish,
                )
                self.assertIn(
                    "BASE_ARCHIVE_SHA256: ${{ steps.restore_snapshot.outputs.archive_sha256 }}",
                    publish,
                )
                self.assertIn(
                    "BASE_MANIFEST_SHA256: ${{ steps.restore_snapshot.outputs.manifest_sha256 }}",
                    publish,
                )

        for mutation in (
            'gh_mutate_once release create "$release_tag"',
            'gh_mutate_once release upload "$release_tag"',
            'gh_mutate_once release edit "$release_tag"',
        ):
            mutation_at = publisher.index(mutation)
            loop_at = publisher.rfind("for attempt in 0 1 2; do", 0, mutation_at)
            base_guard_at = publisher.rfind(
                "verify_base_snapshot_current", 0, mutation_at
            )
            self.assertGreater(base_guard_at, loop_at)
            self.assertLess(base_guard_at, mutation_at)

    def test_pages_finalizer_requires_exact_snapshot_identity_before_mutations(self):
        pages = read(".github/workflows/deploy-pages.yml")
        finalization = self._finalization_shell()

        for output_name in ("archive_sha256", "manifest_sha256"):
            self.assertIn(
                f"{output_name}: ${{{{ steps.target.outputs.{output_name} }}}}",
                pages,
            )
            self.assertIn(
                f"EXPECTED_{output_name.upper()}: "
                f"${{{{ needs.resolve.outputs.{output_name} }}}}",
                finalization,
            )
        self.assertIn("verify_target_snapshot_current() {", finalization)
        self.assertIn("python scripts/data_snapshot.py resolve", finalization)

        for mutation in (
            'gh_mutate_once release upload "$EXPECTED_RELEASE_TAG"',
            'gh_mutate_once release edit "$EXPECTED_RELEASE_TAG"',
        ):
            mutation_at = finalization.index(mutation)
            loop_at = finalization.rfind("for attempt in 0 1 2; do", 0, mutation_at)
            identity_guard_at = finalization.rfind(
                "verify_target_snapshot_current", loop_at, mutation_at
            )
            self.assertGreater(identity_guard_at, loop_at)
            self.assertLess(identity_guard_at, mutation_at)

    def test_same_tag_pages_finalizer_asset_replacement_blocks_before_mutation(self):
        finalization = self._finalization_shell()
        function_start = finalization.index(
            "          verify_target_snapshot_current() {"
        )
        function_end = finalization.index(
            "          marker_matches_remote() {", function_start
        )
        function_source = textwrap.dedent(finalization[function_start:function_end])
        expected = {
            "dataset_id": "1" * 64,
            "archive_sha256": "2" * 64,
            "manifest_sha256": "3" * 64,
        }

        for changed_field in expected:
            with (
                self.subTest(changed_field=changed_field),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                current = dict(expected)
                current[changed_field] = "9" * 64
                mutation_log = Path(tmpdir) / "mutation.log"
                current_json = json.dumps(
                    {
                        **current,
                        "release_tag": "dataset-target",
                        "repository": "owner/private-data",
                    },
                    sort_keys=True,
                )
                script = "\n".join(
                    (
                        "set -euo pipefail",
                        'DATA_REPOSITORY="owner/private-data"',
                        'EXPECTED_RELEASE_TAG="dataset-target"',
                        f'EXPECTED_DATASET_ID="{expected["dataset_id"]}"',
                        f'EXPECTED_ARCHIVE_SHA256="{expected["archive_sha256"]}"',
                        f'EXPECTED_MANIFEST_SHA256="{expected["manifest_sha256"]}"',
                        'python() { printf "%s\\n" "$CURRENT_JSON"; }',
                        function_source,
                        "verify_target_snapshot_current",
                        'printf "mutation\\n" > "$MUTATION_LOG"',
                    )
                )
                result = subprocess.run(
                    ["bash"],
                    cwd=ROOT,
                    env={
                        **os.environ,
                        "CURRENT_JSON": current_json,
                        "MUTATION_LOG": str(mutation_log),
                    },
                    input=script,
                    text=True,
                    capture_output=True,
                    check=False,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn(
                    "no longer matches the resolved target identity",
                    result.stdout,
                )
                self.assertFalse(mutation_log.exists(), result.stdout + result.stderr)

        for mutation in (
            'gh_mutate_once release upload "$EXPECTED_RELEASE_TAG"',
            'gh_mutate_once release edit "$EXPECTED_RELEASE_TAG"',
        ):
            with (
                self.subTest(mutation_boundary=mutation),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                mutation_at = finalization.index(mutation)
                loop_at = finalization.rfind("for attempt in 0 1 2; do", 0, mutation_at)
                guard_at = finalization.rfind(
                    "if ! verify_target_snapshot_current; then", loop_at, mutation_at
                )
                self.assertGreater(guard_at, loop_at)
                guard_end = finalization.index("fi\n", guard_at) + len("fi\n")
                guard_source = textwrap.dedent(finalization[guard_at:guard_end])
                mutation_log = Path(tmpdir) / "mutation.log"
                script = "\n".join(
                    (
                        "set -euo pipefail",
                        "verify_target_snapshot_current() { return 1; }",
                        'gh_mutate_once() { printf "mutation\\n" > "$MUTATION_LOG"; }',
                        guard_source,
                        "gh_mutate_once forbidden-finalizer-mutation",
                    )
                )
                result = subprocess.run(
                    ["bash"],
                    cwd=ROOT,
                    env={**os.environ, "MUTATION_LOG": str(mutation_log)},
                    input=script,
                    text=True,
                    capture_output=True,
                    check=False,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertFalse(mutation_log.exists(), result.stdout + result.stderr)

    def test_same_tag_remote_asset_replacement_blocks_before_mutation(self):
        publisher = read(PUBLISHER_SCRIPT)
        function_start = publisher.index("verify_base_snapshot_current() {")
        function_end = publisher.index("\ncode_sha=", function_start)
        function_source = publisher[function_start:function_end]
        expected = {
            "dataset_id": "1" * 64,
            "archive_sha256": "2" * 64,
            "manifest_sha256": "3" * 64,
        }

        for changed_field in expected:
            with (
                self.subTest(changed_field=changed_field),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                current = dict(expected)
                current[changed_field] = "9" * 64
                mutation_log = Path(tmpdir) / "mutation.log"
                current_json = json.dumps(
                    {
                        **current,
                        "release_tag": "dataset-base",
                        "repository": "owner/private-data",
                    },
                    sort_keys=True,
                )
                script = "\n".join(
                    (
                        "set -euo pipefail",
                        'DATA_REPOSITORY="owner/private-data"',
                        'BASE_RELEASE_TAG="dataset-base"',
                        f'BASE_DATASET_ID="{expected["dataset_id"]}"',
                        f'BASE_ARCHIVE_SHA256="{expected["archive_sha256"]}"',
                        f'BASE_MANIFEST_SHA256="{expected["manifest_sha256"]}"',
                        f"python() {{ printf '%s\\n' {json.dumps(current_json)}; }}",
                        function_source,
                        "verify_base_snapshot_current",
                        'printf "mutation\\n" > "$MUTATION_LOG"',
                    )
                )
                result = subprocess.run(
                    ["bash"],
                    cwd=ROOT,
                    env={**os.environ, "MUTATION_LOG": str(mutation_log)},
                    input=script,
                    text=True,
                    capture_output=True,
                    check=False,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn(
                    "no longer matches the restored base identity",
                    result.stdout,
                )
                self.assertFalse(mutation_log.exists(), result.stdout + result.stderr)

    def test_upload_rejects_replaced_or_nonempty_owned_draft_before_mutation(self):
        publisher = read(PUBLISHER_SCRIPT)
        function_start = publisher.index("verify_owned_draft_current() {")
        function_end = publisher.index(
            "\n}\n\nexpected_latest_release_tag=", function_start
        ) + len("\n}")
        function_source = publisher[function_start:function_end]
        base_release = {
            "id": 101,
            "tag_name": "dataset-target",
            "name": "dataset-target",
            "body": "owned notes",
            "draft": True,
            "prerelease": False,
            "assets": [],
        }
        unsafe_releases = (
            {**base_release, "id": 202},
            {**base_release, "draft": False},
            {
                **base_release,
                "assets": [{"id": 301, "name": "foreign.bin", "state": "uploaded"}],
            },
        )

        for current_release in unsafe_releases:
            with (
                self.subTest(current_release=current_release),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                mutation_log = Path(tmpdir) / "mutation.log"
                script = "\n".join(
                    (
                        "set -euo pipefail",
                        'owned_release_id="101"',
                        'release_tag="dataset-target"',
                        'release_title="dataset-target"',
                        'release_notes="owned notes"',
                        'archive_name="snapshot.tar.zst"',
                        'manifest_name="snapshot.manifest.json"',
                        'release_record() { printf "%s\\n" "$CURRENT_RELEASE_JSON"; }',
                        function_source,
                        "verify_owned_draft_current empty",
                        'printf "mutation\\n" > "$MUTATION_LOG"',
                    )
                )
                result = subprocess.run(
                    ["bash"],
                    cwd=ROOT,
                    env={
                        **os.environ,
                        "CURRENT_RELEASE_JSON": json.dumps(current_release),
                        "MUTATION_LOG": str(mutation_log),
                    },
                    input=script,
                    text=True,
                    capture_output=True,
                    check=False,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn("exact owned snapshot draft", result.stdout)
                self.assertFalse(mutation_log.exists(), result.stdout + result.stderr)

        upload_start = publisher.index("snapshot_verified=false")
        upload_end = publisher.index(
            'if [ "$snapshot_verified" != true ]; then', upload_start
        )
        upload = publisher[upload_start:upload_end]
        mutation_at = upload.index('gh_mutate_once release upload "$release_tag"')
        ownership_guard_at = upload.rfind(
            "if ! verify_owned_draft_current empty; then", 0, mutation_at
        )
        base_guard_at = upload.rfind(
            "if ! verify_current_main || ! verify_base_snapshot_current; then",
            0,
            mutation_at,
        )
        self.assertGreater(ownership_guard_at, base_guard_at)
        self.assertLess(ownership_guard_at, mutation_at)
        upload_command = upload[mutation_at : upload.index("\n\n", mutation_at)]
        self.assertNotIn("--clobber", upload_command)

    def test_publication_rechecks_owned_target_bytes_immediately_before_edit(self):
        publisher = read(PUBLISHER_SCRIPT)
        publication_start = publisher.index("publication_verified=false")
        mutation_at = publisher.index(
            'gh_mutate_once release edit "$release_tag"', publication_start
        )
        guard_text = (
            "if ! verify_owned_draft_current complete ||\n"
            "     ! verify_remote_snapshot; then"
        )
        guard_at = publisher.rfind(guard_text, publication_start, mutation_at)
        self.assertGreater(guard_at, publication_start)
        self.assertLess(guard_at, mutation_at)
        guard_end = publisher.index("  fi\n", guard_at) + len("  fi\n")
        guard_source = textwrap.dedent(publisher[guard_at:guard_end])

        with tempfile.TemporaryDirectory() as tmpdir:
            mutation_log = Path(tmpdir) / "mutation.log"
            script = "\n".join(
                (
                    "set -euo pipefail",
                    "verify_owned_draft_current() { return 0; }",
                    'verify_remote_snapshot() { [ "$REMOTE_STATE" = exact ]; }',
                    'abort_with_owned_draft_cleanup() { printf "%s\\n" "$1"; exit 1; }',
                    'gh_mutate_once() { printf "mutation\\n" > "$MUTATION_LOG"; }',
                    guard_source,
                    "gh_mutate_once forbidden-publication-edit",
                )
            )
            result = subprocess.run(
                ["bash"],
                cwd=ROOT,
                env={
                    **os.environ,
                    "REMOTE_STATE": "replaced",
                    "MUTATION_LOG": str(mutation_log),
                },
                input=script,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("changed before snapshot publication", result.stdout)
            self.assertFalse(mutation_log.exists(), result.stdout + result.stderr)

    def test_uploaded_target_round_trip_requires_exact_local_asset_hashes(self):
        publisher = read(PUBLISHER_SCRIPT)
        function_start = publisher.index("verify_remote_snapshot() {")
        function_end = publisher.index("\nwait_for_remote_snapshot() {", function_start)
        function_source = publisher[function_start:function_end]
        expected_dataset_id = "1" * 64
        expected_source_sha = "a" * 40
        expected_archive_sha256 = "2" * 64
        expected_manifest_sha256 = "3" * 64
        replaced_archive_sha256 = "8" * 64
        replaced_manifest_sha256 = "9" * 64

        with tempfile.TemporaryDirectory() as tmpdir:
            script = "\n".join(
                (
                    "set -euo pipefail",
                    'release_tag="dataset-target"',
                    'DATA_REPOSITORY="owner/private-data"',
                    'archive_name="snapshot.tar.zst"',
                    'manifest_name="snapshot.manifest.json"',
                    f'dataset_id="{expected_dataset_id}"',
                    f'code_sha="{expected_source_sha}"',
                    f'archive_sha256="{expected_archive_sha256}"',
                    f'manifest_sha256="{expected_manifest_sha256}"',
                    r"""
gh_read_retry() {
  local candidate_dir=""
  while [ "$#" -gt 0 ]; do
    if [ "$1" = "--dir" ]; then
      candidate_dir=$2
      break
    fi
    shift
  done
  : > "$candidate_dir/$archive_name"
  printf '{"dataset_id":"%s","source_sha":"%s"}\n' \
    "$dataset_id" "$code_sha" > "$candidate_dir/$manifest_name"
}
""",
                    r"""
python() {
  printf '{"archive_sha256":"%s","dataset_id":"%s","manifest_sha256":"%s","source_sha":"%s","verified":true}\n' \
    "$REPLACED_ARCHIVE_SHA256" "$dataset_id" \
    "$REPLACED_MANIFEST_SHA256" "$code_sha"
}
""",
                    function_source,
                    "verify_remote_snapshot",
                )
            )
            result = subprocess.run(
                ["bash"],
                cwd=ROOT,
                env={
                    **os.environ,
                    "RUNNER_TEMP": tmpdir,
                    "REPLACED_ARCHIVE_SHA256": replaced_archive_sha256,
                    "REPLACED_MANIFEST_SHA256": replaced_manifest_sha256,
                },
                input=script,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("exact local target assets", result.stdout)

        pack_validation = publisher.split("pack_json=$(", 1)[1].split(
            'if [ "$dataset_id" = "$BASE_DATASET_ID" ]; then', 1
        )[0]
        self.assertIn(
            "manifest_sha256=$(jq -er '.manifest_sha256' <<<\"$pack_json\")",
            pack_validation,
        )
        self.assertIn('[[ ! "$manifest_sha256" =~ ^[0-9a-f]{64}$ ]]', pack_validation)

    def test_newer_private_snapshot_blocks_each_target_mutation_boundary(self):
        publisher = read(PUBLISHER_SCRIPT)
        for mutation in (
            'gh_mutate_once release create "$release_tag"',
            'gh_mutate_once release upload "$release_tag"',
            'gh_mutate_once release edit "$release_tag"',
        ):
            with (
                self.subTest(mutation=mutation),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                mutation_at = publisher.index(mutation)
                loop_at = publisher.rfind("for attempt in 0 1 2; do", 0, mutation_at)
                guard_at = publisher.rfind(
                    "  if ! verify_current_main || ! verify_base_snapshot_current; then",
                    loop_at,
                    mutation_at,
                )
                self.assertGreater(guard_at, loop_at)
                guard_end = publisher.index("  fi\n", guard_at) + len("  fi\n")
                guard = textwrap.dedent(publisher[guard_at:guard_end])
                mutation_log = Path(tmpdir) / "mutation.log"
                cleanup_log = Path(tmpdir) / "cleanup.log"
                script = "\n".join(
                    (
                        "set -euo pipefail",
                        "verify_current_main() { return 0; }",
                        'verify_base_snapshot_current() { echo "newer snapshot"; return 1; }',
                        'abort_with_owned_draft_cleanup() { printf "%s\\n" "$1" > "$CLEANUP_LOG"; exit 1; }',
                        'gh_mutate_once() { printf "%s\\n" "$*" > "$MUTATION_LOG"; }',
                        guard,
                        "gh_mutate_once forbidden-target-mutation",
                    )
                )
                result = subprocess.run(
                    ["bash"],
                    cwd=ROOT,
                    env={
                        **os.environ,
                        "MUTATION_LOG": str(mutation_log),
                        "CLEANUP_LOG": str(cleanup_log),
                    },
                    input=script,
                    text=True,
                    capture_output=True,
                    check=False,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn("newer snapshot", result.stdout)
                self.assertFalse(mutation_log.exists(), result.stdout + result.stderr)
                self.assertTrue(cleanup_log.is_file(), result.stdout + result.stderr)

    def test_release_record_treats_an_absent_tag_as_valid_null(self):
        publisher = read(PUBLISHER_SCRIPT)
        function_start = publisher.index("release_record() {")
        function_end = publisher.index("release_state() {", function_start)
        function_source = publisher[function_start:function_end]
        script = "\n".join(
            (
                "set -euo pipefail",
                'DATA_REPOSITORY="owner/private-data"',
                'gh_read_retry() { printf "%s\\n" "[]"; }',
                function_source,
                'release_record "dataset-missing"',
            )
        )

        result = subprocess.run(
            ["bash"],
            cwd=ROOT,
            input=script,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("null", result.stdout.strip())

    def test_owned_draft_abort_never_deletes_and_requires_manual_reconciliation(self):
        publisher = read(PUBLISHER_SCRIPT)
        function_start = publisher.index("abort_with_owned_draft_cleanup() {")
        function_end = publisher.index("expected_latest_release_tag=", function_start)
        function_source = publisher[function_start:function_end]

        for owned_draft in ("true", "false"):
            with (
                self.subTest(owned_draft=owned_draft),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                mutation_log = Path(tmpdir) / "mutation.log"
                script = "\n".join(
                    (
                        "set -euo pipefail",
                        f"owned_draft={owned_draft}",
                        'gh_mutate_once() { printf "%s\\n" "$*" > "$MUTATION_LOG"; }',
                        function_source,
                        'abort_with_owned_draft_cleanup "forced reconciliation"',
                    )
                )
                result = subprocess.run(
                    ["bash"],
                    cwd=ROOT,
                    env={**os.environ, "MUTATION_LOG": str(mutation_log)},
                    input=script,
                    text=True,
                    capture_output=True,
                    check=False,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertFalse(mutation_log.exists(), result.stdout + result.stderr)
                self.assertIn("forced reconciliation", result.stdout)
                if owned_draft == "true":
                    self.assertIn(
                        "Owned unpublished snapshot draft requires manual reconciliation",
                        result.stdout,
                    )

    def test_publishers_are_atomic_private_releases_not_git_commits(self):
        required_fragments = (
            "python scripts/data_snapshot.py pack",
            "--max-archive-bytes 1932735283",
            'gh_mutate_once release create "$release_tag"',
            "--draft",
            'gh_mutate_once release upload "$release_tag"',
            'gh_read_retry release download "$release_tag"',
            "python scripts/data_snapshot.py verify",
            "does not contain exactly two snapshot assets",
            'gh_mutate_once release edit "$release_tag"',
            "--draft=false",
            "--latest",
        )
        publisher = read(PUBLISHER_SCRIPT)
        for fragment in required_fragments:
            self.assertIn(fragment, publisher)
        self.assertLess(
            publisher.index('gh_mutate_once release create "$release_tag"'),
            publisher.index("python scripts/data_snapshot.py verify"),
        )
        self.assertLess(
            publisher.index("python scripts/data_snapshot.py verify"),
            publisher.index('gh_mutate_once release edit "$release_tag"'),
        )
        self.assertNotIn("git add data/", publisher)
        self.assertNotIn("git commit", publisher)
        self.assertNotIn("git push", publisher)

        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                self.assertIn(f"bash {PUBLISHER_SCRIPT}", workflow)
                self.assertNotIn("actions/cache", workflow)
                self.assertIn("permissions:\n  contents: read", workflow)
                self.assertNotRegex(workflow, r"(?m)^  contents: write$")

    def test_publisher_restricts_0644_downloaded_archive_before_verify(self):
        publisher = read(PUBLISHER_SCRIPT)
        round_trip = publisher.split(
            "verify_remote_snapshot() {", 1
        )[1].split("\nsnapshot_verified=false", 1)[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            downloaded_archive = Path(tmpdir) / "downloaded.tar.gz"
            downloaded_archive.write_bytes(b"release asset")
            downloaded_archive.chmod(0o644)
            self.assertEqual(0o644, downloaded_archive.stat().st_mode & 0o777)

        chmod = 'chmod 600 "$candidate_dir/$archive_name"'
        chmod_guard = (
            'if ! chmod 600 "$candidate_dir/$archive_name"; then\n'
            '    rm -rf "$candidate_dir"\n'
            "    return 1\n"
            "  fi"
        )
        verify = "python scripts/data_snapshot.py verify"
        self.assertIn(chmod_guard, round_trip)
        self.assertLess(round_trip.index(chmod), round_trip.index(verify))

    def test_publishers_output_exact_deployment_identity(self):
        publisher = read(PUBLISHER_SCRIPT)
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                for output in (
                    "code_sha",
                    "release_tag",
                    "dataset_id",
                    "site_changed",
                ):
                    self.assertRegex(
                        workflow,
                        rf"(?m)^      {output}: \$\{{\{{ steps\.publish_snapshot\.outputs\.{output} \}}\}}$",
                    )
                    self.assertIn(
                        f'echo "{output}=',
                        publisher,
                    )
                self.assertIn(
                    'if [ "$dataset_id" = "$BASE_DATASET_ID" ]; then',
                    publisher,
                )
                self.assertIn("PUBLIC_GITHUB_TOKEN: ${{ github.token }}", workflow)
                self.assertIn('GH_TOKEN="$PUBLIC_GITHUB_TOKEN"', publisher)
                self.assertIn('echo "site_changed=true"', publisher)

    def test_maintenance_stale_code_guard_fails_closed(self):
        publisher = read(PUBLISHER_SCRIPT)
        fetch = "git fetch --no-tags origin main:refs/remotes/origin/main"
        self.assertIn(fetch, publisher)
        self.assertIn("verify_current_main() {", publisher)
        self.assertIn("current_main_sha=$(git rev-parse origin/main)", publisher)
        self.assertIn(
            'if [ "$code_sha" != "$current_main_sha" ]; then',
            publisher,
        )
        self.assertIn("aborting stale publication", publisher)
        self.assertNotIn("git reset --hard", publisher)

    def test_data_workflow_timeouts_preserve_durable_partial_progress(self):
        workflow = read(".github/workflows/update-data.yml")
        pipeline = workflow.split("- name: Run pipeline", 1)[1].split(
            "- name: Refresh recently accepted 13F filings", 1
        )[0]
        recent = workflow.split(
            "- name: Refresh recently accepted 13F filings", 1
        )[1].split("- name: Regenerate registry-backed site data", 1)[0]

        self.assertIn("timeout --signal=TERM --kill-after=120s 210m", pipeline)
        self.assertIn('if [ "$pipeline_status" -eq 124 ]; then', pipeline)
        self.assertIn('elif [ "$pipeline_status" -ne 0 ]; then', pipeline)
        self.assertIn('exit "$pipeline_status"', pipeline)
        self.assertIn("timeout --signal=TERM --kill-after=120s 15m", recent)
        self.assertIn('if [ "$recent_status" -eq 124 ]; then', recent)
        self.assertIn('elif [ "$recent_status" -ne 0 ]; then', recent)
        self.assertIn('exit "$recent_status"', recent)

    def test_registry_regeneration_has_realistic_timeout_headroom(self):
        workflow = read(".github/workflows/update-data.yml")
        regenerate = workflow.split(
            "- name: Regenerate registry-backed site data", 1
        )[1].split("- name: Validate generated data", 1)[0]

        self.assertIn("timeout-minutes: 45", regenerate)

    def test_every_snapshot_publisher_runs_contract_tests_first(self):
        command = (
            "python -m unittest discover -s tests "
            "-p 'test_data_contract.py' -v"
        )
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                self.assertIn(command, workflow)
                self.assertLess(
                    workflow.index(command),
                    workflow.index("- name: Publish "),
                )

    def test_every_publishing_path_runs_complete_python_and_node_suites(self):
        python_command = "python -m unittest discover -s tests -v"
        node_command = "node --test tests/test_site_data_loader.mjs"
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                publish_at = workflow.index("- name: Publish ")
                self.assertIn(python_command, workflow)
                self.assertIn(node_command, workflow)
                self.assertLess(workflow.index(python_command), publish_at)
                self.assertLess(workflow.index(node_command), publish_at)

        pages = read(".github/workflows/deploy-pages.yml")
        build = pages.split("\n  build:", 1)[1].split("\n  deploy:", 1)[0]
        build_artifact_at = build.index("- name: Build bounded public Pages artifact")
        self.assertIn(python_command, build)
        self.assertIn(node_command, build)
        self.assertLess(build.index(python_command), build_artifact_at)
        self.assertLess(build.index(node_command), build_artifact_at)

    def test_health_annotations_are_observable_but_truly_nonfatal(self):
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                annotation = workflow.split(
                    "- name: Annotate ticker health report", 1
                )[1].split("\n      - name:", 1)[0]
                self.assertIn("if: always()", annotation)
                self.assertIn("continue-on-error: true", annotation)
                self.assertIn("python scripts/annotate_ticker_health.py", annotation)

    def test_pages_restores_exact_snapshot_and_builds_explicit_allowlist(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        build = workflow.split("\n  build:", 1)[1].split("\n  deploy:", 1)[0]

        restore_at = build.index("- name: Restore exact validated private snapshot")
        build_at = build.index("- name: Build bounded public Pages artifact")
        self.assertLess(restore_at, build_at)
        self.assertIn('--release-tag "$EXPECTED_RELEASE_TAG"', build)
        self.assertIn('--dataset-id "$EXPECTED_DATASET_ID"', build)
        self.assertLess(
            build.index("python scripts/data_snapshot.py pull"),
            build.index("run: python validate_data.py"),
        )
        self.assertLess(
            build.index("run: python validate_data.py"),
            build.index("python scripts/build_pages_artifact.py"),
        )
        self.assertIn(
            "pip install --require-hashes -r requirements.lock",
            build,
        )
        self.assertIn("- name: Audit the public artifact allowlist", build)
        self.assertIn("include-hidden-files: true", build)
        for public_file in (
            "data/funds-index.json",
            "data/index.json",
            "data/security_labels.json",
            "data/insiders/public/manifest.json",
        ):
            self.assertIn(public_file, build)
        self.assertNotIn("data/funds/*.json.gz", build)
        self.assertNotIn("data/stocks/*.json.gz", build)
        self.assertNotIn("data/insiders/public/securities/*.json.gz", build)
        self.assertNotIn("data/insiders/public/filings/*.json.gz", build)
        self.assertIn(
            "^data/funds/[0-9]{1,10}\\.json\\.gz$",
            build,
        )
        self.assertIn(
            "^data/stocks/[A-Z0-9][A-Z0-9._-]{0,159}\\.json\\.gz$",
            build,
        )
        self.assertIn(
            "^data/insiders/public/securities/[A-Z0-9][A-Z0-9._-]{0,159}\\.json\\.gz$",
            build,
        )
        self.assertIn(
            "^data/insiders/public/filings/[0-9]{10}-[0-9]{2}-[0-9]{6}\\.json\\.gz$",
            build,
        )
        self.assertIn(
            'done < <(find "$ARTIFACT_DIR" -mindepth 1 -print0)',
            build,
        )
        self.assertIn('if [ -L "$path" ]; then', build)
        self.assertIn('elif [ -d "$path" ]; then', build)
        self.assertIn('elif [ -f "$path" ]; then', build)
        for public_directory in (
            "data",
            "data/funds",
            "data/stocks",
            "data/insiders",
            "data/insiders/public",
            "data/insiders/public/securities",
            "data/insiders/public/filings",
        ):
            self.assertIn(public_directory, build)
        self.assertIn("Unsupported public Pages entry type", build)
        self.assertIn("Required public Pages entry is missing", build)
        for private_file in (
            "cusip_registry.json",
            "pipeline_state.json",
            "ticker_health.json",
            "pages-deployment.json",
        ):
            self.assertNotIn(private_file, build)

    def test_pages_stale_guard_checks_public_code_and_latest_private_release(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        guard = workflow.split(
            "- name: Refuse an artifact superseded by newer public or private inputs",
            1,
        )[1].split("- name: Deploy exact validated artifact", 1)[0]

        fetch = "git fetch --no-tags origin main:refs/remotes/origin/main"
        resolve = "current_sha=$(git rev-parse refs/remotes/origin/main)"
        self.assertIn(fetch, guard)
        self.assertIn(resolve, guard)
        self.assertLess(guard.index(fetch), guard.index(resolve))
        self.assertIn("--between", guard)
        self.assertIn(
            'gh api "/repos/$DATA_REPOSITORY/releases/latest"', guard
        )
        self.assertIn("the active private release changed", guard)
        self.assertIn('if [ "$ALLOW_OLDER_RELEASE" != true ]; then', guard)

    def test_live_manifest_requires_code_and_dataset_identity(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        verify = workflow.split("- name: Verify live deployment manifest", 1)[1].split(
            "\n  finalize-private-snapshots:", 1
        )[0]

        self.assertIn("observed_code_sha", verify)
        self.assertIn("observed_dataset_id", verify)
        self.assertIn("$EXPECTED_CODE_SHA", verify)
        self.assertIn("$EXPECTED_DATASET_ID", verify)
        self.assertIn("saw_mismatched_identity=true", verify)
        self.assertIn('if [ "$http_status" = 403 ]', verify)
        self.assertIn("cf-mitigated:", verify)
        self.assertIn(
            "Pages completed but did not serve the expected code and dataset",
            verify,
        )

    def test_cloudflare_fallback_is_bound_to_private_deployment_marker(self):
        helper = read("scripts/pages_deploy_needed.sh")

        self.assertIn("latest_pages_deployment_matches_marker", helper)
        self.assertIn("pages-deployment.json", helper)
        self.assertIn(".deployment_id", helper)
        self.assertIn(".code_sha", helper)
        self.assertIn(".dataset_id", helper)
        self.assertIn(".release_tag", helper)
        self.assertIn("deployments?environment=github-pages&per_page=1", helper)
        self.assertIn("statuses?per_page=1", helper)
        self.assertIn('if [ "$status" != success ]; then', helper)
        self.assertIn('if [ "$http_status" = 403 ]', helper)
        self.assertIn("cf-mitigated:", helper)

    def test_finalization_self_heals_marker_and_preserves_private_history(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        deploy = workflow.split("\n  deploy:", 1)[1].split(
            "\n  finalize-private-snapshots:", 1
        )[0]
        finalization = workflow.split(
            "\n  finalize-private-snapshots:", 1
        )[1].split("\n  cleanup-public-pages-artifacts:", 1)[0]

        self.assertIn("needs: [resolve, deploy]", finalization)
        self.assertIn("${{ always() &&", finalization)
        self.assertIn("needs.resolve.result == 'success'", finalization)
        self.assertIn(
            "needs.resolve.outputs.deploy_needed == 'false'", finalization
        )
        self.assertIn("needs.deploy.result == 'success'", finalization)
        self.assertIn("pages-deployment.json", finalization)
        self.assertNotIn("pages-deployment.json", deploy)
        self.assertIn(
            "deployments?environment=github-pages&per_page=20", finalization
        )
        self.assertIn('for candidate_id in "${deployment_ids[@]}"', finalization)
        self.assertIn('if [ "$candidate_status" = success ]; then', finalization)
        self.assertIn("candidate_status_json", finalization)
        self.assertIn(".[0].created_at | fromdateiso8601", finalization)
        self.assertIn('deployed_at="$candidate_deployed_at"', finalization)
        marker_build = finalization.split('marker_path="$RUNNER_TEMP', 1)[1].split(
            "gh_mutate_once release upload", 1
        )[0]
        self.assertIn('--arg deployed_at "$deployed_at"', marker_build)
        self.assertNotIn("$(date -u", marker_build)
        self.assertIn("jq -er '.deployed_at'", finalization)
        self.assertIn(
            'if [[ ! "$deployment_id" =~ ^[0-9]+$ ]] || [ -z "$deployed_at" ]; then',
            finalization,
        )
        self.assertIn('gh_mutate_once release upload "$EXPECTED_RELEASE_TAG"', finalization)
        self.assertIn("Private Pages deployment marker did not round-trip exactly", finalization)
        self.assertIn(
            "Automatic private snapshot retention cleanup is disabled",
            finalization,
        )
        self.assertIn("prior releases, drafts, and dataset tags", finalization)
        self.assertNotIn("gh_mutate_once release delete", finalization)
        self.assertNotIn("gh_mutate_once api --method DELETE", finalization)

        self.assertNotIn("actions/artifacts/$artifact_id", finalization)

    def test_public_pages_artifacts_are_cleaned_even_after_deploy_failure(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        cleanup = workflow.split(
            "\n  cleanup-public-pages-artifacts:", 1
        )[1]

        self.assertIn("needs: [resolve, build, deploy]", cleanup)
        self.assertIn("if: ${{ always() }}", cleanup)
        self.assertIn("actions: write", cleanup)
        self.assertIn("Checkout trusted GitHub retry helper for cleanup", cleanup)
        self.assertIn("repository: ${{ job.workflow_repository }}", cleanup)
        self.assertIn("ref: ${{ job.workflow_sha }}", cleanup)
        self.assertIn("sparse-checkout: scripts/github_cli_retry.py", cleanup)
        self.assertIn("persist-credentials: false", cleanup)
        self.assertIn("RUN_ID: ${{ github.run_id }}", cleanup)
        self.assertIn('if [[ ! "$RUN_ID" =~ ^[0-9]+$ ]]', cleanup)
        run_artifacts_endpoint = (
            '"/repos/$GITHUB_REPOSITORY/actions/runs/$RUN_ID/'
            'artifacts?name=github-pages&per_page=100"'
        )
        self.assertIn(run_artifacts_endpoint, cleanup)
        self.assertIn(
            'gh_read_retry api --paginate --slurp "$RUN_ARTIFACTS_ENDPOINT"',
            cleanup,
        )
        self.assertNotIn(
            '"/repos/$GITHUB_REPOSITORY/actions/artifacts?per_page=100"',
            cleanup,
        )
        self.assertIn("(.workflow_run.id == $run_id)", cleanup)
        self.assertIn('(.name == "github-pages")', cleanup)
        self.assertIn('(.expired | type) == "boolean"', cleanup)
        self.assertIn("gh_delete_once()", cleanup)
        self.assertEqual(1, cleanup.count("gh_delete_once api --method DELETE"))
        self.assertNotIn("\n            gh api --method DELETE", cleanup)
        self.assertIn("|| delete_status=$?", cleanup)
        self.assertIn("Deletion outcome is uncertain; reconciling by readback", cleanup)
        self.assertIn("for attempt in 0 1 2; do", cleanup)
        self.assertIn("readonly -a READBACK_DELAYS_SECONDS=(1 3)", cleanup)
        self.assertIn(
            "Public Pages artifacts remain downloadable after cleanup",
            cleanup,
        )

    def test_latest_release_pointer_persists_manual_rollback(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        resolve = workflow.split("\n  resolve:", 1)[1].split("\n  build:", 1)[0]
        finalization = workflow.split(
            "\n  finalize-private-snapshots:", 1
        )[1].split("\n  cleanup-public-pages-artifacts:", 1)[0]

        self.assertIn(
            'latest_release_tag=$(\n            gh_read_retry api '
            '"/repos/$DATA_REPOSITORY/releases/latest"',
            resolve,
        )
        self.assertIn("allow_older_release=true", resolve)
        self.assertIn('gh_mutate_once release edit "$EXPECTED_RELEASE_TAG"', finalization)
        self.assertIn("--latest", finalization)
        self.assertIn('if [ "$ALLOW_OLDER_RELEASE" = true ]; then', finalization)
        self.assertIn("The active release changed before finalization", finalization)
        self.assertIn(
            'gh_read_retry api "/repos/$DATA_REPOSITORY/releases/latest"', finalization
        )

    def test_unchanged_maintenance_preserves_active_pages_release(self):
        publisher = read(PUBLISHER_SCRIPT)
        unchanged = publisher.split(
            'if [ "$dataset_id" = "$BASE_DATASET_ID" ]; then', 1
        )[1].split("\nfi", 1)[0]
        self.assertIn(
            'active_release_json=$(gh_read_retry api "/repos/$DATA_REPOSITORY/releases/latest")',
            unchanged,
        )
        self.assertIn('"$active_dataset_id" "$active_release_tag"', unchanged)
        self.assertIn('echo "release_tag=$active_release_tag"', unchanged)
        self.assertIn('echo "dataset_id=$active_dataset_id"', unchanged)
        self.assertNotIn('echo "release_tag=$BASE_RELEASE_TAG"', unchanged)

    def test_maintenance_callers_pass_exact_snapshot_and_inherit_secrets(self):
        expected_outputs = {
            ".github/workflows/update-data.yml": "needs.update.outputs",
            ".github/workflows/refresh-cusip-registry.yml": (
                "needs.refresh-cusips.outputs"
            ),
        }
        for path, outputs in expected_outputs.items():
            with self.subTest(path=path):
                workflow = read(path)
                self.assertIn("uses: ./.github/workflows/deploy-pages.yml", workflow)
                for field in ("code_sha", "release_tag", "dataset_id"):
                    self.assertIn(
                        f"{field}: ${{{{ {outputs}.{field} }}}}",
                        workflow,
                    )
                self.assertIn("secrets: inherit", workflow)

    def test_ci_privacy_guard_covers_current_history_and_test_residue(self):
        workflow = read(".github/workflows/test.yml")
        gitignore = read(".gitignore")

        self.assertRegex(workflow, r"(?m)^  push:\s*$")
        self.assertNotIn("branches: [main]", workflow)
        self.assertIn("github.event_name != 'pull_request'", workflow)
        self.assertIn(
            "github.event.pull_request.head.repo.full_name != github.repository",
            workflow,
        )
        self.assertIn("fetch-depth: 0", workflow)
        self.assertIn("git ls-files -- data/ .cache/", workflow)
        self.assertIn("git log --all --format= --name-only -- data/ .cache/", workflow)
        self.assertIn("Private generated data exists in reachable Git history", workflow)
        self.assertIn("for private_path in data .cache; do", workflow)
        self.assertIn('if [ -e "$private_path" ]; then', workflow)
        self.assertIn("Tests left private generated data", workflow)
        self.assertNotIn("git status --porcelain", workflow)
        self.assertNotIn("git diff --exit-code -- data/", workflow)
        self.assertRegex(gitignore, r"(?m)^data/$")


if __name__ == "__main__":
    unittest.main()
