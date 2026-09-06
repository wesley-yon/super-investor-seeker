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

    def _run_private_repository_guard(
        self,
        *,
        repository_json: str,
        read_status: int = 0,
    ) -> tuple[subprocess.CompletedProcess[str], bool, str]:
        publisher = read(PUBLISHER_SCRIPT)
        function_start = publisher.index("verify_private_data_repository() {")
        function_end = publisher.index("\nrelease_state() {", function_start)
        function_source = publisher[function_start:function_end]

        with tempfile.TemporaryDirectory() as tmpdir:
            mutation_path = Path(tmpdir) / "mutation"
            call_path = Path(tmpdir) / "read-call"
            script = "\n".join(
                (
                    "set -euo pipefail",
                    'DATA_REPOSITORY="owner/private-data"',
                    r"""
gh_read_retry() {
  printf '%s\n' "$*" > "$GH_CALL_PATH"
  printf '%s\n' "$REPOSITORY_JSON"
  return "$GH_READ_STATUS"
}
gh_mutate_once() {
  printf '%s\n' "$*" > "$MUTATION_PATH"
}
""",
                    function_source,
                    "verify_private_data_repository",
                    'gh_mutate_once release create "dataset-new"',
                )
            )
            env = os.environ.copy()
            env.update(
                {
                    "GH_CALL_PATH": str(call_path),
                    "GH_READ_STATUS": str(read_status),
                    "MUTATION_PATH": str(mutation_path),
                    "REPOSITORY_JSON": repository_json,
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
            mutated = mutation_path.exists()
            read_call = call_path.read_text(encoding="utf-8").strip()
        return result, mutated, read_call

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
                    "sleep_before_retry() { return 0; }",
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
            "\n          releases_json=$(\n", block_start
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

    def _run_orphan_tag_sweep(
        self,
        *,
        release_pages: str,
        ref_pages: str,
    ) -> tuple[subprocess.CompletedProcess[str], list[str]]:
        finalization = self._finalization_shell()
        comment_start = finalization.index(
            "# A prior run can be interrupted after deleting a release"
        )
        block_start = finalization.index("          retained_json=$(\n", comment_start)
        block_end = finalization.index(
            "          # Fail closed if cleanup", block_start
        )
        block_source = textwrap.dedent(finalization[block_start:block_end])

        with tempfile.TemporaryDirectory() as tmpdir:
            deletion_log = Path(tmpdir) / "deletions"
            deletion_log.write_text("", encoding="utf-8")
            script = "\n".join(
                (
                    "set -euo pipefail",
                    'DATA_REPOSITORY="owner/private-data"',
                    'EXPECTED_RELEASE_TAG="dataset-active"',
                    'fallback_tag="dataset-fallback"',
                    r"""
gh_read_retry() {
  case "$*" in
    *"/releases?per_page=100"*) printf '%s\n' "$RELEASE_PAGES" ;;
    *"/git/matching-refs/tags/dataset-"*) printf '%s\n' "$REF_PAGES" ;;
    *) return 99 ;;
  esac
}
release_state() {
  printf 'missing\n'
}
mapfile() {
  if [ "${1:-}" = -t ]; then
    shift
  fi
  local array_name=$1
  local line
  eval "$array_name=()"
  while IFS= read -r line; do
    eval "$array_name+=(\"\$line\")"
  done
}
reconcile_tag_deletion() {
  printf '%s\n' "$1" >> "$DELETION_LOG"
}
""",
                    block_source,
                )
            )
            env = os.environ.copy()
            env.update(
                {
                    "DELETION_LOG": str(deletion_log),
                    "REF_PAGES": ref_pages,
                    "RELEASE_PAGES": release_pages,
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
            deleted_tags = deletion_log.read_text(encoding="utf-8").splitlines()
        return result, deleted_tags

    def _run_release_tag_reconciliation(
        self,
        *,
        tag_sequence: str,
        mutation_sequence: str,
    ) -> tuple[subprocess.CompletedProcess[str], int, int]:
        finalization = self._finalization_shell()
        function_start = finalization.index("          wait_for_release_missing() {")
        function_end = finalization.index(
            "          marker_matches_remote() {", function_start
        )
        function_source = textwrap.dedent(
            finalization[function_start:function_end]
        )
        loop_start = finalization.index(
            '          for release_tag in "${delete_tags[@]}"; do'
        )
        loop_end = finalization.index(
            "\n\n          # A prior run can be interrupted", loop_start
        )
        loop_source = textwrap.dedent(finalization[loop_start:loop_end])

        with tempfile.TemporaryDirectory() as tmpdir:
            counters = Path(tmpdir)
            for name in ("tag", "tag_mutation", "release_mutation"):
                (counters / name).write_text("0\n", encoding="utf-8")
            script = "\n".join(
                (
                    "set -euo pipefail",
                    SHELL_SEQUENCE_HELPER,
                    "readonly TRANSIENT_MUTATION_EXIT_CODE=75",
                    'DATA_REPOSITORY="owner/private-data"',
                    'delete_tags=("dataset-stale")',
                    "sleep_before_retry() { return 0; }",
                    r"""
release_state() {
  printf 'missing\n'
}
tag_ref_state() {
  next_sequence_value "$TAG_SEQUENCE" "$COUNTER_DIR/tag"
}
gh_delete_once() {
  local count
  count=$(<"$COUNTER_DIR/release_mutation")
  printf '%s\n' "$((count + 1))" > "$COUNTER_DIR/release_mutation"
  return 0
}
gh_mutate_once() {
  local status
  status=$(next_sequence_value "$MUTATION_SEQUENCE" "$COUNTER_DIR/tag_mutation")
  return "$status"
}
""",
                    function_source,
                    loop_source,
                )
            )
            env = os.environ.copy()
            env.update(
                {
                    "COUNTER_DIR": tmpdir,
                    "MUTATION_SEQUENCE": mutation_sequence,
                    "TAG_SEQUENCE": tag_sequence,
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
            release_mutation_count = int(
                (counters / "release_mutation").read_text(encoding="utf-8").strip()
            )
            tag_mutation_count = int(
                (counters / "tag_mutation").read_text(encoding="utf-8").strip()
            )
        return result, release_mutation_count, tag_mutation_count

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

    def test_security_master_workflows_use_only_sec_refresh_contracts(self):
        update = read(".github/workflows/update-data.yml")
        rebuild = read(".github/workflows/refresh-cusip-registry.yml")

        self.assertIn(
            "pipeline.py --regenerate-only --refresh-security-master",
            update,
        )
        self.assertIn(
            "pipeline.py --regenerate-only --rebuild-security-master --apply-quantity-policy",
            rebuild,
        )
        self.assertIn("rebuild_security_master:", rebuild)
        self.assertIn("inputs.rebuild_security_master == true", rebuild)
        self.assertIn("inputs.rebuild_security_master != true", rebuild)
        for workflow in (update, rebuild):
            self.assertIn("SEC_USER_AGENT: ${{ secrets.SEC_USER_AGENT }}", workflow)
            lowered = workflow.lower()
            retired_provider = "open" + "figi"
            for retired_contract in (
                retired_provider,
                "--retry-unresolved",
                "--full-cusip-refresh",
            ):
                self.assertNotIn(retired_contract, lowered)

        self.assertIn(
            "if: steps.restore_snapshot.outputs.legacy_snapshot != 'true'",
            update.split("- name: Run pipeline", 1)[1].split(
                "- name: Refresh recently accepted 13F filings", 1
            )[0],
        )
        self.assertIn(
            "steps.restore_snapshot.outputs.legacy_snapshot != 'true'",
            update.split(
                "- name: Refresh recently accepted 13F filings", 1
            )[1].split(
                "- name: Rebuild SEC-only registry-backed site data", 1
            )[0],
        )
        legacy_rebuild = update.split(
            "- name: Rebuild SEC-only registry-backed site data", 1
        )[1].split("- name: Regenerate registry-backed site data", 1)[0]
        self.assertIn(
            "if: steps.restore_snapshot.outputs.legacy_snapshot == 'true'",
            legacy_rebuild,
        )
        self.assertIn("timeout-minutes: 280", legacy_rebuild)
        self.assertIn(
            "timeout --signal=TERM --kill-after=120s 260m",
            legacy_rebuild,
        )
        self.assertIn(
            "pipeline.py --regenerate-only --rebuild-security-master --apply-quantity-policy",
            legacy_rebuild,
        )
        for workflow in (update, rebuild):
            self.assertNotIn(
                "Preserve one-release SEC cutover difference report",
                workflow,
            )
            self.assertNotIn(
                ".cache/sec_security_master_migration_report.json",
                workflow,
            )

    def test_repo_has_no_retired_provider_runtime_or_config_residue(self):
        """Only deletion code and adversarial fixtures may name the provider."""

        result = subprocess.run(
            ["git", "ls-files", "-co", "--exclude-standard", "-z"],
            cwd=ROOT,
            capture_output=True,
            check=True,
        )
        provider = "open" + "figi"
        expected = {
            (
                "scripts/data_snapshot.py",
                f'Path(".cache/{provider}_details.json"),',
            ),
            (
                "tests/test_sec_provenance_validation.py",
                f'"label_source": "{provider}",',
            ),
            (
                "tests/test_sec_provenance_validation.py",
                f'"sources": ["{provider}"],',
            ),
            (
                "tests/test_sec_provenance_validation.py",
                f'"{provider}"',
            ),
        }
        pattern = re.compile(rf"open[\s_-]*{provider[4:]}", re.IGNORECASE)
        actual: list[tuple[str, str]] = []
        for raw_path in result.stdout.split(b"\0"):
            if not raw_path:
                continue
            relative_path = raw_path.decode("utf-8", errors="surrogateescape")
            path = ROOT / relative_path
            if not path.is_file() or path.is_symlink():
                continue
            if pattern.search(relative_path):
                actual.append((relative_path, "<path>"))
            payload = path.read_bytes()
            if b"\0" in payload:
                continue
            for line in payload.decode("utf-8", errors="replace").splitlines():
                if pattern.search(line):
                    actual.append((relative_path, line.strip()))

        self.assertEqual(sorted(expected), sorted(actual))

    def test_legacy_snapshot_boolean_parses_true_and_false(self):
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(workflow=path):
                workflow = read(path)
                match = re.search(
                    r"legacy_snapshot=\$\(jq -er '(.*?)' <<<\"\$restore_json\"\)",
                    workflow,
                    re.DOTALL,
                )
                self.assertIsNotNone(match)
                expression = match.group(1)
                for value, expected in ((True, "true"), (False, "false")):
                    result = subprocess.run(
                        ["jq", "-er", expression],
                        input=f'{{"legacy_snapshot":{str(value).lower()}}}',
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                    self.assertEqual(0, result.returncode, result.stderr)
                    self.assertEqual(expected, result.stdout.strip())

                invalid = subprocess.run(
                    ["jq", "-er", expression],
                    input='{"legacy_snapshot":"false"}',
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertNotEqual(0, invalid.returncode)

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
            "- name: Checkout trusted GitHub retry helper", 1
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
        expected_action = "uses: actions/create-github-app-token@v3"
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
            "- name: Checkout trusted GitHub retry helper", 1
        )[1].split("\n      - name:", 1)[0]
        self.assertIn(
            "uses: actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09 # v5.1.0",
            trusted_helper_checkout,
        )
        self.assertIn("repository: ${{ job.workflow_repository }}", trusted_helper_checkout)
        self.assertIn("ref: ${{ job.workflow_sha }}", trusted_helper_checkout)
        self.assertIn("path: .workflow-tools", trusted_helper_checkout)
        self.assertIn(f"sparse-checkout: {GH_RETRY_SCRIPT}", trusted_helper_checkout)
        target_checkouts = resolve.split(
            "- name: Checkout trusted GitHub retry helper", 1
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
        refresh_job = refresh.split("\n  rebuild_security_master:", 1)[1].split(
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

    def test_orphan_sweep_recovers_crash_between_release_and_tag_deletion(self):
        result, deleted_tags = self._run_orphan_tag_sweep(
            release_pages=(
                '[[{"tag_name":"dataset-active"},'
                '{"tag_name":"dataset-fallback"}]]'
            ),
            ref_pages=(
                '[[{"ref":"refs/tags/dataset-active"},'
                '{"ref":"refs/tags/dataset-fallback"},'
                '{"ref":"refs/tags/dataset-orphan"}]]'
            ),
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(["dataset-orphan"], deleted_tags)

    def test_orphan_sweep_fails_closed_if_active_release_is_missing(self):
        result, deleted_tags = self._run_orphan_tag_sweep(
            release_pages='[[{"tag_name":"dataset-fallback"}]]',
            ref_pages=(
                '[[{"ref":"refs/tags/dataset-active"},'
                '{"ref":"refs/tags/dataset-fallback"}]]'
            ),
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("Refusing to delete protected snapshot tag", result.stdout)
        self.assertEqual([], deleted_tags)

    def test_orphan_sweep_fails_closed_if_fallback_release_is_missing(self):
        result, deleted_tags = self._run_orphan_tag_sweep(
            release_pages='[[{"tag_name":"dataset-active"}]]',
            ref_pages=(
                '[[{"ref":"refs/tags/dataset-active"},'
                '{"ref":"refs/tags/dataset-fallback"}]]'
            ),
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("Refusing to delete protected snapshot tag", result.stdout)
        self.assertEqual([], deleted_tags)

    def test_release_cleanup_reconciles_tag_after_release_is_already_missing(self):
        for mutation_status in ("0", "75"):
            with self.subTest(mutation_status=mutation_status):
                result, release_mutations, tag_mutations = (
                    self._run_release_tag_reconciliation(
                        tag_sequence="present,missing",
                        mutation_sequence=mutation_status,
                    )
                )

                self.assertEqual(
                    0,
                    result.returncode,
                    result.stdout + result.stderr,
                )
                self.assertEqual(0, release_mutations)
                self.assertEqual(1, tag_mutations)

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
            self.assertIn("release_state()", script)
            self.assertNotIn("gh_read_retry release create", script)
            self.assertNotIn("gh_read_retry release upload", script)
            self.assertNotIn("gh_read_retry release edit", script)
            self.assertNotIn("gh_read_retry release delete", script)

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

        self.assertIn(f"sparse-checkout: {GH_RETRY_SCRIPT}", finalization)
        self.assertIn(
            f'python {GH_RETRY_SCRIPT} --allow-release-not-found -- "$@"',
            finalization,
        )
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
        deletion = finalization.split(
            'for release_tag in "${delete_tags[@]}"', 1
        )[1].split("# Fail closed if cleanup", 1)[0]
        self.assertIn('gh_delete_once release delete "$release_tag"', deletion)
        self.assertIn('release_state "$release_tag"', deletion)
        self.assertIn("wait_for_release_missing()", finalization)
        self.assertIn('wait_for_release_missing "$release_tag"', deletion)
        self.assertIn("Stale release deletion could not be reconciled", deletion)
        self.assertIn('tag_ref_state "$tag"', finalization)
        self.assertIn("wait_for_tag_missing()", finalization)
        self.assertIn('wait_for_tag_missing "$tag"', finalization)
        self.assertIn("reconcile_tag_deletion()", finalization)
        self.assertIn('reconcile_tag_deletion "$release_tag"', deletion)
        self.assertIn("gh_mutate_once api --method DELETE", finalization)
        self.assertIn(
            '"/repos/$DATA_REPOSITORY/git/refs/tags/$tag"', finalization
        )
        self.assertIn("Stale tag deletion could not be reconciled", deletion)
        self.assertIn(
            '"/repos/$DATA_REPOSITORY/git/matching-refs/tags/dataset-"',
            finalization,
        )
        self.assertIn('for orphan_tag in "${orphan_tags[@]}"', finalization)
        self.assertIn("Refusing to delete protected snapshot tag", finalization)
        self.assertIn('release_state "$orphan_tag"', finalization)
        self.assertIn('reconcile_tag_deletion "$orphan_tag"', finalization)
        self.assertIn("Orphan tag deletion could not be reconciled", finalization)

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
                "- name: Clean-backfill SEC identity and security master"
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

                # Only checksum-bound Form 13F reconstruction work and compact
                # exact-EDGAR batch journals may use Actions cache. The large
                # staged state/master contain normalized official-list rows and
                # must stay process-local. Generated data and snapshot members
                # continue to come from the authenticated private release.
                self.assertEqual(2, workflow.count("uses: actions/cache/"))
                self.assertIn("actions/cache/restore@v4", workflow)
                self.assertIn("actions/cache/save@v4", workflow)
                self.assertIn(
                    ".cache/sec_13f_bulk_rebuild_checkpoint.json",
                    workflow,
                )
                self.assertIn(
                    ".cache/sec_13f_bulk_source_state.json",
                    workflow,
                )
                self.assertIn(
                    ".cache/sec_13f_bulk_completed_receipt.json",
                    workflow,
                )
                self.assertIn(
                    ".cache/sec_13f_bulk_indices/.rebuild-*.sqlite3.partial",
                    workflow,
                )
                self.assertIn(
                    ".cache/sec_13f_bulk_indices/index-*.sqlite3",
                    workflow,
                )
                self.assertIn(
                    ".cache/"
                    "sec_13f_bulk_rebuild_checkpoint.accession-discovery.json",
                    workflow,
                )
                self.assertNotIn(
                    ".cache/sec-security-master-rebuild-work/"
                    "sec_13f_bulk_rebuild_checkpoint.accession-discovery.json",
                    workflow,
                )
                self.assertNotRegex(
                    workflow,
                    r"(?m)^\s+\.cache/sec-security-master-rebuild-work\s*$",
                )
                self.assertIn(
                    ".cache/sec-security-master-rebuild-work/manifest.json",
                    workflow,
                )
                self.assertIn(
                    ".cache/sec-security-master-rebuild-work/"
                    "edgar-exception-batch-*.json",
                    workflow,
                )
                self.assertNotIn(
                    ".cache/sec-security-master-rebuild-work/"
                    "sec_source_state.json",
                    workflow,
                )
                self.assertNotIn(
                    ".cache/sec-security-master-rebuild-work/"
                    "sec_security_master.json",
                    workflow,
                )
                self.assertIn("sec-clean-v1-parser1-", workflow)
                self.assertIn(
                    "steps.restore_snapshot.outputs.code_sha",
                    workflow,
                )
                clean_cache_lines = [
                    line
                    for line in workflow.splitlines()
                    if "sec-clean-v1-parser1-" in line
                ]
                self.assertTrue(clean_cache_lines)
                self.assertTrue(
                    all("github.sha" not in line for line in clean_cache_lines)
                )
                self.assertNotIn("path: data", workflow)
                self.assertNotIn("path: .cache/sec_security_master.json", workflow)
                self.assertIn("permissions:\n  contents: read", workflow)
                self.assertNotRegex(workflow, r"(?m)^  contents: write$")

    def test_snapshot_publisher_requires_exact_private_repository(self):
        publisher = read(PUBLISHER_SCRIPT)
        guard_call = publisher.index("\nverify_private_data_repository\n")
        for mutation in (
            'gh_mutate_once release create "$release_tag"',
            'gh_mutate_once release upload "$release_tag"',
            'gh_mutate_once release edit "$release_tag"',
        ):
            self.assertLess(guard_call, publisher.index(mutation))

        result, mutated, read_call = self._run_private_repository_guard(
            repository_json=(
                '{"full_name":"owner/private-data","private":true,'
                '"visibility":"private"}'
            )
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(mutated)
        self.assertEqual("api /repos/owner/private-data", read_call)

        rejected = (
            (
                "public",
                '{"full_name":"owner/private-data","private":false,'
                '"visibility":"public"}',
                0,
            ),
            (
                "different repository",
                '{"full_name":"owner/different-data","private":true,'
                '"visibility":"private"}',
                0,
            ),
            (
                "unconfirmed visibility",
                '{"full_name":"owner/private-data","private":true}',
                0,
            ),
            ("API read failure", "", 22),
        )
        for label, repository_json, read_status in rejected:
            with self.subTest(label=label):
                result, mutated, read_call = self._run_private_repository_guard(
                    repository_json=repository_json,
                    read_status=read_status,
                )
                self.assertNotEqual(0, result.returncode)
                self.assertFalse(mutated)
                self.assertEqual("api /repos/owner/private-data", read_call)
                self.assertIn("::error::", result.stderr)

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
        self.assertIn(
            'if [ "$code_sha" != "$(git rev-parse origin/main)" ]; then',
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

        self.assertIn("timeout --signal=TERM --kill-after=120s 180m", pipeline)
        self.assertIn('if [ "$pipeline_status" -eq 124 ]; then', pipeline)
        self.assertIn('elif [ "$pipeline_status" -ne 0 ]; then', pipeline)
        self.assertIn('exit "$pipeline_status"', pipeline)
        self.assertIn("timeout --signal=TERM --kill-after=120s 15m", recent)
        self.assertIn('if [ "$recent_status" -eq 124 ]; then', recent)
        self.assertIn('elif [ "$recent_status" -ne 0 ]; then', recent)
        self.assertIn('exit "$recent_status"', recent)

    def test_registry_regeneration_has_realistic_timeout_headroom(self):
        workflow = read(".github/workflows/update-data.yml")
        legacy = workflow.split(
            "- name: Rebuild SEC-only registry-backed site data", 1
        )[1].split("- name: Regenerate registry-backed site data", 1)[0]
        regenerate = workflow.split(
            "- name: Regenerate registry-backed site data", 1
        )[1].split("- name: Validate generated data", 1)[0]

        self.assertIn("timeout-minutes: 280", legacy)
        self.assertIn("timeout --signal=TERM --kill-after=120s 260m", legacy)
        self.assertIn(
            "Save nonpublishable SEC cutover work",
            legacy,
        )
        self.assertIn(
            "Remove cached Form 13F working set before validation",
            legacy,
        )
        self.assertIn("timeout-minutes: 90", regenerate)
        weekly = read(".github/workflows/refresh-cusip-registry.yml")
        weekly_job = weekly.split("  rebuild_security_master:", 1)[1].split(
            "\n  deploy-pages:", 1
        )[0]
        self.assertIn("timeout-minutes: 360", weekly_job)
        legacy_step = weekly_job.split(
            "- name: Clean-backfill SEC identity and security master", 1
        )[1].split("- name: Refresh and audit private SEC security master", 1)[0]
        incremental_step = weekly_job.split(
            "- name: Refresh and audit private SEC security master", 1
        )[1].split("- name: Validate generated data", 1)[0]
        self.assertIn("timeout-minutes: 280", legacy_step)
        self.assertIn(
            "timeout --signal=TERM --kill-after=120s 260m",
            legacy_step,
        )
        self.assertIn(
            "Save nonpublishable clean-rebuild work",
            legacy_step,
        )
        self.assertIn(
            "Remove cached Form 13F working set before validation",
            legacy_step,
        )
        self.assertIn("timeout-minutes: 90", incremental_step)
        self.assertIn(
            "steps.restore_snapshot.outputs.legacy_snapshot == 'true' ||",
            legacy_step,
        )
        self.assertIn(
            "inputs.rebuild_security_master == true",
            legacy_step,
        )
        self.assertIn(
            "steps.restore_snapshot.outputs.legacy_snapshot != 'true' &&",
            incremental_step,
        )
        self.assertIn(
            "inputs.rebuild_security_master != true",
            incremental_step,
        )

    def test_clean_checkpoint_is_saved_before_disk_cleanup_and_publication(self):
        cases = (
            (
                read(".github/workflows/update-data.yml"),
                "Restore nonpublishable SEC cutover checkpoint",
                "Rebuild SEC-only registry-backed site data after legacy restore",
                "Save nonpublishable SEC cutover work",
            ),
            (
                read(".github/workflows/refresh-cusip-registry.yml"),
                "Restore nonpublishable clean-rebuild checkpoint",
                "Clean-backfill SEC identity and security master",
                "Save nonpublishable clean-rebuild work",
            ),
        )
        for workflow, restore_name, clean_name, save_name in cases:
            with self.subTest(clean_name=clean_name):
                restore_at = workflow.index(restore_name)
                clean_at = workflow.index(clean_name)
                save_at = workflow.index(save_name)
                cleanup_at = workflow.index(
                    "Remove cached Form 13F working set before validation"
                )
                validate_at = workflow.index("Validate generated data")
                publish_at = workflow.index("bash scripts/publish_private_snapshot.sh")
                self.assertLess(restore_at, clean_at)
                self.assertLess(clean_at, save_at)
                self.assertLess(save_at, cleanup_at)
                self.assertLess(cleanup_at, validate_at)
                self.assertLess(validate_at, publish_at)
                save_step = workflow[save_at:cleanup_at]
                self.assertIn("if: always()", save_step)
                self.assertIn("${{ github.run_attempt }}", save_step)
                cleanup_step = workflow[cleanup_at:validate_at]
                self.assertIn("success()", cleanup_step)

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
        self.assertIn("pip install -r requirements.txt", build)
        self.assertIn("- name: Audit the public artifact allowlist", build)
        self.assertIn("include-hidden-files: true", build)
        for public_file in (
            "data/funds-index.json",
            "data/index.json",
            "data/security_labels.json",
            "data/funds/*.json.gz",
            "data/stocks/*.json.gz",
        ):
            self.assertIn(public_file, build)
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

    def test_finalization_self_heals_marker_and_keeps_active_plus_fallback(self):
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
        self.assertIn("date -u -d '24 hours ago'", finalization)
        self.assertIn("and .created_at < $stale_draft_cutoff", finalization)
        self.assertIn('and .tag_name != $active', finalization)
        self.assertIn('and ($fallback == "" or .tag_name != $fallback)', finalization)
        self.assertIn("gh_delete_once release delete", finalization)
        self.assertIn("--cleanup-tag", finalization)
        self.assertIn("active release and one fallback", finalization)

        self.assertNotIn("actions/artifacts/$artifact_id", finalization)

    def test_public_pages_artifacts_are_cleaned_even_after_deploy_failure(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        cleanup = workflow.split(
            "\n  cleanup-public-pages-artifacts:", 1
        )[1]

        self.assertIn("needs: [resolve, build, deploy]", cleanup)
        self.assertIn("if: ${{ always() }}", cleanup)
        self.assertIn("actions: write", cleanup)
        self.assertIn("artifacts_json=$(", cleanup)
        self.assertIn("gh api --paginate --slurp", cleanup)
        self.assertNotIn("mapfile -t pages_artifact_ids < <(", cleanup)
        self.assertIn(
            'select(.name == "github-pages" and .expired == false)', cleanup
        )
        self.assertIn("actions/artifacts/$artifact_id", cleanup)
        self.assertIn("--method DELETE", cleanup)
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
                "needs.rebuild_security_master.outputs"
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


if __name__ == "__main__":
    unittest.main()
