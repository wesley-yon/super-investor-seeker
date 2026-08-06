import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAINTENANCE_WORKFLOWS = (
    ".github/workflows/update-data.yml",
    ".github/workflows/refresh-cusip-registry.yml",
)


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


class WorkflowResilienceTests(unittest.TestCase):
    def test_critical_schedules_avoid_top_of_hour_without_changing_windows(self):
        update = read(".github/workflows/update-data.yml")
        refresh = read(".github/workflows/refresh-cusip-registry.yml")

        self.assertIn("cron: '23 11-23 * * 1-5'", update)
        self.assertIn("cron: '23 4 * * 0'", refresh)
        self.assertNotRegex(update, r"(?m)^\s*- cron: '0 ")
        self.assertNotRegex(refresh, r"(?m)^\s*- cron: '0 ")

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

    def test_pages_resolve_checkouts_are_sparse_and_blobless(self):
        workflow = read(".github/workflows/deploy-pages.yml")
        resolve = workflow.split("  resolve:", 1)[1].split("\n  build:", 1)[0]
        checkouts = re.findall(
            r"(?ms)^      - name: Checkout .*?(?=^      - name: |\Z)",
            resolve,
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

    def test_publishers_are_atomic_private_releases_not_git_commits(self):
        required_fragments = (
            "python scripts/data_snapshot.py pack",
            "--max-archive-bytes 1932735283",
            'gh release create "$release_tag"',
            "--draft",
            'gh release upload "$release_tag"',
            'gh release download "$release_tag"',
            "python scripts/data_snapshot.py verify",
            "does not contain exactly two snapshot assets",
            'gh release edit "$release_tag"',
            "--draft=false",
            "--latest",
        )
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                for fragment in required_fragments:
                    self.assertIn(fragment, workflow)
                self.assertLess(
                    workflow.index('gh release create "$release_tag"'),
                    workflow.index("python scripts/data_snapshot.py verify"),
                )
                self.assertLess(
                    workflow.index("python scripts/data_snapshot.py verify"),
                    workflow.index('gh release edit "$release_tag"'),
                )
                self.assertNotIn("git add data/", workflow)
                self.assertNotIn("git commit", workflow)
                self.assertNotIn("git push", workflow)
                self.assertNotIn("actions/cache", workflow)
                self.assertIn("permissions:\n  contents: read", workflow)
                self.assertNotRegex(workflow, r"(?m)^  contents: write$")

    def test_publishers_output_exact_deployment_identity(self):
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
                        workflow,
                    )
                self.assertIn(
                    'if [ "$dataset_id" = "$BASE_DATASET_ID" ]; then',
                    workflow,
                )
                self.assertIn("PUBLIC_GITHUB_TOKEN: ${{ github.token }}", workflow)
                self.assertIn('GH_TOKEN="$PUBLIC_GITHUB_TOKEN"', workflow)
                self.assertIn('echo "site_changed=true"', workflow)

    def test_maintenance_stale_code_guard_fails_closed(self):
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                publish = workflow.split(
                    "- name: Publish ", 1
                )[1]
                fetch = "git fetch --no-tags origin main:refs/remotes/origin/main"
                self.assertIn(fetch, publish)
                self.assertIn(
                    'if [ "$code_sha" != "$(git rev-parse origin/main)" ]; then',
                    publish,
                )
                self.assertIn("aborting stale publication", publish)
                self.assertNotIn("git reset --hard", publish)

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
            "gh release upload", 1
        )[0]
        self.assertIn('--arg deployed_at "$deployed_at"', marker_build)
        self.assertNotIn("$(date -u", marker_build)
        self.assertIn("jq -er '.deployed_at'", finalization)
        self.assertIn(
            'if [[ ! "$deployment_id" =~ ^[0-9]+$ ]] || [ -z "$deployed_at" ]; then',
            finalization,
        )
        self.assertIn('gh release upload "$EXPECTED_RELEASE_TAG"', finalization)
        self.assertIn("Private Pages deployment marker did not round-trip exactly", finalization)
        self.assertIn("date -u -d '24 hours ago'", finalization)
        self.assertIn("and .created_at < $stale_draft_cutoff", finalization)
        self.assertIn('and .tag_name != $active', finalization)
        self.assertIn('and ($fallback == "" or .tag_name != $fallback)', finalization)
        self.assertIn("gh release delete", finalization)
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
            'release_tag=$(gh api "/repos/$DATA_REPOSITORY/releases/latest"',
            resolve,
        )
        self.assertIn("allow_older_release=true", resolve)
        self.assertIn('gh release edit "$EXPECTED_RELEASE_TAG"', finalization)
        self.assertIn("--latest", finalization)
        self.assertIn('if [ "$ALLOW_OLDER_RELEASE" = true ]; then', finalization)
        self.assertIn("The active release changed before finalization", finalization)
        self.assertIn(
            'gh api "/repos/$DATA_REPOSITORY/releases/latest"', finalization
        )

    def test_unchanged_maintenance_preserves_active_pages_release(self):
        for path in MAINTENANCE_WORKFLOWS:
            with self.subTest(path=path):
                workflow = read(path)
                unchanged = workflow.split(
                    'if [ "$dataset_id" = "$BASE_DATASET_ID" ]; then', 1
                )[1].split("\n          fi", 1)[0]
                self.assertIn(
                    'active_release_json=$(gh api "/repos/$DATA_REPOSITORY/releases/latest")',
                    unchanged,
                )
                self.assertIn('"$active_dataset_id" "$active_release_tag"', unchanged)
                self.assertIn(
                    'echo "release_tag=$active_release_tag"', unchanged
                )
                self.assertIn(
                    'echo "dataset_id=$active_dataset_id"', unchanged
                )
                self.assertNotIn(
                    'echo "release_tag=$BASE_RELEASE_TAG"', unchanged
                )

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

        self.assertRegex(workflow, r"(?m)^  push:\s*$")
        self.assertNotIn("branches: [main]", workflow)
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
