import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"


class DeploymentQueueTests(unittest.TestCase):
    def test_waiting_for_private_publication_cannot_hold_pages(self):
        wrapper = (WORKFLOWS / "deploy-pages.yml").read_text()
        publish = (WORKFLOWS / "publish-pages.yml").read_text()
        finalizer = (WORKFLOWS / "finalize-private-snapshots.yml").read_text()
        self.assertNotIn("\nconcurrency:", wrapper)
        self.assertNotIn("group: private-release-publication", publish)
        waiting_job = wrapper.split("\n  finalize-private-snapshots:", 1)[1]
        self.assertIn("group: private-release-publication", waiting_job)
        self.assertIn("uses: ./.github/workflows/finalize-private-snapshots.yml", waiting_job)
        self.assertNotIn("group: pages-production", waiting_job)
        for workflow in (publish, finalizer):
            self.assertIn("\nconcurrency:\n  group: pages-production\n  cancel-in-progress: false\n  queue: max", workflow)
        self.assertIn("needs: publish", waiting_job)
        self.assertIn("needs.publish.result == 'success'", waiting_job)
        self.assertIn("deployment_id: ${{ needs.publish.outputs.deployment_id }}", waiting_job)

    def test_superseded_or_invalid_receipt_cannot_reach_release_mutations(self):
        finalizer = (WORKFLOWS / "finalize-private-snapshots.yml").read_text()
        start = finalizer.index('          if [[ ! "$EXPECTED_DEPLOYMENT_ID"')
        end = finalizer.index('          if [[ ! "$EXPECTED_PREVIOUS_LATEST_RELEASE_TAG"', start)
        guard = textwrap.dedent(finalizer[start:end])
        for expected, observed, reaches, exit_code in (("123", 123, True, 0), ("123", 124, False, 0), ("invalid", 123, False, 1)):
            with self.subTest(expected=expected, observed=observed), tempfile.TemporaryDirectory() as directory:
                marker = Path(directory) / "release-mutation-reached"
                env = {**os.environ, "EXPECTED_DEPLOYMENT_ID": expected,
                       "EXPECTED_CODE_SHA": "a" * 40, "EXPECTED_DATASET_ID": "b" * 64,
                       "EXPECTED_RELEASE_TAG": "dataset-exact", "ALLOW_OLDER_RELEASE": "false",
                       "PUBLIC_GITHUB_TOKEN": "fixture", "RECEIPT": json.dumps({"deployment_id": observed}),
                       "MUTATION_MARKER": str(marker)}
                result = subprocess.run(["bash"], input='set -euo pipefail\npython() { printf "%s\\n" "$RECEIPT"; }\n' + guard + 'touch "$MUTATION_MARKER"\n',
                                        text=True, capture_output=True, env=env, check=False)
                self.assertEqual(exit_code, result.returncode, result.stdout + result.stderr)
                self.assertEqual(reaches, marker.exists())
