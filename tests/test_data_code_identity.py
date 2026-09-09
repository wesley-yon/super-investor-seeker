import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.data_code_identity import PRESENTATION_PATHS, compatible_target, data_fingerprint


class DataCodeIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.invalid")
        for path in (*PRESENTATION_PATHS, "pipeline.py", "requirements.txt", "data_contract.py"):
            (self.root / path).write_text("original\n")
        self.base = self.save("base")

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.root), *args], stderr=subprocess.PIPE).decode().strip()

    def save(self, message):
        self.git("add", ".")
        self.git("commit", "-qm", message)
        return self.git("rev-parse", "HEAD")

    def test_frontend_advance_selects_new_target_without_relabeling_producer(self):
        original_fingerprint = data_fingerprint(self.root, self.base)
        for path in PRESENTATION_PATHS:
            (self.root / path).write_text("changed presentation\n")
        target = self.save("frontend")
        snapshot = {"source_sha": self.base}
        self.assertEqual(target, compatible_target(self.root, self.base, target))
        self.assertEqual(original_fingerprint, data_fingerprint(self.root, target))
        self.assertEqual(self.base, json.loads(json.dumps(snapshot))["source_sha"])

    def test_unknown_and_data_dependency_changes_fail_closed(self):
        for path in ("pipeline.py", "requirements.txt", "data_contract.py", "new_dependency.py", ".github/workflows/update-data.yml"):
            with self.subTest(path=path):
                self.git("checkout", "-q", self.base)
                target_path = self.root / path
                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_text("changed\n")
                target = self.save("data")
                with self.assertRaisesRegex(ValueError, "aborting stale publication"):
                    compatible_target(self.root, self.base, target)

    def test_non_descendant_with_identical_data_is_rejected(self):
        (self.root / "index.html").write_text("left\n")
        left = self.save("left")
        self.git("checkout", "-q", self.base)
        (self.root / "index.html").write_text("right\n")
        right = self.save("right")
        self.assertEqual(data_fingerprint(self.root, left), data_fingerprint(self.root, right))
        with self.assertRaises(ValueError):
            compatible_target(self.root, left, right)

    def test_unchanged_commit_is_compatible(self):
        self.assertEqual(self.base, compatible_target(self.root, self.base, self.base))

    def test_workflow_ignores_only_the_same_presentation_paths(self):
        workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/update-data.yml").read_text()
        ignored = workflow.split("    paths-ignore:\n", 1)[1].split("  schedule:", 1)[0]
        paths = {line.strip()[3:-1] for line in ignored.splitlines() if line.strip().startswith("- '")}
        self.assertEqual(PRESENTATION_PATHS, paths)
