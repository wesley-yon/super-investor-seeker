from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = ROOT / ".github" / "workflows"
EXTERNAL_ACTION_RE = re.compile(
    r"^\s*uses:\s*([^\s@]+)@([^\s#]+)(?:\s+#\s*(.+))?\s*$"
)
SHA_RE = re.compile(r"[0-9a-f]{40}")


class RuntimeSupplyChainContractTests(unittest.TestCase):
    def test_python_311_contract_is_machine_readable_and_documented(self) -> None:
        pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn('requires-python = ">=3.11"', pyproject)
        self.assertIn("Python 3.11 or newer is required", readme)
        self.assertIn("python3.11 -m venv .venv", readme)
        self.assertIn(
            ".venv/bin/pip install --require-hashes -r requirements.lock",
            readme,
        )

    def test_hash_locked_runtime_and_development_dependencies_exist(self) -> None:
        for relative_path, required_packages in (
            ("requirements.lock", {"requests", "lxml"}),
            ("requirements-dev.lock", {"requests", "lxml", "ruff"}),
        ):
            with self.subTest(path=relative_path):
                payload = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn("--generate-hashes", payload)
                self.assertNotRegex(payload, r"(?m)^\s*-r\s+")
                pinned = {
                    match.group(1).lower()
                    for match in re.finditer(
                        r"(?m)^([A-Za-z0-9_.-]+)==[^\s\\]+(?:\s*\\)?$",
                        payload,
                    )
                }
                self.assertTrue(required_packages <= pinned)
                self.assertGreaterEqual(payload.count("--hash=sha256:"), len(pinned))

    def test_workflows_install_only_hash_locked_dependencies(self) -> None:
        expectations = {
            "update-data.yml": "requirements.lock",
            "deploy-pages.yml": "requirements.lock",
            "refresh-cusip-registry.yml": "requirements.lock",
            "test.yml": "requirements-dev.lock",
        }
        for workflow_name, lockfile in expectations.items():
            with self.subTest(workflow=workflow_name):
                payload = (WORKFLOW_ROOT / workflow_name).read_text(encoding="utf-8")
                self.assertIn(
                    f"pip install --require-hashes -r {lockfile}",
                    payload,
                )
                self.assertNotRegex(
                    payload,
                    r"pip install -r requirements(?:-dev)?\.txt",
                )

    def test_all_external_actions_are_full_sha_pinned_with_version_comments(self) -> None:
        found = 0
        for workflow in sorted(WORKFLOW_ROOT.glob("*.yml")):
            for line_number, line in enumerate(
                workflow.read_text(encoding="utf-8").splitlines(),
                1,
            ):
                stripped = line.strip()
                if not stripped.startswith("uses:"):
                    continue
                reference = stripped.removeprefix("uses:").strip()
                if reference.startswith("./"):
                    continue
                found += 1
                match = EXTERNAL_ACTION_RE.match(line)
                self.assertIsNotNone(
                    match,
                    f"invalid action reference at {workflow}:{line_number}",
                )
                assert match is not None
                self.assertRegex(
                    match.group(2),
                    SHA_RE,
                    f"mutable action reference at {workflow}:{line_number}",
                )
                self.assertIsNotNone(
                    match.group(3),
                    f"missing version comment at {workflow}:{line_number}",
                )
                self.assertRegex(
                    match.group(3) or "",
                    r"^v\d",
                    f"invalid version comment at {workflow}:{line_number}",
                )
        self.assertGreater(found, 0)


if __name__ == "__main__":
    unittest.main()
