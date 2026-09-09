#!/usr/bin/env python3
"""Bind generated data to Git inputs while allowing presentation-only advances.

The snapshot keeps its actual producing SHA. A compatible newer SHA is only a
Pages target: Pages restores that exact snapshot and runs its current frontend
contract and data checks before deployment. Unknown paths are data dependencies.
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import subprocess


PRESENTATION_PATHS = frozenset({
    "index.html", "app.js", "site-data-loader.js", "CNAME", ".nojekyll",
})


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *args])


def commit(root: Path, ref: str) -> str:
    return git(root, "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}").decode().strip()


def data_fingerprint(root: Path, ref: str) -> str:
    digest = hashlib.sha256(b"data-code-identity-v1\0")
    tree = git(root, "ls-tree", "-rz", "--full-tree", commit(root, ref))
    for entry in tree.split(b"\0"):
        if not entry:
            continue
        _, path = entry.split(b"\t", 1)
        if path.decode("utf-8", errors="surrogateescape") not in PRESENTATION_PATHS:
            # Include mode and object type as well as path and content identity.
            digest.update(entry + b"\0")
    return digest.hexdigest()


def compatible_target(root: Path, source: str, target: str) -> str:
    source_sha, target_sha = commit(root, source), commit(root, target)
    if source_sha == target_sha:
        return target_sha
    ancestry = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", source_sha, target_sha],
        check=False,
    )
    if ancestry.returncode != 0 or data_fingerprint(root, source_sha) != data_fingerprint(root, target_sha):
        raise ValueError("data-processing inputs changed on main; aborting stale publication")
    return target_sha


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", default="origin/main")
    args = parser.parse_args()
    try:
        print(compatible_target(args.root, args.source, args.target))
    except (ValueError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f"::error::{exc}\n")


if __name__ == "__main__":
    main()
