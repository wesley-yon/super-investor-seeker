#!/usr/bin/env python3
"""Restore an explicitly selected private prerelease for an isolated runner test.

Production pull rules stay unchanged: candidate releases are never selected by
the production latest-dataset or Pages flows. This helper only performs GETs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import data_snapshot as snapshot  # noqa: E402


def validate_candidate(repository, tag, repository_info, release):
    if not snapshot.REPOSITORY_RE.fullmatch(repository):
        raise snapshot.SnapshotError("Invalid candidate repository")
    if not re.fullmatch(r"candidate-sec-[A-Za-z0-9._-]+", tag):
        raise snapshot.SnapshotError("Candidate tag must use the isolated candidate-sec- prefix")
    if not isinstance(repository_info, dict) or not (
        repository_info.get("full_name") == repository
        and repository_info.get("private") is True
        and repository_info.get("visibility") == "private"
    ):
        raise snapshot.SnapshotError("Candidate repository is not API-confirmed private")
    if not isinstance(release, dict) or not (
        release.get("tag_name") == tag
        and release.get("draft") is False
        and release.get("prerelease") is True
        and isinstance(release.get("assets"), list)
    ):
        raise snapshot.SnapshotError("Expected the exact published candidate prerelease")
    for asset in release["assets"]:
        name = asset.get("name") if isinstance(asset, dict) else None
        if not isinstance(name, str) or not name or Path(name).name != name or "\\" in name:
            raise snapshot.SnapshotError("Candidate asset name is not a plain filename")


def restore_candidate(*, repository: str, tag: str, root: Path, expected_source_sha: str,
                      with_benchmark_batch: bool = False):
    if not snapshot.REPOSITORY_RE.fullmatch(repository):
        raise snapshot.SnapshotError("Invalid candidate repository")
    if not re.fullmatch(r"candidate-sec-[A-Za-z0-9._-]+", tag):
        raise snapshot.SnapshotError("Invalid candidate tag")
    if not snapshot.SOURCE_SHA_RE.fullmatch(expected_source_sha):
        raise snapshot.SnapshotError("Expected candidate code SHA is invalid")
    token = os.environ.get(snapshot.TOKEN_ENV)
    if not token:
        raise snapshot.SnapshotError("Runtime private repository read credential is missing")
    base = f"{snapshot.API_BASE}/repos/{repository}"
    repository_info = snapshot._github_json(base, token)
    release = snapshot._github_json(f"{base}/releases/tags/{quote(tag, safe='')}", token)
    validate_candidate(repository, tag, repository_info, release)
    root = root.resolve()
    # This helper is deliberately restricted to a fresh test checkout.
    if (root / "data").exists() or (root / ".cache").exists():
        raise snapshot.SnapshotError("Candidate restore requires a checkout without data or caches")
    assets = release["assets"]
    manifest_asset = snapshot._find_asset(assets, suffix=snapshot.MANIFEST_SUFFIX)
    with tempfile.TemporaryDirectory(prefix="sec-candidate-", dir=root.parent) as directory:
        temporary = Path(directory)
        manifest_path = temporary / manifest_asset["name"]
        snapshot._download_asset(asset=manifest_asset, destination=manifest_path,
                                 token=token, max_bytes=snapshot.MAX_MANIFEST_BYTES)
        manifest = snapshot._load_manifest(manifest_path)
        if manifest["contract_version"] != snapshot.CONTRACT_VERSION:
            raise snapshot.SnapshotError("Candidate must use the current snapshot contract")
        if manifest["source_sha"] != expected_source_sha:
            raise snapshot.SnapshotError("Candidate snapshot does not match the tested code SHA")
        archive_asset = snapshot._find_asset(assets, name=manifest["archive"]["filename"])
        names = [item.get("name") for item in assets if isinstance(item, dict)]
        expected_names = {manifest_asset["name"], archive_asset["name"]}
        if with_benchmark_batch:
            expected_names.add('incremental-benchmark-batch.json.gz')
        if len(assets) != len(expected_names) or set(names) != expected_names:
            raise snapshot.SnapshotError("Candidate must contain exactly its archive and manifest")
        if archive_asset["size"] != manifest["archive"]["bytes"]:
            raise snapshot.SnapshotError("Candidate asset size differs from its manifest")
        archive_path = temporary / archive_asset["name"]
        snapshot._download_asset(asset=archive_asset, destination=archive_path, token=token,
                                 max_bytes=snapshot.DEFAULT_MAX_ARCHIVE_BYTES)
        payload = temporary / "payload"
        summary = snapshot.verify_snapshot(archive_path=archive_path, manifest_path=manifest_path,
                                           extract_root=payload)
        snapshot._replace_payload(root, payload, contract_version=summary["contract_version"])
        if with_benchmark_batch:
            batch_asset = snapshot._find_asset(assets, name='incremental-benchmark-batch.json.gz')
            batch_path = root / '.cache' / batch_asset['name']
            digest = batch_asset.get('digest', '')
            if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
                raise snapshot.SnapshotError('Benchmark asset has no GitHub checksum')
            snapshot._download_asset(asset=batch_asset, destination=batch_path, token=token,
                                     max_bytes=64 * 1024 * 1024)
            if 'sha256:' + hashlib.sha256(batch_path.read_bytes()).hexdigest() != digest:
                raise snapshot.SnapshotError('Benchmark asset checksum mismatch')
    summary.pop("extract_root", None)
    summary.update({"candidate_release": tag, "code_sha": expected_source_sha,
                    "repository_private": True, "production_publication": False})
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--expected-source-sha", required=True)
    parser.add_argument("--with-benchmark-batch", action="store_true")
    args = parser.parse_args()
    print(json.dumps(restore_candidate(repository=args.repository, tag=args.tag, root=args.root,
                                       expected_source_sha=args.expected_source_sha,
                                       with_benchmark_batch=args.with_benchmark_batch), sort_keys=True))


if __name__ == "__main__":
    main()
