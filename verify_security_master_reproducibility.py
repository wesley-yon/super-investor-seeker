#!/usr/bin/env python3
"""Compare two independently generated SEC security-master state pairs."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sec_security_master import (
    MASTER_AUDIT_SCHEMA_VERSION,
    SOURCE_STATE_SCHEMA_VERSION,
    SecurityMasterError,
    load_security_master,
    load_source_state,
    normalized_security_master_bytes,
    normalized_source_state_evidence_bytes,
    source_state_sha256,
)


_SHA256_RE = re.compile(r"[0-9a-f]{64}")


class ReproducibilityVerificationError(ValueError):
    """Raised when two clean rebuilds cannot be proven equivalent."""


@dataclass(frozen=True)
class SecurityMasterPair:
    """One validated master and its exact companion SEC source state."""

    master_path: Path
    source_state_path: Path
    master: dict[str, Any]
    source_state: dict[str, Any]


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _regular_file(path: Path, label: str) -> tuple[Path, tuple[int, int]]:
    candidate = Path(path).absolute()
    try:
        metadata = candidate.lstat()
    except OSError as exc:
        raise ReproducibilityVerificationError(
            f"{label} is not a readable regular file: {candidate}"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ReproducibilityVerificationError(
            f"{label} must be a real regular file, not a symlink: {candidate}"
        )
    return candidate, (metadata.st_dev, metadata.st_ino)


def _load_pair(
    *,
    master_path: Path,
    source_state_path: Path,
    label: str,
) -> SecurityMasterPair:
    master = load_security_master(master_path)
    source_state = load_source_state(source_state_path)

    if source_state.get("schema_version") != SOURCE_STATE_SCHEMA_VERSION:
        raise ReproducibilityVerificationError(
            f"{label} source state is not on the current schema"
        )
    if master.get("source_state_schema_version") != SOURCE_STATE_SCHEMA_VERSION:
        raise ReproducibilityVerificationError(
            f"{label} master is not bound to the current source-state schema"
        )
    audit = master.get("audit")
    if (
        not isinstance(audit, dict)
        or audit.get("schema_version") != MASTER_AUDIT_SCHEMA_VERSION
    ):
        raise ReproducibilityVerificationError(
            f"{label} master does not contain the current acceptance audit"
        )
    if _SHA256_RE.fullmatch(str(master.get("universe_sha256") or "")) is None:
        raise ReproducibilityVerificationError(
            f"{label} master has no valid security-universe identity"
        )
    if not isinstance(source_state.get("updated_at"), str):
        raise ReproducibilityVerificationError(
            f"{label} source state has no completed-build timestamp"
        )
    if master.get("generated_at") != source_state["updated_at"]:
        raise ReproducibilityVerificationError(
            f"{label} master and source state have different build clocks"
        )
    if not master.get("records") or not master.get("sources"):
        raise ReproducibilityVerificationError(
            f"{label} master is empty and cannot prove a full rebuild"
        )
    if not source_state.get("sources"):
        raise ReproducibilityVerificationError(
            f"{label} source state has no accepted SEC sources"
        )

    expected_state_digest = source_state_sha256(source_state)
    if master.get("source_state_sha256") != expected_state_digest:
        raise ReproducibilityVerificationError(
            f"{label} master is not bound to its companion source state"
        )

    return SecurityMasterPair(
        master_path=master_path,
        source_state_path=source_state_path,
        master=master,
        source_state=source_state,
    )


def verify_security_master_reproducibility(
    *,
    first_master_path: Path,
    first_source_state_path: Path,
    second_master_path: Path,
    second_source_state_path: Path,
) -> dict[str, Any]:
    """Require two distinct, valid pairs to have identical normalized output."""

    requested = (
        (Path(first_master_path), "first master"),
        (Path(first_source_state_path), "first source state"),
        (Path(second_master_path), "second master"),
        (Path(second_source_state_path), "second source state"),
    )
    checked = [_regular_file(path, label) for path, label in requested]
    paths = [item[0] for item in checked]
    identities = [item[1] for item in checked]
    if len(set(identities)) != len(identities):
        raise ReproducibilityVerificationError(
            "all four inputs must be distinct filesystem files"
        )

    first = _load_pair(
        master_path=paths[0],
        source_state_path=paths[1],
        label="first",
    )
    second = _load_pair(
        master_path=paths[2],
        source_state_path=paths[3],
        label="second",
    )

    first_evidence = normalized_source_state_evidence_bytes(first.source_state)
    second_evidence = normalized_source_state_evidence_bytes(second.source_state)
    first_master = normalized_security_master_bytes(first.master)
    second_master = normalized_security_master_bytes(second.master)
    evidence_digests = (_sha256(first_evidence), _sha256(second_evidence))
    master_digests = (_sha256(first_master), _sha256(second_master))

    mismatches: list[str] = []
    if first_evidence != second_evidence:
        mismatches.append(
            "SEC input/evidence identity differs "
            f"({evidence_digests[0]} != {evidence_digests[1]})"
        )
    if first_master != second_master:
        mismatches.append(
            "normalized security-master output differs "
            f"({master_digests[0]} != {master_digests[1]})"
        )
    if mismatches:
        raise ReproducibilityVerificationError("; ".join(mismatches))

    return {
        "ok": True,
        "normalized_master_sha256": master_digests[0],
        "sec_evidence_sha256": evidence_digests[0],
        "record_count": len(first.master["records"]),
        "source_count": len(first.source_state["sources"]),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first-master", type=Path, required=True)
    parser.add_argument("--first-source-state", type=Path, required=True)
    parser.add_argument("--second-master", type=Path, required=True)
    parser.add_argument("--second-source-state", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        summary = verify_security_master_reproducibility(
            first_master_path=args.first_master,
            first_source_state_path=args.first_source_state,
            second_master_path=args.second_master,
            second_source_state_path=args.second_source_state,
        )
    except (ReproducibilityVerificationError, SecurityMasterError) as exc:
        print(f"SEC rebuild reproducibility verification failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
