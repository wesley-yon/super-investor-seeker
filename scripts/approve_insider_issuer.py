#!/usr/bin/env python3
"""Approve one exact issuer for private Section 16 ingestion."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import NamedTuple, NoReturn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pipeline  # noqa: E402
from insider_storage import (  # noqa: E402
    InsiderStateRevisionError,
    InsiderStateStore,
    InsiderStorageError,
    canonical_insider_state_json_bytes,
)
from security_identity import normalize_section16_cik  # noqa: E402

SERVICENOW_CIK = "0001373715"
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


class InsiderIssuerApprovalError(ValueError):
    """Raised when a manual private-ingestion approval is invalid."""


class _ConfigurationError(ValueError):
    """Raised when the explicit manual approval scope is invalid."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _ConfigurationError("command-line arguments are invalid") from None


class _Configuration(NamedTuple):
    repository_root: Path
    issuer_cik: str
    expected_current_sha256: str


def _canonical_servicenow_cik(value: object) -> str:
    try:
        normalized_issuer = normalize_section16_cik(value)
    except (TypeError, ValueError) as error:
        raise InsiderIssuerApprovalError("issuer CIK is invalid") from error
    if type(value) is not str or normalized_issuer != value:
        raise InsiderIssuerApprovalError("issuer CIK must be canonical")
    if normalized_issuer != SERVICENOW_CIK:
        raise InsiderIssuerApprovalError(
            "issuer CIK is not approved for this operation"
        )
    return normalized_issuer


def approve_insider_issuer(
    *,
    repository_root: Path,
    issuer_cik: str,
    expected_current_sha256: str,
) -> dict[str, object]:
    """Add one exact canonical issuer CIK through a compare-and-swap update."""

    normalized_issuer = _canonical_servicenow_cik(issuer_cik)

    state_store = InsiderStateStore(Path(repository_root))
    current = state_store.read("approved-issuers-v1")
    current_sha256 = hashlib.sha256(
        canonical_insider_state_json_bytes(current)
    ).hexdigest()
    if current_sha256 != expected_current_sha256:
        raise InsiderStateRevisionError("private state revision is stale")

    current_issuers = current["issuer_ciks"]
    assert isinstance(current_issuers, list)
    candidate = {
        "contract_version": current["contract_version"],
        "issuer_ciks": sorted({*current_issuers, normalized_issuer}),
    }
    stored = state_store.write(
        "approved-issuers-v1",
        candidate,
        expected_sha256=current_sha256,
    )
    return {
        "issuer_cik": normalized_issuer,
        "changed": stored.created,
        "previous_issuer_count": len(current_issuers),
        "approved_issuer_count": len(candidate["issuer_ciks"]),
        "approved_state_sha256": stored.sha256,
    }


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", default=str(ROOT))
    parser.add_argument("--issuer-cik", required=True)
    parser.add_argument("--expected-current-sha256", required=True)
    return parser


def _configuration(argv: Sequence[str] | None) -> _Configuration:
    if argv is not None and (
        isinstance(argv, (str, bytes))
        or any(type(argument) is not str for argument in argv)
    ):
        raise _ConfigurationError("command-line arguments are invalid")
    arguments = _parser().parse_args(None if argv is None else list(argv))
    repository_root = arguments.repository_root
    if type(repository_root) is not str or not repository_root:
        raise _ConfigurationError("repository root is invalid")
    try:
        issuer_cik = _canonical_servicenow_cik(arguments.issuer_cik)
    except InsiderIssuerApprovalError as error:
        raise _ConfigurationError("issuer CIK is invalid") from error
    expected_current_sha256 = arguments.expected_current_sha256
    if (
        type(expected_current_sha256) is not str
        or _SHA256_RE.fullmatch(expected_current_sha256) is None
    ):
        raise _ConfigurationError("expected state SHA-256 is invalid")
    return _Configuration(
        repository_root=Path(repository_root),
        issuer_cik=issuer_cik,
        expected_current_sha256=expected_current_sha256,
    )


@pipeline._serialize_pipeline_maintenance
def main(argv: Sequence[str] | None = None) -> int:
    """Apply one exact private-ingestion approval and emit bounded metadata."""

    try:
        configuration = _configuration(argv)
    except _ConfigurationError:
        pipeline.log.error("private insider approval configuration is invalid")
        return 2

    try:
        result = approve_insider_issuer(
            repository_root=configuration.repository_root,
            issuer_cik=configuration.issuer_cik,
            expected_current_sha256=configuration.expected_current_sha256,
        )
    except (InsiderStorageError, OSError, ValueError):
        pipeline.log.error("private insider approval failed closed")
        return 1

    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
