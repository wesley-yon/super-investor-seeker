#!/usr/bin/env python3
"""Approve one exact reviewed ServiceNow publication policy privately."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import NamedTuple, NoReturn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pipeline  # noqa: E402
from insider_publication_policy import (  # noqa: E402
    SERVICENOW_CIK,
    ServiceNowPublicationPolicyError,
    publication_policy_sha256,
)
from insider_storage import (  # noqa: E402
    MAX_INSIDER_STATE_BYTES,
    InsiderStateRevisionError,
    InsiderStateStore,
    InsiderStorageError,
    canonical_insider_state_json_bytes,
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_FILE_READ_FLAGS = (
    os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class InsiderPublicationPolicyApprovalError(ValueError):
    """Raised when a fixed-scope publication-policy approval is invalid."""


class _ConfigurationError(ValueError):
    """Raised when explicit approval CLI configuration is invalid."""


class _CandidateFileError(ValueError):
    """Raised when the owner-only candidate file is unsafe or malformed."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _ConfigurationError("command-line arguments are invalid") from None


class _Configuration(NamedTuple):
    repository_root: Path
    candidate_policy: Path
    expected_current_policy_sha256: str
    expected_issuer_generation_digest: str
    expected_candidate_policy_sha256: str


def _sha256(value: object, label: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise InsiderPublicationPolicyApprovalError(label)
    return value


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _CandidateFileError("candidate JSON is invalid")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> NoReturn:
    raise _CandidateFileError("candidate JSON is invalid")


def _read_candidate_policy(path: Path) -> object:
    try:
        descriptor = os.open(path, _FILE_READ_FLAGS)
    except OSError as error:
        raise _CandidateFileError("candidate file is unavailable") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size < 1
            or before.st_size > MAX_INSIDER_STATE_BYTES
        ):
            raise _CandidateFileError("candidate file is unsafe")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                raise _CandidateFileError("candidate file changed during read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise _CandidateFileError("candidate file changed during read")
        after = os.fstat(descriptor)
        identity = lambda value: (
            value.st_dev,
            value.st_ino,
            value.st_uid,
            value.st_gid,
            value.st_mode,
            value.st_nlink,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )
        if identity(after) != identity(before):
            raise _CandidateFileError("candidate file changed during read")
        rendered = b"".join(chunks)
    except OSError as error:
        raise _CandidateFileError("candidate file is unavailable") from error
    finally:
        os.close(descriptor)

    try:
        parsed = json.loads(
            rendered.decode("utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_nonfinite,
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        ValueError,
    ) as error:
        if isinstance(error, _CandidateFileError):
            raise
        raise _CandidateFileError("candidate JSON is invalid") from error
    try:
        if canonical_insider_state_json_bytes(parsed) != rendered:
            raise _CandidateFileError("candidate JSON is not canonical")
    except (RecursionError, TypeError, ValueError) as error:
        if isinstance(error, _CandidateFileError):
            raise
        raise _CandidateFileError("candidate JSON is invalid") from error
    return parsed


def approve_servicenow_publication_policy(
    *,
    repository_root: Path,
    candidate_policy: object,
    expected_current_policy_sha256: str,
    expected_issuer_generation_digest: str,
    expected_candidate_policy_sha256: str,
) -> dict[str, object]:
    """CAS-approve one exact ServiceNow policy or fail before mutation."""

    expected_current = _sha256(
        expected_current_policy_sha256,
        "expected current policy SHA-256",
    )
    expected_generation = _sha256(
        expected_issuer_generation_digest,
        "expected issuer generation digest",
    )
    expected_candidate = _sha256(
        expected_candidate_policy_sha256,
        "expected candidate policy SHA-256",
    )
    detached_candidate = json.loads(
        canonical_insider_state_json_bytes(candidate_policy)
    )
    candidate_sha256 = publication_policy_sha256(detached_candidate)
    if candidate_sha256 != expected_candidate:
        raise InsiderStateRevisionError("private state revision is stale")
    issuers = detached_candidate["issuers"]
    assert isinstance(issuers, list) and len(issuers) == 1
    issuer = issuers[0]
    assert isinstance(issuer, dict)
    mappings = issuer["security_mappings"]
    assert isinstance(mappings, dict)

    stored = InsiderStateStore(
        Path(repository_root)
    ).approve_publication_policy_for_approved_issuer(
        SERVICENOW_CIK,
        detached_candidate,
        expected_current_policy_sha256=expected_current,
        expected_issuer_generation_digest=expected_generation,
        expected_candidate_policy_sha256=expected_candidate,
    )
    return {
        "issuer_cik": SERVICENOW_CIK,
        "changed": stored.created,
        "security_class_count": len(mappings),
        "issuer_generation_digest": expected_generation,
        "publication_policy_sha256": stored.sha256,
    }


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", required=True)
    parser.add_argument("--candidate-policy", required=True)
    parser.add_argument("--expected-current-policy-sha256", required=True)
    parser.add_argument("--expected-issuer-generation-digest", required=True)
    parser.add_argument("--expected-candidate-policy-sha256", required=True)
    return parser


def _configuration(argv: Sequence[str] | None) -> _Configuration:
    if argv is not None and (
        isinstance(argv, (str, bytes))
        or any(type(argument) is not str for argument in argv)
    ):
        raise _ConfigurationError("command-line arguments are invalid")
    arguments = _parser().parse_args(None if argv is None else list(argv))
    values = (
        arguments.repository_root,
        arguments.candidate_policy,
        arguments.expected_current_policy_sha256,
        arguments.expected_issuer_generation_digest,
        arguments.expected_candidate_policy_sha256,
    )
    if any(type(value) is not str or not value for value in values):
        raise _ConfigurationError("command-line arguments are invalid")
    try:
        expected_current = _sha256(
            arguments.expected_current_policy_sha256,
            "expected current policy SHA-256",
        )
        expected_generation = _sha256(
            arguments.expected_issuer_generation_digest,
            "expected issuer generation digest",
        )
        expected_candidate = _sha256(
            arguments.expected_candidate_policy_sha256,
            "expected candidate policy SHA-256",
        )
    except InsiderPublicationPolicyApprovalError as error:
        raise _ConfigurationError("command-line arguments are invalid") from error
    return _Configuration(
        repository_root=Path(arguments.repository_root),
        candidate_policy=Path(arguments.candidate_policy),
        expected_current_policy_sha256=expected_current,
        expected_issuer_generation_digest=expected_generation,
        expected_candidate_policy_sha256=expected_candidate,
    )


@pipeline._serialize_pipeline_maintenance
def main(argv: Sequence[str] | None = None) -> int:
    """Apply one fixed-scope private policy approval and emit bounded metadata."""

    try:
        configuration = _configuration(argv)
    except _ConfigurationError:
        pipeline.log.error(
            "private publication-policy approval configuration is invalid"
        )
        return 2

    try:
        candidate = _read_candidate_policy(configuration.candidate_policy)
        result = approve_servicenow_publication_policy(
            repository_root=configuration.repository_root,
            candidate_policy=candidate,
            expected_current_policy_sha256=(
                configuration.expected_current_policy_sha256
            ),
            expected_issuer_generation_digest=(
                configuration.expected_issuer_generation_digest
            ),
            expected_candidate_policy_sha256=(
                configuration.expected_candidate_policy_sha256
            ),
        )
    except (
        InsiderStorageError,
        ServiceNowPublicationPolicyError,
        OSError,
        ValueError,
    ):
        pipeline.log.error("private publication-policy approval failed closed")
        return 1

    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


__all__ = [
    "InsiderPublicationPolicyApprovalError",
    "approve_servicenow_publication_policy",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
