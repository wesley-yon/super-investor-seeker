#!/usr/bin/env python3
"""Initialize exact empty private insider authority documents."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
from pathlib import Path
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
)


EMPTY_APPROVED_ISSUERS = {"contract_version": 1, "issuer_ciks": []}
EMPTY_PUBLICATION_POLICY = {"contract_version": 1, "issuers": []}
CONFIRMATION = "INITIALIZE_EMPTY_PRIVATE_INSIDER_AUTHORITY_ONLY"


class InsiderAuthorityBootstrapError(ValueError):
    """Raised when the empty private-authority genesis precondition is invalid."""


class _ConfigurationError(ValueError):
    """Raised when the explicit manual genesis scope is invalid."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _ConfigurationError("command-line arguments are invalid") from None


class _Configuration(NamedTuple):
    repository_root: Path


def _read_optional_state(
    state_store: InsiderStateStore,
    key: str,
) -> dict[str, object] | None:
    try:
        return state_store.read(key)
    except FileNotFoundError:
        return None
    except (InsiderStorageError, OSError) as error:
        raise InsiderAuthorityBootstrapError(
            "existing private authority state is invalid"
        ) from error


def bootstrap_insider_authority_state(*, repository_root: Path) -> dict[str, object]:
    """Create the two exact empty authority roots idempotently."""

    state_store = InsiderStateStore(Path(repository_root))
    existing = {
        "approved-issuers-v1": _read_optional_state(state_store, "approved-issuers-v1"),
        "publication-policy-v1": _read_optional_state(
            state_store, "publication-policy-v1"
        ),
    }
    expected = {
        "approved-issuers-v1": EMPTY_APPROVED_ISSUERS,
        "publication-policy-v1": EMPTY_PUBLICATION_POLICY,
    }
    if any(
        existing[key] is not None and existing[key] != expected[key] for key in expected
    ):
        raise InsiderAuthorityBootstrapError(
            "private authority state is already nonempty"
        )

    try:
        approved = state_store.write("approved-issuers-v1", EMPTY_APPROVED_ISSUERS)
        policy = state_store.write("publication-policy-v1", EMPTY_PUBLICATION_POLICY)
    except InsiderStateRevisionError as error:
        raise InsiderAuthorityBootstrapError(
            "private authority state changed during bootstrap"
        ) from error
    created_keys = [
        key
        for key, created in (
            ("approved-issuers-v1", approved.created),
            ("publication-policy-v1", policy.created),
        )
        if created
    ]
    return {
        "changed": bool(created_keys),
        "created_keys": created_keys,
        "approved_state_sha256": approved.sha256,
        "publication_policy_sha256": policy.sha256,
    }


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", default=str(ROOT))
    parser.add_argument("--confirmation", required=True)
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
    if arguments.confirmation != CONFIRMATION:
        raise _ConfigurationError("confirmation is invalid")
    return _Configuration(repository_root=Path(repository_root))


@pipeline._serialize_pipeline_maintenance
def main(argv: Sequence[str] | None = None) -> int:
    """Initialize empty authority state and emit only bounded metadata."""

    try:
        configuration = _configuration(argv)
    except _ConfigurationError:
        pipeline.log.error(
            "private insider authority bootstrap configuration is invalid"
        )
        return 2

    try:
        result = bootstrap_insider_authority_state(
            repository_root=configuration.repository_root
        )
    except (InsiderAuthorityBootstrapError, InsiderStorageError, OSError, ValueError):
        pipeline.log.error("private insider authority bootstrap failed closed")
        return 1

    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
