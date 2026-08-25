#!/usr/bin/env python3
"""Reprocess verified stored Section 16 filings without network access."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
import re
import sys
import time
from typing import NoReturn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pipeline  # noqa: E402
from insider_pipeline import (  # noqa: E402
    MAX_RECENT_INSIDER_DEADLINE_SECONDS,
    CooperativeDeadline,
    InsiderReparseError,
    InsiderReparseOutcome,
    InsiderReparseRunResult,
    insider_telemetry_run,
    new_insider_telemetry_run_id,
    run_insider_reparse,
)
from insider_storage import (  # noqa: E402
    MAX_INSIDER_STATE_COLLECTION,
    InsiderApprovalScopeError,
    InsiderStateStore,
    InsiderStorage,
    InsiderStorageError,
)
from security_identity import normalize_section16_cik  # noqa: E402


_ACCESSION_RE = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")


class _ConfigurationError(ValueError):
    """Raised when an explicit offline reprocessing scope is invalid."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _ConfigurationError("command-line arguments are invalid") from None


@dataclass(frozen=True, slots=True)
class _Configuration:
    scope: str
    scope_identifier: str | None
    max_accessions: int | None
    deadline_seconds: int
    resume: bool


def _bounded_integer(value: object, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise _ConfigurationError(f"{label} is invalid")
    return value


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(description=__doc__)
    scopes = parser.add_mutually_exclusive_group(required=True)
    scopes.add_argument("--accession")
    scopes.add_argument("--issuer-cik")
    scopes.add_argument("--all", action="store_true", dest="all_scope")
    parser.add_argument("--max-accessions", type=int)
    parser.add_argument("--deadline-seconds", required=True, type=int)
    parser.add_argument("--resume", action="store_true")
    return parser


def _configuration(argv: Sequence[str] | None) -> _Configuration:
    if argv is not None and (
        isinstance(argv, (str, bytes))
        or any(type(argument) is not str for argument in argv)
    ):
        raise _ConfigurationError("command-line arguments are invalid")
    arguments = _parser().parse_args(None if argv is None else list(argv))

    if arguments.accession is not None:
        if (
            type(arguments.accession) is not str
            or _ACCESSION_RE.fullmatch(arguments.accession) is None
            or arguments.max_accessions is not None
        ):
            raise _ConfigurationError("accession scope is invalid")
        scope = "accession"
        scope_identifier = arguments.accession
        maximum = None
    elif arguments.issuer_cik is not None:
        if arguments.max_accessions is not None:
            raise _ConfigurationError("issuer scope is invalid")
        try:
            scope_identifier = normalize_section16_cik(arguments.issuer_cik)
        except (TypeError, ValueError) as error:
            raise _ConfigurationError("issuer scope is invalid") from error
        scope = "issuer"
        maximum = None
    else:
        if not arguments.all_scope or arguments.max_accessions is None:
            raise _ConfigurationError("all scope is invalid")
        scope = "all"
        scope_identifier = None
        maximum = _bounded_integer(
            arguments.max_accessions,
            "maximum accessions",
            MAX_INSIDER_STATE_COLLECTION,
        )

    return _Configuration(
        scope=scope,
        scope_identifier=scope_identifier,
        max_accessions=maximum,
        deadline_seconds=_bounded_integer(
            arguments.deadline_seconds,
            "deadline seconds",
            MAX_RECENT_INSIDER_DEADLINE_SECONDS,
        ),
        resume=arguments.resume,
    )


def _result_exit_code(result: InsiderReparseRunResult) -> int:
    """Map only a clean cooperative checkpoint to the dedicated CI status."""

    if not isinstance(result, InsiderReparseRunResult):
        raise TypeError("result must be an InsiderReparseRunResult")
    if result.outcome is InsiderReparseOutcome.COMPLETED:
        return 0
    if result.outcome is InsiderReparseOutcome.CHECKPOINTED and all(
        item.error_class is None and not item.retry
        for item in result.accession_results
    ):
        return 75
    return 1


@pipeline._serialize_pipeline_maintenance
def main(argv: Sequence[str] | None = None) -> int:
    """Validate one local scope and run its bounded offline reprocessing."""

    try:
        configuration = _configuration(argv)
        storage = InsiderStorage(ROOT)
        state_store = InsiderStateStore(ROOT)
        deadline = CooperativeDeadline(
            started_monotonic=time.monotonic(),
            deadline_seconds=configuration.deadline_seconds,
        )
    except (_ConfigurationError, InsiderStorageError, OSError, ValueError):
        pipeline.log.error("insider reprocessing configuration is invalid")
        return 2

    try:
        with insider_telemetry_run(
            state_store,
            run_id=new_insider_telemetry_run_id("reparse"),
        ):
            result = run_insider_reparse(
                scope=configuration.scope,
                scope_identifier=configuration.scope_identifier,
                max_accessions=configuration.max_accessions,
                storage=storage,
                state_store=state_store,
                deadline=deadline,
                resume=configuration.resume,
            )
    except (
        InsiderApprovalScopeError,
        InsiderReparseError,
        InsiderStorageError,
        OSError,
    ):
        pipeline.log.error("insider reprocessing failed before a durable outcome")
        return 1

    pipeline.log.info(
        "insider reprocessing %s: queued %s, completed %s",
        result.outcome.value,
        len(result.queued_accessions),
        len(result.completed_accessions),
    )
    return _result_exit_code(result)


if __name__ == "__main__":
    raise SystemExit(main())
