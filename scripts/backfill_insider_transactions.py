#!/usr/bin/env python3
"""Run one bounded, resumable quarterly Section 16 backfill."""

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
    MAX_INSIDER_BULK_SELECTED_ACCESSIONS,
    MAX_RECENT_INSIDER_DEADLINE_SECONDS,
    CooperativeDeadline,
    InsiderBackfillError,
    InsiderBackfillOutcome,
    insider_telemetry_run,
    new_insider_telemetry_run_id,
    run_insider_backfill,
)
from insider_storage import (  # noqa: E402
    InsiderStateStore,
    InsiderStorage,
    InsiderStorageError,
)
from security_identity import normalize_section16_cik  # noqa: E402


_QUARTER_RE = re.compile(r"[0-9]{4}Q[1-4]")


class _ConfigurationError(ValueError):
    """Raised when the explicit local backfill scope is invalid."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _ConfigurationError("command-line arguments are invalid") from None


@dataclass(frozen=True, slots=True)
class _Configuration:
    issuer_cik: str
    quarter: str
    max_accessions: int
    deadline_seconds: int
    plan_only: bool
    resume: bool


def _bounded_integer(value: object, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise _ConfigurationError(f"{label} is invalid")
    return value


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(description=__doc__)
    parser.add_argument("--issuer-cik", required=True)
    parser.add_argument("--quarter", required=True)
    parser.add_argument("--max-accessions", required=True, type=int)
    parser.add_argument("--deadline-seconds", required=True, type=int)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--plan-only", action="store_true")
    modes.add_argument("--resume", action="store_true")
    return parser


def _configuration(argv: Sequence[str] | None) -> _Configuration:
    if argv is not None and (
        isinstance(argv, (str, bytes))
        or any(type(argument) is not str for argument in argv)
    ):
        raise _ConfigurationError("command-line arguments are invalid")
    arguments = _parser().parse_args(None if argv is None else list(argv))
    try:
        issuer_cik = normalize_section16_cik(arguments.issuer_cik)
    except (TypeError, ValueError) as error:
        raise _ConfigurationError("issuer CIK is invalid") from error
    quarter = arguments.quarter
    if type(quarter) is not str or _QUARTER_RE.fullmatch(quarter) is None:
        raise _ConfigurationError("quarter is invalid")
    return _Configuration(
        issuer_cik=issuer_cik,
        quarter=quarter,
        max_accessions=_bounded_integer(
            arguments.max_accessions,
            "maximum accessions",
            MAX_INSIDER_BULK_SELECTED_ACCESSIONS,
        ),
        deadline_seconds=_bounded_integer(
            arguments.deadline_seconds,
            "deadline seconds",
            MAX_RECENT_INSIDER_DEADLINE_SECONDS,
        ),
        plan_only=arguments.plan_only,
        resume=arguments.resume,
    )


@pipeline._serialize_pipeline_maintenance
def main(argv: Sequence[str] | None = None) -> int:
    """Validate one explicit scope and run its cooperative backfill."""

    try:
        configuration = _configuration(argv)
        pipeline.require_declared_sec_user_agent()
        storage = InsiderStorage(ROOT)
        state_store = InsiderStateStore(ROOT)
        deadline = CooperativeDeadline(
            started_monotonic=time.monotonic(),
            deadline_seconds=configuration.deadline_seconds,
        )
    except (_ConfigurationError, InsiderStorageError, OSError, ValueError):
        pipeline.log.error("insider backfill configuration is invalid")
        return 2

    try:
        with insider_telemetry_run(
            state_store,
            run_id=new_insider_telemetry_run_id("backfill"),
        ):
            result = run_insider_backfill(
                issuer_cik=configuration.issuer_cik,
                quarter=configuration.quarter,
                max_accessions=configuration.max_accessions,
                deadline=deadline,
                storage=storage,
                state_store=state_store,
                plan_only=configuration.plan_only,
                resume=configuration.resume,
                http=pipeline.HTTP,
            )
    except (InsiderBackfillError, InsiderStorageError, OSError):
        pipeline.log.error("insider backfill failed before a durable outcome")
        return 1

    pipeline.log.info(
        "insider backfill %s for %s: selected %s, completed %s",
        result.outcome.value,
        result.quarter,
        len(result.selected_accessions),
        len(result.completed_accessions),
    )
    if result.outcome is InsiderBackfillOutcome.QUARANTINED:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
