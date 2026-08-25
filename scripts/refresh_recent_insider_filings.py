#!/usr/bin/env python3
"""Discover and durably checkpoint recent Section 16 filing accessions."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
import os
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
    MAX_RECENT_INSIDER_LOOKBACK_SECONDS,
    MAX_RECENT_INSIDER_PAGE_SIZE,
    MAX_RECENT_INSIDER_PAGES,
    CooperativeDeadline,
    InsiderAccessionOutcome,
    InsiderDiscoveryError,
    discover_recent_insider_accessions,
    insider_telemetry_run,
    new_insider_telemetry_run_id,
    pending_incremental_candidates,
    persist_incremental_discovery_queue,
    process_insider_accession,
    resolve_incremental_checkpoint_action,
    validate_incremental_checkpoint_scope,
    verify_insider_accession_cache,
)
from insider_parser import INSIDER_PARSER_VERSION  # noqa: E402
from insider_storage import (  # noqa: E402
    MAX_INSIDER_STATE_COLLECTION,
    InsiderStateStore,
    InsiderStorage,
    InsiderStorageError,
)
from security_identity import normalize_section16_cik  # noqa: E402


_DEFAULT_LOOKBACK_SECONDS = 3 * 24 * 60 * 60
_DEFAULT_MAX_PAGES = 20
_DEFAULT_PAGE_SIZE = 100
_DEFAULT_MAX_ACCESSIONS = 100
_DEFAULT_DEADLINE_SECONDS = 300
_INTEGER_RE = re.compile(r"[0-9]+")


class _ConfigurationError(ValueError):
    """Raised for invalid local discovery configuration."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _ConfigurationError("command-line arguments are invalid") from None


@dataclass(frozen=True, slots=True)
class _Configuration:
    issuer_ciks: tuple[str, ...]
    lookback_seconds: int
    max_pages: int
    page_size: int
    max_accessions: int
    deadline_seconds: int


def _bounded_integer(value: object, label: str, maximum: int) -> int:
    if type(value) is int:
        parsed = value
    elif type(value) is str and _INTEGER_RE.fullmatch(value):
        parsed = int(value)
    else:
        raise _ConfigurationError(f"{label} is invalid")
    if not 1 <= parsed <= maximum:
        raise _ConfigurationError(f"{label} is invalid")
    return parsed


def _configured_integer(
    cli_value: int | None,
    *,
    environment_name: str,
    default: int,
    label: str,
    maximum: int,
) -> int:
    value: object = cli_value
    if value is None:
        value = os.environ.get(environment_name, str(default))
    return _bounded_integer(value, label, maximum)


def _issuer_configuration(cli_values: list[str] | None) -> tuple[str, ...]:
    raw_values = cli_values
    if raw_values is None:
        raw = os.environ.get("RECENT_INSIDER_ISSUER_CIKS", "")
        raw_values = raw.split(",") if raw else []
    if not raw_values or any(not value for value in raw_values):
        raise _ConfigurationError("at least one issuer CIK is required")
    try:
        normalized = tuple(normalize_section16_cik(value) for value in raw_values)
    except (TypeError, ValueError) as error:
        raise _ConfigurationError("issuer CIK configuration is invalid") from error
    if (
        len(normalized) > MAX_INSIDER_STATE_COLLECTION
        or len(set(normalized)) != len(normalized)
    ):
        raise _ConfigurationError("issuer CIK configuration is invalid")
    return tuple(sorted(normalized))


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(description=__doc__)
    parser.add_argument("--issuer-cik", action="append", dest="issuer_ciks")
    parser.add_argument("--lookback-seconds", type=int)
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--page-size", type=int)
    parser.add_argument("--max-accessions", type=int)
    parser.add_argument("--deadline-seconds", type=int)
    return parser


def _configuration(argv: Sequence[str] | None) -> _Configuration:
    if argv is not None and (
        isinstance(argv, (str, bytes))
        or any(type(argument) is not str for argument in argv)
    ):
        raise _ConfigurationError("command-line arguments are invalid")
    arguments = _parser().parse_args(None if argv is None else list(argv))
    return _Configuration(
        issuer_ciks=_issuer_configuration(arguments.issuer_ciks),
        lookback_seconds=_configured_integer(
            arguments.lookback_seconds,
            environment_name="RECENT_INSIDER_LOOKBACK_SECONDS",
            default=_DEFAULT_LOOKBACK_SECONDS,
            label="lookback seconds",
            maximum=MAX_RECENT_INSIDER_LOOKBACK_SECONDS,
        ),
        max_pages=_configured_integer(
            arguments.max_pages,
            environment_name="RECENT_INSIDER_MAX_PAGES",
            default=_DEFAULT_MAX_PAGES,
            label="maximum pages",
            maximum=MAX_RECENT_INSIDER_PAGES,
        ),
        page_size=_configured_integer(
            arguments.page_size,
            environment_name="RECENT_INSIDER_PAGE_SIZE",
            default=_DEFAULT_PAGE_SIZE,
            label="page size",
            maximum=MAX_RECENT_INSIDER_PAGE_SIZE,
        ),
        max_accessions=_configured_integer(
            arguments.max_accessions,
            environment_name="RECENT_INSIDER_MAX_ACCESSIONS",
            default=_DEFAULT_MAX_ACCESSIONS,
            label="maximum accessions",
            maximum=MAX_INSIDER_STATE_COLLECTION,
        ),
        deadline_seconds=_configured_integer(
            arguments.deadline_seconds,
            environment_name="RECENT_INSIDER_DEADLINE_SECONDS",
            default=_DEFAULT_DEADLINE_SECONDS,
            label="deadline seconds",
            maximum=MAX_RECENT_INSIDER_DEADLINE_SECONDS,
        ),
    )


def _approved_issuer_subset(
    state_store: InsiderStateStore, requested: tuple[str, ...]
) -> tuple[str, ...]:
    approved_state = state_store.read("approved-issuers-v1")
    approved_values = approved_state["issuer_ciks"]
    if not isinstance(approved_values, list):
        raise _ConfigurationError("approved issuer state is invalid")
    approved = set(approved_values)
    if not set(requested) <= approved:
        raise _ConfigurationError("requested issuer CIK is not approved")
    return requested


@pipeline._serialize_pipeline_maintenance
def main(argv: Sequence[str] | None = None) -> int:
    """Validate configuration, discover safely, and checkpoint before processing."""

    try:
        configuration = _configuration(argv)
        state_store = InsiderStateStore(ROOT)
        storage = InsiderStorage(ROOT)
        approved = _approved_issuer_subset(state_store, configuration.issuer_ciks)
        existing_incremental: dict[str, object] | None = None
        incremental_action = "new"
        try:
            existing_incremental = state_store.read("incremental-v1")
        except FileNotFoundError:
            pass
        else:
            incremental_action = resolve_incremental_checkpoint_action(
                existing_incremental,
                issuer_ciks=approved,
                max_accessions=configuration.max_accessions,
            )
        pipeline.require_declared_sec_user_agent()
        deadline = CooperativeDeadline(
            started_monotonic=time.monotonic(),
            deadline_seconds=configuration.deadline_seconds,
        )
    except (
        _ConfigurationError,
        FileNotFoundError,
        InsiderDiscoveryError,
        InsiderStorageError,
        ValueError,
    ):
        pipeline.log.error("recent insider discovery configuration is invalid")
        return 2

    try:
        with insider_telemetry_run(
            state_store,
            run_id=new_insider_telemetry_run_id("incremental"),
        ):
            quarantined_count = 0
            if incremental_action == "resume":
                assert existing_incremental is not None
                persisted = existing_incremental
            else:
                result = discover_recent_insider_accessions(
                    approved_issuer_ciks=approved,
                    lookback_seconds=configuration.lookback_seconds,
                    max_pages=configuration.max_pages,
                    page_size=configuration.page_size,
                    max_accessions=configuration.max_accessions,
                    deadline_seconds=configuration.deadline_seconds,
                    deadline_monotonic=deadline.deadline_monotonic,
                    http=pipeline.HTTP,
                )
                persisted = persist_incremental_discovery_queue(
                    state_store,
                    result=result,
                    lookback_seconds=configuration.lookback_seconds,
                    completed_artifact_verifier=lambda candidate: (
                        verify_insider_accession_cache(
                            candidate,
                            storage=storage,
                            parser_version=INSIDER_PARSER_VERSION,
                            approved_issuer_ciks=approved,
                        )
                        is not None
                    ),
                )
                quarantined_count = len(result.quarantined_accessions)
            processor_results = []
            for candidate in pending_incremental_candidates(persisted):
                if deadline.reached(time.monotonic):
                    break
                processor_result = process_insider_accession(
                    candidate,
                    storage=storage,
                    state_store=state_store,
                    approved_issuer_ciks=approved,
                    parser_version=INSIDER_PARSER_VERSION,
                    http=pipeline.HTTP,
                    deadline=deadline,
                )
                processor_results.append(processor_result)
                if (
                    processor_result.outcome
                    == InsiderAccessionOutcome.CHECKPOINTED
                    and processor_result.reason_code == "deadline"
                ):
                    break
    except (InsiderDiscoveryError, InsiderStorageError, OSError):
        pipeline.log.error("recent insider discovery failed closed")
        return 1

    blocking_failures = [
        processor_result
        for processor_result in processor_results
        if processor_result.outcome is InsiderAccessionOutcome.QUARANTINED
        or (
            processor_result.outcome == InsiderAccessionOutcome.CHECKPOINTED
            and processor_result.reason_code == "checkpoint_failed"
        )
    ]
    if blocking_failures:
        pipeline.log.error(
            "recent insider processing completed with %s failed accession(s)",
            len(blocking_failures),
        )
        return 1

    try:
        checkpoint = state_store.read("incremental-v1")
        validate_incremental_checkpoint_scope(
            checkpoint,
            issuer_ciks=approved,
            max_accessions=configuration.max_accessions,
        )
    except (FileNotFoundError, InsiderDiscoveryError, InsiderStorageError, ValueError):
        pipeline.log.error("recent insider checkpoint failed validation")
        return 1

    queue = checkpoint["queue"]
    queue_count = len(queue) if isinstance(queue, list) else 0
    pipeline.log.info(
        "recent insider discovery checkpointed %s accession(s), processed %s; "
        "%s quarantined group(s)",
        queue_count,
        len(processor_results),
        quarantined_count,
    )
    if checkpoint["status"] in {"running", "incomplete"}:
        pipeline.log.warning(
            "recent insider processing reached a durable cooperative checkpoint"
        )
        return 75
    if checkpoint["status"] != "completed":
        pipeline.log.error("recent insider checkpoint failed validation")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
