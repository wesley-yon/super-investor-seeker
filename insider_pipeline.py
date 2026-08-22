"""Public offline insider-ingestion and bounded discovery API."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from contextlib import contextmanager
from contextvars import ContextVar
import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime, parsedate_to_datetime
from enum import StrEnum
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import stat
import tempfile
import time
from typing import TypeVar
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit
from uuid import uuid4
import zipfile

from lxml import etree
import requests

import pipeline

from insider_contract import (
    InsiderContractError,
    canonical_insider_json_bytes,
    validate_insider_filing,
)
from insider_parser import (
    INSIDER_PARSER_VERSION,
    InsiderParseError,
    parse_ownership_xml,
)

from insider_source import (
    INSIDER_SOURCE_METADATA_VERSION,
    MAX_INDEX_FIELD_CHARS,
    MAX_INDEX_HTML_BYTES,
    MAX_INDEX_HTML_ELEMENTS,
    MAX_INDEX_TABLE_ROWS,
    InsiderIndexParseError,
    build_insider_source_metadata,
    canonical_source_metadata_json_bytes,
    parse_insider_filing_index,
    validate_insider_source_metadata,
)
from insider_storage import (
    APPROVED_ISSUERS_STATE_CONTRACT_VERSION,
    BACKFILL_STATE_CONTRACT_VERSION,
    INCREMENTAL_STATE_CONTRACT_VERSION,
    ISSUER_STATE_CONTRACT_VERSION,
    MAX_INSIDER_STATE_BYTES,
    MAX_INSIDER_STATE_COLLECTION,
    MAX_INSIDER_STATE_INTEGER,
    MAX_INSIDER_STATE_STRING_CHARS,
    MAX_TELEMETRY_ACCESSION_EXAMPLES,
    MAX_TELEMETRY_RECENT_RUNS,
    QUARANTINE_STATE_CONTRACT_VERSION,
    REPARSE_STATE_CONTRACT_VERSION,
    TELEMETRY_STATE_CONTRACT_VERSION,
    PRIVATE_INSIDER_STATE_ROOT,
    MAX_RAW_XML_BYTES,
    ImmutableInsiderStorageConflict,
    InsiderApprovalScopeError,
    InsiderStateRevisionError,
    InsiderStateStore,
    InsiderStorage,
    InsiderStorageError,
    canonical_insider_state_json_bytes,
    issuer_generation_digest,
    validate_incremental_state_payload,
)
from security_identity import (
    normalize_section16_cik,
    section16_owner_group_key,
    section16_security_class_key,
)


SECTION16_CURRENT_FORMS: tuple[str, ...] = ("3", "3/A", "4", "4/A", "5", "5/A")
CURRENT_FILINGS_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
MAX_RECENT_INSIDER_ATOM_BYTES = 1_000_000
MAX_RECENT_INSIDER_ATOM_ENTRIES = 1_000
MAX_RECENT_INSIDER_ATOM_ELEMENTS = 20_000
MAX_RECENT_INSIDER_ATOM_FIELD_CHARS = 4_096
MAX_RECENT_INSIDER_GROUPS = 5_000
MAX_RECENT_INSIDER_DISCOVERY_ENTRIES = 10_000
MAX_RECENT_INSIDER_LOOKBACK_SECONDS = 31 * 24 * 60 * 60
MAX_RECENT_INSIDER_PAGES = 100
MAX_RECENT_INSIDER_PAGE_SIZE = 100
MAX_RECENT_INSIDER_DEADLINE_SECONDS = 3_600

INSIDER_BULK_CATALOG_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "insider-transactions-data-sets"
)
MIN_INSIDER_BULK_QUARTER = "2006Q1"
MAX_INSIDER_BULK_CATALOG_BYTES = 2_000_000
MAX_INSIDER_BULK_CATALOG_LINKS = 10_000
MAX_INSIDER_BULK_CATALOG_ELEMENTS = 50_000
MAX_INSIDER_BULK_ARCHIVE_BYTES = 1_000_000_000
MAX_INSIDER_BULK_ZIP_MEMBERS = 16
MAX_INSIDER_BULK_COMPRESSED_BYTES = 1_000_000_000
MAX_INSIDER_BULK_UNCOMPRESSED_BYTES = 2_000_000_000
MAX_INSIDER_BULK_COMPRESSION_RATIO = 250
MAX_INSIDER_BULK_TSV_COLUMNS = 256
MAX_INSIDER_BULK_TSV_FIELD_CHARS = 131_072
MAX_INSIDER_BULK_TSV_RECORD_CHARS = 4_000_000
MAX_INSIDER_BULK_TSV_ROWS = 5_000_000
MAX_INSIDER_BULK_SELECTED_ACCESSIONS = MAX_INSIDER_STATE_COLLECTION

_INSIDER_BULK_TABLES = frozenset({
    "SUBMISSION",
    "REPORTINGOWNER",
    "NONDERIV_TRANS",
    "NONDERIV_HOLDING",
    "DERIV_TRANS",
    "DERIV_HOLDING",
    "FOOTNOTES",
    "OWNER_SIGNATURE",
})
_INSIDER_BULK_AUXILIARY_MEMBERS = frozenset({
    "FORM_345_metadata.json",
    "FORM_345_readme.htm",
})
_INSIDER_BULK_OPTIONAL_TABLES = _INSIDER_BULK_TABLES - {"SUBMISSION"}
_INSIDER_BULK_QUARTER_RE = re.compile(r"(?P<year>[0-9]{4})Q(?P<quarter>[1-4])")
_INSIDER_BULK_HEADER_RE = re.compile(r"[A-Z][A-Z0-9_]{0,127}")
_INSIDER_BULK_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_INSIDER_BULK_FILING_DATE_RE = re.compile(
    r"(?P<day>0[1-9]|[12][0-9]|3[01])-"
    r"(?P<month>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-"
    r"(?P<year>[0-9]{4})"
)
_INSIDER_BULK_CANONICAL_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_INSIDER_BULK_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_INSIDER_BULK_ETAG_RE = re.compile(
    r'(?:W/)?"[\x20-\x21\x23-\x7e\x80-\xff]*"'
)

_INSIDER_TELEMETRY_COUNTERS = frozenset({
    "discovery_attempts",
    "discovery_entries",
    "discovered_accession_groups",
    "index_fetches",
    "index_cache_hits",
    "raw_fetches",
    "raw_cache_hits",
    "parse_attempts",
    "parse_successes",
    "parse_failures",
    "reporting_owner_rows",
    "non_derivative_rows",
    "derivative_rows",
    "non_derivative_transaction_rows",
    "non_derivative_holding_rows",
    "derivative_transaction_rows",
    "derivative_holding_rows",
    "footnote_rows",
    "owner_signature_rows",
    "unknown_codes",
    "unknown_elements",
    "parse_warnings",
    "unmapped_security_titles",
    "amendments",
    "amendments_resolved",
    "amendments_unresolved",
    "http_attempts",
    "http_status_2xx",
    "http_status_4xx",
    "http_status_5xx",
    "http_latency_ms",
    "limiter_wait_ms",
    "limiter_utilization",
    "backfill_source_quarters",
    "backfill_source_hashes",
    "backfill_tables",
    "backfill_table_evidence",
    "backfill_reconciliations",
    "checkpoint_writes",
    "checkpoint_failures",
    "reparse_attempts",
    "reparse_completed",
    "reparse_failures",
})
_INSIDER_TELEMETRY_RUN_ID_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,127}")
_INSIDER_TELEMETRY_ERROR_CLASSES = frozenset({
    "InsiderIndexParseError",
    "InsiderContractError",
    "InsiderIssuerReductionError",
    "InsiderParseError",
    "InsiderStorageError",
    "TimeoutError",
    "ConnectionError",
    "HTTPError",
})
_INSIDER_TELEMETRY_STAGES = frozenset({
    "discovery",
    "cache",
    "index",
    "raw",
    "parse",
    "source",
    "archive",
    "normalized",
    "issuer",
    "checkpoint",
    "backfill",
    "reparse",
    "telemetry",
})
_INSIDER_TELEMETRY_REASON_MAP = {
    "discovery_invalid": "discovery_invalid",
    "cache_invalid": "cache_invalid",
    "index_cache_invalid": "cache_invalid",
    "index_invalid": "index_parse_invalid",
    "index_parse_invalid": "index_parse_invalid",
    "raw_cache_invalid": "cache_invalid",
    "raw_invalid": "raw_invalid",
    "raw_parse_invalid": "raw_invalid",
    "source_invalid": "source_invalid",
    "archive_invalid": "archive_invalid",
    "backfill_invalid": "backfill_invalid",
    "reparse_invalid": "reparse_invalid",
    "issuer_invalid": "issuer_invalid",
    "checkpoint_failed": "checkpoint_invalid",
    "checkpoint_invalid": "checkpoint_invalid",
}
_INSIDER_TELEMETRY_MAX_RETRY_SECONDS = 3_600


def _canonical_telemetry_timestamp(value: object, label: str) -> tuple[str, datetime]:
    if type(value) is not datetime or value.tzinfo is None:
        raise InsiderStorageError(f"telemetry {label} is invalid")
    try:
        offset = value.utcoffset()
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        raise InsiderStorageError(f"telemetry {label} is invalid") from None
    if offset is None:
        raise InsiderStorageError(f"telemetry {label} is invalid")
    canonical = value.astimezone(timezone.utc).replace(microsecond=0)
    return canonical.strftime("%Y-%m-%dT%H:%M:%SZ"), canonical


def _telemetry_timestamp_sort_key(value: object) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise InsiderStorageError("telemetry run timestamp is invalid")
    try:
        instant = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as error:
        raise InsiderStorageError("telemetry run timestamp is invalid") from error
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise InsiderStorageError("telemetry run timestamp is invalid")
    return instant.astimezone(timezone.utc)


def _empty_telemetry_state() -> dict[str, object]:
    return {
        "contract_version": TELEMETRY_STATE_CONTRACT_VERSION,
        "counters": {},
        "recent_runs": [],
    }


def _mutate_telemetry_state(
    state_store: InsiderStateStore,
    transform: Callable[[dict[str, object]], object],
) -> dict[str, object]:
    try:
        return state_store.update("telemetry-v1", transform)
    except FileNotFoundError:
        candidate = transform(_empty_telemetry_state())
        try:
            state_store.write("telemetry-v1", candidate)
        except InsiderStateRevisionError:
            return state_store.update("telemetry-v1", transform)
        return state_store.read("telemetry-v1")


def _bounded_telemetry_runs(
    runs: list[dict[str, object]],
) -> list[dict[str, object]]:
    ordered = sorted(
        runs,
        key=lambda run: (
            _telemetry_timestamp_sort_key(run.get("started_at")),
            run.get("run_id"),
        ),
    )
    return ordered[-MAX_TELEMETRY_RECENT_RUNS:]


def _safe_telemetry_error_class(value: object) -> str:
    if type(value) is str and value in _INSIDER_TELEMETRY_ERROR_CLASSES:
        return value
    return "InsiderStorageError"


def _telemetry_transient_reason(error_class: str) -> str:
    if error_class == "ConnectionError":
        return "connection_failed"
    if error_class == "TimeoutError":
        return "timeout"
    if error_class == "HTTPError":
        return "http_error"
    return "raw_fetch_failed"


@dataclass(slots=True)
class InsiderTelemetryRun:
    """One bounded, source-free telemetry accumulator for an ingestion run."""

    state_store: InsiderStateStore
    run_id: str
    started_at: str
    started_instant: datetime
    now: Callable[[], datetime]
    counters: dict[str, int] = field(default_factory=dict)
    _examples: dict[str, dict[str, object]] = field(default_factory=dict)

    def increment(self, counter: str, amount: int = 1) -> None:
        if (
            type(counter) is not str
            or counter not in _INSIDER_TELEMETRY_COUNTERS
            or type(amount) is not int
            or amount < 0
        ):
            raise InsiderStorageError("telemetry counter update is invalid")
        current = self.counters.get(counter, 0)
        if current > MAX_INSIDER_STATE_INTEGER - amount:
            raise InsiderStorageError("telemetry counter limit is exceeded")
        self.counters[counter] = current + amount

    @staticmethod
    def _milliseconds(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise InsiderStorageError("telemetry duration is invalid")
        numeric = float(value)
        if not math.isfinite(numeric) or numeric < 0:
            raise InsiderStorageError("telemetry duration is invalid")
        return min(MAX_INSIDER_STATE_INTEGER, int(round(numeric * 1_000)))

    def observe_http_event(self, event: object) -> None:
        if not isinstance(event, dict) or set(event) != {
            "attempt",
            "status",
            "latency",
            "sleep",
            "limiter_wait",
        }:
            raise InsiderStorageError("telemetry HTTP event is invalid")
        attempt = event["attempt"]
        status = event["status"]
        if type(attempt) is not int or attempt < 1:
            raise InsiderStorageError("telemetry HTTP attempt is invalid")
        if status is not None and (type(status) is not int or not 100 <= status <= 599):
            raise InsiderStorageError("telemetry HTTP status is invalid")
        latency_ms = self._milliseconds(event["latency"])
        limiter_wait_ms = self._milliseconds(event["limiter_wait"])
        self._milliseconds(event["sleep"])
        self.increment("http_attempts")
        if status is not None:
            if 200 <= status <= 299:
                self.increment("http_status_2xx")
            elif 400 <= status <= 499:
                self.increment("http_status_4xx")
            elif 500 <= status <= 599:
                self.increment("http_status_5xx")
        self.increment("http_latency_ms", latency_ms)
        self.increment("limiter_wait_ms", limiter_wait_ms)
        if limiter_wait_ms:
            self.increment("limiter_utilization")

    def observe_normalized(self, normalized: object) -> None:
        if not isinstance(normalized, dict):
            raise InsiderStorageError("telemetry normalized filing is invalid")
        owners = normalized.get("owners")
        transactions = normalized.get("transactions")
        holdings = normalized.get("holdings")
        footnotes = normalized.get("footnotes")
        signatures = normalized.get("signatures")
        warnings = normalized.get("warnings")
        unknown_elements = normalized.get("unknown_elements")
        collections = (
            owners,
            transactions,
            holdings,
            footnotes,
            signatures,
            warnings,
            unknown_elements,
        )
        if any(not isinstance(collection, list) for collection in collections):
            raise InsiderStorageError("telemetry normalized filing is invalid")
        assert isinstance(owners, list)
        assert isinstance(transactions, list)
        assert isinstance(holdings, list)
        assert isinstance(footnotes, list)
        assert isinstance(signatures, list)
        assert isinstance(warnings, list)
        assert isinstance(unknown_elements, list)
        self.increment("reporting_owner_rows", len(owners))
        self.increment("footnote_rows", len(footnotes))
        self.increment("owner_signature_rows", len(signatures))
        self.increment("unknown_elements", len(unknown_elements))
        self.increment("parse_warnings", len(warnings))
        self.increment(
            "unknown_codes",
            sum(
                isinstance(warning, dict)
                and warning.get("code")
                in {"unknown_transaction_code", "unknown_control_code"}
                for warning in warnings
            ),
        )
        unmapped_classes: set[str] = set()
        for collection_name, collection in (
            ("transaction", transactions),
            ("holding", holdings),
        ):
            for row in collection:
                if not isinstance(row, dict):
                    raise InsiderStorageError("telemetry normalized row is invalid")
                source_table = row.get("source_table")
                if source_table not in {"non_derivative", "derivative"}:
                    raise InsiderStorageError("telemetry normalized row is invalid")
                derivative = source_table == "derivative"
                self.increment(
                    "derivative_rows" if derivative else "non_derivative_rows"
                )
                self.increment(
                    f"{'derivative' if derivative else 'non_derivative'}_{collection_name}_rows"
                )
                if row.get("normalized_security_id") is None:
                    class_key = row.get("security_class_key")
                    if type(class_key) is str:
                        unmapped_classes.add(class_key)
        self.increment("unmapped_security_titles", len(unmapped_classes))
        if normalized.get("is_amendment") is True:
            self.increment("amendments")

    def observe_reduction(self, reduction: IssuerReductionResult) -> None:
        if not isinstance(reduction, IssuerReductionResult):
            raise InsiderStorageError("telemetry issuer reduction is invalid")
        self.increment("amendments_resolved", reduction.amendments_resolved)
        self.increment("amendments_unresolved", reduction.amendments_unresolved)

    def _store_example(
        self,
        *,
        accession_number: str,
        issuer_cik: str | None,
        form_type: str | None,
        parser_version: str | None,
        stage: str,
        outcome: str,
        error_class: str | None,
        reason_code: str | None,
    ) -> None:
        if stage not in _INSIDER_TELEMETRY_STAGES:
            raise InsiderStorageError("telemetry example stage is invalid")
        retry_count = 0
        next_retry_at: str | None = None
        if outcome == "retry_later":
            previous = self._examples.get(accession_number)
            previous_retry_count = 0
            if previous is not None:
                value = previous.get("retry_count")
                if type(value) is int:
                    previous_retry_count = value
            retry_count = min(MAX_INSIDER_STATE_INTEGER, previous_retry_count + 1)
            delay = min(
                _INSIDER_TELEMETRY_MAX_RETRY_SECONDS,
                30 * (2 ** min(retry_count - 1, 7)),
            )
            now_value, now_instant = _canonical_telemetry_timestamp(
                self.now(), "retry timestamp"
            )
            del now_value
            next_retry_at = (now_instant + timedelta(seconds=delay)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        example = {
            "accession_number": accession_number,
            "issuer_cik": issuer_cik,
            "form_type": form_type,
            "parser_version": parser_version,
            "stage": stage,
            "outcome": outcome,
            "error_class": error_class,
            "reason_code": reason_code,
            "retry_count": retry_count,
            "next_retry_at": next_retry_at,
        }
        if accession_number not in self._examples and (
            len(self._examples) >= MAX_TELEMETRY_ACCESSION_EXAMPLES
        ):
            oldest = next(iter(self._examples))
            del self._examples[oldest]
        self._examples[accession_number] = example

    def observe_processor_result(self, result: InsiderAccessionProcessResult) -> None:
        if not isinstance(result, InsiderAccessionProcessResult):
            raise InsiderStorageError("telemetry processor result is invalid")
        outcome = result.outcome.value
        error_class: str | None = None
        reason_code: str | None = None
        if outcome in {"quarantined", "retry_later"}:
            error_class = _safe_telemetry_error_class(result.error_class)
            if outcome == "retry_later":
                reason_code = _telemetry_transient_reason(error_class)
            else:
                reason_code = (
                    _INSIDER_TELEMETRY_REASON_MAP.get(result.reason_code)
                    if result.reason_code is not None
                    else None
                ) or "telemetry_invalid"
        self._store_example(
            accession_number=result.accession_number,
            issuer_cik=result.issuer_cik,
            form_type=result.form_type,
            parser_version=result.parser_version,
            stage=result.stage,
            outcome=outcome,
            error_class=error_class,
            reason_code=reason_code,
        )

    def observe_reparse_result(self, result: InsiderReparseAccessionResult) -> None:
        if not isinstance(result, InsiderReparseAccessionResult):
            raise InsiderStorageError("telemetry reparse result is invalid")
        self.increment("reparse_attempts")
        if result.outcome in {
            InsiderAccessionOutcome.CREATED,
            InsiderAccessionOutcome.CACHE_HIT,
        }:
            self.increment("reparse_completed")
            outcome = result.outcome.value
            error_class = None
            reason_code = None
        elif result.retry:
            self.increment("reparse_failures")
            outcome = "retry_later"
            error_class = _safe_telemetry_error_class(result.error_class)
            reason_code = _telemetry_transient_reason(error_class)
        elif result.outcome is InsiderAccessionOutcome.QUARANTINED:
            self.increment("reparse_failures")
            outcome = "quarantined"
            error_class = _safe_telemetry_error_class(result.error_class)
            reason_code = "reparse_invalid"
        else:
            outcome = "checkpointed"
            error_class = None
            reason_code = None
        self._store_example(
            accession_number=result.accession_number,
            issuer_cik=result.issuer_cik,
            form_type=result.form_type,
            parser_version=result.parser_version,
            stage=result.stage,
            outcome=outcome,
            error_class=error_class,
            reason_code=reason_code,
        )

    def observe_backfill_archive(self, archive: InsiderBulkArchiveResult) -> None:
        _validate_bulk_result(archive)
        self.increment("backfill_source_quarters")
        self.increment("backfill_source_hashes")
        self.increment("backfill_tables", len(archive.table_evidence))
        self.increment("backfill_table_evidence", len(archive.table_evidence))

    def observe_backfill_reconciliations(self, count: int) -> None:
        self.increment("backfill_reconciliations", count)

    def recent_examples(self) -> list[dict[str, object]]:
        return [dict(self._examples[key]) for key in sorted(self._examples)]

    def start(self) -> None:
        run = {
            "run_id": self.run_id,
            "status": "running",
            "started_at": self.started_at,
            "finished_at": None,
            "counters": {},
            "accession_examples": [],
        }

        def add_run(current: dict[str, object]) -> dict[str, object]:
            counters = current.get("counters")
            recent = current.get("recent_runs")
            if not isinstance(counters, dict) or not isinstance(recent, list):
                raise InsiderStorageError("telemetry state is invalid")
            if any(
                isinstance(entry, dict) and entry.get("run_id") == self.run_id
                for entry in recent
            ):
                raise InsiderStorageError("telemetry run ID already exists")
            runs = [dict(entry) for entry in recent if isinstance(entry, dict)]
            if len(runs) != len(recent):
                raise InsiderStorageError("telemetry recent runs are invalid")
            runs.append(run)
            return {
                "contract_version": TELEMETRY_STATE_CONTRACT_VERSION,
                "counters": dict(counters),
                "recent_runs": _bounded_telemetry_runs(runs),
            }

        _mutate_telemetry_state(self.state_store, add_run)

    def finish(self, status: str) -> None:
        if status not in {"completed", "failed", "cancelled"}:
            raise InsiderStorageError("telemetry run status is invalid")
        finished_at, finished_instant = _canonical_telemetry_timestamp(
            self.now(), "finish timestamp"
        )
        if finished_instant < self.started_instant:
            raise InsiderStorageError("telemetry run timestamps are invalid")

        def complete_run(current: dict[str, object]) -> dict[str, object]:
            aggregate = current.get("counters")
            recent = current.get("recent_runs")
            if not isinstance(aggregate, dict) or not isinstance(recent, list):
                raise InsiderStorageError("telemetry state is invalid")
            next_aggregate = dict(aggregate)
            for key, amount in self.counters.items():
                prior = next_aggregate.get(key, 0)
                if type(prior) is not int or prior > MAX_INSIDER_STATE_INTEGER - amount:
                    raise InsiderStorageError("telemetry counter limit is exceeded")
                next_aggregate[key] = prior + amount
            updated = False
            runs: list[dict[str, object]] = []
            for entry in recent:
                if not isinstance(entry, dict):
                    raise InsiderStorageError("telemetry recent runs are invalid")
                candidate = dict(entry)
                if candidate.get("run_id") == self.run_id:
                    if candidate.get("status") != "running":
                        raise InsiderStorageError("telemetry run is not active")
                    candidate = {
                        "run_id": self.run_id,
                        "status": status,
                        "started_at": self.started_at,
                        "finished_at": finished_at,
                        "counters": dict(self.counters),
                        "accession_examples": self.recent_examples(),
                    }
                    updated = True
                runs.append(candidate)
            if not updated:
                raise InsiderStorageError("telemetry run is missing")
            return {
                "contract_version": TELEMETRY_STATE_CONTRACT_VERSION,
                "counters": next_aggregate,
                "recent_runs": _bounded_telemetry_runs(runs),
            }

        _mutate_telemetry_state(self.state_store, complete_run)


_ACTIVE_INSIDER_TELEMETRY: ContextVar[InsiderTelemetryRun | None] = ContextVar(
    "active_insider_telemetry",
    default=None,
)


def _active_insider_telemetry() -> InsiderTelemetryRun | None:
    return _ACTIVE_INSIDER_TELEMETRY.get()


def new_insider_telemetry_run_id(prefix: object) -> str:
    """Create one opaque source-free run identifier for a production entry point."""

    if (
        type(prefix) is not str
        or not prefix
        or len(prefix) > 64
        or re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]*", prefix) is None
    ):
        raise InsiderStorageError("telemetry run prefix is invalid")
    run_id = f"{prefix}-{uuid4().hex}"
    if _INSIDER_TELEMETRY_RUN_ID_RE.fullmatch(run_id) is None:
        raise InsiderStorageError("telemetry run ID is invalid")
    return run_id


@contextmanager
def insider_telemetry_run(
    state_store: InsiderStateStore,
    *,
    run_id: str,
    started_at: datetime | None = None,
    now: Callable[[], datetime] | None = None,
):
    """Persist one privacy-safe aggregate run and observe shared SEC requests."""

    if not isinstance(state_store, InsiderStateStore):
        raise TypeError("state store must be an InsiderStateStore")
    if type(run_id) is not str or _INSIDER_TELEMETRY_RUN_ID_RE.fullmatch(run_id) is None:
        raise InsiderStorageError("telemetry run ID is invalid")
    if _ACTIVE_INSIDER_TELEMETRY.get() is not None:
        raise InsiderStorageError("nested telemetry runs are invalid")
    clock = (lambda: datetime.now(timezone.utc)) if now is None else now
    if not callable(clock):
        raise TypeError("telemetry clock must be callable")
    start_value = clock() if started_at is None else started_at
    canonical_start, start_instant = _canonical_telemetry_timestamp(
        start_value,
        "start timestamp",
    )
    recorder = InsiderTelemetryRun(
        state_store=state_store,
        run_id=run_id,
        started_at=canonical_start,
        started_instant=start_instant,
        now=clock,
    )
    recorder.start()
    token = _ACTIVE_INSIDER_TELEMETRY.set(recorder)
    try:
        with pipeline.observe_sec_request_events(recorder.observe_http_event):
            try:
                yield recorder
            except BaseException as error:
                try:
                    recorder.finish(
                        "cancelled" if pipeline.is_control_flow_exception(error) else "failed"
                    )
                except Exception:  # noqa: BLE001 - preserve the primary failure
                    pass
                raise
            else:
                recorder.finish("completed")
    finally:
        _ACTIVE_INSIDER_TELEMETRY.reset(token)


_ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
_ATOM_ENTRY = f"{{{_ATOM_NAMESPACE}}}entry"
_ATOM_TITLE = f"{{{_ATOM_NAMESPACE}}}title"
_ATOM_UPDATED = f"{{{_ATOM_NAMESPACE}}}updated"
_ATOM_LINK = f"{{{_ATOM_NAMESPACE}}}link"
_DISCOVERY_TITLE_RE = re.compile(
    r"(?P<form>[345](?:/A)?) - (?P<name>.+) \((?P<cik>[0-9]{10})\) "
    r"\((?P<role>Issuer|Reporting)\)"
)
_DISCOVERY_ACCESSION_RE = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_DISCOVERY_COMPACT_ACCESSION_RE = re.compile(r"[0-9]{18}")
_DISCOVERY_REPORTING_FILENAME_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}\.html?"
)
_DiscoveryValue = TypeVar("_DiscoveryValue")
_BackfillValue = TypeVar("_BackfillValue")


class InsiderDiscoveryError(ValueError):
    """Raised when current-filings discovery cannot fail closed."""


@dataclass(frozen=True, slots=True)
class RecentInsiderFeedEntry:
    accession_number: str
    form_type: str
    entity_role: str
    entity_cik: str
    entry_url: str
    accepted_at: str
    observed_at: str


@dataclass(frozen=True, slots=True)
class DiscoveredInsiderAccession:
    accession_number: str
    issuer_cik: str
    form_type: str
    index_url: str
    accepted_at: str
    observed_at: str
    reporting_entry_count: int
    source_entries: tuple[RecentInsiderFeedEntry, ...]


@dataclass(frozen=True, slots=True)
class InsiderAccessionIdentity:
    """Source-neutral filing identity used by the authoritative processor."""

    accession_number: str
    issuer_cik: str
    form_type: str
    index_url: str
    accepted_at: str
    reporting_owner_ciks: tuple[str, ...]

    def __post_init__(self) -> None:
        _validate_insider_accession_identity(self)


@dataclass(frozen=True, slots=True)
class IncrementalDiscoveryResult:
    accessions: tuple[DiscoveredInsiderAccession, ...]
    quarantined_accessions: tuple[str, ...]
    pages_fetched: int = 0
    deadline_reached: bool = False


class InsiderBackfillError(ValueError):
    """Raised when a quarterly insider source cannot be consumed safely."""


class InsiderBulkSourceRevisionError(InsiderBackfillError):
    """Raised when completed quarterly source identity changes."""


@dataclass(frozen=True, slots=True)
class InsiderBulkCatalogEntry:
    source_quarter: str
    catalog_url: str
    zip_url: str

    def __post_init__(self) -> None:
        _run_backfill_boundary(
            lambda: _validate_bulk_catalog_entry(self),
            fallback_label="catalog entry",
        )


@dataclass(frozen=True, slots=True)
class InsiderBulkSourceIdentity:
    source_quarter: str
    zip_url: str
    zip_sha256: str

    def __post_init__(self) -> None:
        _run_backfill_boundary(
            lambda: _validate_bulk_source_identity(self),
            fallback_label="source identity",
        )


@dataclass(frozen=True, slots=True)
class InsiderBulkTableEvidence:
    table_name: str
    headers: tuple[str, ...]
    row_count: int
    selected_row_count: int

    def __post_init__(self) -> None:
        _run_backfill_boundary(
            lambda: _validate_bulk_table_evidence(self),
            fallback_label="table evidence",
        )


@dataclass(frozen=True, slots=True)
class InsiderBulkAccessionEvidence:
    accession_number: str
    issuer_cik: str
    form_type: str
    filing_date: str
    reporting_owner_ciks: tuple[str, ...]
    table_row_counts: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        _run_backfill_boundary(
            lambda: _validate_bulk_accession_evidence(self),
            fallback_label="selected accession",
        )


@dataclass(frozen=True, slots=True)
class InsiderBulkArchiveResult:
    source_quarter: str
    catalog_url: str
    zip_url: str
    zip_sha256: str
    zip_byte_count: int
    etag: str | None
    last_modified: str | None
    table_evidence: tuple[InsiderBulkTableEvidence, ...]
    missing_optional_tables: tuple[str, ...]
    selected_accessions: tuple[InsiderBulkAccessionEvidence, ...]

    def __post_init__(self) -> None:
        _run_backfill_boundary(
            lambda: _validate_bulk_result(self),
            fallback_label="archive result",
        )


class InsiderBackfillOutcome(StrEnum):
    """Durable outcome of one explicitly bounded quarterly backfill run."""

    PLANNED = "planned"
    CHECKPOINTED = "checkpointed"
    COMPLETED = "completed"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class InsiderBackfillRunResult:
    """Bounded source identity and progress returned by one backfill run."""

    quarter: str
    issuer_cik: str
    outcome: InsiderBackfillOutcome
    selected_accessions: tuple[str, ...]
    completed_accessions: tuple[str, ...]
    catalog_url: str
    zip_url: str
    zip_sha256: str

    def __post_init__(self) -> None:
        _run_backfill_boundary(
            lambda: _validate_backfill_run_result(self),
            fallback_label="run result",
        )


def _backfill_error(label: str) -> InsiderBackfillError:
    return InsiderBackfillError(f"insider backfill source is invalid: {label}")


def _validate_backfill_run_result(
    result: object,
) -> InsiderBackfillRunResult:
    if type(result) is not InsiderBackfillRunResult:
        raise _backfill_error("run result")
    _validate_bulk_quarter(result.quarter)
    if type(result.issuer_cik) is not str:
        raise _backfill_error("run issuer CIK")
    try:
        issuer_cik = normalize_section16_cik(result.issuer_cik)
    except (TypeError, ValueError) as error:
        raise _backfill_error("run issuer CIK") from error
    if issuer_cik != result.issuer_cik or type(result.outcome) is not InsiderBackfillOutcome:
        raise _backfill_error("run result")
    for accessions, label in (
        (result.selected_accessions, "run selected accessions"),
        (result.completed_accessions, "run completed accessions"),
    ):
        if (
            type(accessions) is not tuple
            or len(accessions) > MAX_INSIDER_STATE_COLLECTION
            or any(
                type(accession) is not str
                or _DISCOVERY_ACCESSION_RE.fullmatch(accession) is None
                for accession in accessions
            )
            or list(accessions) != sorted(set(accessions))
        ):
            raise _backfill_error(label)
    if not set(result.completed_accessions) <= set(result.selected_accessions):
        raise _backfill_error("run completion bindings")
    if result.outcome is InsiderBackfillOutcome.PLANNED and result.completed_accessions:
        raise _backfill_error("planned completion bindings")
    if (
        result.outcome is InsiderBackfillOutcome.COMPLETED
        and result.completed_accessions != result.selected_accessions
    ):
        raise _backfill_error("completed run bindings")
    _validate_bulk_catalog_url(result.catalog_url)
    _validate_bulk_zip_url(result.zip_url, result.quarter)
    if (
        type(result.zip_sha256) is not str
        or _INSIDER_BULK_SHA256_RE.fullmatch(result.zip_sha256) is None
    ):
        raise _backfill_error("run source SHA-256")
    return result


def _validate_bulk_source_identity(
    identity: object,
) -> InsiderBulkSourceIdentity:
    if type(identity) is not InsiderBulkSourceIdentity:
        raise _backfill_error("source identity")
    _validate_bulk_quarter(identity.source_quarter)
    _validate_bulk_zip_url(identity.zip_url, identity.source_quarter)
    if (
        type(identity.zip_sha256) is not str
        or _INSIDER_BULK_SHA256_RE.fullmatch(identity.zip_sha256) is None
    ):
        raise _backfill_error("source SHA-256")
    return identity


def _validate_bulk_table_evidence(
    evidence: object,
) -> InsiderBulkTableEvidence:
    if type(evidence) is not InsiderBulkTableEvidence:
        raise _backfill_error("table evidence")
    if (
        type(evidence.table_name) is not str
        or evidence.table_name not in _INSIDER_BULK_TABLES
    ):
        raise _backfill_error("table evidence")
    if (
        type(evidence.headers) is not tuple
        or not evidence.headers
        or len(evidence.headers) > MAX_INSIDER_BULK_TSV_COLUMNS
        or any(
            type(header) is not str
            or _INSIDER_BULK_HEADER_RE.fullmatch(header) is None
            for header in evidence.headers
        )
        or len(set(evidence.headers)) != len(evidence.headers)
    ):
        raise _backfill_error("table headers")
    if (
        type(evidence.row_count) is not int
        or not 0 <= evidence.row_count <= MAX_INSIDER_BULK_TSV_ROWS
        or type(evidence.selected_row_count) is not int
        or not 0 <= evidence.selected_row_count <= evidence.row_count
    ):
        raise _backfill_error("table counts")
    return evidence


def _validate_bulk_accession_evidence(
    evidence: object,
) -> InsiderBulkAccessionEvidence:
    if type(evidence) is not InsiderBulkAccessionEvidence:
        raise _backfill_error("selected accession")
    if (
        type(evidence.accession_number) is not str
        or _DISCOVERY_ACCESSION_RE.fullmatch(evidence.accession_number) is None
        or type(evidence.issuer_cik) is not str
        or type(evidence.form_type) is not str
        or evidence.form_type not in SECTION16_CURRENT_FORMS
        or type(evidence.filing_date) is not str
        or _INSIDER_BULK_CANONICAL_DATE_RE.fullmatch(evidence.filing_date) is None
        or type(evidence.reporting_owner_ciks) is not tuple
        or len(evidence.reporting_owner_ciks) > MAX_INSIDER_STATE_COLLECTION
    ):
        raise _backfill_error("selected accession")
    try:
        normalized_cik = normalize_section16_cik(evidence.issuer_cik)
    except (TypeError, ValueError) as error:
        raise _backfill_error("selected issuer CIK") from error
    try:
        parsed_filing_date = datetime.strptime(evidence.filing_date, "%Y-%m-%d")
    except ValueError as error:
        raise _backfill_error("selected filing date") from error
    if parsed_filing_date.strftime("%Y-%m-%d") != evidence.filing_date:
        raise _backfill_error("selected filing date")
    normalized_owner_ciks: list[str] = []
    for owner_cik in evidence.reporting_owner_ciks:
        if type(owner_cik) is not str:
            raise _backfill_error("selected reporting owner CIKs")
        try:
            normalized_owner_cik = normalize_section16_cik(owner_cik)
        except (TypeError, ValueError) as error:
            raise _backfill_error("selected reporting owner CIKs") from error
        if normalized_owner_cik != owner_cik:
            raise _backfill_error("selected reporting owner CIKs")
        normalized_owner_ciks.append(owner_cik)
    if (
        normalized_cik != evidence.issuer_cik
        or normalized_owner_ciks != sorted(set(normalized_owner_ciks))
        or type(evidence.table_row_counts) is not tuple
    ):
        raise _backfill_error("selected accession")
    normalized_counts: list[tuple[str, int]] = []
    for item in evidence.table_row_counts:
        if (
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not str
            or item[0] not in _INSIDER_BULK_TABLES
            or type(item[1]) is not int
            or not 0 <= item[1] <= MAX_INSIDER_BULK_TSV_ROWS
        ):
            raise _backfill_error("selected table counts")
        normalized_counts.append(item)
    if (
        normalized_counts != sorted(normalized_counts)
        or len({item[0] for item in normalized_counts}) != len(normalized_counts)
        or dict(normalized_counts).get("SUBMISSION") != 1
        or dict(normalized_counts).get("REPORTINGOWNER", 0)
        != len(normalized_owner_ciks)
    ):
        raise _backfill_error("selected table counts")
    return evidence


def _normalize_bulk_filing_date(value: object) -> str:
    if type(value) is not str:
        raise _backfill_error("SUBMISSION filing date")
    match = _INSIDER_BULK_FILING_DATE_RE.fullmatch(value)
    if match is None:
        raise _backfill_error("SUBMISSION filing date")
    try:
        parsed = datetime(
            int(match.group("year")),
            _INSIDER_BULK_MONTHS[match.group("month")],
            int(match.group("day")),
        )
    except (KeyError, ValueError) as error:
        raise _backfill_error("SUBMISSION filing date") from error
    return parsed.strftime("%Y-%m-%d")


def _run_backfill_boundary(
    operation: Callable[[], _BackfillValue],
    *,
    fallback_label: str,
) -> _BackfillValue:
    """Normalize one public backfill boundary without retaining hostile errors."""

    error_type: type[InsiderBackfillError] | None = None
    error_message: str | None = None
    try:
        return operation()
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        if isinstance(error, InsiderBulkSourceRevisionError):
            error_type = InsiderBulkSourceRevisionError
            error_message = str(error)
        elif isinstance(error, InsiderBackfillError):
            error_type = InsiderBackfillError
            error_message = str(error)
        else:
            error_type = InsiderBackfillError
            error_message = str(_backfill_error(fallback_label))
    assert error_type is not None and error_message is not None
    raise error_type(error_message)


def _validate_bulk_quarter(
    value: object,
    *,
    as_of: datetime | None = None,
) -> tuple[int, int]:
    if type(value) is not str:
        raise _backfill_error("source quarter")
    match = _INSIDER_BULK_QUARTER_RE.fullmatch(value)
    if match is None:
        raise _backfill_error("source quarter")
    year = int(match.group("year"))
    quarter = int(match.group("quarter"))
    minimum = _INSIDER_BULK_QUARTER_RE.fullmatch(MIN_INSIDER_BULK_QUARTER)
    if minimum is None:  # defensive invariant for the module-owned policy constant
        raise RuntimeError("invalid minimum insider bulk quarter")
    minimum_value = (int(minimum.group("year")), int(minimum.group("quarter")))
    if (year, quarter) < minimum_value:
        raise _backfill_error("source quarter")
    if as_of is not None:
        if type(as_of) is not datetime or as_of.tzinfo is None:
            raise _backfill_error("as-of time")
        try:
            offset = as_of.utcoffset()
        except BaseException as error:
            if pipeline.is_control_flow_exception(error):
                raise
            raise _backfill_error("as-of time") from None
        if offset is None:
            raise _backfill_error("as-of time")
        current = as_of.astimezone(timezone.utc)
        current_quarter = (current.month - 1) // 3 + 1
        if (year, quarter) > (current.year, current_quarter):
            raise _backfill_error("future source quarter")
    return year, quarter


def _canonical_bulk_url(url: str) -> str:
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    netloc = host if parsed.port in (None, 443) else parsed.netloc
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def _validate_bulk_catalog_url(value: object) -> str:
    if type(value) is not str or value != INSIDER_BULK_CATALOG_URL:
        raise _backfill_error("catalog URL")
    try:
        pipeline.validate_sec_url(value)
    except (TypeError, ValueError) as error:
        raise _backfill_error("catalog URL") from error
    parsed = urlsplit(value)
    if parsed.query or parsed.fragment:
        raise _backfill_error("catalog URL")
    return value


def _bulk_zip_filenames(source_quarter: str) -> tuple[str, str]:
    quarter = source_quarter.lower()
    return (f"{quarter}.zip", f"{quarter}_form345.zip")


def _bulk_zip_paths(source_quarter: str) -> tuple[str, ...]:
    prefixes = (
        "/files/dera/data/insider-transactions-data-sets/",
        "/files/structureddata/data/insider-transactions-data-sets/",
    )
    return tuple(
        f"{prefix}{filename}"
        for prefix in prefixes
        for filename in _bulk_zip_filenames(source_quarter)
    )


def _validate_bulk_zip_url(value: object, source_quarter: str) -> str:
    _validate_bulk_quarter(source_quarter)
    if (
        type(value) is not str
        or len(value) > MAX_INSIDER_STATE_STRING_CHARS
        or any(
            ord(character) < 32
            or ord(character) == 127
            or ord(character) > 127
            for character in value
        )
    ):
        raise _backfill_error("ZIP URL")
    try:
        pipeline.validate_sec_url(value)
    except (TypeError, ValueError) as error:
        raise _backfill_error("ZIP URL") from error
    parsed = urlsplit(value)
    path = parsed.path
    pieces = path.split("/")
    if (
        parsed.hostname != "www.sec.gov"
        or parsed.query
        or parsed.fragment
        or not path.startswith("/files/")
        or "%" in path
        or "\\" in path
        or "//" in path
        or any(piece in {".", ".."} for piece in pieces)
        or len(pieces) < 4
        or pieces[-2] != "insider-transactions-data-sets"
        or pieces[-1] not in _bulk_zip_filenames(source_quarter)
        or path not in _bulk_zip_paths(source_quarter)
    ):
        raise _backfill_error("ZIP URL")
    return _canonical_bulk_url(value)


def _validate_bulk_catalog_entry(entry: object) -> InsiderBulkCatalogEntry:
    if type(entry) is not InsiderBulkCatalogEntry:
        raise _backfill_error("catalog entry")
    _validate_bulk_quarter(entry.source_quarter)
    catalog_url = _validate_bulk_catalog_url(entry.catalog_url)
    zip_url = _validate_bulk_zip_url(entry.zip_url, entry.source_quarter)
    if catalog_url != entry.catalog_url or zip_url != entry.zip_url:
        raise _backfill_error("catalog entry")
    return entry


def _validate_bulk_etag(value: object) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or len(value) > 1_024
        or _INSIDER_BULK_ETAG_RE.fullmatch(value) is None
    ):
        raise _backfill_error("ETag")
    return value


def _validate_bulk_last_modified(value: object) -> str | None:
    if value is None:
        return None
    if type(value) is not str or len(value) > 128:
        raise _backfill_error("Last-Modified")
    try:
        parsed = parsedate_to_datetime(value)
        canonical = format_datetime(parsed.astimezone(timezone.utc), usegmt=True)
    except (TypeError, ValueError, IndexError, OverflowError) as error:
        raise _backfill_error("Last-Modified") from error
    if parsed.tzinfo is None or canonical != value:
        raise _backfill_error("Last-Modified")
    return value


def _validate_bulk_result(result: object) -> InsiderBulkArchiveResult:
    if type(result) is not InsiderBulkArchiveResult:
        raise _backfill_error("archive result")
    entry = InsiderBulkCatalogEntry(
        source_quarter=result.source_quarter,
        catalog_url=result.catalog_url,
        zip_url=result.zip_url,
    )
    _validate_bulk_catalog_entry(entry)
    if (
        type(result.zip_sha256) is not str
        or _INSIDER_BULK_SHA256_RE.fullmatch(result.zip_sha256) is None
        or type(result.zip_byte_count) is not int
        or not 0 < result.zip_byte_count <= MAX_INSIDER_BULK_ARCHIVE_BYTES
    ):
        raise _backfill_error("archive identity")
    _validate_bulk_etag(result.etag)
    _validate_bulk_last_modified(result.last_modified)
    if type(result.table_evidence) is not tuple or not result.table_evidence:
        raise _backfill_error("table evidence")
    if any(type(item) is not InsiderBulkTableEvidence for item in result.table_evidence):
        raise _backfill_error("table evidence")
    for item in result.table_evidence:
        _validate_bulk_table_evidence(item)
    table_names = [item.table_name for item in result.table_evidence]
    if table_names != sorted(set(table_names)) or "SUBMISSION" not in table_names:
        raise _backfill_error("table evidence")
    if type(result.missing_optional_tables) is not tuple or any(
        type(item) is not str or item not in _INSIDER_BULK_OPTIONAL_TABLES
        for item in result.missing_optional_tables
    ):
        raise _backfill_error("missing optional tables")
    if list(result.missing_optional_tables) != sorted(set(result.missing_optional_tables)):
        raise _backfill_error("missing optional tables")
    present = set(table_names)
    missing = set(result.missing_optional_tables)
    if present & missing or present | missing != _INSIDER_BULK_TABLES:
        raise _backfill_error("table bindings")
    if (
        type(result.selected_accessions) is not tuple
        or len(result.selected_accessions) > MAX_INSIDER_BULK_SELECTED_ACCESSIONS
        or any(
        type(item) is not InsiderBulkAccessionEvidence
        for item in result.selected_accessions
        )
    ):
        raise _backfill_error("selected accessions")
    for item in result.selected_accessions:
        _validate_bulk_accession_evidence(item)
    accessions = [item.accession_number for item in result.selected_accessions]
    if accessions != sorted(set(accessions)):
        raise _backfill_error("selected accessions")
    expected_tables = sorted(present)
    selected_totals = {name: 0 for name in expected_tables}
    for accession in result.selected_accessions:
        if [item[0] for item in accession.table_row_counts] != expected_tables:
            raise _backfill_error("selected table bindings")
        for table_name, count in accession.table_row_counts:
            selected_totals[table_name] += count
    for evidence in result.table_evidence:
        if selected_totals[evidence.table_name] != evidence.selected_row_count:
            raise _backfill_error("selected table bindings")
    return result


def _reject_unsafe_catalog_declarations(catalog_html: bytes) -> None:
    upper = catalog_html.upper()
    if b"<!ENTITY" in upper:
        raise _backfill_error("catalog declaration")
    for declaration in re.findall(br"<!DOCTYPE[^>]*>", catalog_html, flags=re.I | re.S):
        if declaration.strip().lower() != b"<!doctype html>":
            raise _backfill_error("catalog declaration")


def _preflight_insider_bulk_catalog_structure(catalog_html: bytes) -> None:
    """Bound catalog HTML structure before building the full DOM."""

    parser = etree.HTMLPullParser(
        events=("start", "end"),
        recover=True,
        no_network=True,
        huge_tree=False,
    )
    elements = 0
    links = 0

    def process(event: str, element: etree._Element) -> None:
        nonlocal elements, links
        if event == "start":
            elements += 1
            if elements > MAX_INSIDER_BULK_CATALOG_ELEMENTS:
                raise _backfill_error("catalog element limit")
            if isinstance(element.tag, str) and element.tag.lower() == "a":
                links += 1
                if links > MAX_INSIDER_BULK_CATALOG_LINKS:
                    raise _backfill_error("catalog link limit")
            return
        parent = element.getparent()
        element.clear()
        if parent is not None:
            while element.getprevious() is not None:
                del parent[0]

    try:
        for offset in range(0, len(catalog_html), 8_192):
            parser.feed(catalog_html[offset : offset + 8_192])
            for event, element in parser.read_events():
                process(event, element)
        parser.close()
        for event, element in parser.read_events():
            process(event, element)
    except InsiderBackfillError:
        raise
    except (etree.XMLSyntaxError, ValueError, TypeError) as error:
        raise _backfill_error("catalog markup") from error


def _parse_insider_bulk_catalog_impl(
    catalog_html: bytes,
    *,
    quarter: object,
    as_of: datetime | None = None,
    catalog_url: object = INSIDER_BULK_CATALOG_URL,
) -> InsiderBulkCatalogEntry:
    """Select one exact bounded quarterly ZIP link from the official catalog."""

    if type(catalog_html) is not bytes or not catalog_html:
        raise _backfill_error("catalog body")
    if len(catalog_html) > MAX_INSIDER_BULK_CATALOG_BYTES or b"\x00" in catalog_html:
        raise _backfill_error("catalog body")
    when = datetime.now(timezone.utc) if as_of is None else as_of
    _validate_bulk_quarter(quarter, as_of=when)
    assert type(quarter) is str
    source_quarter = quarter
    catalog = _validate_bulk_catalog_url(catalog_url)
    _reject_unsafe_catalog_declarations(catalog_html)
    _preflight_insider_bulk_catalog_structure(catalog_html)
    parser = etree.HTMLParser(
        recover=True,
        no_network=True,
        huge_tree=False,
        remove_comments=False,
    )
    try:
        root = etree.fromstring(catalog_html, parser=parser)
    except (etree.XMLSyntaxError, ValueError, TypeError) as error:
        raise _backfill_error("catalog markup") from error
    elements = 0
    links = 0
    matching: dict[str, str] = {}
    target_paths = frozenset(_bulk_zip_paths(source_quarter))
    for element in root.iter():
        elements += 1
        if elements > MAX_INSIDER_BULK_CATALOG_ELEMENTS:
            raise _backfill_error("catalog element limit")
        if not isinstance(element.tag, str) or element.tag.lower() != "a":
            continue
        links += 1
        if links > MAX_INSIDER_BULK_CATALOG_LINKS:
            raise _backfill_error("catalog link limit")
        href = element.get("href")
        if (
            type(href) is not str
            or not href
            or len(href) > MAX_INSIDER_STATE_STRING_CHARS
            or href != href.strip()
            or any(
                ord(character) < 32
                or ord(character) == 127
                or ord(character) > 127
                for character in href
            )
            or "%" in href
            or "\\" in href
        ):
            continue
        try:
            href_parts = urlsplit(href)
        except ValueError:
            continue
        if href_parts.scheme:
            if href_parts.scheme != "https" or not href_parts.netloc:
                continue
        elif href_parts.netloc or not href.startswith("/") or href.startswith("//"):
            continue
        href_path = href_parts.path
        if any(piece in {".", ".."} for piece in href_path.split("/")):
            continue
        candidate = urljoin(catalog, href)
        candidate_path = urlsplit(candidate).path
        if candidate_path not in target_paths:
            continue
        validated = _validate_bulk_zip_url(candidate, source_quarter)
        matching[_canonical_bulk_url(validated)] = validated
    if len(matching) != 1:
        raise _backfill_error("catalog quarter link")
    zip_url = next(iter(matching))
    return InsiderBulkCatalogEntry(
        source_quarter=source_quarter,
        catalog_url=catalog,
        zip_url=zip_url,
    )


def parse_insider_bulk_catalog(
    catalog_html: bytes,
    *,
    quarter: object,
    as_of: datetime | None = None,
    catalog_url: object = INSIDER_BULK_CATALOG_URL,
) -> InsiderBulkCatalogEntry:
    """Select one exact bounded quarterly ZIP link from the official catalog."""

    return _run_backfill_boundary(
        lambda: _parse_insider_bulk_catalog_impl(
            catalog_html,
            quarter=quarter,
            as_of=as_of,
            catalog_url=catalog_url,
        ),
        fallback_label="catalog parsing",
    )


def _fetch_insider_bulk_catalog_impl(
    *,
    quarter: object,
    as_of: datetime | None = None,
    http: object = pipeline.HTTP,
    deadline_monotonic: float | None = None,
    monotonic: object | None = None,
) -> InsiderBulkCatalogEntry:
    """Fetch and parse one bounded official quarterly catalog entry."""

    when = datetime.now(timezone.utc) if as_of is None else as_of
    _validate_bulk_quarter(quarter, as_of=when)
    assert type(quarter) is str
    deadline = pipeline.validate_sec_deadline_monotonic(deadline_monotonic)
    if deadline is not None:
        pipeline.sec_deadline_remaining(deadline, monotonic=monotonic)
    try:
        get = getattr(http, "get")
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        raise _backfill_error("catalog HTTP client") from None
    if not callable(get):
        raise _backfill_error("catalog HTTP client")
    try:
        request_kwargs: dict[str, object] = {
            "stream": True,
            "headers": {"Accept-Encoding": "identity"},
        }
        if deadline is not None:
            request_kwargs["deadline_monotonic"] = deadline
        response = get(
            INSIDER_BULK_CATALOG_URL,
            **request_kwargs,
        )
    except BaseException as error:
        pipeline.close_sec_exception_response(error)
        if pipeline.is_control_flow_exception(error):
            raise
        if pipeline.is_sec_deadline_reached(error):
            raise _backfill_error("deadline") from None
        raise _backfill_error("catalog request") from None
    try:
        final_url = pipeline.sec_response_url(response)
        status = pipeline.sec_response_status(response)
        if (
            _canonical_bulk_url(final_url) != INSIDER_BULK_CATALOG_URL
            or status != 200
        ):
            raise _backfill_error("catalog response")
        _require_bulk_identity_encoding(response)
        declared_length = _bulk_declared_content_length(
            response,
            max_bytes=MAX_INSIDER_BULK_CATALOG_BYTES,
        )
    except BaseException as error:
        pipeline.close_sec_response(response)
        if pipeline.is_control_flow_exception(error):
            raise
        if isinstance(error, InsiderBackfillError):
            raise
        raise _backfill_error("catalog response") from None
    try:
        body = pipeline.read_bounded_sec_response(
            response,
            max_bytes=MAX_INSIDER_BULK_CATALOG_BYTES,
            deadline_monotonic=deadline,
            monotonic=monotonic,
        )
        if declared_length is not None and len(body) != declared_length:
            raise _backfill_error("Content-Length mismatch")
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        if pipeline.is_sec_deadline_reached(error):
            raise _backfill_error("deadline") from None
        if isinstance(error, InsiderBackfillError):
            raise
        raise _backfill_error("catalog response") from None
    return parse_insider_bulk_catalog(body, quarter=quarter, as_of=when)


def fetch_insider_bulk_catalog(
    *,
    quarter: object,
    as_of: datetime | None = None,
    http: object = pipeline.HTTP,
    deadline_monotonic: float | None = None,
    monotonic: object | None = None,
) -> InsiderBulkCatalogEntry:
    """Fetch and parse one bounded official quarterly catalog entry."""

    return _run_backfill_boundary(
        lambda: _fetch_insider_bulk_catalog_impl(
            quarter=quarter,
            as_of=as_of,
            http=http,
            deadline_monotonic=deadline_monotonic,
            monotonic=monotonic,
        ),
        fallback_label="catalog fetch",
    )


def _bulk_response_header(response: object, name: str) -> str | None:
    access_failed = False
    try:
        headers = getattr(response, "headers")
        get = getattr(headers, "get")
        value = get(name)
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        access_failed = True
        value = None
    if access_failed or (value is not None and type(value) is not str):
        raise _backfill_error("response headers")
    if value is not None and len(value) > MAX_INSIDER_STATE_STRING_CHARS:
        raise _backfill_error("response headers")
    return value


def _bulk_declared_content_length(
    response: object,
    *,
    max_bytes: int,
) -> int | None:
    content_length = _bulk_response_header(response, "Content-Length")
    if content_length is None:
        return None
    if re.fullmatch(r"(?:0|[1-9][0-9]{0,19})", content_length) is None:
        raise _backfill_error("Content-Length")
    declared_length = int(content_length)
    if declared_length > max_bytes:
        raise _backfill_error("response byte limit")
    return declared_length


def _require_bulk_identity_encoding(response: object) -> None:
    content_encoding = _bulk_response_header(response, "Content-Encoding")
    if (
        content_encoding is not None
        and content_encoding.strip().lower() != "identity"
    ):
        raise _backfill_error("Content-Encoding")


def _bulk_temp_directory(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, Path):
        raise _backfill_error("temporary directory")
    try:
        metadata = value.lstat()
    except OSError as error:
        raise _backfill_error("temporary directory") from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise _backfill_error("temporary directory")
    return os.fspath(value)


def _download_insider_bulk_archive(
    catalog_entry: InsiderBulkCatalogEntry,
    *,
    http: object,
    temp_directory: object,
    deadline_monotonic: float | None = None,
    monotonic: object | None = None,
) -> tuple[Path, str, int, str | None, str | None]:
    directory = _bulk_temp_directory(temp_directory)
    deadline = pipeline.validate_sec_deadline_monotonic(deadline_monotonic)
    if deadline is not None:
        pipeline.sec_deadline_remaining(deadline, monotonic=monotonic)
    try:
        fd, raw_path = tempfile.mkstemp(
            prefix=".insider-backfill-",
            suffix=".zip",
            dir=directory,
        )
    except OSError as error:
        raise _backfill_error("temporary archive") from error
    path = Path(raw_path)
    response: object | None = None
    completed = False
    primary_error: BaseException | None = None
    try:
        os.fchmod(fd, 0o600)
        try:
            get = getattr(http, "get")
        except BaseException as error:
            if pipeline.is_control_flow_exception(error):
                raise
            raise _backfill_error("HTTP client") from None
        if not callable(get):
            raise _backfill_error("HTTP client")
        try:
            request_kwargs: dict[str, object] = {
                "stream": True,
                "headers": {"Accept-Encoding": "identity"},
            }
            if deadline is not None:
                request_kwargs["deadline_monotonic"] = deadline
            response = get(
                catalog_entry.zip_url,
                **request_kwargs,
            )
        except BaseException as error:
            pipeline.close_sec_exception_response(error)
            if pipeline.is_control_flow_exception(error):
                raise
            if pipeline.is_sec_deadline_reached(error):
                raise _backfill_error("deadline") from None
            raise _backfill_error("archive request") from None
        final_url = pipeline.sec_response_url(response)
        status_code = pipeline.sec_response_status(response)
        if (
            _canonical_bulk_url(final_url) != catalog_entry.zip_url
            or status_code != 200
        ):
            raise _backfill_error("archive response")
        _require_bulk_identity_encoding(response)
        declared_length = _bulk_declared_content_length(
            response,
            max_bytes=MAX_INSIDER_BULK_ARCHIVE_BYTES,
        )
        etag = _validate_bulk_etag(_bulk_response_header(response, "ETag"))
        last_modified = _validate_bulk_last_modified(
            _bulk_response_header(response, "Last-Modified")
        )
        digest = hashlib.sha256()
        byte_count = 0
        with os.fdopen(fd, "wb", closefd=True) as output:
            fd = -1
            stream: Iterable[object] | None = None
            try:
                stream = pipeline.iter_sec_response_chunks(
                    response,
                    chunk_size=64 * 1024,
                    deadline_monotonic=deadline,
                    monotonic=monotonic,
                )
                for chunk in stream:
                    if not chunk:
                        continue
                    if type(chunk) is not bytes:
                        raise _backfill_error("archive response chunk")
                    byte_count += len(chunk)
                    if byte_count > MAX_INSIDER_BULK_ARCHIVE_BYTES:
                        raise _backfill_error("archive byte limit")
                    output.write(chunk)
                    digest.update(chunk)
            except BaseException as error:
                if pipeline.is_control_flow_exception(error):
                    raise
                if pipeline.is_sec_deadline_reached(error):
                    raise _backfill_error("deadline") from None
                if isinstance(error, InsiderBackfillError):
                    raise
                raise _backfill_error("archive stream") from None
            finally:
                if stream is not None:
                    close_stream = getattr(stream, "close", None)
                    if callable(close_stream):
                        close_stream()
            if declared_length is not None and byte_count != declared_length:
                raise _backfill_error("Content-Length mismatch")
            if byte_count == 0:
                raise _backfill_error("empty archive")
            output.flush()
            os.fsync(output.fileno())
        completed = True
        return path, digest.hexdigest(), byte_count, etag, last_modified
    except BaseException as error:
        primary_error = error
        if pipeline.is_control_flow_exception(error):
            raise
        if isinstance(error, InsiderBackfillError):
            raise
        raise _backfill_error("temporary archive I/O") from None
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        if response is not None:
            pipeline.close_sec_response_once(response)
        if not completed:
            cleanup_failed = False
            try:
                path.unlink(missing_ok=True)
            except OSError:
                cleanup_failed = True
            if cleanup_failed and (
                primary_error is None
                or not pipeline.is_control_flow_exception(primary_error)
            ):
                raise _backfill_error("temporary archive cleanup") from None


def _validate_bulk_zip_members(
    archive: zipfile.ZipFile,
) -> dict[str, zipfile.ZipInfo]:
    try:
        members = archive.infolist()
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        raise _backfill_error("ZIP directory") from error
    if not members or len(members) > MAX_INSIDER_BULK_ZIP_MEMBERS:
        raise _backfill_error("ZIP member limit")
    by_table: dict[str, zipfile.ZipInfo] = {}
    names: set[str] = set()
    compressed_total = 0
    uncompressed_total = 0
    for member in members:
        name = member.filename
        original_name = member.orig_filename
        if (
            type(name) is not str
            or type(original_name) is not str
            or original_name != name
            or "\x00" in original_name
            or not name
            or name.startswith(("/", "\\"))
            or "/" in name
            or "\\" in name
            or ":" in name
            or name in {".", ".."}
            or name.casefold() in names
            or member.is_dir()
        ):
            raise _backfill_error("ZIP member path")
        names.add(name.casefold())
        table_name: str | None = None
        if name not in _INSIDER_BULK_AUXILIARY_MEMBERS:
            if not name.endswith(".tsv"):
                raise _backfill_error("ZIP member name")
            table_name = name[:-4]
            if table_name not in _INSIDER_BULK_TABLES:
                raise _backfill_error("ZIP member name")
        if member.flag_bits & 1:
            raise _backfill_error("encrypted ZIP member")
        if member.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
            raise _backfill_error("ZIP compression")
        unix_mode = member.external_attr >> 16
        file_type = stat.S_IFMT(unix_mode)
        if file_type not in {0, stat.S_IFREG} or member.external_attr & 0x10:
            raise _backfill_error("ZIP member type")
        if (
            type(member.compress_size) is not int
            or member.compress_size < 0
            or type(member.file_size) is not int
            or member.file_size < 0
        ):
            raise _backfill_error("ZIP member size")
        compressed_total += member.compress_size
        uncompressed_total += member.file_size
        if (
            compressed_total > MAX_INSIDER_BULK_COMPRESSED_BYTES
            or uncompressed_total > MAX_INSIDER_BULK_UNCOMPRESSED_BYTES
            or (
                member.file_size > 0
                and member.file_size
                > max(member.compress_size, 1) * MAX_INSIDER_BULK_COMPRESSION_RATIO
            )
        ):
            raise _backfill_error("ZIP size or ratio")
        if table_name is not None:
            by_table[table_name] = member
    if "SUBMISSION" not in by_table:
        raise _backfill_error("required SUBMISSION table")
    return by_table


def _bulk_table_headers(reader: csv.DictReader[str], table_name: str) -> tuple[str, ...]:
    try:
        fields = reader.fieldnames
    except (csv.Error, UnicodeError) as error:
        raise _backfill_error(f"{table_name} headers") from error
    if (
        type(fields) is not list
        or not fields
        or len(fields) > MAX_INSIDER_BULK_TSV_COLUMNS
        or any(
            type(field) is not str
            or "\x00" in field
            or len(field) > MAX_INSIDER_BULK_TSV_FIELD_CHARS
            or _INSIDER_BULK_HEADER_RE.fullmatch(field) is None
            for field in fields
        )
        or len(set(fields)) != len(fields)
        or "ACCESSION_NUMBER" not in fields
        or (
            table_name == "SUBMISSION"
            and not {
                "ACCESSION_NUMBER",
                "FILING_DATE",
                "DOCUMENT_TYPE",
                "ISSUERCIK",
            }
            <= set(fields)
        )
        or (
            table_name == "REPORTINGOWNER"
            and "RPTOWNERCIK" not in fields
        )
    ):
        raise _backfill_error(f"{table_name} headers")
    return tuple(fields)


class _BoundedBulkTSVLines:
    """Bound physical and multiline logical TSV records before CSV allocation."""

    def __init__(self, stream: io.TextIOBase, *, max_record_chars: int) -> None:
        self._stream = stream
        self._max_record_chars = max_record_chars
        self._remaining = 0

    def start_record(self) -> None:
        self._remaining = self._max_record_chars

    def __iter__(self) -> _BoundedBulkTSVLines:
        return self

    def __next__(self) -> str:
        if self._remaining <= 0:
            raise _backfill_error("TSV record limit")
        line = self._stream.readline(self._remaining + 1)
        if type(line) is not str:
            raise _backfill_error("TSV text stream")
        if line == "":
            raise StopIteration
        if len(line) > self._remaining:
            raise _backfill_error("TSV record limit")
        self._remaining -= len(line)
        return line


def _read_bulk_table(
    archive: zipfile.ZipFile,
    *,
    table_name: str,
    member: zipfile.ZipInfo,
    approved_issuers: set[str],
    selected_identities: dict[str, tuple[str, str, str]],
    selected_owner_ciks: dict[str, set[str]],
    selected_counts: dict[str, dict[str, int]],
) -> InsiderBulkTableEvidence:
    row_count = 0
    selected_row_count = 0
    try:
        with archive.open(member, mode="r") as raw:
            with io.TextIOWrapper(
                raw,
                encoding="utf-8",
                errors="strict",
                newline="",
            ) as text:
                lines = _BoundedBulkTSVLines(
                    text,
                    max_record_chars=MAX_INSIDER_BULK_TSV_RECORD_CHARS,
                )
                reader = csv.DictReader(
                    lines,
                    delimiter="\t",
                    restkey="__EXTRA_FIELDS__",
                    restval=None,
                    strict=True,
                )
                lines.start_record()
                headers = _bulk_table_headers(reader, table_name)
                while True:
                    lines.start_record()
                    try:
                        row = next(reader)
                    except StopIteration:
                        break
                    row_count += 1
                    if row_count > MAX_INSIDER_BULK_TSV_ROWS:
                        raise _backfill_error(f"{table_name} row limit")
                    if (
                        "__EXTRA_FIELDS__" in row
                        or set(row) != set(headers)
                        or any(
                            value is None
                            or type(value) is not str
                            or "\x00" in value
                            or len(value) > MAX_INSIDER_BULK_TSV_FIELD_CHARS
                            for value in row.values()
                        )
                    ):
                        raise _backfill_error(f"{table_name} row")
                    accession = row["ACCESSION_NUMBER"]
                    if _DISCOVERY_ACCESSION_RE.fullmatch(accession) is None:
                        raise _backfill_error(f"{table_name} accession")
                    if table_name == "SUBMISSION":
                        try:
                            issuer_cik = normalize_section16_cik(row["ISSUERCIK"])
                        except (TypeError, ValueError) as error:
                            raise _backfill_error("SUBMISSION issuer CIK") from error
                        form_type = row["DOCUMENT_TYPE"]
                        if form_type not in SECTION16_CURRENT_FORMS:
                            raise _backfill_error("SUBMISSION form type")
                        filing_date = _normalize_bulk_filing_date(row["FILING_DATE"])
                        if issuer_cik in approved_issuers:
                            if accession in selected_identities:
                                raise _backfill_error("duplicate SUBMISSION accession")
                            if (
                                len(selected_counts)
                                >= MAX_INSIDER_BULK_SELECTED_ACCESSIONS
                            ):
                                raise _backfill_error("selected accession limit")
                            selected_identities[accession] = (
                                issuer_cik,
                                form_type,
                                filing_date,
                            )
                            selected_owner_ciks[accession] = set()
                            selected_counts[accession] = {"SUBMISSION": 1}
                            selected_row_count += 1
                        continue
                    if accession in selected_identities:
                        if table_name == "REPORTINGOWNER":
                            try:
                                owner_cik = normalize_section16_cik(row["RPTOWNERCIK"])
                            except (TypeError, ValueError) as error:
                                raise _backfill_error(
                                    "REPORTINGOWNER reporting owner CIK"
                                ) from error
                            owners = selected_owner_ciks[accession]
                            if owner_cik in owners:
                                raise _backfill_error(
                                    "duplicate REPORTINGOWNER reporting owner CIK"
                                )
                            if len(owners) >= MAX_INSIDER_STATE_COLLECTION:
                                raise _backfill_error("reporting owner CIK limit")
                            owners.add(owner_cik)
                        selected_counts[accession][table_name] = (
                            selected_counts[accession].get(table_name, 0) + 1
                        )
                        selected_row_count += 1
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        if isinstance(error, InsiderBackfillError):
            raise
        if isinstance(
            error,
            (csv.Error, UnicodeError, OSError, RuntimeError, zipfile.BadZipFile),
        ):
            raise _backfill_error(f"{table_name} TSV") from None
        raise _backfill_error(f"{table_name} TSV") from None
    return InsiderBulkTableEvidence(
        table_name=table_name,
        headers=headers,
        row_count=row_count,
        selected_row_count=selected_row_count,
    )


def _open_verified_bulk_archive(
    path: Path,
    *,
    zip_sha256: str,
    zip_byte_count: int,
) -> io.BufferedRandom:
    """Copy verified archive bytes into an anonymous hash-bound parse snapshot."""

    descriptor = -1
    snapshot: io.BufferedRandom | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        metadata = os.fstat(descriptor)
        path_metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
            or metadata.st_size != zip_byte_count
            or path_metadata.st_dev != metadata.st_dev
            or path_metadata.st_ino != metadata.st_ino
        ):
            raise _backfill_error("temporary archive identity")
        snapshot = tempfile.TemporaryFile(
            mode="w+b",
            prefix=".insider-backfill-verified-",
            dir=os.fspath(path.parent),
        )
        os.fchmod(snapshot.fileno(), 0o600)
        snapshot_metadata = os.fstat(snapshot.fileno())
        if (
            not stat.S_ISREG(snapshot_metadata.st_mode)
            or snapshot_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(snapshot_metadata.st_mode) != 0o600
            or snapshot_metadata.st_nlink != 0
        ):
            raise _backfill_error("temporary archive snapshot")
        digest = hashlib.sha256()
        observed = 0
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            observed += len(chunk)
            if observed > zip_byte_count or observed > MAX_INSIDER_BULK_ARCHIVE_BYTES:
                raise _backfill_error("temporary archive size")
            digest.update(chunk)
            remaining = memoryview(chunk)
            while remaining:
                written = snapshot.write(remaining)
                if type(written) is not int or written <= 0:
                    raise _backfill_error("temporary archive snapshot")
                remaining = remaining[written:]
        if observed != zip_byte_count or digest.hexdigest() != zip_sha256:
            raise _backfill_error("temporary archive SHA-256")
        snapshot.flush()
        os.fsync(snapshot.fileno())
        snapshot_metadata = os.fstat(snapshot.fileno())
        if (
            not stat.S_ISREG(snapshot_metadata.st_mode)
            or snapshot_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(snapshot_metadata.st_mode) != 0o600
            or snapshot_metadata.st_nlink != 0
            or snapshot_metadata.st_size != observed
        ):
            raise _backfill_error("temporary archive snapshot")
        snapshot.seek(0)
        os.close(descriptor)
        descriptor = -1
        verified = snapshot
        snapshot = None
        return verified
    except BaseException as error:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if snapshot is not None:
            try:
                snapshot.close()
            except OSError:
                pass
        if pipeline.is_control_flow_exception(error):
            raise
        if isinstance(error, InsiderBackfillError):
            raise
        raise _backfill_error("temporary archive verification") from None


def _parse_insider_bulk_archive_file(
    path: Path,
    *,
    catalog_entry: InsiderBulkCatalogEntry,
    approved_issuers: tuple[str, ...],
    zip_sha256: str,
    zip_byte_count: int,
    etag: str | None,
    last_modified: str | None,
) -> InsiderBulkArchiveResult:
    _validate_bulk_quarter(catalog_entry.source_quarter)
    try:
        with _open_verified_bulk_archive(
            path,
            zip_sha256=zip_sha256,
            zip_byte_count=zip_byte_count,
        ) as source, zipfile.ZipFile(source, mode="r") as archive:
            members = _validate_bulk_zip_members(archive)
            selected_identities: dict[str, tuple[str, str, str]] = {}
            selected_owner_ciks: dict[str, set[str]] = {}
            selected_counts: dict[str, dict[str, int]] = {}
            evidence = [
                _read_bulk_table(
                    archive,
                    table_name="SUBMISSION",
                    member=members["SUBMISSION"],
                    approved_issuers=set(approved_issuers),
                    selected_identities=selected_identities,
                    selected_owner_ciks=selected_owner_ciks,
                    selected_counts=selected_counts,
                )
            ]
            for table_name in sorted(set(members) - {"SUBMISSION"}):
                evidence.append(
                    _read_bulk_table(
                        archive,
                        table_name=table_name,
                        member=members[table_name],
                        approved_issuers=set(approved_issuers),
                        selected_identities=selected_identities,
                        selected_owner_ciks=selected_owner_ciks,
                        selected_counts=selected_counts,
                    )
                )
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        if isinstance(error, InsiderBackfillError):
            raise
        raise _backfill_error("ZIP archive") from None
    present_tables = {item.table_name for item in evidence}
    selected: list[InsiderBulkAccessionEvidence] = []
    for accession in sorted(selected_counts):
        issuer_cik, form_type, filing_date = selected_identities[accession]
        counts = tuple(
            (table_name, selected_counts[accession].get(table_name, 0))
            for table_name in sorted(present_tables)
        )
        selected.append(
            InsiderBulkAccessionEvidence(
                accession_number=accession,
                issuer_cik=issuer_cik,
                form_type=form_type,
                filing_date=filing_date,
                reporting_owner_ciks=tuple(sorted(selected_owner_ciks[accession])),
                table_row_counts=counts,
            )
        )
    return InsiderBulkArchiveResult(
        source_quarter=catalog_entry.source_quarter,
        catalog_url=catalog_entry.catalog_url,
        zip_url=catalog_entry.zip_url,
        zip_sha256=zip_sha256,
        zip_byte_count=zip_byte_count,
        etag=etag,
        last_modified=last_modified,
        table_evidence=tuple(sorted(evidence, key=lambda item: item.table_name)),
        missing_optional_tables=tuple(sorted(_INSIDER_BULK_OPTIONAL_TABLES - present_tables)),
        selected_accessions=tuple(selected),
    )


def _fetch_insider_bulk_archive_impl(
    catalog_entry: InsiderBulkCatalogEntry,
    *,
    approved_issuer_ciks: Iterable[object],
    http: object = pipeline.HTTP,
    temp_directory: Path | None = None,
    expected_source: InsiderBulkSourceIdentity | None = None,
    as_of: datetime | None = None,
    deadline_monotonic: float | None = None,
    monotonic: object | None = None,
) -> InsiderBulkArchiveResult:
    """Download, validate, and summarize one quarterly insider ZIP safely."""

    entry = _validate_bulk_catalog_entry(catalog_entry)
    when = datetime.now(timezone.utc) if as_of is None else as_of
    _validate_bulk_quarter(entry.source_quarter, as_of=when)
    try:
        approved_issuers = _normalize_approved_issuer_ciks(approved_issuer_ciks)
    except InsiderDiscoveryError as error:
        raise _backfill_error("approved issuer CIKs") from error
    if expected_source is not None:
        expected_source = _validate_bulk_source_identity(expected_source)
        if (
            expected_source.source_quarter != entry.source_quarter
            or _canonical_bulk_url(expected_source.zip_url) != entry.zip_url
        ):
            raise InsiderBulkSourceRevisionError(
                "completed insider bulk source URL changed"
            )
    path: Path | None = None
    primary_error: BaseException | None = None
    try:
        path, zip_sha256, zip_byte_count, etag, last_modified = (
            _download_insider_bulk_archive(
                entry,
                http=http,
                temp_directory=temp_directory,
                deadline_monotonic=deadline_monotonic,
                monotonic=monotonic,
            )
        )
        if expected_source is not None and expected_source.zip_sha256 != zip_sha256:
            raise InsiderBulkSourceRevisionError(
                "completed insider bulk source SHA-256 changed"
            )
        return _parse_insider_bulk_archive_file(
            path,
            catalog_entry=entry,
            approved_issuers=approved_issuers,
            zip_sha256=zip_sha256,
            zip_byte_count=zip_byte_count,
            etag=etag,
            last_modified=last_modified,
        )
    except BaseException as error:
        primary_error = error
        raise
    finally:
        if path is not None:
            cleanup_failed = False
            try:
                path.unlink(missing_ok=True)
            except OSError:
                cleanup_failed = True
            if cleanup_failed and (
                primary_error is None
                or not pipeline.is_control_flow_exception(primary_error)
            ):
                raise _backfill_error("temporary archive cleanup") from None


def fetch_insider_bulk_archive(
    catalog_entry: InsiderBulkCatalogEntry,
    *,
    approved_issuer_ciks: Iterable[object],
    http: object = pipeline.HTTP,
    temp_directory: Path | None = None,
    expected_source: InsiderBulkSourceIdentity | None = None,
    as_of: datetime | None = None,
    deadline_monotonic: float | None = None,
    monotonic: object | None = None,
) -> InsiderBulkArchiveResult:
    """Download, validate, and summarize one quarterly insider ZIP safely."""

    return _run_backfill_boundary(
        lambda: _fetch_insider_bulk_archive_impl(
            catalog_entry,
            approved_issuer_ciks=approved_issuer_ciks,
            http=http,
            temp_directory=temp_directory,
            expected_source=expected_source,
            as_of=as_of,
            deadline_monotonic=deadline_monotonic,
            monotonic=monotonic,
        ),
        fallback_label="archive processing",
    )


class InsiderIssuerReductionError(ValueError):
    """Raised when deterministic issuer-state reduction cannot fail closed."""


_ISSUER_PARSER_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_ISSUER_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_ISSUER_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_ISSUER_TIMESTAMP_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z"
)
_ISSUER_BASE_FORMS = frozenset({"3", "4", "5"})
_ISSUER_SOURCE_TABLES = frozenset({"non_derivative", "derivative"})


def _issuer_reduction_error(label: str) -> InsiderIssuerReductionError:
    return InsiderIssuerReductionError(f"insider issuer reduction is invalid: {label}")


def _issuer_date(value: object, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or not _ISSUER_DATE_RE.fullmatch(value):
        raise _issuer_reduction_error(label)
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as error:
        raise _issuer_reduction_error(label) from error
    if parsed.strftime("%Y-%m-%d") != value:
        raise _issuer_reduction_error(label)
    return value


def _issuer_timestamp(value: object, label: str) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or len(value) > MAX_INSIDER_STATE_STRING_CHARS
        or not _ISSUER_TIMESTAMP_RE.fullmatch(value)
    ):
        raise _issuer_reduction_error(label)
    try:
        datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as error:
        raise _issuer_reduction_error(label) from error
    return value


def _issuer_timestamp_value(value: str) -> datetime:
    return datetime.fromisoformat(f"{value[:-1]}+00:00")


def _issuer_safe_code(value: object, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if (
        type(value) is not str
        or not value
        or len(value) > 64
        or any(ord(character) < 32 or ord(character) > 126 for character in value)
    ):
        raise _issuer_reduction_error(label)
    return value


@dataclass(frozen=True, slots=True)
class NormalizedIssuerRecord:
    """Bounded private projection of one verified normalized filing."""

    accession_number: str
    parser_version: str
    normalized_sha256: str
    issuer_cik: str
    base_form_type: str
    is_amendment: bool
    filing_date: str
    accepted_at: str | None
    original_submission_date: str | None
    period_of_report: str | None
    owner_group_key: str
    owner_ciks: tuple[str, ...]
    transaction_signature: tuple[
        tuple[str, int, str, str | None, str | None], ...
    ]
    security_classes: tuple[tuple[str, bool, str], ...]

    def __post_init__(self) -> None:
        if (
            type(self.accession_number) is not str
            or not _DISCOVERY_ACCESSION_RE.fullmatch(self.accession_number)
            or type(self.parser_version) is not str
            or not _ISSUER_PARSER_VERSION_RE.fullmatch(self.parser_version)
            or type(self.normalized_sha256) is not str
            or not _ISSUER_SHA256_RE.fullmatch(self.normalized_sha256)
            or type(self.issuer_cik) is not str
            or type(self.base_form_type) is not str
            or self.base_form_type not in _ISSUER_BASE_FORMS
            or type(self.is_amendment) is not bool
            or type(self.owner_group_key) is not str
            or not _ISSUER_SHA256_RE.fullmatch(self.owner_group_key)
        ):
            raise _issuer_reduction_error("record identity")
        try:
            issuer_cik = normalize_section16_cik(self.issuer_cik)
        except (TypeError, ValueError) as error:
            raise _issuer_reduction_error("issuer CIK") from error
        if issuer_cik != self.issuer_cik:
            raise _issuer_reduction_error("issuer CIK")
        filing_date = _issuer_date(self.filing_date, "filing date")
        accepted_at = _issuer_timestamp(self.accepted_at, "accepted at")
        original_submission_date = _issuer_date(
            self.original_submission_date,
            "original submission date",
            nullable=True,
        )
        period_of_report = _issuer_date(
            self.period_of_report,
            "period of report",
            nullable=True,
        )

        if type(self.owner_ciks) is not tuple or not self.owner_ciks:
            raise _issuer_reduction_error("owner CIKs")
        if len(self.owner_ciks) > MAX_INSIDER_STATE_COLLECTION:
            raise _issuer_reduction_error("owner CIK limit")
        if any(type(value) is not str for value in self.owner_ciks):
            raise _issuer_reduction_error("owner CIKs")
        try:
            owner_ciks = tuple(
                sorted(normalize_section16_cik(value) for value in self.owner_ciks)
            )
        except (TypeError, ValueError) as error:
            raise _issuer_reduction_error("owner CIKs") from error
        if len(owner_ciks) != len(set(owner_ciks)):
            raise _issuer_reduction_error("owner CIKs")
        try:
            expected_owner_group = section16_owner_group_key(owner_ciks)
        except (TypeError, ValueError) as error:
            raise _issuer_reduction_error("owner group") from error
        if self.owner_group_key != expected_owner_group:
            raise _issuer_reduction_error("owner group")

        if (
            type(self.transaction_signature) is not tuple
            or len(self.transaction_signature) > MAX_INSIDER_STATE_COLLECTION
        ):
            raise _issuer_reduction_error("transaction signature")
        transaction_signature: list[tuple[str, int, str, str | None, str | None]] = []
        for coordinate in self.transaction_signature:
            if type(coordinate) is not tuple or len(coordinate) != 5:
                raise _issuer_reduction_error("transaction coordinate")
            source_table, row_index, security_class_key, transaction_date, transaction_code = coordinate
            if (
                type(source_table) is not str
                or source_table not in _ISSUER_SOURCE_TABLES
                or type(row_index) is not int
                or type(row_index) is bool
                or not 0 <= row_index <= MAX_INSIDER_STATE_INTEGER
                or type(security_class_key) is not str
                or not _ISSUER_SHA256_RE.fullmatch(security_class_key)
            ):
                raise _issuer_reduction_error("transaction coordinate")
            canonical_date = _issuer_date(
                transaction_date,
                "transaction date",
                nullable=True,
            )
            canonical_code = _issuer_safe_code(
                transaction_code,
                "transaction code",
                nullable=True,
            )
            transaction_signature.append(
                (
                    source_table,
                    row_index,
                    security_class_key,
                    canonical_date,
                    canonical_code,
                )
            )
        canonical_signature = tuple(sorted(transaction_signature))
        source_coordinates = {
            (source_table, row_index)
            for source_table, row_index, _, _, _ in canonical_signature
        }
        if (
            len(canonical_signature) != len(set(canonical_signature))
            or len(canonical_signature) != len(source_coordinates)
        ):
            raise _issuer_reduction_error("transaction signature")

        if (
            type(self.security_classes) is not tuple
            or len(self.security_classes) > MAX_INSIDER_STATE_COLLECTION
        ):
            raise _issuer_reduction_error("security classes")
        security_classes: list[tuple[str, bool, str]] = []
        for security_class in self.security_classes:
            if type(security_class) is not tuple or len(security_class) != 3:
                raise _issuer_reduction_error("security class")
            key, derivative, title = security_class
            if (
                type(key) is not str
                or not _ISSUER_SHA256_RE.fullmatch(key)
                or type(derivative) is not bool
                or type(title) is not str
            ):
                raise _issuer_reduction_error("security class")
            try:
                expected_key = section16_security_class_key(
                    issuer_cik,
                    title,
                    is_derivative=derivative,
                )
            except (TypeError, ValueError) as error:
                raise _issuer_reduction_error("security class") from error
            if key != expected_key:
                raise _issuer_reduction_error("security class")
            assert isinstance(title, str)
            security_classes.append((key, derivative, title))
        canonical_security_classes = tuple(sorted(security_classes))
        security_class_dimensions = {
            key: derivative
            for key, derivative, _ in canonical_security_classes
        }
        if len(canonical_security_classes) != len(security_class_dimensions):
            raise _issuer_reduction_error("security classes")
        if any(
            coordinate[2] not in security_class_dimensions
            or security_class_dimensions[coordinate[2]]
            != (coordinate[0] == "derivative")
            for coordinate in canonical_signature
        ):
            raise _issuer_reduction_error("transaction security class")

        object.__setattr__(self, "filing_date", filing_date)
        object.__setattr__(self, "accepted_at", accepted_at)
        object.__setattr__(self, "original_submission_date", original_submission_date)
        object.__setattr__(self, "period_of_report", period_of_report)
        object.__setattr__(self, "owner_ciks", owner_ciks)
        object.__setattr__(self, "transaction_signature", canonical_signature)
        object.__setattr__(self, "security_classes", canonical_security_classes)


@dataclass(frozen=True, slots=True)
class IssuerReductionResult:
    issuer_state: dict[str, object]
    accession_count: int
    owner_group_count: int
    security_class_count: int
    amendments_resolved: int
    amendments_unresolved: int


def issuer_record_from_normalized(
    normalized: object,
    *,
    parser_version: str,
) -> NormalizedIssuerRecord:
    """Project one verified immutable normalized filing into reducer evidence."""

    if (
        type(parser_version) is not str
        or not _ISSUER_PARSER_VERSION_RE.fullmatch(parser_version)
    ):
        raise _issuer_reduction_error("parser version")
    try:
        filing = validate_insider_filing(normalized)
        normalized_bytes = canonical_insider_json_bytes(filing)
        filing = validate_insider_filing(json.loads(normalized_bytes))
    except (
        InsiderContractError,
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        ValueError,
    ) as error:
        raise _issuer_reduction_error("normalized filing") from error
    if filing["parser_version"] != parser_version:
        raise _issuer_reduction_error("parser version binding")

    issuer = filing["issuer"]
    owners = filing["owners"]
    transactions = filing["transactions"]
    holdings = filing["holdings"]
    assert isinstance(issuer, dict)
    assert isinstance(owners, list)
    assert isinstance(transactions, list)
    assert isinstance(holdings, list)

    transaction_signature = tuple(sorted(
        (
            row["source_table"],
            row["source_row_index"],
            row["security_class_key"],
            row["transaction_date"],
            row["transaction_code"],
        )
        for row in transactions
        if isinstance(row, dict)
    ))
    security_classes_by_key: dict[str, tuple[bool, str]] = {}
    for collection in (transactions, holdings):
        for row in collection:
            assert isinstance(row, dict)
            key = row["security_class_key"]
            derivative = row["source_table"] == "derivative"
            title = row["security_title_as_filed"]
            assert isinstance(key, str)
            assert isinstance(title, str)
            existing = security_classes_by_key.setdefault(key, (derivative, title))
            if existing != (derivative, title):
                raise _issuer_reduction_error("security class collision")

            underlying_key = row["underlying_security_class_key"]
            underlying_title = row["underlying_security_title"]
            if underlying_key is None:
                if underlying_title is not None:
                    raise _issuer_reduction_error("underlying security class")
                continue
            if not isinstance(underlying_key, str) or not isinstance(
                underlying_title,
                str,
            ):
                raise _issuer_reduction_error("underlying security class")
            existing_underlying = security_classes_by_key.setdefault(
                underlying_key,
                (False, underlying_title),
            )
            if existing_underlying != (False, underlying_title):
                raise _issuer_reduction_error("security class collision")

    owner_ciks = tuple(
        owner["cik"]
        for owner in owners
        if isinstance(owner, dict)
    )
    return NormalizedIssuerRecord(
        accession_number=filing["accession_number"],
        parser_version=parser_version,
        normalized_sha256=hashlib.sha256(normalized_bytes).hexdigest(),
        issuer_cik=issuer["cik"],
        base_form_type=filing["base_form_type"],
        is_amendment=filing["is_amendment"],
        filing_date=filing["filing_date"],
        accepted_at=filing["accepted_at"],
        original_submission_date=filing["original_submission_date"],
        period_of_report=filing["period_of_report"],
        owner_group_key=filing["owner_group_key"],
        owner_ciks=owner_ciks,
        transaction_signature=transaction_signature,
        security_classes=tuple(
            (key, derivative, title)
            for key, (derivative, title) in sorted(security_classes_by_key.items())
        ),
    )


def _bounded_issuer_records(
    records: Iterable[NormalizedIssuerRecord],
) -> tuple[NormalizedIssuerRecord, ...]:
    if isinstance(records, (str, bytes)):
        raise _issuer_reduction_error("records")
    iterator = None
    try:
        iterator = iter(records)
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
    if iterator is None:
        raise _issuer_reduction_error("records")

    bounded: list[NormalizedIssuerRecord] = []
    while True:
        iteration_failed = False
        record: object = None
        try:
            record = next(iterator)
        except StopIteration:
            break
        except BaseException as error:
            if pipeline.is_control_flow_exception(error):
                raise
            iteration_failed = True
        if iteration_failed:
            raise _issuer_reduction_error("records")
        if len(bounded) >= MAX_INSIDER_STATE_COLLECTION:
            raise _issuer_reduction_error("record limit")
        if type(record) is not NormalizedIssuerRecord:
            raise _issuer_reduction_error("record type")
        bounded.append(record)
    return tuple(bounded)


def _amendment_resolution(
    amendment: NormalizedIssuerRecord,
    originals: tuple[NormalizedIssuerRecord, ...],
) -> dict[str, object]:
    candidates = [
        candidate
        for candidate in originals
        if candidate.base_form_type == amendment.base_form_type
        and candidate.owner_group_key == amendment.owner_group_key
        and amendment.original_submission_date is not None
        and candidate.filing_date == amendment.original_submission_date
        and candidate.filing_date <= amendment.filing_date
        and (
            candidate.accepted_at is None
            or amendment.accepted_at is None
            or _issuer_timestamp_value(candidate.accepted_at)
            <= _issuer_timestamp_value(amendment.accepted_at)
        )
    ]
    confidence: str | None = "high" if len(candidates) == 1 else None
    if len(candidates) > 1 and amendment.period_of_report is not None:
        period_candidates = [
            candidate
            for candidate in candidates
            if candidate.period_of_report is not None
            and candidate.period_of_report == amendment.period_of_report
        ]
        if period_candidates:
            candidates = period_candidates
            if len(candidates) == 1:
                confidence = "medium"
    if len(candidates) > 1 and amendment.transaction_signature:
        signature_candidates = [
            candidate
            for candidate in candidates
            if candidate.transaction_signature == amendment.transaction_signature
        ]
        if signature_candidates:
            candidates = signature_candidates
            if len(candidates) == 1:
                confidence = "low"
    candidate_accessions = sorted(
        candidate.accession_number for candidate in candidates
    )
    if confidence is not None and len(candidate_accessions) == 1:
        return {
            "accession_number": amendment.accession_number,
            "effective_accession": candidate_accessions[0],
            "confidence": confidence,
            "reason_code": "single_candidate",
            "candidates": candidate_accessions,
        }
    reason = "no_candidate" if not candidate_accessions else "ambiguous_candidates"
    return {
        "accession_number": amendment.accession_number,
        "effective_accession": None,
        "confidence": "unresolved",
        "reason_code": reason,
        "candidates": candidate_accessions,
    }


def reduce_issuer_state(
    *,
    issuer_cik: str,
    records: Iterable[NormalizedIssuerRecord],
) -> IssuerReductionResult:
    """Deterministically rebuild one issuer state from verified immutable records."""

    if type(issuer_cik) is not str:
        raise _issuer_reduction_error("issuer CIK")
    try:
        issuer = normalize_section16_cik(issuer_cik)
    except (TypeError, ValueError) as error:
        raise _issuer_reduction_error("issuer CIK") from error
    if issuer != issuer_cik:
        raise _issuer_reduction_error("issuer CIK")
    unique_by_accession: dict[str, NormalizedIssuerRecord] = {}
    for record in _bounded_issuer_records(records):
        if record.issuer_cik != issuer:
            raise _issuer_reduction_error("record issuer")
        existing = unique_by_accession.get(record.accession_number)
        if existing is None:
            unique_by_accession[record.accession_number] = record
            continue
        if (
            existing.parser_version != record.parser_version
            or existing.normalized_sha256 != record.normalized_sha256
            or existing != record
        ):
            raise _issuer_reduction_error("conflicting accession reference")

    unique = tuple(
        unique_by_accession[accession]
        for accession in sorted(unique_by_accession)
    )
    accessions = [
        {
            "accession_number": record.accession_number,
            "parser_version": record.parser_version,
            "normalized_sha256": record.normalized_sha256,
        }
        for record in unique
    ]
    owner_groups_by_key: dict[str, tuple[str, ...]] = {}
    security_classes_by_key: dict[str, tuple[bool, str]] = {}
    for record in unique:
        existing_owners = owner_groups_by_key.setdefault(
            record.owner_group_key,
            record.owner_ciks,
        )
        if existing_owners != record.owner_ciks:
            raise _issuer_reduction_error("owner group collision")
        for key, derivative, title in record.security_classes:
            existing_class = security_classes_by_key.get(key)
            if existing_class is None:
                if len(security_classes_by_key) >= MAX_INSIDER_STATE_COLLECTION:
                    raise _issuer_reduction_error("security class limit")
                security_classes_by_key[key] = (derivative, title)
                continue
            if existing_class != (derivative, title):
                raise _issuer_reduction_error("security class collision")
    owner_groups = [
        {
            "owner_group_key": key,
            "owner_ciks": list(owner_groups_by_key[key]),
        }
        for key in sorted(owner_groups_by_key)
    ]
    security_classes = [
        {
            "security_class_key": key,
            "derivative": security_classes_by_key[key][0],
            "title": security_classes_by_key[key][1],
        }
        for key in sorted(security_classes_by_key)
    ]
    originals = tuple(record for record in unique if not record.is_amendment)
    amendments = [
        _amendment_resolution(record, originals)
        for record in unique
        if record.is_amendment
    ]
    unresolved_ambiguities = [
        {
            "accession_number": amendment["accession_number"],
            "reason_code": amendment["reason_code"],
            "candidates": amendment["candidates"],
        }
        for amendment in amendments
        if amendment["confidence"] == "unresolved"
    ]
    resolution_by_accession = {
        amendment["accession_number"]: {
            "effective_accession": amendment["effective_accession"],
            "confidence": amendment["confidence"],
            "reason_code": amendment["reason_code"],
            "candidates": amendment["candidates"],
        }
        for amendment in amendments
    }
    generation_material = [
        {
            "accession_number": record.accession_number,
            "parser_version": record.parser_version,
            "normalized_sha256": record.normalized_sha256,
            "amendment_resolution": resolution_by_accession.get(
                record.accession_number
            ),
        }
        for record in unique
    ]
    generation_digest = issuer_generation_digest(generation_material)
    issuer_state: dict[str, object] = {
        "contract_version": ISSUER_STATE_CONTRACT_VERSION,
        "issuer_cik": issuer,
        "accessions": accessions,
        "owner_groups": owner_groups,
        "security_classes": security_classes,
        "amendments": amendments,
        "unresolved_ambiguities": unresolved_ambiguities,
        "generation_digest": generation_digest,
    }
    try:
        rendered_state = canonical_insider_state_json_bytes(issuer_state)
    except (RecursionError, TypeError, ValueError) as error:
        raise _issuer_reduction_error("issuer state") from error
    if len(rendered_state) > MAX_INSIDER_STATE_BYTES:
        raise _issuer_reduction_error("issuer state size limit")
    return IssuerReductionResult(
        issuer_state=issuer_state,
        accession_count=len(accessions),
        owner_group_count=len(owner_groups),
        security_class_count=len(security_classes),
        amendments_resolved=sum(
            amendment["confidence"] != "unresolved" for amendment in amendments
        ),
        amendments_unresolved=len(unresolved_ambiguities),
    )


def _discovery_error(label: str) -> InsiderDiscoveryError:
    return InsiderDiscoveryError(f"recent insider discovery is invalid: {label}")


def _canonical_discovery_timestamp(value: object, label: str) -> tuple[str, datetime]:
    if type(value) is not str or not value or len(value) > MAX_RECENT_INSIDER_ATOM_FIELD_CHARS:
        raise _discovery_error(label)
    text = value.strip()
    if not text or any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise _discovery_error(label)
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError as error:
        raise _discovery_error(label) from error
    if parsed.tzinfo is None or parsed.microsecond:
        raise _discovery_error(label)
    instant = parsed.astimezone(timezone.utc)
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ"), instant


def _reject_atom_declarations(atom_bytes: bytes) -> None:
    """Reject XML declarations other than the harmless processing instruction."""

    position = 0
    while (marker := atom_bytes.find(b"<!", position)) >= 0:
        if atom_bytes.startswith(b"<!--", marker):
            comment_end = atom_bytes.find(b"-->", marker + 4)
            if comment_end < 0:
                raise _discovery_error("XML declaration")
            position = comment_end + 3
            continue
        raise _discovery_error("DTD and entity declarations are disabled")


def _parse_bounded_atom(atom_bytes: bytes) -> etree._Element:
    if type(atom_bytes) is not bytes:
        raise TypeError("recent insider Atom input must be bytes")
    if not atom_bytes or len(atom_bytes) > MAX_RECENT_INSIDER_ATOM_BYTES:
        raise _discovery_error("Atom response size limit")
    _reject_atom_declarations(atom_bytes)
    parser = etree.XMLPullParser(
        events=("start", "end"),
        recover=False,
        resolve_entities=False,
        load_dtd=False,
        no_network=True,
        huge_tree=False,
        remove_comments=False,
    )
    element_count = 0
    entry_count = 0
    try:
        for offset in range(0, len(atom_bytes), 8192):
            parser.feed(atom_bytes[offset : offset + 8192])
            for event, element in parser.read_events():
                if event == "start" and isinstance(element.tag, str):
                    element_count += 1
                    if element_count > MAX_RECENT_INSIDER_ATOM_ELEMENTS:
                        raise _discovery_error("Atom element limit")
                    if element.tag == _ATOM_ENTRY:
                        entry_count += 1
                        if entry_count > MAX_RECENT_INSIDER_ATOM_ENTRIES:
                            raise _discovery_error("Atom entry limit")
                    for attribute in element.attrib.values():
                        if len(attribute) > MAX_RECENT_INSIDER_ATOM_FIELD_CHARS:
                            raise _discovery_error("Atom field size limit")
                elif (
                    event == "end"
                    and element.tag in {_ATOM_TITLE, _ATOM_UPDATED}
                    and len("".join(element.itertext())) > MAX_RECENT_INSIDER_ATOM_FIELD_CHARS
                ):
                    raise _discovery_error("Atom field size limit")
        root = parser.close()
        for event, element in parser.read_events():
            if event == "start" and isinstance(element.tag, str):
                element_count += 1
                if element_count > MAX_RECENT_INSIDER_ATOM_ELEMENTS:
                    raise _discovery_error("Atom element limit")
    except etree.XMLSyntaxError as error:
        raise _discovery_error("malformed Atom XML") from error
    if root is None or root.tag != f"{{{_ATOM_NAMESPACE}}}feed":
        raise _discovery_error("Atom feed root")
    return root


def _one_atom_text(entry: etree._Element, tag: str, label: str) -> str:
    matches = entry.findall(tag)
    if len(matches) != 1 or len(matches[0]) or matches[0].text is None:
        raise _discovery_error(label)
    text = matches[0].text.strip()
    if (
        not text
        or len(text) > MAX_RECENT_INSIDER_ATOM_FIELD_CHARS
        or any(ord(character) < 32 or ord(character) == 127 for character in text)
    ):
        raise _discovery_error(label)
    return text


def _one_atom_link(entry: etree._Element) -> str:
    links = entry.findall(_ATOM_LINK)
    candidates = [
        link
        for link in links
        if link.get("rel") in (None, "alternate") and link.get("href") is not None
    ]
    if len(candidates) != 1 or len(links) != 1:
        raise _discovery_error("entry link")
    href = candidates[0].get("href")
    assert href is not None
    if not href or len(href) > MAX_RECENT_INSIDER_ATOM_FIELD_CHARS:
        raise _discovery_error("entry link")
    return href


def _accession_from_compact(value: str) -> str:
    if not _DISCOVERY_COMPACT_ACCESSION_RE.fullmatch(value):
        raise _discovery_error("accession")
    accession = f"{value[:10]}-{value[10:12]}-{value[12:]}"
    if not _DISCOVERY_ACCESSION_RE.fullmatch(accession):
        raise _discovery_error("accession")
    return accession


def _validate_discovery_entry_url(
    url: object, *, entity_cik: str, entity_role: str
) -> tuple[str, str]:
    if type(url) is not str or not url or len(url) > MAX_RECENT_INSIDER_ATOM_FIELD_CHARS:
        raise _discovery_error("entry URL")
    if any(
        ord(character) < 32
        or ord(character) == 127
        or ord(character) > 127
        or character in {"%", "\\"}
        for character in url
    ):
        raise _discovery_error("entry URL")
    try:
        pipeline.validate_sec_url(url)
        parsed = urlsplit(url)
        port = parsed.port
    except (TypeError, ValueError) as error:
        raise _discovery_error("entry URL") from error
    parts = parsed.path.split("/")
    if (
        parsed.scheme != "https"
        or parsed.netloc not in {"www.sec.gov", "www.sec.gov:443"}
        or parsed.hostname != "www.sec.gov"
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or len(parts) != 7
        or parts[1:4] != ["Archives", "edgar", "data"]
        or parts[4] != str(int(entity_cik))
    ):
        raise _discovery_error("entry URL")
    accession = _accession_from_compact(parts[5])
    filename = parts[6]
    if entity_role == "issuer":
        if filename not in {
            f"{accession}-index.htm",
            f"{accession}-index.html",
        }:
            raise _discovery_error("issuer index URL")
    elif entity_role == "reporting_owner":
        if not _DISCOVERY_REPORTING_FILENAME_RE.fullmatch(filename):
            raise _discovery_error("reporting entry URL")
    else:
        raise _discovery_error("entity role")
    return url, accession


def parse_recent_insider_atom(
    atom_bytes: bytes, *, requested_form_type: str, observed_at: str
) -> tuple[RecentInsiderFeedEntry, ...]:
    """Parse one bounded current-filings Atom page into safe source evidence."""

    if requested_form_type not in SECTION16_CURRENT_FORMS:
        raise _discovery_error("requested form type")
    observed, observed_instant = _canonical_discovery_timestamp(observed_at, "observed at")
    root = _parse_bounded_atom(atom_bytes)
    entries: list[RecentInsiderFeedEntry] = []
    for entry in root.findall(_ATOM_ENTRY):
        title = _one_atom_text(entry, _ATOM_TITLE, "entry title")
        match = _DISCOVERY_TITLE_RE.fullmatch(title)
        if match is None:
            raise _discovery_error("entry form type")
        actual_form_type = match.group("form")
        allowed_form_types = {requested_form_type}
        if not requested_form_type.endswith("/A"):
            allowed_form_types.add(f"{requested_form_type}/A")
        if actual_form_type not in allowed_form_types:
            raise _discovery_error("entry form type")
        try:
            entity_cik = normalize_section16_cik(match.group("cik"))
        except ValueError as error:
            raise _discovery_error("entry CIK") from error
        role = "issuer" if match.group("role") == "Issuer" else "reporting_owner"
        accepted, accepted_instant = _canonical_discovery_timestamp(
            _one_atom_text(entry, _ATOM_UPDATED, "entry updated timestamp"),
            "entry updated timestamp",
        )
        if accepted_instant > observed_instant:
            raise _discovery_error("entry timestamps")
        url, accession = _validate_discovery_entry_url(
            _one_atom_link(entry), entity_cik=entity_cik, entity_role=role
        )
        entries.append(
            RecentInsiderFeedEntry(
                accession_number=accession,
                form_type=actual_form_type,
                entity_role=role,
                entity_cik=entity_cik,
                entry_url=url,
                accepted_at=accepted,
                observed_at=observed,
            )
        )
    return tuple(entries)


def _source_entry_sort_key(entry: RecentInsiderFeedEntry) -> tuple[object, ...]:
    return (
        _canonical_discovery_timestamp(entry.accepted_at, "accepted at")[1],
        entry.accession_number,
        entry.entity_role,
        entry.entity_cik,
        entry.entry_url,
        _canonical_discovery_timestamp(entry.observed_at, "observed at")[1],
    )


def _bounded_discovery_values(
    values: Iterable[_DiscoveryValue],
    *,
    label: str,
    maximum: int,
    require_nonempty: bool = False,
) -> tuple[_DiscoveryValue, ...]:
    if isinstance(values, (str, bytes)):
        raise _discovery_error(label)
    try:
        iterator = iter(values)
    except TypeError as error:
        raise _discovery_error(label) from error
    bounded: list[_DiscoveryValue] = []
    for value in iterator:
        if len(bounded) >= maximum:
            raise _discovery_error(label)
        bounded.append(value)
    if require_nonempty and not bounded:
        raise _discovery_error(label)
    return tuple(bounded)


def _normalize_approved_issuer_ciks(
    approved_issuer_ciks: Iterable[object],
) -> tuple[str, ...]:
    values = _bounded_discovery_values(
        approved_issuer_ciks,
        label="approved issuer CIKs",
        maximum=MAX_INSIDER_STATE_COLLECTION,
        require_nonempty=True,
    )
    try:
        normalized = tuple(normalize_section16_cik(value) for value in values)
    except (TypeError, ValueError) as error:
        raise _discovery_error("approved issuer CIKs") from error
    if len(set(normalized)) != len(normalized):
        raise _discovery_error("approved issuer CIKs")
    return tuple(sorted(normalized))


def _validate_recent_feed_entry(entry: object) -> RecentInsiderFeedEntry:
    if not isinstance(entry, RecentInsiderFeedEntry):
        raise _discovery_error("entry type")
    if (
        type(entry.accession_number) is not str
        or not _DISCOVERY_ACCESSION_RE.fullmatch(entry.accession_number)
        or entry.form_type not in SECTION16_CURRENT_FORMS
        or entry.entity_role not in {"issuer", "reporting_owner"}
    ):
        raise _discovery_error("entry contract")
    try:
        normalized_cik = normalize_section16_cik(entry.entity_cik)
    except (TypeError, ValueError) as error:
        raise _discovery_error("entry CIK") from error
    if normalized_cik != entry.entity_cik:
        raise _discovery_error("entry CIK")
    accepted, accepted_instant = _canonical_discovery_timestamp(
        entry.accepted_at, "accepted at"
    )
    observed, observed_instant = _canonical_discovery_timestamp(
        entry.observed_at, "observed at"
    )
    if (
        accepted != entry.accepted_at
        or observed != entry.observed_at
        or accepted_instant > observed_instant
    ):
        raise _discovery_error("entry timestamps")
    url, accession = _validate_discovery_entry_url(
        entry.entry_url,
        entity_cik=entry.entity_cik,
        entity_role=entry.entity_role,
    )
    if url != entry.entry_url or accession != entry.accession_number:
        raise _discovery_error("entry URL binding")
    return entry


def group_recent_insider_entries(
    entries: Iterable[RecentInsiderFeedEntry],
    *,
    approved_issuer_ciks: Iterable[object],
    max_accessions: int,
) -> IncrementalDiscoveryResult:
    """Collapse safe issuer/reporting entries into one deterministic accession queue."""

    approved = set(_normalize_approved_issuer_ciks(approved_issuer_ciks))
    if (
        type(max_accessions) is not int
        or not 1 <= max_accessions <= MAX_INSIDER_STATE_COLLECTION
    ):
        raise _discovery_error("maximum accessions")

    grouped: dict[str, list[RecentInsiderFeedEntry]] = {}
    entry_count = 0
    for entry in entries:
        entry_count += 1
        if entry_count > MAX_RECENT_INSIDER_DISCOVERY_ENTRIES:
            raise _discovery_error("discovery entry limit")
        entry = _validate_recent_feed_entry(entry)
        grouped.setdefault(entry.accession_number, []).append(entry)
        if len(grouped) > MAX_RECENT_INSIDER_GROUPS:
            raise _discovery_error("accession group limit")

    accessions: list[DiscoveredInsiderAccession] = []
    quarantined: list[str] = []
    for accession, repeated_sources in grouped.items():
        sources = sorted(set(repeated_sources), key=_source_entry_sort_key)
        issuer_sources = [source for source in sources if source.entity_role == "issuer"]
        if len(issuer_sources) != 1:
            quarantined.append(accession)
            continue
        issuer = issuer_sources[0]
        if issuer.entity_cik not in approved:
            continue
        if any(
            source.accession_number != accession
            or source.form_type != issuer.form_type
            or source.accepted_at != issuer.accepted_at
            or source.entity_role not in {"issuer", "reporting_owner"}
            for source in sources
        ):
            quarantined.append(accession)
            continue
        reporting_sources = sorted(
            set(source for source in sources if source.entity_role == "reporting_owner"),
            key=_source_entry_sort_key,
        )
        ordered_sources = tuple(
            sorted((issuer, *reporting_sources), key=_source_entry_sort_key)
        )
        accessions.append(
            DiscoveredInsiderAccession(
                accession_number=accession,
                issuer_cik=issuer.entity_cik,
                form_type=issuer.form_type,
                index_url=issuer.entry_url,
                accepted_at=issuer.accepted_at,
                observed_at=issuer.observed_at,
                reporting_entry_count=len(reporting_sources),
                source_entries=ordered_sources,
            )
        )
    accessions.sort(
        key=lambda entry: (
            _canonical_discovery_timestamp(entry.accepted_at, "accepted at")[1],
            entry.accession_number,
        )
    )
    selected = tuple(accessions[:max_accessions])
    if sum(len(entry.source_entries) for entry in selected) > MAX_INSIDER_STATE_COLLECTION:
        raise _discovery_error("state source evidence limit")
    return IncrementalDiscoveryResult(
        accessions=selected,
        quarantined_accessions=tuple(sorted(set(quarantined))),
    )


def _bounded_discovery_integer(value: object, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise _discovery_error(label)
    return value


def build_recent_insider_feed_url(
    form_type: str, *, start: int, page_size: int
) -> str:
    """Build one exact, bounded SEC current-filings Atom query."""

    if form_type not in SECTION16_CURRENT_FORMS:
        raise _discovery_error("requested form type")
    if type(start) is not int or start < 0:
        raise _discovery_error("page start")
    size = _bounded_discovery_integer(
        page_size, "page size", MAX_RECENT_INSIDER_PAGE_SIZE
    )
    query = urlencode(
        {
            "action": "getcurrent",
            "type": form_type,
            "owner": "include",
            "start": start,
            "count": size,
            "output": "atom",
        }
    )
    return f"{CURRENT_FILINGS_URL}?{query}"


def discover_recent_insider_accessions(
    *,
    approved_issuer_ciks: Iterable[object],
    lookback_seconds: int,
    max_pages: int,
    page_size: int,
    max_accessions: int,
    deadline_seconds: int,
    deadline_monotonic: object | None = None,
    now: datetime | None = None,
    http: object = pipeline.HTTP,
    monotonic: object | None = None,
) -> IncrementalDiscoveryResult:
    """Fetch bounded current-filings pages through the one shared SEC client."""

    approved = _normalize_approved_issuer_ciks(approved_issuer_ciks)
    lookback = _bounded_discovery_integer(
        lookback_seconds,
        "lookback seconds",
        MAX_RECENT_INSIDER_LOOKBACK_SECONDS,
    )
    pages = _bounded_discovery_integer(
        max_pages, "maximum pages", MAX_RECENT_INSIDER_PAGES
    )
    size = _bounded_discovery_integer(
        page_size, "page size", MAX_RECENT_INSIDER_PAGE_SIZE
    )
    maximum = _bounded_discovery_integer(
        max_accessions,
        "maximum accessions",
        MAX_INSIDER_STATE_COLLECTION,
    )
    deadline = _bounded_discovery_integer(
        deadline_seconds,
        "deadline seconds",
        MAX_RECENT_INSIDER_DEADLINE_SECONDS,
    )
    observed_instant = (
        datetime.now(timezone.utc).replace(microsecond=0) if now is None else now
    )
    if (
        not isinstance(observed_instant, datetime)
        or observed_instant.tzinfo is None
        or observed_instant.utcoffset() is None
        or observed_instant.microsecond
    ):
        raise _discovery_error("current timestamp")
    observed_instant = observed_instant.astimezone(timezone.utc)
    observed_at = observed_instant.strftime("%Y-%m-%dT%H:%M:%SZ")
    cutoff = observed_instant - timedelta(seconds=lookback)
    get_access_failed = False
    try:
        get = getattr(http, "get", None)
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        get_access_failed = True
        get = None
    if get_access_failed or not callable(get):
        raise _discovery_error("HTTP client")
    clock = time.monotonic if monotonic is None else monotonic
    if not callable(clock):
        raise _discovery_error("monotonic clock")
    started = clock()
    if isinstance(started, bool) or not isinstance(started, (int, float)):
        raise _discovery_error("monotonic clock")
    try:
        started_value = float(started)
    except (OverflowError, ValueError):
        raise _discovery_error("monotonic clock") from None
    if not math.isfinite(started_value) or started_value < 0:
        raise _discovery_error("monotonic clock")
    try:
        supplied_deadline = pipeline.validate_sec_deadline_monotonic(
            deadline_monotonic
        )
    except ValueError:
        raise _discovery_error("request deadline") from None
    request_deadline = (
        started_value + deadline
        if supplied_deadline is None
        else supplied_deadline
    )
    if not math.isfinite(request_deadline):
        raise _discovery_error("monotonic clock")

    telemetry = _active_insider_telemetry()
    if telemetry is not None:
        telemetry.increment("discovery_attempts")
    collected: list[RecentInsiderFeedEntry] = []
    pages_fetched = 0
    deadline_reached = False
    for form_type in SECTION16_CURRENT_FORMS:
        page_digests: set[str] = set()
        for page in range(pages):
            current = clock()
            if isinstance(current, bool) or not isinstance(current, (int, float)):
                raise _discovery_error("monotonic clock")
            try:
                current_value = float(current)
            except (OverflowError, ValueError):
                raise _discovery_error("monotonic clock") from None
            if (
                not math.isfinite(current_value)
                or current_value < 0
                or current_value < started_value
            ):
                raise _discovery_error("monotonic clock")
            if current_value >= request_deadline:
                deadline_reached = True
                break
            url = build_recent_insider_feed_url(
                form_type, start=page * size, page_size=size
            )
            response_failed = False
            try:
                response = get(
                    url,
                    stream=True,
                    deadline_monotonic=request_deadline,
                )
                body = pipeline.read_bounded_sec_response(
                    response,
                    max_bytes=MAX_RECENT_INSIDER_ATOM_BYTES,
                    deadline_monotonic=request_deadline,
                    monotonic=clock,
                )
            except BaseException as error:
                if pipeline.is_control_flow_exception(error):
                    raise
                if pipeline.is_sec_deadline_reached(error):
                    deadline_reached = True
                    break
                response_failed = True
                body = b""
            if response_failed:
                raise _discovery_error("Atom response") from None
            pages_fetched += 1
            digest = hashlib.sha256(body).hexdigest()
            if digest in page_digests:
                raise _discovery_error("Atom page loop")
            page_digests.add(digest)
            parsed = parse_recent_insider_atom(
                body,
                requested_form_type=form_type,
                observed_at=observed_at,
            )
            if not parsed:
                break
            if len(collected) + len(parsed) > MAX_RECENT_INSIDER_DISCOVERY_ENTRIES:
                raise _discovery_error("discovery entry limit")
            recent = [
                entry
                for entry in parsed
                if _canonical_discovery_timestamp(entry.accepted_at, "accepted at")[1]
                >= cutoff
            ]
            collected.extend(recent)
            if not recent:
                break
        if deadline_reached:
            break

    grouped = group_recent_insider_entries(
        collected,
        approved_issuer_ciks=approved,
        max_accessions=maximum,
    )
    if telemetry is not None:
        telemetry.increment("discovery_entries", len(collected))
        telemetry.increment("discovered_accession_groups", len(grouped.accessions))
    return IncrementalDiscoveryResult(
        accessions=grouped.accessions,
        quarantined_accessions=grouped.quarantined_accessions,
        pages_fetched=pages_fetched,
        deadline_reached=deadline_reached,
    )


def _validate_incremental_discovery_result(
    result: object,
) -> IncrementalDiscoveryResult:
    if (
        not isinstance(result, IncrementalDiscoveryResult)
        or type(result.accessions) is not tuple
        or type(result.quarantined_accessions) is not tuple
        or type(result.pages_fetched) is not int
        or not 0 <= result.pages_fetched <= len(SECTION16_CURRENT_FORMS) * MAX_RECENT_INSIDER_PAGES
        or type(result.deadline_reached) is not bool
        or len(result.accessions) > MAX_INSIDER_STATE_COLLECTION
        or len(result.quarantined_accessions) > MAX_RECENT_INSIDER_GROUPS
    ):
        raise _discovery_error("discovery result")
    quarantined = result.quarantined_accessions
    if (
        any(
            type(accession) is not str
            or not _DISCOVERY_ACCESSION_RE.fullmatch(accession)
            for accession in quarantined
        )
        or quarantined != tuple(sorted(set(quarantined)))
    ):
        raise _discovery_error("quarantined accessions")
    queued: set[str] = set()
    source_count = 0
    for discovered in result.accessions:
        if (
            not isinstance(discovered, DiscoveredInsiderAccession)
            or type(discovered.source_entries) is not tuple
            or type(discovered.reporting_entry_count) is not int
            or discovered.reporting_entry_count < 0
        ):
            raise _discovery_error("discovered accession")
        if source_count + len(discovered.source_entries) > MAX_INSIDER_STATE_COLLECTION:
            raise _discovery_error("discovery result evidence limit")
        sources = tuple(
            _validate_recent_feed_entry(source)
            for source in discovered.source_entries
        )
        issuer_sources = [source for source in sources if source.entity_role == "issuer"]
        reporting_count = sum(
            source.entity_role == "reporting_owner" for source in sources
        )
        if (
            len(issuer_sources) != 1
            or reporting_count != discovered.reporting_entry_count
            or discovered.accession_number in queued
        ):
            raise _discovery_error("discovered accession evidence")
        issuer = issuer_sources[0]
        if (
            discovered.accession_number != issuer.accession_number
            or discovered.issuer_cik != issuer.entity_cik
            or discovered.form_type != issuer.form_type
            or discovered.index_url != issuer.entry_url
            or discovered.accepted_at != issuer.accepted_at
            or discovered.observed_at != issuer.observed_at
            or any(
                source.accession_number != discovered.accession_number
                or source.form_type != discovered.form_type
                or source.accepted_at != discovered.accepted_at
                for source in sources
            )
        ):
            raise _discovery_error("discovered accession evidence")
        queued.add(discovered.accession_number)
        source_count += len(sources)
    if queued & set(quarantined):
        raise _discovery_error("discovery result evidence limit")
    return result


def _incremental_state_payload(
    result: IncrementalDiscoveryResult,
    *,
    lookback_seconds: int,
    completed_accessions: Iterable[str] = (),
) -> dict[str, object]:
    result = _validate_incremental_discovery_result(result)
    lookback = _bounded_discovery_integer(
        lookback_seconds,
        "lookback seconds",
        MAX_RECENT_INSIDER_LOOKBACK_SECONDS,
    )
    if len(result.accessions) > MAX_INSIDER_STATE_COLLECTION:
        raise _discovery_error("accession collection limit")
    queue: list[dict[str, object]] = []
    sources: list[dict[str, object]] = []
    observed: list[tuple[datetime, str]] = []
    queue_accessions: set[str] = set()
    for discovered in result.accessions:
        if not isinstance(discovered, DiscoveredInsiderAccession):
            raise _discovery_error("discovered accession")
        if discovered.accession_number in queue_accessions:
            raise _discovery_error("duplicate discovered accession")
        queue_accessions.add(discovered.accession_number)
        queue.append(
            {
                "accession_number": discovered.accession_number,
                "issuer_cik": discovered.issuer_cik,
                "form_type": discovered.form_type,
                "index_url": discovered.index_url,
                "accepted_at": discovered.accepted_at,
                "observed_at": discovered.observed_at,
            }
        )
        for source in discovered.source_entries:
            if not isinstance(source, RecentInsiderFeedEntry):
                raise _discovery_error("source entry")
            sources.append(
                {
                    "accession_number": source.accession_number,
                    "form_type": source.form_type,
                    "entity_role": source.entity_role,
                    "entity_cik": source.entity_cik,
                    "entry_url": source.entry_url,
                    "accepted_at": source.accepted_at,
                    "observed_at": source.observed_at,
                }
            )
            canonical, instant = _canonical_discovery_timestamp(
                source.observed_at, "observed at"
            )
            observed.append((instant, canonical))
    queue.sort(
        key=lambda entry: (
            _canonical_discovery_timestamp(entry["accepted_at"], "accepted at")[1],
            entry["accession_number"],
        )
    )
    sources.sort(
        key=lambda entry: (
            _canonical_discovery_timestamp(entry["accepted_at"], "accepted at")[1],
            entry["accession_number"],
            entry["entity_role"],
            entry["entity_cik"],
            entry["entry_url"],
            _canonical_discovery_timestamp(entry["observed_at"], "observed at")[1],
        )
    )
    completed_values = _bounded_discovery_values(
        completed_accessions,
        label="completed accessions",
        maximum=MAX_INSIDER_STATE_COLLECTION,
    )
    if any(
        type(accession) is not str
        or not _DISCOVERY_ACCESSION_RE.fullmatch(accession)
        for accession in completed_values
    ) or len(set(completed_values)) != len(completed_values):
        raise _discovery_error("completed accessions")
    completed = sorted(completed_values)
    if set(completed) - queue_accessions:
        raise _discovery_error("completed accessions")
    all_completed = set(completed) == queue_accessions
    status = (
        "completed"
        if all_completed and not result.deadline_reached
        else "incomplete"
    )
    return {
        "contract_version": INCREMENTAL_STATE_CONTRACT_VERSION,
        "status": status,
        "lookback_seconds": lookback,
        "first_observed_at": min(observed)[1] if observed else None,
        "last_observed_at": max(observed)[1] if observed else None,
        "queue": queue,
        "completed_accessions": completed,
        "source_entries": sources,
    }


def pending_incremental_candidates(
    state: object,
) -> tuple[DiscoveredInsiderAccession, ...]:
    """Reconstruct the validated pending Task 4 queue for sequential processing."""

    try:
        state = validate_incremental_state_payload(state)
    except (TypeError, ValueError) as error:
        raise _discovery_error("incremental state") from error

    required = {
        "contract_version",
        "status",
        "lookback_seconds",
        "first_observed_at",
        "last_observed_at",
        "queue",
        "completed_accessions",
        "source_entries",
    }
    if not isinstance(state, dict) or set(state) != required:
        raise _discovery_error("incremental state")
    queue = state.get("queue")
    completed = state.get("completed_accessions")
    source_entries = state.get("source_entries")
    if (
        state.get("contract_version") != INCREMENTAL_STATE_CONTRACT_VERSION
        or not isinstance(queue, list)
        or not isinstance(completed, list)
        or not isinstance(source_entries, list)
        or len(queue) > MAX_INSIDER_STATE_COLLECTION
        or len(completed) > MAX_INSIDER_STATE_COLLECTION
        or len(source_entries) > MAX_INSIDER_STATE_COLLECTION
    ):
        raise _discovery_error("incremental state")
    sources_by_accession: dict[str, list[RecentInsiderFeedEntry]] = {}
    for source in source_entries:
        if not isinstance(source, dict):
            raise _discovery_error("incremental source entry")
        try:
            entry = RecentInsiderFeedEntry(
                accession_number=source["accession_number"],
                form_type=source["form_type"],
                entity_role=source["entity_role"],
                entity_cik=source["entity_cik"],
                entry_url=source["entry_url"],
                accepted_at=source["accepted_at"],
                observed_at=source["observed_at"],
            )
        except KeyError as error:
            raise _discovery_error("incremental source entry") from error
        sources_by_accession.setdefault(entry.accession_number, []).append(entry)

    candidates: list[DiscoveredInsiderAccession] = []
    for queued in queue:
        if not isinstance(queued, dict):
            raise _discovery_error("incremental queue entry")
        try:
            accession = queued["accession_number"]
            sources = tuple(sources_by_accession.pop(accession))
            candidates.append(
                DiscoveredInsiderAccession(
                    accession_number=accession,
                    issuer_cik=queued["issuer_cik"],
                    form_type=queued["form_type"],
                    index_url=queued["index_url"],
                    accepted_at=queued["accepted_at"],
                    observed_at=queued["observed_at"],
                    reporting_entry_count=sum(
                        source.entity_role == "reporting_owner"
                        for source in sources
                    ),
                    source_entries=sources,
                )
            )
        except (KeyError, TypeError) as error:
            raise _discovery_error("incremental queue entry") from error
    if sources_by_accession:
        raise _discovery_error("incremental source entry")
    validated = _validate_incremental_discovery_result(
        IncrementalDiscoveryResult(
            accessions=tuple(candidates),
            quarantined_accessions=(),
        )
    )
    if (
        any(
            type(accession) is not str
            or not _DISCOVERY_ACCESSION_RE.fullmatch(accession)
            for accession in completed
        )
        or len(set(completed)) != len(completed)
        or set(completed)
        - {candidate.accession_number for candidate in validated.accessions}
    ):
        raise _discovery_error("incremental completion checkpoint")
    completed_set = set(completed)
    return tuple(
        candidate
        for candidate in validated.accessions
        if candidate.accession_number not in completed_set
    )


def validate_incremental_checkpoint_scope(
    state: object,
    *,
    issuer_ciks: Iterable[object],
    max_accessions: int,
) -> tuple[DiscoveredInsiderAccession, ...]:
    """Bind one validated incremental checkpoint to its requested work scope."""

    requested = frozenset(_normalize_approved_issuer_ciks(issuer_ciks))
    maximum = _bounded_discovery_integer(
        max_accessions,
        "maximum accessions",
        MAX_INSIDER_STATE_COLLECTION,
    )
    try:
        validated = validate_incremental_state_payload(state)
    except (TypeError, ValueError) as error:
        raise _discovery_error("incremental checkpoint scope") from error
    queue = validated["queue"]
    assert isinstance(queue, list)
    if len(queue) > maximum or any(
        not isinstance(entry, dict) or entry.get("issuer_cik") not in requested
        for entry in queue
    ):
        raise _discovery_error("incremental checkpoint scope")
    return pending_incremental_candidates(validated)


def resolve_incremental_checkpoint_action(
    state: object,
    *,
    issuer_ciks: Iterable[object],
    max_accessions: int,
) -> str:
    """Resolve new versus resume without crossing a pending durable scope."""

    try:
        validated = validate_incremental_state_payload(state)
    except (TypeError, ValueError) as error:
        raise _discovery_error("incremental checkpoint scope") from error
    pending = pending_incremental_candidates(validated)
    if validated["status"] == "completed" or not pending:
        return "new"
    validate_incremental_checkpoint_scope(
        validated,
        issuer_ciks=issuer_ciks,
        max_accessions=max_accessions,
    )
    return "resume"


def _durable_approved_issuer_ciks(
    state_store: InsiderStateStore,
) -> frozenset[str]:
    try:
        approved_state = state_store.read("approved-issuers-v1")
    except FileNotFoundError as error:
        raise _discovery_error("approved issuer state") from error
    approved_values = approved_state.get("issuer_ciks")
    if not isinstance(approved_values, list):
        raise _discovery_error("approved issuer state")
    return frozenset(approved_values)


def _backfill_source_identity_from_state(
    state: dict[str, object],
    *,
    quarter: str,
    issuer_cik: str,
) -> InsiderBulkSourceIdentity:
    catalog_url = state.get("catalog_url")
    zip_url = state.get("zip_url")
    zip_sha256 = state.get("zip_sha256")
    if (
        state.get("quarter") != quarter
        or state.get("issuer_cik") != issuer_cik
        or type(catalog_url) is not str
        or type(zip_url) is not str
        or type(zip_sha256) is not str
    ):
        raise _backfill_error("resume checkpoint")
    return InsiderBulkSourceIdentity(
        source_quarter=quarter,
        zip_url=zip_url,
        zip_sha256=zip_sha256,
    )


def _backfill_reconciliation_expectations(
    state: dict[str, object],
) -> tuple[tuple[str, int], ...]:
    entries = state.get("reconciliation")
    if not isinstance(entries, list):
        raise _backfill_error("resume reconciliation")
    result: list[tuple[str, int]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise _backfill_error("resume reconciliation")
        name = entry.get("name")
        expected_count = entry.get("expected_count")
        if type(name) is not str or type(expected_count) is not int:
            raise _backfill_error("resume reconciliation")
        result.append((name, expected_count))
    return tuple(result)


def _validate_backfill_resume_checkpoint(
    existing: dict[str, object],
    candidate: dict[str, object],
) -> tuple[str, ...]:
    immutable_keys = (
        "contract_version",
        "quarter",
        "issuer_cik",
        "catalog_url",
        "zip_url",
        "zip_sha256",
        "zip_byte_count",
        "etag",
        "last_modified",
        "table_evidence",
        "missing_optional_tables",
        "selected_accessions",
    )
    if any(existing.get(key) != candidate.get(key) for key in immutable_keys) or (
        _backfill_reconciliation_expectations(existing)
        != _backfill_reconciliation_expectations(candidate)
    ):
        raise InsiderBulkSourceRevisionError(
            "quarterly insider backfill source evidence changed"
        )
    completed = existing.get("completed_accessions")
    if not isinstance(completed, list) or any(type(value) is not str for value in completed):
        raise _backfill_error("resume completion checkpoint")
    return tuple(completed)


def _backfill_resume_selection(
    existing: dict[str, object],
    *,
    max_accessions: int,
) -> tuple[str, ...]:
    selected = existing.get("selected_accessions")
    if not isinstance(selected, list) or any(type(value) is not str for value in selected):
        raise _backfill_error("resume selection")
    if len(selected) > max_accessions:
        raise _backfill_error("resume accession bound")
    return tuple(selected)


def _backfill_selected_evidence(
    archive: InsiderBulkArchiveResult,
    *,
    max_accessions: int,
    resume_selection: tuple[str, ...] | None,
) -> tuple[InsiderBulkAccessionEvidence, ...]:
    if resume_selection is None:
        return archive.selected_accessions[:max_accessions]
    evidence_by_accession = {
        evidence.accession_number: evidence for evidence in archive.selected_accessions
    }
    try:
        return tuple(evidence_by_accession[accession] for accession in resume_selection)
    except KeyError:
        raise InsiderBulkSourceRevisionError(
            "quarterly insider backfill source evidence changed"
        ) from None


def _checkpoint_backfill_completion(
    current: dict[str, object],
    accession_number: str,
) -> dict[str, object]:
    selected = current.get("selected_accessions")
    completed = current.get("completed_accessions")
    if (
        not isinstance(selected, list)
        or not isinstance(completed, list)
        or accession_number not in selected
        or any(type(value) is not str for value in completed)
    ):
        raise _backfill_error("completion checkpoint")
    return {
        **current,
        "status": "running",
        "completed_accessions": sorted(set(completed) | {accession_number}),
    }


def _quarantine_backfill_checkpoint(
    state_store: InsiderStateStore,
    *,
    quarter: str,
    issuer_cik: str,
) -> None:
    state_store.update_backfill_if_issuer_approved(
        quarter,
        issuer_cik,
        lambda current: {**current, "status": "quarantined"},
    )


def _backfill_normalized_table_counts(
    *,
    storage: InsiderStorage,
    selected_accessions: tuple[InsiderBulkAccessionEvidence, ...],
    present_tables: frozenset[str],
) -> dict[str, int]:
    counts = {table_name: 0 for table_name in present_tables}
    if "SUBMISSION" in counts:
        counts["SUBMISSION"] = len(selected_accessions)
    if not (present_tables - {"SUBMISSION"}):
        return counts

    for evidence in selected_accessions:
        try:
            normalized = validate_insider_filing(
                storage.read_normalized(
                    evidence.accession_number,
                    INSIDER_PARSER_VERSION,
                )
            )
        except (InsiderContractError, InsiderStorageError) as error:
            raise _backfill_error("parser reconciliation") from error
        if (
            normalized.get("accession_number") != evidence.accession_number
            or normalized.get("issuer", {}).get("cik") != evidence.issuer_cik
        ):
            raise _backfill_error("parser reconciliation")

        owners = normalized["owners"]
        transactions = normalized["transactions"]
        holdings = normalized["holdings"]
        footnotes = normalized["footnotes"]
        signatures = normalized["signatures"]
        assert isinstance(owners, list)
        assert isinstance(transactions, list)
        assert isinstance(holdings, list)
        assert isinstance(footnotes, list)
        assert isinstance(signatures, list)
        if "REPORTINGOWNER" in counts:
            counts["REPORTINGOWNER"] += len(owners)
        if "FOOTNOTES" in counts:
            counts["FOOTNOTES"] += len(footnotes)
        if "OWNER_SIGNATURE" in counts:
            counts["OWNER_SIGNATURE"] += len(signatures)
        for transaction in transactions:
            assert isinstance(transaction, dict)
            source_table = transaction.get("source_table")
            table_name = (
                "NONDERIV_TRANS"
                if source_table == "non_derivative"
                else "DERIV_TRANS"
            )
            if table_name in counts:
                counts[table_name] += 1
        for holding in holdings:
            assert isinstance(holding, dict)
            source_table = holding.get("source_table")
            table_name = (
                "NONDERIV_HOLDING"
                if source_table == "non_derivative"
                else "DERIV_HOLDING"
            )
            if table_name in counts:
                counts[table_name] += 1
    return counts


def _completed_backfill_reconciliation(
    *,
    storage: InsiderStorage,
    checkpoint: dict[str, object],
    selected_accessions: tuple[InsiderBulkAccessionEvidence, ...],
) -> list[dict[str, object]]:
    expectations = _backfill_reconciliation_expectations(checkpoint)
    present_tables = frozenset(name for name, _ in expectations)
    actual_counts = _backfill_normalized_table_counts(
        storage=storage,
        selected_accessions=selected_accessions,
        present_tables=present_tables,
    )
    reconciliation = [
        {
            "name": name,
            "expected_count": expected_count,
            "actual_count": actual_counts[name],
            "status": (
                "matched" if expected_count == actual_counts[name] else "mismatch"
            ),
        }
        for name, expected_count in expectations
    ]
    telemetry = _active_insider_telemetry()
    if telemetry is not None:
        telemetry.observe_backfill_reconciliations(len(reconciliation))
    return reconciliation


def _run_insider_backfill_impl(
    *,
    issuer_cik: object,
    quarter: object,
    max_accessions: object,
    deadline: CooperativeDeadline,
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    plan_only: bool,
    resume: bool,
    http: object,
    temp_directory: Path | None,
    as_of: datetime | None,
    monotonic: Callable[[], float],
) -> InsiderBackfillRunResult:
    if not isinstance(storage, InsiderStorage):
        raise _backfill_error("storage")
    if not isinstance(state_store, InsiderStateStore):
        raise _backfill_error("state store")
    if not isinstance(deadline, CooperativeDeadline):
        raise _backfill_error("deadline")
    if type(plan_only) is not bool or type(resume) is not bool:
        raise _backfill_error("run mode")
    if plan_only and resume:
        raise _backfill_error("run mode")
    if (
        type(max_accessions) is not int
        or not 1 <= max_accessions <= MAX_INSIDER_BULK_SELECTED_ACCESSIONS
    ):
        raise _backfill_error("max accessions")
    when = datetime.now(timezone.utc) if as_of is None else as_of
    _validate_bulk_quarter(quarter, as_of=when)
    assert type(quarter) is str
    source_quarter = quarter
    try:
        issuer = normalize_section16_cik(issuer_cik)
    except (TypeError, ValueError) as error:
        raise _backfill_error("issuer CIK") from error
    if deadline.reached(monotonic):
        raise _backfill_error("deadline")
    try:
        approved_issuer_ciks = _durable_approved_issuer_ciks(state_store)
    except (InsiderDiscoveryError, InsiderStorageError) as error:
        raise _backfill_error("approved issuer state") from error
    if issuer not in approved_issuer_ciks:
        raise _backfill_error("approved issuer scope")

    existing: dict[str, object] | None = None
    expected_source: InsiderBulkSourceIdentity | None = None
    resume_selection: tuple[str, ...] | None = None
    if not plan_only:
        try:
            existing = state_store.read(f"backfill/{source_quarter}")
        except FileNotFoundError:
            if resume:
                raise _backfill_error("resume checkpoint") from None
        except InsiderStorageError as error:
            raise _backfill_error("resume checkpoint") from error
        if existing is not None:
            if not resume:
                raise _backfill_error("checkpoint already exists")
            expected_source = _backfill_source_identity_from_state(
                existing,
                quarter=source_quarter,
                issuer_cik=issuer,
            )
            resume_selection = _backfill_resume_selection(
                existing,
                max_accessions=max_accessions,
            )

    catalog = fetch_insider_bulk_catalog(
        quarter=source_quarter,
        as_of=when,
        http=http,
        deadline_monotonic=deadline.deadline_monotonic,
        monotonic=monotonic,
    )
    _validate_bulk_catalog_entry(catalog)
    if existing is not None and (
        existing.get("catalog_url") != catalog.catalog_url
        or expected_source is None
        or expected_source.zip_url != catalog.zip_url
    ):
        _quarantine_backfill_checkpoint(
            state_store,
            quarter=source_quarter,
            issuer_cik=issuer,
        )
        raise InsiderBulkSourceRevisionError(
            "quarterly insider backfill catalog source changed"
        )
    if deadline.reached(monotonic):
        raise _backfill_error("deadline")
    try:
        archive = fetch_insider_bulk_archive(
            catalog,
            approved_issuer_ciks=(issuer,),
            http=http,
            temp_directory=temp_directory,
            expected_source=expected_source,
            as_of=when,
            deadline_monotonic=deadline.deadline_monotonic,
            monotonic=monotonic,
        )
    except InsiderBulkSourceRevisionError:
        if existing is not None:
            _quarantine_backfill_checkpoint(
                state_store,
                quarter=source_quarter,
                issuer_cik=issuer,
            )
        raise
    _validate_bulk_result(archive)
    if (
        archive.source_quarter != source_quarter
        or archive.catalog_url != catalog.catalog_url
        or archive.zip_url != catalog.zip_url
        or any(item.issuer_cik != issuer for item in archive.selected_accessions)
    ):
        raise _backfill_error("source bindings")
    if expected_source is not None and (
        archive.source_quarter != expected_source.source_quarter
        or archive.zip_url != expected_source.zip_url
        or archive.zip_sha256 != expected_source.zip_sha256
    ):
        _quarantine_backfill_checkpoint(
            state_store,
            quarter=source_quarter,
            issuer_cik=issuer,
        )
        raise InsiderBulkSourceRevisionError(
            "quarterly insider backfill archive source changed"
        )
    telemetry = _active_insider_telemetry()
    if telemetry is not None:
        telemetry.observe_backfill_archive(archive)

    try:
        selected_evidence = _backfill_selected_evidence(
            archive,
            max_accessions=max_accessions,
            resume_selection=resume_selection,
        )
    except InsiderBulkSourceRevisionError:
        if existing is not None:
            _quarantine_backfill_checkpoint(
                state_store,
                quarter=source_quarter,
                issuer_cik=issuer,
            )
        raise
    selected_accessions = tuple(
        evidence.accession_number for evidence in selected_evidence
    )
    if plan_only:
        if deadline.reached(monotonic):
            raise _backfill_error("deadline")
        return InsiderBackfillRunResult(
            quarter=source_quarter,
            issuer_cik=issuer,
            outcome=InsiderBackfillOutcome.PLANNED,
            selected_accessions=selected_accessions,
            completed_accessions=(),
            catalog_url=archive.catalog_url,
            zip_url=archive.zip_url,
            zip_sha256=archive.zip_sha256,
        )

    checkpoint = _backfill_state_payload(
        archive=archive,
        issuer_cik=issuer,
        selected_accessions=selected_evidence,
    )
    if existing is None:
        try:
            state_store.write_backfill_if_issuer_approved(
                source_quarter,
                issuer,
                checkpoint,
            )
        except InsiderApprovalScopeError:
            raise
        except InsiderStorageError as error:
            raise _backfill_error("checkpoint") from error
        completed: tuple[str, ...] = ()
        status = "running"
    else:
        try:
            completed = _validate_backfill_resume_checkpoint(existing, checkpoint)
        except InsiderBulkSourceRevisionError:
            _quarantine_backfill_checkpoint(
                state_store,
                quarter=source_quarter,
                issuer_cik=issuer,
            )
            raise
        status_value = existing.get("status")
        if type(status_value) is not str:
            raise _backfill_error("resume checkpoint")
        status = status_value

    if status == "completed":
        return InsiderBackfillRunResult(
            quarter=source_quarter,
            issuer_cik=issuer,
            outcome=InsiderBackfillOutcome.COMPLETED,
            selected_accessions=selected_accessions,
            completed_accessions=completed,
            catalog_url=archive.catalog_url,
            zip_url=archive.zip_url,
            zip_sha256=archive.zip_sha256,
        )
    if status == "quarantined":
        return InsiderBackfillRunResult(
            quarter=source_quarter,
            issuer_cik=issuer,
            outcome=InsiderBackfillOutcome.QUARANTINED,
            selected_accessions=selected_accessions,
            completed_accessions=completed,
            catalog_url=archive.catalog_url,
            zip_url=archive.zip_url,
            zip_sha256=archive.zip_sha256,
        )
    if status not in {"running", "incomplete"}:
        raise _backfill_error("resume status")

    completed_set = set(completed)
    for evidence in selected_evidence:
        if evidence.accession_number in completed_set:
            continue
        if deadline.reached(monotonic):
            state_store.update_backfill_if_issuer_approved(
                source_quarter,
                issuer,
                lambda current: {**current, "status": "incomplete"},
            )
            return InsiderBackfillRunResult(
                quarter=source_quarter,
                issuer_cik=issuer,
                outcome=InsiderBackfillOutcome.CHECKPOINTED,
                selected_accessions=selected_accessions,
                completed_accessions=tuple(sorted(completed_set)),
                catalog_url=archive.catalog_url,
                zip_url=archive.zip_url,
                zip_sha256=archive.zip_sha256,
            )
        result = process_insider_backfill_accession(
            evidence,
            storage=storage,
            state_store=state_store,
            approved_issuer_ciks=(issuer,),
            deadline=deadline,
            http=http,
            monotonic=monotonic,
        )
        if (
            type(result) is not InsiderAccessionProcessResult
            or result.accession_number != evidence.accession_number
            or result.issuer_cik != issuer
            or result.form_type != evidence.form_type
        ):
            raise _backfill_error("processor result")
        if result.outcome in {
            InsiderAccessionOutcome.CREATED,
            InsiderAccessionOutcome.CACHE_HIT,
        } or (
            result.outcome is InsiderAccessionOutcome.CHECKPOINTED
            and result.error_class is None
            and result.reason_code is None
        ):
            try:
                updated = state_store.update_backfill_if_issuer_approved(
                    source_quarter,
                    issuer,
                    lambda current, accession=evidence.accession_number: (
                        _checkpoint_backfill_completion(current, accession)
                    ),
                )
            except (FileNotFoundError, InsiderStorageError):
                if telemetry is not None:
                    telemetry.increment("checkpoint_failures")
                raise
            if telemetry is not None:
                telemetry.increment("checkpoint_writes")
            updated_completed = updated.get("completed_accessions")
            if not isinstance(updated_completed, list) or any(
                type(value) is not str for value in updated_completed
            ):
                raise _backfill_error("completion checkpoint")
            completed_set = set(updated_completed)
            continue
        failure_status = (
            "quarantined"
            if result.outcome is InsiderAccessionOutcome.QUARANTINED
            else "incomplete"
        )
        state_store.update_backfill_if_issuer_approved(
            source_quarter,
            issuer,
            lambda current, failure_status=failure_status: {
                **current,
                "status": failure_status,
            },
        )
        outcome = (
            InsiderBackfillOutcome.QUARANTINED
            if failure_status == "quarantined"
            else InsiderBackfillOutcome.CHECKPOINTED
        )
        return InsiderBackfillRunResult(
            quarter=source_quarter,
            issuer_cik=issuer,
            outcome=outcome,
            selected_accessions=selected_accessions,
            completed_accessions=tuple(sorted(completed_set)),
            catalog_url=archive.catalog_url,
            zip_url=archive.zip_url,
            zip_sha256=archive.zip_sha256,
        )

    durable_checkpoint = state_store.read(f"backfill/{source_quarter}")
    try:
        reconciliation = _completed_backfill_reconciliation(
            storage=storage,
            checkpoint=durable_checkpoint,
            selected_accessions=selected_evidence,
        )
    except InsiderBackfillError:
        state_store.update_backfill_if_issuer_approved(
            source_quarter,
            issuer,
            lambda current: {**current, "status": "quarantined"},
        )
        return InsiderBackfillRunResult(
            quarter=source_quarter,
            issuer_cik=issuer,
            outcome=InsiderBackfillOutcome.QUARANTINED,
            selected_accessions=selected_accessions,
            completed_accessions=tuple(sorted(completed_set)),
            catalog_url=archive.catalog_url,
            zip_url=archive.zip_url,
            zip_sha256=archive.zip_sha256,
        )

    completed_state = state_store.update_backfill_if_issuer_approved(
        source_quarter,
        issuer,
        lambda current: {
            **current,
            "status": "completed",
            "completed_accessions": list(selected_accessions),
            "reconciliation": reconciliation,
        },
    )
    final_completed = completed_state.get("completed_accessions")
    if not isinstance(final_completed, list) or any(
        type(value) is not str for value in final_completed
    ):
        raise _backfill_error("completed checkpoint")
    return InsiderBackfillRunResult(
        quarter=source_quarter,
        issuer_cik=issuer,
        outcome=InsiderBackfillOutcome.COMPLETED,
        selected_accessions=selected_accessions,
        completed_accessions=tuple(final_completed),
        catalog_url=archive.catalog_url,
        zip_url=archive.zip_url,
        zip_sha256=archive.zip_sha256,
    )


def run_insider_backfill(
    *,
    issuer_cik: object,
    quarter: object,
    max_accessions: object,
    deadline: CooperativeDeadline,
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    plan_only: bool = False,
    resume: bool = False,
    http: object = pipeline.HTTP,
    temp_directory: Path | None = None,
    as_of: datetime | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> InsiderBackfillRunResult:
    """Plan or execute one bounded quarterly backfill for one approved issuer."""

    return _run_backfill_boundary(
        lambda: _run_insider_backfill_impl(
            issuer_cik=issuer_cik,
            quarter=quarter,
            max_accessions=max_accessions,
            deadline=deadline,
            storage=storage,
            state_store=state_store,
            plan_only=plan_only,
            resume=resume,
            http=http,
            temp_directory=temp_directory,
            as_of=as_of,
            monotonic=monotonic,
        ),
        fallback_label="orchestration",
    )


def _backfill_state_payload(
    *,
    archive: InsiderBulkArchiveResult,
    issuer_cik: str,
    selected_accessions: tuple[InsiderBulkAccessionEvidence, ...],
) -> dict[str, object]:
    selected_counts = {evidence.table_name: 0 for evidence in archive.table_evidence}
    for accession in selected_accessions:
        for table_name, row_count in accession.table_row_counts:
            selected_counts[table_name] += row_count
    return {
        "contract_version": BACKFILL_STATE_CONTRACT_VERSION,
        "quarter": archive.source_quarter,
        "issuer_cik": issuer_cik,
        "status": "running",
        "catalog_url": archive.catalog_url,
        "zip_url": archive.zip_url,
        "zip_sha256": archive.zip_sha256,
        "zip_byte_count": archive.zip_byte_count,
        "etag": archive.etag,
        "last_modified": archive.last_modified,
        "table_evidence": [
            {
                "table_name": evidence.table_name,
                "headers": list(evidence.headers),
                "row_count": evidence.row_count,
            }
            for evidence in archive.table_evidence
        ],
        "missing_optional_tables": list(archive.missing_optional_tables),
        "selected_accessions": [
            evidence.accession_number for evidence in selected_accessions
        ],
        "completed_accessions": [],
        "reconciliation": [
            {
                "name": table_name,
                "expected_count": expected_count,
                "actual_count": 0,
                "status": "pending",
            }
            for table_name, expected_count in sorted(selected_counts.items())
        ],
    }


def _require_approved_discovery_scope(
    accessions: Iterable[DiscoveredInsiderAccession],
    approved_issuer_ciks: frozenset[str],
) -> None:
    if any(
        discovered.issuer_cik not in approved_issuer_ciks
        for discovered in accessions
    ):
        raise _discovery_error("approved issuer scope")


def _write_approved_incremental_state(
    state_store: InsiderStateStore,
    payload: object,
    *,
    expected_sha256: str | None = None,
) -> None:
    try:
        state_store.write_incremental_if_issuers_approved(
            payload,
            expected_sha256=expected_sha256,
        )
    except InsiderApprovalScopeError as error:
        raise _discovery_error("approved issuer scope") from error


def persist_incremental_discovery_queue(
    state_store: InsiderStateStore,
    *,
    result: IncrementalDiscoveryResult,
    lookback_seconds: int,
    completed_artifact_verifier: Callable[[DiscoveredInsiderAccession], bool]
    | None = None,
) -> dict[str, object]:
    """Persist one bounded approved batch before processing, resuming pending work safely."""

    if not isinstance(state_store, InsiderStateStore):
        raise TypeError("state store must be an InsiderStateStore")
    if completed_artifact_verifier is not None and not callable(
        completed_artifact_verifier
    ):
        raise TypeError("completed artifact verifier must be callable")
    validated_result = _validate_incremental_discovery_result(result)
    approved_issuer_ciks = _durable_approved_issuer_ciks(state_store)
    _require_approved_discovery_scope(
        validated_result.accessions,
        approved_issuer_ciks,
    )
    candidate = _incremental_state_payload(
        validated_result,
        lookback_seconds=lookback_seconds,
    )
    try:
        existing = state_store.read("incremental-v1")
    except FileNotFoundError:
        _write_approved_incremental_state(state_store, candidate)
        return state_store.read("incremental-v1")

    existing_queue = existing["queue"]
    existing_completed = existing["completed_accessions"]
    assert isinstance(existing_queue, list) and isinstance(existing_completed, list)
    if any(
        not isinstance(entry, dict)
        or entry.get("issuer_cik") not in approved_issuer_ciks
        for entry in existing_queue
    ):
        raise _discovery_error("approved issuer scope")
    pending = {
        entry["accession_number"]
        for entry in existing_queue
        if isinstance(entry, dict)
    } - set(existing_completed)
    if existing["status"] != "completed" and pending:
        # Any nonterminal checkpoint can be the durable remainder of a crashed,
        # failed, or quarantined processor run.  Never replace that bounded work
        # merely because its lifecycle status advanced beyond ``incomplete``.
        return existing

    verified: list[str] = []
    previously_completed = set(existing_completed)
    existing_by_accession = {
        entry["accession_number"]: entry
        for entry in existing_queue
        if isinstance(entry, dict)
    }
    for discovered in validated_result.accessions:
        accession = discovered.accession_number
        if accession not in previously_completed:
            continue
        previous = existing_by_accession[accession]
        if (
            previous["issuer_cik"] != discovered.issuer_cik
            or previous["form_type"] != discovered.form_type
            or urlsplit(previous["index_url"]).path
            != urlsplit(discovered.index_url).path
            or previous["accepted_at"] != discovered.accepted_at
        ):
            raise _discovery_error("completed accession rediscovery binding")
        if completed_artifact_verifier is not None:
            verdict = completed_artifact_verifier(discovered)
            if type(verdict) is not bool:
                raise _discovery_error("completed artifact verifier result")
            if verdict:
                verified.append(accession)
    candidate = _incremental_state_payload(
        validated_result,
        lookback_seconds=lookback_seconds,
        completed_accessions=verified,
    )
    expected_sha256 = hashlib.sha256(
        canonical_insider_state_json_bytes(existing)
    ).hexdigest()
    _write_approved_incremental_state(
        state_store,
        candidate,
        expected_sha256=expected_sha256,
    )
    return state_store.read("incremental-v1")


class InsiderAccessionOutcome(StrEnum):
    """Bounded public outcomes for one sequential accession attempt."""

    CREATED = "created"
    CACHE_HIT = "cache_hit"
    CHECKPOINTED = "checkpointed"
    QUARANTINED = "quarantined"
    RETRY_LATER = "retry_later"


_PROCESSOR_RESULT_STAGES = frozenset(
    {
        "discovery",
        "cache",
        "index",
        "raw",
        "parse",
        "source",
        "normalized",
        "issuer",
        "checkpoint",
    }
)
_PROCESSOR_DEADLINE_STAGES = frozenset(
    {"cache", "index", "raw", "parse", "source", "normalized", "issuer", "checkpoint"}
)
_PROCESSOR_ERROR_CLASSES = frozenset(
    {
        "ConnectionError",
        "FileNotFoundError",
        "HTTPError",
        "InsiderContractError",
        "InsiderDiscoveryError",
        "InsiderIndexParseError",
        "InsiderIssuerReductionError",
        "InsiderParseError",
        "InsiderStorageError",
        "OSError",
        "RuntimeError",
        "TimeoutError",
        "TypeError",
        "ValueError",
    }
)
_PROCESSOR_QUARANTINE_REASONS_BY_STAGE = {
    "discovery": frozenset({"discovery_invalid", "index_parse_invalid"}),
    "cache": frozenset({"cache_invalid"}),
    "index": frozenset(
        {"index_cache_invalid", "index_invalid", "index_parse_invalid"}
    ),
    "raw": frozenset({"raw_cache_invalid", "raw_invalid", "raw_parse_invalid"}),
    "source": frozenset({"source_invalid"}),
    "issuer": frozenset({"issuer_invalid"}),
    "checkpoint": frozenset({"checkpoint_invalid"}),
}
_PROCESSOR_DURABLE_QUARANTINE_REASONS_BY_STAGE = {
    "cache": frozenset({"cache_invalid"}),
    "discovery": frozenset({"discovery_invalid", "index_parse_invalid"}),
    "raw": frozenset({"raw_invalid"}),
    "source": frozenset({"source_invalid"}),
    "issuer": frozenset({"issuer_invalid"}),
}


@dataclass(frozen=True, slots=True)
class InsiderAccessionProcessResult:
    accession_number: str
    issuer_cik: str
    form_type: str
    parser_version: str
    outcome: InsiderAccessionOutcome
    stage: str
    error_class: str | None = None
    reason_code: str | None = None

    def __post_init__(self) -> None:
        try:
            canonical_issuer = normalize_section16_cik(self.issuer_cik)
        except (TypeError, ValueError) as error:
            raise _discovery_error("processor result") from error
        if (
            type(self.accession_number) is not str
            or _DISCOVERY_ACCESSION_RE.fullmatch(self.accession_number) is None
            or type(self.issuer_cik) is not str
            or canonical_issuer != self.issuer_cik
            or type(self.form_type) is not str
            or self.form_type not in SECTION16_CURRENT_FORMS
            or type(self.parser_version) is not str
            or self.parser_version != INSIDER_PARSER_VERSION
            or type(self.outcome) is not InsiderAccessionOutcome
            or type(self.stage) is not str
            or self.stage not in _PROCESSOR_RESULT_STAGES
            or (
                self.error_class is not None
                and (
                    type(self.error_class) is not str
                    or self.error_class not in _PROCESSOR_ERROR_CLASSES
                )
            )
            or (
                self.reason_code is not None
                and type(self.reason_code) is not str
            )
        ):
            raise _discovery_error("processor result")

        if self.outcome is InsiderAccessionOutcome.CREATED:
            valid = (
                self.stage == "checkpoint"
                and self.error_class is None
                and self.reason_code is None
            )
        elif self.outcome is InsiderAccessionOutcome.CACHE_HIT:
            valid = (
                self.stage == "cache"
                and self.error_class is None
                and self.reason_code is None
            )
        elif self.outcome is InsiderAccessionOutcome.CHECKPOINTED:
            valid = (
                self.stage == "checkpoint"
                and self.error_class is None
                and self.reason_code is None
            ) or (
                self.stage in _PROCESSOR_DEADLINE_STAGES
                and self.error_class is None
                and self.reason_code == "deadline"
            ) or (
                self.stage == "checkpoint"
                and self.error_class is not None
                and self.reason_code == "checkpoint_failed"
            )
        elif self.outcome is InsiderAccessionOutcome.QUARANTINED:
            valid = (
                self.error_class is not None
                and self.reason_code
                in _PROCESSOR_QUARANTINE_REASONS_BY_STAGE.get(
                    self.stage,
                    frozenset(),
                )
            )
        else:
            valid = (
                self.stage in {"index", "raw"}
                and self.error_class is not None
                and self.reason_code == "fetch_failed"
            )
        if not valid:
            raise _discovery_error("processor result")


def _processor_error_class(error: BaseException) -> str:
    if isinstance(error, requests.HTTPError):
        return "HTTPError"
    if isinstance(error, InsiderIndexParseError):
        return "InsiderIndexParseError"
    if isinstance(error, InsiderIssuerReductionError):
        return "InsiderIssuerReductionError"
    if isinstance(error, InsiderParseError):
        return "InsiderParseError"
    if isinstance(error, InsiderContractError):
        return "InsiderContractError"
    if isinstance(error, InsiderDiscoveryError):
        return "InsiderDiscoveryError"
    if isinstance(error, InsiderStorageError):
        return "InsiderStorageError"
    if isinstance(error, FileNotFoundError):
        return "FileNotFoundError"
    if isinstance(error, TimeoutError):
        return "TimeoutError"
    if isinstance(error, ConnectionError):
        return "ConnectionError"
    if isinstance(error, OSError):
        return "OSError"
    if isinstance(error, RuntimeError):
        return "RuntimeError"
    if type(error) is TypeError:
        return "TypeError"
    if type(error) is ValueError:
        return "ValueError"
    raise _discovery_error("processor error class")


@dataclass(frozen=True, slots=True)
class CooperativeDeadline:
    """A validated monotonic deadline shared across sequential processor stages."""

    started_monotonic: float
    deadline_seconds: int

    def __post_init__(self) -> None:
        if isinstance(self.started_monotonic, bool) or not isinstance(
            self.started_monotonic,
            (int, float),
        ):
            raise _discovery_error("processor deadline")
        try:
            started = float(self.started_monotonic)
        except (OverflowError, ValueError) as error:
            raise _discovery_error("processor deadline") from error
        if (
            not math.isfinite(started)
            or started < 0
            or type(self.deadline_seconds) is not int
            or not 1
            <= self.deadline_seconds
            <= MAX_RECENT_INSIDER_DEADLINE_SECONDS
            or not math.isfinite(started + self.deadline_seconds)
        ):
            raise _discovery_error("processor deadline")

    @property
    def deadline_monotonic(self) -> float:
        return float(self.started_monotonic) + self.deadline_seconds

    def reached(self, monotonic: Callable[[], float]) -> bool:
        if not callable(monotonic):
            raise TypeError("processor monotonic clock must be callable")
        reading = monotonic()
        if isinstance(reading, bool) or not isinstance(reading, (int, float)):
            raise _discovery_error("processor monotonic clock")
        try:
            reading_value = float(reading)
            started = float(self.started_monotonic)
        except (OverflowError, ValueError) as error:
            raise _discovery_error("processor monotonic clock") from error
        if not math.isfinite(reading_value) or reading_value < started:
            raise _discovery_error("processor monotonic clock")
        return reading_value - started >= self.deadline_seconds


def _validated_processor_candidate(
    discovered: object,
    approved_issuer_ciks: Iterable[object],
) -> tuple[DiscoveredInsiderAccession, str, tuple[str, ...]]:
    result = _validate_incremental_discovery_result(
        IncrementalDiscoveryResult(
            accessions=(discovered,),
            quarantined_accessions=(),
        )
    )
    candidate = result.accessions[0]
    approved = frozenset(_normalize_approved_issuer_ciks(approved_issuer_ciks))
    if candidate.issuer_cik not in approved:
        raise _discovery_error("approved issuer scope")
    canonical_index_url = _canonical_processor_sec_url(candidate.index_url)
    reporting_owner_ciks = tuple(
        sorted(
            source.entity_cik
            for source in candidate.source_entries
            if source.entity_role == "reporting_owner"
        )
    )
    if len(set(reporting_owner_ciks)) != len(reporting_owner_ciks):
        raise _discovery_error("reporting owner CIKs")
    return candidate, canonical_index_url, reporting_owner_ciks


def _validate_insider_accession_identity(
    identity: object,
) -> InsiderAccessionIdentity:
    if type(identity) is not InsiderAccessionIdentity:
        raise _discovery_error("processor identity")
    if (
        type(identity.accession_number) is not str
        or _DISCOVERY_ACCESSION_RE.fullmatch(identity.accession_number) is None
        or type(identity.issuer_cik) is not str
        or type(identity.form_type) is not str
        or identity.form_type not in SECTION16_CURRENT_FORMS
        or type(identity.index_url) is not str
        or type(identity.accepted_at) is not str
        or type(identity.reporting_owner_ciks) is not tuple
        or len(identity.reporting_owner_ciks) > MAX_INSIDER_STATE_COLLECTION
    ):
        raise _discovery_error("processor identity")
    try:
        issuer_cik = normalize_section16_cik(identity.issuer_cik)
    except (TypeError, ValueError) as error:
        raise _discovery_error("processor identity") from error
    canonical_index_url = _canonical_processor_sec_url(identity.index_url)
    accepted_at, _ = _canonical_discovery_timestamp(
        identity.accepted_at,
        "processor accepted timestamp",
    )
    owner_ciks: list[str] = []
    for value in identity.reporting_owner_ciks:
        if type(value) is not str:
            raise _discovery_error("processor reporting owner CIKs")
        try:
            owner_ciks.append(normalize_section16_cik(value))
        except (TypeError, ValueError) as error:
            raise _discovery_error("processor reporting owner CIKs") from error
    if (
        issuer_cik != identity.issuer_cik
        or canonical_index_url != identity.index_url
        or accepted_at != identity.accepted_at
        or owner_ciks != list(identity.reporting_owner_ciks)
        or owner_ciks != sorted(set(owner_ciks))
    ):
        raise _discovery_error("processor identity")
    return identity


def _processor_identity_from_discovered(
    discovered: object,
    approved_issuer_ciks: Iterable[object],
) -> InsiderAccessionIdentity:
    candidate, canonical_index_url, reporting_owner_ciks = (
        _validated_processor_candidate(discovered, approved_issuer_ciks)
    )
    return InsiderAccessionIdentity(
        accession_number=candidate.accession_number,
        issuer_cik=candidate.issuer_cik,
        form_type=candidate.form_type,
        index_url=canonical_index_url,
        accepted_at=candidate.accepted_at,
        reporting_owner_ciks=reporting_owner_ciks,
    )


def _bulk_accession_index_url(evidence: InsiderBulkAccessionEvidence) -> str:
    _validate_bulk_accession_evidence(evidence)
    compact_accession = evidence.accession_number.replace("-", "")
    issuer_path = str(int(evidence.issuer_cik))
    return _canonical_processor_sec_url(
        "https://www.sec.gov/Archives/edgar/data/"
        f"{issuer_path}/{compact_accession}/{evidence.accession_number}-index.html"
    )


def _canonical_processor_sec_url(value: object) -> str:
    if type(value) is not str or not value:
        raise _discovery_error("processor SEC URL")
    try:
        pipeline.validate_sec_url(value)
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError) as error:
        raise _discovery_error("processor SEC URL") from error
    if (
        parsed.scheme != "https"
        or parsed.netloc not in {"www.sec.gov", "www.sec.gov:443"}
        or parsed.hostname != "www.sec.gov"
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/Archives/edgar/data/")
    ):
        raise _discovery_error("processor SEC URL")
    return urlunsplit(("https", "www.sec.gov", parsed.path, "", ""))


def _processor_result(
    candidate: (
        DiscoveredInsiderAccession
        | InsiderAccessionIdentity
        | InsiderBulkAccessionEvidence
    ),
    parser_version: str,
    outcome: InsiderAccessionOutcome,
    stage: str,
    *,
    error: BaseException | None = None,
    reason_code: str | None = None,
) -> InsiderAccessionProcessResult:
    result = InsiderAccessionProcessResult(
        accession_number=candidate.accession_number,
        issuer_cik=candidate.issuer_cik,
        form_type=candidate.form_type,
        parser_version=parser_version,
        outcome=outcome,
        stage=stage,
        error_class=_processor_error_class(error) if error is not None else None,
        reason_code=reason_code,
    )
    telemetry = _active_insider_telemetry()
    if telemetry is not None:
        telemetry.observe_processor_result(result)
    return result


def _storage_artifact_is_missing(error: InsiderStorageError) -> bool:
    """Recognize only a direct, public-reader FileNotFoundError cause as absent."""

    return isinstance(error.__cause__, FileNotFoundError)


def _processor_source_hashes(
    storage: InsiderStorage,
    candidate: InsiderAccessionIdentity,
) -> tuple[str, ...]:
    """Fingerprint the currently durable bounded source artifacts safely."""

    digests: set[str] = set()
    for reader in (storage.read_index_html, storage.read_raw):
        try:
            rendered = reader(candidate.accession_number)
        except InsiderStorageError as error:
            if _storage_artifact_is_missing(error):
                continue
            raise
        digests.add(hashlib.sha256(rendered).hexdigest())
    return tuple(sorted(digests))


def _quarantine_error_class(error: BaseException) -> str:
    if isinstance(error, InsiderIndexParseError):
        return "InsiderIndexParseError"
    if isinstance(error, InsiderIssuerReductionError):
        return "InsiderIssuerReductionError"
    if isinstance(error, InsiderParseError):
        return "InsiderParseError"
    if isinstance(error, InsiderContractError):
        return "InsiderContractError"
    return "InsiderStorageError"


def _processor_quarantine_identity(
    candidate: InsiderAccessionIdentity,
) -> dict[str, object]:
    return {
        "index_url": candidate.index_url,
        "accepted_at": candidate.accepted_at,
        "reporting_owner_ciks": list(candidate.reporting_owner_ciks),
    }


def _persist_deterministic_quarantine(
    state_store: InsiderStateStore,
    storage: InsiderStorage,
    candidate: InsiderAccessionIdentity,
    parser_version: str,
    *,
    stage: str,
    reason_code: str,
    error: BaseException,
    source_hashes: tuple[str, ...] | None = None,
) -> tuple[str, str, BaseException]:
    persisted_stage = stage
    persisted_reason_code = reason_code
    persisted_error = error
    if source_hashes is None:
        try:
            persisted_source_hashes = _processor_source_hashes(storage, candidate)
        except InsiderStorageError as source_error:
            persisted_stage = "cache"
            persisted_reason_code = "cache_invalid"
            persisted_error = source_error
            persisted_source_hashes = ()
    else:
        persisted_source_hashes = source_hashes
    key = f"quarantine/accessions/{candidate.accession_number}"
    payload = {
        "contract_version": QUARANTINE_STATE_CONTRACT_VERSION,
        "stage": persisted_stage,
        "error_class": _quarantine_error_class(persisted_error),
        "reason_code": persisted_reason_code,
        "retry_count": 0,
        "next_retry_at": None,
        "parser_version": parser_version,
        "source_hashes": list(persisted_source_hashes),
        "accession_number": candidate.accession_number,
        "issuer_cik": candidate.issuer_cik,
        "form_type": candidate.form_type,
        **_processor_quarantine_identity(candidate),
    }
    last_error: InsiderStorageError | None = None
    for _ in range(3):
        try:
            current = state_store.read(key)
        except FileNotFoundError:
            expected_sha256 = None
        else:
            expected_sha256 = hashlib.sha256(
                canonical_insider_state_json_bytes(current)
            ).hexdigest()
        try:
            state_store.write_accession_quarantine_if_issuer_approved(
                candidate.accession_number,
                candidate.issuer_cik,
                payload,
                expected_sha256=expected_sha256,
            )
        except InsiderApprovalScopeError:
            raise
        except InsiderStorageError as write_error:
            last_error = write_error
            continue
        return persisted_stage, persisted_reason_code, persisted_error
    assert last_error is not None
    raise last_error


def _persist_processor_quarantine_result(
    state_store: InsiderStateStore,
    storage: InsiderStorage,
    candidate: InsiderAccessionIdentity,
    parser_version: str,
    *,
    durable_stage: str,
    durable_reason_code: str,
    result_stage: str,
    result_reason_code: str,
    error: BaseException,
    source_hashes: tuple[str, ...] | None = None,
) -> InsiderAccessionProcessResult:
    persisted_stage, persisted_reason_code, persisted_error = (
        _persist_deterministic_quarantine(
            state_store,
            storage,
            candidate,
            parser_version,
            stage=durable_stage,
            reason_code=durable_reason_code,
            error=error,
            source_hashes=source_hashes,
        )
    )
    if persisted_stage == "cache":
        result_stage = persisted_stage
        result_reason_code = persisted_reason_code
        error = persisted_error
    return _processor_result(
        candidate,
        parser_version,
        InsiderAccessionOutcome.QUARANTINED,
        result_stage,
        error=error,
        reason_code=result_reason_code,
    )


def _matching_deterministic_quarantine(
    state_store: InsiderStateStore,
    storage: InsiderStorage,
    candidate: InsiderAccessionIdentity,
    parser_version: str,
) -> dict[str, object] | None:
    try:
        quarantine = state_store.read(
            f"quarantine/accessions/{candidate.accession_number}"
        )
    except FileNotFoundError:
        return None
    stage = quarantine.get("stage")
    reason_code = quarantine.get("reason_code")
    if type(stage) is not str or type(reason_code) is not str:
        return None
    if reason_code not in _PROCESSOR_DURABLE_QUARANTINE_REASONS_BY_STAGE.get(
        stage,
        frozenset(),
    ):
        return None
    if (
        quarantine.get("issuer_cik") != candidate.issuer_cik
        or quarantine.get("form_type") != candidate.form_type
    ):
        raise InsiderStorageError(
            "accession quarantine does not match discovered filing identity"
        )
    identity = _processor_quarantine_identity(candidate)
    if any(quarantine.get(field) != value for field, value in identity.items()):
        return None
    if (
        quarantine.get("next_retry_at") is None
        and quarantine.get("parser_version") == parser_version
    ):
        try:
            current_source_hashes = list(_processor_source_hashes(storage, candidate))
        except InsiderStorageError as source_error:
            if stage == "cache" and quarantine.get("source_hashes") == []:
                return quarantine
            _persist_deterministic_quarantine(
                state_store,
                storage,
                candidate,
                parser_version,
                stage="cache",
                reason_code="cache_invalid",
                error=source_error,
                source_hashes=(),
            )
            return state_store.read(
                f"quarantine/accessions/{candidate.accession_number}"
            )
        if stage != "cache" and quarantine.get("source_hashes") == current_source_hashes:
            return quarantine
    return None


def _processor_quarantine_replay_result(
    candidate: InsiderAccessionIdentity,
    parser_version: str,
    quarantine: dict[str, object],
) -> InsiderAccessionProcessResult:
    stage = quarantine.get("stage")
    error_class = quarantine.get("error_class")
    reason_code = quarantine.get("reason_code")
    if (
        type(stage) is not str
        or type(error_class) is not str
        or type(reason_code) is not str
    ):
        raise InsiderStorageError("accession quarantine summary is invalid")
    return InsiderAccessionProcessResult(
        accession_number=candidate.accession_number,
        issuer_cik=candidate.issuer_cik,
        form_type=candidate.form_type,
        parser_version=parser_version,
        outcome=InsiderAccessionOutcome.QUARANTINED,
        stage=stage,
        error_class=error_class,
        reason_code=reason_code,
    )


def _fetch_bounded_processor_artifact(
    http: object,
    url: str,
    *,
    max_bytes: int,
    deadline_monotonic: float | None = None,
    monotonic: object | None = None,
) -> bytes:
    get_access_failed = False
    try:
        get = getattr(http, "get", None)
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        get_access_failed = True
        get = None
    if get_access_failed or not callable(get):
        raise TypeError("processor HTTP client must provide get")
    deadline = pipeline.validate_sec_deadline_monotonic(deadline_monotonic)
    if deadline is not None:
        pipeline.sec_deadline_remaining(deadline, monotonic=monotonic)
    request_kwargs: dict[str, object] = {"stream": True}
    if deadline is not None:
        request_kwargs["deadline_monotonic"] = deadline
    request_failure: str | None = None
    try:
        response = get(url, **request_kwargs)
    except BaseException as error:
        if pipeline.is_control_flow_exception(error):
            raise
        pipeline.close_sec_exception_response(error)
        if pipeline.is_sec_deadline_reached(error):
            raise
        if isinstance(error, requests.HTTPError):
            request_failure = "http"
        elif isinstance(error, requests.Timeout):
            request_failure = "timeout"
        elif isinstance(error, requests.ConnectionError):
            request_failure = "connection"
        elif isinstance(error, requests.RequestException):
            request_failure = "request"
        else:
            request_failure = "runtime"
        response = None
    if request_failure == "http":
        raise requests.HTTPError("processor HTTP request failed") from None
    if request_failure == "timeout":
        raise requests.Timeout("processor HTTP request failed") from None
    if request_failure == "connection":
        raise requests.ConnectionError("processor HTTP request failed") from None
    if request_failure == "request":
        raise requests.RequestException("processor HTTP request failed") from None
    if request_failure is not None or response is None:
        raise RuntimeError("processor HTTP request failed") from None
    try:
        final_url = _canonical_processor_sec_url(
            pipeline.sec_response_url(response)
        )
        if final_url != url:
            raise _discovery_error("processor response URL")
        status = pipeline.sec_response_status(response)
        if not 200 <= status < 300:
            raise requests.HTTPError("processor HTTP response failed") from None
    except BaseException:
        pipeline.close_sec_response(response)
        raise
    return pipeline.read_bounded_sec_response(
        response,
        max_bytes=max_bytes,
        deadline_monotonic=deadline,
        monotonic=monotonic,
    )


def _candidate_reporting_owner_ciks(
    candidate: InsiderAccessionIdentity,
) -> tuple[str, ...]:
    return candidate.reporting_owner_ciks


def _normalized_matches_candidate(
    normalized: object,
    candidate: InsiderAccessionIdentity,
    canonical_index_url: str,
) -> bool:
    if not isinstance(normalized, dict):
        return False
    issuer = normalized.get("issuer")
    owners = normalized.get("owners")
    source = normalized.get("source")
    if (
        not isinstance(issuer, dict)
        or not isinstance(owners, list)
        or not isinstance(source, dict)
    ):
        return False
    owner_ciks = sorted(
        owner.get("cik")
        for owner in owners
        if isinstance(owner, dict) and type(owner.get("cik")) is str
    )
    if len(owner_ciks) != len(owners):
        return False
    try:
        stored_index_url = _canonical_processor_sec_url(source.get("index_url"))
    except InsiderDiscoveryError:
        return False
    return (
        normalized.get("accession_number") == candidate.accession_number
        and normalized.get("form_type") == candidate.form_type
        and normalized.get("accepted_at") == candidate.accepted_at
        and issuer.get("cik") == candidate.issuer_cik
        and owner_ciks == list(_candidate_reporting_owner_ciks(candidate))
        and stored_index_url == canonical_index_url
    )


def _verify_insider_identity_cache(
    identity: InsiderAccessionIdentity,
    *,
    storage: InsiderStorage,
    parser_version: str,
) -> dict[str, object] | None:
    """Return a fully revalidated immutable filing for a source-neutral identity."""

    candidate = _validate_insider_accession_identity(identity)
    try:
        normalized = storage.read_normalized(
            candidate.accession_number,
            parser_version,
        )
    except InsiderStorageError as error:
        if _storage_artifact_is_missing(error):
            return None
        raise
    if not _normalized_matches_candidate(
        normalized,
        candidate,
        candidate.index_url,
    ):
        raise InsiderStorageError(
            "cached normalized filing does not match accession identity"
        )
    return normalized


def verify_insider_accession_cache(
    discovered: DiscoveredInsiderAccession,
    *,
    storage: InsiderStorage,
    parser_version: str,
    approved_issuer_ciks: Iterable[object],
) -> dict[str, object] | None:
    """Return a fully revalidated immutable filing, or None only when absent."""

    if not isinstance(storage, InsiderStorage):
        raise TypeError("storage must be an InsiderStorage")
    if parser_version != INSIDER_PARSER_VERSION:
        raise _discovery_error("authoritative parser version")
    identity = _processor_identity_from_discovered(
        discovered,
        approved_issuer_ciks,
    )
    return _verify_insider_identity_cache(
        identity,
        storage=storage,
        parser_version=parser_version,
    )


def _incremental_queue_entry(
    state: dict[str, object],
    candidate: DiscoveredInsiderAccession,
) -> dict[str, object]:
    queue = state.get("queue")
    if not isinstance(queue, list):
        raise InsiderStorageError("incremental queue is invalid")
    matches = [
        entry
        for entry in queue
        if isinstance(entry, dict)
        and entry.get("accession_number") == candidate.accession_number
    ]
    if len(matches) != 1:
        raise InsiderStorageError("incremental queue does not contain accession")
    entry = matches[0]
    try:
        stored_index_url = _canonical_processor_sec_url(entry.get("index_url"))
        candidate_index_url = _canonical_processor_sec_url(candidate.index_url)
    except InsiderDiscoveryError as error:
        raise InsiderStorageError("incremental queue filing identity is invalid") from error
    if (
        entry.get("issuer_cik") != candidate.issuer_cik
        or entry.get("form_type") != candidate.form_type
        or entry.get("accepted_at") != candidate.accepted_at
        or stored_index_url != candidate_index_url
    ):
        raise InsiderStorageError("incremental queue filing identity changed")
    return entry


def _incremental_accession_is_completed(
    state: dict[str, object],
    candidate: DiscoveredInsiderAccession,
) -> bool:
    _incremental_queue_entry(state, candidate)
    completed = state.get("completed_accessions")
    if not isinstance(completed, list):
        raise InsiderStorageError("incremental completion checkpoint is invalid")
    return candidate.accession_number in completed


def _checkpoint_incremental_accession(
    state_store: InsiderStateStore,
    candidate: DiscoveredInsiderAccession,
) -> dict[str, object]:
    def complete(current: dict[str, object]) -> dict[str, object]:
        _incremental_queue_entry(current, candidate)
        queue = current["queue"]
        completed = current["completed_accessions"]
        assert isinstance(queue, list) and isinstance(completed, list)
        completed_accessions = sorted(
            {*completed, candidate.accession_number}
        )
        queue_accessions = {
            entry["accession_number"]
            for entry in queue
            if isinstance(entry, dict)
        }
        return {
            **current,
            "status": (
                "completed"
                if set(completed_accessions) == queue_accessions
                else "running"
            ),
            "completed_accessions": completed_accessions,
        }

    telemetry = _active_insider_telemetry()
    try:
        checkpoint = state_store.update_incremental_if_issuers_approved(complete)
    except (FileNotFoundError, InsiderStorageError):
        if telemetry is not None:
            telemetry.increment("checkpoint_failures")
        raise
    if telemetry is not None:
        telemetry.increment("checkpoint_writes")
    return checkpoint


def _processor_deadline_result(
    candidate: InsiderAccessionIdentity,
    parser_version: str,
    stage: str,
) -> InsiderAccessionProcessResult:
    return _processor_result(
        candidate,
        parser_version,
        InsiderAccessionOutcome.CHECKPOINTED,
        stage,
        reason_code="deadline",
    )


_MAX_ISSUER_STATE_CAS_ATTEMPTS = 5


def _reconcile_issuer_state(
    *,
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    candidate: InsiderAccessionIdentity,
    parser_version: str,
    normalized: object,
) -> None:
    """Rebuild one issuer from verified immutable records and publish it by CAS."""

    current_record = issuer_record_from_normalized(
        normalized,
        parser_version=parser_version,
    )
    if current_record.issuer_cik != candidate.issuer_cik:
        raise _issuer_reduction_error("current issuer binding")

    last_revision_error: InsiderStateRevisionError | None = None
    issuer_key = f"issuers/{candidate.issuer_cik}"
    for _ in range(_MAX_ISSUER_STATE_CAS_ATTEMPTS):
        try:
            existing = state_store.read(issuer_key)
        except FileNotFoundError:
            existing = None
            expected_sha256 = None
            references: list[object] = []
        else:
            expected_sha256 = hashlib.sha256(
                canonical_insider_state_json_bytes(existing)
            ).hexdigest()
            references_value = existing.get("accessions")
            if not isinstance(references_value, list):
                raise InsiderStorageError("issuer accession references are invalid")
            references = references_value

        records = [current_record]
        for reference in references:
            if not isinstance(reference, dict):
                raise InsiderStorageError("issuer accession reference is invalid")
            accession_number = reference.get("accession_number")
            if accession_number == current_record.accession_number:
                continue
            reference_parser_version = reference.get("parser_version")
            normalized_sha256 = reference.get("normalized_sha256")
            if (
                type(accession_number) is not str
                or type(reference_parser_version) is not str
                or type(normalized_sha256) is not str
            ):
                raise InsiderStorageError("issuer accession reference is invalid")
            prior_normalized = storage.read_normalized(
                accession_number,
                reference_parser_version,
            )
            prior_record = issuer_record_from_normalized(
                prior_normalized,
                parser_version=reference_parser_version,
            )
            if (
                prior_record.issuer_cik != candidate.issuer_cik
                or prior_record.normalized_sha256 != normalized_sha256
            ):
                raise InsiderStorageError(
                    "issuer accession reference does not match immutable normalized data"
                )
            records.append(prior_record)

        reduction = reduce_issuer_state(
            issuer_cik=candidate.issuer_cik,
            records=records,
        )
        if existing is not None and reduction.issuer_state == existing:
            telemetry = _active_insider_telemetry()
            if telemetry is not None:
                telemetry.observe_reduction(reduction)
            return
        try:
            state_store.write_issuer_if_approved(
                candidate.issuer_cik,
                reduction.issuer_state,
                expected_sha256=expected_sha256,
            )
        except InsiderStateRevisionError as error:
            last_revision_error = error
            continue
        telemetry = _active_insider_telemetry()
        if telemetry is not None:
            telemetry.observe_reduction(reduction)
        return

    assert last_revision_error is not None
    raise last_revision_error


def _reconcile_issuer_or_result(
    *,
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    candidate: InsiderAccessionIdentity,
    parser_version: str,
    normalized: object,
) -> InsiderAccessionProcessResult | None:
    try:
        _reconcile_issuer_state(
            storage=storage,
            state_store=state_store,
            candidate=candidate,
            parser_version=parser_version,
            normalized=normalized,
        )
    except InsiderApprovalScopeError:
        raise
    except InsiderStateRevisionError as error:
        return _processor_result(
            candidate,
            parser_version,
            InsiderAccessionOutcome.CHECKPOINTED,
            "checkpoint",
            error=error,
            reason_code="checkpoint_failed",
        )
    except (
        InsiderIssuerReductionError,
        InsiderStorageError,
        TypeError,
        ValueError,
    ) as error:
        return _persist_processor_quarantine_result(
            state_store,
            storage,
            candidate,
            parser_version,
            durable_stage="issuer",
            durable_reason_code="issuer_invalid",
            result_stage="issuer",
            result_reason_code="issuer_invalid",
            error=error,
        )
    return None


def process_insider_backfill_accession(
    evidence: InsiderBulkAccessionEvidence,
    *,
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    approved_issuer_ciks: Iterable[object],
    deadline: CooperativeDeadline,
    parser_version: str = INSIDER_PARSER_VERSION,
    http: object = pipeline.HTTP,
    monotonic: Callable[[], float] = time.monotonic,
) -> InsiderAccessionProcessResult:
    """Prepare quarterly evidence and enter the authoritative filing processor."""

    if not isinstance(storage, InsiderStorage):
        raise TypeError("storage must be an InsiderStorage")
    if not isinstance(state_store, InsiderStateStore):
        raise TypeError("state store must be an InsiderStateStore")
    if not isinstance(deadline, CooperativeDeadline):
        raise TypeError("deadline must be a CooperativeDeadline")
    if parser_version != INSIDER_PARSER_VERSION:
        raise _discovery_error("authoritative parser version")
    _validate_bulk_accession_evidence(evidence)
    approved_values = _normalize_approved_issuer_ciks(approved_issuer_ciks)
    if evidence.issuer_cik not in approved_values:
        raise _discovery_error("approved issuer scope")
    durable_approved = _durable_approved_issuer_ciks(state_store)
    if evidence.issuer_cik not in durable_approved:
        raise _discovery_error("approved issuer scope")
    if deadline.reached(monotonic):
        return _processor_result(
            evidence,
            parser_version,
            InsiderAccessionOutcome.CHECKPOINTED,
            "index",
            reason_code="deadline",
        )

    canonical_index_url = _bulk_accession_index_url(evidence)
    telemetry = _active_insider_telemetry()
    try:
        index_html = storage.read_index_html(evidence.accession_number)
        if telemetry is not None:
            telemetry.increment("index_cache_hits")
    except InsiderStorageError as error:
        if not _storage_artifact_is_missing(error):
            return _processor_result(
                evidence,
                parser_version,
                InsiderAccessionOutcome.QUARANTINED,
                "cache",
                error=error,
                reason_code="cache_invalid",
            )
        try:
            if telemetry is not None:
                telemetry.increment("index_fetches")
            index_html = _fetch_bounded_processor_artifact(
                http,
                canonical_index_url,
                max_bytes=MAX_INDEX_HTML_BYTES,
                deadline_monotonic=deadline.deadline_monotonic,
                monotonic=monotonic,
            )
            state_store.publish_if_issuer_approved(
                evidence.issuer_cik,
                lambda: storage.store_index_html(
                    evidence.accession_number,
                    index_html,
                ),
            )
        except InsiderApprovalScopeError:
            raise
        except (
            ImmutableInsiderStorageConflict,
            InsiderDiscoveryError,
            InsiderStorageError,
            TypeError,
            ValueError,
        ) as deterministic_error:
            return _processor_result(
                evidence,
                parser_version,
                InsiderAccessionOutcome.QUARANTINED,
                "index",
                error=deterministic_error,
                reason_code="index_invalid",
            )
        except (OSError, RuntimeError) as fetch_error:
            if pipeline.is_sec_deadline_reached(fetch_error):
                return _processor_result(
                    evidence,
                    parser_version,
                    InsiderAccessionOutcome.CHECKPOINTED,
                    "index",
                    reason_code="deadline",
                )
            return _processor_result(
                evidence,
                parser_version,
                InsiderAccessionOutcome.RETRY_LATER,
                "index",
                error=fetch_error,
                reason_code="fetch_failed",
            )
    try:
        index_metadata = parse_insider_filing_index(
            index_html,
            index_url=canonical_index_url,
            accession_number=evidence.accession_number,
            issuer_cik=evidence.issuer_cik,
            reporting_owner_ciks=evidence.reporting_owner_ciks,
        )
        if (
            index_metadata["form_type"] != evidence.form_type
            or index_metadata["filing_date"] != evidence.filing_date
            or index_metadata["issuer_cik"] != evidence.issuer_cik
            or index_metadata["index_url"] != canonical_index_url
        ):
            raise InsiderIndexParseError(
                "filing index does not match quarterly accession evidence"
            )
        accepted_at = index_metadata["accepted_at"]
        if type(accepted_at) is not str:
            raise InsiderIndexParseError("filing index acceptance timestamp is invalid")
        identity = InsiderAccessionIdentity(
            accession_number=evidence.accession_number,
            issuer_cik=evidence.issuer_cik,
            form_type=evidence.form_type,
            index_url=canonical_index_url,
            accepted_at=accepted_at,
            reporting_owner_ciks=evidence.reporting_owner_ciks,
        )
    except InsiderIndexParseError as error:
        return _processor_result(
            evidence,
            parser_version,
            InsiderAccessionOutcome.QUARANTINED,
            "index",
            error=error,
            reason_code="index_parse_invalid",
        )
    except (InsiderDiscoveryError, TypeError, ValueError) as error:
        raise _backfill_error("filing index evidence") from error

    return _process_insider_accession_identity(
        identity,
        storage=storage,
        state_store=state_store,
        approved_issuer_ciks=approved_values,
        deadline=deadline,
        parser_version=parser_version,
        http=http,
        monotonic=monotonic,
        prepared_index_html=index_html,
        prepared_index_metadata=index_metadata,
        checkpoint_is_completed=None,
        checkpoint_complete=None,
    )


def _process_insider_accession_identity(
    identity: InsiderAccessionIdentity,
    *,
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    approved_issuer_ciks: Iterable[object],
    deadline: CooperativeDeadline,
    parser_version: str = INSIDER_PARSER_VERSION,
    http: object = pipeline.HTTP,
    monotonic: Callable[[], float] = time.monotonic,
    prepared_index_html: bytes | None = None,
    prepared_index_metadata: dict[str, object] | None = None,
    checkpoint_is_completed: Callable[[], bool] | None = None,
    checkpoint_complete: Callable[[], object] | None = None,
) -> InsiderAccessionProcessResult:
    """Process one source-neutral filing identity in durable sequential order."""

    if not isinstance(storage, InsiderStorage):
        raise TypeError("storage must be an InsiderStorage")
    if not isinstance(state_store, InsiderStateStore):
        raise TypeError("state store must be an InsiderStateStore")
    if not isinstance(deadline, CooperativeDeadline):
        raise TypeError("deadline must be a CooperativeDeadline")
    if parser_version != INSIDER_PARSER_VERSION:
        raise _discovery_error("authoritative parser version")
    if (checkpoint_is_completed is None) != (checkpoint_complete is None):
        raise TypeError("processor checkpoint callbacks must be supplied together")
    if checkpoint_is_completed is not None and not callable(checkpoint_is_completed):
        raise TypeError("processor completion check must be callable")
    if checkpoint_complete is not None and not callable(checkpoint_complete):
        raise TypeError("processor completion callback must be callable")
    if (prepared_index_html is None) != (prepared_index_metadata is None):
        raise TypeError("prepared filing index bytes and metadata must be supplied together")
    if prepared_index_html is not None and type(prepared_index_html) is not bytes:
        raise TypeError("prepared filing index bytes must be bytes")
    if prepared_index_metadata is not None and type(prepared_index_metadata) is not dict:
        raise TypeError("prepared filing index metadata must be a dict")

    candidate = _validate_insider_accession_identity(identity)
    approved_values = _normalize_approved_issuer_ciks(approved_issuer_ciks)
    if candidate.issuer_cik not in approved_values:
        raise _discovery_error("approved issuer scope")
    canonical_index_url = candidate.index_url
    reporting_owner_ciks = candidate.reporting_owner_ciks
    durable_approved = _durable_approved_issuer_ciks(state_store)
    if candidate.issuer_cik not in durable_approved:
        raise _discovery_error("approved issuer scope")
    telemetry = _active_insider_telemetry()
    if deadline.reached(monotonic):
        return _processor_deadline_result(candidate, parser_version, "cache")

    try:
        cached = _verify_insider_identity_cache(
            candidate,
            storage=storage,
            parser_version=parser_version,
        )
    except InsiderStorageError as error:
        return _persist_processor_quarantine_result(
            state_store,
            storage,
            candidate,
            parser_version,
            durable_stage="cache",
            durable_reason_code="cache_invalid",
            result_stage="cache",
            result_reason_code="cache_invalid",
            error=error,
            source_hashes=(),
        )
    if cached is not None:
        if telemetry is not None:
            telemetry.increment("index_cache_hits")
            telemetry.increment("raw_cache_hits")
        if deadline.reached(monotonic):
            return _processor_deadline_result(candidate, parser_version, "issuer")
        issuer_result = _reconcile_issuer_or_result(
            storage=storage,
            state_store=state_store,
            candidate=candidate,
            parser_version=parser_version,
            normalized=cached,
        )
        if issuer_result is not None:
            return issuer_result
        if checkpoint_is_completed is None:
            return _processor_result(
                candidate,
                parser_version,
                InsiderAccessionOutcome.CACHE_HIT,
                "cache",
            )
        try:
            completed = checkpoint_is_completed()
            if type(completed) is not bool:
                raise InsiderStorageError("processor completion check is invalid")
            if completed:
                return _processor_result(
                    candidate,
                    parser_version,
                    InsiderAccessionOutcome.CACHE_HIT,
                    "cache",
                )
            if deadline.reached(monotonic):
                return _processor_deadline_result(
                    candidate,
                    parser_version,
                    "checkpoint",
                )
            assert checkpoint_complete is not None
            checkpoint_complete()
        except (FileNotFoundError, InsiderDiscoveryError, InsiderStorageError) as error:
            return _processor_result(
                candidate,
                parser_version,
                InsiderAccessionOutcome.CHECKPOINTED,
                "checkpoint",
                error=error,
                reason_code="checkpoint_failed",
            )
        return _processor_result(
            candidate,
            parser_version,
            InsiderAccessionOutcome.CHECKPOINTED,
            "checkpoint",
        )

    quarantine = _matching_deterministic_quarantine(
        state_store,
        storage,
        candidate,
        parser_version,
    )
    if quarantine is not None:
        return _processor_quarantine_replay_result(
            candidate,
            parser_version,
            quarantine,
        )

    if deadline.reached(monotonic):
        return _processor_deadline_result(candidate, parser_version, "index")
    if prepared_index_html is None:
        try:
            index_html = storage.read_index_html(candidate.accession_number)
            if telemetry is not None:
                telemetry.increment("index_cache_hits")
        except InsiderStorageError as error:
            if not _storage_artifact_is_missing(error):
                return _persist_processor_quarantine_result(
                    state_store,
                    storage,
                    candidate,
                    parser_version,
                    durable_stage="cache",
                    durable_reason_code="cache_invalid",
                    result_stage="cache",
                    result_reason_code="cache_invalid",
                    error=error,
                    source_hashes=(),
                )
            try:
                if telemetry is not None:
                    telemetry.increment("index_fetches")
                index_html = _fetch_bounded_processor_artifact(
                    http,
                    canonical_index_url,
                    max_bytes=MAX_INDEX_HTML_BYTES,
                    deadline_monotonic=deadline.deadline_monotonic,
                    monotonic=monotonic,
                )
                state_store.publish_if_issuer_approved(
                    candidate.issuer_cik,
                    lambda: storage.store_index_html(
                        candidate.accession_number,
                        index_html,
                    ),
                )
            except (OSError, RuntimeError) as fetch_error:
                if pipeline.is_sec_deadline_reached(fetch_error):
                    return _processor_deadline_result(
                        candidate,
                        parser_version,
                        "index",
                    )
                return _processor_result(
                    candidate,
                    parser_version,
                    InsiderAccessionOutcome.RETRY_LATER,
                    "index",
                    error=fetch_error,
                    reason_code="fetch_failed",
                )
            except InsiderApprovalScopeError:
                raise
            except (
                ImmutableInsiderStorageConflict,
                InsiderDiscoveryError,
                InsiderStorageError,
                TypeError,
                ValueError,
            ) as deterministic_error:
                return _persist_processor_quarantine_result(
                    state_store,
                    storage,
                    candidate,
                    parser_version,
                    durable_stage="discovery",
                    durable_reason_code="discovery_invalid",
                    result_stage="index",
                    result_reason_code="index_invalid",
                    error=deterministic_error,
                )
        try:
            index_metadata = parse_insider_filing_index(
                index_html,
                index_url=canonical_index_url,
                accession_number=candidate.accession_number,
                issuer_cik=candidate.issuer_cik,
                reporting_owner_ciks=reporting_owner_ciks,
            )
        except (InsiderDiscoveryError, InsiderIndexParseError, TypeError, ValueError) as error:
            return _persist_processor_quarantine_result(
                state_store,
                storage,
                candidate,
                parser_version,
                durable_stage="discovery",
                durable_reason_code="index_parse_invalid",
                result_stage="index",
                result_reason_code="index_parse_invalid",
                error=error,
            )
    else:
        assert prepared_index_metadata is not None
        index_html = prepared_index_html
        index_metadata = dict(prepared_index_metadata)

    try:
        document_url = _canonical_processor_sec_url(
            index_metadata["document_url"]
        )
        if (
            index_metadata["form_type"] != candidate.form_type
            or index_metadata["accepted_at"] != candidate.accepted_at
            or index_metadata["issuer_cik"] != candidate.issuer_cik
            or index_metadata["reporting_owner_ciks"]
            != list(candidate.reporting_owner_ciks)
            or index_metadata["index_url"] != canonical_index_url
        ):
            raise InsiderIndexParseError(
                "filing index does not match accession identity"
            )
    except (InsiderDiscoveryError, InsiderIndexParseError, KeyError, TypeError, ValueError) as error:
        return _persist_processor_quarantine_result(
            state_store,
            storage,
            candidate,
            parser_version,
            durable_stage="discovery",
            durable_reason_code="index_parse_invalid",
            result_stage="index",
            result_reason_code="index_parse_invalid",
            error=error,
        )

    if deadline.reached(monotonic):
        return _processor_deadline_result(candidate, parser_version, "raw")
    try:
        raw_xml = storage.read_raw(candidate.accession_number)
        if telemetry is not None:
            telemetry.increment("raw_cache_hits")
    except InsiderStorageError as error:
        if not _storage_artifact_is_missing(error):
            return _persist_processor_quarantine_result(
                state_store,
                storage,
                candidate,
                parser_version,
                durable_stage="cache",
                durable_reason_code="cache_invalid",
                result_stage="cache",
                result_reason_code="cache_invalid",
                error=error,
                source_hashes=(),
            )
        try:
            if telemetry is not None:
                telemetry.increment("raw_fetches")
            raw_xml = _fetch_bounded_processor_artifact(
                http,
                document_url,
                max_bytes=MAX_RAW_XML_BYTES,
                deadline_monotonic=deadline.deadline_monotonic,
                monotonic=monotonic,
            )
            state_store.publish_if_issuer_approved(
                candidate.issuer_cik,
                lambda: storage.store_raw(
                    candidate.accession_number,
                    raw_xml,
                ),
            )
        except (OSError, RuntimeError) as fetch_error:
            if pipeline.is_sec_deadline_reached(fetch_error):
                return _processor_deadline_result(
                    candidate,
                    parser_version,
                    "raw",
                )
            return _processor_result(
                candidate,
                parser_version,
                InsiderAccessionOutcome.RETRY_LATER,
                "raw",
                error=fetch_error,
                reason_code="fetch_failed",
            )
        except InsiderApprovalScopeError:
            raise
        except (
            ImmutableInsiderStorageConflict,
            InsiderDiscoveryError,
            InsiderStorageError,
            TypeError,
            ValueError,
        ) as deterministic_error:
            return _persist_processor_quarantine_result(
                state_store,
                storage,
                candidate,
                parser_version,
                durable_stage="raw",
                durable_reason_code="raw_invalid",
                result_stage="raw",
                result_reason_code="raw_invalid",
                error=deterministic_error,
            )
    if deadline.reached(monotonic):
        return _processor_deadline_result(candidate, parser_version, "parse")
    telemetry = _active_insider_telemetry()
    if telemetry is not None:
        telemetry.increment("parse_attempts")
    try:
        normalized = parse_ownership_xml(
            raw_xml,
            accession_number=candidate.accession_number,
            filing_date=index_metadata["filing_date"],
            accepted_at=index_metadata["accepted_at"],
            source_index_url=canonical_index_url,
            source_document_url=document_url,
        )
        if not _normalized_matches_candidate(
            normalized,
            candidate,
            canonical_index_url,
        ):
            raise InsiderParseError(
                "normalized filing does not match discovered accession"
            )
    except (InsiderParseError, TypeError, ValueError) as error:
        if telemetry is not None:
            telemetry.increment("parse_failures")
        return _persist_processor_quarantine_result(
            state_store,
            storage,
            candidate,
            parser_version,
            durable_stage="raw",
            durable_reason_code="raw_invalid",
            result_stage="raw",
            result_reason_code="raw_parse_invalid",
            error=error,
        )
    if telemetry is not None:
        telemetry.increment("parse_successes")
        telemetry.observe_normalized(normalized)

    if deadline.reached(monotonic):
        return _processor_deadline_result(candidate, parser_version, "source")
    try:
        try:
            storage.read_source_metadata(candidate.accession_number)
        except InsiderStorageError as source_error:
            if not _storage_artifact_is_missing(source_error):
                raise
            source_metadata = build_insider_source_metadata(
                index_metadata,
                index_html,
                raw_xml,
            )
            state_store.publish_if_issuer_approved(
                candidate.issuer_cik,
                lambda: storage.store_source_metadata(
                    candidate.accession_number,
                    source_metadata,
                ),
            )
        if deadline.reached(monotonic):
            return _processor_deadline_result(
                candidate,
                parser_version,
                "normalized",
            )
        state_store.publish_if_issuer_approved(
            candidate.issuer_cik,
            lambda: storage.store_normalized(
                candidate.accession_number,
                parser_version,
                normalized,
            ),
        )
        verified = _verify_insider_identity_cache(
            candidate,
            storage=storage,
            parser_version=parser_version,
        )
        if verified is None:
            raise InsiderStorageError(
                "normalized filing was not durable after publication"
            )
    except InsiderApprovalScopeError:
        raise
    except (
        ImmutableInsiderStorageConflict,
        InsiderDiscoveryError,
        InsiderStorageError,
        TypeError,
        ValueError,
    ) as error:
        return _persist_processor_quarantine_result(
            state_store,
            storage,
            candidate,
            parser_version,
            durable_stage="source",
            durable_reason_code="source_invalid",
            result_stage="source",
            result_reason_code="source_invalid",
            error=error,
        )

    if deadline.reached(monotonic):
        return _processor_deadline_result(candidate, parser_version, "issuer")
    issuer_result = _reconcile_issuer_or_result(
        storage=storage,
        state_store=state_store,
        candidate=candidate,
        parser_version=parser_version,
        normalized=verified,
    )
    if issuer_result is not None:
        return issuer_result

    if deadline.reached(monotonic):
        return _processor_deadline_result(candidate, parser_version, "checkpoint")
    if checkpoint_complete is not None:
        try:
            checkpoint_complete()
        except (InsiderDiscoveryError, InsiderStorageError) as error:
            return _processor_result(
                candidate,
                parser_version,
                InsiderAccessionOutcome.CHECKPOINTED,
                "checkpoint",
                error=error,
                reason_code="checkpoint_failed",
            )
    return _processor_result(
        candidate,
        parser_version,
        InsiderAccessionOutcome.CREATED,
        "checkpoint",
    )


class InsiderReparseError(ValueError):
    """Raised when an offline parser-version reprocessing request is invalid."""


class InsiderReparseOutcome(StrEnum):
    """Bounded outcomes for one offline reprocessing run."""

    COMPLETED = "completed"
    CHECKPOINTED = "checkpointed"
    QUARANTINED = "quarantined"


_REPARSE_STAGES = frozenset(
    {"source", "raw", "parse", "normalized", "issuer", "checkpoint"}
)
_REPARSE_ERROR_CLASSES = frozenset(
    {
        "ImmutableInsiderStorageConflict",
        "InsiderContractError",
        "InsiderDiscoveryError",
        "InsiderIndexParseError",
        "InsiderIssuerReductionError",
        "InsiderParseError",
        "InsiderStateRevisionError",
        "InsiderStorageError",
        "OSError",
        "RuntimeError",
        "TypeError",
        "ValueError",
    }
)


def _reparse_error(message: str) -> InsiderReparseError:
    return InsiderReparseError(f"offline insider reprocessing is invalid: {message}")


def _reparse_error_class(error: BaseException) -> str:
    if isinstance(error, ImmutableInsiderStorageConflict):
        result = "ImmutableInsiderStorageConflict"
    elif isinstance(error, InsiderStateRevisionError):
        result = "InsiderStateRevisionError"
    elif isinstance(error, InsiderIndexParseError):
        result = "InsiderIndexParseError"
    elif isinstance(error, InsiderIssuerReductionError):
        result = "InsiderIssuerReductionError"
    elif isinstance(error, InsiderParseError):
        result = "InsiderParseError"
    elif isinstance(error, InsiderContractError):
        result = "InsiderContractError"
    elif isinstance(error, InsiderDiscoveryError):
        result = "InsiderDiscoveryError"
    elif isinstance(error, InsiderStorageError):
        result = "InsiderStorageError"
    elif isinstance(error, OSError):
        result = "OSError"
    elif isinstance(error, RuntimeError):
        result = "RuntimeError"
    elif type(error) is TypeError:
        result = "TypeError"
    elif type(error) is ValueError:
        result = "ValueError"
    else:
        raise _reparse_error("error class")
    if result not in _REPARSE_ERROR_CLASSES:
        raise _reparse_error("error class")
    return result


@dataclass(frozen=True, slots=True)
class InsiderReparseAccessionResult:
    """Source-free telemetry for one offline reprocessing attempt."""

    accession_number: str
    issuer_cik: str
    form_type: str | None
    parser_version: str
    outcome: InsiderAccessionOutcome
    stage: str
    error_class: str | None = None
    retry: bool = False

    def __post_init__(self) -> None:
        try:
            canonical_issuer = normalize_section16_cik(self.issuer_cik)
        except (TypeError, ValueError) as error:
            raise _reparse_error("accession result") from error
        if (
            type(self.accession_number) is not str
            or _DISCOVERY_ACCESSION_RE.fullmatch(self.accession_number) is None
            or type(self.issuer_cik) is not str
            or canonical_issuer != self.issuer_cik
            or (
                self.form_type is not None
                and (
                    type(self.form_type) is not str
                    or self.form_type not in SECTION16_CURRENT_FORMS
                )
            )
            or type(self.parser_version) is not str
            or self.parser_version != INSIDER_PARSER_VERSION
            or type(self.outcome) is not InsiderAccessionOutcome
            or type(self.stage) is not str
            or self.stage not in _REPARSE_STAGES
            or type(self.retry) is not bool
            or (
                self.error_class is not None
                and (
                    type(self.error_class) is not str
                    or self.error_class not in _REPARSE_ERROR_CLASSES
                )
            )
            or (
                self.form_type is None
                and (self.stage != "source" or self.error_class is None)
            )
        ):
            raise _reparse_error("accession result")
        if self.outcome in {
            InsiderAccessionOutcome.CREATED,
            InsiderAccessionOutcome.CACHE_HIT,
        }:
            valid = (
                self.stage == "checkpoint"
                and self.error_class is None
                and not self.retry
            )
        elif self.outcome is InsiderAccessionOutcome.QUARANTINED:
            valid = self.error_class is not None and not self.retry
        elif self.outcome is InsiderAccessionOutcome.CHECKPOINTED:
            valid = (self.error_class is None and not self.retry) or (
                self.error_class is not None and self.retry
            )
        else:
            valid = False
        if not valid:
            raise _reparse_error("accession result")


@dataclass(frozen=True, slots=True)
class InsiderReparseRunResult:
    """Bounded aggregate outcome for one offline reprocessing run."""

    outcome: InsiderReparseOutcome
    parser_version: str
    scope: str
    scope_identifier: str | None
    queued_accessions: tuple[str, ...]
    completed_accessions: tuple[str, ...]
    accession_results: tuple[InsiderReparseAccessionResult, ...]

    def __post_init__(self) -> None:
        if (
            type(self.outcome) is not InsiderReparseOutcome
            or type(self.parser_version) is not str
            or self.parser_version != INSIDER_PARSER_VERSION
            or type(self.scope) is not str
            or self.scope not in {"accession", "issuer", "all"}
            or type(self.queued_accessions) is not tuple
            or type(self.completed_accessions) is not tuple
            or type(self.accession_results) is not tuple
            or len(self.queued_accessions) > MAX_INSIDER_STATE_COLLECTION
            or len(self.completed_accessions) > MAX_INSIDER_STATE_COLLECTION
            or len(self.accession_results) > MAX_INSIDER_STATE_COLLECTION
            or any(
                type(accession) is not str
                or _DISCOVERY_ACCESSION_RE.fullmatch(accession) is None
                for accession in self.queued_accessions
            )
            or any(
                type(accession) is not str
                or _DISCOVERY_ACCESSION_RE.fullmatch(accession) is None
                for accession in self.completed_accessions
            )
            or any(
                type(result) is not InsiderReparseAccessionResult
                for result in self.accession_results
            )
        ):
            raise _reparse_error("run result")
        if self.scope == "accession":
            valid_identifier = (
                type(self.scope_identifier) is str
                and _DISCOVERY_ACCESSION_RE.fullmatch(self.scope_identifier) is not None
            )
        elif self.scope == "issuer":
            try:
                canonical_identifier = normalize_section16_cik(self.scope_identifier)
            except (TypeError, ValueError):
                valid_identifier = False
            else:
                valid_identifier = canonical_identifier == self.scope_identifier
        else:
            valid_identifier = self.scope_identifier is None
        queued = self.queued_accessions
        completed = self.completed_accessions
        result_accessions = tuple(
            result.accession_number for result in self.accession_results
        )
        successful_accessions = {
            result.accession_number
            for result in self.accession_results
            if result.outcome
            in {
                InsiderAccessionOutcome.CREATED,
                InsiderAccessionOutcome.CACHE_HIT,
            }
        }
        unfinished_accessions = {
            result.accession_number
            for result in self.accession_results
            if result.outcome
            in {
                InsiderAccessionOutcome.CHECKPOINTED,
                InsiderAccessionOutcome.QUARANTINED,
            }
        }
        has_quarantine = any(
            result.outcome is InsiderAccessionOutcome.QUARANTINED
            for result in self.accession_results
        )
        if has_quarantine:
            expected_outcome = InsiderReparseOutcome.QUARANTINED
        elif completed == queued:
            expected_outcome = InsiderReparseOutcome.COMPLETED
        else:
            expected_outcome = InsiderReparseOutcome.CHECKPOINTED
        if (
            not valid_identifier
            or queued != tuple(sorted(set(queued)))
            or completed != tuple(sorted(set(completed)))
            or not set(completed) <= set(queued)
            or result_accessions != tuple(sorted(set(result_accessions)))
            or not set(result_accessions) <= set(queued)
            or not successful_accessions <= set(completed)
            or bool(unfinished_accessions & set(completed))
            or self.outcome is not expected_outcome
            or (
                self.scope == "accession"
                and queued != (self.scope_identifier,)
            )
            or (
                self.scope == "issuer"
                and any(
                    result.issuer_cik != self.scope_identifier
                    for result in self.accession_results
                )
            )
        ):
            raise _reparse_error("run result")


def _reparse_queue_entry(accession_number: object, issuer_cik: object) -> dict[str, str]:
    if (
        type(accession_number) is not str
        or _DISCOVERY_ACCESSION_RE.fullmatch(accession_number) is None
    ):
        raise _reparse_error("accession")
    try:
        issuer = normalize_section16_cik(issuer_cik)
    except (TypeError, ValueError) as error:
        raise _reparse_error("queue identity") from error
    if type(issuer_cik) is not str or issuer != issuer_cik:
        raise _reparse_error("queue identity")
    return {"accession_number": accession_number, "issuer_cik": issuer}


def _reparse_issuer_accessions(
    state_store: InsiderStateStore,
    issuer_cik: str,
) -> tuple[dict[str, str], ...]:
    try:
        issuer_state = state_store.read(f"issuers/{issuer_cik}")
    except FileNotFoundError as error:
        raise _reparse_error("issuer state") from error
    references = issuer_state.get("accessions")
    if not isinstance(references, list):
        raise _reparse_error("issuer state")
    entries: list[dict[str, str]] = []
    for reference in references:
        if not isinstance(reference, dict):
            raise _reparse_error("issuer state")
        entries.append(
            _reparse_queue_entry(reference.get("accession_number"), issuer_cik)
        )
    entries.sort(key=lambda entry: entry["accession_number"])
    if len({entry["accession_number"] for entry in entries}) != len(entries):
        raise _reparse_error("issuer state")
    return tuple(entries)


def _initial_reparse_queue(
    *,
    scope: str,
    scope_identifier: str | None,
    max_accessions: int | None,
    state_store: InsiderStateStore,
    approved_issuer_ciks: frozenset[str],
) -> tuple[tuple[dict[str, str], ...], int]:
    if scope == "accession":
        assert scope_identifier is not None
        matching_entries: list[dict[str, str]] = []
        for issuer_cik in sorted(approved_issuer_ciks):
            try:
                entries = _reparse_issuer_accessions(state_store, issuer_cik)
            except InsiderReparseError as error:
                if isinstance(error.__cause__, FileNotFoundError):
                    continue
                raise
            for entry in entries:
                if entry["accession_number"] == scope_identifier:
                    matching_entries.append(entry)
                    break
            if len(matching_entries) > 1:
                break
        if len(matching_entries) != 1:
            raise _reparse_error("approved accession scope")
        return (matching_entries[0],), 1
    if scope == "issuer":
        assert scope_identifier is not None
        if scope_identifier not in approved_issuer_ciks:
            raise _reparse_error("approved issuer scope")
        entries = _reparse_issuer_accessions(state_store, scope_identifier)
        return entries, max(1, len(entries))

    assert max_accessions is not None
    selected_entries: list[dict[str, str]] = []
    for issuer_cik in sorted(approved_issuer_ciks):
        try:
            entries = _reparse_issuer_accessions(state_store, issuer_cik)
        except InsiderReparseError as error:
            if isinstance(error.__cause__, FileNotFoundError):
                continue
            raise
        selected_entries.extend(entries)
        selected_entries.sort(
            key=lambda entry: (entry["accession_number"], entry["issuer_cik"])
        )
        del selected_entries[max_accessions:]
        del entries
    return tuple(selected_entries), max_accessions


def _reparse_scope(
    scope: object,
    scope_identifier: object,
    max_accessions: object,
) -> tuple[str, str | None, int | None]:
    if type(scope) is not str or scope not in {"accession", "issuer", "all"}:
        raise _reparse_error("scope")
    if scope == "accession":
        if (
            type(scope_identifier) is not str
            or _DISCOVERY_ACCESSION_RE.fullmatch(scope_identifier) is None
            or max_accessions is not None
        ):
            raise _reparse_error("accession scope")
        return scope, scope_identifier, None
    if scope == "issuer":
        if max_accessions is not None:
            raise _reparse_error("issuer scope")
        try:
            issuer = normalize_section16_cik(scope_identifier)
        except (TypeError, ValueError) as error:
            raise _reparse_error("issuer scope") from error
        return scope, issuer, None
    if (
        scope_identifier is not None
        or type(max_accessions) is not int
        or not 1 <= max_accessions <= MAX_INSIDER_STATE_COLLECTION
    ):
        raise _reparse_error("all scope")
    return scope, None, max_accessions


def _reparse_state_queue(
    checkpoint: dict[str, object],
) -> tuple[dict[str, str], ...]:
    queue = checkpoint.get("queue")
    if not isinstance(queue, list):
        raise _reparse_error("checkpoint queue")
    result = tuple(
        _reparse_queue_entry(
            entry.get("accession_number") if isinstance(entry, dict) else None,
            entry.get("issuer_cik") if isinstance(entry, dict) else None,
        )
        for entry in queue
    )
    if result != tuple(
        sorted(result, key=lambda entry: (entry["accession_number"], entry["issuer_cik"]))
    ):
        raise _reparse_error("checkpoint queue")
    return result


def _reparse_checkpoint_payload(
    *,
    parser_version: str,
    scope: str,
    scope_identifier: str | None,
    maximum: int,
    queue: tuple[dict[str, str], ...],
) -> dict[str, object]:
    return {
        "contract_version": REPARSE_STATE_CONTRACT_VERSION,
        "status": "running" if queue else "completed",
        "parser_version": parser_version,
        "scope": scope,
        "scope_identifier": scope_identifier,
        "max_accessions": maximum,
        "queue": [dict(entry) for entry in queue],
        "completed_accessions": [],
    }


def _assert_reparse_checkpoint_identity(
    checkpoint: dict[str, object],
    *,
    parser_version: str,
    scope: str,
    scope_identifier: str | None,
    queue: tuple[dict[str, str], ...],
) -> None:
    if (
        checkpoint.get("contract_version") != REPARSE_STATE_CONTRACT_VERSION
        or checkpoint.get("parser_version") != parser_version
        or checkpoint.get("scope") != scope
        or checkpoint.get("scope_identifier") != scope_identifier
        or _reparse_state_queue(checkpoint) != queue
    ):
        raise _reparse_error("checkpoint identity")


def _update_reparse_status(
    state_store: InsiderStateStore,
    *,
    parser_version: str,
    scope: str,
    scope_identifier: str | None,
    queue: tuple[dict[str, str], ...],
    status: str,
    completed_accession: str | None = None,
) -> dict[str, object]:
    def transform(current: dict[str, object]) -> dict[str, object]:
        _assert_reparse_checkpoint_identity(
            current,
            parser_version=parser_version,
            scope=scope,
            scope_identifier=scope_identifier,
            queue=queue,
        )
        completed_value = current.get("completed_accessions")
        if not isinstance(completed_value, list) or any(
            type(value) is not str for value in completed_value
        ):
            raise _reparse_error("completion checkpoint")
        completed = set(completed_value)
        if completed_accession is not None:
            if completed_accession not in {
                entry["accession_number"] for entry in queue
            }:
                raise _reparse_error("completion checkpoint")
            completed.add(completed_accession)
        completed_list = sorted(completed)
        next_status = status
        if status == "running" and len(completed_list) == len(queue):
            next_status = "completed"
        return {
            **current,
            "status": next_status,
            "completed_accessions": completed_list,
        }

    return state_store.update_reparse_if_issuers_approved(transform)


def _reparse_identity_from_source(
    entry: dict[str, str],
    source: object,
) -> InsiderAccessionIdentity:
    if not isinstance(source, dict):
        raise InsiderStorageError("stored reparse source metadata is invalid")
    index = source.get("index")
    owners = source.get("reporting_owner_ciks")
    if (
        source.get("accession_number") != entry["accession_number"]
        or source.get("issuer_cik") != entry["issuer_cik"]
        or type(source.get("form_type")) is not str
        or type(source.get("accepted_at")) is not str
        or not isinstance(index, dict)
        or type(index.get("url")) is not str
        or not isinstance(owners, list)
        or any(type(owner) is not str for owner in owners)
    ):
        raise InsiderStorageError("stored reparse source metadata is invalid")
    try:
        return InsiderAccessionIdentity(
            accession_number=entry["accession_number"],
            issuer_cik=entry["issuer_cik"],
            form_type=source["form_type"],
            index_url=index["url"],
            accepted_at=source["accepted_at"],
            reporting_owner_ciks=tuple(owners),
        )
    except (InsiderDiscoveryError, TypeError, ValueError) as error:
        raise InsiderStorageError(
            "stored reparse source metadata is invalid"
        ) from error


def _reparse_document_url(source: dict[str, object]) -> str:
    document = source.get("document")
    if not isinstance(document, dict) or type(document.get("url")) is not str:
        raise InsiderStorageError("stored reparse source metadata is invalid")
    try:
        return _canonical_processor_sec_url(document["url"])
    except InsiderDiscoveryError as error:
        raise InsiderStorageError(
            "stored reparse source metadata is invalid"
        ) from error


def _reparse_result(
    identity: InsiderAccessionIdentity,
    *,
    parser_version: str,
    outcome: InsiderAccessionOutcome,
    stage: str,
    error: BaseException | None = None,
    retry: bool = False,
) -> InsiderReparseAccessionResult:
    result = InsiderReparseAccessionResult(
        accession_number=identity.accession_number,
        issuer_cik=identity.issuer_cik,
        form_type=identity.form_type,
        parser_version=parser_version,
        outcome=outcome,
        stage=stage,
        error_class=_reparse_error_class(error) if error is not None else None,
        retry=retry,
    )
    telemetry = _active_insider_telemetry()
    if telemetry is not None:
        telemetry.observe_reparse_result(result)
    return result


def _reparse_result_without_source_identity(
    entry: dict[str, str],
    *,
    parser_version: str,
    outcome: InsiderAccessionOutcome,
    stage: str,
    error: BaseException,
    retry: bool,
) -> InsiderReparseAccessionResult:
    result = InsiderReparseAccessionResult(
        accession_number=entry["accession_number"],
        issuer_cik=entry["issuer_cik"],
        form_type=None,
        parser_version=parser_version,
        outcome=outcome,
        stage=stage,
        error_class=_reparse_error_class(error),
        retry=retry,
    )
    telemetry = _active_insider_telemetry()
    if telemetry is not None:
        telemetry.observe_reparse_result(result)
    return result


def _run_reparse_accession(
    entry: dict[str, str],
    *,
    parser_version: str,
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    deadline: CooperativeDeadline,
    monotonic: Callable[[], float],
) -> InsiderReparseAccessionResult:
    stage = "source"
    identity: InsiderAccessionIdentity | None = None
    try:
        source = storage.read_source_metadata(entry["accession_number"])
        identity = _reparse_identity_from_source(entry, source)
        if deadline.reached(monotonic):
            return _reparse_result(
                identity,
                parser_version=parser_version,
                outcome=InsiderAccessionOutcome.CHECKPOINTED,
                stage="source",
            )

        stage = "raw"
        raw_xml = storage.read_raw(identity.accession_number)
        document = source.get("document")
        if (
            not isinstance(document, dict)
            or document.get("byte_count") != len(raw_xml)
            or document.get("sha256") != hashlib.sha256(raw_xml).hexdigest()
        ):
            raise InsiderStorageError("stored reparse raw source is invalid")
        if deadline.reached(monotonic):
            return _reparse_result(
                identity,
                parser_version=parser_version,
                outcome=InsiderAccessionOutcome.CHECKPOINTED,
                stage="raw",
            )

        stage = "parse"
        filing_date = source.get("filing_date")
        if type(filing_date) is not str:
            raise InsiderStorageError("stored reparse source metadata is invalid")
        telemetry = _active_insider_telemetry()
        if telemetry is not None:
            telemetry.increment("parse_attempts")
        try:
            normalized = parse_ownership_xml(
                raw_xml,
                accession_number=identity.accession_number,
                filing_date=filing_date,
                accepted_at=identity.accepted_at,
                source_index_url=identity.index_url,
                source_document_url=_reparse_document_url(source),
            )
            normalized = validate_insider_filing(normalized)
            if (
                normalized.get("parser_version") != parser_version
                or not _normalized_matches_candidate(
                    normalized,
                    identity,
                    identity.index_url,
                )
            ):
                raise InsiderParseError(
                    "reprocessed filing does not match stored source identity"
                )
        except BaseException as error:
            if pipeline.is_control_flow_exception(error):
                raise
            if telemetry is not None:
                telemetry.increment("parse_failures")
            raise
        if telemetry is not None:
            telemetry.increment("parse_successes")
            telemetry.observe_normalized(normalized)
        rendered = canonical_insider_json_bytes(normalized)
        if deadline.reached(monotonic):
            return _reparse_result(
                identity,
                parser_version=parser_version,
                outcome=InsiderAccessionOutcome.CHECKPOINTED,
                stage="parse",
            )

        stage = "normalized"
        cache_hit = False
        try:
            existing = storage.read_normalized(
                identity.accession_number,
                parser_version,
            )
        except InsiderStorageError as error:
            if not _storage_artifact_is_missing(error):
                raise
            identity_accession = identity.accession_number
            identity_issuer = identity.issuer_cik
            try:
                state_store.publish_if_issuer_approved(
                    identity_issuer,
                    lambda: storage.store_normalized(
                        identity_accession,
                        parser_version,
                        normalized,
                    ),
                )
            except ImmutableInsiderStorageConflict:
                existing = storage.read_normalized(
                    identity.accession_number,
                    parser_version,
                )
                if canonical_insider_json_bytes(existing) != rendered:
                    raise
                cache_hit = True
        else:
            if canonical_insider_json_bytes(existing) != rendered:
                raise ImmutableInsiderStorageConflict(
                    "normalized filing already exists with different bytes"
                )
            cache_hit = True
        stored = storage.read_normalized(identity.accession_number, parser_version)
        if canonical_insider_json_bytes(stored) != rendered:
            raise InsiderStorageError(
                "stored reprocessed normalized filing is inconsistent"
            )
        if deadline.reached(monotonic):
            return _reparse_result(
                identity,
                parser_version=parser_version,
                outcome=InsiderAccessionOutcome.CHECKPOINTED,
                stage="normalized",
            )

        stage = "issuer"
        _reconcile_issuer_state(
            storage=storage,
            state_store=state_store,
            candidate=identity,
            parser_version=parser_version,
            normalized=stored,
        )
        if deadline.reached(monotonic):
            return _reparse_result(
                identity,
                parser_version=parser_version,
                outcome=InsiderAccessionOutcome.CHECKPOINTED,
                stage="issuer",
            )
        return _reparse_result(
            identity,
            parser_version=parser_version,
            outcome=(
                InsiderAccessionOutcome.CACHE_HIT
                if cache_hit
                else InsiderAccessionOutcome.CREATED
            ),
            stage="checkpoint",
        )
    except InsiderApprovalScopeError:
        raise
    except InsiderStateRevisionError as error:
        if identity is None:
            return _reparse_result_without_source_identity(
                entry,
                parser_version=parser_version,
                outcome=InsiderAccessionOutcome.CHECKPOINTED,
                stage=stage,
                error=error,
                retry=True,
            )
        return _reparse_result(
            identity,
            parser_version=parser_version,
            outcome=InsiderAccessionOutcome.CHECKPOINTED,
            stage=stage,
            error=error,
            retry=True,
        )
    except (OSError, RuntimeError) as error:
        if identity is None:
            return _reparse_result_without_source_identity(
                entry,
                parser_version=parser_version,
                outcome=InsiderAccessionOutcome.CHECKPOINTED,
                stage=stage,
                error=error,
                retry=True,
            )
        return _reparse_result(
            identity,
            parser_version=parser_version,
            outcome=InsiderAccessionOutcome.CHECKPOINTED,
            stage=stage,
            error=error,
            retry=True,
        )
    except (
        ImmutableInsiderStorageConflict,
        InsiderContractError,
        InsiderDiscoveryError,
        InsiderIndexParseError,
        InsiderIssuerReductionError,
        InsiderParseError,
        InsiderStorageError,
        TypeError,
        ValueError,
    ) as error:
        if identity is None:
            return _reparse_result_without_source_identity(
                entry,
                parser_version=parser_version,
                outcome=InsiderAccessionOutcome.QUARANTINED,
                stage=stage,
                error=error,
                retry=False,
            )
        return _reparse_result(
            identity,
            parser_version=parser_version,
            outcome=InsiderAccessionOutcome.QUARANTINED,
            stage=stage,
            error=error,
        )


def run_insider_reparse(
    *,
    scope: object,
    scope_identifier: object,
    max_accessions: object,
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    deadline: CooperativeDeadline,
    parser_version: str = INSIDER_PARSER_VERSION,
    resume: bool = False,
    monotonic: Callable[[], float] = time.monotonic,
) -> InsiderReparseRunResult:
    """Reprocess verified stored insider sources without any network access."""

    if not isinstance(storage, InsiderStorage):
        raise TypeError("storage must be an InsiderStorage")
    if not isinstance(state_store, InsiderStateStore):
        raise TypeError("state store must be an InsiderStateStore")
    if not isinstance(deadline, CooperativeDeadline):
        raise TypeError("deadline must be a CooperativeDeadline")
    if type(resume) is not bool:
        raise _reparse_error("resume mode")
    if type(parser_version) is not str or parser_version != INSIDER_PARSER_VERSION:
        raise _reparse_error("parser version")
    canonical_scope, canonical_identifier, canonical_maximum = _reparse_scope(
        scope,
        scope_identifier,
        max_accessions,
    )
    try:
        approved = _durable_approved_issuer_ciks(state_store)
    except (InsiderDiscoveryError, InsiderStorageError) as error:
        raise _reparse_error("approved issuer state") from error

    existing: dict[str, object] | None
    try:
        existing = state_store.read("reparse-v1")
    except FileNotFoundError:
        existing = None
    if resume:
        if existing is None:
            raise _reparse_error("resume checkpoint")
        queue = _reparse_state_queue(existing)
        if (
            existing.get("parser_version") != parser_version
            or existing.get("scope") != canonical_scope
            or existing.get("scope_identifier") != canonical_identifier
        ):
            raise _reparse_error("resume checkpoint")
        persisted_maximum = existing.get("max_accessions")
        if type(persisted_maximum) is not int:
            raise _reparse_error("resume checkpoint")
        if (
            canonical_scope == "all"
            and canonical_maximum is not None
            and canonical_maximum < len(queue)
        ):
            raise _reparse_error("resume accession bound")
        maximum = persisted_maximum
        checkpoint = _update_reparse_status(
            state_store,
            parser_version=parser_version,
            scope=canonical_scope,
            scope_identifier=canonical_identifier,
            queue=queue,
            status="running",
        )
    else:
        queue, maximum = _initial_reparse_queue(
            scope=canonical_scope,
            scope_identifier=canonical_identifier,
            max_accessions=canonical_maximum,
            state_store=state_store,
            approved_issuer_ciks=approved,
        )
        candidate = _reparse_checkpoint_payload(
            parser_version=parser_version,
            scope=canonical_scope,
            scope_identifier=canonical_identifier,
            maximum=maximum,
            queue=queue,
        )
        expected_sha256: str | None = None
        if existing is not None:
            completed = existing.get("completed_accessions")
            existing_queue = _reparse_state_queue(existing)
            if (
                existing.get("parser_version") == parser_version
                and (
                    existing.get("status") != "completed"
                    or not isinstance(completed, list)
                    or set(completed)
                    != {entry["accession_number"] for entry in existing_queue}
                )
            ):
                raise _reparse_error("checkpoint already exists")
            expected_sha256 = hashlib.sha256(
                canonical_insider_state_json_bytes(existing)
            ).hexdigest()
        state_store.write_reparse_if_issuers_approved(
            candidate,
            expected_sha256=expected_sha256,
        )
        checkpoint = state_store.read("reparse-v1")

    _assert_reparse_checkpoint_identity(
        checkpoint,
        parser_version=parser_version,
        scope=canonical_scope,
        scope_identifier=canonical_identifier,
        queue=queue,
    )
    completed_value = checkpoint.get("completed_accessions")
    if not isinstance(completed_value, list) or any(
        type(value) is not str for value in completed_value
    ):
        raise _reparse_error("completion checkpoint")
    completed = set(completed_value)
    results: list[InsiderReparseAccessionResult] = []

    for entry in queue:
        accession_number = entry["accession_number"]
        if accession_number in completed:
            continue
        if deadline.reached(monotonic):
            return InsiderReparseRunResult(
                outcome=InsiderReparseOutcome.CHECKPOINTED,
                parser_version=parser_version,
                scope=canonical_scope,
                scope_identifier=canonical_identifier,
                queued_accessions=tuple(item["accession_number"] for item in queue),
                completed_accessions=tuple(sorted(completed)),
                accession_results=tuple(results),
            )
        result = _run_reparse_accession(
            entry,
            parser_version=parser_version,
            storage=storage,
            state_store=state_store,
            deadline=deadline,
            monotonic=monotonic,
        )
        results.append(result)
        if result.outcome is InsiderAccessionOutcome.QUARANTINED:
            _update_reparse_status(
                state_store,
                parser_version=parser_version,
                scope=canonical_scope,
                scope_identifier=canonical_identifier,
                queue=queue,
                status="quarantined",
            )
            return InsiderReparseRunResult(
                outcome=InsiderReparseOutcome.QUARANTINED,
                parser_version=parser_version,
                scope=canonical_scope,
                scope_identifier=canonical_identifier,
                queued_accessions=tuple(item["accession_number"] for item in queue),
                completed_accessions=tuple(sorted(completed)),
                accession_results=tuple(results),
            )
        if result.outcome is InsiderAccessionOutcome.CHECKPOINTED:
            return InsiderReparseRunResult(
                outcome=InsiderReparseOutcome.CHECKPOINTED,
                parser_version=parser_version,
                scope=canonical_scope,
                scope_identifier=canonical_identifier,
                queued_accessions=tuple(item["accession_number"] for item in queue),
                completed_accessions=tuple(sorted(completed)),
                accession_results=tuple(results),
            )
        telemetry = _active_insider_telemetry()
        try:
            checkpoint = _update_reparse_status(
                state_store,
                parser_version=parser_version,
                scope=canonical_scope,
                scope_identifier=canonical_identifier,
                queue=queue,
                status="running",
                completed_accession=accession_number,
            )
        except (FileNotFoundError, InsiderStorageError):
            if telemetry is not None:
                telemetry.increment("checkpoint_failures")
            raise
        if telemetry is not None:
            telemetry.increment("checkpoint_writes")
        checkpoint_completed = checkpoint.get("completed_accessions")
        if not isinstance(checkpoint_completed, list) or any(
            type(value) is not str for value in checkpoint_completed
        ):
            raise _reparse_error("completion checkpoint")
        completed = set(checkpoint_completed)

    return InsiderReparseRunResult(
        outcome=InsiderReparseOutcome.COMPLETED,
        parser_version=parser_version,
        scope=canonical_scope,
        scope_identifier=canonical_identifier,
        queued_accessions=tuple(item["accession_number"] for item in queue),
        completed_accessions=tuple(sorted(completed)),
        accession_results=tuple(results),
    )


def process_insider_accession(
    discovered: DiscoveredInsiderAccession,
    *,
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    approved_issuer_ciks: Iterable[object],
    deadline: CooperativeDeadline,
    parser_version: str = INSIDER_PARSER_VERSION,
    http: object = pipeline.HTTP,
    monotonic: Callable[[], float] = time.monotonic,
) -> InsiderAccessionProcessResult:
    """Process one approved incremental filing through the shared authority."""

    if not isinstance(storage, InsiderStorage):
        raise TypeError("storage must be an InsiderStorage")
    if not isinstance(state_store, InsiderStateStore):
        raise TypeError("state store must be an InsiderStateStore")
    if not isinstance(deadline, CooperativeDeadline):
        raise TypeError("deadline must be a CooperativeDeadline")
    if parser_version != INSIDER_PARSER_VERSION:
        raise _discovery_error("authoritative parser version")
    approved_values = _normalize_approved_issuer_ciks(approved_issuer_ciks)
    candidate, canonical_index_url, reporting_owner_ciks = (
        _validated_processor_candidate(discovered, approved_values)
    )
    identity = InsiderAccessionIdentity(
        accession_number=candidate.accession_number,
        issuer_cik=candidate.issuer_cik,
        form_type=candidate.form_type,
        index_url=canonical_index_url,
        accepted_at=candidate.accepted_at,
        reporting_owner_ciks=reporting_owner_ciks,
    )

    def is_completed() -> bool:
        state = state_store.read("incremental-v1")
        return _incremental_accession_is_completed(state, candidate)

    def complete() -> object:
        return _checkpoint_incremental_accession(state_store, candidate)

    return _process_insider_accession_identity(
        identity,
        storage=storage,
        state_store=state_store,
        approved_issuer_ciks=approved_values,
        deadline=deadline,
        parser_version=parser_version,
        http=http,
        monotonic=monotonic,
        checkpoint_is_completed=is_completed,
        checkpoint_complete=complete,
    )


__all__ = [
    "APPROVED_ISSUERS_STATE_CONTRACT_VERSION",
    "BACKFILL_STATE_CONTRACT_VERSION",
    "CURRENT_FILINGS_URL",
    "INSIDER_BULK_CATALOG_URL",
    "INCREMENTAL_STATE_CONTRACT_VERSION",
    "INSIDER_SOURCE_METADATA_VERSION",
    "ISSUER_STATE_CONTRACT_VERSION",
    "MAX_INDEX_FIELD_CHARS",
    "MAX_INDEX_HTML_BYTES",
    "MAX_INDEX_HTML_ELEMENTS",
    "MAX_INDEX_TABLE_ROWS",
    "MAX_INSIDER_BULK_SELECTED_ACCESSIONS",
    "MAX_RECENT_INSIDER_ATOM_BYTES",
    "MIN_INSIDER_BULK_QUARTER",
    "MAX_INSIDER_STATE_BYTES",
    "MAX_INSIDER_STATE_COLLECTION",
    "MAX_INSIDER_STATE_INTEGER",
    "MAX_INSIDER_STATE_STRING_CHARS",
    "MAX_TELEMETRY_ACCESSION_EXAMPLES",
    "MAX_TELEMETRY_RECENT_RUNS",
    "PRIVATE_INSIDER_STATE_ROOT",
    "QUARANTINE_STATE_CONTRACT_VERSION",
    "REPARSE_STATE_CONTRACT_VERSION",
    "TELEMETRY_STATE_CONTRACT_VERSION",
    "SECTION16_CURRENT_FORMS",
    "CooperativeDeadline",
    "DiscoveredInsiderAccession",
    "IncrementalDiscoveryResult",
    "InsiderAccessionOutcome",
    "InsiderAccessionIdentity",
    "InsiderAccessionProcessResult",
    "InsiderBackfillError",
    "InsiderBackfillOutcome",
    "InsiderBackfillRunResult",
    "InsiderBulkAccessionEvidence",
    "InsiderBulkArchiveResult",
    "InsiderBulkCatalogEntry",
    "InsiderBulkSourceIdentity",
    "InsiderBulkSourceRevisionError",
    "InsiderBulkTableEvidence",
    "InsiderDiscoveryError",
    "InsiderIndexParseError",
    "InsiderIssuerReductionError",
    "InsiderReparseAccessionResult",
    "InsiderReparseError",
    "InsiderReparseOutcome",
    "InsiderReparseRunResult",
    "InsiderStateStore",
    "InsiderTelemetryRun",
    "IssuerReductionResult",
    "NormalizedIssuerRecord",
    "RecentInsiderFeedEntry",
    "build_insider_source_metadata",
    "build_recent_insider_feed_url",
    "canonical_source_metadata_json_bytes",
    "canonical_insider_state_json_bytes",
    "discover_recent_insider_accessions",
    "fetch_insider_bulk_archive",
    "fetch_insider_bulk_catalog",
    "group_recent_insider_entries",
    "issuer_record_from_normalized",
    "insider_telemetry_run",
    "new_insider_telemetry_run_id",
    "pending_incremental_candidates",
    "parse_recent_insider_atom",
    "parse_insider_filing_index",
    "persist_incremental_discovery_queue",
    "process_insider_accession",
    "process_insider_backfill_accession",
    "reduce_issuer_state",
    "run_insider_backfill",
    "run_insider_reparse",
    "validate_insider_source_metadata",
    "verify_insider_accession_cache",
]
