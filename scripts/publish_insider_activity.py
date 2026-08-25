#!/usr/bin/env python3
"""Materialize the approved public insider corpus from verified private state."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
import sys
from typing import NoReturn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pipeline  # noqa: E402
from insider_contract import canonical_insider_json_bytes  # noqa: E402
from insider_parser import INSIDER_PARSER_VERSION  # noqa: E402
from insider_pipeline import (  # noqa: E402
    issuer_record_from_normalized,
    reduce_issuer_state,
    validate_incremental_checkpoint_scope,
)
from insider_publication import (  # noqa: E402
    MAX_PUBLIC_FILING_FILES,
    MAX_PUBLIC_TOTAL_BYTES,
    build_insider_publication,
    combine_insider_publications,
    write_insider_publication,
)
from insider_storage import (  # noqa: E402
    InsiderStateStore,
    InsiderStorage,
    InsiderStorageError,
)
from security_identity import normalize_section16_cik  # noqa: E402


_MAINTENANCE_MODES = frozenset({"incremental", "backfill", "reparse"})
_QUARTER_RE = re.compile(r"[0-9]{4}Q[1-4]")
_UTC_TIMESTAMP_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
)
_MAX_MAINTENANCE_ACCESSIONS = 100
_MAX_MATERIALIZATION_ACCESSIONS = MAX_PUBLIC_FILING_FILES
_MAX_MATERIALIZATION_NORMALIZED_BYTES = MAX_PUBLIC_TOTAL_BYTES


class InsiderPublicationMaterializationError(ValueError):
    """Raised when verified private state cannot produce one safe public corpus."""


class _ConfigurationError(ValueError):
    """Raised when the fixed production command receives an invalid scope."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        raise _ConfigurationError("command-line arguments are invalid") from None


@dataclass
class _MaterializationBudget:
    accessions: int = 0
    normalized_bytes: int = 0

    def admit_accessions(self, count: int) -> None:
        if self.accessions + count > _MAX_MATERIALIZATION_ACCESSIONS:
            raise _fail("normalized accession budget")
        self.accessions += count

    def admit_normalized(self, normalized: object) -> None:
        self.normalized_bytes += len(canonical_insider_json_bytes(normalized))
        if self.normalized_bytes > _MAX_MATERIALIZATION_NORMALIZED_BYTES:
            raise _fail("normalized byte budget")


def _fail(label: str) -> InsiderPublicationMaterializationError:
    return InsiderPublicationMaterializationError(
        f"insider publication materialization is invalid: {label}"
    )


def _canonical_timestamp(value: object, label: str) -> str:
    if type(value) is not str or _UTC_TIMESTAMP_RE.fullmatch(value) is None:
        raise _fail(label)
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise _fail(label) from error
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise _fail(label)
    return value


def _canonical_maintenance_configuration(
    *,
    maintenance_mode: object,
    maintenance_issuer_cik: object,
    maintenance_quarter: object,
    maintenance_max_accessions: object,
    as_of: object,
    latest_successful_sync_at: object,
) -> tuple[str, str, str | None, int, str, str | None]:
    if type(maintenance_mode) is not str or maintenance_mode not in _MAINTENANCE_MODES:
        raise _fail("maintenance mode")
    try:
        issuer_cik = normalize_section16_cik(maintenance_issuer_cik)
    except (TypeError, ValueError) as error:
        raise _fail("maintenance issuer CIK") from error
    if type(maintenance_issuer_cik) is not str or issuer_cik != maintenance_issuer_cik:
        raise _fail("maintenance issuer CIK")
    if (
        type(maintenance_max_accessions) is not int
        or type(maintenance_max_accessions) is bool
        or not 1 <= maintenance_max_accessions <= _MAX_MAINTENANCE_ACCESSIONS
    ):
        raise _fail("maintenance accession bound")
    if maintenance_mode == "backfill":
        if (
            type(maintenance_quarter) is not str
            or _QUARTER_RE.fullmatch(maintenance_quarter) is None
            or int(maintenance_quarter[:4]) < 2006
        ):
            raise _fail("maintenance quarter")
        quarter: str | None = maintenance_quarter
    else:
        if maintenance_quarter is not None:
            raise _fail("maintenance quarter")
        quarter = None
    canonical_as_of = _canonical_timestamp(as_of, "asOf")
    if latest_successful_sync_at is None:
        canonical_sync_at = None
    else:
        canonical_sync_at = _canonical_timestamp(
            latest_successful_sync_at,
            "latest successful sync timestamp",
        )
        if canonical_sync_at > canonical_as_of:
            raise _fail("latest successful sync timestamp after asOf")
    return (
        maintenance_mode,
        issuer_cik,
        quarter,
        maintenance_max_accessions,
        canonical_as_of,
        canonical_sync_at,
    )


def _require_completed_maintenance_checkpoint(
    state_store: InsiderStateStore,
    *,
    mode: str,
    issuer_cik: str,
    quarter: str | None,
    maximum: int,
) -> None:
    try:
        if mode == "incremental":
            checkpoint = state_store.read("incremental-v1")
            validate_incremental_checkpoint_scope(
                checkpoint,
                issuer_ciks=(issuer_cik,),
                max_accessions=maximum,
            )
            queue = checkpoint["queue"]
            # Incremental v1 binds issuer scope only through queue entries.  An
            # empty completed checkpoint is a valid ingestion no-op, but it has
            # no durable issuer identity and therefore cannot authorize public
            # materialization.
            if (
                checkpoint["status"] != "completed"
                or not isinstance(queue, list)
                or not queue
            ):
                raise _fail("maintenance checkpoint is not completed and exact")
            return

        if mode == "backfill":
            assert quarter is not None
            checkpoint = state_store.read(f"backfill/{quarter}")
            selected = checkpoint["selected_accessions"]
            completed = checkpoint["completed_accessions"]
            if (
                checkpoint["status"] != "completed"
                or checkpoint["quarter"] != quarter
                or checkpoint["issuer_cik"] != issuer_cik
                or not isinstance(selected, list)
                or not isinstance(completed, list)
                or len(selected) > maximum
                or selected != completed
            ):
                raise _fail("maintenance checkpoint is not completed and exact")
            return

        checkpoint = state_store.read("reparse-v1")
        queue = checkpoint["queue"]
        completed = checkpoint["completed_accessions"]
        if not isinstance(queue, list) or not isinstance(completed, list):
            raise _fail("maintenance checkpoint is not completed and exact")
        queue_accessions = [
            entry["accession_number"] for entry in queue if isinstance(entry, dict)
        ]
        if (
            checkpoint["status"] != "completed"
            or checkpoint["parser_version"] != INSIDER_PARSER_VERSION
            or checkpoint["scope"] != "issuer"
            or checkpoint["scope_identifier"] != issuer_cik
            or type(checkpoint["max_accessions"]) is not int
            or checkpoint["max_accessions"] > maximum
            or len(queue) > maximum
            or len(queue_accessions) != len(queue)
            or queue_accessions != completed
        ):
            raise _fail("maintenance checkpoint is not completed and exact")
    except InsiderPublicationMaterializationError:
        raise
    except (FileNotFoundError, OSError, TypeError, ValueError) as error:
        raise _fail("maintenance checkpoint") from error


def _policy_issuer_rows(
    state_store: InsiderStateStore,
) -> tuple[tuple[str, dict[str, object]], ...]:
    approved = state_store.read("approved-issuers-v1")
    policy = state_store.read("publication-policy-v1")
    approved_values = approved.get("issuer_ciks")
    issuer_rows = policy.get("issuers")
    if not isinstance(approved_values, list) or not isinstance(issuer_rows, list):
        raise _fail("approved issuer scope")
    approved_ciks = frozenset(approved_values)
    result: list[tuple[str, dict[str, object]]] = []
    for row in issuer_rows:
        if not isinstance(row, dict):
            raise _fail("publication policy")
        issuer_cik = row.get("issuer_cik")
        mappings = row.get("security_mappings")
        if (
            type(issuer_cik) is not str
            or issuer_cik not in approved_ciks
            or not isinstance(mappings, dict)
        ):
            raise _fail("approved issuer scope")
        result.append((issuer_cik, mappings))
    if not result:
        raise _fail("publication policy")
    return tuple(result)


def _load_verified_issuer_filings(
    storage: InsiderStorage,
    state_store: InsiderStateStore,
    issuer_cik: str,
    budget: _MaterializationBudget,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    issuer_state = state_store.read(f"issuers/{issuer_cik}")
    references = issuer_state.get("accessions")
    if not isinstance(references, list) or not references:
        raise _fail("issuer state accessions")
    budget.admit_accessions(len(references))

    filings: list[dict[str, object]] = []
    records = []
    for reference in references:
        if not isinstance(reference, dict):
            raise _fail("normalized filing binding")
        accession = reference.get("accession_number")
        parser_version = reference.get("parser_version")
        normalized_sha256 = reference.get("normalized_sha256")
        if (
            type(accession) is not str
            or parser_version != INSIDER_PARSER_VERSION
            or type(normalized_sha256) is not str
        ):
            raise _fail("normalized filing binding")
        try:
            normalized = storage.read_normalized_by_sha256(
                accession,
                parser_version,
                normalized_sha256,
            )
        except InsiderStorageError as error:
            raise _fail("normalized filing binding") from error
        budget.admit_normalized(normalized)
        record = issuer_record_from_normalized(
            normalized,
            parser_version=parser_version,
        )
        expected_binding = {
            "accession_number": record.accession_number,
            "parser_version": record.parser_version,
            "normalized_sha256": record.normalized_sha256,
        }
        if reference != expected_binding or record.issuer_cik != issuer_cik:
            raise _fail("normalized filing binding")
        filings.append(normalized)
        records.append(record)

    rebuilt = reduce_issuer_state(issuer_cik=issuer_cik, records=records).issuer_state
    if rebuilt != issuer_state:
        raise _fail("issuer state reconstruction")
    return filings, issuer_state


def _require_complete_security_mappings(
    issuer_state: Mapping[str, object],
    security_mappings: Mapping[str, object],
) -> None:
    security_classes = issuer_state.get("security_classes")
    if not isinstance(security_classes, list):
        raise _fail("security mapping")
    expected_keys = {
        entry["security_class_key"]
        for entry in security_classes
        if isinstance(entry, dict) and type(entry.get("security_class_key")) is str
    }
    if (
        len(expected_keys) != len(security_classes)
        or set(security_mappings) != expected_keys
    ):
        raise _fail("security mapping is incomplete")


def materialize_insider_publication(
    *,
    repository_root: Path,
    maintenance_mode: str,
    maintenance_issuer_cik: str,
    maintenance_quarter: str | None,
    maintenance_max_accessions: int,
    as_of: str,
    latest_successful_sync_at: str | None,
) -> dict[str, object]:
    """Build every policy issuer and publish one complete atomic public generation."""

    (
        mode,
        issuer_cik,
        quarter,
        maximum,
        canonical_as_of,
        canonical_sync_at,
    ) = _canonical_maintenance_configuration(
        maintenance_mode=maintenance_mode,
        maintenance_issuer_cik=maintenance_issuer_cik,
        maintenance_quarter=maintenance_quarter,
        maintenance_max_accessions=maintenance_max_accessions,
        as_of=as_of,
        latest_successful_sync_at=latest_successful_sync_at,
    )
    try:
        storage = InsiderStorage(repository_root)
        state_store = InsiderStateStore(repository_root)
        _require_completed_maintenance_checkpoint(
            state_store,
            mode=mode,
            issuer_cik=issuer_cik,
            quarter=quarter,
            maximum=maximum,
        )
        publications = []
        budget = _MaterializationBudget()
        for policy_issuer_cik, security_mappings in _policy_issuer_rows(state_store):
            filings, issuer_state = _load_verified_issuer_filings(
                storage,
                state_store,
                policy_issuer_cik,
                budget,
            )
            _require_complete_security_mappings(issuer_state, security_mappings)
            publication = build_insider_publication(
                filings,
                issuer_state=issuer_state,
                security_mappings=security_mappings,
                as_of=canonical_as_of,
                latest_successful_sync_at=canonical_sync_at,
            )
            for payload in publication.security_payloads.values():
                quality = payload.get("dataQuality")
                if (
                    not isinstance(quality, dict)
                    or quality.get("unmappedSecurityRowCount") != 0
                ):
                    raise _fail("security mapping left unmapped public rows")
            publications.append(publication)
        combined = combine_insider_publications(publications)
        publication_summary = write_insider_publication(
            combined,
            repository_root=repository_root,
        )
    except InsiderPublicationMaterializationError:
        raise
    except (FileNotFoundError, OSError, TypeError, ValueError) as error:
        raise _fail("verified private publication state") from error

    return {
        "issuerCiks": list(combined.issuer_ciks),
        **publication_summary,
    }


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(description=__doc__)
    parser.add_argument(
        "--maintenance-mode",
        required=True,
        choices=sorted(_MAINTENANCE_MODES),
    )
    parser.add_argument("--maintenance-issuer-cik", required=True)
    parser.add_argument("--maintenance-quarter")
    parser.add_argument("--maintenance-max-accessions", required=True, type=int)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--latest-successful-sync-at", required=True)
    return parser


def _configuration(
    argv: Sequence[str] | None,
) -> tuple[str, str, str | None, int, str, str | None]:
    if argv is not None and (
        isinstance(argv, (str, bytes))
        or any(type(argument) is not str for argument in argv)
    ):
        raise _ConfigurationError("command-line arguments are invalid")
    arguments = _parser().parse_args(None if argv is None else list(argv))
    latest_sync = (
        None
        if arguments.latest_successful_sync_at == "none"
        else arguments.latest_successful_sync_at
    )
    try:
        return _canonical_maintenance_configuration(
            maintenance_mode=arguments.maintenance_mode,
            maintenance_issuer_cik=arguments.maintenance_issuer_cik,
            maintenance_quarter=arguments.maintenance_quarter,
            maintenance_max_accessions=arguments.maintenance_max_accessions,
            as_of=arguments.as_of,
            latest_successful_sync_at=latest_sync,
        )
    except InsiderPublicationMaterializationError as error:
        raise _ConfigurationError("command-line arguments are invalid") from error


@pipeline._serialize_pipeline_maintenance
def main(argv: Sequence[str] | None = None) -> int:
    """Run one offline, complete, policy-bound materialization."""

    try:
        mode, issuer_cik, quarter, maximum, as_of, sync_at = _configuration(argv)
    except _ConfigurationError:
        pipeline.log.error("insider publication configuration is invalid")
        return 2

    try:
        result = materialize_insider_publication(
            repository_root=ROOT,
            maintenance_mode=mode,
            maintenance_issuer_cik=issuer_cik,
            maintenance_quarter=quarter,
            maintenance_max_accessions=maximum,
            as_of=as_of,
            latest_successful_sync_at=sync_at,
        )
    except InsiderPublicationMaterializationError:
        pipeline.log.error("insider publication materialization failed closed")
        return 1

    print(json.dumps(result, allow_nan=False, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
