"""Provider-neutral shadow comparison for the SEC security-master cutover.

The frozen baseline contains only the public CUSIP/ticker projection and
position-preservation invariants.  It is captured before the clean SEC rebuild
and is never used as an input to resolution.  The resulting private report is
therefore useful for migration review without retaining a vendor cache or
creating a fallback path.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections import Counter
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from security_identity import holding_instrument_type


SCHEMA_VERSION = 1
POSITION_DIGEST_ALGORITHM = "sha256-economic-position-multiset-v2"
_DIGEST_MODULUS = 1 << 256


class SecurityMasterMigrationError(ValueError):
    """Raised when a cutover projection or report is malformed."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _normalized_ticker(value: object) -> str | None:
    ticker = " ".join(str(value or "").upper().split())
    return ticker or None


def _normalized_text(value: object) -> str:
    return " ".join(str(value or "").split())


def _canonical_number(value: object) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SecurityMasterMigrationError(
            f"position contains a non-numeric value: {value!r}"
        ) from exc
    if not number.is_finite():
        raise SecurityMasterMigrationError(
            f"position contains a non-finite value: {value!r}"
        )
    return format(number.normalize(), "f")


def economic_positions_for_fund(
    fund: Mapping[str, Any],
    *,
    fallback_cik: str = "",
) -> dict[tuple[str, str, str, str], tuple[Decimal, Decimal]]:
    """Return the exact economic projection shared by pre-apply and final gates.

    Same-security row splits may change physical row counts, but cannot change
    any manager/period/CUSIP/instrument identity or its value and real shares.
    Imputed shares remain excluded under the established v2 digest contract.
    """

    cik = _normalized_text(fund.get("cik") or fallback_cik)
    quarters = fund.get("quarters")
    if not isinstance(quarters, list):
        raise SecurityMasterMigrationError("fund has no quarters list")
    positions: dict[tuple[str, str, str, str], tuple[Decimal, Decimal]] = {}
    for quarter in quarters:
        if not isinstance(quarter, dict):
            raise SecurityMasterMigrationError("fund has a malformed quarter")
        report_date = _normalized_text(quarter.get("report_date"))
        holdings = quarter.get("holdings")
        if not isinstance(holdings, list):
            raise SecurityMasterMigrationError("fund quarter has no holdings list")
        for holding in holdings:
            if not isinstance(holding, dict):
                raise SecurityMasterMigrationError("fund has a malformed holding")
            value = Decimal(_canonical_number(holding.get("value", 0)))
            shares = Decimal(_canonical_number(
                0 if holding.get("shares_imputed") is True
                else holding.get("shares", 0)
            ))
            key = (
                cik,
                report_date,
                _normalized_text(
                    holding.get("reported_cusip") or holding.get("cusip")
                ).upper(),
                holding_instrument_type(holding),
            )
            prior_value, prior_shares = positions.get(key, (Decimal(0), Decimal(0)))
            positions[key] = (prior_value + value, prior_shares + shares)
    return positions


def _project_fund_file(path: Path) -> tuple[int, int, int, int, Decimal, int, int]:
    """Compute a small, additive digest fragment without sharing holding data."""
    fund_count = quarter_count = source_holding_count = economic_position_count = 0
    total_value = Decimal(0)
    digest_sum_0 = digest_sum_1 = 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SecurityMasterMigrationError(
            f"cannot freeze cutover baseline from {path.name}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise SecurityMasterMigrationError(
            f"fund file is not an object: {path.name}"
        )
    fund_count += 1
    quarters = payload.get("quarters")
    if not isinstance(quarters, list):
        raise SecurityMasterMigrationError(
            f"fund file has no quarters list: {path.name}"
        )
    # A reported-identity rebuild may replace one legacy aggregate with
    # multiple exact SEC information-table rows. Those rows are the same
    # public economic position when manager, period, and the stable public
    # ``CUSIP | instrument_type`` identity agree. Raw share-unit metadata
    # is deliberately excluded because the SEC identity backfill can add
    # it to otherwise unchanged legacy rows. Issuer/class/accession/display
    # metadata are evidence or presentation, not portfolio economics.
    fund_positions = economic_positions_for_fund(payload, fallback_cik=path.stem)
    quarter_count += len(quarters)
    source_holding_count += sum(len(quarter["holdings"]) for quarter in quarters)
    total_value += sum((value for value, _shares in fund_positions.values()), Decimal(0))

    for key, (value, shares) in fund_positions.items():
        cik, report_date, cusip, instrument_type = key
        row = {
            "cik": cik,
            "report_date": report_date,
            "cusip": cusip,
            "instrument_type": instrument_type,
            "value": _canonical_number(value),
            "shares": _canonical_number(shares),
        }
        canonical_row = _canonical_bytes(row)
        digest_sum_0 = (
            digest_sum_0
            + int.from_bytes(
                hashlib.sha256(b"\x00" + canonical_row).digest(),
                "big",
            )
        ) % _DIGEST_MODULUS
        digest_sum_1 = (
            digest_sum_1
            + int.from_bytes(
                hashlib.sha256(b"\x01" + canonical_row).digest(),
                "big",
            )
        ) % _DIGEST_MODULUS
        economic_position_count += 1
    return (fund_count, quarter_count, source_holding_count,
            economic_position_count, total_value, digest_sum_0, digest_sum_1)


def _position_projection(
    funds_dir: Path, *, workers: int | None = None,
) -> dict[str, Any]:
    paths = sorted(Path(funds_dir).glob("*.json"))
    if workers is None:
        configured = os.environ.get("SEC_PIPELINE_WORKERS")
        workers = int(configured) if configured is not None else (
            min(6, os.cpu_count() or 1) if len(paths) >= 32 else 1
        )
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise SecurityMasterMigrationError("SEC pipeline workers must be a positive integer")
    workers = min(workers, 12, max(1, len(paths)))
    fund_count = quarter_count = source_holding_count = economic_position_count = 0
    total_value = Decimal(0)
    digest_sum_0 = digest_sum_1 = 0
    executor = ProcessPoolExecutor(max_workers=workers) if workers > 1 else None
    try:
        fragments = (
            executor.map(_project_fund_file, paths, chunksize=4)
            if executor is not None else map(_project_fund_file, paths)
        )
        # Ordered reduction preserves the established Decimal arithmetic and
        # multiset digest exactly. Each worker holds at most one fund at a time;
        # only seven scalar values per fund cross the process boundary.
        for funds, quarters, rows, positions, value, sum_0, sum_1 in fragments:
            fund_count += funds
            quarter_count += quarters
            source_holding_count += rows
            economic_position_count += positions
            total_value += value
            digest_sum_0 = (digest_sum_0 + sum_0) % _DIGEST_MODULUS
            digest_sum_1 = (digest_sum_1 + sum_1) % _DIGEST_MODULUS
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
    digest_payload = b"\x00".join(
        (
            POSITION_DIGEST_ALGORITHM.encode("ascii"),
            economic_position_count.to_bytes(16, "big"),
            digest_sum_0.to_bytes(32, "big"),
            digest_sum_1.to_bytes(32, "big"),
        )
    )
    return {
        "fund_count": fund_count,
        "quarter_count": quarter_count,
        # Retain the established field name for report compatibility. It now
        # counts canonical economic positions rather than physical JSON rows.
        "holding_count": economic_position_count,
        "source_holding_count": source_holding_count,
        "total_value": format(total_value.normalize(), "f"),
        "position_digest_algorithm": POSITION_DIGEST_ALGORITHM,
        "position_sha256": hashlib.sha256(digest_payload).hexdigest(),
    }


def _mapping_projection(registry: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    projection: dict[str, dict[str, Any]] = {}
    for raw_cusip, raw_entry in registry.items():
        cusip = _normalized_text(raw_cusip).upper()
        if not cusip or not isinstance(raw_entry, Mapping):
            continue
        projection[cusip] = {
            "ticker": _normalized_ticker(raw_entry.get("ticker")),
            "mapping_status": (
                _normalized_text(raw_entry.get("mapping_status")) or None
            ),
            "ticker_source": (
                _normalized_text(raw_entry.get("ticker_source")) or None
            ),
            "ticker_as_of": (
                _normalized_text(raw_entry.get("ticker_as_of")) or None
            ),
        }
    return {cusip: projection[cusip] for cusip in sorted(projection)}


def capture_cutover_projection(
    funds_dir: Path,
    registry: Mapping[str, Any],
) -> dict[str, Any]:
    """Freeze a read-only baseline that cannot seed the SEC resolver."""

    return {
        "schema_version": SCHEMA_VERSION,
        "corpus": _position_projection(Path(funds_dir)),
        "mappings": _mapping_projection(registry),
    }


def build_cutover_difference_report(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    generated_at: str | None,
) -> dict[str, Any]:
    """Compare a frozen pre-cutover projection with the SEC-only result."""

    if before.get("schema_version") != SCHEMA_VERSION:
        raise SecurityMasterMigrationError("unsupported pre-cutover projection")
    if after.get("schema_version") != SCHEMA_VERSION:
        raise SecurityMasterMigrationError("unsupported SEC-only projection")
    before_corpus = before.get("corpus")
    after_corpus = after.get("corpus")
    before_mappings = before.get("mappings")
    after_mappings = after.get("mappings")
    if not all(
        isinstance(value, Mapping)
        for value in (
            before_corpus,
            after_corpus,
            before_mappings,
            after_mappings,
        )
    ):
        raise SecurityMasterMigrationError("cutover projection is malformed")

    invariant_fields = (
        "fund_count",
        "quarter_count",
        "holding_count",
        "total_value",
        "position_digest_algorithm",
        "position_sha256",
    )
    invariants = {
        field: {
            "before": before_corpus.get(field),
            "after": after_corpus.get(field),
            "ok": before_corpus.get(field) == after_corpus.get(field),
        }
        for field in invariant_fields
    }
    outcomes: Counter[str] = Counter()
    differences: list[dict[str, Any]] = []
    for cusip in sorted(set(before_mappings) | set(after_mappings)):
        prior = before_mappings.get(cusip, {})
        current = after_mappings.get(cusip, {})
        prior_ticker = prior.get("ticker")
        current_ticker = current.get("ticker")
        if prior_ticker == current_ticker:
            outcome = "unchanged_ticker" if current_ticker else "unchanged_tickerless"
        elif prior_ticker and not current_ticker:
            outcome = "now_tickerless"
        elif not prior_ticker and current_ticker:
            outcome = "newly_sec_resolved"
        else:
            outcome = "ticker_changed"
        outcomes[outcome] += 1
        if prior_ticker != current_ticker:
            differences.append(
                {
                    "cusip": cusip,
                    "pre_cutover_ticker": prior_ticker,
                    "sec_ticker": current_ticker,
                    "sec_mapping_status": current.get("mapping_status"),
                    "sec_ticker_source": current.get("ticker_source"),
                    "sec_ticker_as_of": current.get("ticker_as_of"),
                    "outcome": outcome,
                }
            )

    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "sec_security_master_cutover_difference",
        "generated_at": generated_at,
        "baseline_usage": "comparison_only_never_resolution_input",
        "corpus_invariants_ok": all(
            result["ok"] for result in invariants.values()
        ),
        "corpus_invariants": invariants,
        "mapping_summary": {
            "pre_cutover_entries": len(before_mappings),
            "sec_entries": len(after_mappings),
            "differences": len(differences),
            "outcomes": {
                outcome: outcomes[outcome] for outcome in sorted(outcomes)
            },
        },
        "mapping_differences": differences,
    }
    report["report_sha256"] = _sha256(report)
    return report


def write_cutover_difference_report(
    report: Mapping[str, Any],
    path: Path,
) -> None:
    """Atomically persist the private one-release cutover report."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_bytes(dict(report)) + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, destination)
    finally:
        temporary_path.unlink(missing_ok=True)


__all__ = [
    "POSITION_DIGEST_ALGORITHM",
    "SCHEMA_VERSION",
    "SecurityMasterMigrationError",
    "build_cutover_difference_report",
    "capture_cutover_projection",
    "write_cutover_difference_report",
]
