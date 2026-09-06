#!/usr/bin/env python3
"""Emit GitHub Actions warnings for ticker health buckets."""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path


DEFAULT_REPORT_PATH = Path("data/ticker_health.json")
_CASH_PLACEHOLDER_IDS = frozenset({"00000CASH", "MONEYMRKT"})


def main() -> int:
    report_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_REPORT_PATH
    if not report_path.exists():
        print("ticker_health.json not present; skipping annotation")
        return 0

    # This step is wired as non-fatal in the workflow. A partial write from a
    # timed-out prior step would raise JSONDecodeError here and fail the step
    # anyway; swallow it so the Actions run summary still reports the rest.
    try:
        report = json.loads(report_path.read_text())
    except (OSError, ValueError) as e:
        print(f"::notice title=ticker_health::could not read {report_path}: {e}")
        return 0
    if not isinstance(report, dict):
        print(f"::notice title=ticker_health::{report_path} did not contain a JSON object")
        return 0
    label_coverage = report.get("label_coverage") or {}
    label_total = int(label_coverage.get("total") or 0)
    label_count = int(label_coverage.get("labeled") or 0)
    label_missing = int(label_coverage.get("unlabeled") or 0)
    if label_missing:
        samples = ", ".join(label_coverage.get("unlabeled_samples") or [])
        print(
            f"::notice title=security_labels::{label_missing} of "
            f"{label_total} CUSIP(s) lack a display label"
            + (f" — samples: {samples}" if samples else "")
        )
    elif label_total:
        print(f"security_labels: {label_count}/{label_total} CUSIPs labeled")
    summary = report.get("summary") or {}
    if not summary:
        print("ticker_health: all CUSIPs resolved cleanly")
        return 0

    buckets = report.get("buckets", {}) or {}
    observed_dates = sorted({
        str(entry.get("last_seen") or "")
        for entries in buckets.values()
        for entry in (entries or [])
        if isinstance(entry, dict) and entry.get("last_seen")
    })
    recent_dates = set(observed_dates[-2:])
    for bucket, count in sorted(summary.items()):
        top = (buckets.get(bucket) or [])[:5]
        examples = ", ".join(f"{e['cusip']}={e.get('ticker') or '∅'}" for e in top)
        if bucket == "unresolved":
            unresolved_entries = list(buckets.get(bucket) or [])
            unresolved_dates = sorted({
                str(entry.get("last_seen") or "")
                for entry in unresolved_entries
                if re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}",
                    str(entry.get("last_seen") or ""),
                )
            })
            current_dates = set(unresolved_dates[-2:])
            actionable_types = {"EQUITY", "PREF", "WARRANT"}
            actionable = []
            deferred = []
            for entry in unresolved_entries:
                instrument_type = str(
                    entry.get("instrument_type") or "UNKNOWN"
                ).upper()
                last_seen = str(entry.get("last_seen") or "")
                legacy_entry = instrument_type == "UNKNOWN" or not last_seen
                if legacy_entry or (
                    instrument_type in actionable_types
                    and (not current_dates or last_seen in current_dates)
                ):
                    actionable.append(entry)
                else:
                    deferred.append(entry)
            if actionable:
                actionable_examples = ", ".join(
                    f"{entry['cusip']}={entry.get('ticker') or '∅'}"
                    for entry in actionable[:5]
                )
                print(
                    "::notice title=ticker_health::"
                    f"{len(actionable)} current unresolved "
                    "EQUITY/PREF/WARRANT or legacy CUSIP(s)"
                    + (
                        f" — top by value: {actionable_examples}"
                        if actionable_examples
                        else ""
                    )
                )
            type_counts = Counter(
                str(entry.get("instrument_type") or "UNKNOWN")
                for entry in deferred
            )
            breakdown = ", ".join(
                f"{instrument_type} {type_count}"
                for instrument_type, type_count in sorted(
                    type_counts.items(),
                    key=lambda item: (-item[1], item[0]),
                )
            )
            if deferred:
                print(
                    f"::notice title=ticker_health_backlog::{len(deferred)} "
                    "stable debt/option, stale, or specialized unresolved "
                    "CUSIP(s) retained for observability"
                    + (f" — {breakdown}" if breakdown else "")
                )
            continue
        if bucket == "synthetic_identifier":
            actionable = [
                entry
                for entry in (buckets.get(bucket) or [])
                if int(entry.get("max_value") or 0) > 0
                and str(entry.get("cusip") or "").strip().upper()
                not in _CASH_PLACEHOLDER_IDS
                and (
                    not entry.get("last_seen")
                    or not recent_dates
                    or entry.get("last_seen") in recent_dates
                )
            ]
            if actionable:
                actionable_examples = ", ".join(
                    f"{entry['cusip']}={entry.get('ticker') or '∅'}"
                    for entry in actionable[:5]
                )
                print(
                    "::notice title=ticker_health::"
                    f"{len(actionable)} current nonzero synthetic identifier(s)"
                    + (
                        f" — top by value: {actionable_examples}"
                        if actionable_examples
                        else ""
                    )
                )
            deferred_count = count - len(actionable)
            if deferred_count:
                print(
                    "::notice title=ticker_health_backlog::"
                    f"{deferred_count} stale, zero-value, or cash placeholder(s) "
                    "retained for observability"
                )
            continue
        print(
            f"::notice title=ticker_health::{count} {bucket} CUSIP(s)"
            + (f" — top by value: {examples}" if examples else "")
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
