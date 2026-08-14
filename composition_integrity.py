"""Canonical amendment-composition serialization and hashing.

This module is stdlib-only so ingestion, validation, and repair tools share the
same historical byte protocol without importing pipeline orchestration.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence

from security_identity import holding_instrument_type


def canonical_json_hash(payload: object) -> str:
    """Return SHA-256 of the project's canonical UTF-8 JSON representation."""

    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def composition_holdings_payload(
    holdings: Sequence[Mapping[str, object]],
    *,
    include_holding_type: bool = False,
) -> list[dict[str, object]]:
    """Return stable SEC-derived fields, excluding mutable display metadata."""

    payload: list[dict[str, object]] = []
    for holding in holdings:
        row: dict[str, object] = {
            "cusip": holding.get("cusip"),
            "class": holding.get("class"),
            "value": holding.get("value"),
            # Zero-share repair is explicitly derived. Hash the reported zero
            # so an imputation cannot invalidate immutable SEC provenance.
            "shares": (
                0 if holding.get("shares_imputed") else holding.get("shares")
            ),
            "put_call": holding.get("put_call"),
        }
        if include_holding_type:
            row["holding_type"] = holding_instrument_type(holding)
        payload.append(row)
    payload.sort(
        key=lambda row: json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
    )
    return payload


def composition_source_decisions(
    source_filings: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Return immutable v2 amendment decisions included in the hash."""

    decisions: list[dict[str, object]] = []
    for source in source_filings:
        decision: dict[str, object] = {
            "accession": source.get("accession"),
            "source_hash": source.get("source_hash"),
            "form_type": source.get("form_type"),
            "accepted_at": source.get("accepted_at"),
            "amendment_number": source.get("amendment_number"),
            "amendment_kind": source.get("amendment_kind"),
            "composition_action": source.get("composition_action"),
            "new_holdings_overlap": source.get("new_holdings_overlap"),
        }
        if "security_identity_version" in source:
            decision["security_identity_version"] = source.get(
                "security_identity_version"
            )
        decisions.append(decision)
    return decisions


def calculate_composition_hash(
    report_date: object,
    base_accession: object,
    applied_accessions: list[str],
    applied_source_hashes: list[str],
    holdings: list[dict],
    *,
    composition_version: object,
    source_filings: list[dict] | None = None,
    security_identity_version: int | None = None,
    composition_hash_version: int = 1,
) -> str:
    """Hash one already-reduced quarter using the historical v1/v2 protocol."""

    payload: dict[str, object] = {
        "composition_version": composition_version,
        "report_date": report_date,
        "base_accession": base_accession,
        "applied_accessions": applied_accessions,
        "applied_source_hashes": applied_source_hashes,
        "holdings": composition_holdings_payload(
            holdings,
            include_holding_type=composition_hash_version >= 2,
        ),
    }
    if composition_hash_version >= 2:
        payload["composition_hash_version"] = composition_hash_version
    if composition_version == 2:
        if source_filings is None:
            raise ValueError("v2 composition hashes require source filing decisions")
        payload["source_decisions"] = composition_source_decisions(source_filings)
        if security_identity_version is not None:
            payload["security_identity_version"] = security_identity_version
    return canonical_json_hash(payload)


def calculate_quarter_composition_hash(
    quarter: dict,
    *,
    current_hash_version: int,
) -> str:
    """Derive a composition hash from a persisted quarter record."""

    sources: dict[str, dict] = {}
    for source in quarter["source_filings"]:
        if isinstance(source, dict) and isinstance(source.get("accession"), str):
            sources[source["accession"]] = source
    applied = quarter["applied_accessions"]
    raw_hash_version = quarter.get("composition_hash_version", 1)
    hash_version = (
        raw_hash_version
        if (
            type(raw_hash_version) is int
            and raw_hash_version in {1, current_hash_version}
        )
        else 1
    )
    return calculate_composition_hash(
        quarter.get("report_date"),
        quarter["base_accession"],
        applied,
        [sources[accession]["source_hash"] for accession in applied],
        quarter["holdings"],
        composition_version=quarter.get("composition_version"),
        source_filings=quarter["source_filings"],
        security_identity_version=(
            quarter.get("security_identity_version")
            if "security_identity_version" in quarter
            else None
        ),
        composition_hash_version=hash_version,
    )


__all__ = [
    "calculate_composition_hash",
    "calculate_quarter_composition_hash",
    "canonical_json_hash",
    "composition_holdings_payload",
    "composition_source_decisions",
]
