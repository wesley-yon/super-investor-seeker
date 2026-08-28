#!/usr/bin/env python3
"""Audit or repair verified historical 1,000x value-unit errors.

This is an idempotent migration for data produced by the former unweighted
median-price heuristic. It uses peer consensus for broad discovery plus a
narrow SEC-backed manifest for cases peer prices cannot classify. By default it
reports candidates; pass ``--apply`` to write mechanically safe repairs and
recompute composition hashes. ``--migrate-policy`` performs the one-time
policy-v2 cutover only after a bounded peer and adjacent-quarter scan of every
retained quarter succeeds.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import statistics
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import validate_data
from value_units import (
    VALUE_UNIT_POLICY_VERSION,
    is_unit_evidence_holding,
    peer_scale_evidence,
)


FUNDS_DIR = ROOT / "data" / "funds"
STATE_PATH = ROOT / "data" / "pipeline_state.json"
LEGACY_VALUE_UNIT_POLICY_VERSION = 1

# Auditable manifest of the historical quarters repaired by this migration.
# The value is the correct multiplier for the SEC-reported source values.
KNOWN_REPAIRS: dict[int, dict[int, tuple[str, ...]]] = {
    1: {
        724683: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
        919185: ("2025-06-30", "2025-09-30", "2025-12-31"),
        920760: ("2025-12-31", "2026-03-31"),
        1042537: ("2025-06-30",),
        1259927: ("2025-06-30",),
        1301396: ("2026-03-31",),
        1346554: ("2025-06-30", "2025-09-30", "2026-03-31"),
        1395064: ("2025-06-30",),
        1453885: ("2025-06-30", "2025-12-31"),
        1524362: ("2025-09-30",),
        1533551: ("2026-03-31",),
        1600745: ("2025-06-30",),
        1674784: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
        1698093: ("2025-06-30",),
        1738654: ("2025-06-30",),
        1746382: ("2025-09-30",),
        1750183: ("2025-09-30", "2025-12-31"),
        1778719: ("2025-06-30", "2025-09-30"),
        1782866: ("2025-06-30",),
        1801577: ("2025-06-30",),
        1815467: ("2026-03-31",),
        1853431: ("2025-06-30",),
        1854423: ("2025-12-31", "2026-03-31"),
        1857418: ("2025-09-30",),
        1869028: ("2025-12-31", "2026-03-31"),
        1878495: ("2025-06-30", "2025-09-30"),
        1905106: ("2025-09-30", "2025-12-31", "2026-03-31"),
        1910592: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
        1950677: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
        1964236: ("2025-06-30", "2025-09-30"),
        1983408: ("2025-06-30",),
        2032489: ("2025-12-31",),
    },
    1000: {
        1624050: ("2025-09-30", "2025-12-31", "2026-03-31"),
        1912699: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
    },
}

# SEC-verified repairs that cannot be recovered by the peer-price scan:
# non-equity filings, later quarters absent from the original migration, and
# one filing whose rows used mixed units. Exact totals and row signatures make
# this manifest fail closed if the historical inputs ever differ.
EXPLICIT_HISTORICAL_REPAIRS: dict[tuple[int, str], dict] = {
    (1629996, "2025-09-30"): {
        "accession": "0001629996-25-000006",
        "operation": "multiply_all",
        "factor": 1000,
        "bad_total": 61_071,
        "correct_total": 61_071_000,
        "holding_count": 36,
        "bad_signature": (
            "8856181ddc3c72cd455ae998c694fdaadc93ff1600834421505bc5189cf7c26f"
        ),
        "correct_signature": (
            "db3bafdebeee0b073b296bb057ed1b2da8e5d6fb629dbe9735348fc18ec7582e"
        ),
        "value_multiplier": 1000,
        "repair_status": "understated_1000x",
    },
    (1629996, "2025-12-31"): {
        "accession": "0001629996-26-000001",
        "operation": "multiply_all",
        "factor": 1000,
        "bad_total": 61_299,
        "correct_total": 61_299_000,
        "holding_count": 38,
        "bad_signature": (
            "e89e43256e4e400fcd8528ea4eea3b2ef1e2c677ebb214307d701257ad8ab5a6"
        ),
        "correct_signature": (
            "9e5f537c6ae4639a4ede036d4959fc6bd3a3ed255bba2035d979f06d2e2e4442"
        ),
        "value_multiplier": 1000,
        "repair_status": "understated_1000x",
    },
    (1631562, "2025-06-30"): {
        "accession": "0001631562-25-000005",
        "operation": "multiply_except_cusips",
        "factor": 1000,
        "unscaled_cusips": ("002824100",),
        "bad_total": 124_792_282,
        "correct_total": 6_102_754_336,
        "holding_count": 53,
        "bad_signature": (
            "bc3e032305c4b7ce5d04e5bf2f1e0448bdf0a1afa8c6e2f7e12951ca75cf2074"
        ),
        "correct_signature": (
            "04531b33766ffc529c200dc5fcb383347ad3d542e02b04e4941080f10fa05d16"
        ),
        "value_multiplier": None,
        "repair_status": "mixed_source_units",
    },
    (1738654, "2025-09-30"): {
        "accession": "0000919574-25-006977",
        "operation": "divide_all",
        "divisor": 1000,
        "bad_total": 1_600_206_697_000,
        "correct_total": 1_600_206_697,
        "holding_count": 44,
        "bad_signature": (
            "5c8296654d4edafb2303bf2d4f577d382e67239acebaf988db1864b3324bbe97"
        ),
        "correct_signature": (
            "202d78fe815f85a8847875c89f6a6d0837e975b723503fb3b2329ef30e96e69d"
        ),
        "value_multiplier": 1,
        "repair_status": "inflated_1000x",
    },
    (1738654, "2025-12-31"): {
        "accession": "0000919574-26-001163",
        "operation": "divide_all",
        "divisor": 1000,
        "bad_total": 1_371_559_130_000,
        "correct_total": 1_371_559_130,
        "holding_count": 43,
        "bad_signature": (
            "b7dff725ce4689abef84f690533f3d8529f1b2bcb3100cd5748ed2a0a18c3ab1"
        ),
        "correct_signature": (
            "f460c49f4b9320d81703323df9f9fadcdc265f3f3242eb86bdd44740e76e6cfb"
        ),
        "value_multiplier": 1,
        "repair_status": "inflated_1000x",
    },
    (1738654, "2026-03-31"): {
        "accession": "0000919574-26-003413",
        "operation": "divide_all",
        "divisor": 1000,
        "bad_total": 1_388_169_795_000,
        "correct_total": 1_388_169_795,
        "holding_count": 36,
        "bad_signature": (
            "6aa30e73aeb9f46a4b9824b8e2d0d93fed745f9b79dad8571921844d4a51caf1"
        ),
        "correct_signature": (
            "6bf6ea4c40d43820002575f32ac89cfcc76ca3e7e7c2a5d6e4f5cd4329e7cdcd"
        ),
        "value_multiplier": 1,
        "repair_status": "inflated_1000x",
    },
}


def load_fund(path: Path) -> dict:
    with open(path) as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} is not a fund object")
    return payload


def atomic_write_json(path: Path, payload: dict) -> None:
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as temp:
        json.dump(payload, temp, indent=2)
        temp_path = Path(temp.name)
    os.replace(temp_path, path)


def holding_value_signature(holdings: list[dict]) -> str:
    """Hash the SEC identity, amount, and value fields affected by a repair."""
    rows: list[list[str | int | float]] = []
    for holding in holdings:
        if not isinstance(holding, dict):
            raise ValueError("repair quarter contains a non-object holding")
        value = holding.get("value")
        shares = holding.get("shares")
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or isinstance(shares, bool)
            or not isinstance(shares, (int, float))
        ):
            raise ValueError("repair quarter contains non-numeric value or shares")
        rows.append([
            str(holding.get("cusip") or "").strip().upper(),
            shares,
            value,
        ])
    rows.sort(key=lambda row: (str(row[0]), str(row[1]), str(row[2])))
    canonical = json.dumps(rows, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def explicit_repair_metadata(spec: dict) -> dict:
    evidence = {
        "migration": True,
        "repair_status": spec["repair_status"],
        "sec_accession": spec["accession"],
        "pre_repair_total": spec["bad_total"],
        "normalized_value_total": spec["correct_total"],
    }
    if spec["operation"] == "multiply_except_cusips":
        evidence["row_value_multipliers"] = {
            "default": spec["factor"],
            **{cusip: 1 for cusip in spec["unscaled_cusips"]},
        }
    if spec["value_multiplier"] is None:
        # A mixed filing has no truthful filing-wide multiplier. Keep its
        # audit trail separate from the uniform convention fields consumed by
        # future-quarter inference.
        return {
            "value_unit_repair": {
                "method": "sec_verified_historical_migration",
                "confidence": "high",
                "evidence": evidence,
            },
        }
    return {
        "value_unit_policy_version": VALUE_UNIT_POLICY_VERSION,
        "value_unit_method": "sec_verified_historical_migration",
        "value_unit_confidence": "high",
        "value_unit_evidence": evidence,
    }


def repair_explicit_historical_quarter(quarter: dict, spec: dict) -> bool:
    """Apply one exact SEC-backed repair, or no-op on its repaired form."""
    holdings = quarter.get("holdings")
    if not isinstance(holdings, list):
        raise ValueError("explicit repair quarter holdings are not a list")
    if len(holdings) != spec["holding_count"]:
        raise ValueError(
            "explicit repair holding count changed: "
            f"expected {spec['holding_count']}, found {len(holdings)}"
        )
    total = quarter.get("total_value")
    if total != sum(holding.get("value", 0) or 0 for holding in holdings):
        raise ValueError("explicit repair quarter total does not match holdings")

    signature = holding_value_signature(holdings)
    value_changed = False
    if total == spec["bad_total"]:
        if signature != spec["bad_signature"]:
            raise ValueError("explicit repair source row signature changed")
        unscaled_cusips = set(spec.get("unscaled_cusips", ()))
        for holding in holdings:
            cusip = str(holding.get("cusip") or "").strip().upper()
            if (
                spec["operation"] == "multiply_except_cusips"
                and cusip in unscaled_cusips
            ):
                continue
            value = holding["value"]
            if spec["operation"] == "divide_all":
                divisor = spec["divisor"]
                if not isinstance(value, int) or value % divisor != 0:
                    raise ValueError(
                        "explicit downscale value is not an exact multiple of 1,000"
                    )
                holding["value"] = value // divisor
            elif spec["factor"] == 1000:
                holding["value"] = value * 1000
            else:
                raise ValueError("unsupported explicit repair factor")
        quarter["total_value"] = sum(
            holding.get("value", 0) or 0 for holding in holdings
        )
        if (
            quarter["total_value"] != spec["correct_total"]
            or holding_value_signature(holdings) != spec["correct_signature"]
        ):
            raise ValueError("explicit repair did not produce the verified result")
        value_changed = True
    elif (
        total != spec["correct_total"]
        or signature != spec["correct_signature"]
    ):
        raise ValueError(
            "explicit repair quarter matches neither verified source nor result"
        )

    metadata = explicit_repair_metadata(spec)
    metadata_changed = any(
        quarter.get(key) != value for key, value in metadata.items()
    )
    quarter.update(metadata)
    multiplier = spec["value_multiplier"]
    if multiplier is None:
        for key in (
            "value_unit_policy_version",
            "value_multiplier",
            "value_unit_method",
            "value_unit_confidence",
            "value_unit_evidence",
        ):
            if key in quarter:
                del quarter[key]
                metadata_changed = True
    elif quarter.get("value_multiplier") != multiplier:
        quarter["value_multiplier"] = multiplier
        metadata_changed = True

    if value_changed and quarter.get("composition_version") in {1, 2}:
        quarter["composition_hash"] = validate_data.calculate_composition_hash(
            quarter
        )
    return value_changed or metadata_changed


def apply_explicit_historical_repairs() -> int:
    """Write the narrow manifest only after every target validates in memory."""
    funds: dict[Path, dict] = {}
    changed_paths: set[Path] = set()
    changed_quarters = 0
    for (cik, report_date), spec in EXPLICIT_HISTORICAL_REPAIRS.items():
        path = FUNDS_DIR / f"{cik}.json"
        fund = funds.setdefault(path, load_fund(path))
        matches = [
            quarter
            for quarter in fund.get("quarters", []) or []
            if (
                isinstance(quarter, dict)
                and quarter.get("report_date") == report_date
            )
        ]
        if len(matches) != 1:
            raise ValueError(
                f"{path} does not contain exactly one {report_date} quarter"
            )
        if repair_explicit_historical_quarter(matches[0], spec):
            changed_paths.add(path)
            changed_quarters += 1

    for path in changed_paths:
        atomic_write_json(path, funds[path])
    return changed_quarters


def build_peer_references() -> dict[tuple[str, str], tuple[float, int]]:
    prices: dict[
        tuple[str, str], dict[str, list[float]]
    ] = defaultdict(lambda: defaultdict(list))
    for path in sorted(FUNDS_DIR.glob("*.json")):
        fund = load_fund(path)
        cik = str(fund.get("cik") or path.stem)
        for quarter in fund.get("quarters", []) or []:
            if not isinstance(quarter, dict):
                continue
            report_date = str(quarter.get("report_date") or "")
            for holding in quarter.get("holdings", []) or []:
                if not isinstance(holding, dict) or not is_unit_evidence_holding(
                    holding
                ):
                    continue
                value = holding.get("value")
                shares = holding.get("shares")
                if (
                    report_date
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and value > 0
                    and isinstance(shares, (int, float))
                    and not isinstance(shares, bool)
                    and shares > 0
                ):
                    cusip = str(holding.get("cusip") or "").strip().upper()
                    if cusip:
                        prices[(report_date, cusip)][cik].append(value / shares)
    return {
        key: (
            statistics.median(
                statistics.median(filer_values)
                for filer_values in by_filer.values()
            ),
            len(by_filer),
        )
        for key, by_filer in prices.items()
        if len(by_filer) >= 4
    }


def find_candidates(
    references: dict[tuple[str, str], tuple[float, int]],
) -> list[dict]:
    candidates: list[dict] = []
    for path in sorted(FUNDS_DIR.glob("*.json")):
        fund = load_fund(path)
        cik = int(fund.get("cik") or path.stem)
        for quarter_index, quarter in enumerate(fund.get("quarters", []) or []):
            if not isinstance(quarter, dict):
                continue
            report_date = str(quarter.get("report_date") or "")
            holdings = quarter.get("holdings")
            if not report_date or not isinstance(holdings, list):
                continue
            if (cik, report_date) in EXPLICIT_HISTORICAL_REPAIRS:
                continue
            peer_prices = {
                str(holding.get("cusip") or "").strip().upper(): reference
                for holding in holdings
                if isinstance(holding, dict)
                and (
                    reference := references.get(
                        (
                            report_date,
                            str(holding.get("cusip") or "").strip().upper(),
                        )
                    )
                )
            }
            evidence = peer_scale_evidence(
                holdings,
                peer_prices,
                min_scale_positions=1,
            )
            if evidence["status"] is not None:
                candidates.append({
                    "path": path,
                    "cik": cik,
                    "name": fund.get("name"),
                    "quarter_index": quarter_index,
                    "report_date": report_date,
                    "evidence": evidence,
                })
    return candidates


def infer_source_multipliers(
    quarter: dict,
) -> list[tuple[dict, int]] | None:
    applied = [
        source
        for source in quarter.get("source_filings", []) or []
        if isinstance(source, dict) and source.get("applied") is True
    ]
    totals = [source.get("reported_value_total") for source in applied]
    if (
        not applied
        or any(
            isinstance(total, bool) or not isinstance(total, int) or total < 0
            for total in totals
        )
    ):
        return None
    matches = [
        multipliers
        for multipliers in itertools.product((1, 1000), repeat=len(applied))
        if sum(total * multiplier for total, multiplier in zip(totals, multipliers))
        == quarter.get("total_value")
    ]
    if len(matches) != 1:
        return None
    return list(zip(applied, matches[0]))


def backfill_unit_provenance(quarter: dict) -> None:
    """Record arithmetic source attribution without treating it as proof."""
    assignment = infer_source_multipliers(quarter)
    if assignment is None:
        return
    for source, multiplier in assignment:
        source["value_unit_policy_version"] = VALUE_UNIT_POLICY_VERSION
        source["value_multiplier"] = multiplier
        source["normalized_value_total"] = (
            source["reported_value_total"] * multiplier
        )
        source["value_unit_method"] = "arithmetic_only_migration"
        source["value_unit_confidence"] = "low"
        source["value_unit_evidence"] = {
            "migration": True,
            "independent_unit_proof": False,
        }


def repair_mixed_composition(quarter: dict) -> None:
    assignment = infer_source_multipliers(quarter)
    if assignment is None:
        raise ValueError("mixed composition has no unique current unit assignment")

    corrected = 0
    for source, multiplier in assignment:
        if multiplier != 1000:
            continue
        if source.get("reported_entry_total") != 1:
            raise ValueError(
                "mixed composition scaled source is not a one-row supplement"
            )
        scaled_total = source["reported_value_total"] * 1000
        matches = [
            holding
            for holding in quarter.get("holdings", [])
            if holding.get("value") == scaled_total
        ]
        if len(matches) != 1:
            raise ValueError(
                "mixed composition scaled source does not match one holding"
            )
        matches[0]["value"] = source["reported_value_total"]
        corrected += 1
    if corrected == 0:
        raise ValueError("mixed composition had no scaled component to repair")


def repair_candidate(quarter: dict, status: str) -> None:
    holdings = quarter.get("holdings")
    if not isinstance(holdings, list):
        raise ValueError("quarter holdings are not a list")

    applied = [
        source
        for source in quarter.get("source_filings", []) or []
        if isinstance(source, dict) and source.get("applied") is True
    ]
    if status == "inflated_1000x":
        if quarter.get("composition_version") in {1, 2} and len(applied) > 1:
            repair_mixed_composition(quarter)
        else:
            invalid = [
                holding.get("value")
                for holding in holdings
                if (
                    isinstance(holding.get("value"), bool)
                    or not isinstance(holding.get("value"), int)
                    or holding["value"] % 1000 != 0
                )
            ]
            if invalid:
                raise ValueError(
                    "inflated quarter contains values that are not exact "
                    "multiples of 1,000"
                )
            for holding in holdings:
                holding["value"] //= 1000
    elif status == "understated_1000x":
        if quarter.get("composition_version") in {1, 2} and len(applied) > 1:
            raise ValueError(
                "refusing to scale a mixed composition without row provenance"
            )
        for holding in holdings:
            value = holding.get("value")
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError("understated quarter contains a non-numeric value")
            holding["value"] = value * 1000
    else:
        raise ValueError(f"unsupported repair status {status!r}")

    quarter["total_value"] = sum(
        holding.get("value", 0) or 0 for holding in holdings
    )
    backfill_unit_provenance(quarter)
    if quarter.get("composition_version") in {1, 2}:
        quarter["composition_hash"] = validate_data.calculate_composition_hash(
            quarter
        )


def apply_candidates(candidates: list[dict]) -> int:
    funds: dict[Path, dict] = {}
    for candidate in candidates:
        path = candidate["path"]
        fund = funds.setdefault(path, load_fund(path))
        quarter = fund["quarters"][candidate["quarter_index"]]
        if quarter.get("report_date") != candidate["report_date"]:
            raise ValueError(f"{path} changed while planning repairs")
        repair_candidate(quarter, candidate["evidence"]["status"])

    for path, fund in funds.items():
        atomic_write_json(path, fund)
    return len(candidates)


def backfill_known_repair_provenance() -> int:
    """Fill missing legacy metadata without promoting it to current proof."""
    changed_quarters = 0
    for multiplier, funds in KNOWN_REPAIRS.items():
        repair_status = (
            "inflated_1000x" if multiplier == 1 else "understated_1000x"
        )
        for cik, report_dates in funds.items():
            path = FUNDS_DIR / f"{cik}.json"
            fund = load_fund(path)
            quarters = {
                quarter.get("report_date"): quarter
                for quarter in fund.get("quarters", []) or []
                if isinstance(quarter, dict)
            }
            changed = False
            for report_date in report_dates:
                quarter = quarters.get(report_date)
                if quarter is None:
                    raise ValueError(
                        f"{path} is missing known repair quarter {report_date}"
                    )
                applied_sources = [
                    source
                    for source in quarter.get("source_filings", []) or []
                    if isinstance(source, dict) and source.get("applied") is True
                ]
                if applied_sources or quarter.get(
                    "value_unit_policy_version"
                ) is not None:
                    # Current composed quarters already carry stronger
                    # per-source proof. Existing legacy metadata is retained as
                    # history but is no longer trusted by policy v2.
                    continue
                metadata = {
                    "value_unit_policy_version": (
                        LEGACY_VALUE_UNIT_POLICY_VERSION
                    ),
                    "value_multiplier": multiplier,
                    "value_unit_method": "legacy_peer_consensus_migration",
                    "value_unit_confidence": "low",
                    "value_unit_evidence": {
                        "migration": True,
                        "repair_status": repair_status,
                        "independent_unit_proof": False,
                    },
                }
                if any(
                    quarter.get(key) != value
                    for key, value in metadata.items()
                ):
                    quarter.update(metadata)
                    changed = True
                    changed_quarters += 1
            if changed:
                atomic_write_json(path, fund)
    return changed_quarters


def retained_policy_inventory() -> Counter:
    """Count the current proof status of every retained fund quarter."""
    inventory: Counter = Counter()
    for path in sorted(FUNDS_DIR.glob("*.json")):
        fund = load_fund(path)
        for quarter in fund.get("quarters", []) or []:
            if not isinstance(quarter, dict):
                continue
            inventory["quarters"] += 1
            if isinstance(quarter.get("value_unit_repair"), dict):
                inventory["explicit_mixed_repairs"] += 1

            source_filings = quarter.get("source_filings")
            applied_sources = []
            if isinstance(source_filings, list):
                applied_sources = [
                    source
                    for source in source_filings
                    if (
                        isinstance(source, dict)
                        and source.get("applied") is True
                    )
                ]
            proof_records = applied_sources or [quarter]
            versions = {
                record.get("value_unit_policy_version")
                for record in proof_records
                if record.get("value_unit_policy_version") is not None
            }
            if (
                proof_records
                and all(
                    record.get("value_unit_policy_version")
                    == VALUE_UNIT_POLICY_VERSION
                    and record.get("value_unit_confidence") == "high"
                    for record in proof_records
                )
            ):
                inventory["current_high_confidence"] += 1
            elif versions:
                inventory["legacy_or_low_confidence"] += 1
            else:
                inventory["without_unit_provenance"] += 1
    return inventory


def audit_retained_value_unit_policy() -> tuple[Counter, list[str]]:
    """Run bounded peer and exact-adjacent checks over every retained quarter."""
    references = build_peer_references()
    candidates = find_candidates(references)
    errors: list[str] = []
    for candidate in candidates:
        evidence = candidate["evidence"]
        errors.append(
            f"{candidate['cik']} {candidate['report_date']} "
            f"{evidence['status']} "
            f"(coverage={evidence['matched_value_coverage']:.3f})"
        )

    for path in sorted(FUNDS_DIR.glob("*.json")):
        fund = load_fund(path)
        validate_data.validate_adjacent_quarter_value_units(
            fund,
            f"fund file {path.name}",
            errors,
        )
    return retained_policy_inventory(), errors


def mark_value_unit_migration_complete() -> None:
    """Persist the v2 cutover only after the complete retained-corpus audit."""
    state = load_fund(STATE_PATH)
    state["value_unit_migration_version"] = VALUE_UNIT_POLICY_VERSION
    atomic_write_json(STATE_PATH, state)


def migrate_value_unit_policy() -> tuple[Counter, int]:
    """Apply exact manifests, audit all retained quarters, then cut over."""
    explicit = apply_explicit_historical_repairs()
    inventory, errors = audit_retained_value_unit_policy()
    if errors:
        examples = "\n".join(f"  - {error}" for error in errors[:20])
        raise ValueError(
            f"value-unit policy migration found {len(errors)} anomaly(s):\n"
            f"{examples}"
        )
    mark_value_unit_migration_complete()
    return inventory, explicit


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write exact verified repairs; default is audit-only",
    )
    parser.add_argument(
        "--migrate-policy",
        action="store_true",
        help="run the bounded retained-corpus audit and mark policy v2 complete",
    )
    args = parser.parse_args()

    if args.apply and args.migrate_policy:
        parser.error("--apply and --migrate-policy are mutually exclusive")
    if args.migrate_policy:
        try:
            inventory, explicit = migrate_value_unit_policy()
        except (OSError, ValueError) as exc:
            print(f"Value-unit policy migration failed: {exc}", file=sys.stderr)
            return 1
        print(
            "Value-unit policy migration complete: "
            f"{inventory['quarters']} retained quarters audited, "
            f"{inventory['current_high_confidence']} current high-confidence, "
            f"{inventory['legacy_or_low_confidence']} legacy/low-confidence, "
            f"{inventory['without_unit_provenance']} without provenance, "
            f"{inventory['explicit_mixed_repairs']} explicit mixed repairs"
        )
        print(f"Applied explicit repairs/provenance to {explicit} quarter(s)")
        return 0

    references = build_peer_references()
    candidates = find_candidates(references)
    for candidate in candidates:
        evidence = candidate["evidence"]
        if (
            evidence["status"] == "mixed_scale_clusters"
            and evidence["intrinsic_count_value_conflict"]
            and evidence["matched_count_coverage"] < 0.8
        ):
            support = (
                "intrinsic-low-price="
                f"{evidence['low_price_count_support']:.3f}/"
                f"{evidence['low_price_value_support']:.3f}, "
                f"peer-count={evidence['matched_count_coverage']:.3f}"
            )
        elif evidence["status"] == "mixed_scale_clusters":
            support = (
                "mixed="
                f"aligned {evidence['aligned_count_support']:.3f}/"
                f"{evidence['aligned_value_support']:.3f}, "
                f"inflated {evidence['inflated_count_support']:.3f}/"
                f"{evidence['inflated_value_support']:.3f}, "
                f"understated {evidence['understated_count_support']:.3f}/"
                f"{evidence['understated_value_support']:.3f}"
            )
        else:
            support = (
                f"inflated={evidence['inflated_value_support']:.3f}\t"
                f"understated={evidence['understated_value_support']:.3f}"
            )
        print(
            f"{candidate['cik']}\t{candidate['report_date']}\t"
            f"{evidence['status']}\t"
            f"coverage={evidence['matched_value_coverage']:.3f}\t"
            f"{support}\t"
            f"{candidate['name']}"
        )
    print(f"{len(candidates)} value-unit anomaly candidate quarter(s)")

    if not args.apply:
        return 0
    repaired = apply_candidates(candidates)
    explicit = apply_explicit_historical_repairs()
    backfilled = backfill_known_repair_provenance()
    print(f"Repaired {repaired} quarter(s)")
    print(f"Applied explicit repairs/provenance to {explicit} quarter(s)")
    print(f"Backfilled provenance for {backfilled} repaired quarter(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
