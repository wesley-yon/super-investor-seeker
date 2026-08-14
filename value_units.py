"""Deterministic SEC 13F value-unit classification helpers.

Some filers still submit information-table values in thousands even though
modern filings normally use dollars.  The raw XML does not provide a reliable
unit flag, so classification must use the filing's own ordinary-equity rows.

The policy deliberately requires broad evidence before applying a 1,000x
multiplier and treats disagreement between position-count and raw-value
dominance as mixed units. This prevents one economically dominant position
from concealing a differently scaled majority of the portfolio.
"""

from __future__ import annotations

import re
from typing import Iterable


VALUE_UNIT_POLICY_VERSION = 2
VALUE_UNIT_SCALE = 1000
LOW_PRICE_CUTOFF = 1.0
DOLLAR_MAX_LOW_PRICE_VALUE_SHARE = 0.50
THOUSANDS_MIN_LOW_PRICE_VALUE_SHARE = 0.75
THOUSANDS_MIN_LOW_PRICE_CUSIPS = 5
MIXED_MIN_LOW_PRICE_COUNT_SUPPORT = 0.80

PEER_RATIO_MIN = 500.0
PEER_RATIO_MAX = 2000.0
PEER_MIN_REFERENCE_COUNT = 4
PEER_MIN_VALUE_COVERAGE = 0.50
PEER_MIN_SCALE_SUPPORT = 0.80
PEER_MIN_SCALE_COUNT_SUPPORT = 0.80
PEER_MIN_SCALE_POSITIONS = 3
PEER_ALIGNMENT_FACTOR = 3.0
RUNTIME_PEER_MIN_REFERENCE_COUNT = 3
RUNTIME_STALE_PEER_MIN_POSITIONS = 3

ADJACENT_MIN_SHARED_POSITIONS = 10
ADJACENT_MIN_COUNT_SUPPORT = 0.80
ADJACENT_MIN_VALUE_SUPPORT = 0.80

_WARRANT_CLASS_RE = re.compile(
    r"(?:^|\s)(?:\*?W(?:T|TS)?(?:\s+EXP)?|WARRANTS?)(?:\s|$)",
    re.IGNORECASE,
)


class AmbiguousValueUnits(ValueError):
    """The filing does not contain enough consistent evidence to choose units."""


def _positive_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if number > 0 else None


def _has_intrinsic_count_value_conflict(
    *,
    eligible_cusips: int,
    low_price_cusips: int,
    low_price_value_share: float,
) -> bool:
    """Return whether row count and raw value imply different unit clusters."""
    low_price_count_support = (
        low_price_cusips / eligible_cusips if eligible_cusips else 0.0
    )
    return (
        low_price_cusips >= THOUSANDS_MIN_LOW_PRICE_CUSIPS
        and low_price_count_support >= MIXED_MIN_LOW_PRICE_COUNT_SUPPORT
        and low_price_value_share <= DOLLAR_MAX_LOW_PRICE_VALUE_SHARE
    )


def is_unit_evidence_holding(holding: dict) -> bool:
    """Return whether a row has meaningful ordinary-share price evidence."""
    if holding.get("shares_imputed"):
        return False

    holding_type = str(
        holding.get("holding_type") or holding.get("option_type") or "EQUITY"
    ).strip().upper()
    if holding_type != "EQUITY":
        return False

    put_call = str(holding.get("put_call") or "").strip().upper()
    if put_call in {"PUT", "CALL"}:
        return False

    amount_type = str(
        holding.get("share_amount_type")
        or holding.get("amount_type")
        or holding.get("sshPrnamtType")
        or ""
    ).strip().upper()
    if amount_type == "PRN":
        return False

    title = str(holding.get("class") or "")
    if _WARRANT_CLASS_RE.search(title):
        return False

    return (
        _positive_number(holding.get("value")) is not None
        and _positive_number(holding.get("shares")) is not None
    )


def _unit_evidence_row(
    holding: dict,
) -> tuple[str, float, float] | None:
    """Normalize policy-neutral fields used by each unit-evidence path."""

    if not is_unit_evidence_holding(holding):
        return None
    value = _positive_number(holding.get("value"))
    shares = _positive_number(holding.get("shares"))
    if value is None or shares is None:
        return None
    cusip = str(holding.get("cusip") or "").strip().upper()
    return cusip, value, shares


def _scale_ratio_cluster(ratio: float) -> str | None:
    """Classify a price ratio without applying any acceptance thresholds."""

    if 1 / PEER_ALIGNMENT_FACTOR <= ratio <= PEER_ALIGNMENT_FACTOR:
        return "aligned_1x"
    if PEER_RATIO_MIN <= ratio <= PEER_RATIO_MAX:
        return "inflated_1000x"
    if 1 / PEER_RATIO_MAX <= ratio <= 1 / PEER_RATIO_MIN:
        return "understated_1000x"
    return None


def _adjacent_position_key(holding: dict) -> tuple[str, str] | None:
    """Return a stable cross-quarter key without excluding principal rows."""
    cusip = str(holding.get("cusip") or "").strip().upper()
    if not cusip:
        return None

    put_call = str(holding.get("put_call") or "").strip().upper()
    instrument_type = str(
        holding.get("holding_type")
        or holding.get("option_type")
        or put_call
        or "EQUITY"
    ).strip().upper()
    if put_call in {"PUT", "CALL"}:
        instrument_type = put_call
    return cusip, instrument_type


def _aggregate_adjacent_positions(
    holdings: Iterable[dict],
) -> dict[tuple[str, str], tuple[float, float]]:
    """Aggregate duplicate rows to one value and amount per position key."""
    aggregated: dict[tuple[str, str], list[float]] = {}
    for holding in holdings:
        if not isinstance(holding, dict):
            continue
        if holding.get("shares_imputed"):
            continue
        value = _positive_number(holding.get("value"))
        shares = _positive_number(holding.get("shares"))
        key = _adjacent_position_key(holding)
        if value is None or shares is None or key is None:
            continue
        totals = aggregated.setdefault(key, [0.0, 0.0])
        totals[0] += value
        totals[1] += shares
    return {
        key: (totals[0], totals[1])
        for key, totals in aggregated.items()
    }


def adjacent_quarter_scale_evidence(
    holdings: Iterable[dict],
    adjacent_holdings: Iterable[dict],
) -> dict:
    """Detect a broad, uniform value-scale break between adjacent quarters.

    ``holdings`` must still contain the current filing's raw SEC values.
    ``adjacent_holdings`` must come from a trusted, normalized exact-adjacent
    quarter; the caller remains responsible for enforcing those conditions.
    This deliberately includes principal-amount rows. A conclusion requires
    at least ten shared position keys and 80% support by both position count
    and current raw value. Mixed status requires different recognized scale
    clusters to dominate position count and raw value.
    """
    current = _aggregate_adjacent_positions(holdings)
    adjacent = _aggregate_adjacent_positions(adjacent_holdings)
    shared_keys = sorted(current.keys() & adjacent.keys())

    matched_raw_value = sum(
        (current[key][0] for key in shared_keys),
        0.0,
    )
    clusters = {
        "aligned_1x": {"positions": 0, "raw_value": 0.0},
        "inflated_1000x": {"positions": 0, "raw_value": 0.0},
        "understated_1000x": {"positions": 0, "raw_value": 0.0},
    }
    for key in shared_keys:
        current_value, current_shares = current[key]
        adjacent_value, adjacent_shares = adjacent[key]
        ratio = (
            (current_value / current_shares)
            / (adjacent_value / adjacent_shares)
        )
        cluster = _scale_ratio_cluster(ratio)
        if cluster is not None:
            clusters[cluster]["positions"] += 1
            clusters[cluster]["raw_value"] += current_value

    matched_positions = len(shared_keys)
    for cluster in clusters.values():
        cluster["count_support"] = (
            cluster["positions"] / matched_positions
            if matched_positions
            else 0.0
        )
        cluster["raw_value_support"] = (
            cluster["raw_value"] / matched_raw_value
            if matched_raw_value > 0
            else 0.0
        )

    status = None
    if matched_positions >= ADJACENT_MIN_SHARED_POSITIONS:
        for name, cluster in clusters.items():
            if (
                cluster["count_support"] >= ADJACENT_MIN_COUNT_SUPPORT
                and cluster["raw_value_support"]
                >= ADJACENT_MIN_VALUE_SUPPORT
            ):
                status = name
                break
        if status is None:
            # A mixed filing has one recognized scale dominating by row count
            # and another dominating by raw value. Unclustered price
            # dispersion alone is inconclusive, so it cannot trigger this.
            count_cluster = next(
                (
                    name
                    for name, cluster in clusters.items()
                    if cluster["count_support"]
                    >= ADJACENT_MIN_COUNT_SUPPORT
                ),
                None,
            )
            value_cluster = next(
                (
                    name
                    for name, cluster in clusters.items()
                    if cluster["raw_value_support"]
                    >= ADJACENT_MIN_VALUE_SUPPORT
                ),
                None,
            )
            if (
                count_cluster is not None
                and value_cluster is not None
                and count_cluster != value_cluster
            ):
                status = "mixed_scale_clusters"

    evidence = {
        "status": status,
        "matched_positions": matched_positions,
        "matched_raw_value": (
            int(matched_raw_value)
            if matched_raw_value.is_integer()
            else matched_raw_value
        ),
    }
    for name, cluster in clusters.items():
        prefix = name.removesuffix("_1x").removesuffix("_1000x")
        raw_value = cluster["raw_value"]
        evidence[f"{prefix}_positions"] = cluster["positions"]
        evidence[f"{prefix}_count_support"] = round(
            cluster["count_support"], 6
        )
        evidence[f"{prefix}_raw_value_support"] = round(
            cluster["raw_value_support"], 6
        )
        evidence[f"{prefix}_raw_value"] = (
            int(raw_value) if raw_value.is_integer() else raw_value
        )
    return evidence


def enforce_adjacent_value_scale(
    holdings: Iterable[dict],
    adjacent_holdings: Iterable[dict],
    proposed_multiplier: int,
) -> dict:
    """Fail closed when an adjacent quarter contradicts one uniform scale."""
    if proposed_multiplier not in {1, VALUE_UNIT_SCALE}:
        raise ValueError(f"invalid proposed multiplier {proposed_multiplier!r}")

    evidence = adjacent_quarter_scale_evidence(
        holdings,
        adjacent_holdings,
    )
    status = evidence["status"]
    if status == "mixed_scale_clusters":
        raise AmbiguousValueUnits(
            "adjacent-quarter positions contain mixed value-unit clusters "
            f"(matched={evidence['matched_positions']}, "
            f"aligned value={evidence['aligned_raw_value_support']:.3f}, "
            "understated value="
            f"{evidence['understated_raw_value_support']:.3f}, "
            f"inflated value={evidence['inflated_raw_value_support']:.3f})"
        )
    expected_multiplier = {
        "aligned_1x": 1,
        "understated_1000x": VALUE_UNIT_SCALE,
    }.get(status)
    if status == "inflated_1000x":
        raise AmbiguousValueUnits(
            "current raw values are broadly 1,000x above the adjacent quarter"
        )
    if (
        expected_multiplier is not None
        and proposed_multiplier != expected_multiplier
    ):
        raise AmbiguousValueUnits(
            "adjacent-quarter scale evidence contradicts proposed multiplier "
            f"{proposed_multiplier} (status={status}, "
            f"matched={evidence['matched_positions']})"
        )
    return evidence


def classify_value_units(
    holdings: Iterable[dict],
    peer_prices: dict[
        str, tuple[float, int] | tuple[float, int, int]
    ] | None = None,
    *,
    prior_multiplier: int | None = None,
) -> dict:
    """Classify raw filing values as dollars or thousands without mutating."""
    if prior_multiplier not in {None, 1, VALUE_UNIT_SCALE}:
        raise ValueError(f"invalid prior multiplier {prior_multiplier!r}")

    rows = list(holdings)
    raw_value_total = sum(
        (
            value
            for holding in rows
            if (value := _positive_number(holding.get("value"))) is not None
        ),
        0.0,
    )
    eligible_positions = 0
    eligible_value = 0.0
    low_price_positions = 0
    low_price_value = 0.0
    eligible_cusips: set[str] = set()
    low_price_cusips: set[str] = set()

    for holding in rows:
        row = _unit_evidence_row(holding)
        if row is None:
            continue
        cusip, value, shares = row

        eligible_positions += 1
        eligible_value += value
        if cusip:
            eligible_cusips.add(cusip)
        if value / shares < LOW_PRICE_CUTOFF:
            low_price_positions += 1
            low_price_value += value
            if cusip:
                low_price_cusips.add(cusip)

    low_price_share = (
        low_price_value / eligible_value if eligible_value > 0 else 0.0
    )
    low_price_count_support = (
        len(low_price_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    intrinsic_count_value_conflict = _has_intrinsic_count_value_conflict(
        eligible_cusips=len(eligible_cusips),
        low_price_cusips=len(low_price_cusips),
        low_price_value_share=low_price_share,
    )
    evidence = {
        "eligible_positions": eligible_positions,
        "eligible_cusips": len(eligible_cusips),
        "low_price_positions": low_price_positions,
        "low_price_cusips": len(low_price_cusips),
        "low_price_count_support": round(
            low_price_count_support, 6
        ),
        "eligible_value": (
            int(eligible_value)
            if eligible_value.is_integer()
            else eligible_value
        ),
        "low_price_value": (
            int(low_price_value)
            if low_price_value.is_integer()
            else low_price_value
        ),
        "low_price_value_share": round(low_price_share, 6),
        "intrinsic_count_value_conflict": (
            intrinsic_count_value_conflict
        ),
        "prior_multiplier": prior_multiplier,
        "raw_value_total": (
            int(raw_value_total)
            if raw_value_total.is_integer()
            else raw_value_total
        ),
    }

    peer_prices = peer_prices or {}
    peer_matched_value = 0.0
    peer_dollar_value = 0.0
    peer_thousands_value = 0.0
    peer_matched_positions = 0
    peer_matched_cusips: set[str] = set()
    peer_dollar_cusips: set[str] = set()
    peer_thousands_cusips: set[str] = set()
    exact_peer_matched_value = 0.0
    exact_peer_dollar_value = 0.0
    exact_peer_thousands_value = 0.0
    exact_peer_matched_positions = 0
    exact_peer_matched_cusips: set[str] = set()
    exact_peer_dollar_cusips: set[str] = set()
    exact_peer_thousands_cusips: set[str] = set()
    for holding in rows:
        row = _unit_evidence_row(holding)
        if row is None:
            continue
        cusip, value, shares = row
        reference = peer_prices.get(cusip)
        if not reference:
            continue
        reference_price, reference_count = reference[:2]
        distance_days = reference[2] if len(reference) >= 3 else 0
        if (
            reference_count < RUNTIME_PEER_MIN_REFERENCE_COUNT
            or reference_price <= 0
            or isinstance(distance_days, bool)
            or not isinstance(distance_days, int)
            or distance_days < 0
        ):
            continue

        ratio = (value / shares) / reference_price
        peer_matched_positions += 1
        peer_matched_cusips.add(cusip)
        peer_matched_value += value
        is_exact = distance_days == 0
        if is_exact:
            exact_peer_matched_positions += 1
            exact_peer_matched_cusips.add(cusip)
            exact_peer_matched_value += value
        cluster = _scale_ratio_cluster(ratio)
        if cluster == "aligned_1x":
            peer_dollar_value += value
            peer_dollar_cusips.add(cusip)
            if is_exact:
                exact_peer_dollar_value += value
                exact_peer_dollar_cusips.add(cusip)
        # Runtime peer prices deliberately allow the same threefold tolerance
        # around 1/1,000 used for the aligned 1x cluster. The validation
        # backstop below uses the narrower 500x-2,000x policy instead.
        if (
            1 / (VALUE_UNIT_SCALE * PEER_ALIGNMENT_FACTOR)
            <= ratio
            <= PEER_ALIGNMENT_FACTOR / VALUE_UNIT_SCALE
        ):
            peer_thousands_value += value
            peer_thousands_cusips.add(cusip)
            if is_exact:
                exact_peer_thousands_value += value
                exact_peer_thousands_cusips.add(cusip)

    peer_coverage = (
        peer_matched_value / eligible_value if eligible_value > 0 else 0.0
    )
    peer_dollar_support = (
        peer_dollar_value / peer_matched_value
        if peer_matched_value > 0
        else 0.0
    )
    peer_thousands_support = (
        peer_thousands_value / peer_matched_value
        if peer_matched_value > 0
        else 0.0
    )
    peer_dollar_count_support = (
        len(peer_dollar_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    peer_thousands_count_support = (
        len(peer_thousands_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    peer_matched_count_coverage = (
        len(peer_matched_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    exact_peer_coverage = (
        exact_peer_matched_value / eligible_value
        if eligible_value > 0
        else 0.0
    )
    exact_peer_dollar_support = (
        exact_peer_dollar_value / exact_peer_matched_value
        if exact_peer_matched_value > 0
        else 0.0
    )
    exact_peer_thousands_support = (
        exact_peer_thousands_value / exact_peer_matched_value
        if exact_peer_matched_value > 0
        else 0.0
    )
    exact_peer_dollar_count_support = (
        len(exact_peer_dollar_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    exact_peer_thousands_count_support = (
        len(exact_peer_thousands_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    exact_peer_matched_count_coverage = (
        len(exact_peer_matched_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    evidence["peer"] = {
        "matched_positions": peer_matched_positions,
        "matched_cusips": len(peer_matched_cusips),
        "matched_count_coverage": round(
            peer_matched_count_coverage, 6
        ),
        "matched_value_coverage": round(peer_coverage, 6),
        "dollar_value_support": round(peer_dollar_support, 6),
        "thousands_value_support": round(peer_thousands_support, 6),
        "dollar_count_support": round(peer_dollar_count_support, 6),
        "thousands_count_support": round(
            peer_thousands_count_support, 6
        ),
        "exact_matched_positions": exact_peer_matched_positions,
        "exact_matched_cusips": len(exact_peer_matched_cusips),
        "exact_matched_count_coverage": round(
            exact_peer_matched_count_coverage, 6
        ),
        "exact_matched_value_coverage": round(exact_peer_coverage, 6),
        "exact_dollar_value_support": round(
            exact_peer_dollar_support, 6
        ),
        "exact_thousands_value_support": round(
            exact_peer_thousands_support, 6
        ),
        "exact_dollar_count_support": round(
            exact_peer_dollar_count_support, 6
        ),
        "exact_thousands_count_support": round(
            exact_peer_thousands_count_support, 6
        ),
    }

    if intrinsic_count_value_conflict:
        # A majority of rows look 1,000x understated while a different,
        # economically dominant cluster looks like ordinary dollars. Only
        # broad same-quarter proof that those low-price rows really trade
        # below $1 can make a uniform dollar decision safe.
        if (
            exact_peer_dollar_count_support
            >= PEER_MIN_SCALE_COUNT_SUPPORT
            and len(exact_peer_dollar_cusips)
            >= PEER_MIN_SCALE_POSITIONS
        ):
            return {
                "value_unit_policy_version": VALUE_UNIT_POLICY_VERSION,
                "value_multiplier": 1,
                "value_unit_method": "same_quarter_peer_dollars",
                "value_unit_confidence": "high",
                "value_unit_evidence": evidence,
            }
        raise AmbiguousValueUnits(
            "ordinary-equity position count and raw-value evidence support "
            "different unit scales "
            f"(low-price CUSIP count="
            f"{low_price_count_support:.3f}, "
            f"low-price raw value={low_price_share:.3f}, "
            "same-quarter dollar peer count="
            f"{exact_peer_dollar_count_support:.3f})"
        )

    peer_method = None
    decisive_peer_dollar_support = 0.0
    decisive_peer_thousands_support = 0.0
    decisive_peer_dollar_count_support = 0.0
    decisive_peer_thousands_count_support = 0.0
    decisive_peer_coverage = 0.0
    if (
        exact_peer_coverage >= PEER_MIN_VALUE_COVERAGE
        and max(
            exact_peer_dollar_count_support,
            exact_peer_thousands_count_support,
        )
        >= PEER_MIN_SCALE_COUNT_SUPPORT
    ):
        peer_method = "same_quarter_peer"
        decisive_peer_dollar_support = exact_peer_dollar_support
        decisive_peer_thousands_support = exact_peer_thousands_support
        decisive_peer_dollar_count_support = (
            exact_peer_dollar_count_support
        )
        decisive_peer_thousands_count_support = (
            exact_peer_thousands_count_support
        )
        decisive_peer_coverage = exact_peer_coverage
    elif (
        len(peer_matched_cusips) >= RUNTIME_STALE_PEER_MIN_POSITIONS
        and peer_coverage >= PEER_MIN_VALUE_COVERAGE
        and max(
            peer_dollar_count_support,
            peer_thousands_count_support,
        )
        >= PEER_MIN_SCALE_COUNT_SUPPORT
    ):
        peer_method = "nearby_quarter_peers"
        decisive_peer_dollar_support = peer_dollar_support
        decisive_peer_thousands_support = peer_thousands_support
        decisive_peer_dollar_count_support = peer_dollar_count_support
        decisive_peer_thousands_count_support = (
            peer_thousands_count_support
        )
        decisive_peer_coverage = peer_coverage

    if peer_method is not None:
        peer_scale_conflict = (
            decisive_peer_dollar_count_support
            >= PEER_MIN_SCALE_COUNT_SUPPORT
            and decisive_peer_thousands_support >= PEER_MIN_SCALE_SUPPORT
        ) or (
            decisive_peer_thousands_count_support
            >= PEER_MIN_SCALE_COUNT_SUPPORT
            and decisive_peer_dollar_support >= PEER_MIN_SCALE_SUPPORT
        )
        if peer_scale_conflict:
            raise AmbiguousValueUnits(
                "same-security peer count and raw-value evidence support "
                "different unit scales "
                f"(dollar count={decisive_peer_dollar_count_support:.3f}, "
                f"dollar value={decisive_peer_dollar_support:.3f}, "
                "thousands count="
                f"{decisive_peer_thousands_count_support:.3f}, "
                f"thousands value={decisive_peer_thousands_support:.3f})"
            )
        if (
            decisive_peer_dollar_support >= PEER_MIN_SCALE_SUPPORT
            and decisive_peer_dollar_count_support
            >= PEER_MIN_SCALE_COUNT_SUPPORT
        ):
            return {
                "value_unit_policy_version": VALUE_UNIT_POLICY_VERSION,
                "value_multiplier": 1,
                "value_unit_method": f"{peer_method}_dollars",
                "value_unit_confidence": "high",
                "value_unit_evidence": evidence,
            }
        if (
            decisive_peer_thousands_support >= PEER_MIN_SCALE_SUPPORT
            and decisive_peer_thousands_count_support
            >= PEER_MIN_SCALE_COUNT_SUPPORT
        ):
            return {
                "value_unit_policy_version": VALUE_UNIT_POLICY_VERSION,
                "value_multiplier": VALUE_UNIT_SCALE,
                "value_unit_method": f"{peer_method}_thousands",
                "value_unit_confidence": "high",
                "value_unit_evidence": evidence,
            }
        raise AmbiguousValueUnits(
            "same-security peers conflict with both unit choices "
            f"(coverage={decisive_peer_coverage:.3f}, "
            f"dollars={decisive_peer_dollar_support:.3f}, "
            f"thousands={decisive_peer_thousands_support:.3f})"
        )
    if (
        peer_coverage >= PEER_MIN_VALUE_COVERAGE
        and max(peer_dollar_support, peer_thousands_support)
        >= PEER_MIN_SCALE_SUPPORT
    ):
        peer_scale_count_support = max(
            peer_dollar_count_support,
            peer_thousands_count_support,
        )
        if peer_scale_count_support < PEER_MIN_SCALE_COUNT_SUPPORT:
            raise AmbiguousValueUnits(
                "same-security peer raw-value evidence lacks broad eligible "
                "CUSIP support "
                f"(matched count coverage="
                f"{peer_matched_count_coverage:.3f}, "
                f"scale count support={peer_scale_count_support:.3f}, "
                f"value coverage={peer_coverage:.3f})"
            )
        suggested_multiplier = (
            1
            if peer_dollar_support >= PEER_MIN_SCALE_SUPPORT
            else VALUE_UNIT_SCALE
        )
        if prior_multiplier == suggested_multiplier:
            return {
                "value_unit_policy_version": VALUE_UNIT_POLICY_VERSION,
                "value_multiplier": prior_multiplier,
                "value_unit_method": "prior_confirmed_nearby_quarter_peers",
                "value_unit_confidence": "high",
                "value_unit_evidence": evidence,
            }
        raise AmbiguousValueUnits(
            "nearby-quarter peer evidence lacks enough independent "
            "securities to choose units safely "
            f"(matched CUSIPs={len(peer_matched_cusips)}, "
            f"coverage={peer_coverage:.3f})"
        )

    if prior_multiplier is not None:
        return {
            "value_unit_policy_version": VALUE_UNIT_POLICY_VERSION,
            "value_multiplier": prior_multiplier,
            "value_unit_method": "prior_unit_convention",
            # A prior convention is useful for completing the current filing,
            # but it is not independent evidence. Keep it out of the trusted
            # prior chain so one weak decision cannot propagate indefinitely.
            "value_unit_confidence": "low",
            "value_unit_evidence": evidence,
        }

    multiplier = 1
    confidence = "high"
    if eligible_value == 0:
        if raw_value_total > 0:
            raise AmbiguousValueUnits(
                "positive filing has no ordinary-equity unit evidence and no "
                "trusted prior convention"
            )
        method = "zero_value_component"
    elif low_price_share <= DOLLAR_MAX_LOW_PRICE_VALUE_SHARE:
        method = "weighted_equity_dollars"
    elif low_price_share >= THOUSANDS_MIN_LOW_PRICE_VALUE_SHARE:
        if len(low_price_cusips) >= THOUSANDS_MIN_LOW_PRICE_CUSIPS:
            raise AmbiguousValueUnits(
                "broad sub-dollar evidence lacks independent confirmation "
                f"(low-price value share={low_price_share:.3f}, "
                f"low-price CUSIPs={len(low_price_cusips)})"
            )
        raise AmbiguousValueUnits(
            "concentrated sub-dollar evidence has no trusted prior or "
            "same-quarter peer confirmation"
        )
    else:
        raise AmbiguousValueUnits(
            "ordinary-equity evidence is split between dollars and thousands "
            f"(low-price value share={low_price_share:.3f}, "
            f"eligible positions={eligible_positions})"
        )

    return {
        "value_unit_policy_version": VALUE_UNIT_POLICY_VERSION,
        "value_multiplier": multiplier,
        "value_unit_method": method,
        "value_unit_confidence": confidence,
        "value_unit_evidence": evidence,
    }


def normalize_value_units(
    holdings: list[dict],
    peer_prices: dict[
        str, tuple[float, int] | tuple[float, int, int]
    ] | None = None,
    *,
    prior_multiplier: int | None = None,
    adjacent_holdings: Iterable[dict] | None = None,
) -> dict:
    """Classify and normalize one immutable filing component in place."""
    decision = classify_value_units(
        holdings,
        peer_prices,
        prior_multiplier=prior_multiplier,
    )
    multiplier = decision["value_multiplier"]
    if adjacent_holdings is not None:
        adjacent_evidence = enforce_adjacent_value_scale(
            holdings,
            adjacent_holdings,
            multiplier,
        )
        decision["value_unit_evidence"]["adjacent_quarter"] = (
            adjacent_evidence
        )
        expected_status = (
            "aligned_1x"
            if multiplier == 1
            else "understated_1000x"
        )
        if adjacent_evidence["status"] == expected_status:
            # Exact-adjacent position evidence is independent of a carried
            # prior label. Promote a matching decision so the next quarter can
            # use it without creating an unbounded prior-only trust chain.
            decision["value_unit_method"] = (
                "adjacent_quarter_dollars"
                if multiplier == 1
                else "adjacent_quarter_thousands"
            )
            decision["value_unit_confidence"] = "high"
    if multiplier != 1:
        for holding in holdings:
            holding["value"] = holding.get("value", 0) * multiplier
    return decision


def peer_scale_evidence(
    holdings: Iterable[dict],
    peer_prices: dict[str, tuple[float, int]],
    *,
    min_scale_positions: int = PEER_MIN_SCALE_POSITIONS,
) -> dict:
    """Measure whether a quarter is consistently 1,000x away from peers.

    ``peer_prices`` maps CUSIP to ``(median implied price, observation count)``
    for the same report date. This is a validation backstop, not the primary
    normalization rule. As with adjacent-quarter evidence, a different scale
    dominating CUSIP count and raw value is reported as mixed.
    """
    eligible_value = 0.0
    matched_value = 0.0
    aligned_value = 0.0
    inflated_value = 0.0
    understated_value = 0.0
    matched_positions = 0
    aligned_positions = 0
    inflated_positions = 0
    understated_positions = 0
    eligible_cusips: set[str] = set()
    low_price_cusips: set[str] = set()
    low_price_value = 0.0
    matched_cusips: set[str] = set()
    aligned_cusips: set[str] = set()
    inflated_cusips: set[str] = set()
    understated_cusips: set[str] = set()

    for holding in holdings:
        row = _unit_evidence_row(holding)
        if row is None:
            continue
        cusip, value, shares = row
        eligible_value += value

        if cusip:
            eligible_cusips.add(cusip)
        if value / shares < LOW_PRICE_CUTOFF:
            low_price_value += value
            if cusip:
                low_price_cusips.add(cusip)
        reference = peer_prices.get(cusip)
        if not reference:
            continue
        reference_price, reference_count = reference
        if reference_count < PEER_MIN_REFERENCE_COUNT or reference_price <= 0:
            continue

        matched_positions += 1
        matched_cusips.add(cusip)
        matched_value += value
        ratio = (value / shares) / reference_price
        cluster = _scale_ratio_cluster(ratio)
        if cluster == "aligned_1x":
            aligned_positions += 1
            aligned_cusips.add(cusip)
            aligned_value += value
        elif cluster == "inflated_1000x":
            inflated_positions += 1
            inflated_cusips.add(cusip)
            inflated_value += value
        elif cluster == "understated_1000x":
            understated_positions += 1
            understated_cusips.add(cusip)
            understated_value += value

    coverage = matched_value / eligible_value if eligible_value > 0 else 0.0
    aligned_support = (
        aligned_value / matched_value if matched_value > 0 else 0.0
    )
    inflated_support = (
        inflated_value / matched_value if matched_value > 0 else 0.0
    )
    understated_support = (
        understated_value / matched_value if matched_value > 0 else 0.0
    )
    matched_cusip_count = len(matched_cusips)
    aligned_count_support = (
        len(aligned_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    inflated_count_support = (
        len(inflated_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    understated_count_support = (
        len(understated_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    matched_count_coverage = (
        matched_cusip_count / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    low_price_value_share = (
        low_price_value / eligible_value if eligible_value > 0 else 0.0
    )
    low_price_count_support = (
        len(low_price_cusips) / len(eligible_cusips)
        if eligible_cusips
        else 0.0
    )
    intrinsic_count_value_conflict = _has_intrinsic_count_value_conflict(
        eligible_cusips=len(eligible_cusips),
        low_price_cusips=len(low_price_cusips),
        low_price_value_share=low_price_value_share,
    )

    status = None
    if (
        intrinsic_count_value_conflict
        and aligned_count_support < PEER_MIN_SCALE_COUNT_SUPPORT
    ):
        status = "mixed_scale_clusters"
    elif coverage >= PEER_MIN_VALUE_COVERAGE:
        clusters = {
            "aligned_1x": (
                aligned_count_support,
                aligned_support,
            ),
            "inflated_1000x": (
                inflated_count_support,
                inflated_support,
            ),
            "understated_1000x": (
                understated_count_support,
                understated_support,
            ),
        }
        count_cluster = next(
            (
                name
                for name, (count_support, _) in clusters.items()
                if count_support >= PEER_MIN_SCALE_COUNT_SUPPORT
            ),
            None,
        )
        value_cluster = next(
            (
                name
                for name, (_, value_support) in clusters.items()
                if value_support >= PEER_MIN_SCALE_SUPPORT
            ),
            None,
        )
        if (
            count_cluster is not None
            and value_cluster is not None
            and count_cluster != value_cluster
        ):
            status = "mixed_scale_clusters"
        elif (
            len(inflated_cusips) >= min_scale_positions
            and inflated_count_support >= PEER_MIN_SCALE_COUNT_SUPPORT
            and inflated_support >= PEER_MIN_SCALE_SUPPORT
        ):
            status = "inflated_1000x"
        elif (
            len(understated_cusips) >= min_scale_positions
            and understated_count_support
            >= PEER_MIN_SCALE_COUNT_SUPPORT
            and understated_support >= PEER_MIN_SCALE_SUPPORT
        ):
            status = "understated_1000x"

    return {
        "status": status,
        "eligible_value": eligible_value,
        "eligible_cusips": len(eligible_cusips),
        "low_price_cusips": len(low_price_cusips),
        "low_price_count_support": round(
            low_price_count_support, 6
        ),
        "low_price_value_support": round(
            low_price_value_share, 6
        ),
        "intrinsic_count_value_conflict": (
            intrinsic_count_value_conflict
        ),
        "matched_positions": matched_positions,
        "matched_cusips": matched_cusip_count,
        "matched_count_coverage": round(
            matched_count_coverage, 6
        ),
        "matched_value_coverage": round(coverage, 6),
        "aligned_positions": aligned_positions,
        "aligned_cusips": len(aligned_cusips),
        "aligned_count_support": round(aligned_count_support, 6),
        "aligned_value_support": round(aligned_support, 6),
        "inflated_positions": inflated_positions,
        "inflated_cusips": len(inflated_cusips),
        "inflated_count_support": round(inflated_count_support, 6),
        "inflated_value_support": round(inflated_support, 6),
        "understated_positions": understated_positions,
        "understated_cusips": len(understated_cusips),
        "understated_count_support": round(
            understated_count_support, 6
        ),
        "understated_value_support": round(understated_support, 6),
    }
