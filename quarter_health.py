"""Fail-closed health checks for one persisted 13F quarter.

The checks in this module deliberately identify malformed quarters without
repairing them.  They are stdlib-only and accept caller-supplied peer prices so
both the offline validator and the ingestion pipeline can enforce the same
policy before publishing.
"""

from __future__ import annotations

import math
import re
import statistics
from bisect import bisect_left
from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from typing import Union

from security_identity import (
    holding_instrument_type,
    normalize_security_identifier,
)


PEER_MIN_OBSERVATIONS = 4
PEER_MIN_MATCHED_POSITIONS = 10
PEER_MIN_COVERAGE = 0.50
PEER_MAX_ALIGNED_SUPPORT = 0.20
PEER_ALIGNED_RATIO_MIN = 0.50
PEER_ALIGNED_RATIO_MAX = 2.00
PEER_SEVERE_RATIO_MIN = 0.10
PEER_SEVERE_RATIO_MAX = 10.00

DUPLICATED_COLUMN_MIN_POSITIONS = 10
DUPLICATED_COLUMN_MIN_SUPPORT = 0.90

_PEER_PRICE_INSTRUMENT_TYPES = frozenset({"EQUITY", "PREF"})
_WARRANT_CLASS_RE = re.compile(
    r"(?:^|\s)(?:\*?W(?:T|TS)?(?:\s+EXP)?|WARRANTS?)(?:\s|$)",
    re.IGNORECASE,
)

PositionKey = tuple[str, str]
PeerIndexKey = tuple[str, str, str]
PeerPriceReference = tuple[float, int]
PeerPriceReferences = Mapping[
    Union[PositionKey, str],
    PeerPriceReference,
]
PeerPriceIndex = MutableMapping[
    PeerIndexKey,
    MutableMapping[str, list[float]],
]
CompiledPeerPriceIndex = Mapping[
    PeerIndexKey,
    tuple[tuple[float, str], ...],
]


@dataclass(frozen=True)
class QuarterHealthIssue:
    """One deterministic reason a quarter must not be published."""

    code: str
    detail: str


def _positive_finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def is_peer_price_holding(holding: object) -> bool:
    """Return whether a row can provide an ordinary share-price observation.

    Options, warrants, imputed shares, and principal-amount rows are excluded.
    Preferred shares remain eligible because their reported amount is shares,
    while their distinct CUSIP prevents them from voting on common-stock prices.
    """

    if not isinstance(holding, Mapping) or holding.get("shares_imputed"):
        return False
    if holding_instrument_type(holding) not in _PEER_PRICE_INSTRUMENT_TYPES:
        return False

    amount_type = str(
        holding.get("share_amount_type")
        or holding.get("amount_type")
        or holding.get("sshPrnamtType")
        or ""
    ).strip().upper()
    if amount_type == "PRN":
        return False
    if _WARRANT_CLASS_RE.search(str(holding.get("class") or "")):
        return False

    return (
        bool(normalize_security_identifier(holding.get("cusip")))
        and _positive_finite_number(holding.get("value")) is not None
        and _positive_finite_number(holding.get("shares")) is not None
    )


def _eligible_positions(
    holdings: Iterable[object],
) -> dict[PositionKey, tuple[float, float]]:
    """Aggregate duplicate rows to one independently priced security."""

    totals: dict[PositionKey, list[float]] = {}
    for holding in holdings:
        if not is_peer_price_holding(holding):
            continue
        assert isinstance(holding, Mapping)
        cusip = normalize_security_identifier(holding.get("cusip"))
        key = (cusip, holding_instrument_type(holding))
        value = _positive_finite_number(holding.get("value"))
        shares = _positive_finite_number(holding.get("shares"))
        if value is None or shares is None:
            continue
        position = totals.setdefault(key, [0.0, 0.0])
        position[0] += value
        position[1] += shares
    return {
        key: (value, shares)
        for key, (value, shares) in totals.items()
        if value > 0 and shares > 0
    }


def structural_quarter_health_issues(
    quarter: Mapping[str, object],
) -> list[QuarterHealthIssue]:
    """Return fail-closed count and duplicated-column findings."""

    holdings = quarter.get("holdings")
    if not isinstance(holdings, list):
        return [
            QuarterHealthIssue(
                "invalid_holdings",
                "holdings must be a list",
            )
        ]

    issues: list[QuarterHealthIssue] = []
    num_holdings = quarter.get("num_holdings")
    if type(num_holdings) is not int or num_holdings != len(holdings):
        issues.append(
            QuarterHealthIssue(
                "holding_count_mismatch",
                f"num_holdings={num_holdings!r} does not match "
                f"holdings length {len(holdings)}",
            )
        )

    eligible = _eligible_positions(holdings)
    duplicated_positions = sum(
        1 for value, shares in eligible.values() if value == shares
    )
    duplicated_support = (
        duplicated_positions / len(eligible) if eligible else 0.0
    )
    if (
        len(eligible) >= DUPLICATED_COLUMN_MIN_POSITIONS
        and duplicated_support >= DUPLICATED_COLUMN_MIN_SUPPORT
    ):
        issues.append(
            QuarterHealthIssue(
                "duplicated_value_share_columns",
                "value equals shares for "
                f"{duplicated_positions}/{len(eligible)} eligible positions "
                f"({duplicated_support:.1%}); the filing-wide columns appear "
                "duplicated",
            )
        )
    return issues


def _lookup_peer_reference(
    peer_prices: PeerPriceReferences,
    key: PositionKey,
) -> PeerPriceReference | None:
    reference = peer_prices.get(key)
    if reference is None and key[1] == "EQUITY":
        # Compatibility with the ingestion peer loader, whose keys are CUSIPs.
        reference = peer_prices.get(key[0])
    if (
        not isinstance(reference, tuple)
        or len(reference) < 2
        or isinstance(reference[1], bool)
        or not isinstance(reference[1], int)
        or reference[1] < PEER_MIN_OBSERVATIONS
    ):
        return None
    price = _positive_finite_number(reference[0])
    if price is None:
        return None
    return price, reference[1]


def peer_price_quarter_health_issue(
    quarter: Mapping[str, object],
    peer_prices: PeerPriceReferences,
) -> QuarterHealthIssue | None:
    """Detect a broad quarter-wide price distortion against exact-date peers."""

    holdings = quarter.get("holdings")
    if not isinstance(holdings, list):
        return None
    positions = _eligible_positions(holdings)
    if not positions:
        return None

    ratios: list[float] = []
    for key, (value, shares) in positions.items():
        reference = _lookup_peer_reference(peer_prices, key)
        if reference is None:
            continue
        reference_price, _observation_count = reference
        ratio = (value / shares) / reference_price
        if math.isfinite(ratio) and ratio > 0:
            ratios.append(ratio)

    matched_positions = len(ratios)
    coverage = matched_positions / len(positions)
    if (
        matched_positions < PEER_MIN_MATCHED_POSITIONS
        or coverage < PEER_MIN_COVERAGE
    ):
        return None

    aligned_positions = sum(
        PEER_ALIGNED_RATIO_MIN <= ratio <= PEER_ALIGNED_RATIO_MAX
        for ratio in ratios
    )
    aligned_support = aligned_positions / matched_positions
    median_ratio = statistics.median(ratios)
    if (
        aligned_support > PEER_MAX_ALIGNED_SUPPORT
        or (
            median_ratio >= PEER_SEVERE_RATIO_MIN
            and median_ratio <= PEER_SEVERE_RATIO_MAX
        )
    ):
        return None

    direction = "below" if median_ratio < PEER_SEVERE_RATIO_MIN else "above"
    return QuarterHealthIssue(
        "peer_price_distortion",
        "quarter price-per-share is broadly distorted: "
        f"median ratio {median_ratio:.6g}x {direction} same-date peers, "
        f"aligned={aligned_positions}/{matched_positions} "
        f"({aligned_support:.1%}), coverage={matched_positions}/"
        f"{len(positions)} ({coverage:.1%})",
    )


def quarter_health_issues(
    quarter: Mapping[str, object],
    *,
    peer_prices: PeerPriceReferences | None = None,
) -> list[QuarterHealthIssue]:
    """Return every reason a quarter must be quarantined before publication."""

    issues = structural_quarter_health_issues(quarter)
    if peer_prices is not None:
        peer_issue = peer_price_quarter_health_issue(quarter, peer_prices)
        if peer_issue is not None:
            issues.append(peer_issue)
    return issues


def add_quarter_peer_observations(
    index: PeerPriceIndex,
    *,
    filer_id: object,
    quarter: Mapping[str, object],
) -> None:
    """Add one filer's aggregated same-date prices to a corpus peer index."""

    report_date = quarter.get("report_date")
    holdings = quarter.get("holdings")
    if not isinstance(report_date, str) or not isinstance(holdings, list):
        return
    normalized_filer_id = str(filer_id)
    for (cusip, instrument_type), (value, shares) in _eligible_positions(
        holdings
    ).items():
        by_filer = index.setdefault(
            (report_date, cusip, instrument_type),
            {},
        )
        by_filer.setdefault(normalized_filer_id, []).append(value / shares)


def compile_peer_price_index(
    index: Mapping[
        PeerIndexKey,
        Mapping[str, Iterable[float]],
    ],
    *,
    consume: bool = False,
) -> dict[PeerIndexKey, tuple[tuple[float, str], ...]]:
    """Compact raw observations into sorted one-price-per-filer references.

    Corpus callers can set ``consume`` to release the much larger raw mapping
    incrementally while the compact index is built.
    """

    if consume and not isinstance(index, MutableMapping):
        raise TypeError("consume=True requires a mutable peer-price index")

    def source_items() -> Iterable[
        tuple[PeerIndexKey, Mapping[str, Iterable[float]]]
    ]:
        if not consume:
            yield from index.items()
            return
        assert isinstance(index, MutableMapping)
        while index:
            key = next(iter(index))
            yield key, index.pop(key)

    compiled: dict[PeerIndexKey, tuple[tuple[float, str], ...]] = {}
    for key, by_filer in source_items():
        observations: list[tuple[float, str]] = []
        for filer_id, prices in by_filer.items():
            valid_prices = [
                float(price)
                for price in prices
                if _positive_finite_number(price) is not None
            ]
            if valid_prices:
                observations.append(
                    (statistics.median(valid_prices), filer_id)
                )
        if observations:
            compiled[key] = tuple(sorted(observations))
    return compiled


def _median_excluding_target(
    observations: tuple[tuple[float, str], ...],
    *,
    target_price: float,
    filer_id: str,
) -> tuple[float, int] | None:
    """Return the exact leave-one-filer-out median in logarithmic time."""

    target = (target_price, filer_id)
    target_index = bisect_left(observations, target)
    if (
        target_index >= len(observations)
        or observations[target_index] != target
    ):
        target_index = -1

    remaining_count = len(observations) - (target_index >= 0)
    if remaining_count < PEER_MIN_OBSERVATIONS:
        return None

    def remaining_price(index: int) -> float:
        source_index = (
            index + 1
            if target_index >= 0 and index >= target_index
            else index
        )
        return observations[source_index][0]

    middle = remaining_count // 2
    if remaining_count % 2:
        median = remaining_price(middle)
    else:
        median = (
            remaining_price(middle - 1) + remaining_price(middle)
        ) / 2
    return median, remaining_count


def same_date_peer_price_references(
    index: CompiledPeerPriceIndex,
    *,
    filer_id: object,
    quarter: Mapping[str, object],
) -> dict[PositionKey, PeerPriceReference]:
    """Return exact-date medians from at least four other independent filers."""

    report_date = quarter.get("report_date")
    holdings = quarter.get("holdings")
    if not isinstance(report_date, str) or not isinstance(holdings, list):
        return {}

    normalized_filer_id = str(filer_id)
    references: dict[PositionKey, PeerPriceReference] = {}
    for position_key, (value, shares) in _eligible_positions(holdings).items():
        observations = index.get((report_date, *position_key), ())
        reference = _median_excluding_target(
            observations,
            target_price=value / shares,
            filer_id=normalized_filer_id,
        )
        if reference is None:
            continue
        references[position_key] = reference
    return references


__all__ = [
    "DUPLICATED_COLUMN_MIN_POSITIONS",
    "DUPLICATED_COLUMN_MIN_SUPPORT",
    "CompiledPeerPriceIndex",
    "PEER_MIN_COVERAGE",
    "PEER_MIN_MATCHED_POSITIONS",
    "PEER_MIN_OBSERVATIONS",
    "PeerPriceIndex",
    "PeerPriceReference",
    "PeerPriceReferences",
    "PositionKey",
    "QuarterHealthIssue",
    "add_quarter_peer_observations",
    "compile_peer_price_index",
    "is_peer_price_holding",
    "peer_price_quarter_health_issue",
    "quarter_health_issues",
    "same_date_peer_price_references",
    "structural_quarter_health_issues",
]
