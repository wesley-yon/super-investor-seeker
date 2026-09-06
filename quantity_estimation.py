"""Evidence-bound estimates of missing 13F quantities.

SEC-reported quantities remain authoritative. Market prices are a separate input
from security-master resolution; this module never assigns a ticker. Every
estimate binds a frozen price observation, so validation does not silently move
its goalposts when unrelated filings arrive.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import math
import os
import re
import tempfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse
import statistics

from composition_integrity import canonical_json_hash
from security_identity import holding_instrument_type
from security_master_migration import economic_positions_for_fund


POLICY_VERSION = 1
ROOT = Path(__file__).resolve().parent
DEFAULT_EVIDENCE_PATH = ROOT / ".cache/quantity_estimation_evidence.json"
DEFAULT_MARKET_PATH = ROOT / ".cache/quarter_close_prices.json"
DEFAULT_REQUEST_PATH = ROOT / ".cache/quarter_close_price_requests.json"
MIN_PEER_FILERS = 3
MIN_PEER_AGREEMENT = 0.8
SUPPORTED_UNITS = {"EQUITY": "SH", "NOTE": "PRN", "CALL": "SH", "PUT": "SH"}
QUANTITY_BASES = {"EQUITY": "shares", "NOTE": "principal_usd", "CALL": "underlying_shares", "PUT": "underlying_shares"}
PEER_TOLERANCES = {"EQUITY": 0.01, "NOTE": 0.05, "CALL": 0.01, "PUT": 0.01}


class QuantityEstimationError(ValueError):
    """An estimate cannot be safely prepared, applied, or reproduced."""


def positive_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def load_book(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text())
    except FileNotFoundError:
        return {"schema_version": POLICY_VERSION, "references": {}, "reported_rows": {}}
    if not isinstance(payload, dict) or payload.get("schema_version") != POLICY_VERSION:
        raise QuantityEstimationError(f"invalid quantity evidence schema: {path}")
    if not isinstance(payload.get("references", {}), dict):
        raise QuantityEstimationError(f"invalid quantity references: {path}")
    return payload


def atomic_json(path: Path, payload: object) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"), allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def quarter_close_date(report_date: str) -> str:
    """US quarter-end session, including a Good Friday quarter boundary.

    Only canonical quarter ends are admitted. The currently supported corpus
    starts in 2010; a quote must still exist on this precise session and carry
    a supported US listing identity. Never substitute an older security trade.
    """
    day = date.fromisoformat(report_date)
    if day.year < 2010 or day.month not in {3, 6, 9, 12} or day.day != calendar.monthrange(day.year, day.month)[1]:
        raise QuantityEstimationError("unsupported quarter-end date")
    year = day.year
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    n = h + l - 7 * m + 114
    good_friday = date(year, n // 31, n % 31 + 1) - timedelta(days=2)
    while day.weekday() >= 5 or day == good_friday:
        day -= timedelta(days=1)
    return day.isoformat()


def observation_key(cik: object, report_date: str, holding: Mapping[str, Any]) -> str:
    """Stable identity for a reported row, independent of a derived quantity."""
    return canonical_json_hash({
        "cik": str(cik), "report_date": report_date,
        "cusip": holding.get("reported_cusip", holding.get("cusip")),
        "type": holding_instrument_type(holding),
        "accession": holding.get("accession"),
        "issuer": holding.get("reported_issuer"),
        "class": holding.get("reported_class"),
        "figi": holding.get("reported_figi"),
        "value": holding.get("value"),
        "reported_shares": 0 if holding.get("shares_imputed") else holding.get("shares", 0),
    })


def is_quantity_target(holding: Mapping[str, Any]) -> bool:
    return positive_number(holding.get("value")) and (
        holding.get("shares_imputed") is True or holding.get("shares") == 0
    )


def _read_targets(path: Path) -> list[dict[str, Any]]:
    raw = path.read_bytes()
    fund = json.loads(raw)
    targets = []
    file_sha256 = None
    for qi, quarter in enumerate(fund["quarters"]):
        for hi, holding in enumerate(quarter["holdings"]):
            if is_quantity_target(holding):
                if file_sha256 is None:
                    file_sha256 = hashlib.sha256(raw).hexdigest()
                targets.append({
                    "file": path.name, "file_sha256": file_sha256,
                    "cik": str(fund["cik"]), "quarter_index": qi, "holding_index": hi,
                    "report_date": quarter["report_date"], "holding": holding,
                })
    return targets


def collect_targets(funds_dir: Path) -> list[dict[str, Any]]:
    workers = min(6, os.cpu_count() or 1)
    rows = []
    paths = sorted(Path(funds_dir).glob("*.json"))
    if len(paths) < 32:
        return [row for path in paths for row in _read_targets(path)]
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for batch in executor.map(_read_targets, paths, chunksize=4):
            rows.extend(batch)
    return rows


def cache_dir_for_funds(funds_dir: Path) -> Path:
    parent = Path(funds_dir).parent
    return (parent.parent if parent.name == "data" else parent) / ".cache"


def _source_for_holding(holding: Mapping[str, Any], quarter: Mapping[str, Any]) -> dict | None:
    accession = holding.get("accession")
    candidates = []
    for source in quarter.get("reported_identity_sources", []):
        if not isinstance(source, dict) or source.get("accession") != accession:
            continue
        url = source.get("url", source.get("source_url"))
        sha = source.get("sha256", source.get("source_sha256"))
        if isinstance(url, str) and urlparse(url).hostname in {"www.sec.gov", "sec.gov"} and isinstance(sha, str) and len(sha) == 64:
            candidates.append({"accession": accession, "url": url, "sha256": sha})
    if not candidates:
        return None
    return min(candidates, key=lambda row: (row["url"], row["sha256"]))


_PEER_TARGET_KEYS: set[tuple[str, str, str]] = set()
_PEER_POSITION_KEYS: set[tuple[str, str]] = set()


def _initialize_peer_targets(keys: set[tuple[str, str, str]]) -> None:
    global _PEER_TARGET_KEYS, _PEER_POSITION_KEYS
    _PEER_TARGET_KEYS = keys
    _PEER_POSITION_KEYS = {(day, cusip) for day, cusip, _kind in keys}


def _read_peer_file(path: Path) -> list:
    fund = json.loads(path.read_text())
    rows = []
    for quarter in fund["quarters"]:
        for holding in quarter["holdings"]:
            report_date, cusip = quarter["report_date"], holding.get("cusip")
            if (report_date, cusip) not in _PEER_POSITION_KEYS:
                continue
            key = (report_date, cusip, holding_instrument_type(holding))
            if key not in _PEER_TARGET_KEYS:
                continue
            value, quantity = holding.get("value"), holding.get("shares")
            if "shares_imputed" in holding or holding.get("quantity_unknown") or not positive_number(value) or not positive_number(quantity):
                continue
            unit = holding.get("share_amount_type")
            if unit != SUPPORTED_UNITS.get(key[2]):
                continue
            source = _source_for_holding(holding, quarter)
            if source is None:
                continue
            rows.append((key, {
                "cik": str(fund["cik"]), "report_date": key[0], "cusip": key[1],
                "instrument_type": key[2], "unit": unit, "value": value,
                "quantity": quantity, "source": source,
            }))
    return rows


def collect_peer_observations(funds_dir: Path, target_keys: set[tuple[str, str, str]]) -> dict:
    """Read only reported, unit-labelled, SEC-provenanced candidate peers."""
    observations = defaultdict(list)
    paths = sorted(Path(funds_dir).glob("*.json"))
    if not target_keys:
        return {}
    if len(paths) < 32:
        _initialize_peer_targets(target_keys)
        batches = map(_read_peer_file, paths)
        for rows in batches:
            for key, row in rows:
                observations[key].append(row)
    else:
        with ProcessPoolExecutor(max_workers=min(6, os.cpu_count() or 1), initializer=_initialize_peer_targets, initargs=(target_keys,)) as executor:
            for rows in executor.map(_read_peer_file, paths, chunksize=4):
                for key, row in rows:
                    observations[key].append(row)
    return dict(observations)


def peer_reference(key: tuple[str, str, str], observations: list[dict], *, exclude_cik: str | None = None) -> dict | None:
    report_date, cusip, kind = key
    unit = SUPPORTED_UNITS.get(kind)
    if unit is None:
        return None
    by_filer = defaultdict(list)
    for row in observations:
        if exclude_cik is not None and row.get("cik") == exclude_cik:
            continue
        if (row.get("report_date"), row.get("cusip"), row.get("instrument_type")) != key or row.get("unit") != unit:
            continue
        if positive_number(row.get("value")) and positive_number(row.get("quantity")):
            by_filer[row["cik"]].append(row)
    peers = []
    tolerance = PEER_TOLERANCES[kind]
    for cik, rows in sorted(by_filer.items()):
        prices = [row["value"] / row["quantity"] for row in rows]
        middle = statistics.median(prices)
        if max(abs(price / middle - 1) for price in prices) > tolerance:
            continue
        # One equally weighted observation per filer, regardless of row splits.
        peers.append({"cik": cik, "price": middle, "observations": rows})
    if len(peers) < MIN_PEER_FILERS:
        return None
    middle = statistics.median(row["price"] for row in peers)
    accepted = [row for row in peers if abs(row["price"] / middle - 1) <= tolerance]
    if len(accepted) < MIN_PEER_FILERS or len(accepted) / len(peers) < MIN_PEER_AGREEMENT:
        return None
    # Preserve a small, exact median witness. Symmetric trimming with the same
    # parity leaves the median unchanged, while avoiding copying hundreds of
    # complete filings into every target's receipt. All peers are screened;
    # the complete screening input remains bound by its content digest.
    accepted.sort(key=lambda row: (row["price"], row["cik"]))
    witness_count = min(len(accepted), 6 if len(accepted) % 2 == 0 else 5)
    start = (len(accepted) - witness_count) // 2
    witnesses = accepted[start:start + witness_count]
    return {
        "policy_version": POLICY_VERSION, "method": "sec_same_quarter_median",
        "report_date": report_date, "price_date": quarter_close_date(report_date),
        "cusip": cusip, "instrument_type": kind, "unit": unit, "currency": "USD",
        "quantity_basis": QUANTITY_BASES[kind],
        "price": statistics.median(row["price"] for row in accepted),
        "peer_count": len(witnesses), "candidate_peer_count": len(peers),
        "screened_inlier_count": len(accepted),
        "screening_price": middle,
        "screened_observations_sha256": canonical_json_hash(peers),
        "peers": witnesses, "excluded_peer_count": len(peers) - len(accepted),
        "excluded_target_cik": exclude_cik,
    }


def validate_reference(reference: Mapping[str, Any]) -> list[str]:
    try:
        return _validate_reference(reference)
    except (TypeError, ValueError, KeyError, AttributeError, ZeroDivisionError, OverflowError):
        return ["malformed quantity reference"]


def _validate_reference(reference: Mapping[str, Any]) -> list[str]:
    errors = []
    if reference.get("policy_version") != POLICY_VERSION:
        errors.append("unsupported policy version")
    kind = reference.get("instrument_type")
    if kind not in SUPPORTED_UNITS or reference.get("unit") != SUPPORTED_UNITS.get(kind):
        errors.append("unsupported security or quantity unit")
    if reference.get("quantity_basis") != QUANTITY_BASES.get(kind):
        errors.append("incompatible quantity basis")
    if not positive_number(reference.get("price")) or reference.get("currency") != "USD":
        errors.append("invalid USD price")
    try:
        if reference.get("price_date") != quarter_close_date(reference["report_date"]):
            errors.append("price is not from the quarter-end session")
    except (ValueError, KeyError, TypeError):
        errors.append("invalid report date")
    method = reference.get("method")
    if method == "sec_same_quarter_median":
        prices, ciks = [], set()
        key = (reference.get("report_date"), reference.get("cusip"), kind)
        tolerance = PEER_TOLERANCES.get(kind, 0)
        for peer in reference.get("peers", []):
            if peer.get("cik") in ciks or peer.get("cik") == reference.get("excluded_target_cik"):
                errors.append("peer filer is duplicated or is the target filer")
            ciks.add(peer.get("cik"))
            row_prices = []
            for row in peer.get("observations", []):
                source = row.get("source", {})
                if urlparse(str(source.get("url", ""))).hostname not in {"www.sec.gov", "sec.gov"} or len(str(source.get("sha256", ""))) != 64:
                    errors.append("peer lacks SEC provenance")
                if (row.get("report_date"), row.get("cusip"), row.get("instrument_type")) != key or row.get("unit") != SUPPORTED_UNITS.get(kind) or row.get("cik") != peer.get("cik"):
                    errors.append("peer security, date, filer, or unit does not match")
                if not positive_number(row.get("value")) or not positive_number(row.get("quantity")):
                    errors.append("invalid reported peer quantity or value")
                    continue
                row_prices.append(row["value"] / row["quantity"])
            if not row_prices:
                errors.append("peer has no reported observations")
                continue
            price = statistics.median(row_prices)
            if peer.get("price") != price or any(abs(p / price - 1) > tolerance for p in row_prices):
                errors.append("peer implied price cannot be reproduced")
            prices.append(price)
        screening = reference.get("screening_price")
        if not positive_number(screening) or any(abs(p / screening - 1) > tolerance for p in prices):
            errors.append("peer price lies outside its screened agreement range")
        if len(prices) < MIN_PEER_FILERS or len(prices) != reference.get("peer_count") or statistics.median(prices) != reference.get("price"):
            errors.append("peer price cannot be reproduced")
        candidate_count = reference.get("candidate_peer_count", 0)
        accepted_count = reference.get("screened_inlier_count", 0)
        if not isinstance(candidate_count, int) or candidate_count < accepted_count or not candidate_count or accepted_count / candidate_count < MIN_PEER_AGREEMENT:
            errors.append("insufficient peer agreement")
        if accepted_count < reference.get("peer_count", 0) or len(str(reference.get("screened_observations_sha256", ""))) != 64:
            errors.append("invalid peer screening receipt")
    elif method == "fiscal_ai_quarter_close":
        if kind not in {"EQUITY", "CALL", "PUT"}:
            errors.append("stock price cannot estimate a debt or other security quantity")
        if reference.get("price_basis") != "quarter_end_unadjusted":
            errors.append("unsupported price adjustment basis")
        if not isinstance(reference.get("provider_response_sha256"), str) or len(reference["provider_response_sha256"]) != 64:
            errors.append("missing provider response checksum")
        if not reference.get("listing_id") or not reference.get("company_key") or not reference.get("sec_identity"):
            errors.append("missing exact listing identity")
        identity = reference.get("sec_identity", {})
        interval = identity.get("interval", {})
        listing = reference.get("provider_listing", {})
        if (
            identity.get("cusip") != reference.get("cusip")
            or identity.get("instrument_type") != "EQUITY"
            or identity.get("ticker_source") != "sec_ftd"
            or identity.get("ticker") != listing.get("ticker")
            or interval.get("symbol") != identity.get("ticker")
            or not interval.get("first_seen", "9999") <= reference.get("report_date", "") <= interval.get("last_seen", "")
            or listing.get("tradingCurrency") != "USD"
            or listing.get("operatingMic") not in {"XNAS", "XNYS", "XASE", "ARCX", "BATS", "IEXG"}
            or listing.get("listingFiscalIdentifier") != reference.get("listing_id")
            or reference.get("company_key") != f"{listing.get('exchangeCode')}_{listing.get('ticker')}"
        ):
            errors.append("market listing does not match the dated SEC identity")
        sources = interval.get("sources", [])
        if not sources or any(
            urlparse(str(source.get("url", ""))).hostname not in {"www.sec.gov", "sec.gov"}
            or re.fullmatch(r"[0-9a-f]{64}", str(source.get("sha256", ""))) is None
            for source in sources
        ):
            errors.append("market identity lacks SEC source receipts")
        if reference.get("volume", 0) <= 0 or not positive_number(reference.get("split_adjustment_factor")):
            errors.append("missing traded close or invalid split adjustment")
        adjusted = reference.get("split_adjusted_close")
        factor = reference.get("split_adjustment_factor")
        if not positive_number(adjusted) or not positive_number(factor) or not math.isclose(adjusted * factor, reference.get("price", 0), rel_tol=1e-12):
            errors.append("unadjusted close cannot be reproduced")
        try:
            expected_factor = split_adjustment_factor(
                reference.get("split_history", []),
                reference["price_date"], reference["series_through"],
            )
            if factor != expected_factor:
                errors.append("split factor does not match the provider history")
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(f"unverified split basis: {exc}")
    else:
        errors.append("unsupported estimation method")
    return errors


def split_adjustment_factor(splits: list[dict], price_date: str, series_through: str) -> float:
    """Undo documented later forward splits; ambiguous event units fail closed."""
    date.fromisoformat(price_date)
    date.fromisoformat(series_through)
    if series_through < price_date:
        raise QuantityEstimationError("price date exceeds the provider series")
    events = {}
    for split in splits:
        ex_date = split.get("exDate")
        if not isinstance(ex_date, str):
            raise QuantityEstimationError("split history has an undated event")
        date.fromisoformat(ex_date)
        if not price_date < ex_date <= series_through:
            continue
        # The connector documents split-adjusted prices but does not define
        # reverse-split/stock-dividend rate conventions. Do not guess them.
        rate = split.get("rate")
        if split.get("splitType") != "Stock Split" or not positive_number(rate) or rate < 1:
            raise QuantityEstimationError("later corporate-action rate needs explicit verification")
        if ex_date in events and events[ex_date] != rate:
            raise QuantityEstimationError("conflicting split events")
        events[ex_date] = rate
    return math.prod(events.values())


def fiscal_reference(export: Mapping[str, Any], request: Mapping[str, Any], company: Mapping[str, Any]) -> dict:
    """Admit a connector export only for the exact preverified US listing."""
    if export.get("error"):
        raise QuantityEstimationError(str(export["error"]))
    listing = export.get("listing", {})
    if export.get("companyKey") != company.get("companyKey"):
        raise QuantityEstimationError("provider company does not match its request")
    if listing.get("ticker") != company.get("ticker") or listing.get("operatingMic") != company.get("micCode"):
        raise QuantityEstimationError("provider returned another listing")
    if company.get("tradingCurrency") != "USD" or listing.get("tradingCurrency") != "USD":
        raise QuantityEstimationError("quantity estimation requires a USD quote")
    price_date = quarter_close_date(request["report_date"])
    points = [point for point in export.get("prices", []) if point.get("date") == price_date]
    if len(points) != 1 or not positive_number(points[0].get("closePrice")) or not positive_number(points[0].get("volume")):
        raise QuantityEstimationError("no unique traded close on the quarter-end session")
    point = points[0]
    factor = split_adjustment_factor(export.get("splits", []), price_date, export["seriesThrough"])
    reference = {
        "policy_version": POLICY_VERSION, "method": "fiscal_ai_quarter_close",
        "report_date": request["report_date"], "price_date": price_date,
        "cusip": request["cusip"], "instrument_type": "EQUITY", "unit": "SH",
        "quantity_basis": "shares", "currency": "USD",
        "price": point["closePrice"] * factor,
        "price_basis": "quarter_end_unadjusted",
        "price_conversion": "undo_later_forward_splits" if factor != 1 else "none",
        "split_adjusted_close": point["closePrice"], "split_adjustment_factor": factor,
        "split_history": export.get("splits", []), "series_through": export["seriesThrough"],
        "volume": point["volume"], "listing_id": listing.get("listingFiscalIdentifier"),
        "company_key": company["companyKey"], "provider_listing": listing,
        "provider_response_sha256": canonical_json_hash(export),
        "retrieved_at": export.get("fetchedAt"), "sec_identity": request["sec_identity"],
    }
    errors = validate_reference(reference)
    if errors:
        raise QuantityEstimationError("; ".join(errors))
    return reference


def validate_quantity_annotation(holding: Mapping[str, Any], report_date: str, evidence: Mapping[str, Any], *, cik: str | None = None) -> list[str]:
    annotation = holding.get("quantity_estimate")
    unknown = holding.get("quantity_unknown")
    imputed = holding.get("shares_imputed") is True
    if unknown is not None and unknown is not True:
        return ["invalid quantity_unknown marker"]
    if unknown:
        if imputed or annotation is not None or holding.get("shares") != 0 or holding.get("reported_shares") != 0 or not holding.get("quantity_unknown_reason"):
            return ["unknown quantity must preserve reported zero without an estimate"]
        return []
    if not imputed:
        return ["quantity estimate lacks estimated marker"] if annotation is not None else []
    if holding.get("reported_shares") != 0 or not isinstance(annotation, dict):
        return ["estimated quantity lacks reported zero or evidence"]
    reference_id = annotation.get("reference_id")
    reference = evidence.get("references", {}).get(reference_id)
    if not isinstance(reference, dict) or canonical_json_hash(reference) != reference_id:
        return ["missing or changed quantity reference"]
    errors = validate_reference(reference)
    reported = evidence.get("reported_rows", {}).get(observation_key(cik, report_date, holding), {}) if cik is not None else {}
    unit = holding.get("share_amount_type") or reported.get("unit")
    if unit != reference.get("unit"):
        errors.append("estimated quantity unit lacks matching reported evidence")
    if reference.get("report_date") != report_date or reference.get("cusip") != holding.get("cusip") or reference.get("instrument_type") != holding_instrument_type(holding):
        errors.append("quantity reference is for a different security or quarter")
    if reference.get("method") == "sec_same_quarter_median" and cik is not None and reference.get("excluded_target_cik") != str(cik):
        errors.append("peer estimate did not exclude the target filer")
    if annotation != {"policy_version": POLICY_VERSION, "reference_id": reference_id, "method": reference.get("method"), "unit": reference.get("unit")}:
        errors.append("quantity annotation disagrees with its evidence")
    price = reference.get("price")
    if not positive_number(holding.get("value")) or not positive_number(holding.get("shares")) or holding.get("shares", 0) < 1:
        errors.append("estimate requires positive value and at least one quantity unit")
    if positive_number(price) and (holding.get("value", 0) < price or holding.get("shares") != round(holding.get("value", 0) / price, 6)):
        errors.append("estimated quantity does not reproduce its frozen reference")
    return errors


def build_plan(funds_dir: Path, *, evidence_path: Path = DEFAULT_EVIDENCE_PATH, market_path: Path = DEFAULT_MARKET_PATH) -> dict:
    targets = collect_targets(funds_dir)
    old_book = load_book(evidence_path)
    market_book = load_book(market_path)
    keys = {(row["report_date"], row["holding"]["cusip"], holding_instrument_type(row["holding"])) for row in targets}
    observations = collect_peer_observations(funds_dir, keys)
    peers = {}
    market = {}
    for reference_id, reference in market_book.get("references", {}).items():
        errors = validate_reference(reference)
        if canonical_json_hash(reference) != reference_id or errors:
            raise QuantityEstimationError(f"invalid market reference {reference_id}: {errors}")
        key = (reference["report_date"], reference["cusip"], reference["instrument_type"])
        if key in market and market[key] != reference:
            raise QuantityEstimationError(f"conflicting market references for {key}")
        market[key] = reference
    evidence = {"schema_version": POLICY_VERSION, "references": dict(old_book.get("references", {})), "reported_rows": old_book.get("reported_rows", {})}
    changes, requests = [], {}
    for target in targets:
        holding = target["holding"]
        key = (target["report_date"], holding["cusip"], holding_instrument_type(holding))
        reported = evidence["reported_rows"].get(observation_key(target["cik"], target["report_date"], holding), {})
        unit = holding.get("share_amount_type") or reported.get("unit")
        reason = "insufficient_same_quarter_evidence"
        reference = None
        if key[2] not in SUPPORTED_UNITS:
            reason = "instrument_requires_separate_quantity_model"
        elif unit != SUPPORTED_UNITS[key[2]]:
            reason = "unverified_or_incompatible_quantity_unit"
        else:
            peer_key = (key, target["cik"])
            if peer_key not in peers:
                peers[peer_key] = peer_reference(key, observations.get(key, []), exclude_cik=target["cik"])
            market_key = (key[0], key[1], "EQUITY") if key[2] in {"CALL", "PUT"} else key
            reference = market.get(market_key)
            if reference is not None and key[2] in {"CALL", "PUT"}:
                # Form 13F Special Instruction 10 reports option table amounts
                # in terms of the underlying shares, not option premiums or
                # contract counts. Preserve CALL/PUT as separate identities.
                reference = {**reference, "instrument_type": key[2], "quantity_basis": "underlying_shares"}
            reference = reference or peers[peer_key]
            needs_quote = (
                holding.get("shares_imputed") is True
                or reference is None
                or holding["value"] >= reference["price"]
            )
            if key[2] in {"EQUITY", "CALL", "PUT"} and market_key not in market and needs_quote:
                requests[market_key] = {"report_date": key[0], "cusip": key[1], "instrument_type": "EQUITY", "ticker": holding.get("ticker")}
        revised = dict(holding)
        for field in ("shares_imputed", "quantity_estimate", "quantity_unknown", "quantity_unknown_reason"):
            revised.pop(field, None)
        revised.update({"reported_shares": 0, "shares": 0})
        if reference is not None:
            reference_id = canonical_json_hash(reference)
            quantity = round(holding["value"] / reference["price"], 6)
            if holding["value"] >= reference["price"] and quantity >= 1:
                evidence["references"][reference_id] = reference
                revised.update({"shares": int(quantity) if float(quantity).is_integer() else quantity, "shares_imputed": True, "quantity_estimate": {"policy_version": POLICY_VERSION, "reference_id": reference_id, "method": reference["method"], "unit": reference["unit"]}})
            else:
                reason = "below_one_reported_unit"
        if not revised.get("shares_imputed"):
            revised.update({"quantity_unknown": True, "quantity_unknown_reason": reason})
        errors = validate_quantity_annotation(revised, target["report_date"], evidence, cik=target["cik"])
        if errors:
            raise QuantityEstimationError(f"invalid proposed estimate: {errors}")
        changes.append({**target, "revised": revised})
    return {"schema_version": POLICY_VERSION, "targets": changes, "evidence": evidence, "price_requests": [requests[key] for key in sorted(requests)]}


def apply_plan(plan: dict, funds_dir: Path, *, evidence_path: Path = DEFAULT_EVIDENCE_PATH, request_path: Path = DEFAULT_REQUEST_PATH) -> dict:
    """Preflight the entire change set before any holding file is replaced."""
    by_file = defaultdict(list)
    for target in plan["targets"]:
        if Path(target["file"]).name != target["file"]:
            raise QuantityEstimationError("quantity plan contains an unsafe fund path")
        by_file[target["file"]].append(target)
    staged = []
    for filename, targets in sorted(by_file.items()):
        path = Path(funds_dir) / filename
        raw = path.read_bytes()
        if any(hashlib.sha256(raw).hexdigest() != target["file_sha256"] for target in targets):
            raise QuantityEstimationError(f"fund changed since quantity preflight: {filename}")
        fund = json.loads(raw)
        before = economic_positions_for_fund(fund)
        changed = False
        for target in targets:
            quarter = fund["quarters"][target["quarter_index"]]
            old = quarter["holdings"][target["holding_index"]]
            if old != target["holding"]:
                raise QuantityEstimationError(f"holding changed since quantity preflight: {filename}")
            revised = target["revised"]
            if positive_number(old.get("reported_shares")):
                raise QuantityEstimationError("quantity plan would replace a positive reported quantity")
            derived_fields = {"shares", "reported_shares", "shares_imputed", "quantity_estimate", "quantity_unknown", "quantity_unknown_reason"}
            if not is_quantity_target(old) or {k: v for k, v in old.items() if k not in derived_fields} != {k: v for k, v in revised.items() if k not in derived_fields}:
                raise QuantityEstimationError("quantity plan changes an authoritative holding field")
            errors = validate_quantity_annotation(revised, quarter["report_date"], plan["evidence"], cik=str(fund["cik"]))
            if errors:
                raise QuantityEstimationError(f"invalid quantity plan for {filename}: {errors}")
            quarter["holdings"][target["holding_index"]] = revised
            changed |= old != revised
        if economic_positions_for_fund(fund) != before:
            raise QuantityEstimationError(f"quantity repair changed reported economics: {filename}")
        if changed:
            staged.append((path, fund))
    # Additive evidence is durable first. An interruption can leave mixed policy
    # generations, which validation rejects; it cannot leave dangling receipts.
    atomic_json(evidence_path, plan["evidence"])
    atomic_json(request_path, {"schema_version": POLICY_VERSION, "requests": plan["price_requests"]})
    for path, fund in staged:
        atomic_json(path, fund)
    targets = plan["targets"]
    return {"targets_reviewed": len(targets), "files_changed": len(staged), "estimated_rows": sum(row["revised"].get("shares_imputed") is True for row in targets), "unknown_rows": sum(row["revised"].get("quantity_unknown") is True for row in targets), "market_price_requests": len(plan["price_requests"])}
