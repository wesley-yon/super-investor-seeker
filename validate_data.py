#!/usr/bin/env python3
"""
Validate generated data artifacts for Super Investor Seeker.

This is a structural integrity check meant to be safe to run before or after
pipeline changes. It verifies that:
  - bootstrap indexes match the frontend's generated-data contract version
  - index.json points at existing fund/stock files
  - funds-index.json is lightweight and matches the fund portion of index.json
  - fund quarter histories are sorted and de-duplicated
  - filing dates preserve their actual day or retained legacy month precision
  - each fund index entry exposes its four newest actual report quarters
  - stock holder histories are sorted, de-duplicated, and carry pct_of_fund
  - stock histories reconcile to every retained quarter without inventing
    observations for sparse/absent positions

Fund files may retain more than four historical quarters. The compact index
calendar is deliberately limited to four; it is metadata for UI alignment, not
a retention policy.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import TypedDict

from composition_integrity import (
    calculate_quarter_composition_hash as _calculate_quarter_composition_hash,
)
from data_contract import DATA_CONTRACT_VERSION
from quantity_estimation import (
    cache_dir_for_funds,
    load_book as load_quantity_evidence,
    validate_quantity_annotation,
)
from quarter_health import (
    add_quarter_peer_observations,
    compile_peer_price_index,
    peer_price_quarter_health_issue,
    same_date_peer_price_references,
    structural_quarter_health_issues,
)
from sec_13f_bulk_backfill import (
    Sec13FBulkError,
    normalize_sec_identity_source_url,
)
from sec_security_master import (
    MASTER_AUDIT_SCHEMA_VERSION,
    MASTER_SCHEMA_VERSION,
    PRODUCTION_MIN_ACTIVE_OFFICIAL_CUSIP_COUNT,
    PRODUCTION_MIN_CURRENT_SYMBOL_POPULATION_BY_KIND,
    PRODUCTION_MIN_CURRENT_SYMBOL_TITLE_RATIO,
    SOURCE_STATE_SCHEMA_VERSION,
    SecurityMasterError,
    audit_security_master,
    load_security_master,
    load_source_state,
    project_ftd_master_evidence,
    project_master_audit,
    security_key as sec_security_key,
    source_state_sha256,
)
from security_identity import (
    EQUITY_FUND_SECURITY_KINDS,
    VALID_INSTRUMENT_TYPES,
    is_canonical_security_identifier,
    is_synthetic_identifier as is_synthetic_identifier,
    normalize_instrument_type,
    normalize_note_security_label,
    normalize_security_kind,
    normalize_security_label,
    normalize_security_identifier,
    published_holding_instrument_type,
    registry_entry_has_trusted_fund_symbol_evidence,
    stock_file_stem,
    stock_filename,
    stock_lookup_id,
)
from value_units import (
    PEER_MIN_SCALE_COUNT_SUPPORT,
    VALUE_UNIT_POLICY_VERSION,
    VALUE_UNIT_SCALE,
    adjacent_quarter_scale_evidence,
    peer_scale_evidence,
)

# Recompute generated split proof from the exact public holder histories.
# Importing this pure helper avoids maintaining a second, subtly divergent
# split algorithm in the release gate.
from pipeline import (
    _FUND_PRODUCT_NAME_KINDS,
    infer_proven_split_adjustments,
    normalize_filer_identity_name,
)


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
FUNDS_DIR = DATA_DIR / "funds"
STOCKS_DIR = DATA_DIR / "stocks"
INDEX_PATH = DATA_DIR / "index.json"
FUNDS_INDEX_PATH = DATA_DIR / "funds-index.json"
CUSIP_REGISTRY_PATH = DATA_DIR / "cusip_registry.json"
SECURITY_LABELS_PATH = DATA_DIR / "security_labels.json"
STATE_PATH = DATA_DIR / "pipeline_state.json"
SEC_SECURITY_MASTER_PATH = ROOT / ".cache" / "sec_security_master.json"
SEC_SOURCE_STATE_PATH = ROOT / ".cache" / "sec_source_state.json"
SEC_TICKER_SOURCES = frozenset({
    "sec_ftd",
    "sec_ixbrl",
})
SEC_METADATA_SOURCES = frozenset({
    *SEC_TICKER_SOURCES,
    "sec_13f_list",
    "sec_company_tickers",
    "sec_fund_series",
    "sec_schedule_13dg",
    "sec_13f_filer_consensus",
})
# Public registry provenance is intentionally narrower than the private
# security-master evidence vocabulary.  These are the only values emitted by
# the SEC-only registry builder.  Keeping the allowlists explicit makes a
# restored vendor-era registry fail publication even when its ticker happens to
# match the SEC master.
PUBLIC_REGISTRY_LABEL_SOURCES = frozenset({
    "sec_13f_list",
    "sec_13f_filer_consensus",
    "sec_ftd",
    "synthetic_identifier",
})
PUBLIC_REGISTRY_EVIDENCE_SOURCES = frozenset({
    "sec_13f_list",
    "sec_13f_filer_consensus",
    "sec_company_tickers",
    "sec_fund_series",
    "sec_ftd",
    "sec_ixbrl",
})
SEC_MAPPING_STATUSES = frozenset({
    "resolved",
    "unresolved",
    "ambiguous",
    "no_listed_symbol",
    "malformed_as_filed",
})
SEC_EQUITY_SAFETY_ANCHORS = {
    "037833100": "AAPL",
    "30231G102": "XOM",
    "76954A103": "RIVN",
}
SEC_TICKERLESS_NOTE_SAFETY_ANCHORS = frozenset({
    "090043AF7",
    "26210CAC8",
    "26210CAD6",
    "76954AAD5",
})
PRIVATE_SEC_EVIDENCE_FIELDS = frozenset({
    "candidate_as_of",
    "candidate_symbols",
    "candidate_ticker",
    "confirmation_dates",
    "effective_from",
    "effective_to",
    "fund_series_evidence",
    "fund_series_name",
    "mapping_method",
    "official_13f",
    "reported_identities",
    "reported_identity_evidence",
    "resolution_reason",
    "sec_edgar_evidence",
    "source_ticker",
    "symbol_evidence",
    "symbol_intervals",
    "symbol_validation_exchanges",
    "symbol_validation_sources",
    "symbol_validation_titles",
    "ticker_evidence_cusips",
})
_AMENDMENT_REDUCER_VERSION = 2
_COMPOSITION_HASH_VERSION = 3
_NEW_HOLDINGS_IDENTITY_VERSION = 1
_NEW_HOLDINGS_REPLACEMENT_MIN_MATCHED_ROWS = 5
_NEW_HOLDINGS_REPLACEMENT_COVERAGE_NUMERATOR = 9
_NEW_HOLDINGS_REPLACEMENT_COVERAGE_DENOMINATOR = 10
_SECURITY_IDENTITY_VERSION = 1
_EXCLUSIVE_ETF_ISSUER_RE = re.compile(
    r"(?:"
    r"ISHARES\s+TR|"
    r"ETFIS\s+SER(?:IES)?\s+TR(?:UST)?(?:\s+I)?|"
    r"JANUS\s+DETROIT\s+STR\s+TR|"
    r"(?:SELECT\s+SECTOR\s+)?SPDR\s+"
    r"(?:S&P\s+500\s+ETF\s+)?TR(?:UST)?"
    r")",
    re.IGNORECASE,
)
_ETN_KIND_RE = re.compile(
    r"\bETNS?\b|\bEXCHANGE[- ]TRADED\s+NOTES?\b",
    re.IGNORECASE,
)
_DEPOSITARY_RECEIPT_RE = re.compile(
    r"\b(?:ADRS?|ADS|DEPOSITARY|DEP(?:OSITARY)?(?:\s+SHS?)?)\b",
    re.IGNORECASE,
)
_LEGACY_VALUE_UNIT_POLICY_VERSIONS = frozenset(
    range(1, VALUE_UNIT_POLICY_VERSION)
)
_SUPPORTED_VALUE_UNIT_POLICY_VERSIONS = (
    _LEGACY_VALUE_UNIT_POLICY_VERSIONS
    | {VALUE_UNIT_POLICY_VERSION}
)
_COMPOSITION_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_REPORT_QUARTER_BY_MONTH_DAY = {
    (3, 31): 1,
    (6, 30): 2,
    (9, 30): 3,
    (12, 31): 4,
}
_POSITION_DIGEST_MODULUS = 1 << 256


def is_finite_number(value: object) -> bool:
    """Return whether a JSON number is safe for downstream numeric work."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value))
    except (OverflowError, TypeError, ValueError):
        return False


def canonical_cik(value: object) -> str | None:
    """Return the canonical key for a positive integer SEC CIK."""
    if type(value) is not int or value <= 0:
        return None
    return str(value)


class FilerNameCollision(TypedDict):
    name: str
    ciks: tuple[str, ...]


def filer_name_collision_groups(
    fund_calendars: dict[str, dict],
) -> list[FilerNameCollision]:
    """Return legal-name collisions that must stay distinct by SEC CIK."""
    by_name: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for cik, metadata in sorted(fund_calendars.items()):
        if not isinstance(metadata, dict):
            continue
        name = metadata.get("name")
        if not isinstance(name, str):
            continue
        key = normalize_filer_identity_name(name)
        if key:
            by_name[key].append((cik, name))

    return [
        {
            "name": entries[0][1],
            "ciks": tuple(cik for cik, _name in entries),
        }
        for _key, entries in sorted(by_name.items())
        if len(entries) > 1
    ]


def report_quarter_code(report_date: object) -> int | None:
    """Encode a canonical quarter-end report date as YYYYQ."""
    if not isinstance(report_date, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}", report_date
    ):
        return None
    try:
        parsed = date.fromisoformat(report_date)
    except ValueError:
        return None
    quarter = _REPORT_QUARTER_BY_MONTH_DAY.get((parsed.month, parsed.day))
    if quarter is None:
        return None
    return parsed.year * 10 + quarter


def filing_date_precision(value: object) -> str | None:
    """Classify a valid SEC filing date without inventing missing precision."""
    if not isinstance(value, str):
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            date.fromisoformat(value)
        except ValueError:
            return None
        return "DAY"
    if re.fullmatch(r"\d{4}-\d{2}", value):
        try:
            date.fromisoformat(f"{value}-01")
        except ValueError:
            return None
        return "MONTH"
    return None


def expected_fund_quarter_codes(report_dates: object) -> list[int]:
    """Return the four newest distinct valid persisted report quarters."""
    if not isinstance(report_dates, (list, tuple)):
        return []
    codes = {
        code
        for report_date in report_dates
        if (code := report_quarter_code(report_date)) is not None
    }
    return sorted(codes, reverse=True)[:4]


def classify_sparse_history(
    history: object,
    report_dates: object,
) -> tuple[str, dict | None, dict | None]:
    """Align positive-only history to a fund's own two latest reports.

    Absence is interpreted only relative to an actual persisted fund report.
    A present record remains current even when its shares and value are zero.
    """
    if not isinstance(history, list) or not isinstance(report_dates, (list, tuple)):
        return "HISTORICAL", None, None
    by_date = {
        entry.get("date"): entry
        for entry in history
        if isinstance(entry, dict) and isinstance(entry.get("date"), str)
    }
    latest_date = report_dates[0] if report_dates else None
    previous_date = report_dates[1] if len(report_dates) > 1 else None
    current = by_date.get(latest_date)
    previous = by_date.get(previous_date)
    if current is not None:
        return ("CURRENT" if previous is not None else "NEW"), current, previous
    if previous is not None:
        return "EXIT", None, previous
    return "HISTORICAL", None, None


def holding_stock_id(
    holding: dict,
    registry: dict[str, dict] | None = None,
) -> str:
    """Derive the same instrument-aware identity used by generated stocks."""
    cusip = normalize_security_identifier(holding.get("cusip"))
    ticker = normalize_security_identifier(holding.get("ticker"))
    identifier = cusip or ticker
    if not identifier:
        return ""
    registry_entry = registry.get(cusip) if registry and cusip else None
    return stock_lookup_id(
        identifier,
        published_holding_instrument_type(holding, registry_entry),
    )


def expected_registry_position_ticker(
    entry: dict | None,
    instrument_type: str,
) -> str | None:
    """Project the only ticker one public fund holding may retain."""

    if not isinstance(entry, dict):
        return None
    normalized_type = normalize_instrument_type(instrument_type)
    if normalized_type in {"CALL", "PUT", "OPT"}:
        raw_ticker = entry.get("underlying_ticker")
        source = entry.get("underlying_ticker_source")
        as_of = entry.get("underlying_ticker_as_of")
    else:
        raw_entry_type = entry.get("type")
        if (
            not isinstance(raw_entry_type, str)
            or raw_entry_type.strip().upper() not in VALID_INSTRUMENT_TYPES
            or normalize_instrument_type(raw_entry_type) != normalized_type
        ):
            return None
        raw_ticker = entry.get("ticker")
        if entry.get("mapping_status") != "resolved":
            return None
        source = entry.get("ticker_source")
        as_of = entry.get("ticker_as_of")
    if source not in SEC_TICKER_SOURCES or not is_strict_iso_date(as_of):
        return None
    ticker = str(raw_ticker or "").strip().upper()
    if not ticker:
        return None
    note_label = normalize_note_security_label(ticker)
    if normalized_type == "NOTE":
        return note_label
    return None if note_label else ticker


def validate_fund_holding_identity(
    holding: dict,
    context: str,
    errors: list[str],
) -> str:
    """Validate one persisted row and return its registry-independent identity."""
    if "option_type" in holding:
        errors.append(f"{context} contains obsolete option_type")

    raw_type = holding.get("holding_type")
    type_is_valid = (
        type(raw_type) is str
        and raw_type in VALID_INSTRUMENT_TYPES
    )
    if not type_is_valid:
        errors.append(
            f"{context} has invalid holding_type {raw_type!r}; expected one of "
            + ", ".join(sorted(VALID_INSTRUMENT_TYPES))
            + " in canonical uppercase form"
        )

    if "put_call" in holding:
        put_call = holding.get("put_call")
        if type(put_call) is not str or put_call not in {"CALL", "PUT"}:
            errors.append(
                f"{context} has invalid put_call {put_call!r}; "
                "expected CALL or PUT"
            )
        elif type_is_valid and raw_type != put_call:
            errors.append(
                f"{context} has put_call {put_call} inconsistent with "
                f"holding_type {raw_type}"
            )

    raw_cusip = holding.get("cusip")
    if not is_canonical_security_identifier(raw_cusip):
        errors.append(
            f"{context} has invalid canonical cusip {raw_cusip!r}"
        )

    raw_ticker = holding.get("ticker")
    normalized_note_label = normalize_note_security_label(raw_ticker)
    if raw_type == "NOTE" and raw_ticker:
        if normalized_note_label != raw_ticker:
            errors.append(
                f"{context} has non-canonical NOTE ticker label "
                f"{raw_ticker!r}"
            )
    elif normalized_note_label:
        errors.append(
            f"{context} publishes NOTE label {raw_ticker!r} on "
            f"holding_type {raw_type!r}"
        )

    return holding_stock_id(holding)


def _canonical_number(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return repr(value)


def _empty_current_stats() -> dict[str, int | float | None]:
    return {
        "holder_count": 0,
        "total_value": 0,
        "total_shares": 0,
        "largest_value": None,
        "position_digest": 0,
        "transition_count": 0,
        "transition_digest": 0,
        "history_count": 0,
        "history_total_value": 0,
        "history_total_shares": 0,
        "history_digest": 0,
    }


def _add_current_position(
    stats: dict[str, int | float | None],
    *,
    cik: object,
    value: int | float,
    shares: int | float,
    pct_of_fund: int | float,
) -> None:
    stats["holder_count"] += 1
    stats["total_value"] += value
    stats["total_shares"] += shares
    largest_value = stats["largest_value"]
    if largest_value is None or value > largest_value:
        stats["largest_value"] = value
    payload = "\x1f".join((
        str(cik),
        _canonical_number(value),
        _canonical_number(shares),
        _canonical_number(pct_of_fund),
    )).encode("utf-8")
    token = int.from_bytes(hashlib.sha256(payload).digest(), "big")
    stats["position_digest"] = (
        int(stats["position_digest"]) + token
    ) % _POSITION_DIGEST_MODULUS


def _add_transition_observation(
    stats: dict[str, int | float | None],
    *,
    cik: object,
    report_date: str,
    value: int | float,
    shares: int | float,
    pct_of_fund: int | float,
) -> None:
    stats["transition_count"] += 1
    payload = "\x1f".join((
        str(cik),
        report_date,
        _canonical_number(value),
        _canonical_number(shares),
        _canonical_number(pct_of_fund),
    )).encode("utf-8")
    token = int.from_bytes(hashlib.sha256(payload).digest(), "big")
    stats["transition_digest"] = (
        int(stats["transition_digest"]) + token
    ) % _POSITION_DIGEST_MODULUS


def _add_history_observation(
    stats: dict[str, int | float | None],
    *,
    cik: object,
    report_date: str,
    value: int | float,
    shares: int | float,
    pct_of_fund: int | float,
    shares_imputed: bool,
    quantity_unknown: bool = False,
) -> None:
    """Add one present fund/security/report observation to an all-history digest."""
    stats["history_count"] += 1
    stats["history_total_value"] += value
    stats["history_total_shares"] += shares
    payload = "\x1f".join((
        str(cik),
        report_date,
        _canonical_number(value),
        _canonical_number(shares),
        _canonical_number(pct_of_fund),
        "1" if shares_imputed else "0",
        "1" if quantity_unknown else "0",
    )).encode("utf-8")
    token = int.from_bytes(hashlib.sha256(payload).digest(), "big")
    stats["history_digest"] = (
        int(stats["history_digest"]) + token
    ) % _POSITION_DIGEST_MODULUS


def _numbers_match(left: object, right: object) -> bool:
    if left is None or right is None:
        return left is right
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        return left == right
    return math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-9)


def calculate_composition_hash(quarter: dict) -> str:
    return _calculate_quarter_composition_hash(
        quarter,
        current_hash_version=_COMPOSITION_HASH_VERSION,
    )


def load_json(path: Path, errors: list[str]) -> dict | list | None:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        errors.append(f"missing file: {path}")
    except json.JSONDecodeError as exc:
        errors.append(f"invalid JSON in {path}: {exc}")
    return None


def validate_data_contract(
    artifact: dict,
    artifact_name: str,
    errors: list[str],
) -> None:
    """Require generated bootstrap data to match the frontend contract."""
    version = artifact.get("data_contract_version")
    if type(version) is not int or version != DATA_CONTRACT_VERSION:
        errors.append(
            f"{artifact_name} has unsupported data_contract_version "
            f"{version!r}; expected {DATA_CONTRACT_VERSION}"
        )


def normalize_issuer_key(issuer: str | None) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(issuer or "").upper())


def is_strict_utc_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ") == value


def is_strict_iso_date(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return False
    # The round trip rejects compact dates and ISO week dates accepted by
    # fromisoformat, without locale-dependent strptime/strftime per holding.
    return parsed.isoformat() == value


def validate_sec_mapping_provenance(
    registry: dict[str, dict],
    errors: list[str],
) -> None:
    """Require fail-closed public mapping status and SEC provenance fields."""

    malformed_status: list[str] = []
    malformed_resolved: list[str] = []
    malformed_unresolved: list[str] = []
    for cusip, entry in registry.items():
        if not isinstance(entry, dict):
            malformed_status.append(cusip)
            continue
        status = entry.get("mapping_status")
        ticker = entry.get("ticker")
        source = entry.get("ticker_source")
        as_of = entry.get("ticker_as_of")
        missing_mapping_fields = {
            field
            for field in ("ticker", "ticker_source", "ticker_as_of")
            if field not in entry
        }
        if status not in SEC_MAPPING_STATUSES:
            malformed_status.append(cusip)
            continue
        if status == "resolved":
            if (
                missing_mapping_fields
                or not isinstance(ticker, str)
                or not ticker.strip()
                or ticker != ticker.strip().upper()
                or source not in SEC_TICKER_SOURCES
                or not is_strict_iso_date(as_of)
            ):
                malformed_resolved.append(cusip)
        elif (
            missing_mapping_fields
            or ticker is not None
            or source is not None
            or as_of is not None
        ):
            malformed_unresolved.append(cusip)

    if malformed_status:
        errors.append(
            "cusip_registry.json has "
            f"{len(malformed_status)} entries with invalid mapping_status; "
            f"samples: {', '.join(sorted(malformed_status)[:10])}"
        )
    if malformed_resolved:
        errors.append(
            "cusip_registry.json has "
            f"{len(malformed_resolved)} resolved mappings without a canonical "
            "ticker, SEC ticker_source, or YYYY-MM-DD ticker_as_of; samples: "
            f"{', '.join(sorted(malformed_resolved)[:10])}"
        )
    if malformed_unresolved:
        errors.append(
            "cusip_registry.json has "
            f"{len(malformed_unresolved)} non-resolved mappings that publish "
            "ticker or ticker provenance; samples: "
            f"{', '.join(sorted(malformed_unresolved)[:10])}"
        )


def validate_public_registry_provenance(
    registry: dict[str, dict],
    errors: list[str],
) -> None:
    """Reject provenance values the SEC-only registry builder cannot emit."""

    invalid_label_sources: list[str] = []
    invalid_evidence_sources: list[str] = []
    for cusip, entry in registry.items():
        if not isinstance(entry, dict):
            continue
        label_source = entry.get("label_source")
        if label_source not in PUBLIC_REGISTRY_LABEL_SOURCES:
            invalid_label_sources.append(cusip)

        sources = entry.get("sources")
        if (
            not isinstance(sources, list)
            or not sources
            or any(
                type(source) is not str
                or source not in PUBLIC_REGISTRY_EVIDENCE_SOURCES
                for source in sources
            )
        ):
            invalid_evidence_sources.append(cusip)

    if invalid_label_sources:
        errors.append(
            "cusip_registry.json has "
            f"{len(invalid_label_sources)} security labels with provenance "
            "outside the SEC/filer/synthetic allowlist; samples: "
            f"{', '.join(sorted(invalid_label_sources)[:10])}"
        )
    if invalid_evidence_sources:
        errors.append(
            "cusip_registry.json has "
            f"{len(invalid_evidence_sources)} entries with sources outside "
            "the SEC/filer allowlist; samples: "
            f"{', '.join(sorted(invalid_evidence_sources)[:10])}"
        )


def validate_sec_safety_anchors(
    registry: dict[str, dict],
    master: dict,
    errors: list[str],
) -> None:
    """Lock named high-risk securities to their exact SEC-master identity."""

    records = master.get("records") if isinstance(master, dict) else None
    if not isinstance(records, dict):
        errors.append(
            "private SEC security master cannot validate required safety "
            "anchors because its records object is missing"
        )
        return

    for cusip, ticker in SEC_EQUITY_SAFETY_ANCHORS.items():
        public_entry = registry.get(cusip)
        master_entry = records.get(sec_security_key(cusip, "EQUITY"))
        public_fields_present = isinstance(public_entry, dict) and all(
            field in public_entry
            for field in ("ticker", "ticker_source", "ticker_as_of")
        )
        master_fields_present = isinstance(master_entry, dict) and all(
            field in master_entry
            for field in ("ticker", "ticker_source", "ticker_as_of")
        )
        public_ok = (
            public_fields_present
            and public_entry.get("type") == "EQUITY"
            and public_entry.get("mapping_status") == "resolved"
            and public_entry.get("ticker") == ticker
            and public_entry.get("ticker_source") in SEC_TICKER_SOURCES
            and is_strict_iso_date(public_entry.get("ticker_as_of"))
        )
        master_ok = (
            master_fields_present
            and master_entry.get("cusip") == cusip
            and master_entry.get("instrument_type") == "EQUITY"
            and master_entry.get("mapping_status") == "resolved"
            and master_entry.get("ticker") == ticker
            and master_entry.get("ticker_source") in SEC_TICKER_SOURCES
            and is_strict_iso_date(master_entry.get("ticker_as_of"))
        )
        fields_agree = public_ok and master_ok and all(
            public_entry.get(field) == master_entry.get(field)
            for field in (
                "mapping_status",
                "ticker",
                "ticker_source",
                "ticker_as_of",
            )
        )
        if not fields_agree:
            errors.append(
                f"required SEC safety-case equity {cusip} must resolve "
                f"exactly to {ticker} in both the public registry and the "
                "private security master"
            )

    for cusip in sorted(SEC_TICKERLESS_NOTE_SAFETY_ANCHORS):
        public_entry = registry.get(cusip)
        master_entry = records.get(sec_security_key(cusip, "NOTE"))
        public_mapping_fields_present = isinstance(public_entry, dict) and all(
            field in public_entry
            for field in ("ticker", "ticker_source", "ticker_as_of")
        )
        master_mapping_fields_present = isinstance(master_entry, dict) and all(
            field in master_entry
            for field in ("ticker", "ticker_source", "ticker_as_of")
        )
        public_ok = (
            public_mapping_fields_present
            and public_entry.get("type") == "NOTE"
            and public_entry.get("security_kind") == "BOND"
            and public_entry.get("mapping_status")
            in SEC_MAPPING_STATUSES - {"resolved"}
            and all(
                public_entry.get(field) is None
                for field in (
                    "ticker",
                    "ticker_source",
                    "ticker_as_of",
                    "underlying_ticker",
                    "underlying_ticker_source",
                    "underlying_ticker_as_of",
                )
            )
        )
        master_ok = (
            master_mapping_fields_present
            and master_entry.get("cusip") == cusip
            and master_entry.get("instrument_type") == "NOTE"
            and master_entry.get("mapping_status")
            in SEC_MAPPING_STATUSES - {"resolved"}
            and all(
                master_entry.get(field) is None
                for field in ("ticker", "ticker_source", "ticker_as_of")
            )
        )
        fields_agree = public_ok and master_ok and all(
            public_entry.get(field) == master_entry.get(field)
            for field in (
                "mapping_status",
                "ticker",
                "ticker_source",
                "ticker_as_of",
            )
        )
        if not fields_agree:
            errors.append(
                f"required SEC safety-case debt {cusip} must remain an "
                "exact tickerless NOTE/BOND in both the public registry and "
                "the private security master"
            )


def validate_private_sec_security_state(
    registry: dict[str, dict],
    errors: list[str],
    *,
    master_path: Path | None = None,
    source_state_path: Path | None = None,
    enforce_production_source_gates: bool = True,
) -> None:
    """Validate private SEC caches and reconcile the public registry to them."""

    resolved_master_path = Path(master_path or SEC_SECURITY_MASTER_PATH)
    resolved_source_path = Path(source_state_path or SEC_SOURCE_STATE_PATH)
    for path, label in (
        (resolved_master_path, "SEC security master"),
        (resolved_source_path, "SEC source state"),
    ):
        if path.is_symlink() or not path.is_file():
            errors.append(f"missing or invalid private {label}: {path}")
    if any(
        path.is_symlink() or not path.is_file()
        for path in (resolved_master_path, resolved_source_path)
    ):
        return

    try:
        raw_source_state = json.loads(
            resolved_source_path.read_text(encoding="utf-8")
        )
        if not isinstance(raw_source_state, dict):
            raise SecurityMasterError(
                "SEC source-state document must be an object"
            )
        raw_source_state_schema_version = raw_source_state.get(
            "schema_version"
        )
        master = load_security_master(resolved_master_path)
        source_state = load_source_state(resolved_source_path)
        projected_audit = (
            project_master_audit(master, source_state)
            if enforce_production_source_gates
            else None
        )
        production_gates = (
            {
                "minimum_current_symbol_population_by_kind": (
                    PRODUCTION_MIN_CURRENT_SYMBOL_POPULATION_BY_KIND
                ),
                "minimum_current_symbol_title_ratio": (
                    PRODUCTION_MIN_CURRENT_SYMBOL_TITLE_RATIO
                ),
                "minimum_active_official_cusip_count": (
                    PRODUCTION_MIN_ACTIVE_OFFICIAL_CUSIP_COUNT
                ),
                "enforce_latest_completed_official_period": True,
            }
            if enforce_production_source_gates
            else {}
        )
        master_audit = audit_security_master(
            master,
            as_of=date.today(),
            enforce_reported_identity_evidence=True,
            **production_gates,
        )
    except (OSError, json.JSONDecodeError, SecurityMasterError) as error:
        errors.append(f"invalid private SEC security state: {error}")
        return
    if not master_audit["ok"]:
        errors.append(
            "private SEC security master failed publication gates: "
            + ", ".join(master_audit["issues"])
        )
    if enforce_production_source_gates:
        schema_mismatches: list[str] = []
        if (
            raw_source_state_schema_version
            != SOURCE_STATE_SCHEMA_VERSION
        ):
            schema_mismatches.append("source state")
        if master.get("schema_version") != MASTER_SCHEMA_VERSION:
            schema_mismatches.append("security master")
        if (
            master.get("source_state_schema_version")
            != SOURCE_STATE_SCHEMA_VERSION
        ):
            schema_mismatches.append("master source-state binding")
        audit_payload = master.get("audit")
        if (
            not isinstance(audit_payload, dict)
            or audit_payload.get("schema_version")
            != MASTER_AUDIT_SCHEMA_VERSION
        ):
            schema_mismatches.append("master audit")
        if schema_mismatches:
            errors.append(
                "private SEC security state does not use the current "
                "production schemas: " + ", ".join(schema_mismatches)
            )
        if master.get("audit") != projected_audit:
            errors.append(
                "private SEC security-master audit claims do not match the "
                "retained SEC source state and record set"
            )
    if master.get("source_state_sha256") != source_state_sha256(source_state):
        errors.append(
            "private SEC security master is not bound to the current "
            "source-state digest"
        )

    # The state digest alone is not sufficient: the master also carries a
    # compact projection of the accepted SEC documents used by each record.
    # Reconcile that projection back to the retained source state so a
    # well-formed but substituted checksum cannot masquerade as evidence.
    expected_source_references = {
        (
            str(source.get("url") or url),
            str(source.get("sha256") or ""),
            str(source.get("kind") or ""),
        )
        for url, source in source_state.get("sources", {}).items()
        if isinstance(source, dict)
    }
    edgar_cache = source_state.get("edgar_evidence", {})
    if isinstance(edgar_cache, dict):
        expected_source_references.update(
            (
                str(source.get("url") or ""),
                str(source.get("sha256") or ""),
                str(source.get("kind") or ""),
            )
            for source in edgar_cache.get("sources", [])
            if isinstance(source, dict)
        )
    actual_source_references = {
        (
            str(source.get("url") or ""),
            str(source.get("sha256") or ""),
            str(source.get("kind") or ""),
        )
        for source in master.get("sources", [])
        if isinstance(source, dict)
    }
    if actual_source_references != expected_source_references:
        errors.append(
            "private SEC security master source checksums do not match the "
            "current source state"
        )

    records = master.get("records")
    if not isinstance(records, dict):
        errors.append("private SEC security master has no records object")
        return

    # Reproject the compact stable timeline and its two reversible tail
    # archives. This proves each published CUSIP/symbol/date against an exact
    # SEC URL/hash without expanding the all-history corpus in memory.
    master_cusips = {
        normalize_security_identifier(entry.get("cusip"))
        for entry in records.values()
        if isinstance(entry, dict)
        and normalize_security_identifier(entry.get("cusip"))
    }
    (
        expected_ftd_evidence,
        expected_ftd_intervals,
        _latest_ftd_date,
    ) = project_ftd_master_evidence(source_state, master_cusips)

    validation_sources_by_symbol: dict[str, set[str]] = defaultdict(set)
    validation_titles_by_symbol: dict[str, set[str]] = defaultdict(set)
    validation_exchanges_by_symbol: dict[str, set[str]] = defaultdict(set)
    validation_kinds = {
        "sec_company_tickers",
        "sec_company_exchange_tickers",
        "sec_fund_tickers",
    }
    for source in source_state.get("sources", {}).values():
        if not isinstance(source, dict) or source.get("kind") not in validation_kinds:
            continue
        kind = str(source["kind"])
        for symbol in source.get("symbols", []):
            if isinstance(symbol, str) and symbol:
                validation_sources_by_symbol[symbol].add(kind)
        for symbol, titles in source.get("symbol_titles", {}).items():
            if isinstance(symbol, str) and isinstance(titles, list):
                validation_titles_by_symbol[symbol].update(
                    title for title in titles if isinstance(title, str)
                )
        for symbol, exchanges in source.get("symbol_exchanges", {}).items():
            if isinstance(symbol, str) and isinstance(exchanges, list):
                validation_exchanges_by_symbol[symbol].update(
                    exchange for exchange in exchanges if isinstance(exchange, str)
                )

    official_candidates = [
        (str(source.get("list_period") or ""), url, source)
        for url, source in source_state.get("sources", {}).items()
        if isinstance(source, dict) and source.get("kind") == "sec_13f_list"
    ]
    official_rows_by_cusip: dict[str, list[dict]] = defaultdict(list)
    official_source_reference: dict[str, str] | None = None
    if official_candidates:
        official_period, official_url, official_source = max(
            official_candidates,
            key=lambda item: (item[0], item[1]),
        )
        official_source_reference = {
            "period": official_period,
            "url": official_url,
            "sha256": str(official_source.get("sha256") or ""),
        }
        for row in official_source.get("records", []):
            if not isinstance(row, dict):
                continue
            cusip = normalize_security_identifier(row.get("cusip"))
            if cusip:
                official_rows_by_cusip[cusip].append(dict(row))
        for rows in official_rows_by_cusip.values():
            rows.sort(
                key=lambda row: (
                    row.get("description") or "",
                    row.get("issuer") or "",
                    row.get("status") or "",
                )
            )

    fund_records_by_symbol: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    fund_pages_by_cik: dict[str, tuple[str, dict]] = {}
    for url, source in source_state.get("sources", {}).items():
        if not isinstance(source, dict):
            continue
        if source.get("kind") == "sec_fund_tickers":
            for row in source.get("fund_records", []):
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol") or "")
                if symbol:
                    fund_records_by_symbol[symbol].add(
                        (
                            str(row.get("cik") or ""),
                            str(row.get("series_id") or ""),
                            str(row.get("class_id") or ""),
                        )
                    )
        elif source.get("kind") == "sec_fund_series":
            fund_pages_by_cik[str(source.get("cik") or "")] = (url, source)
    expected_fund_series_by_symbol: dict[str, dict] = {}
    for symbol, candidates in sorted(fund_records_by_symbol.items()):
        if len(candidates) != 1:
            continue
        cik, series_id, class_id = next(iter(candidates))
        page = fund_pages_by_cik.get(cik)
        if page is None:
            continue
        url, source = page
        series_name = " ".join(
            str(source.get("series_names", {}).get(series_id) or "").split()
        )
        class_name = " ".join(
            str(source.get("class_names", {}).get(class_id) or "").split()
        )
        if not series_name:
            continue
        if not class_name or class_name.casefold() == series_name.casefold():
            name = series_name
        elif series_name.casefold() in class_name.casefold():
            name = class_name
        else:
            name = f"{series_name} — {class_name}"
        verified_at = source.get(
            "last_successful_check_at",
            source.get("accepted_at"),
        )
        expected_fund_series_by_symbol[symbol] = {
            "symbol": symbol,
            "name": name,
            "cik": cik,
            "series_id": series_id,
            "class_id": class_id,
            "url": url,
            "sha256": str(source.get("sha256") or ""),
            "verified_at": verified_at,
        }

    ftd_content_mismatches: list[str] = []
    ftd_interval_mismatches: list[str] = []
    validation_content_mismatches: list[str] = []
    official_content_mismatches: list[str] = []
    fund_series_content_mismatches: list[str] = []
    edgar_content_mismatches: list[str] = []
    cached_edgar_records = (
        edgar_cache.get("records", {}) if isinstance(edgar_cache, dict) else {}
    )
    if not isinstance(cached_edgar_records, dict):
        cached_edgar_records = {}
    for key, master_entry in records.items():
        if not isinstance(master_entry, dict):
            continue
        cusip = normalize_security_identifier(master_entry.get("cusip"))
        if not cusip:
            continue
        if master_entry.get("symbol_evidence", []) != expected_ftd_evidence.get(
            cusip, []
        ):
            ftd_content_mismatches.append(key)
        if (
            master.get("source_state_schema_version")
            == SOURCE_STATE_SCHEMA_VERSION
            and master_entry.get("symbol_intervals", [])
            != expected_ftd_intervals.get(cusip, [])
        ):
            ftd_interval_mismatches.append(key)
        candidate = master_entry.get("candidate_ticker")
        if (
            not candidate
            and master_entry.get("mapping_status") == "resolved"
            and master_entry.get("ticker_source") == "sec_ftd"
        ):
            candidate = master_entry.get("ticker")
        if isinstance(candidate, str) and candidate:
            if (
                master_entry.get("symbol_validation_sources", [])
                != sorted(validation_sources_by_symbol.get(candidate, set()))
                or master_entry.get("symbol_validation_titles", [])
                != sorted(validation_titles_by_symbol.get(candidate, set()))
                or master_entry.get("symbol_validation_exchanges", [])
                != sorted(validation_exchanges_by_symbol.get(candidate, set()))
            ):
                validation_content_mismatches.append(key)
        official_rows = official_rows_by_cusip.get(cusip, [])
        if official_rows and official_source_reference is not None:
            active_rows = [
                row
                for row in official_rows
                if row.get("status") != "*D*"
                and str(row.get("description") or "").strip().upper()
                not in {"CALL", "PUT"}
            ]
            official_status = (
                "active"
                if active_rows
                else "deleted"
                if all(row.get("status") == "*D*" for row in official_rows)
                else "option_only"
            )
            expected_official = {
                **official_source_reference,
                "status": official_status,
                "records": active_rows or official_rows,
            }
            if (
                master_entry.get("official_13f") != expected_official
                or master_entry.get("official_13f_status") != official_status
                or master_entry.get("official_13f_as_of")
                != official_source_reference["period"]
            ):
                official_content_mismatches.append(key)
        elif any(
            field in master_entry
            for field in (
                "official_13f",
                "official_13f_status",
                "official_13f_as_of",
            )
        ):
            official_content_mismatches.append(key)
        expected_fund_evidence = (
            expected_fund_series_by_symbol.get(str(master_entry.get("ticker") or ""))
            if master_entry.get("mapping_status") == "resolved"
            else None
        )
        if (
            master_entry.get("fund_series_evidence") != expected_fund_evidence
            or master_entry.get("fund_series_name")
            != (
                expected_fund_evidence.get("name")
                if isinstance(expected_fund_evidence, dict)
                else None
            )
        ):
            fund_series_content_mismatches.append(key)
        if (
            master_entry.get("mapping_status") == "resolved"
            and master_entry.get("ticker_source") == "sec_ixbrl"
        ):
            cached = cached_edgar_records.get(cusip)
            proof = master_entry.get("sec_edgar_evidence")
            schedule = proof.get("schedule_13dg") if isinstance(proof, dict) else None
            ixbrl = proof.get("ixbrl") if isinstance(proof, dict) else None
            if (
                not isinstance(cached, dict)
                or master_entry.get("ticker") != cached.get("ticker")
                or master_entry.get("ticker_as_of") != cached.get("ticker_as_of")
                or master_entry.get("issuer") != cached.get("issuer_name")
                or master_entry.get("security_class")
                != cached.get("security_class")
                or master_entry.get("exchange") != cached.get("exchange")
                or master_entry.get("exchanges") != cached.get("exchanges")
                or not isinstance(proof, dict)
                or proof.get("issuer_cik") != cached.get("issuer_cik")
                or not isinstance(schedule, dict)
                or schedule.get("accession")
                != cached.get("schedule_13dg_accession")
                or schedule.get("url") != cached.get("schedule_13dg_url")
                or schedule.get("as_of") != cached.get("schedule_13dg_as_of")
                or not isinstance(ixbrl, dict)
                or ixbrl.get("accession") != cached.get("ixbrl_accession")
                or ixbrl.get("url") != cached.get("ixbrl_url")
                or ixbrl.get("as_of") != cached.get("ixbrl_as_of")
                or ixbrl.get("context_ids")
                != cached.get("ixbrl_context_ids")
            ):
                edgar_content_mismatches.append(key)
    if ftd_content_mismatches:
        errors.append(
            "private SEC security master FTD records do not match retained "
            "SEC source rows; samples: "
            + ", ".join(sorted(ftd_content_mismatches)[:10])
        )
    if ftd_interval_mismatches:
        errors.append(
            "private SEC security master FTD intervals do not match retained "
            "SEC compact timeline; samples: "
            + ", ".join(sorted(ftd_interval_mismatches)[:10])
        )
    if validation_content_mismatches:
        errors.append(
            "private SEC security master symbol validation metadata does not "
            "match retained SEC source rows; samples: "
            + ", ".join(sorted(validation_content_mismatches)[:10])
        )
    if official_content_mismatches:
        errors.append(
            "private SEC security master official-list records do not match "
            "retained SEC source rows; samples: "
            + ", ".join(sorted(official_content_mismatches)[:10])
        )
    if fund_series_content_mismatches:
        errors.append(
            "private SEC security master fund-series records do not match "
            "retained SEC source rows; samples: "
            + ", ".join(sorted(fund_series_content_mismatches)[:10])
        )
    if edgar_content_mismatches:
        errors.append(
            "private SEC security master iXBRL records do not match retained "
            "SEC filing evidence; samples: "
            + ", ".join(sorted(edgar_content_mismatches)[:10])
        )

    missing: list[str] = []
    mismatched: list[str] = []
    underlying_mismatched: list[str] = []
    leaked_private_fields: list[str] = []
    for cusip, entry in registry.items():
        if not isinstance(entry, dict):
            missing.append(cusip)
            continue
        if PRIVATE_SEC_EVIDENCE_FIELDS & set(entry):
            leaked_private_fields.append(cusip)
        key = sec_security_key(cusip, entry.get("type"))
        master_entry = records.get(key)
        if not isinstance(master_entry, dict):
            missing.append(key)
            continue
        fields = ("mapping_status", "ticker", "ticker_source", "ticker_as_of")
        if any(entry.get(field) != master_entry.get(field) for field in fields):
            mismatched.append(key)
        elif entry.get("product_name_source") == "sec_fund_series" and (
            entry.get("product_name") != master_entry.get("fund_series_name")
        ):
            mismatched.append(key)

        underlying_fields = (
            "underlying_ticker",
            "underlying_ticker_source",
            "underlying_ticker_as_of",
        )
        present_underlying_fields = {
            field for field in underlying_fields if field in entry
        }
        if present_underlying_fields:
            equity_proof = records.get(sec_security_key(cusip, "EQUITY"))
            complete_public_proof = (
                present_underlying_fields == set(underlying_fields)
                and expected_registry_position_ticker(entry, "CALL")
                is not None
            )
            if (
                not complete_public_proof
                or not isinstance(equity_proof, dict)
                or equity_proof.get("mapping_status") != "resolved"
                or equity_proof.get("ticker_source") not in SEC_TICKER_SOURCES
                or not is_strict_iso_date(equity_proof.get("ticker_as_of"))
                or any(
                    entry.get(public_field) != equity_proof.get(master_field)
                    for public_field, master_field in (
                        ("underlying_ticker", "ticker"),
                        ("underlying_ticker_source", "ticker_source"),
                        ("underlying_ticker_as_of", "ticker_as_of"),
                    )
                )
            ):
                underlying_mismatched.append(cusip)

    if missing:
        errors.append(
            "cusip_registry.json has "
            f"{len(missing)} entries absent from the exact private SEC "
            f"security master; samples: {', '.join(sorted(missing)[:10])}"
        )
    if mismatched:
        errors.append(
            "cusip_registry.json has "
            f"{len(mismatched)} mappings that differ from private SEC "
            f"status or ticker provenance; samples: "
            f"{', '.join(sorted(mismatched)[:10])}"
        )
    if underlying_mismatched:
        errors.append(
            "cusip_registry.json has "
            f"{len(underlying_mismatched)} option-underlying mappings that "
            "lack an exact resolved SEC EQUITY proof; samples: "
            f"{', '.join(sorted(underlying_mismatched)[:10])}"
        )
    if leaked_private_fields:
        errors.append(
            "cusip_registry.json exposes private SEC evidence fields for "
            f"{len(leaked_private_fields)} entries; samples: "
            f"{', '.join(sorted(leaked_private_fields)[:10])}"
        )
    if enforce_production_source_gates:
        validate_sec_safety_anchors(registry, master, errors)


def _validate_v2_source_decision(
    source: dict,
    accession: str,
    context: str,
    errors: list[str],
) -> None:
    action = source.get("composition_action")
    if (
        not isinstance(action, str)
        or action not in ("SUPERSEDED", "BASE", "APPEND", "REPLACE")
    ):
        errors.append(
            f"{context} source {accession} has invalid composition_action "
            f"{action!r}"
        )
        return

    kind = source.get("amendment_kind")
    overlap = source.get("new_holdings_overlap")
    if action == "BASE":
        if (
            not isinstance(kind, str)
            or kind not in ("ORIGINAL", "RESTATEMENT")
        ):
            errors.append(
                f"{context} BASE source {accession} has invalid "
                f"amendment_kind {kind!r}"
            )
        if overlap is not None:
            errors.append(
                f"{context} BASE source {accession} must not have overlap evidence"
            )
        return

    if action in ("APPEND", "REPLACE") and kind != "NEW_HOLDINGS":
        errors.append(
            f"{context} {action} source {accession} must be NEW_HOLDINGS"
        )
    if kind != "NEW_HOLDINGS":
        if overlap is not None:
            errors.append(
                f"{context} non-NEW_HOLDINGS source {accession} must not have "
                "overlap evidence"
            )
        return
    if overlap is None and action == "SUPERSEDED":
        # It may have preceded a later complete restatement and never needed
        # to be evaluated by the active reducer.
        return
    if not isinstance(overlap, dict):
        errors.append(
            f"{context} source {accession} has invalid new_holdings_overlap"
        )
        return

    required = {
        "identity_version",
        "matched_rows",
        "prior_rows",
        "amendment_rows",
        "exact_positions",
    }
    if set(overlap) != required:
        errors.append(
            f"{context} source {accession} has invalid overlap evidence fields"
        )
        return
    if overlap.get("identity_version") != _NEW_HOLDINGS_IDENTITY_VERSION:
        errors.append(
            f"{context} source {accession} has unsupported overlap "
            "identity_version"
        )

    counts = [
        overlap.get("matched_rows"),
        overlap.get("prior_rows"),
        overlap.get("amendment_rows"),
    ]
    if any(type(value) is not int or value < 0 for value in counts):
        errors.append(
            f"{context} source {accession} has invalid overlap row counts"
        )
        return
    matched_rows, prior_rows, amendment_rows = counts
    if matched_rows > min(prior_rows, amendment_rows):
        errors.append(
            f"{context} source {accession} overlap exceeds its row counts"
        )
    if amendment_rows == 0:
        errors.append(
            f"{context} source {accession} has an empty NEW_HOLDINGS table"
        )

    exact_positions = overlap.get("exact_positions")
    if type(exact_positions) is not bool:
        errors.append(
            f"{context} source {accession} has invalid exact_positions flag"
        )
        return
    if exact_positions and not (
        matched_rows > 0
        and matched_rows == prior_rows
        and matched_rows == amendment_rows
    ):
        errors.append(
            f"{context} source {accession} has inconsistent exact_positions evidence"
        )

    if action == "APPEND" and matched_rows != 0:
        errors.append(
            f"{context} APPEND source {accession} overlaps the active portfolio"
        )
    if action == "REPLACE":
        near_complete = (
            matched_rows >= _NEW_HOLDINGS_REPLACEMENT_MIN_MATCHED_ROWS
            and matched_rows * _NEW_HOLDINGS_REPLACEMENT_COVERAGE_DENOMINATOR
            >= prior_rows * _NEW_HOLDINGS_REPLACEMENT_COVERAGE_NUMERATOR
            and matched_rows * _NEW_HOLDINGS_REPLACEMENT_COVERAGE_DENOMINATOR
            >= amendment_rows * _NEW_HOLDINGS_REPLACEMENT_COVERAGE_NUMERATOR
        )
        if not exact_positions and not near_complete:
            errors.append(
                f"{context} REPLACE source {accession} lacks clear replacement "
                "overlap"
            )


def validate_amendment_composition(
    quarter: dict,
    context: str,
    errors: list[str],
) -> None:
    """Validate the semantic contract for amendment-composed quarters.

    Quarters without composition metadata predate the amendment reducer and
    intentionally remain valid. Versioned quarters must be complete,
    internally reconciled, and carry a coherent accession chain. Version 2
    additionally proves whether each NEW_HOLDINGS source appended or replaced.
    """
    if "composition_version" not in quarter:
        return
    version = quarter.get("composition_version")
    if type(version) is not int or version not in {1, 2}:
        errors.append(f"{context} has unsupported composition_version {version!r}")
        return
    identity_version = quarter.get("security_identity_version")
    if (
        identity_version is not None
        and (
            type(identity_version) is not int
            or identity_version != _SECURITY_IDENTITY_VERSION
        )
    ):
        errors.append(
            f"{context} has unsupported security_identity_version "
            f"{identity_version!r}"
        )
    hash_version = quarter.get("composition_hash_version", 1)
    if (
        type(hash_version) is not int
        or hash_version not in {1, 2, _COMPOSITION_HASH_VERSION}
    ):
        errors.append(
            f"{context} has unsupported composition_hash_version "
            f"{hash_version!r}"
        )

    if quarter.get("is_complete") is not True:
        errors.append(f"{context} composition is not complete")

    holdings = quarter.get("holdings")
    if isinstance(holdings, list):
        num_holdings = quarter.get("num_holdings")
        if type(num_holdings) is not int:
            errors.append(f"{context} composition has invalid num_holdings")
        elif num_holdings != len(holdings):
            errors.append(
                f"{context} composition num_holdings={num_holdings} does not match "
                f"holdings length {len(holdings)}"
            )

        values: list[int | float] = []
        for h_idx, holding in enumerate(holdings):
            value = holding.get("value") if isinstance(holding, dict) else None
            if not is_finite_number(value):
                errors.append(
                    f"{context} composition holding {h_idx} has non-numeric value"
                )
                continue
            values.append(value)

        total_value = quarter.get("total_value")
        if not is_finite_number(total_value):
            errors.append(f"{context} composition has invalid total_value")
        elif len(values) == len(holdings) and total_value != sum(values):
            errors.append(
                f"{context} composition total_value={total_value} does not match "
                f"holdings total {sum(values)}"
            )
    else:
        errors.append(f"{context} composition has non-list holdings")

    composition_hash = quarter.get("composition_hash")
    if not isinstance(composition_hash, str) or not _COMPOSITION_HASH_RE.fullmatch(
        composition_hash
    ):
        errors.append(
            f"{context} composition_hash must be 64 lowercase hexadecimal characters"
        )

    base_accession = quarter.get("base_accession")
    if not isinstance(base_accession, str) or not base_accession.strip():
        errors.append(f"{context} composition has invalid base_accession")
        base_accession = None

    raw_applied_accessions = quarter.get("applied_accessions")
    applied_accessions: list[str] = []
    if not isinstance(raw_applied_accessions, list):
        errors.append(f"{context} composition has non-list applied_accessions")
    else:
        invalid_applied = [
            accession
            for accession in raw_applied_accessions
            if not isinstance(accession, str) or not accession.strip()
        ]
        if invalid_applied:
            errors.append(f"{context} composition has invalid applied accession")
        applied_accessions = [
            accession
            for accession in raw_applied_accessions
            if isinstance(accession, str) and accession.strip()
        ]
        if len(applied_accessions) != len(set(applied_accessions)):
            errors.append(f"{context} composition has duplicate applied accessions")

    if not applied_accessions:
        errors.append(f"{context} composition has no applied accessions")
    elif base_accession is not None and applied_accessions[0] != base_accession:
        errors.append(
            f"{context} base_accession is not first in applied_accessions"
        )

    raw_identity_sources = quarter.get("reported_identity_sources")
    identity_sources: list[dict[str, str]] = []
    if identity_version == _SECURITY_IDENTITY_VERSION:
        if not isinstance(raw_identity_sources, list):
            errors.append(
                f"{context} security identity proof is missing "
                "reported_identity_sources"
            )
        else:
            for source_idx, source in enumerate(raw_identity_sources):
                if not isinstance(source, dict) or set(source) != {
                    "accession",
                    "report_date",
                    "url",
                    "sha256",
                }:
                    errors.append(
                        f"{context} reported identity source {source_idx} "
                        "must contain accession, report_date, url, and sha256"
                    )
                    continue
                accession = source.get("accession")
                report_date = source.get("report_date")
                url = source.get("url")
                checksum = source.get("sha256")
                try:
                    normalized_url = normalize_sec_identity_source_url(
                        url,
                        accession=accession,
                    )
                except (Sec13FBulkError, TypeError, ValueError):
                    normalized_url = None
                if (
                    not isinstance(accession, str)
                    or accession not in applied_accessions
                    or report_date != quarter.get("report_date")
                    or not isinstance(url, str)
                    or normalized_url != url
                    or not isinstance(checksum, str)
                    or _COMPOSITION_HASH_RE.fullmatch(checksum) is None
                ):
                    errors.append(
                        f"{context} reported identity source {source_idx} "
                        "is not canonical exact SEC evidence"
                    )
                    continue
                identity_sources.append(source)
            canonical_identity_sources = sorted(
                identity_sources,
                key=lambda source: (
                    source["accession"],
                    source["report_date"],
                    source["url"],
                    source["sha256"],
                ),
            )
            if (
                len(identity_sources) != len(raw_identity_sources)
                or identity_sources != canonical_identity_sources
                or len({
                    (
                        source["accession"],
                        source["report_date"],
                        source["url"],
                        source["sha256"],
                    )
                    for source in identity_sources
                })
                != len(identity_sources)
            ):
                errors.append(
                    f"{context} reported_identity_sources are not unique "
                    "and canonically ordered"
                )
            if isinstance(holdings, list):
                covered_filings = {
                    (source["accession"], source["report_date"])
                    for source in identity_sources
                }
                missing_evidence = sorted({
                    (
                        str(holding.get("accession") or ""),
                        str(holding.get("report_date") or ""),
                    )
                    for holding in holdings
                    if isinstance(holding, dict)
                    and (
                        str(holding.get("accession") or ""),
                        str(holding.get("report_date") or ""),
                    )
                    not in covered_filings
                })
                if missing_evidence:
                    errors.append(
                        f"{context} reported_identity_sources do not cover "
                        f"{len(missing_evidence)} holding filing identity(s)"
                    )

    source_filings = quarter.get("source_filings")
    if not isinstance(source_filings, list):
        errors.append(f"{context} composition has non-list source_filings")
        return

    source_by_accession: dict[str, dict] = {}
    valid_sources: list[dict] = []
    applied_sources: list[dict] = []
    for source_idx, source in enumerate(source_filings):
        if not isinstance(source, dict):
            errors.append(
                f"{context} composition source filing {source_idx} is not an object"
            )
            continue

        accession = source.get("accession")
        if not isinstance(accession, str) or not accession.strip():
            errors.append(
                f"{context} composition source filing {source_idx} has invalid accession"
            )
            continue
        if accession in source_by_accession:
            errors.append(
                f"{context} composition has duplicate source accession {accession}"
            )
        else:
            source_by_accession[accession] = source
        valid_sources.append(source)

        if type(source.get("applied")) is not bool:
            errors.append(
                f"{context} composition source {accession} has invalid applied flag"
            )
        elif source["applied"]:
            applied_sources.append(source)

        nested_identity_source = source.get("reported_identity_source")
        if nested_identity_source is not None and (
            not isinstance(nested_identity_source, dict)
            or nested_identity_source not in identity_sources
        ):
            errors.append(
                f"{context} source {accession} has unbound reported "
                "identity evidence"
            )

        source_hash = source.get("source_hash")
        if not isinstance(source_hash, str) or not _COMPOSITION_HASH_RE.fullmatch(
            source_hash
        ):
            errors.append(
                f"{context} source {accession} has invalid source_hash"
            )
        form_type = source.get("form_type")
        if (
            not isinstance(form_type, str)
            or form_type not in ("13F-HR", "13F-HR/A")
        ):
            errors.append(
                f"{context} source {accession} has invalid form_type {form_type!r}"
            )
        amendment_kind = source.get("amendment_kind")
        if amendment_kind == "ORIGINAL" and form_type != "13F-HR":
            errors.append(
                f"{context} original source {accession} must use form 13F-HR"
            )
        elif (
            isinstance(amendment_kind, str)
            and amendment_kind in ("RESTATEMENT", "NEW_HOLDINGS", "UNKNOWN")
            and form_type != "13F-HR/A"
        ):
            errors.append(
                f"{context} amendment source {accession} must use form 13F-HR/A"
            )
        if version == 2:
            _validate_v2_source_decision(
                source, accession, context, errors
            )
        source_identity_version = source.get("security_identity_version")
        if (
            source_identity_version is not None
            and (
                type(source_identity_version) is not int
                or source_identity_version != _SECURITY_IDENTITY_VERSION
            )
        ):
            errors.append(
                f"{context} source {accession} has unsupported "
                f"security_identity_version {source_identity_version!r}"
            )
        if filing_date_precision(source.get("filing_date")) != "DAY":
            errors.append(
                f"{context} source {accession} has invalid filing_date; "
                "structured provenance requires a valid YYYY-MM-DD date"
            )
        for total_key in ("reported_entry_total", "reported_value_total"):
            total = source.get(total_key)
            if isinstance(total, bool) or not isinstance(total, int) or total < 0:
                errors.append(
                    f"{context} source {accession} has invalid {total_key}"
                )

        cover_fields = (
            "cover_reported_entry_total",
            "cover_reported_value_total",
            "cover_reconciliation_status",
        )
        present_cover_fields = [
            field for field in cover_fields if field in source
        ]
        if present_cover_fields and len(present_cover_fields) != len(cover_fields):
            errors.append(
                f"{context} source {accession} has incomplete cover "
                "reconciliation metadata"
            )
        elif len(present_cover_fields) == len(cover_fields):
            cover_entries = source.get("cover_reported_entry_total")
            cover_value = source.get("cover_reported_value_total")
            cover_status = source.get("cover_reconciliation_status")
            if (
                isinstance(cover_entries, bool)
                or not isinstance(cover_entries, int)
                or cover_entries < 0
            ):
                errors.append(
                    f"{context} source {accession} has invalid "
                    "cover_reported_entry_total"
                )
            if (
                isinstance(cover_value, bool)
                or not isinstance(cover_value, int)
                or cover_value < 0
            ):
                errors.append(
                    f"{context} source {accession} has invalid "
                    "cover_reported_value_total"
                )
            if cover_status not in ("EXACT", "MISMATCH_UNIQUE_TABLE"):
                errors.append(
                    f"{context} source {accession} has invalid "
                    "cover_reconciliation_status"
                )
            elif (
                isinstance(cover_entries, int)
                and not isinstance(cover_entries, bool)
                and isinstance(cover_value, int)
                and not isinstance(cover_value, bool)
            ):
                table_totals = (
                    source.get("reported_entry_total"),
                    source.get("reported_value_total"),
                )
                cover_totals = (cover_entries, cover_value)
                if cover_status == "EXACT" and cover_totals != table_totals:
                    errors.append(
                        f"{context} source {accession} marks unequal cover "
                        "and table totals as EXACT"
                    )
                elif (
                    cover_status == "MISMATCH_UNIQUE_TABLE"
                    and cover_totals == table_totals
                ):
                    errors.append(
                        f"{context} source {accession} marks equal cover "
                        "and table totals as mismatched"
                    )

        unit_version = source.get("value_unit_policy_version")
        if unit_version is not None:
            if (
                isinstance(unit_version, bool)
                or not isinstance(unit_version, int)
                or unit_version not in _SUPPORTED_VALUE_UNIT_POLICY_VERSIONS
            ):
                errors.append(
                    f"{context} source {accession} has unsupported "
                    f"value_unit_policy_version {unit_version!r}"
                )
            multiplier = source.get("value_multiplier")
            if type(multiplier) is not int or multiplier not in (1, 1000):
                errors.append(
                    f"{context} source {accession} has invalid value_multiplier"
                )
            normalized_total = source.get("normalized_value_total")
            reported_total = source.get("reported_value_total")
            if (
                isinstance(normalized_total, bool)
                or not isinstance(normalized_total, int)
                or normalized_total < 0
            ):
                errors.append(
                    f"{context} source {accession} has invalid "
                    "normalized_value_total"
                )
            elif (
                type(multiplier) is int
                and multiplier in (1, 1000)
                and isinstance(reported_total, int)
                and normalized_total != reported_total * multiplier
            ):
                errors.append(
                    f"{context} source {accession} normalized_value_total "
                    "does not match its SEC-reported total and multiplier"
                )
            if not isinstance(source.get("value_unit_method"), str):
                errors.append(
                    f"{context} source {accession} has invalid value_unit_method"
                )
            if source.get("value_unit_confidence") not in ("high", "low"):
                errors.append(
                    f"{context} source {accession} has invalid "
                    "value_unit_confidence"
                )
            if not isinstance(source.get("value_unit_evidence"), dict):
                errors.append(
                    f"{context} source {accession} has invalid value_unit_evidence"
                )

    source_applied_accessions = [source["accession"] for source in applied_sources]
    if source_applied_accessions != applied_accessions:
        errors.append(
            f"{context} applied_accessions do not match applied source_filings"
        )
    if identity_version == _SECURITY_IDENTITY_VERSION:
        if version != 2:
            errors.append(
                f"{context} security identity proof requires composition v2"
            )
        if hash_version != _COMPOSITION_HASH_VERSION:
            errors.append(
                f"{context} security identity proof requires composition hash v"
                f"{_COMPOSITION_HASH_VERSION}"
            )
        if not applied_sources or any(
            source.get("security_identity_version")
            != _SECURITY_IDENTITY_VERSION
            for source in applied_sources
        ):
            errors.append(
                f"{context} top-level security identity proof is not backed "
                "by every applied source"
            )
    elif any(
        source.get("security_identity_version")
        == _SECURITY_IDENTITY_VERSION
        for source in applied_sources
    ):
        errors.append(
            f"{context} applied source identity proof is missing its "
            "top-level marker"
        )

    if base_accession is not None and base_accession not in source_by_accession:
        errors.append(
            f"{context} base_accession is missing from source_filings"
        )

    if applied_accessions:
        latest_accession = applied_accessions[-1]
        if quarter.get("accession") != latest_accession:
            errors.append(
                f"{context} top-level accession does not match latest applied source"
            )
        latest_source = source_by_accession.get(latest_accession)
        if (
            latest_source is not None
            and quarter.get("filing_date") != latest_source.get("filing_date")
        ):
            errors.append(
                f"{context} top-level filing_date does not match latest applied source"
            )

    accepted_at = [source.get("accepted_at") for source in valid_sources]
    if len(valid_sources) > 1:
        if any(not isinstance(value, str) or not value.strip() for value in accepted_at):
            errors.append(
                f"{context} source_filings have invalid accepted_at values"
            )
        elif accepted_at != sorted(accepted_at):
            errors.append(
                f"{context} source_filings are not in acceptance order"
            )

    original_indexes = [
        index
        for index, source in enumerate(valid_sources)
        if source.get("amendment_kind") == "ORIGINAL"
    ]
    if len(original_indexes) > 1:
        errors.append(f"{context} composition has multiple original sources")
    elif original_indexes and original_indexes[0] != 0:
        errors.append(f"{context} original source is not first")

    active_sources = applied_sources
    if base_accession is not None and base_accession in source_by_accession:
        base_index = next(
            index
            for index, source in enumerate(valid_sources)
            if source.get("accession") == base_accession
        )
        active_sources = valid_sources[base_index:]
        active_accessions = [source["accession"] for source in active_sources]
        if active_accessions != applied_accessions:
            errors.append(
                f"{context} applied_accessions do not match active source tail"
            )
        active_flags = [source.get("applied") is True for source in valid_sources]
        expected_flags = [index >= base_index for index in range(len(valid_sources))]
        if active_flags != expected_flags:
            errors.append(
                f"{context} source applied flags do not identify the active source tail"
            )

    if active_sources and version == 1:
        base_kind = active_sources[0].get("amendment_kind")
        if base_kind not in ("ORIGINAL", "RESTATEMENT"):
            errors.append(
                f"{context} applied base source has invalid amendment_kind {base_kind!r}"
            )
        for source in active_sources[1:]:
            kind = source.get("amendment_kind")
            if kind != "NEW_HOLDINGS":
                errors.append(
                    f"{context} later applied source {source['accession']} has invalid "
                    f"amendment_kind {kind!r}"
                )

        expected_number = 1
        if base_kind == "RESTATEMENT":
            base_number = active_sources[0].get("amendment_number")
            if not isinstance(base_number, int) or base_number < 1:
                errors.append(
                    f"{context} restatement base has invalid amendment_number"
                )
            else:
                expected_number = base_number + 1
        for source in active_sources[1:]:
            number = source.get("amendment_number")
            if not isinstance(number, int) or number != expected_number:
                errors.append(
                    f"{context} active source {source['accession']} has invalid "
                    f"amendment_number {number!r}; expected {expected_number}"
                )
            expected_number += 1

    if active_sources and version == 2:
        base_source = active_sources[0]
        base_kind = base_source.get("amendment_kind")
        base_action = base_source.get("composition_action")
        if base_kind in ("ORIGINAL", "RESTATEMENT"):
            if base_action != "BASE":
                errors.append(
                    f"{context} applied complete base source must use BASE action"
                )
        elif base_kind == "NEW_HOLDINGS":
            if base_action != "REPLACE":
                errors.append(
                    f"{context} applied NEW_HOLDINGS base must use REPLACE action"
                )
        else:
            errors.append(
                f"{context} applied base source has invalid amendment_kind "
                f"{base_kind!r}"
            )

        for source in active_sources[1:]:
            kind = source.get("amendment_kind")
            if kind != "NEW_HOLDINGS":
                errors.append(
                    f"{context} later applied source {source['accession']} has "
                    f"invalid amendment_kind {kind!r}"
                )
            if source.get("composition_action") != "APPEND":
                errors.append(
                    f"{context} later applied source {source['accession']} must "
                    "use APPEND action"
                )

        if base_accession is not None and base_accession in source_by_accession:
            base_index = next(
                index
                for index, source in enumerate(valid_sources)
                if source.get("accession") == base_accession
            )
            for source in valid_sources[:base_index]:
                if source.get("composition_action") != "SUPERSEDED":
                    errors.append(
                        f"{context} non-applied source {source['accession']} must "
                        "use SUPERSEDED action"
                    )

        complete_indexes = [
            index
            for index, source in enumerate(valid_sources)
            if source.get("amendment_kind") in ("ORIGINAL", "RESTATEMENT")
        ]
        if not complete_indexes:
            errors.append(f"{context} composition has no declared complete base")
        else:
            declared_base_index = complete_indexes[-1]
            declared_base = valid_sources[declared_base_index]
            declared_tail = valid_sources[declared_base_index:]
            for source in declared_tail[1:]:
                if (
                    source.get("amendment_kind") == "NEW_HOLDINGS"
                    and not isinstance(source.get("new_holdings_overlap"), dict)
                ):
                    errors.append(
                        f"{context} evaluated NEW_HOLDINGS source "
                        f"{source['accession']} is missing overlap evidence"
                    )
            for source in declared_tail[1:]:
                if source.get("amendment_kind") != "NEW_HOLDINGS":
                    errors.append(
                        f"{context} declared active source {source['accession']} "
                        "must be NEW_HOLDINGS"
                    )

            expected_number = 1
            if declared_base.get("amendment_kind") == "RESTATEMENT":
                base_number = declared_base.get("amendment_number")
                if not isinstance(base_number, int) or base_number < 1:
                    errors.append(
                        f"{context} restatement base has invalid amendment_number"
                    )
                else:
                    expected_number = base_number + 1
            for source in declared_tail[1:]:
                number = source.get("amendment_number")
                if not isinstance(number, int) or number != expected_number:
                    errors.append(
                        f"{context} active source {source['accession']} has invalid "
                        f"amendment_number {number!r}; expected {expected_number}"
                    )
                expected_number += 1

    if applied_sources and all(
        source.get("value_unit_policy_version")
        in _SUPPORTED_VALUE_UNIT_POLICY_VERSIONS
        and type(source.get("normalized_value_total")) is int
        for source in applied_sources
    ):
        expected_quarter_total = sum(
            source.get("normalized_value_total", 0) for source in applied_sources
        )
        if quarter.get("total_value") != expected_quarter_total:
            errors.append(
                f"{context} total_value does not match applied sources' "
                "normalized value totals"
            )

    can_recompute = (
        type(hash_version) is int
        and hash_version in {1, 2, _COMPOSITION_HASH_VERSION}
        and isinstance(holdings, list)
        and all(isinstance(holding, dict) for holding in holdings)
        and isinstance(raw_applied_accessions, list)
        and raw_applied_accessions == applied_accessions
        and len(valid_sources) == len(source_filings)
        and len(source_by_accession) == len(source_filings)
        and all(accession in source_by_accession for accession in applied_accessions)
        and all(
            isinstance(source_by_accession[accession].get("source_hash"), str)
            and _COMPOSITION_HASH_RE.fullmatch(
                source_by_accession[accession]["source_hash"]
            )
            for accession in applied_accessions
        )
        and isinstance(base_accession, str)
    )
    if can_recompute and composition_hash != calculate_composition_hash(quarter):
        errors.append(f"{context} composition_hash does not match composition content")


def validate_value_unit_peer_consistency(
    report_references: dict[tuple[str, str], tuple[float, int]],
    errors: list[str],
    quarter_peer_references: dict[
        tuple[str, str, str],
        tuple[tuple[float, str], ...],
    ]
    | None = None,
    *, paths: list[Path] | None = None,
) -> None:
    """Reject quarter-wide disagreements with same-security peers."""
    for fp in sorted(FUNDS_DIR.glob("*.json")) if paths is None else sorted(paths):
        try:
            with open(fp) as f:
                fund = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(fund, dict):
            continue
        filer_id = canonical_cik(fund.get("cik")) or f"invalid:{fp.name}"

        for quarter_idx, quarter in enumerate(fund.get("quarters", [])):
            if not isinstance(quarter, dict):
                continue
            holdings = quarter.get("holdings")
            report_date = str(quarter.get("report_date") or "")
            if not isinstance(holdings, list) or not report_date:
                continue

            if quarter_peer_references is not None:
                health_references = same_date_peer_price_references(
                    quarter_peer_references,
                    filer_id=filer_id,
                    quarter=quarter,
                )
                issue = peer_price_quarter_health_issue(
                    quarter,
                    health_references,
                )
                if issue is not None:
                    errors.append(
                        f"fund file {fp.name} quarter {quarter_idx} "
                        f"({report_date}) failed quarter health "
                        f"[{issue.code}]: {issue.detail}"
                    )

            peer_prices = {}
            for holding in holdings:
                if not isinstance(holding, dict):
                    continue
                cusip = str(holding.get("cusip") or "").strip().upper()
                reference = report_references.get((report_date, cusip))
                if reference:
                    peer_prices[cusip] = reference

            evidence = peer_scale_evidence(holdings, peer_prices)
            status = evidence["status"]
            if status is None:
                continue
            if status == "mixed_scale_clusters":
                if (
                    evidence["intrinsic_count_value_conflict"]
                    and evidence["matched_count_coverage"]
                    < PEER_MIN_SCALE_COUNT_SUPPORT
                ):
                    detail = (
                        "intrinsic low-price count/value support="
                        f"{evidence['low_price_count_support']:.3f}/"
                        f"{evidence['low_price_value_support']:.3f}, "
                        "peer count coverage="
                        f"{evidence['matched_count_coverage']:.3f}"
                    )
                else:
                    detail = (
                        "count/value support="
                        f"aligned {evidence['aligned_count_support']:.3f}/"
                        f"{evidence['aligned_value_support']:.3f}, "
                        f"inflated {evidence['inflated_count_support']:.3f}/"
                        f"{evidence['inflated_value_support']:.3f}, "
                        "understated "
                        f"{evidence['understated_count_support']:.3f}/"
                        f"{evidence['understated_value_support']:.3f}"
                    )
            else:
                support_key = (
                    "inflated_value_support"
                    if status == "inflated_1000x"
                    else "understated_value_support"
                )
                detail = f"scale_support={evidence[support_key]:.3f}"
            errors.append(
                f"fund file {fp.name} quarter {quarter_idx} "
                f"({report_date}) has a {status} value-unit "
                f"mismatch: matched_cusips={evidence['matched_cusips']}, "
                f"value_coverage={evidence['matched_value_coverage']:.3f}, "
                f"{detail}"
            )


def _quarter_ordinal(report_date: object) -> int | None:
    code = report_quarter_code(report_date)
    if code is None:
        return None
    year, quarter = divmod(code, 10)
    return year * 4 + quarter


def validate_adjacent_quarter_value_units(
    fund: dict,
    context: str,
    errors: list[str],
) -> None:
    """Reject broad, coherent 1,000x value-unit jumps across adjacent quarters.

    Comparing value per reported share or principal makes the check insensitive
    to position-size changes. It intentionally accepts every instrument type,
    including PRN-backed notes, and does not depend on unit provenance that
    legacy persisted quarters may lack.
    """
    quarters = fund.get("quarters")
    if not isinstance(quarters, list):
        return

    dated_quarters: list[tuple[int, int, dict]] = []
    for quarter_idx, quarter in enumerate(quarters):
        if not isinstance(quarter, dict):
            continue
        ordinal = _quarter_ordinal(quarter.get("report_date"))
        if ordinal is not None:
            dated_quarters.append((ordinal, quarter_idx, quarter))
    dated_quarters.sort(key=lambda item: item[0], reverse=True)

    for newer, older in zip(dated_quarters, dated_quarters[1:]):
        newer_ordinal, newer_idx, newer_quarter = newer
        older_ordinal, older_idx, older_quarter = older
        if newer_ordinal - older_ordinal != 1:
            continue

        newer_holdings = newer_quarter.get("holdings")
        older_holdings = older_quarter.get("holdings")
        if not isinstance(newer_holdings, list) or not isinstance(
            older_holdings,
            list,
        ):
            continue
        evidence = adjacent_quarter_scale_evidence(
            newer_holdings,
            older_holdings,
        )
        status = evidence["status"]
        if status in {None, "aligned_1x"}:
            continue

        if status in {"inflated_1000x", "understated_1000x"}:
            prefix = (
                "inflated" if status == "inflated_1000x" else "understated"
            )
            direction = "higher" if prefix == "inflated" else "lower"
            detail = (
                f"newer value per share/principal is about "
                f"{VALUE_UNIT_SCALE}x {direction} for "
                f"{evidence[f'{prefix}_positions']}/"
                f"{evidence['matched_positions']} shared positions "
                f"(count support="
                f"{evidence[f'{prefix}_count_support']:.3f}, "
                f"raw-value support="
                f"{evidence[f'{prefix}_raw_value_support']:.3f})"
            )
        else:
            detail = (
                "shared positions contain mixed value-unit clusters "
                f"(matched={evidence['matched_positions']}, "
                f"aligned count/value="
                f"{evidence['aligned_count_support']:.3f}/"
                f"{evidence['aligned_raw_value_support']:.3f}, "
                f"inflated count/value="
                f"{evidence['inflated_count_support']:.3f}/"
                f"{evidence['inflated_raw_value_support']:.3f}, "
                f"understated count/value="
                f"{evidence['understated_count_support']:.3f}/"
                f"{evidence['understated_raw_value_support']:.3f})"
            )
        errors.append(
            f"{context} quarters {newer_idx} "
            f"({newer_quarter.get('report_date')}) and {older_idx} "
            f"({older_quarter.get('report_date')}) have an adjacent-quarter "
            f"value-unit discontinuity: {detail}"
        )


def validate_funds(
    errors: list[str],
    registry: dict[str, dict] | None = None,
    quality_summary: dict[str, object] | None = None,
    *,
    paths: list[Path] | None = None,
    check_peers: bool = True,
    peer_observations: dict | None = None,
    quantity_evidence: dict | None = None,
) -> tuple[
    dict[str, Path],
    dict[str, dict[str, set[str]]],
    set[str],
    dict[str, dict],
    dict[str, dict[str, int | float | None]],
]:
    registry = registry or {}
    fund_files: dict[str, Path] = {}
    stock_groups: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: {"cusips": set(), "issuers": set()}
    )
    fund_cusips: set[str] = set()
    fund_calendars: dict[str, dict] = {}
    expected_current_stats: dict[
        str, dict[str, int | float | None]
    ] = defaultdict(_empty_current_stats)
    if quantity_evidence is None:
        quantity_evidence = load_quantity_evidence(
            cache_dir_for_funds(FUNDS_DIR) / "quantity_estimation_evidence.json"
        )
    quarter_peer_price_index = (
        defaultdict(lambda: defaultdict(list))
        if peer_observations is None else peer_observations
    )

    for fp in sorted(FUNDS_DIR.glob("*.json")) if paths is None else sorted(paths):
        fund_files[fp.stem] = fp
        fund = load_json(fp, errors)
        if not isinstance(fund, dict):
            continue

        raw_cik = fund.get("cik")
        cik = canonical_cik(raw_cik)
        if cik is None:
            errors.append(
                f"fund file {fp.name} has invalid cik {raw_cik!r}; "
                "expected a positive integer"
            )
        elif cik != fp.stem:
            errors.append(f"fund file {fp.name} has mismatched cik {raw_cik!r}")

        raw_fund_name = fund.get("name")
        fund_name = (
            raw_fund_name
            if isinstance(raw_fund_name, str) and raw_fund_name.strip()
            else None
        )
        if fund_name is None:
            errors.append(
                f"fund file {fp.name} has invalid filer name "
                f"{raw_fund_name!r}; expected a non-empty SEC legal name"
            )

        quarters = fund.get("quarters", [])
        if not isinstance(quarters, list):
            errors.append(f"fund file {fp.name} has non-list quarters")
            continue

        report_dates = [
            q.get("report_date") for q in quarters if isinstance(q, dict)
        ]
        valid_report_dates = [
            report_date
            for report_date in report_dates
            if report_quarter_code(report_date) is not None
        ]
        if len(valid_report_dates) != len(report_dates):
            errors.append(
                f"fund file {fp.name} has non-canonical report_date; "
                "expected a YYYY-MM-DD calendar quarter end"
            )
        if report_dates != sorted(valid_report_dates, reverse=True):
            errors.append(f"fund file {fp.name} has unsorted quarters")
        if len(valid_report_dates) != len(set(valid_report_dates)):
            errors.append(f"fund file {fp.name} has duplicate report_date values")
        calendar_dates = tuple(sorted(set(valid_report_dates), reverse=True))
        if cik is not None:
            fund_calendars[cik] = {
                "report_dates": calendar_dates,
                "report_date_set": frozenset(calendar_dates),
                "q": tuple(expected_fund_quarter_codes(calendar_dates)),
                **({"name": fund_name} if fund_name is not None else {}),
            }

        validate_adjacent_quarter_value_units(
            fund,
            f"fund file {fp.name}",
            errors,
        )

        for idx, quarter in enumerate(quarters):
            if not isinstance(quarter, dict):
                errors.append(f"fund file {fp.name} quarter {idx} is not an object")
                continue
            if not quarter.get("report_date"):
                errors.append(f"fund file {fp.name} quarter {idx} missing report_date")
            filing_date = quarter.get("filing_date")
            filing_precision = filing_date_precision(filing_date)
            has_structured_provenance = (
                "source_filings" in quarter
                or "composition_version" in quarter
            )
            filing_context = f"fund file {fp.name} quarter {idx}"
            if filing_precision == "MONTH":
                if has_structured_provenance:
                    errors.append(
                        f"{filing_context} has month-only filing_date "
                        f"{filing_date!r} despite structured provenance"
                    )
                elif quality_summary is not None:
                    legacy_count = quality_summary.get(
                        "legacy_month_precision_filing_dates", 0
                    )
                    quality_summary[
                        "legacy_month_precision_filing_dates"
                    ] = (
                        legacy_count if type(legacy_count) is int else 0
                    ) + 1
            elif filing_precision != "DAY":
                errors.append(
                    f"{filing_context} has invalid filing_date "
                    f"{filing_date!r}; expected a valid YYYY-MM-DD date or "
                    "retained legacy YYYY-MM month"
                )

            if (
                quality_summary is not None
                and type(quarter.get("composition_version")) is int
            ):
                summary_key = (
                    "current_composition_hash_quarters"
                    if quarter.get("composition_hash_version", 1)
                    == _COMPOSITION_HASH_VERSION
                    else "legacy_composition_hash_quarters"
                )
                quality_summary[summary_key] = (
                    int(quality_summary.get(summary_key, 0)) + 1
                )

            quarter_unit_version = quarter.get("value_unit_policy_version")
            if quarter_unit_version is not None:
                context = f"fund file {fp.name} quarter {idx}"
                if (
                    type(quarter_unit_version) is not int
                    or quarter_unit_version
                    not in _SUPPORTED_VALUE_UNIT_POLICY_VERSIONS
                ):
                    errors.append(
                        f"{context} has unsupported value_unit_policy_version"
                    )
                elif (
                    quality_summary is not None
                    and quarter_unit_version
                    in _LEGACY_VALUE_UNIT_POLICY_VERSIONS
                    and quarter_unit_version != VALUE_UNIT_POLICY_VERSION
                ):
                    quality_summary["legacy_value_unit_quarters"] = (
                        int(
                            quality_summary.get(
                                "legacy_value_unit_quarters", 0
                            )
                        )
                        + 1
                    )
                if (
                    type(quarter.get("value_multiplier")) is not int
                    or quarter.get("value_multiplier") not in {1, 1000}
                ):
                    errors.append(f"{context} has invalid value_multiplier")
                if not isinstance(quarter.get("value_unit_method"), str):
                    errors.append(f"{context} has invalid value_unit_method")
                if quarter.get("value_unit_confidence") not in {"high", "low"}:
                    errors.append(f"{context} has invalid value_unit_confidence")
                if not isinstance(quarter.get("value_unit_evidence"), dict):
                    errors.append(f"{context} has invalid value_unit_evidence")

            if quality_summary is not None:
                source_filings = quarter.get("source_filings")
                applied_unit_versions: list[int] = []
                if isinstance(source_filings, list):
                    legacy_sources = sum(
                        1
                        for source in source_filings
                        if isinstance(source, dict)
                        and type(
                            source.get("value_unit_policy_version")
                        )
                        is int
                        and source.get("value_unit_policy_version")
                        in _LEGACY_VALUE_UNIT_POLICY_VERSIONS
                        and source.get("value_unit_policy_version")
                        != VALUE_UNIT_POLICY_VERSION
                    )
                    quality_summary["legacy_value_unit_sources"] = (
                        int(
                            quality_summary.get(
                                "legacy_value_unit_sources", 0
                            )
                        )
                        + legacy_sources
                    )
                    applied_unit_versions = [
                        source["value_unit_policy_version"]
                        for source in source_filings
                        if isinstance(source, dict)
                        and source.get("applied") is True
                        and type(
                            source.get("value_unit_policy_version")
                        )
                        is int
                    ]
                if (
                    type(quarter_unit_version) is not int
                    and not applied_unit_versions
                ):
                    quality_summary["without_value_unit_provenance"] = (
                        int(
                            quality_summary.get(
                                "without_value_unit_provenance", 0
                            )
                        )
                        + 1
                    )

            holdings = quarter.get("holdings", [])
            if not isinstance(holdings, list):
                errors.append(f"fund file {fp.name} quarter {idx} has non-list holdings")
                continue

            quarter_context = f"fund file {fp.name} quarter {idx}"
            health_issues = structural_quarter_health_issues(quarter)
            for issue in health_issues:
                errors.append(
                    f"{quarter_context} failed quarter health "
                    f"[{issue.code}]: {issue.detail}"
                )
            if not health_issues:
                add_quarter_peer_observations(
                    quarter_peer_price_index,
                    filer_id=cik or f"invalid:{fp.name}",
                    quarter=quarter,
                )

            validate_amendment_composition(
                quarter,
                quarter_context,
                errors,
            )

            for h_idx, holding in enumerate(holdings):
                if not isinstance(holding, dict):
                    errors.append(
                        f"fund file {fp.name} quarter {idx} holding {h_idx} is not an object"
                    )
                    continue
                holding_context = (
                    f"fund file {fp.name} quarter {idx} holding {h_idx}"
                )
                if "reported_identity_evidence" in holding:
                    errors.append(
                        f"{holding_context} contains forbidden holding-local "
                        "reported_identity_evidence; exact SEC sources belong "
                        "at the verified quarter level"
                    )
                if (
                    quarter.get("composition_hash_version")
                    == _COMPOSITION_HASH_VERSION
                    and quarter.get("security_identity_version")
                    == _SECURITY_IDENTITY_VERSION
                ):
                    # SEC information-table rows can contain a genuinely blank
                    # as-filed issuer or class.  Presence and string type are
                    # still part of the immutable v3 identity contract; never
                    # substitute canonical display metadata for either field.
                    for field in ("reported_issuer", "reported_class"):
                        if field not in holding or not isinstance(
                            holding[field], str
                        ):
                            errors.append(
                                f"{holding_context} is missing or has malformed "
                                f"immutable SEC field {field}"
                            )
                    for field in ("reported_cusip", "accession", "report_date"):
                        value = holding.get(field)
                        if not isinstance(value, str) or not value.strip():
                            errors.append(
                                f"{holding_context} is missing immutable SEC "
                                f"field {field}"
                            )
                    reported_figi = holding.get("reported_figi")
                    # Explicit null is exact negative SEC evidence, unlike an
                    # omitted key (a legacy wildcard during backfill). Keep
                    # that distinction so FIGI/null-FIGI rows verify one-to-one.
                    if reported_figi is not None and (
                        not isinstance(reported_figi, str)
                        or not reported_figi.strip()
                    ):
                        errors.append(
                            f"{holding_context} has invalid optional "
                            "reported_figi"
                        )
                    if holding.get("report_date") != quarter.get("report_date"):
                        errors.append(
                            f"{holding_context} report_date does not match its "
                            "quarter"
                        )
                    applied_accessions = quarter.get("applied_accessions")
                    if (
                        isinstance(applied_accessions, list)
                        and holding.get("accession") not in applied_accessions
                    ):
                        errors.append(
                            f"{holding_context} accession is not an applied "
                            "SEC filing"
                        )
                validate_fund_holding_identity(
                    holding,
                    holding_context,
                    errors,
                )
                lookup_id = holding_stock_id(holding, registry)
                if not holding.get("cusip") and not holding.get("ticker"):
                    errors.append(
                        f"fund file {fp.name} quarter {idx} holding {h_idx} missing identifier"
                    )
                if "value" not in holding:
                    errors.append(
                        f"fund file {fp.name} quarter {idx} holding {h_idx} missing value"
                    )
                    continue

                value = holding.get("value") or 0
                shares = holding.get("shares") or 0
                cusip = normalize_security_identifier(holding.get("cusip"))
                registry_entry = registry.get(cusip) if cusip else None
                published_type = published_holding_instrument_type(
                    holding,
                    registry_entry,
                )
                if isinstance(registry_entry, dict) and (
                    holding.get("issuer") != registry_entry.get("name")
                ):
                    errors.append(
                        f"{holding_context} issuer {holding.get('issuer')!r} "
                        "does not match its canonical SEC registry issuer "
                        f"{registry_entry.get('name')!r}"
                    )
                if isinstance(registry_entry, dict):
                    expected_ticker = expected_registry_position_ticker(
                        registry_entry,
                        published_type,
                    )
                    if holding.get("ticker") != expected_ticker:
                        errors.append(
                            f"{holding_context} ticker "
                            f"{holding.get('ticker')!r} does not match its "
                            "exact SEC registry proof "
                            f"{expected_ticker!r} for {published_type}"
                        )
                has_imputed_marker = "shares_imputed" in holding
                shares_are_imputed = holding.get("shares_imputed") is True
                if has_imputed_marker and not shares_are_imputed:
                    errors.append(
                        f"{holding_context} has invalid shares_imputed marker; "
                        "expected literal true or an absent field"
                    )
                if shares_are_imputed:
                    reported_shares = holding.get("reported_shares")
                    if (
                        isinstance(reported_shares, bool)
                        or reported_shares != 0
                    ):
                        errors.append(
                            f"{holding_context} has invalid reported_shares "
                            f"{reported_shares!r}; imputed rows must preserve "
                            "reported zero"
                        )
                if cusip:
                    fund_cusips.add(cusip)
                for issue in validate_quantity_annotation(
                    holding, quarter.get("report_date", ""), quantity_evidence,
                    cik=str(cik) if cik is not None else None,
                ):
                    errors.append(f"{holding_context} {issue}")

                if lookup_id:
                    if holding.get("cusip"):
                        stock_groups[lookup_id]["cusips"].add(
                            normalize_security_identifier(holding["cusip"])
                        )
                    issuer_key = normalize_issuer_key(holding.get("issuer"))
                    if issuer_key:
                        stock_groups[lookup_id]["issuers"].add(issuer_key)

        if cik is None:
            continue

        quarters_by_date = {
            quarter.get("report_date"): quarter
            for quarter in quarters
            if isinstance(quarter, dict)
            and report_quarter_code(quarter.get("report_date")) is not None
        }
        for report_index, report_date in enumerate(calendar_dates):
            report_quarter = quarters_by_date.get(report_date)
            if report_quarter is None:
                continue
            total_value = report_quarter.get("total_value", 0) or 0
            positions: dict[str, dict[str, int | float]] = {}
            for holding in report_quarter.get("holdings", []):
                if not isinstance(holding, dict):
                    continue
                stock_id = holding_stock_id(holding, registry)
                if not stock_id:
                    continue
                position = positions.setdefault(
                    stock_id,
                    {
                        "value": 0,
                        "shares": 0,
                        "pct_of_fund": 0.0,
                        "shares_imputed": False,
                        "quantity_unknown": False,
                    },
                )
                value = holding.get("value", 0) or 0
                shares = holding.get("shares", 0) or 0
                position["value"] += value
                position["shares"] += shares
                position["shares_imputed"] = bool(
                    position["shares_imputed"]
                    or holding.get("shares_imputed")
                )
                position["quantity_unknown"] = bool(
                    position["quantity_unknown"] or holding.get("quantity_unknown")
                )
                if total_value > 0:
                    position["pct_of_fund"] += value / total_value * 100.0
            for stock_id, position in positions.items():
                pct_of_fund = round(position["pct_of_fund"], 3)
                stats = expected_current_stats[stock_id]
                _add_history_observation(
                    stats,
                    cik=cik,
                    report_date=report_date,
                    value=position["value"],
                    shares=position["shares"],
                    pct_of_fund=pct_of_fund,
                    shares_imputed=bool(position["shares_imputed"]),
                    quantity_unknown=bool(position["quantity_unknown"]),
                )
                if report_index >= 2:
                    continue
                _add_transition_observation(
                    stats,
                    cik=cik,
                    report_date=report_date,
                    value=position["value"],
                    shares=position["shares"],
                    pct_of_fund=pct_of_fund,
                )
                if report_index != 0:
                    continue
                _add_current_position(
                    stats,
                    cik=cik,
                    value=position["value"],
                    shares=position["shares"],
                    pct_of_fund=pct_of_fund,
                )

    if check_peers:
        compiled_peer_prices = compile_peer_price_index(
            quarter_peer_price_index,
            consume=True,
        )
        unit_report_refs = {
            (report_date, cusip): (
                statistics.median(
                    price for price, _filer_id in observations
                ),
                len(observations),
            )
            for (
                report_date,
                cusip,
                instrument_type,
            ), observations in compiled_peer_prices.items()
            if instrument_type == "EQUITY" and len(observations) >= 4
        }
        validate_value_unit_peer_consistency(
            unit_report_refs,
            errors,
            compiled_peer_prices,
        )
    return (
        fund_files,
        stock_groups,
        fund_cusips,
        fund_calendars,
        dict(expected_current_stats),
    )


def validate_pipeline_state(
    fund_files: dict[str, Path],
    errors: list[str],
    warnings: list[str],
    quality_summary: dict[str, object] | None = None,
) -> dict | None:
    """Enforce the durable amendment-migration queue invariant."""
    state = load_json(STATE_PATH, errors)
    if not isinstance(state, dict):
        return None

    version = state.get("amendment_reducer_version")
    if version != _AMENDMENT_REDUCER_VERSION:
        errors.append(
            "pipeline_state.json amendment reducer migration is not complete"
        )

    pending = state.get("amendment_migration_pending", {})
    quarantined = state.get("quarantined", {})
    processed = state.get("processed", [])
    if not isinstance(pending, dict):
        errors.append("pipeline_state.json has invalid amendment_migration_pending")
        return state
    if quality_summary is not None:
        quality_summary["amendment_migration_pending"] = len(pending)
    if not isinstance(quarantined, dict):
        errors.append("pipeline_state.json has invalid quarantined map")
        quarantined = {}
    if not isinstance(processed, list):
        errors.append("pipeline_state.json has invalid processed list")
        processed = []
    processed_set = {
        accession for accession in processed if isinstance(accession, str)
    }

    feed_pending = state.get("recent_feed_pending", {})
    if not isinstance(feed_pending, dict):
        errors.append("pipeline_state.json has invalid recent_feed_pending")
    else:
        for accession, filing in feed_pending.items():
            if (not isinstance(accession, str)
                or not re.fullmatch(r"[0-9]{10}-[0-9]{2}-[0-9]{6}", accession)
                or not isinstance(filing, dict)
                or filing.get("accession") != accession
                or type(filing.get("cik")) is not int or filing["cik"] <= 0
                or accession in processed_set):
                errors.append(f"pipeline_state.json has invalid pending feed accession {accession!r}")

    for accession, target in sorted(pending.items()):
        context = f"pending amendment migration {accession}"
        if not isinstance(accession, str) or not accession:
            errors.append(f"{context} has invalid accession")
            continue
        if not isinstance(target, dict):
            errors.append(f"{context} is not an object")
            continue
        cik = target.get("cik")
        report_date = target.get("report_date")
        if not isinstance(cik, int) or cik <= 0:
            errors.append(f"{context} has invalid cik")
            continue
        if not isinstance(report_date, str) or not report_date:
            errors.append(f"{context} has invalid report_date")
            continue
        if accession in processed_set:
            errors.append(f"{context} is incorrectly marked processed")
        if accession not in quarantined:
            errors.append(f"{context} is missing quarantine diagnostics")

        fund_path = fund_files.get(str(cik))
        fund = load_json(fund_path, errors) if fund_path is not None else None
        if not isinstance(fund, dict):
            errors.append(f"{context} references missing fund {cik}")
            continue
        matching = [
            quarter
            for quarter in fund.get("quarters", [])
            if isinstance(quarter, dict)
            and quarter.get("report_date") == report_date
        ]
        if not matching:
            if version != _AMENDMENT_REDUCER_VERSION:
                errors.append(
                    f"{context} references report date not retained by fund {cik}"
                )
        elif matching[0].get("composition_version") == _AMENDMENT_REDUCER_VERSION:
            errors.append(
                f"{context} remains queued after its composed quarter was published"
            )

    if version == _AMENDMENT_REDUCER_VERSION:
        for cik, fund_path in sorted(fund_files.items()):
            fund = load_json(fund_path, errors)
            if not isinstance(fund, dict):
                continue
            for quarter in fund.get("quarters", []):
                if (
                    not isinstance(quarter, dict)
                    or quarter.get("composition_version") != 1
                ):
                    continue
                if any(
                    isinstance(source, dict)
                    and source.get("amendment_kind") == "NEW_HOLDINGS"
                    for source in quarter.get("source_filings", []) or []
                ):
                    errors.append(
                        f"fund {cik} report date {quarter.get('report_date')} "
                        "still publishes a v1 NEW_HOLDINGS composition after "
                        "the v2 migration"
                    )

    if pending:
        warnings.append(
            f"{len(pending)} amendment migration target(s) remain quarantined "
            "and will retry automatically"
        )

    identity_version = state.get("security_identity_migration_version")
    if identity_version != _SECURITY_IDENTITY_VERSION:
        errors.append(
            "pipeline_state.json security identity migration is not complete"
        )
    identity_pending = state.get(
        "security_identity_migration_pending", {}
    )
    if not isinstance(identity_pending, dict):
        errors.append(
            "pipeline_state.json has invalid "
            "security_identity_migration_pending"
        )
        identity_pending = {}
    if quality_summary is not None:
        quality_summary["security_identity_migration_pending"] = len(
            identity_pending
        )

    for key, target in sorted(identity_pending.items()):
        context = f"pending security identity migration {key}"
        if not isinstance(target, dict):
            errors.append(f"{context} is not an object")
            continue
        cik = target.get("cik")
        report_date = target.get("report_date")
        expected_key = (
            f"{cik}:{report_date}"
            if isinstance(cik, int) and isinstance(report_date, str)
            else None
        )
        if (
            not isinstance(key, str)
            or not key
            or key != expected_key
            or not isinstance(cik, int)
            or cik <= 0
            or report_quarter_code(report_date) is None
        ):
            errors.append(f"{context} has invalid stable target identity")
            continue
        if not isinstance(target.get("reason"), str) or not target["reason"]:
            errors.append(f"{context} has invalid reason")
        if (
            not isinstance(target.get("message"), str)
            or not target["message"].strip()
        ):
            errors.append(f"{context} has invalid message")
        if (
            identity_version == _SECURITY_IDENTITY_VERSION
            and (
                not isinstance(target.get("last_attempt_at"), str)
                or not target["last_attempt_at"]
            )
        ):
            errors.append(f"{context} has invalid last_attempt_at")

        fund_path = fund_files.get(str(cik))
        fund = load_json(fund_path, errors) if fund_path is not None else None
        if not isinstance(fund, dict):
            errors.append(f"{context} references missing fund {cik}")
            continue
        if any(
            isinstance(quarter, dict)
            and quarter.get("report_date") == report_date
            for quarter in fund.get("quarters", [])
        ):
            errors.append(
                f"{context} remains published instead of fail-closed"
            )

    if identity_pending:
        warnings.append(
            f"{len(identity_pending)} security identity migration target(s) "
            "remain withheld and will retry automatically"
        )

    quarter_health_pending = state.get("quarter_health_pending", {})
    if not isinstance(quarter_health_pending, dict):
        errors.append(
            "pipeline_state.json has invalid quarter_health_pending"
        )
        quarter_health_pending = {}
    if quality_summary is not None:
        quality_summary["quarter_health_pending"] = len(
            quarter_health_pending
        )
    for key, target in sorted(quarter_health_pending.items()):
        context = f"pending quarter health target {key}"
        if not isinstance(target, dict):
            errors.append(f"{context} is not an object")
            continue
        cik = target.get("cik")
        report_date = target.get("report_date")
        expected_key = (
            f"{cik}:{report_date}"
            if isinstance(cik, int) and isinstance(report_date, str)
            else None
        )
        if (
            not isinstance(key, str)
            or key != expected_key
            or not isinstance(cik, int)
            or cik <= 0
            or report_quarter_code(report_date) is None
        ):
            errors.append(f"{context} has invalid stable target identity")
            continue
        for field in ("reason", "message", "last_attempt_at"):
            if (
                not isinstance(target.get(field), str)
                or not target[field].strip()
            ):
                errors.append(f"{context} has invalid {field}")
        source_accessions = target.get("source_accessions", [])
        if (
            not isinstance(source_accessions, list)
            or any(
                not isinstance(accession, str) or not accession
                for accession in source_accessions
            )
            or len(source_accessions) != len(set(source_accessions))
        ):
            errors.append(f"{context} has invalid source_accessions")
            source_accessions = []
        for accession in source_accessions:
            if accession in processed_set:
                errors.append(
                    f"{context} source {accession} is incorrectly processed"
                )
            diagnostic = quarantined.get(accession)
            if (
                not isinstance(diagnostic, dict)
                or diagnostic.get("reason") != "QuarterHealthError"
            ):
                errors.append(
                    f"{context} source {accession} lacks quarter-health "
                    "quarantine diagnostics"
                )

        fund_path = fund_files.get(str(cik))
        fund = load_json(fund_path, errors) if fund_path is not None else None
        if not isinstance(fund, dict):
            errors.append(f"{context} references missing fund {cik}")
            continue
        if any(
            isinstance(quarter, dict)
            and quarter.get("report_date") == report_date
            for quarter in fund.get("quarters", [])
        ):
            errors.append(f"{context} remains published instead of fail-closed")
    if quarter_health_pending:
        warnings.append(
            f"{len(quarter_health_pending)} unhealthy quarter target(s) "
            "remain withheld and will retry automatically"
        )

    value_unit_migration_version = state.get("value_unit_migration_version")
    if quality_summary is not None:
        quality_summary["value_unit_migration_version"] = (
            value_unit_migration_version
        )
    if value_unit_migration_version != VALUE_UNIT_POLICY_VERSION:
        if (
            value_unit_migration_version is None
            or (
                type(value_unit_migration_version) is int
                and 0 <= value_unit_migration_version
                < VALUE_UNIT_POLICY_VERSION
            )
        ):
            warnings.append(
                "value-unit corpus migration remains pending "
                f"(state version {value_unit_migration_version!r}, "
                f"current policy {VALUE_UNIT_POLICY_VERSION})"
            )
        else:
            errors.append(
                "pipeline_state.json has invalid value_unit_migration_version "
                f"{value_unit_migration_version!r}"
            )

    # State completion is not accepted as a substitute for inspecting the
    # retained corpus. This prevents a hand-edited version flag from allowing
    # the exact legacy option contamination the migration is meant to remove.
    if identity_version == _SECURITY_IDENTITY_VERSION:
        from pipeline import (
            has_unsafe_legacy_option_identity,
            quarter_has_unsafe_legacy_option_identity,
        )

        unsafe_count = 0
        examples: list[str] = []
        for cik, fund_path in sorted(fund_files.items()):
            fund = load_json(fund_path, errors)
            if not isinstance(fund, dict):
                continue
            for quarter in fund.get("quarters", []):
                if not quarter_has_unsafe_legacy_option_identity(quarter):
                    continue
                for holding in quarter.get("holdings", []):
                    if (
                        not isinstance(holding, dict)
                        or not has_unsafe_legacy_option_identity(holding)
                    ):
                        continue
                    unsafe_count += 1
                    if len(examples) < 5:
                        examples.append(
                            f"{cik}:{quarter.get('report_date')}:"
                            f"{holding.get('cusip')}"
                        )
        if unsafe_count:
            errors.append(
                f"{unsafe_count} unsafe legacy option identity row(s) remain "
                "after migration"
                + (
                    f" (examples: {', '.join(examples)})"
                    if examples
                    else ""
                )
            )
    return state


def expected_index_withheld_statuses(
    state: dict | None,
    fund_calendars: dict[str, dict],
) -> dict[str, str]:
    """Return CIK -> newest unresolved date that affects current display."""
    if not isinstance(state, dict):
        return {}
    newest: dict[str, str] = {}

    def add_target(target: object) -> None:
        if not isinstance(target, dict):
            return
        cik = canonical_cik(target.get("cik"))
        report_date = target.get("report_date")
        if cik is None or report_quarter_code(report_date) is None:
            return
        if report_date > newest.get(cik, ""):
            newest[cik] = report_date

    sources = [state.get("quarantined", {})]
    sources.extend(
        state.get(field, {})
        for field in (
            "amendment_migration_pending",
            "security_identity_migration_pending",
            "quarter_health_pending",
        )
    )
    for source in sources:
        if not isinstance(source, dict):
            continue
        for target in source.values():
            add_target(target)

    expected = {}
    for cik, report_date in newest.items():
        # A quarantined first-ever filing has no public fund snapshot yet.
        # Keep it queued for retry, but do not require a misleading empty
        # public fund row. Existing zero-quarter fund files are different:
        # they still need an explicit WITHHELD status.
        calendar = fund_calendars.get(cik)
        if calendar is None:
            continue
        published_dates = calendar.get("report_dates", ())
        if not published_dates or report_date >= published_dates[0]:
            expected[cik] = report_date
    return expected


def expected_index_unverified_report_dates(
    state: dict | None,
    fund_calendars: dict[str, dict],
) -> dict[str, list[str]]:
    """Return exact unresolved dates that still exist in each 4Q calendar."""
    if not isinstance(state, dict):
        return {}
    target_dates: dict[str, set[str]] = defaultdict(set)

    sources = [state.get("quarantined", {})]
    sources.extend(
        state.get(field, {})
        for field in (
            "amendment_migration_pending",
            "security_identity_migration_pending",
            "quarter_health_pending",
        )
    )
    for source in sources:
        if not isinstance(source, dict):
            continue
        for target in source.values():
            if not isinstance(target, dict):
                continue
            cik = canonical_cik(target.get("cik"))
            report_date = target.get("report_date")
            if cik is not None and report_quarter_code(report_date) is not None:
                target_dates[cik].add(report_date)

    expected: dict[str, list[str]] = {}
    for cik, dates in target_dates.items():
        calendar_dates = set(
            fund_calendars.get(cik, {}).get("report_dates", ())[:4]
        )
        retained = sorted(dates & calendar_dates, reverse=True)
        if retained:
            expected[cik] = retained
    return expected


def _normalized_registry_text(entry: dict, field: str) -> str:
    return " ".join(str(entry.get(field) or "").upper().split())


def _exact_registry_issuer_matches(entry: dict, pattern: re.Pattern) -> bool:
    return any(
        bool(pattern.fullmatch(_normalized_registry_text(entry, field)))
        for field in ("name", "dominant_issuer")
    )


def expected_filer_fund_kind(entry: dict) -> str | None:
    """Independently identify only deterministic filer-side fund evidence."""

    # An issuer name alone cannot override a retained debt/preferred/warrant
    # identity. The SEC cutover preserves those conflicting as-filed buckets
    # and withholds unsupported tickers. A published fund kind on such a bucket
    # is still rejected by the independent non-equity-fund check below.
    if normalize_instrument_type(entry.get("type")) not in {
        "EQUITY", "CALL", "PUT", "OPT"
    }:
        return None
    issuer_text = " ".join((
        _normalized_registry_text(entry, "name"),
        _normalized_registry_text(entry, "dominant_issuer"),
    ))
    dominant_class = _normalized_registry_text(entry, "dominant_class")
    if _ETN_KIND_RE.search(f"{issuer_text} {dominant_class}"):
        return None
    if _exact_registry_issuer_matches(entry, _EXCLUSIVE_ETF_ISSUER_RE):
        return "ETF"
    return None


def registry_entry_has_fund_evidence(entry: dict | None) -> bool:
    """Detect listed-fund evidence independently of the recorded type.

    This validator helper must remain type-agnostic so it can flag a fund that
    was incorrectly published as NOTE, PREF, or WARRANT.  Untyped ticker-only
    evidence still uses the shared fail-closed SEC proof predicate.
    """

    if not isinstance(entry, dict):
        return False
    if (
        normalize_security_kind(entry.get("security_kind"))
        in EQUITY_FUND_SECURITY_KINDS
    ):
        return True
    return registry_entry_has_trusted_fund_symbol_evidence(entry)


def validate_registry(
    fund_cusips: set[str],
    errors: list[str],
    registry: dict[str, dict] | None = None,
) -> dict[str, dict]:
    if registry is None:
        registry = load_json(CUSIP_REGISTRY_PATH, errors)
    if not isinstance(registry, dict):
        return {}

    validate_sec_mapping_provenance(registry, errors)
    validate_public_registry_provenance(registry, errors)

    missing = sorted(cusip for cusip in fund_cusips if cusip not in registry)
    if missing:
        errors.append(
            f"cusip_registry.json missing {len(missing)} fund CUSIPs; "
            f"samples: {', '.join(missing[:10])}"
        )

    non_note_bonds = sorted(
        cusip
        for cusip, entry in registry.items()
        if (
            normalize_security_kind(entry.get("security_kind")) == "BOND"
            and normalize_instrument_type(entry.get("type")) != "NOTE"
        )
    )
    if non_note_bonds:
        errors.append(
            "cusip_registry.json classifies "
            f"{len(non_note_bonds)} bonds as non-NOTE instruments; samples: "
            f"{', '.join(non_note_bonds[:10])}"
        )

    non_equity_funds = sorted(
        cusip
        for cusip, entry in registry.items()
        if (
            registry_entry_has_fund_evidence(entry)
            and normalize_instrument_type(entry.get("type"))
            not in {"EQUITY", "CALL", "PUT", "OPT"}
        )
    )
    if non_equity_funds:
        errors.append(
            "cusip_registry.json classifies "
            f"{len(non_equity_funds)} listed funds as non-EQUITY "
            "non-option instruments; samples: "
            f"{', '.join(non_equity_funds[:10])}"
        )

    filer_fund_kind_mismatches = sorted(
        f"{cusip}:{expected_kind}"
        for cusip, entry in registry.items()
        if (
            (expected_kind := expected_filer_fund_kind(entry)) is not None
            and normalize_security_kind(entry.get("security_kind"))
            != expected_kind
        )
    )
    if filer_fund_kind_mismatches:
        errors.append(
            "cusip_registry.json misses deterministic filer fund kinds for "
            f"{len(filer_fund_kind_mismatches)} entries; samples: "
            + ", ".join(filer_fund_kind_mismatches[:10])
        )

    return registry


def validate_security_labels(
    registry: dict[str, dict],
    errors: list[str],
) -> dict[str, str]:
    """Validate the compact browser label map against the public registry."""

    payload = load_json(SECURITY_LABELS_PATH, errors)
    if not isinstance(payload, dict):
        return {}
    validate_data_contract(payload, "security_labels.json", errors)
    labels = payload.get("labels")
    if not isinstance(labels, dict):
        errors.append("security_labels.json must contain an object-valued labels map")
        return {}
    kinds = payload.get("kinds")
    if not isinstance(kinds, dict):
        errors.append("security_labels.json must contain an object-valued kinds map")
        kinds = {}
    product_names = payload.get("product_names")
    if not isinstance(product_names, dict):
        errors.append(
            "security_labels.json must contain an object-valued "
            "product_names map"
        )
        product_names = {}
    fund_identities = payload.get("fund_identities")
    if not isinstance(fund_identities, list):
        errors.append(
            "security_labels.json must contain an array-valued "
            "fund_identities field"
        )
        fund_identities = []

    normalized_fund_identities = [
        normalize_security_identifier(identifier)
        for identifier in fund_identities
    ]
    expected_fund_identities = sorted(
        cusip
        for cusip, entry in registry.items()
        if registry_entry_has_fund_evidence(entry)
        # The browser marker identifies fund shares, not explicit options on
        # those funds. Keep the type-agnostic check for other instrument types
        # so an incorrectly published NOTE/PREF/WARRANT fund still fails.
        and normalize_instrument_type(entry.get("type"))
        not in {"CALL", "PUT", "OPT"}
    )
    if (
        any(
            type(raw) is not str
            or not normalized
            or raw != normalized
            for raw, normalized in zip(
                fund_identities,
                normalized_fund_identities,
            )
        )
        or normalized_fund_identities
        != sorted(set(normalized_fund_identities))
    ):
        errors.append(
            "security_labels.json fund_identities must be canonical, "
            "sorted, and unique"
        )
    if normalized_fund_identities != expected_fund_identities:
        errors.append(
            "security_labels.json fund_identities differ from the registry "
            f"(published {len(normalized_fund_identities)}, expected "
            f"{len(expected_fund_identities)})"
        )

    missing = sorted(set(registry) - set(labels))
    extra = sorted(set(labels) - set(registry))
    if missing:
        errors.append(
            f"security_labels.json missing {len(missing)} registry CUSIPs; "
            f"samples: {', '.join(missing[:10])}"
        )
    if extra:
        errors.append(
            f"security_labels.json has {len(extra)} CUSIPs absent from registry; "
            f"samples: {', '.join(extra[:10])}"
        )

    expected_kind_cusips = {
        cusip
        for cusip, entry in registry.items()
        if entry.get("security_kind") is not None
    }
    missing_kinds = sorted(expected_kind_cusips - set(kinds))
    extra_kinds = sorted(set(kinds) - set(registry))
    if missing_kinds:
        errors.append(
            f"security_labels.json missing {len(missing_kinds)} registry "
            f"security kinds; samples: {', '.join(missing_kinds[:10])}"
        )
    if extra_kinds:
        errors.append(
            f"security_labels.json has {len(extra_kinds)} security kinds "
            f"absent from registry; samples: {', '.join(extra_kinds[:10])}"
        )
    expected_product_name_cusips = {
        cusip
        for cusip, entry in registry.items()
        if entry.get("product_name") is not None
    }
    missing_product_names = sorted(
        expected_product_name_cusips - set(product_names)
    )
    extra_product_names = sorted(
        set(product_names) - expected_product_name_cusips
    )
    if missing_product_names:
        errors.append(
            "security_labels.json missing "
            f"{len(missing_product_names)} registry product names; samples: "
            f"{', '.join(missing_product_names[:10])}"
        )
    if extra_product_names:
        errors.append(
            "security_labels.json has "
            f"{len(extra_product_names)} product names absent from registry; "
            f"samples: {', '.join(extra_product_names[:10])}"
        )

    mismatched: list[str] = []
    malformed: list[str] = []
    missing_sources: list[str] = []
    missing_classes: list[str] = []
    raw_identifier_names: list[str] = []
    for cusip, label in labels.items():
        if normalize_security_label(label, identifier=cusip) != label:
            malformed.append(cusip)
        registry_entry = registry.get(cusip) or {}
        registry_label = registry_entry.get("security_label")
        if registry_label != label:
            mismatched.append(cusip)
        if not str(registry_entry.get("label_source") or "").strip():
            missing_sources.append(cusip)
        if "dominant_class" not in registry_entry:
            missing_classes.append(cusip)
        if (
            str(registry_entry.get("name") or "").strip().upper()
            == cusip.upper()
        ):
            raw_identifier_names.append(cusip)
    if malformed:
        errors.append(
            "security_labels.json has "
            f"{len(malformed)} blank, raw-CUSIP, or non-canonical labels; "
            f"samples: {', '.join(sorted(malformed)[:10])}"
        )
    if mismatched:
        errors.append(
            "security_labels.json differs from cusip_registry.json for "
            f"{len(mismatched)} CUSIPs; samples: "
            f"{', '.join(sorted(mismatched)[:10])}"
        )
    if missing_sources:
        errors.append(
            "cusip_registry.json has "
            f"{len(missing_sources)} security labels without provenance; "
            f"samples: {', '.join(sorted(missing_sources)[:10])}"
        )
    if missing_classes:
        errors.append(
            "cusip_registry.json has "
            f"{len(missing_classes)} entries without dominant_class; "
            f"samples: {', '.join(sorted(missing_classes)[:10])}"
        )
    if raw_identifier_names:
        errors.append(
            "cusip_registry.json uses raw identifiers as issuer names for "
            f"{len(raw_identifier_names)} CUSIPs; samples: "
            f"{', '.join(sorted(raw_identifier_names)[:10])}"
        )

    malformed_kinds: list[str] = []
    mismatched_kinds: list[str] = []
    bad_kind_sources: list[str] = []
    common_depositary_receipts: list[str] = []
    for cusip, kind in kinds.items():
        if normalize_security_kind(kind) != kind:
            malformed_kinds.append(cusip)
        registry_entry = registry.get(cusip) or {}
        if registry_entry.get("security_kind") != kind:
            mismatched_kinds.append(cusip)
        source = str(
            registry_entry.get("security_kind_source") or ""
        ).strip()
        if not (
            source in SEC_METADATA_SOURCES
            or source == "filer_metadata"
        ):
            bad_kind_sources.append(cusip)
        filer_text = " ".join(
            str(registry_entry.get(field) or "").strip()
            for field in ("name", "dominant_issuer", "dominant_class")
        )
        if kind == "COMMON" and _DEPOSITARY_RECEIPT_RE.search(filer_text):
            common_depositary_receipts.append(cusip)
    dangling_kind_sources = sorted(
        cusip
        for cusip, entry in registry.items()
        if entry.get("security_kind_source") and not entry.get("security_kind")
    )
    if malformed_kinds:
        errors.append(
            "security_labels.json has "
            f"{len(malformed_kinds)} invalid security kinds; samples: "
            f"{', '.join(sorted(malformed_kinds)[:10])}"
        )
    if mismatched_kinds:
        errors.append(
            "security_labels.json kinds differ from cusip_registry.json for "
            f"{len(mismatched_kinds)} CUSIPs; samples: "
            f"{', '.join(sorted(mismatched_kinds)[:10])}"
        )
    if bad_kind_sources or dangling_kind_sources:
        affected = sorted(set(bad_kind_sources) | set(dangling_kind_sources))
        errors.append(
            "cusip_registry.json has "
            f"{len(affected)} security kinds with invalid provenance; "
            f"samples: {', '.join(affected[:10])}"
        )
    if common_depositary_receipts:
        errors.append(
            "cusip_registry.json classifies "
            f"{len(common_depositary_receipts)} depositary receipts as "
            "COMMON; samples: "
            f"{', '.join(sorted(common_depositary_receipts)[:10])}"
        )

    malformed_product_names: list[str] = []
    mismatched_product_names: list[str] = []
    bad_product_name_sources: list[str] = []
    invalid_product_name_kinds: list[str] = []
    product_name_symbols: dict[str, set[str]] = defaultdict(set)
    for cusip, product_name in product_names.items():
        if normalize_security_label(product_name, identifier=cusip) != product_name:
            malformed_product_names.append(cusip)
        registry_entry = registry.get(cusip) or {}
        if registry_entry.get("product_name") != product_name:
            mismatched_product_names.append(cusip)
        source = str(
            registry_entry.get("product_name_source") or ""
        ).strip()
        provenance_source = (
            source[:-7] if source.endswith("_ticker") else source
        )
        entry_kind = normalize_security_kind(
            registry_entry.get("security_kind")
        )
        entry_sources = set(registry_entry.get("sources") or [])
        canonical_name = normalize_security_label(
            registry_entry.get("name"),
            identifier=cusip,
        )
        dominant_issuer = normalize_security_label(
            registry_entry.get("dominant_issuer"),
            identifier=cusip,
        )
        valid_direct_etn_source = (
            entry_kind == "ETN"
            and (
                (
                    provenance_source == "sec_title"
                    and "sec_title" in entry_sources
                    and product_name == canonical_name
                )
                or (
                    provenance_source == "filer_issuer"
                    and "filer_dominant" in entry_sources
                    and product_name in {
                        canonical_name,
                        dominant_issuer,
                    }
                )
            )
        )
        if not (
            provenance_source in SEC_TICKER_SOURCES
            or provenance_source in {
                "filer_class",
                "filer_issuer_class",
                "sec_fund_series",
                "sec_title_class",
            }
            or valid_direct_etn_source
        ):
            bad_product_name_sources.append(cusip)
        if entry_kind not in _FUND_PRODUCT_NAME_KINDS:
            invalid_product_name_kinds.append(cusip)
        symbol = str(registry_entry.get("ticker") or "").strip().upper()
        if symbol:
            product_name_symbols[str(product_name).casefold()].add(symbol)
    dangling_product_name_sources = sorted(
        cusip
        for cusip, entry in registry.items()
        if entry.get("product_name_source") and not entry.get("product_name")
    )
    if malformed_product_names:
        errors.append(
            "security_labels.json has "
            f"{len(malformed_product_names)} invalid product names; samples: "
            f"{', '.join(sorted(malformed_product_names)[:10])}"
        )
    if mismatched_product_names:
        errors.append(
            "security_labels.json product names differ from "
            "cusip_registry.json for "
            f"{len(mismatched_product_names)} CUSIPs; samples: "
            f"{', '.join(sorted(mismatched_product_names)[:10])}"
        )
    ambiguous_product_names = sorted(
        product_name
        for product_name, symbols in product_name_symbols.items()
        if len(symbols) > 1
    )
    if ambiguous_product_names:
        errors.append(
            "security_labels.json has "
            f"{len(ambiguous_product_names)} ambiguous fund product names "
            "shared by different listed symbols; samples: "
            f"{', '.join(ambiguous_product_names[:10])}"
        )
    if (
        bad_product_name_sources
        or invalid_product_name_kinds
        or dangling_product_name_sources
    ):
        affected = sorted(
            set(bad_product_name_sources)
            | set(invalid_product_name_kinds)
            | set(dangling_product_name_sources)
        )
        errors.append(
            "cusip_registry.json has "
            f"{len(affected)} product names with invalid kind or provenance; "
            f"samples: {', '.join(affected[:10])}"
        )
    return labels


def _current_stats_mismatches(
    actual: dict[str, int | float | None],
    expected: dict[str, int | float | None],
) -> list[str]:
    mismatches = []
    for field in (
        "holder_count",
        "total_value",
        "total_shares",
        "largest_value",
    ):
        if not _numbers_match(actual.get(field), expected.get(field)):
            mismatches.append(
                f"{field}={actual.get(field)!r} expected {expected.get(field)!r}"
            )
    if actual.get("position_digest") != expected.get("position_digest"):
        mismatches.append("current holder membership or position values differ")
    if actual.get("transition_count") != expected.get("transition_count"):
        mismatches.append(
            f"latest-two observation count={actual.get('transition_count')!r} "
            f"expected {expected.get('transition_count')!r}"
        )
    if actual.get("transition_digest") != expected.get("transition_digest"):
        mismatches.append("latest-two observations differ")
    for field in (
        "history_count",
        "history_total_value",
        "history_total_shares",
    ):
        if not _numbers_match(actual.get(field), expected.get(field)):
            mismatches.append(
                f"{field}={actual.get(field)!r} expected {expected.get(field)!r}"
            )
    if actual.get("history_digest") != expected.get("history_digest"):
        mismatches.append("retained-quarter observations differ")
    return mismatches


def validate_stocks(
    errors: list[str],
    fund_calendars: dict[str, dict] | None = None,
    expected_current_stats: dict[
        str, dict[str, int | float | None]
    ] | None = None,
    expected_split_adjustments: dict[str, list[dict]] | None = None,
    *,
    registry: dict[str, dict] | None = None,
    paths: list[Path] | None = None,
) -> dict[str, Path]:
    validate_calendars = fund_calendars is not None
    reconcile_current = (
        validate_calendars and expected_current_stats is not None
    )
    fund_calendars = fund_calendars or {}
    expected_current_stats = expected_current_stats or {}
    if expected_split_adjustments is not None:
        expected_split_adjustments.clear()
    stock_files: dict[str, Path] = {}
    seen_stock_ids: set[str] = set()
    bad_fund_non_option_artifacts: list[str] = []
    fund_non_option_types: dict[str, set[str]] = defaultdict(set)
    for fp in sorted(STOCKS_DIR.glob("*.json")) if paths is None else sorted(paths):
        stock_files[fp.stem] = fp
        stock = load_json(fp, errors)
        if not isinstance(stock, dict):
            continue

        raw_stock_id = stock.get("stock_id")
        stock_id = normalize_security_identifier(raw_stock_id)
        if not stock_id:
            errors.append(f"stock file {fp.name} missing stock_id")
        else:
            if type(raw_stock_id) is not str or raw_stock_id != stock_id:
                errors.append(
                    f"stock file {fp.name} has non-canonical stock_id "
                    f"{raw_stock_id!r}"
                )
            if stock_id in seen_stock_ids:
                errors.append(
                    f"multiple stock files publish duplicate stock_id {stock_id}"
                )
            else:
                seen_stock_ids.add(stock_id)

        raw_cusip = stock.get("cusip")
        cusip = normalize_security_identifier(raw_cusip)
        if not is_canonical_security_identifier(raw_cusip):
            errors.append(
                f"stock file {fp.name} has invalid canonical cusip "
                f"{raw_cusip!r}"
            )

        raw_instrument_type = stock.get("instrument_type")
        instrument_type = normalize_instrument_type(raw_instrument_type)
        if (
            type(raw_instrument_type) is not str
            or raw_instrument_type not in VALID_INSTRUMENT_TYPES
        ):
            errors.append(
                f"stock file {fp.name} has invalid instrument_type "
                f"{raw_instrument_type!r}"
            )

        registry_entry = (registry or {}).get(cusip) or {}
        if (
            registry_entry_has_fund_evidence(registry_entry)
            and instrument_type not in {"CALL", "PUT", "OPT"}
        ):
            # A CUSIP-wide fund label cannot rewrite an exact retained
            # NOTE/PREF/WARRANT identity. Such an artifact is permitted only
            # when the independent fund projection contains its typed history;
            # the full history reconciliation below must still match exactly.
            # Missing, invented, merged, or mistickered identities still fail.
            retained_typed_history = (
                reconcile_current
                and instrument_type != "EQUITY"
                and expected_current_stats.get(stock_id, {}).get("history_count", 0)
            )
            if not retained_typed_history:
                fund_non_option_types[cusip].add(instrument_type)
                if instrument_type != "EQUITY":
                    bad_fund_non_option_artifacts.append(
                        f"{cusip}|{instrument_type}"
                    )

        raw_ticker = stock.get("ticker")
        normalized_note_label = normalize_note_security_label(raw_ticker)
        if instrument_type == "NOTE" and raw_ticker:
            if (
                normalized_note_label != raw_ticker
                and normalize_security_identifier(raw_ticker) != cusip
            ):
                errors.append(
                    f"stock file {fp.name} has non-canonical NOTE ticker "
                    f"label {raw_ticker!r}"
                )
        elif normalized_note_label:
            errors.append(
                f"stock file {fp.name} publishes NOTE label "
                f"{raw_ticker!r} on instrument_type {instrument_type}"
            )

        if registry is not None and registry_entry:
            expected_public_ticker = (
                expected_registry_position_ticker(
                    registry_entry,
                    instrument_type,
                )
                or cusip
            )
            if stock.get("ticker") != expected_public_ticker:
                errors.append(
                    f"stock file {fp.name} ticker {stock.get('ticker')!r} "
                    "does not match its exact SEC registry display ticker "
                    f"{expected_public_ticker!r}"
                )
            if stock.get("issuer") != registry_entry.get("name"):
                errors.append(
                    f"stock file {fp.name} issuer {stock.get('issuer')!r} "
                    "does not match its canonical SEC registry issuer "
                    f"{registry_entry.get('name')!r}"
                )

        expected_stock_id = stock_lookup_id(cusip, instrument_type)
        if stock_id and stock_id != expected_stock_id:
            errors.append(
                f"stock file {fp.name} has stock_id {stock_id} but exact "
                f"cusip/type identity is {expected_stock_id or '<missing>'}"
            )
        expected_filename = stock_filename(cusip, instrument_type)
        if fp.name != expected_filename:
            errors.append(
                f"stock file {fp.name} does not match exact cusip/type "
                f"filename {expected_filename or '<missing>'}"
            )

        holders = stock.get("holders", [])
        if not isinstance(holders, list):
            errors.append(f"stock file {fp.name} has non-list holders")
            continue

        emitted_split_adjustments = stock.get("split_adjustments")
        if instrument_type != "EQUITY":
            if "split_adjustments" in stock:
                errors.append(
                    f"stock file {fp.name} publishes split proof for "
                    f"non-equity type {instrument_type}"
                )
        else:
            recomputed_split_adjustments = infer_proven_split_adjustments(
                holders
            )
            if emitted_split_adjustments is None:
                emitted_split_adjustments = []
            if (
                not isinstance(emitted_split_adjustments, list)
                or emitted_split_adjustments != recomputed_split_adjustments
            ):
                errors.append(
                    f"stock file {fp.name} split_adjustments do not match "
                    "independently recomputed holder-history proof"
                )
            if (
                expected_split_adjustments is not None
                and expected_stock_id
                and recomputed_split_adjustments
            ):
                expected_split_adjustments[expected_stock_id] = (
                    recomputed_split_adjustments
                )

        actual_current_stats = _empty_current_stats()
        seen_ciks: set[str] = set()
        for h_idx, holder in enumerate(holders):
            if not isinstance(holder, dict):
                errors.append(f"stock file {fp.name} holder {h_idx} is not an object")
                continue

            raw_cik = holder.get("cik")
            cik = canonical_cik(raw_cik)
            if cik is None:
                errors.append(
                    f"stock file {fp.name} holder {h_idx} has invalid cik "
                    f"{raw_cik!r}; expected a positive integer"
                )
            elif cik in seen_ciks:
                errors.append(
                    f"stock file {fp.name} has duplicate holder cik {cik}"
                )
            if cik is not None:
                seen_ciks.add(cik)
            calendar = fund_calendars.get(cik) if cik is not None else None
            if validate_calendars and cik is not None and calendar is None:
                errors.append(
                    f"stock file {fp.name} holder {h_idx} references unknown fund {cik}"
                )
            expected_name = (
                calendar.get("name")
                if isinstance(calendar, dict)
                else None
            )
            if expected_name is not None and holder.get("name") != expected_name:
                errors.append(
                    f"stock file {fp.name} holder {h_idx} name "
                    f"{holder.get('name')!r} does not match fund {cik} "
                    f"name {expected_name!r}"
                )
            transition_dates = (
                set(calendar["report_dates"][:2])
                if calendar is not None
                else set()
            )

            history = holder.get("history", [])
            if not isinstance(history, list):
                errors.append(f"stock file {fp.name} holder {h_idx} has non-list history")
                continue

            dates = [
                entry.get("date")
                for entry in history
                if isinstance(entry, dict)
            ]
            valid_dates = [value for value in dates if isinstance(value, str)]
            if dates != sorted(valid_dates, reverse=True):
                errors.append(f"stock file {fp.name} holder {h_idx} has unsorted history")
            if len(valid_dates) != len(set(valid_dates)):
                errors.append(f"stock file {fp.name} holder {h_idx} has duplicate dates")

            for e_idx, entry in enumerate(history):
                if not isinstance(entry, dict):
                    errors.append(
                        f"stock file {fp.name} holder {h_idx} history {e_idx} is not an object"
                    )
                    continue
                if "pct_of_fund" not in entry:
                    errors.append(
                        f"stock file {fp.name} holder {h_idx} history {e_idx} missing pct_of_fund"
                    )
                report_date = entry.get("date")
                date_is_retained = False
                if not isinstance(report_date, str):
                    errors.append(
                        f"stock file {fp.name} holder {h_idx} history {e_idx} "
                        "has invalid date"
                    )
                elif calendar is not None and report_date not in calendar[
                    "report_date_set"
                ]:
                    errors.append(
                        f"stock file {fp.name} holder {h_idx} history date "
                        f"{report_date} is not retained by fund {cik}"
                    )
                elif calendar is not None:
                    date_is_retained = report_date in calendar["report_date_set"]

                numeric_fields = ("value", "shares", "pct_of_fund")
                position_is_numeric = all(
                    is_finite_number(entry.get(field))
                    for field in numeric_fields
                )
                if not position_is_numeric:
                    errors.append(
                        f"stock file {fp.name} holder {h_idx} history "
                        f"{e_idx} has non-numeric position data"
                    )

                if "quantity_unknown" in entry and entry.get("quantity_unknown") is not True:
                    errors.append(f"stock file {fp.name} has invalid quantity_unknown marker")
                if (
                    "shares_imputed" in entry
                    and entry.get("shares_imputed") is not True
                ):
                    errors.append(
                        f"stock file {fp.name} holder {h_idx} history "
                        f"{e_idx} has invalid shares_imputed marker"
                    )

                if position_is_numeric and date_is_retained:
                    _add_history_observation(
                        actual_current_stats,
                        cik=cik,
                        report_date=report_date,
                        value=entry["value"],
                        shares=entry["shares"],
                        pct_of_fund=entry["pct_of_fund"],
                        shares_imputed=bool(entry.get("shares_imputed")),
                        quantity_unknown=bool(entry.get("quantity_unknown")),
                    )
                    if report_date in transition_dates:
                        _add_transition_observation(
                            actual_current_stats,
                            cik=cik,
                            report_date=report_date,
                            value=entry["value"],
                            shares=entry["shares"],
                            pct_of_fund=entry["pct_of_fund"],
                        )

            if calendar is None:
                continue
            _status, current, _previous = classify_sparse_history(
                history,
                calendar["report_dates"],
            )
            if current is None:
                continue
            numeric_fields = ("value", "shares", "pct_of_fund")
            if any(
                not is_finite_number(current.get(field))
                for field in numeric_fields
            ):
                continue
            _add_current_position(
                actual_current_stats,
                cik=cik,
                value=current["value"],
                shares=current["shares"],
                pct_of_fund=current["pct_of_fund"],
            )

        if reconcile_current and stock_id:
            expected = expected_current_stats.get(stock_id)
            if expected is None or not expected.get("history_count"):
                errors.append(
                    f"stock file {fp.name} has no retained fund position "
                    f"for stock_id {stock_id}"
                )
                continue
            mismatches = _current_stats_mismatches(
                actual_current_stats,
                expected,
            )
            if mismatches:
                errors.append(
                    f"stock file {fp.name} histories do not reconcile to "
                    f"retained fund quarters: {'; '.join(mismatches)}"
                )

    if reconcile_current:
        missing_current_stocks = sorted(
            stock_id
            for stock_id, stats in expected_current_stats.items()
            if (
                stats.get("holder_count")
                or stats.get("transition_count")
                or stats.get("history_count")
            )
            and stock_id not in seen_stock_ids
        )
        if missing_current_stocks:
            errors.append(
                f"{len(missing_current_stocks)} retained stock identities have "
                "no generated stock file; samples: "
                + ", ".join(missing_current_stocks[:10])
            )
    if bad_fund_non_option_artifacts:
        errors.append(
            f"{len(bad_fund_non_option_artifacts)} generated listed-fund "
            "stock artifacts use a non-EQUITY non-option type; samples: "
            + ", ".join(sorted(bad_fund_non_option_artifacts)[:10])
        )
    fragmented_fund_cusips = sorted(
        cusip
        for cusip, instrument_types in fund_non_option_types.items()
        if instrument_types != {"EQUITY"}
    )
    if fragmented_fund_cusips:
        errors.append(
            f"{len(fragmented_fund_cusips)} listed-fund CUSIPs do not have "
            "exactly one canonical EQUITY non-option artifact; samples: "
            + ", ".join(fragmented_fund_cusips[:10])
        )
    return stock_files


def _index_modal_latest_reporting_quarter(
    funds: list[dict],
) -> int | None:
    """Independently reproduce the frontend's modal current baseline."""
    counts: Counter[int] = Counter()
    for fund in funds:
        if not isinstance(fund, dict) or fund.get("status") == "WITHHELD":
            continue
        calendar = fund.get("q")
        if not isinstance(calendar, list) or not calendar:
            continue
        latest = calendar[0]
        if (
            type(latest) is int
            and latest // 10 in range(1000, 10_000)
            and latest % 10 in {1, 2, 3, 4}
        ):
            counts[latest] += 1
    if not counts:
        return None
    return max(counts, key=lambda quarter: (counts[quarter], quarter))


def _index_current_fund_quarters(
    funds: list[dict],
) -> dict[str, int]:
    baseline = _index_modal_latest_reporting_quarter(funds)
    if baseline is None:
        return {}
    current: dict[str, int] = {}
    for fund in funds:
        if not isinstance(fund, dict) or fund.get("status") == "WITHHELD":
            continue
        cik = canonical_cik(fund.get("cik"))
        calendar = fund.get("q")
        if (
            cik is None
            or not isinstance(calendar, list)
            or not calendar
            or type(calendar[0]) is not int
            or calendar[0] < baseline
        ):
            continue
        current[cik] = calendar[0]
    return current


def _stock_index_metadata(
    path: Path,
    current_fund_quarters: dict[str, int],
    errors: list[str],
) -> dict | None:
    """Recompute searchable metadata from the referenced stock payload."""
    stock = load_json(path, errors)
    if not isinstance(stock, dict):
        return None
    holders = stock.get("holders")
    if not isinstance(holders, list):
        return None

    last_seen = ""
    current_holder_count = 0
    for holder in holders:
        if not isinstance(holder, dict):
            continue
        cik = canonical_cik(holder.get("cik"))
        current_quarter = current_fund_quarters.get(cik or "")
        history = holder.get("history")
        if not isinstance(history, list):
            continue
        holder_is_current = False
        for record in history:
            if not isinstance(record, dict):
                continue
            report_date = record.get("date")
            if report_quarter_code(report_date) is None:
                continue
            last_seen = max(last_seen, report_date)
            if report_quarter_code(report_date) == current_quarter:
                holder_is_current = True
        if holder_is_current:
            current_holder_count += 1

    return {
        "stock_id": stock.get("stock_id"),
        "cusip": stock.get("cusip"),
        "ticker": stock.get("ticker"),
        "issuer": stock.get("issuer"),
        "instrument_type": stock.get("instrument_type"),
        "holder_count": len(holders),
        "current_holder_count": current_holder_count,
        "last_seen": last_seen,
    }


def validate_index(
    index: dict,
    fund_files: dict[str, Path],
    stock_files: dict[str, Path],
    registry: dict[str, dict],
    errors: list[str],
    warnings: list[str],
    fund_calendars: dict[str, dict] | None = None,
    pipeline_state: dict | None = None,
) -> None:
    fund_calendars = fund_calendars or {}
    expected_withheld = expected_index_withheld_statuses(
        pipeline_state,
        fund_calendars,
    )
    expected_unverified = expected_index_unverified_report_dates(
        pipeline_state,
        fund_calendars,
    )
    funds = index.get("funds", [])
    tickers = index.get("tickers", [])

    if not isinstance(funds, list):
        errors.append("index.json has non-list funds")
        funds = []
    if not isinstance(tickers, list):
        errors.append("index.json has non-list tickers")
        tickers = []
    current_fund_quarters = _index_current_fund_quarters(funds)

    seen_ciks: set[str] = set()
    for entry in funds:
        if not isinstance(entry, dict):
            errors.append("index.json contains non-object fund entry")
            continue
        raw_cik = entry.get("cik")
        cik = canonical_cik(raw_cik)
        calendar = None
        if cik is None:
            errors.append(
                f"index.json fund entry has invalid cik {raw_cik!r}; "
                "expected a positive integer"
            )
        else:
            if cik in seen_ciks:
                errors.append(f"index.json contains duplicate fund cik {cik}")
                continue
            seen_ciks.add(cik)
            if cik not in fund_files:
                errors.append(
                    f"index.json references missing fund file for cik {cik}"
                )
            calendar = fund_calendars.get(cik)
            expected_name = (
                calendar.get("name")
                if isinstance(calendar, dict)
                else None
            )
            if expected_name is not None and entry.get("name") != expected_name:
                errors.append(
                    f"index.json fund cik {cik} name {entry.get('name')!r} "
                    f"does not match fund file {expected_name!r}"
                )
            expected_withheld_date = expected_withheld.get(cik)
            expected_unverified_dates = expected_unverified.get(cik, [])
            if expected_withheld_date is not None:
                if entry.get("status") != "WITHHELD":
                    errors.append(
                        f"index.json fund cik {cik} must publish "
                        "status WITHHELD"
                    )
                if (
                    entry.get("latest_withheld_report_date")
                    != expected_withheld_date
                ):
                    errors.append(
                        f"index.json fund cik {cik} withheld date "
                        f"{entry.get('latest_withheld_report_date')!r} does "
                        f"not match active target {expected_withheld_date}"
                    )
                if (
                    not isinstance(entry.get("withheld_reason"), str)
                    or not entry["withheld_reason"].strip()
                ):
                    errors.append(
                        f"index.json fund cik {cik} has no withheld reason"
                    )
            elif any(
                field in entry
                for field in (
                    "status",
                    "latest_withheld_report_date",
                    "withheld_reason",
                )
            ):
                errors.append(
                    f"index.json fund cik {cik} has stale withheld metadata"
                )
            actual_unverified_dates = entry.get(
                "unverified_report_dates"
            )
            if expected_unverified_dates:
                if actual_unverified_dates != expected_unverified_dates:
                    errors.append(
                        f"index.json fund cik {cik} unverified_report_dates="
                        f"{actual_unverified_dates!r} does not match active "
                        f"calendar targets {expected_unverified_dates!r}"
                    )
            elif "unverified_report_dates" in entry:
                errors.append(
                    f"index.json fund cik {cik} has stale "
                    "unverified_report_dates metadata"
                )
        q = entry.get("q")
        if not isinstance(q, list):
            errors.append(
                f"index.json fund cik {cik} has non-list report calendar q"
            )
        else:
            if len(q) > 4:
                errors.append(
                    f"index.json fund cik {cik} report calendar q exceeds four quarters"
                )
            q_is_valid = not any(
                type(code) is not int
                or code // 10 < 1000
                or code // 10 > 9999
                or code % 10 not in {1, 2, 3, 4}
                for code in q
            )
            if not q_is_valid:
                errors.append(
                    f"index.json fund cik {cik} has invalid YYYYQ code in q"
                )
            if q_is_valid and q != sorted(set(q), reverse=True):
                errors.append(
                    f"index.json fund cik {cik} report calendar q is not "
                    "newest-first and de-duplicated"
                )
            if calendar is not None and q != list(calendar["q"]):
                errors.append(
                    f"index.json fund cik {cik} report calendar q={q!r} "
                    f"does not match persisted fund quarters "
                    f"{list(calendar['q'])!r}"
                )

    unindexed_funds = sorted(set(fund_files) - seen_ciks)
    if unindexed_funds:
        errors.append(
            f"index.json omits {len(unindexed_funds)} fund files; samples: "
            + ", ".join(unindexed_funds[:10])
        )
    missing_withheld_status = sorted(
        set(expected_withheld) - seen_ciks
    )
    if missing_withheld_status:
        errors.append(
            f"index.json omits {len(missing_withheld_status)} active "
            "withheld filer status row(s); samples: "
            + ", ".join(missing_withheld_status[:10])
        )

    safe_name_collisions: dict[str, list[str]] = {}
    seen_stock_ids: set[str] = set()
    indexed_entries_by_stock_id: dict[str, dict] = {}
    bad_fund_index_rows: list[str] = []
    for entry in tickers:
        if isinstance(entry, str):
            errors.append(
                "index.json contains legacy string ticker entry; "
                "exact stock identity fields are required"
            )
            continue
        elif isinstance(entry, dict):
            ticker = str(entry.get("ticker") or "").strip()
            raw_cusip = entry.get("cusip")
            cusip = normalize_security_identifier(raw_cusip)
            if not is_canonical_security_identifier(raw_cusip):
                errors.append(
                    f"index.json ticker entry has invalid canonical cusip "
                    f"{raw_cusip!r}"
                )

            raw_instrument_type = entry.get("instrument_type")
            instrument_type = normalize_instrument_type(raw_instrument_type)
            if (
                type(raw_instrument_type) is not str
                or raw_instrument_type not in VALID_INSTRUMENT_TYPES
            ):
                errors.append(
                    f"index.json ticker entry for {cusip or '<missing>'} has "
                    f"invalid instrument_type {raw_instrument_type!r}"
                )
            holder_count = entry.get("holder_count")
            if type(holder_count) is not int or holder_count < 0:
                errors.append(
                    f"index.json ticker entry for {cusip or '<missing>'} has "
                    f"invalid holder_count {holder_count!r}"
                )
            current_holder_count = entry.get("current_holder_count")
            if (
                type(current_holder_count) is not int
                or current_holder_count < 0
                or (
                    type(holder_count) is int
                    and current_holder_count > holder_count
                )
            ):
                errors.append(
                    f"index.json ticker entry for {cusip or '<missing>'} has "
                    "invalid current_holder_count "
                    f"{current_holder_count!r}"
                )
            last_seen = entry.get("last_seen")
            if (
                type(last_seen) is not str
                or report_quarter_code(last_seen) is None
            ):
                errors.append(
                    f"index.json ticker entry for {cusip or '<missing>'} has "
                    f"invalid last_seen {last_seen!r}"
                )

            normalized_note_label = normalize_note_security_label(ticker)
            if instrument_type == "NOTE" and ticker:
                if normalized_note_label != ticker:
                    errors.append(
                        f"index.json ticker entry for "
                        f"{cusip or '<missing>'} has non-canonical NOTE "
                        f"label {ticker!r}"
                    )
            elif normalized_note_label:
                errors.append(
                    f"index.json ticker entry for {cusip or '<missing>'} "
                    f"publishes NOTE label {ticker!r} on instrument_type "
                    f"{instrument_type}"
                )

            raw_lookup_id = entry.get("stock_id")
            lookup_id = normalize_security_identifier(raw_lookup_id)
            expected_lookup_id = stock_lookup_id(cusip, instrument_type)
            if (
                type(raw_lookup_id) is not str
                or not lookup_id
                or raw_lookup_id != lookup_id
            ):
                errors.append(
                    f"index.json ticker entry for {cusip or '<missing>'} has "
                    f"non-canonical stock_id {raw_lookup_id!r}"
                )
            if lookup_id != expected_lookup_id:
                errors.append(
                    f"index.json ticker entry {lookup_id or '<missing>'} does "
                    f"not equal exact cusip/type identity "
                    f"{expected_lookup_id or '<missing>'}"
                )
            if not ticker:
                errors.append(f"index.json ticker entry {lookup_id or '<missing>'} has blank ticker")
            elif cusip and ticker.upper() == cusip:
                errors.append(
                    f"index.json should not expose raw identifier {cusip} as a searchable ticker"
                )
        else:
            errors.append("index.json contains non-object ticker entry")
            continue
        if not lookup_id:
            errors.append("index.json contains ticker entry without a stock_id")
            continue
        if lookup_id in seen_stock_ids:
            errors.append(f"index.json contains duplicate stock_id {lookup_id}")
            continue
        seen_stock_ids.add(lookup_id)
        indexed_entries_by_stock_id[lookup_id] = entry
        registry_entry = registry.get(cusip) or {}
        if registry_entry:
            expected_search_ticker = expected_registry_position_ticker(
                registry_entry,
                instrument_type,
            )
            if entry.get("ticker") != expected_search_ticker:
                errors.append(
                    f"index.json ticker entry {lookup_id} publishes "
                    f"ticker {entry.get('ticker')!r}, expected exact SEC "
                    f"registry ticker {expected_search_ticker!r}"
                )
            if entry.get("issuer") != registry_entry.get("name"):
                errors.append(
                    f"index.json ticker entry {lookup_id} publishes issuer "
                    f"{entry.get('issuer')!r}, expected canonical SEC "
                    f"registry issuer {registry_entry.get('name')!r}"
                )
        if (
            registry_entry_has_fund_evidence(registry_entry)
            and instrument_type not in {"EQUITY", "CALL", "PUT", "OPT"}
        ):
            bad_fund_index_rows.append(lookup_id)

        safe_name = stock_file_stem(lookup_id)
        safe_name_collisions.setdefault(safe_name, []).append(lookup_id)
        if safe_name not in stock_files:
            errors.append(f"index.json references missing stock file for {lookup_id}")
        else:
            expected_filename = stock_filename(cusip, instrument_type)
            if stock_files[safe_name].name != expected_filename:
                errors.append(
                    f"index.json ticker entry {lookup_id} resolves to "
                    f"{stock_files[safe_name].name}, expected {expected_filename}"
                )
            recomputed = _stock_index_metadata(
                stock_files[safe_name],
                current_fund_quarters,
                errors,
            )
            if recomputed is not None:
                for field in (
                    "stock_id",
                    "cusip",
                    "ticker",
                    "issuer",
                    "instrument_type",
                    "holder_count",
                    "current_holder_count",
                    "last_seen",
                ):
                    if entry.get(field) != recomputed.get(field):
                        errors.append(
                            f"index.json ticker entry {lookup_id} has "
                            f"{field}={entry.get(field)!r}, expected "
                            f"{recomputed.get(field)!r} from "
                            f"{stock_files[safe_name].name}"
                        )

    for safe_name, colliding_tickers in safe_name_collisions.items():
        if len(colliding_tickers) > 1:
            joined = ", ".join(sorted(colliding_tickers))
            errors.append(
                f"multiple stock identities map to {safe_name}.json: {joined}"
            )

    if bad_fund_index_rows:
        errors.append(
            f"index.json has {len(bad_fund_index_rows)} listed-fund rows "
            "using a non-EQUITY non-option identity; samples: "
            + ", ".join(sorted(bad_fund_index_rows)[:10])
        )
    missing_fund_search_rows = sorted(
        cusip
        for cusip, entry in registry.items()
        if (
            registry_entry_has_fund_evidence(entry)
            and str(entry.get("ticker") or "").strip()
            and stock_file_stem(cusip) in stock_files
            and (
                not isinstance(
                    indexed_entries_by_stock_id.get(cusip),
                    dict,
                )
                or indexed_entries_by_stock_id[cusip].get("ticker")
                != entry.get("ticker")
                or indexed_entries_by_stock_id[cusip].get(
                    "instrument_type"
                ) != "EQUITY"
            )
        )
    )
    if missing_fund_search_rows:
        errors.append(
            f"index.json omits or mismatches "
            f"{len(missing_fund_search_rows)} canonical ticker-backed "
            "listed-fund EQUITY rows; samples: "
            + ", ".join(missing_fund_search_rows[:10])
        )

    total_filers = index.get("total_filers")
    total_tickers = index.get("total_tickers")
    if total_filers != len(funds):
        errors.append(
            f"index.json total_filers={total_filers} does not match funds length {len(funds)}"
        )
    if total_tickers != len(tickers):
        errors.append(
            f"index.json total_tickers={total_tickers} does not match tickers length {len(tickers)}"
        )


def validate_funds_index(
    funds_index: dict,
    index: dict,
    errors: list[str],
    fund_files: dict[str, Path],
    expected_split_adjustments: dict[str, list[dict]] | None = None,
) -> None:
    funds = funds_index.get("funds")
    if not isinstance(funds, list):
        errors.append("funds-index.json has non-list funds")
        return

    if "tickers" in funds_index:
        errors.append("funds-index.json must not contain ticker rows")

    if funds != index.get("funds"):
        errors.append("funds-index.json funds do not match index.json")

    funds_split_adjustments = funds_index.get("proven_split_adjustments")
    index_split_adjustments = index.get("proven_split_adjustments")
    if not isinstance(funds_split_adjustments, dict):
        errors.append(
            "funds-index.json proven_split_adjustments must be an object"
        )
    if not isinstance(index_split_adjustments, dict):
        errors.append("index.json proven_split_adjustments must be an object")
    if funds_split_adjustments != index_split_adjustments:
        errors.append(
            "funds-index.json proven_split_adjustments do not match index.json"
        )
    if (
        expected_split_adjustments is not None
        and funds_split_adjustments != expected_split_adjustments
    ):
        errors.append(
            "bootstrap proven_split_adjustments do not match independently "
            "recomputed stock proof"
        )

    for field in (
        "data_contract_version",
        "fund_data_revision",
        "last_updated",
        "total_filers",
        "total_tickers",
    ):
        if funds_index.get(field) != index.get(field):
            errors.append(
                f"funds-index.json {field}={funds_index.get(field)!r} "
                f"does not match index.json {field}={index.get(field)!r}"
            )

    fund_data_revision = funds_index.get("fund_data_revision")
    if (
        not isinstance(fund_data_revision, str)
        or re.fullmatch(r"[0-9a-f]{64}", fund_data_revision) is None
    ):
        errors.append(
            "funds-index.json fund_data_revision must be a lowercase "
            "SHA-256 digest"
        )
    else:
        digest = hashlib.sha256()
        try:
            ordered_fund_paths = sorted(
                fund_files.values(),
                key=lambda path: path.name,
            )
            for path in ordered_fund_paths:
                digest.update(path.name.encode("utf-8"))
                digest.update(b"\0")
                digest.update(path.read_bytes())
                digest.update(b"\0")
        except OSError as exc:
            errors.append(
                f"cannot recompute fund_data_revision: {exc}"
            )
        else:
            if fund_data_revision != digest.hexdigest():
                errors.append(
                    "funds-index.json fund_data_revision does not match "
                    "the checked-in fund payloads"
                )

    last_updated = funds_index.get("last_updated")
    if not is_strict_utc_timestamp(last_updated):
        errors.append(
            "funds-index.json last_updated must use strict UTC "
            "YYYY-MM-DDTHH:MM:SSZ format"
        )

    if funds_index.get("total_filers") != len(funds):
        errors.append(
            "funds-index.json total_filers does not match funds length"
        )

    try:
        size = FUNDS_INDEX_PATH.stat().st_size
    except OSError:
        size = 0
    if size > 850_000:
        errors.append(
            f"funds-index.json is {size:,} bytes; compact fund-calendar "
            "bootstrap must stay under 850,000"
        )


def main(*, incremental: bool = False, cache_path: Path | None = None, refresh_cache: bool = False) -> int:
    cache = None
    if incremental:
        from incremental_validation import ValidationCache
        cache = ValidationCache(sys.modules[__name__], cache_path, reuse=not refresh_cache)
    errors: list[str] = []
    warnings: list[str] = []
    quality_summary: dict[str, object] = {
        "amendment_migration_pending": 0,
        "security_identity_migration_pending": 0,
        "quarter_health_pending": 0,
        "current_composition_hash_quarters": 0,
        "legacy_composition_hash_quarters": 0,
        "legacy_value_unit_quarters": 0,
        "legacy_value_unit_sources": 0,
        "without_value_unit_provenance": 0,
        "legacy_month_precision_filing_dates": 0,
        "value_unit_migration_version": None,
        "filer_name_collision_groups": 0,
    }

    if not DATA_DIR.exists():
        print(f"data directory not found: {DATA_DIR}", file=sys.stderr)
        return 1

    registry_data = load_json(CUSIP_REGISTRY_PATH, errors)
    registry_is_valid = isinstance(registry_data, dict)
    registry = registry_data if registry_is_valid else {}
    (
        fund_files,
        stock_groups,
        fund_cusips,
        fund_calendars,
        expected_current_stats,
    ) = (cache.validate_funds(errors, registry, quality_summary)
         if cache is not None else validate_funds(errors, registry, quality_summary))
    raw_legacy_month_filing_dates = quality_summary[
        "legacy_month_precision_filing_dates"
    ]
    legacy_month_filing_dates = (
        raw_legacy_month_filing_dates
        if type(raw_legacy_month_filing_dates) is int
        else 0
    )
    if legacy_month_filing_dates:
        warnings.append(
            f"{legacy_month_filing_dates} retained legacy quarter(s) have "
            "month-only filing dates; the site must display month precision "
            "without inventing a calendar day"
        )
    filer_collisions = filer_name_collision_groups(fund_calendars)
    quality_summary["filer_name_collision_groups"] = len(filer_collisions)
    if filer_collisions:
        collision_samples = "; ".join(
            f"{collision['name']} ({', '.join(collision['ciks'])})"
            for collision in filer_collisions[:5]
        )
        warnings.append(
            f"{len(filer_collisions)} normalized SEC filer legal-name "
            "collision group(s) require CIK-level disambiguation; samples: "
            f"{collision_samples}"
        )
    pipeline_state = validate_pipeline_state(
        fund_files,
        errors,
        warnings,
        quality_summary,
    )
    expected_split_adjustments: dict[str, list[dict]] = {}
    stock_validator = cache.validate_stocks if cache is not None else validate_stocks
    stock_files = stock_validator(
        errors,
        fund_calendars,
        expected_current_stats,
        expected_split_adjustments,
        registry=registry,
    )
    if registry_is_valid:
        registry = validate_registry(fund_cusips, errors, registry)
        validate_private_sec_security_state(registry, errors)
        validate_security_labels(registry, errors)

    index = load_json(INDEX_PATH, errors)
    funds_index = load_json(FUNDS_INDEX_PATH, errors)
    if isinstance(index, dict):
        validate_data_contract(index, "index.json", errors)
        validate_index(
            index,
            fund_files,
            stock_files,
            registry,
            errors,
            warnings,
            fund_calendars,
            pipeline_state,
        )
        if isinstance(funds_index, dict):
            validate_data_contract(
                funds_index,
                "funds-index.json",
                errors,
            )
            validate_funds_index(
                funds_index,
                index,
                errors,
                fund_files,
                expected_split_adjustments,
            )

    for stock_id, info in sorted(stock_groups.items()):
        if len(info["cusips"]) > 1 and len(info["issuers"]) > 1:
            errors.append(
                f"stock identity {stock_id} spans multiple CUSIPs/issuers: "
                f"{', '.join(sorted(info['cusips']))}"
            )

    print("Checked-in corpus quality summary:")
    print(
        "  - Files checked: "
        f"{len(fund_files)} fund, {len(stock_files)} stock"
    )
    print(
        "  - SEC filer legal-name collision groups (kept distinct by CIK): "
        f"{quality_summary['filer_name_collision_groups']}"
    )
    print(
        "  - Amendment migration targets quarantined: "
        f"{quality_summary['amendment_migration_pending']}"
    )
    print(
        "  - Security identity migration targets withheld: "
        f"{quality_summary['security_identity_migration_pending']}"
    )
    print(
        "  - Quarter-health targets withheld: "
        f"{quality_summary['quarter_health_pending']}"
    )
    print(
        "  - Composition provenance: "
        f"{quality_summary['current_composition_hash_quarters']} current "
        f"hash-v{_COMPOSITION_HASH_VERSION} quarter(s), "
        f"{quality_summary['legacy_composition_hash_quarters']} retained "
        "legacy hash quarter(s)"
    )
    print(
        "  - Retained legacy month-precision filing dates: "
        f"{quality_summary['legacy_month_precision_filing_dates']}"
    )
    print(
        "  - Value-unit migration: "
        f"state version {quality_summary['value_unit_migration_version']!r}, "
        f"current policy {VALUE_UNIT_POLICY_VERSION}"
    )
    print(
        "  - Legacy value-unit provenance (internally valid, not current "
        "proof): "
        f"{quality_summary['legacy_value_unit_quarters']} quarter(s), "
        f"{quality_summary['legacy_value_unit_sources']} source filing(s)"
    )
    print(
        "  - Retained quarters without value-unit provenance "
        "(covered by corpus-level anomaly checks): "
        f"{quality_summary['without_value_unit_provenance']}"
    )

    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"  - {warning}")

    if cache is not None:
        cache.finish(success=not errors)
    if errors:
        print("Validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print(
        "Internal consistency checks passed: "
        f"{len(fund_files)} fund files, "
        f"{len(stock_files)} stock files, "
        f"{len(warnings)} warning(s)"
    )
    return 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--incremental", action="store_true", help="Reuse content-bound successful checks; all global gates still run")
    parser.add_argument("--refresh-cache", action="store_true", help="Run every file check and replace cached results (requires --incremental)")
    args = parser.parse_args()
    if args.refresh_cache and not args.incremental:
        parser.error("--refresh-cache requires --incremental")
    sys.exit(main(incremental=args.incremental, refresh_cache=args.refresh_cache))
