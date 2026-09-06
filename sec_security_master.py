"""Deterministic, SEC-hosted security-symbol evidence.

This module deliberately keeps security identity separate from display metadata.
The identity key is ``CUSIP|instrument_type``; a ticker is emitted only when
recent SEC fails-to-deliver (FTD) observations agree and the exact symbol is
present in a current SEC symbol file.  Ambiguous, stale, malformed, and
unsupported securities fail closed with a null ticker.

The source-state file stores normalized evidence for each SEC URL together with
the response SHA-256.  That makes an incremental refresh cheap and permits a
byte-for-byte deterministic rebuild without contacting a third party (or the
SEC).  Fetch failures retain the last accepted source entry and master.
"""

from __future__ import annotations

import copy
import calendar
import csv
import fcntl
import hashlib
import io
import json
import os
import re
import secrets
import stat
import tempfile
import threading
import time
import unicodedata
import zipfile
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

import requests

from security_identity import SEC_TICKER_RE, VALID_INSTRUMENT_TYPES


ROOT = Path(__file__).resolve().parent
DEFAULT_MASTER_PATH = ROOT / ".cache" / "sec_security_master.json"
DEFAULT_SOURCE_STATE_PATH = ROOT / ".cache" / "sec_source_state.json"

_PAIR_LOCK_NAME = ".sec-security-master-pair.lock"
_PAIR_MARKER_NAME = ".sec-security-master-pair.transaction.json"
_PAIR_ARTIFACT_PREFIX = ".sec-security-master-pair."
_PAIR_TRANSACTION_SCHEMA_VERSION = 1

FTD_PAGE_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"
)
OFFICIAL_13F_LIST_PAGE_URL = (
    "https://www.sec.gov/rules-regulations/staff-guidance/"
    "official-list-section-13f-securities"
)
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANY_EXCHANGE_TICKERS_URL = (
    "https://www.sec.gov/files/company_tickers_exchange.json"
)
SEC_FUND_TICKERS_URL = "https://www.sec.gov/files/company_tickers_mf.json"

MASTER_SCHEMA_VERSION = 1
SOURCE_STATE_SCHEMA_VERSION = 4
TIMELINE_SOURCE_STATE_SCHEMA_VERSION = 3
COMPACT_SOURCE_STATE_SCHEMA_VERSION = 2
LEGACY_SOURCE_STATE_SCHEMA_VERSION = 1
FTD_COMPACT_RECORD_SCHEMA_VERSION = 1
FTD_TIMELINE_SCHEMA_VERSION = 1
FTD_MAX_RECENT_EXACT_DATES = 32
FTD_CHECKPOINT_ARCHIVE_INTERVAL = 8
_FTD_COMPACT_RECORD_FIELDS = frozenset({
    "record_schema_version",
    "cusip",
    "symbol",
    "description",
    "first_settlement_date",
    "last_settlement_date",
    "observation_dates",
    "distinct_settlement_date_count",
    "row_count",
})
_FTD_EXACT_OBSERVATION_FIELDS = frozenset({
    "settlement_date",
    "symbol",
    "observation_count",
    "descriptions",
    "sources",
})
_FTD_TIMELINE_INTERVAL_FIELDS = frozenset({
    "timeline_schema_version",
    "symbols",
    "symbol",
    "first_seen",
    "last_seen",
    "observation_dates",
    "observation_date_count",
    "observation_count",
    "sources",
    "descriptions",
    "symbol_descriptions",
    "observations",
})
_FTD_MASTER_INTERVAL_FIELDS = _FTD_TIMELINE_INTERVAL_FIELDS - {
    "timeline_schema_version",
    "observations",
}
_LEGACY_FTD_MASTER_INTERVAL_FIELDS = frozenset({
    "symbol",
    "first_seen",
    "last_seen",
    "observation_dates",
    "observation_date_count",
    "observation_count",
    "sources",
    "descriptions",
})
_FTD_ARCHIVE_INVENTORY_FIELDS = frozenset({
    "url",
    "kind",
    "sha256",
    "accepted_at",
    "record_count",
    "raw_record_count",
    "matched_record_count",
    "matched_cusip_count",
    "first_settlement_date",
    "last_settlement_date",
    "observed_months",
    "date_inventory_complete",
    "boundary_date_proofs",
    "filter_universe_sha256",
    "filter_universe_count",
})
_FTD_BOUNDARY_DATE_PROOF_FIELDS = frozenset({
    "date",
    "row_count",
    "row_multiset_sha256",
})
_OFFICIAL_13F_RECORD_FIELDS = frozenset({
    "cusip",
    "option_indicator",
    "issuer",
    "description",
    "status",
})
MASTER_AUDIT_SCHEMA_VERSION = 5
IXBRL_MASTER_AUDIT_SCHEMA_VERSION = 4
CURRENT_SOURCE_MASTER_AUDIT_SCHEMA_VERSION = 3
FILTER_COVERAGE_MASTER_AUDIT_SCHEMA_VERSION = 2
LEGACY_MASTER_AUDIT_SCHEMA_VERSION = 1
EDGAR_DISCOVERY_SCHEMA_VERSION = 2
LEGACY_EDGAR_DISCOVERY_SCHEMA_VERSION = 1
DEFAULT_FTD_LOOKBACK_MONTHS = 24
DEFAULT_RECENT_WINDOW_DAYS = 31
DEFAULT_MAX_EVIDENCE_AGE_DAYS = 395
DEFAULT_MIN_CONFIRMATION_DATES = 2
DEFAULT_SOURCE_STALENESS_DAYS = 45
DEFAULT_SUCCESSFUL_CHECK_CHECKPOINT_DAYS = 30
DEFAULT_MIN_FTD_COVERAGE_RATIO = 0.95
DEFAULT_MAX_UNEXPLAINED_REGRESSION_PERCENTAGE_POINTS = 1.0
DEFAULT_FTD_COVERAGE_WINDOW_DAYS = 366
DEFAULT_MAX_SOURCE_POPULATION_REGRESSION_RATIO = 0.10
DEFAULT_MAX_RESOLVED_MAPPING_REGRESSION_RATIO = 0.10
PRODUCTION_MIN_CURRENT_SYMBOL_POPULATION_BY_KIND = {
    "sec_company_tickers": 5_000,
    "sec_company_exchange_tickers": 5_000,
    "sec_fund_tickers": 5_000,
}
PRODUCTION_MIN_CURRENT_SYMBOL_TITLE_RATIO = 0.80
PRODUCTION_MIN_ACTIVE_OFFICIAL_CUSIP_COUNT = 10_000
FTD_ARCHIVE_HISTORY_START = date(2004, 3, 22)
FTD_ARCHIVE_DISCOVERY_MATURITY_DAYS = 45
# Add only SEC-documented archive omissions, keyed by _ftd_archive_period_key.
_DOCUMENTED_MISSING_FTD_ARCHIVE_PERIODS: frozenset[tuple[Any, ...]] = frozenset()
_FTD_2004_Q1_PERIOD = ("quarter", 2004, 1)
_FTD_2004_Q2_PERIOD = ("quarter", 2004, 2)
_FTD_2004_BOUNDARY_DATE = date(2004, 4, 1)

VALID_MAPPING_STATUSES = frozenset({
    "resolved",
    "unresolved",
    "ambiguous",
    "no_listed_symbol",
    "malformed_as_filed",
})
VALID_TICKER_SOURCES = frozenset({
    "sec_ftd",
    "sec_ixbrl",
})
VALID_MAPPING_METHODS = frozenset({
    "exact_ftd_symbol_with_sec_metadata_validation",
    "exact_schedule_13dg_ixbrl_class_bridge",
})

# FTD is an equity-security feed.  Calls, puts, options, and notes can share an
# issuer or even an underlying CUSIP, so they must never inherit an FTD symbol.
FTD_ELIGIBLE_INSTRUMENT_TYPES = frozenset({"EQUITY", "PREF", "WARRANT"})

_SEC_HOSTS = frozenset({"sec.gov", "www.sec.gov"})
# SEC's CMS occasionally publishes an archive with a numeric filename revision
# such as ``cnsfails201910a_0.zip``.  The suffix identifies the file URL, not a
# second settlement period; period comparisons deliberately ignore it.
_FTD_ARCHIVE_RE = re.compile(
    r"^(?:cnsfails\d{4}(?:0[1-9]|1[0-2])[ab]|"
    r"cnsp_sec_fails_\d{4}q[1-4])(?:_[0-9]+)?\.zip$",
    re.IGNORECASE,
)
_FTD_SEMIMONTHLY_ARCHIVE_RE = re.compile(
    r"^cnsfails(?P<year>\d{4})(?P<month>0[1-9]|1[0-2])"
    r"(?P<half>[ab])(?:_(?P<revision>[0-9]+))?\.zip$",
    re.IGNORECASE,
)
_FTD_QUARTERLY_ARCHIVE_RE = re.compile(
    r"^cnsp_sec_fails_(?P<year>\d{4})q(?P<quarter>[1-4])"
    r"(?:_(?P<revision>[0-9]+))?\.zip$",
    re.IGNORECASE,
)
_OFFICIAL_13F_LIST_RE = re.compile(
    r"^13flist(?P<year>\d{4})q(?P<quarter>[1-4])(?:-txt)?\.txt$",
    re.IGNORECASE,
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = SEC_TICKER_RE
_SEC_ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
_FORM_13F_DATASET_PATH_RE = re.compile(
    r"/files/structureddata/data/form-13f-data-sets/"
    r"(?:\d{4}q[1-4]|\d{2}[a-z]{3}\d{4}-\d{2}[a-z]{3}\d{4})_form13f\.zip",
    re.IGNORECASE,
)
_FORM_13F_ARCHIVE_DOCUMENT_PATH_RE = re.compile(
    r"/Archives/edgar/data/\d{1,10}/(?P<accession_compact>\d{18})/"
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}",
)
_SYNTHETIC_CUSIPS = frozenset({"000000000"})
_SOURCE_KINDS = frozenset({
    "sec_ftd_index",
    "sec_ftd_archive",
    "sec_13f_list_index",
    "sec_13f_list",
    "sec_company_tickers",
    "sec_company_exchange_tickers",
    "sec_fund_tickers",
    "sec_fund_series",
})
_FORBIDDEN_SOURCE_STATE_KEYS = frozenset({
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credentials",
    "headers",
    "password",
    "request_headers",
    "request_metadata",
    "response_headers",
    "response_metadata",
    "secret",
    "set_cookie",
    "token",
    "user_agent",
})
_FORBIDDEN_SOURCE_STATE_KEY_SUFFIXES = frozenset({
    "api_key",
    "authorization",
    "cookie",
    "credentials",
    "password",
    "request_headers",
    "request_metadata",
    "response_headers",
    "response_metadata",
    "secret",
    "set_cookie",
    "token",
    "user_agent",
})
_FORBIDDEN_SOURCE_STATE_COMPACT_SUFFIXES = frozenset(
    suffix.replace("_", "")
    for suffix in _FORBIDDEN_SOURCE_STATE_KEY_SUFFIXES
)
_VALIDATION_SOURCE_KINDS = frozenset({
    "sec_company_tickers",
    "sec_company_exchange_tickers",
    "sec_fund_tickers",
})
_REQUIRED_CURRENT_SOURCE_KINDS = frozenset({
    "sec_company_tickers",
    "sec_company_exchange_tickers",
    "sec_fund_tickers",
    "sec_ftd_index",
    "sec_13f_list_index",
    "sec_13f_list",
})
_EDGAR_PROVENANCE_SOURCE_KINDS = frozenset({
    "schedule_13dg",
    "periodic_ixbrl",
})
_MASTER_PROVENANCE_SOURCE_KINDS = (
    _SOURCE_KINDS | _EDGAR_PROVENANCE_SOURCE_KINDS
)
_FTD_MAX_ARCHIVE_MEMBERS = 8
_FTD_MAX_UNCOMPRESSED_BYTES = 300_000_000
_SEC_FETCH_LOCK = threading.Lock()
_SEC_NEXT_REQUEST_AT = 0.0
_PAIR_LOCK_REGISTRY_GUARD = threading.Lock()
_PAIR_PROCESS_LOCKS: dict[str, threading.RLock] = {}
_PAIR_LOCK_LOCAL = threading.local()
_DEBT_CLASS_RE = re.compile(
    r"\b(?:BOND|DEBT|DEBENTURE|NOTE|NOTES|SDBCV|SR\s+NT|SUB\s+NT|CV\s+NT)\b"
)
_OPTION_CLASS_RE = re.compile(r"\b(?:CALL|PUT|OPTION|OPT)\b")
_WARRANT_CLASS_RE = re.compile(r"\b(?:WARRANT|WARRANTS|WT|WTS|W\s+EXP)\b")
_PREFERRED_CLASS_RE = re.compile(r"\b(?:PREF|PREFERRED|PFD)\b")


class SecurityMasterError(ValueError):
    """Base error for invalid security-master input or persisted state."""


class SecurityMasterAcceptanceError(SecurityMasterError):
    """Fatal acceptance-gate failure that preserves the last-good master."""

    error_code = "sec_security_master_acceptance_failed"

    def __init__(self, audit: Mapping[str, Any]) -> None:
        self.audit = json.loads(json.dumps(audit))
        issues = ",".join(str(issue) for issue in audit.get("issues", []))
        super().__init__(f"{self.error_code}: {issues or 'unspecified_gate'}")


class SourceParseError(SecurityMasterError):
    """Raised when an SEC response cannot safely become durable evidence."""


class SourceSchemaError(SourceParseError):
    """Raised when a decoded SEC response breaks a required source contract."""


class SourceSchemaChangeError(SecurityMasterError):
    """Fatal source-contract alert that leaves the last-good files unchanged."""

    error_code = "sec_source_schema_change_detected"

    def __init__(self, errors: Iterable[str]) -> None:
        self.errors = tuple(sorted(set(str(error) for error in errors)))
        super().__init__(f"{self.error_code}: {'; '.join(self.errors)}")


class NonSECURL(SecurityMasterError):
    """Raised before any non-SEC URL can enter state or be fetched."""


Fetcher = Callable[[str], bytes]


@dataclass(frozen=True)
class RefreshResult:
    """Result of an incremental refresh.

    ``errors`` are non-fatal source failures.  The associated prior source
    entries remain active, which is the module's last-good behavior.
    """

    master: dict[str, Any]
    state: dict[str, Any]
    changed: bool
    refreshed_urls: tuple[str, ...]
    retained_urls: tuple[str, ...]
    errors: tuple[str, ...]
    acceptance: dict[str, Any]


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.casefold() != "a":
            return
        for name, value in attrs:
            if name.casefold() == "href" and value:
                self.hrefs.append(value.strip())


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _payload_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _mapping_sha256(payload: Mapping[str, Any]) -> str:
    return _payload_sha256(_canonical_json_bytes(payload))


def _normalized_filter_universe(cusips: Iterable[object]) -> list[str]:
    normalized: set[str] = set()
    for cusip in cusips:
        value = normalize_cusip(cusip)
        if value:
            normalized.add(value)
    return sorted(normalized)


def _filter_universe_sha256(cusips: Iterable[object]) -> str:
    return _mapping_sha256({"cusips": _normalized_filter_universe(cusips)})


def _source_schema_fingerprint(source: Mapping[str, Any]) -> str:
    """Hash the normalized output contract, independent of observed row count."""

    kind = str(source.get("kind") or "")
    declared_record_fields = {
        "sec_ftd_archive": _FTD_ARCHIVE_INVENTORY_FIELDS,
        "sec_13f_list": _OFFICIAL_13F_RECORD_FIELDS,
    }.get(kind)
    if declared_record_fields is None:
        record_fields: set[str] = set()
        for record in source.get("records", []):
            if isinstance(record, dict):
                record_fields.update(str(field) for field in record)
    else:
        record_fields = set(declared_record_fields)
    schema = {
        "kind": kind,
        "record_fields": sorted(record_fields),
        "has_discovered_urls": isinstance(source.get("discovered_urls"), list),
        "has_symbols": isinstance(source.get("symbols"), list),
        "has_symbol_titles": isinstance(source.get("symbol_titles"), dict),
        "has_symbol_exchanges": isinstance(
            source.get("symbol_exchanges"), dict
        ),
        "has_fund_records": isinstance(source.get("fund_records"), list),
        "has_series_names": isinstance(source.get("series_names"), dict),
        "has_class_names": isinstance(source.get("class_names"), dict),
    }
    return _mapping_sha256(schema)


def _source_schema_fingerprints_by_kind(
    state: Mapping[str, Any],
) -> dict[str, str]:
    """Return stable normalized-schema hashes grouped by SEC source kind."""

    fingerprints: dict[str, set[str]] = defaultdict(set)
    for source in state.get("sources", {}).values():
        if not isinstance(source, dict):
            continue
        kind = str(source.get("kind") or "")
        if kind:
            fingerprints[kind].add(_source_schema_fingerprint(source))
    return {
        kind: _mapping_sha256({"schema_sha256": sorted(values)})
        for kind, values in sorted(fingerprints.items())
    }


def _utc_timestamp(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc).replace(microsecond=0)
    return current.isoformat().replace("+00:00", "Z")


def _is_canonical_utc_timestamp(value: object | None) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return False
    return _utc_timestamp(parsed) == value


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically replace one JSON file and fsync both file and directory."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink_or_nonregular(path, allow_missing=True)
    temporary_prefix = (
        f"{path.name}." if path.name.startswith(".") else f".{path.name}."
    )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=temporary_prefix,
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as out:
            json.dump(
                payload,
                out,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            out.write("\n")
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            temporary_path.unlink()
        except BaseException:
            # Cleanup is best-effort and must never replace the active write,
            # fsync, SystemExit, or KeyboardInterrupt exception.
            pass
        raise


def _fsync_directory(path: Path) -> None:
    """Durably record same-directory renames and hard-link operations."""

    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    directory_fd = os.open(Path(path), flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _reject_symlink_or_nonregular(
    path: Path,
    *,
    allow_missing: bool,
) -> None:
    """Reject links and special files before reading or replacing them."""

    path = Path(path)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        if allow_missing:
            return
        raise SecurityMasterError(
            f"managed path is missing: {path.name}"
        ) from None
    except OSError as exc:
        raise SecurityMasterError(
            f"cannot inspect managed path {path.name}: {exc}"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SecurityMasterError(
            f"managed path must be a regular file: {path.name}"
        )


def _open_regular_json(path: Path) -> dict[str, Any]:
    """Open one JSON object without following a last-component symlink."""

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(Path(path), flags)
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise SecurityMasterError(f"cannot read JSON object {path}: {exc}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise SecurityMasterError(
                f"managed path must be a regular file: {Path(path).name}"
            )
        with os.fdopen(descriptor, "r", encoding="utf-8") as source:
            descriptor = -1
            payload = json.load(source)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SecurityMasterError(f"cannot read JSON object {path}: {exc}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not isinstance(payload, dict):
        raise SecurityMasterError(f"JSON root must be an object: {path}")
    return payload


def _read_json_object(path: Path) -> dict[str, Any]:
    return _open_regular_json(Path(path))


def normalize_cusip(value: object | None) -> str:
    """Preserve the filed identifier apart from trim and uppercase."""

    return str(value or "").strip().upper()


def normalize_instrument_type(value: object | None) -> str:
    normalized = str(value or "EQUITY").strip().upper() or "EQUITY"
    if normalized not in VALID_INSTRUMENT_TYPES:
        raise SecurityMasterError(
            f"unsupported security-master instrument type: {normalized}"
        )
    return normalized


def security_key(cusip: object | None, instrument_type: object | None) -> str:
    return f"{normalize_cusip(cusip)}|{normalize_instrument_type(instrument_type)}"


def _cusip_character_value(character: str) -> int | None:
    if character.isdigit():
        return int(character)
    if "A" <= character <= "Z":
        return ord(character) - ord("A") + 10
    return {"*": 36, "@": 37, "#": 38}.get(character)


def calculate_cusip_check_digit(first_eight: object | None) -> int:
    """Return the standard CUSIP/CINS Modulus-10 Double-Add-Double digit."""

    normalized = normalize_cusip(first_eight)
    if len(normalized) != 8:
        raise SecurityMasterError("CUSIP check-digit input must be eight characters")
    total = 0
    for index, character in enumerate(normalized):
        value = _cusip_character_value(character)
        if value is None:
            raise SecurityMasterError(
                f"CUSIP contains unsupported character: {character!r}"
            )
        if index % 2 == 1:
            value *= 2
        total += value // 10 + value % 10
    return (10 - total % 10) % 10


def cusip_quarantine_reason(value: object | None) -> str | None:
    normalized = normalize_cusip(value)
    if normalized in _SYNTHETIC_CUSIPS:
        return "synthetic_or_placeholder_identifier"
    if len(normalized) != 9:
        return "identifier_must_be_nine_characters"
    if not normalized[-1].isdigit():
        return "check_digit_must_be_numeric"
    try:
        expected = calculate_cusip_check_digit(normalized[:8])
    except SecurityMasterError:
        return "identifier_contains_unsupported_character"
    if int(normalized[-1]) != expected:
        return "check_digit_mismatch"
    return None


def is_valid_cusip(value: object | None) -> bool:
    return cusip_quarantine_reason(value) is None


def _normalize_symbol(value: object | None) -> str | None:
    symbol = str(value or "").strip().upper()
    return symbol if _SYMBOL_RE.fullmatch(symbol) else None


def normalize_sec_url(url: str, *, base_url: str | None = None) -> str:
    """Return a fragment-free HTTPS SEC URL or fail before network access."""

    candidate = urljoin(base_url, url) if base_url else str(url or "").strip()
    candidate = urldefrag(candidate)[0]
    parsed = urlparse(candidate)
    try:
        port = parsed.port
    except ValueError as exc:
        raise NonSECURL(f"invalid SEC URL port: {candidate!r}") from exc
    if (
        parsed.scheme.casefold() != "https"
        or (parsed.hostname or "").casefold() not in _SEC_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
    ):
        raise NonSECURL(f"only HTTPS sec.gov URLs are allowed: {candidate!r}")
    return candidate


def _normalize_reported_identity_source_url(
    value: object | None,
    *,
    accession: object | None,
) -> str:
    """Accept only an official 13F ZIP or exact accession-bound document."""

    raw = str(value or "").strip()
    normalized_accession = str(accession or "").strip()
    if not _SEC_ACCESSION_RE.fullmatch(normalized_accession):
        raise SecurityMasterError("invalid reported-identity accession")
    try:
        canonical = normalize_sec_url(raw)
    except NonSECURL as exc:
        raise SecurityMasterError(
            "invalid reported-identity source URL"
        ) from exc
    parsed = urlparse(canonical)
    if (
        (parsed.hostname or "").casefold() != "www.sec.gov"
        or parsed.query
        or parsed.fragment
    ):
        raise SecurityMasterError("noncanonical reported-identity source URL")
    if _FORM_13F_DATASET_PATH_RE.fullmatch(parsed.path):
        return canonical
    match = _FORM_13F_ARCHIVE_DOCUMENT_PATH_RE.fullmatch(parsed.path)
    if (
        match is None
        or match.group("accession_compact")
        != normalized_accession.replace("-", "")
    ):
        raise SecurityMasterError(
            "reported-identity URL is not bound to its accession"
        )
    return canonical


def discover_ftd_urls(
    page_html: str | bytes,
    *,
    page_url: str = FTD_PAGE_URL,
) -> list[str]:
    """Discover canonical SEC FTD ZIP URLs from the SEC landing page."""

    page_url = normalize_sec_url(page_url)
    if isinstance(page_html, bytes):
        text = _decode_text(page_html)
    else:
        text = str(page_html)
    parser = _LinkParser()
    parser.feed(text)
    urls: set[str] = set()
    for href in parser.hrefs:
        try:
            candidate = normalize_sec_url(href, base_url=page_url)
        except NonSECURL:
            continue
        filename = Path(urlparse(candidate).path).name
        if _FTD_ARCHIVE_RE.fullmatch(filename):
            urls.add(candidate)
    return sorted(urls, key=_ftd_url_sort_key)


def _require_discovered_ftd_urls(urls: list[str]) -> list[str]:
    if not urls:
        raise SourceSchemaError("SEC FTD page exposed no archive ZIP links")
    return urls


def _ftd_archive_period_key(url: str) -> tuple[Any, ...]:
    """Return one stable period identity and reject wrong-era filenames."""

    canonical_url = normalize_sec_url(url)
    filename = Path(urlparse(canonical_url).path).name
    # This also enforces the documented quarterly/semi-monthly era boundary.
    _ftd_archive_date_bounds(canonical_url)
    semimonthly = _FTD_SEMIMONTHLY_ARCHIVE_RE.fullmatch(filename)
    if semimonthly is not None:
        return (
            "half_month",
            int(semimonthly.group("year")),
            int(semimonthly.group("month")),
            semimonthly.group("half").casefold(),
        )
    quarterly = _FTD_QUARTERLY_ARCHIVE_RE.fullmatch(filename)
    if quarterly is not None:
        return (
            "quarter",
            int(quarterly.group("year")),
            int(quarterly.group("quarter")),
        )
    raise SourceSchemaError("FTD archive URL has no recognized SEC period")


def _ftd_period_label(period: tuple[Any, ...]) -> str:
    if period[0] == "quarter":
        return f"{period[1]}Q{period[2]}"
    return f"{period[1]}-{period[2]:02d}{period[3]}"


def _ftd_urls_by_period(
    urls: Iterable[str],
    *,
    context: str,
) -> dict[tuple[Any, ...], str]:
    """Index canonical archive URLs and reject ambiguous period aliases."""

    periods: dict[tuple[Any, ...], str] = {}
    for raw_url in urls:
        url = normalize_sec_url(raw_url)
        period = _ftd_archive_period_key(url)
        prior = periods.setdefault(period, url)
        if prior != url:
            raise SourceSchemaError(
                f"{context} contains multiple URLs for archive period "
                + _ftd_period_label(period)
            )
    return periods


def _expected_ftd_archive_periods(*, as_of: date) -> set[tuple[Any, ...]]:
    """Return documented archive periods mature at least 45 days ago."""

    mature_through = as_of - timedelta(
        days=FTD_ARCHIVE_DISCOVERY_MATURITY_DAYS
    )
    expected: set[tuple[Any, ...]] = set()
    # The first quarterly bundle's canonical evidence begins on March 22, 2004,
    # but is published under the 2004Q1 filename.
    for year in range(FTD_ARCHIVE_HISTORY_START.year, 2010):
        for quarter in range(1, 5):
            if (year, quarter) > (2009, 2):
                break
            last_month = quarter * 3
            period_end = date(
                year,
                last_month,
                calendar.monthrange(year, last_month)[1],
            )
            if period_end <= mature_through:
                expected.add(("quarter", year, quarter))

    year, month = 2009, 7
    while date(year, month, 1) <= mature_through:
        if date(year, month, 15) <= mature_through:
            expected.add(("half_month", year, month, "a"))
        month_end = date(year, month, calendar.monthrange(year, month)[1])
        if month_end <= mature_through:
            expected.add(("half_month", year, month, "b"))
        month += 1
        if month == 13:
            year += 1
            month = 1
    return expected - set(_DOCUMENTED_MISSING_FTD_ARCHIVE_PERIODS)


def _validate_ftd_archive_discovery(
    urls: Iterable[str],
    *,
    as_of: date,
    require_full_history: bool,
    prior_urls: Iterable[str] = (),
) -> list[str]:
    """Reject incomplete/ambiguous SEC FTD archive discovery results."""

    normalized = sorted(
        {normalize_sec_url(url) for url in urls},
        key=_ftd_url_sort_key,
    )
    periods = _ftd_urls_by_period(normalized, context="SEC FTD page")
    prior_periods = set(
        _ftd_urls_by_period(
            prior_urls,
            context="prior SEC FTD discovery state",
        )
    )
    disappeared = sorted(prior_periods - set(periods), key=_ftd_period_label)
    if disappeared:
        raise SourceParseError(
            "SEC FTD page omitted previously discovered archive periods: "
            + ", ".join(_ftd_period_label(item) for item in disappeared[:20])
        )
    if require_full_history:
        missing = sorted(
            _expected_ftd_archive_periods(as_of=as_of) - set(periods),
            key=_ftd_period_label,
        )
        if missing:
            raise SourceParseError(
                "SEC FTD all-history discovery is incomplete; missing periods: "
                + ", ".join(_ftd_period_label(item) for item in missing[:20])
            )
    return normalized


def discover_latest_13f_list_url(
    page_html: str | bytes,
    *,
    page_url: str = OFFICIAL_13F_LIST_PAGE_URL,
) -> str:
    """Return the newest quarterly TXT link on the SEC official-list page."""

    page_url = normalize_sec_url(page_url)
    text = _decode_text(page_html) if isinstance(page_html, bytes) else str(page_html)
    parser = _LinkParser()
    parser.feed(text)
    candidates: list[tuple[int, int, str]] = []
    for href in parser.hrefs:
        try:
            candidate = normalize_sec_url(href, base_url=page_url)
        except NonSECURL:
            continue
        match = _OFFICIAL_13F_LIST_RE.fullmatch(
            Path(urlparse(candidate).path).name
        )
        if match:
            candidates.append((
                int(match.group("year")),
                int(match.group("quarter")),
                candidate,
            ))
    if not candidates:
        raise SourceSchemaError(
            "SEC official 13F-list page exposed no quarterly TXT link"
        )
    return max(candidates)[2]


def _ftd_archive_month_range(filename: str) -> tuple[int, int] | None:
    semimonthly_match = _FTD_SEMIMONTHLY_ARCHIVE_RE.fullmatch(filename)
    if semimonthly_match:
        year = int(semimonthly_match.group("year"))
        month = int(semimonthly_match.group("month"))
        month_index = year * 12 + month - 1
        return month_index, month_index
    quarterly_match = _FTD_QUARTERLY_ARCHIVE_RE.fullmatch(filename)
    if quarterly_match:
        year = int(quarterly_match.group("year"))
        quarter = int(quarterly_match.group("quarter"))
        first_month = (quarter - 1) * 3 + 1
        return (
            year * 12 + first_month - 1,
            year * 12 + first_month + 1,
        )
    return None


def _ftd_url_sort_key(url: str) -> tuple[int, int, int, str]:
    filename = Path(urlparse(url).path).name
    semimonthly_match = _FTD_SEMIMONTHLY_ARCHIVE_RE.fullmatch(filename)
    if semimonthly_match:
        return (
            int(semimonthly_match.group("year")),
            int(semimonthly_match.group("month")),
            1 if semimonthly_match.group("half").lower() == "a" else 2,
            url,
        )
    quarterly_match = _FTD_QUARTERLY_ARCHIVE_RE.fullmatch(filename)
    if quarterly_match:
        return (
            int(quarterly_match.group("year")),
            int(quarterly_match.group("quarter")) * 3,
            0,
            url,
        )
    return (0, 0, 0, url)


def select_recent_ftd_urls(
    urls: Iterable[str],
    *,
    as_of: date,
    lookback_months: int = DEFAULT_FTD_LOOKBACK_MONTHS,
) -> list[str]:
    if lookback_months < 1:
        raise SecurityMasterError("lookback_months must be positive")
    current_month_index = as_of.year * 12 + as_of.month - 1
    minimum_month_index = current_month_index - lookback_months + 1
    selected: set[str] = set()
    for raw_url in urls:
        url = normalize_sec_url(raw_url)
        month_range = _ftd_archive_month_range(
            Path(urlparse(url).path).name
        )
        if month_range is None:
            continue
        first_month_index, last_month_index = month_range
        if (
            last_month_index >= minimum_month_index
            and first_month_index <= current_month_index
        ):
            selected.add(url)
    return sorted(selected, key=_ftd_url_sort_key)


def _decode_text(payload: bytes) -> str:
    try:
        return payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        return payload.decode("cp1252")


def _normalized_header(value: object | None) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _parse_settlement_date(value: object | None) -> str | None:
    raw = str(value or "").strip()
    for pattern in ("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, pattern).date().isoformat()
        except ValueError:
            continue
    return None


def _parse_calendar_date(value: object | None) -> date | None:
    """Normalize a date, timestamp, or supported settlement-date string."""

    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc)
        return parsed.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    settlement_date = _parse_settlement_date(raw)
    if settlement_date:
        return date.fromisoformat(settlement_date)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.date()


def _source_successful_check_at(source: Mapping[str, Any]) -> str | None:
    """Return a source's durable successful-check checkpoint.

    ``accepted_at`` is the compatibility fallback for source-state entries
    written before successful unchanged checks were checkpointed separately.
    """

    value = source.get("last_successful_check_at", source.get("accepted_at"))
    raw = str(value or "").strip()
    return raw if _parse_calendar_date(raw) is not None else None


def _successful_check_checkpoint_due(
    source: Mapping[str, Any],
    *,
    checked_at: datetime,
    checkpoint_days: int = DEFAULT_SUCCESSFUL_CHECK_CHECKPOINT_DAYS,
) -> bool:
    prior_date = _parse_calendar_date(_source_successful_check_at(source))
    if prior_date is None:
        return True
    return (checked_at.date() - prior_date).days >= checkpoint_days


def _parse_quantity(value: object | None) -> int | None:
    raw = str(value or "").strip().replace(",", "")
    if not raw:
        return None
    try:
        quantity = int(raw)
    except ValueError:
        return None
    return quantity if quantity >= 0 else None


def _iter_ftd_pipe_records(
    lines: Iterable[str],
) -> Iterable[dict[str, Any]]:
    """Yield normalized observations from one FTD member without buffering it."""

    reader = csv.reader(
        (line.replace("\x00", "") for line in lines),
        delimiter="|",
    )
    try:
        header = next(reader)
    except StopIteration as exc:
        raise SourceParseError("FTD pipe file is empty") from exc
    header_index = {
        _normalized_header(name): index for index, name in enumerate(header)
    }
    required = {"SETTLEMENTDATE", "CUSIP", "SYMBOL"}
    if not required.issubset(header_index):
        raise SourceSchemaError(
            "FTD pipe header is missing required fields: "
            + ", ".join(sorted(required - set(header_index)))
        )

    def field(row: list[str], name: str) -> str:
        index = header_index.get(name)
        return row[index].strip() if index is not None and index < len(row) else ""

    yielded = False
    for row in reader:
        if not row or all(not cell.strip() for cell in row):
            continue
        # Some historical files repeat the header between concatenated blocks.
        if _normalized_header(field(row, "SETTLEMENTDATE")) == "SETTLEMENTDATE":
            continue
        settlement_date = _parse_settlement_date(field(row, "SETTLEMENTDATE"))
        cusip = normalize_cusip(field(row, "CUSIP"))
        symbol = _normalize_symbol(field(row, "SYMBOL"))
        if not settlement_date or not cusip or not symbol:
            continue
        record: dict[str, Any] = {
            "settlement_date": settlement_date,
            "cusip": cusip,
            "symbol": symbol,
            "quantity": _parse_quantity(field(row, "QUANTITYFAILS")),
            "description": " ".join(field(row, "DESCRIPTION").split()),
            "price": field(row, "PRICE"),
        }
        yielded = True
        yield record
    if not yielded:
        raise SourceParseError("FTD pipe file contained no usable observations")


def parse_ftd_pipe(payload: str | bytes) -> list[dict[str, Any]]:
    """Parse one SEC pipe-delimited FTD member into normalized observations.

    Invalid check digits are retained and later quarantined.  Rows without a
    usable date, identifier, or symbol are ignored because they cannot support
    a dated mapping decision.
    """

    text = _decode_text(payload) if isinstance(payload, bytes) else str(payload)
    records = list(_iter_ftd_pipe_records(io.StringIO(text)))
    return sorted(
        records,
        key=lambda item: (
            item["settlement_date"],
            item["cusip"],
            item["symbol"],
            item.get("description") or "",
            item.get("quantity") if item.get("quantity") is not None else -1,
        ),
    )


def _iter_ftd_zip_records(payload: bytes) -> Iterable[dict[str, Any]]:
    """Yield all usable ZIP rows while bounding decompressed-memory use."""

    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except (zipfile.BadZipFile, OSError) as exc:
        raise SourceParseError(f"invalid FTD ZIP: {exc}") from exc
    with archive:
        members = [info for info in archive.infolist() if not info.is_dir()]
        if not members or len(members) > _FTD_MAX_ARCHIVE_MEMBERS:
            raise SourceParseError("FTD ZIP has an unsafe member count")
        total_size = sum(info.file_size for info in members)
        if total_size > _FTD_MAX_UNCOMPRESSED_BYTES:
            raise SourceParseError("FTD ZIP exceeds the uncompressed size limit")
        yielded = False
        for info in sorted(members, key=lambda item: item.filename):
            suffix = Path(info.filename).suffix.casefold()
            if suffix not in {"", ".txt", ".csv"}:
                continue
            try:
                with archive.open(info) as member:
                    decoded_lines = (
                        _decode_text(raw_line) for raw_line in member
                    )
                    for record in _iter_ftd_pipe_records(decoded_lines):
                        yielded = True
                        yield record
            except (RuntimeError, OSError, zipfile.BadZipFile) as exc:
                raise SourceParseError(
                    f"cannot read FTD ZIP member {info.filename!r}: {exc}"
                ) from exc
    if not yielded:
        raise SourceParseError("FTD ZIP contained no usable pipe-delimited member")


def parse_ftd_zip(payload: bytes) -> list[dict[str, Any]]:
    """Safely parse all text members of one in-memory SEC FTD ZIP."""

    records = list(_iter_ftd_zip_records(payload))
    return sorted(
        records,
        key=lambda item: (
            item["settlement_date"],
            item["cusip"],
            item["symbol"],
            item.get("description") or "",
            item.get("quantity") if item.get("quantity") is not None else -1,
        ),
    )


def _canonical_ftd_row_json(record: Mapping[str, Any]) -> str:
    """Serialize every normalized parser field for a multiset equality proof."""

    normalized = {
        "settlement_date": _parse_settlement_date(
            record.get("settlement_date")
        ),
        "cusip": normalize_cusip(record.get("cusip")),
        "symbol": _normalize_symbol(record.get("symbol")),
        "quantity": _parse_quantity(record.get("quantity")),
        "description": " ".join(
            str(record.get("description") or "").split()
        ),
        "price": str(record.get("price") or "").strip(),
    }
    return json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _ftd_boundary_date_proof(
    canonical_rows: Iterable[str],
    *,
    settlement_date: date,
) -> dict[str, Any]:
    """Hash a full normalized-row multiset while retaining no source rows."""

    multiplicities: dict[str, int] = defaultdict(int)
    row_count = 0
    for row in canonical_rows:
        multiplicities[str(row)] += 1
        row_count += 1
    proof_payload = {
        "date": settlement_date.isoformat(),
        "rows": [
            [row, multiplicities[row]] for row in sorted(multiplicities)
        ],
    }
    return {
        "date": settlement_date.isoformat(),
        "row_count": row_count,
        "row_multiset_sha256": _mapping_sha256(proof_payload),
    }


def _ftd_archive_date_bounds(source_url: str) -> tuple[date, date]:
    """Return the canonical settlement-date ownership interval for an archive."""

    canonical_url = normalize_sec_url(source_url)
    filename = Path(urlparse(canonical_url).path).name
    semimonthly = _FTD_SEMIMONTHLY_ARCHIVE_RE.fullmatch(filename)
    if semimonthly is not None:
        year = int(semimonthly.group("year"))
        month = int(semimonthly.group("month"))
        if (year, month) < (2009, 7):
            raise SourceSchemaError(
                "semi-monthly FTD archive predates the July 2009 layout"
            )
        if semimonthly.group("half").casefold() == "a":
            # Despite the human-facing "first half" label, the SEC files use
            # a disjoint 14/15 cutover: A owns days 1-14 and B owns day 15
            # through month-end.  Binding day 15 to B is especially important
            # at the July 2009 quarterly-to-semi-monthly transition, where the
            # first B archive begins on 2009-07-15.
            return date(year, month, 1), date(year, month, 14)
        return (
            date(year, month, 15),
            date(year, month, calendar.monthrange(year, month)[1]),
        )

    quarterly = _FTD_QUARTERLY_ARCHIVE_RE.fullmatch(filename)
    if quarterly is not None:
        year = int(quarterly.group("year"))
        quarter = int(quarterly.group("quarter"))
        if (year, quarter) > (2009, 2):
            raise SourceSchemaError(
                "quarterly FTD archive extends beyond the June 2009 layout"
            )
        first_month = (quarter - 1) * 3 + 1
        last_month = first_month + 2
        period_start = date(year, first_month, 1)
        if (year, quarter) == (2004, 1):
            period_start = FTD_ARCHIVE_HISTORY_START
        return (
            period_start,
            date(year, last_month, calendar.monthrange(year, last_month)[1]),
        )
    raise SourceSchemaError("FTD archive URL has no recognized SEC period")


def _ftd_archive_raw_date_bounds(source_url: str) -> tuple[date, date]:
    """Return dates admissible in the raw archive before canonical de-duplication.

    The SEC's 2004Q1 ZIP contains April 1 rows that are repeated byte-for-byte
    (after parser normalization, including multiplicity) in 2004Q2.  Q2 owns
    that date.  Q1 may retain it only as a separately proven raw boundary
    duplicate; it can never enter Q1 compact evidence or the ticker timeline.
    """

    period_start, period_end = _ftd_archive_date_bounds(source_url)
    if _ftd_archive_period_key(source_url) == _FTD_2004_Q1_PERIOD:
        period_end = _FTD_2004_BOUNDARY_DATE
    return period_start, period_end


def _validate_ftd_archive_dates(
    records: Iterable[Mapping[str, Any]],
    *,
    source_url: str,
    require_quarter_month_coverage: bool = False,
) -> None:
    """Reject any FTD observation outside its URL-encoded archive period."""

    period_start, period_end = _ftd_archive_date_bounds(source_url)
    observed_months: set[tuple[int, int]] = set()
    for record in records:
        settlement_dates = record.get("observation_dates")
        if not isinstance(settlement_dates, list):
            settlement_dates = [record.get("settlement_date")]
        for raw_date in settlement_dates:
            normalized = _parse_settlement_date(raw_date)
            if normalized is None:
                raise SourceParseError(
                    "FTD archive contains an invalid settlement date"
                )
            settlement_date = date.fromisoformat(normalized)
            observed_months.add((settlement_date.year, settlement_date.month))
            if not period_start <= settlement_date <= period_end:
                raise SourceSchemaError(
                    "FTD settlement date falls outside archive period: "
                    f"{normalized} not in {period_start.isoformat()}.."
                    f"{period_end.isoformat()} for {source_url}"
                )
    filename = Path(urlparse(normalize_sec_url(source_url)).path).name
    if (
        require_quarter_month_coverage
        and _FTD_QUARTERLY_ARCHIVE_RE.fullmatch(filename)
    ):
        expected_months = {
            (period_start.year, month)
            for month in range(period_start.month, period_end.month + 1)
        }
        missing_months = sorted(expected_months - observed_months)
        if missing_months:
            formatted = ", ".join(
                f"{year:04d}-{month:02d}" for year, month in missing_months
            )
            raise SourceParseError(
                "quarterly FTD archive is missing settlement-month coverage: "
                f"{formatted} for {source_url}"
            )


def compact_ftd_records(
    records: Iterable[Mapping[str, Any]],
    target_cusips: Iterable[object] | None = None,
) -> list[dict[str, Any]]:
    """Collapse one archive to exact target-universe, per-date identity proof.

    Rows are grouped by CUSIP, symbol, and SEC description.  The complete set
    of distinct settlement dates is retained because resolution confirmation
    and time-versioned symbol intervals depend on dates, while quantities and
    duplicate raw rows are not resolution evidence.
    """

    target = (
        set(_normalized_filter_universe(target_cusips))
        if target_cusips is not None
        else None
    )
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    for raw_record in records:
        if not isinstance(raw_record, Mapping):
            continue
        cusip = normalize_cusip(raw_record.get("cusip"))
        symbol = _normalize_symbol(raw_record.get("symbol"))
        settlement_date = _parse_settlement_date(
            raw_record.get("settlement_date")
        )
        if (
            not cusip
            or not symbol
            or not settlement_date
            or (target is not None and cusip not in target)
        ):
            continue
        description = " ".join(
            str(raw_record.get("description") or "").split()
        )
        key = (cusip, symbol, description)
        bucket = buckets.setdefault(key, {
            "dates": set(),
            "row_count": 0,
        })
        bucket["dates"].add(settlement_date)
        bucket["row_count"] += 1

    compacted: list[dict[str, Any]] = []
    for (cusip, symbol, description), bucket in sorted(buckets.items()):
        observation_dates = sorted(bucket["dates"])
        compacted.append({
            "record_schema_version": FTD_COMPACT_RECORD_SCHEMA_VERSION,
            "cusip": cusip,
            "symbol": symbol,
            "description": description,
            "first_settlement_date": observation_dates[0],
            "last_settlement_date": observation_dates[-1],
            "observation_dates": observation_dates,
            "distinct_settlement_date_count": len(observation_dates),
            "row_count": bucket["row_count"],
        })
    return compacted


def parse_official_13f_list(payload: str | bytes) -> list[dict[str, Any]]:
    """Parse the SEC's headerless 80-column quarterly official-list TXT.

    Columns are defined by the SEC page: CUSIP 1-9, option indicator 10,
    issuer 11-40, description 41-67, and status 68-70.  Column 80 is ignored;
    current files use it despite older page text calling it unused.
    """

    text = _decode_text(payload) if isinstance(payload, bytes) else str(payload)
    if not text.strip():
        raise SourceParseError("official 13F-list TXT is empty")
    # SEC-generated lists can repeat the same physical 80-column row. Treat
    # the normalized five-field record as set membership: exact semantic
    # repeats carry no additional identity evidence, while a changed issuer,
    # class, status, or option marker remains distinct conflict evidence.
    records_by_key: dict[
        tuple[str, str, str, str, str],
        dict[str, Any],
    ] = {}
    for row_number, raw_line in enumerate(text.splitlines(), start=1):
        if not raw_line.strip():
            continue
        if len(raw_line) < 70:
            raise SourceSchemaError(
                "official 13F list has a truncated fixed-width row: "
                f"{row_number}"
            )
        if len(raw_line) > 80 and raw_line[80:].strip():
            raise SourceSchemaError(
                "official 13F list has data beyond column 80: "
                f"{row_number}"
            )
        line = raw_line.ljust(80)
        if line[70:79].strip():
            raise SourceSchemaError(
                "official 13F list has data in reserved columns 71-79: "
                f"{row_number}"
            )
        cusip = normalize_cusip(line[0:9])
        option_indicator = line[9:10]
        issuer = " ".join(line[10:40].split())
        description = " ".join(line[40:67].split())
        status = line[67:70].strip()
        if not cusip or not issuer:
            raise SourceSchemaError(
                "official 13F list has a missing CUSIP or issuer at row "
                f"{row_number}"
            )
        if option_indicator not in {"", " ", "*"}:
            raise SourceSchemaError(
                f"official 13F list has invalid option indicator for {cusip}"
            )
        if status not in {"", "*A*", "*D*"}:
            raise SourceSchemaError(
                f"official 13F list has invalid status for {cusip}: {status!r}"
            )
        record = {
            "cusip": cusip,
            "option_indicator": option_indicator.strip(),
            "issuer": issuer,
            "description": description,
            "status": status,
        }
        record_key = (
            record["cusip"],
            record["description"],
            record["issuer"],
            record["status"],
            record["option_indicator"],
        )
        records_by_key.setdefault(record_key, record)
    if not records_by_key:
        raise SourceSchemaError(
            "official 13F-list TXT contained no usable rows"
        )
    return [records_by_key[key] for key in sorted(records_by_key)]


def _parse_json_payload(payload: bytes | str | Mapping[str, Any] | list[Any]) -> Any:
    if isinstance(payload, (dict, list)):
        return payload
    try:
        text = _decode_text(payload) if isinstance(payload, bytes) else str(payload)
        return json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceParseError(f"SEC symbol payload is not valid JSON: {exc}") from exc


def _symbol_metadata_from_payload(payload: Any) -> dict[str, Any]:
    symbols: set[str] = set()
    titles: dict[str, set[str]] = defaultdict(set)
    exchanges: dict[str, set[str]] = defaultdict(set)

    def add_exchanges(symbol: str, raw_value: Any) -> None:
        values = raw_value if isinstance(raw_value, list) else [raw_value]
        for value in values:
            exchange = " ".join(str(value or "").split())
            if exchange:
                exchanges[symbol].add(exchange)

    if isinstance(payload, dict) and isinstance(payload.get("fields"), list):
        fields = [_normalized_header(field) for field in payload["fields"]]
        symbol_index = next(
            (
                index
                for index, field in enumerate(fields)
                if field in {"TICKER", "SYMBOL"}
            ),
            None,
        )
        title_index = next(
            (
                index
                for index, field in enumerate(fields)
                if field in {"TITLE", "NAME", "ISSUER", "SERIESNAME", "CLASSNAME"}
            ),
            None,
        )
        exchange_index = next(
            (
                index
                for index, field in enumerate(fields)
                if field in {"EXCHANGE", "EXCHANGES"}
            ),
            None,
        )
        data = payload.get("data")
        if symbol_index is None or not isinstance(data, list):
            raise SourceSchemaError(
                "SEC tabular symbol payload has no symbol field"
            )
        for row in data:
            if not isinstance(row, list) or symbol_index >= len(row):
                continue
            symbol = _normalize_symbol(row[symbol_index])
            if symbol:
                symbols.add(symbol)
                if title_index is not None and title_index < len(row):
                    title = " ".join(str(row[title_index] or "").split())
                    if title:
                        titles[symbol].add(title)
                if exchange_index is not None and exchange_index < len(row):
                    add_exchanges(symbol, row[exchange_index])
    else:
        if isinstance(payload, dict):
            entries: Iterable[Any] = payload.values()
        elif isinstance(payload, list):
            entries = payload
        else:
            raise SourceSchemaError(
                "SEC symbol payload must be an object or array"
            )
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            symbol = _normalize_symbol(entry.get("ticker") or entry.get("symbol"))
            if symbol:
                symbols.add(symbol)
                title = " ".join(
                    str(
                        entry.get("title")
                        or entry.get("name")
                        or entry.get("issuer")
                        or ""
                    ).split()
                )
                if title:
                    titles[symbol].add(title)
                add_exchanges(
                    symbol,
                    entry.get("exchanges", entry.get("exchange")),
                )
    if not symbols:
        raise SourceSchemaError(
            "SEC symbol payload contained no valid symbols"
        )
    return {
        "symbols": sorted(symbols),
        "symbol_titles": {
            symbol: sorted(titles[symbol]) for symbol in sorted(titles)
        },
        "symbol_exchanges": {
            symbol: sorted(exchanges[symbol]) for symbol in sorted(exchanges)
        },
    }


def _fund_symbol_metadata_from_payload(payload: Any) -> dict[str, Any]:
    """Preserve the SEC fund symbol-to-series/class bridge with its symbols."""

    metadata = _symbol_metadata_from_payload(payload)
    if not isinstance(payload, dict) or not isinstance(payload.get("fields"), list):
        raise SourceSchemaError("SEC fund-symbol payload must be tabular")
    fields = [_normalized_header(field) for field in payload["fields"]]
    required = {"CIK", "SERIESID", "CLASSID", "SYMBOL"}
    if not required.issubset(fields) or not isinstance(payload.get("data"), list):
        raise SourceSchemaError(
            "SEC fund-symbol payload lacks series/class fields"
        )
    indexes = {field: fields.index(field) for field in required}
    records: set[tuple[str, str, str, str]] = set()
    for row in payload["data"]:
        if not isinstance(row, list) or any(
            index >= len(row) for index in indexes.values()
        ):
            continue
        symbol = _normalize_symbol(row[indexes["SYMBOL"]])
        raw_cik = str(row[indexes["CIK"]] or "").strip()
        series_id = str(row[indexes["SERIESID"]] or "").strip().upper()
        class_id = str(row[indexes["CLASSID"]] or "").strip().upper()
        if (
            symbol
            and raw_cik.isdigit()
            and int(raw_cik) > 0
            and re.fullmatch(r"S\d+", series_id)
            and re.fullmatch(r"C\d+", class_id)
        ):
            records.add((symbol, raw_cik.zfill(10), series_id, class_id))
    if not records:
        raise SourceSchemaError(
            "SEC fund-symbol payload has no valid series/class rows"
        )
    metadata["fund_records"] = [
        {
            "symbol": symbol,
            "cik": cik,
            "series_id": series_id,
            "class_id": class_id,
        }
        for symbol, cik, series_id, class_id in sorted(records)
    ]
    return metadata


def _symbols_from_payload(payload: Any) -> list[str]:
    return _symbol_metadata_from_payload(payload)["symbols"]


def parse_sec_company_symbols(payload: Any) -> list[str]:
    return _symbols_from_payload(_parse_json_payload(payload))


def parse_sec_company_exchange_symbols(payload: Any) -> list[str]:
    return _symbols_from_payload(_parse_json_payload(payload))


def parse_sec_fund_symbols(payload: Any) -> list[str]:
    return _symbols_from_payload(_parse_json_payload(payload))


def empty_source_state() -> dict[str, Any]:
    return {
        "schema_version": SOURCE_STATE_SCHEMA_VERSION,
        "updated_at": None,
        "current_filter_universe_sha256": None,
        "current_filter_universe_count": 0,
        "ftd_processed_filter_universe_sha256": None,
        "ftd_processed_filter_universe_count": 0,
        "ftd_filter_cusips": [],
        "ftd_timeline": {},
        "ftd_mutable_tail": {},
        "filter_universes": {},
        "required_filter_coverage_urls": [],
        "edgar_evidence": {},
        "edgar_discovery": {},
        "sources": {},
    }


def _is_sensitive_source_state_key(raw_key: object) -> bool:
    """Recognize credential keys without inspecting or guessing at values."""

    normalized_key = re.sub(
        r"[^a-z0-9]+",
        "_",
        str(raw_key).strip().lower(),
    ).strip("_")
    if normalized_key in _FORBIDDEN_SOURCE_STATE_KEYS:
        return True
    if any(
        normalized_key.endswith(f"_{suffix}")
        for suffix in _FORBIDDEN_SOURCE_STATE_KEY_SUFFIXES
    ):
        return True
    compact_key = normalized_key.replace("_", "")
    return any(
        compact_key == suffix or compact_key.endswith(suffix)
        for suffix in _FORBIDDEN_SOURCE_STATE_COMPACT_SUFFIXES
    )


def _reject_sensitive_source_state_metadata(value: object) -> None:
    """Prevent request credentials/contact metadata from entering snapshots."""

    if isinstance(value, Mapping):
        for raw_key, nested in value.items():
            if _is_sensitive_source_state_key(raw_key):
                raise SecurityMasterError(
                    "SEC source state contains forbidden request metadata"
                )
            _reject_sensitive_source_state_metadata(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            _reject_sensitive_source_state_metadata(nested)


def _migrate_v1_source_state(state: Mapping[str, Any]) -> dict[str, Any]:
    """Read-migrate unfiltered raw-row v1 state to compact v2 evidence."""

    migrated = json.loads(json.dumps(state))
    migrated["schema_version"] = COMPACT_SOURCE_STATE_SCHEMA_VERSION
    migrated["current_filter_universe_sha256"] = None
    migrated["current_filter_universe_count"] = 0
    migrated["filter_universes"] = {}
    migrated["required_filter_coverage_urls"] = []
    migrated["edgar_evidence"] = {}
    migrated["edgar_discovery"] = {}
    for source in migrated.get("sources", {}).values():
        if not isinstance(source, dict) or source.get("kind") != "sec_ftd_archive":
            continue
        raw_records = source.get("records", [])
        if not isinstance(raw_records, list):
            continue
        compacted = compact_ftd_records(raw_records)
        source["records"] = compacted
        source["record_count"] = len(compacted)
        source["raw_record_count"] = len(raw_records)
        # V1 archives were retained without a filter, so they prove coverage
        # for every later target identifier without being downloaded again.
        source["filter_all_cusips"] = True
        source.pop("filter_universe_sha256", None)
        source.pop("filter_universe_count", None)
    return migrated


def _ftd_observations_from_archive_records(
    records: Iterable[Mapping[str, Any]],
    *,
    source_url: str,
    source_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    """Expand legacy compact rows into exact dated evidence for migration.

    This helper is intentionally confined to the one-time v2 -> v3 reader.
    V3 never persists the expanded result: it is immediately reduced to
    bounded interval witnesses.
    """

    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    source_ref = {"url": source_url, "sha256": source_sha256}
    for record in records:
        if not isinstance(record, Mapping):
            continue
        cusip = normalize_cusip(record.get("cusip"))
        symbol = _normalize_symbol(record.get("symbol"))
        dates = record.get("observation_dates")
        if not cusip or not symbol or not isinstance(dates, list):
            continue
        description = " ".join(str(record.get("description") or "").split())
        for raw_date in dates:
            settlement_date = _parse_settlement_date(raw_date)
            if settlement_date is None:
                continue
            key = (cusip, settlement_date, symbol)
            bucket = buckets.setdefault(key, {
                "settlement_date": settlement_date,
                "symbol": symbol,
                "observation_count": 0,
                "descriptions": set(),
                "sources": {(source_url, source_sha256): source_ref},
            })
            bucket["observation_count"] += 1
            if description:
                bucket["descriptions"].add(description)

    by_cusip: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (cusip, _settlement_date, _symbol), bucket in sorted(buckets.items()):
        by_cusip[cusip].append({
            "settlement_date": bucket["settlement_date"],
            "symbol": bucket["symbol"],
            "observation_count": bucket["observation_count"],
            "descriptions": sorted(bucket["descriptions"]),
            "sources": [
                bucket["sources"][key] for key in sorted(bucket["sources"])
            ],
        })
    return dict(by_cusip)


def _bounded_ftd_interval_observations(
    observations: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Keep the first boundary and a bounded tail of exact date witnesses."""

    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for observation in observations:
        settlement_date = _parse_settlement_date(
            observation.get("settlement_date")
        )
        symbol = _normalize_symbol(observation.get("symbol"))
        if settlement_date is None or symbol is None:
            continue
        # Callers already own normalized copies. Reusing the bounded witness
        # objects avoids re-serializing the same 32-date tail on every archive.
        by_date[settlement_date].append(dict(observation))
    dates = sorted(by_date)
    if not dates:
        return []
    retained_dates = {dates[0], *dates[-FTD_MAX_RECENT_EXACT_DATES:]}
    retained: list[dict[str, Any]] = []
    for settlement_date in sorted(retained_dates):
        retained.extend(sorted(
            by_date[settlement_date],
            key=lambda item: (
                str(item.get("symbol") or ""),
                json.dumps(item, sort_keys=True, separators=(",", ":")),
            ),
        ))
    return retained


def _refresh_ftd_interval_projection(interval: dict[str, Any]) -> None:
    """Recompute every bounded projection derived from exact witnesses."""

    observations = _bounded_ftd_interval_observations(
        interval.get("observations", [])
    )
    interval["observations"] = observations
    interval["observation_dates"] = sorted({
        item["settlement_date"] for item in observations
    })
    sources: dict[tuple[str, str], dict[str, str]] = {}
    for observation in observations:
        for source in observation.get("sources", []):
            if not isinstance(source, Mapping):
                continue
            key = (
                str(source.get("url") or ""),
                str(source.get("sha256") or ""),
            )
            if all(key):
                sources[key] = {"url": key[0], "sha256": key[1]}
    interval["sources"] = [sources[key] for key in sorted(sources)]
    symbol_descriptions: dict[str, set[str]] = defaultdict(set)
    for observation in observations:
        symbol = str(observation.get("symbol") or "")
        symbol_descriptions[symbol].update(
            description
            for description in observation.get("descriptions", [])
            if isinstance(description, str) and description
        )
    interval["symbol_descriptions"] = {
        symbol: sorted(set(descriptions))
        for symbol, descriptions in sorted(symbol_descriptions.items())
        if descriptions
    }
    interval["descriptions"] = sorted({
        description
        for descriptions in interval["symbol_descriptions"].values()
        for description in descriptions
    })


def _append_ftd_observations_to_timeline(
    timeline: dict[str, list[dict[str, Any]]],
    observations_by_cusip: Mapping[str, Iterable[Mapping[str, Any]]],
) -> None:
    """Append ordered archive evidence to compact symbol-set intervals.

    SEC archive periods are disjoint and processed chronologically. Refusing
    an insertion at or before an existing interval is deliberate: without the
    superseded archive rows, subtracting or splicing historical contributions
    cannot be proven safe. Such a mutation requires a clean rebuild.
    """

    for cusip in sorted(observations_by_cusip):
        by_date: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for raw in observations_by_cusip[cusip]:
            settlement_date = _parse_settlement_date(raw.get("settlement_date"))
            symbol = _normalize_symbol(raw.get("symbol"))
            if settlement_date is None or symbol is None:
                continue
            by_date[settlement_date][symbol] = copy.deepcopy(raw)
        intervals = timeline.setdefault(cusip, [])
        touched_intervals: list[dict[str, Any]] = []
        for settlement_date in sorted(by_date):
            observed = by_date[settlement_date]
            symbols = sorted(observed)
            if intervals and settlement_date <= intervals[-1]["last_seen"]:
                raise SecurityMasterError(
                    "cannot merge an overlapping historical FTD archive into "
                    f"the compact timeline for {cusip}; run a clean rebuild"
                )
            observation_count = sum(
                int(item.get("observation_count") or 1)
                for item in observed.values()
            )
            symbol_descriptions = {
                symbol: sorted(set(observed[symbol].get("descriptions", [])))
                for symbol in symbols
                if observed[symbol].get("descriptions")
            }
            if intervals and intervals[-1]["symbols"] == symbols:
                interval = intervals[-1]
                interval["last_seen"] = settlement_date
                interval["observation_date_count"] += 1
                interval["observation_count"] += observation_count
                interval["observations"].extend(observed.values())
            else:
                interval = {
                    "timeline_schema_version": FTD_TIMELINE_SCHEMA_VERSION,
                    "symbols": symbols,
                    "symbol": symbols[0] if len(symbols) == 1 else None,
                    "first_seen": settlement_date,
                    "last_seen": settlement_date,
                    "observation_dates": [settlement_date],
                    "observation_date_count": 1,
                    "observation_count": observation_count,
                    "sources": [],
                    "descriptions": [],
                    "symbol_descriptions": symbol_descriptions,
                    "observations": list(observed.values()),
                }
                intervals.append(interval)
            if not touched_intervals or touched_intervals[-1] is not interval:
                touched_intervals.append(interval)
        # Projection performs deduplication and witness retention. Doing it
        # once per touched interval avoids repeatedly sorting the same growing
        # evidence tail for every settlement date in a source archive.
        for interval in touched_intervals:
            _refresh_ftd_interval_projection(interval)


def _append_ftd_observations_atomically(
    timeline: dict[str, list[dict[str, Any]]],
    observations_by_cusip: Mapping[str, Iterable[Mapping[str, Any]]],
) -> None:
    """Apply one archive without leaving a partially mutated timeline.

    A runner interruption or unexpected validation failure may occur after
    some CUSIPs have been appended. Retain an undo log only for the archive's
    affected CUSIPs so the last complete chronological prefix remains safe to
    checkpoint without copying the entire historical timeline.
    """

    affected_cusips = list(observations_by_cusip)
    existing = {
        cusip: copy.deepcopy(timeline[cusip])
        for cusip in affected_cusips
        if cusip in timeline
    }
    absent = set(affected_cusips) - set(existing)
    try:
        _append_ftd_observations_to_timeline(
            timeline,
            observations_by_cusip,
        )
    except BaseException:
        for cusip, intervals in existing.items():
            timeline[cusip] = intervals
        for cusip in absent:
            timeline.pop(cusip, None)
        raise


def _archive_inventory_from_compact_records(
    records: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    dates = sorted({
        settlement_date
        for record in records
        for raw_date in (
            record.get("observation_dates", [])
            if isinstance(record, Mapping)
            and isinstance(record.get("observation_dates"), list)
            else []
        )
        if (settlement_date := _parse_settlement_date(raw_date)) is not None
    })
    return {
        "first_settlement_date": dates[0] if dates else None,
        "last_settlement_date": dates[-1] if dates else None,
        "observed_months": sorted({item[:7] for item in dates}),
        "date_inventory_complete": False,
        # V2 retained only filtered compact rows, so it cannot prove the full
        # raw 2004Q1/Q2 boundary multiset. Final historical publication must
        # refetch those archives through the v3 operational parser.
        "boundary_date_proofs": [],
    }


def _migrate_v2_source_state(state: Mapping[str, Any]) -> dict[str, Any]:
    """Read-migrate per-archive date arrays to the bounded timeline state."""

    migrated = json.loads(json.dumps(state))
    migrated["schema_version"] = SOURCE_STATE_SCHEMA_VERSION
    timeline: dict[str, list[dict[str, Any]]] = {}
    mutable_tail: dict[str, dict[str, Any]] = {}
    processed_cusips: set[str] = set()
    filter_sequence: list[str] = []
    filter_seen: set[str] = set()
    filter_all = False
    profiles = migrated.setdefault("filter_universes", {})
    ftd_urls = sorted(
        (
            url
            for url, source in migrated.get("sources", {}).items()
            if isinstance(source, dict)
            and source.get("kind") == "sec_ftd_archive"
        ),
        key=_ftd_url_sort_key,
    )
    mutable_urls = set(ftd_urls[-2:])
    for url in ftd_urls:
        source = migrated["sources"][url]
        if not isinstance(source, dict) or source.get("kind") != "sec_ftd_archive":
            continue
        records = source.get("records", [])
        if not isinstance(records, list):
            raise SecurityMasterError(
                f"v2 FTD archive entry has no records list: {url}"
            )
        _validate_compact_ftd_records(records, source_url=url)
        raw_record_count = source.get("raw_record_count")
        if (
            source.get("record_count") != len(records)
            or type(raw_record_count) is not int
            or raw_record_count < len(records)
        ):
            raise SecurityMasterError(
                f"v2 FTD archive has invalid record counts: {url}"
            )
        if source.get("filter_all_cusips") is not True:
            prior_digest = source.get("filter_universe_sha256")
            prior_profile = profiles.get(prior_digest)
            if (
                not isinstance(prior_profile, dict)
                or prior_profile.get("cusips")
                != _normalized_filter_universe(prior_profile.get("cusips", []))
                or prior_profile.get("count")
                != len(prior_profile.get("cusips", []))
                or _filter_universe_sha256(prior_profile.get("cusips", []))
                != prior_digest
                or any(
                    record.get("cusip") not in prior_profile["cusips"]
                    for record in records
                )
            ):
                raise SecurityMasterError(
                    f"v2 FTD archive has invalid filter coverage: {url}"
                )
        observations = _ftd_observations_from_archive_records(
            records,
            source_url=url,
            source_sha256=str(source.get("sha256") or ""),
        )
        if url in mutable_urls:
            mutable_tail[url] = {
                "sha256": str(source.get("sha256") or ""),
                "records": records,
            }
        else:
            _append_ftd_observations_to_timeline(timeline, observations)
        inventory = _archive_inventory_from_compact_records(records)
        source.update(inventory)
        source["matched_record_count"] = len(records)
        source["matched_cusip_count"] = len(observations)
        source.setdefault("record_count", len(records))
        source.pop("records", None)
        if source.get("filter_all_cusips") is True:
            filter_all = True
        else:
            digest = source.get("filter_universe_sha256")
            profile = profiles.get(digest, {})
            if isinstance(profile, dict):
                coverage = set(
                    _normalized_filter_universe(profile.get("cusips", []))
                )
                if not filter_seen.issubset(coverage):
                    raise SecurityMasterError(
                        "v2 FTD archive filters are not monotonic; run a "
                        "clean security-master rebuild"
                    )
                additions = sorted(coverage - filter_seen)
                filter_sequence.extend(additions)
                filter_seen.update(additions)
                processed_cusips.update(coverage)
                source["filter_universe_count"] = len(filter_sequence)
                source["filter_universe_sha256"] = _filter_universe_sha256(
                    filter_sequence
                )

    migrated["ftd_timeline"] = {
        cusip: timeline[cusip] for cusip in sorted(timeline)
    }
    migrated["ftd_mutable_tail"] = {
        url: mutable_tail[url]
        for url in sorted(mutable_tail, key=_ftd_url_sort_key)
    }
    migrated["ftd_filter_cusips"] = filter_sequence
    if filter_all:
        migrated["ftd_processed_filter_universe_sha256"] = None
        migrated["ftd_processed_filter_universe_count"] = 0
        migrated["ftd_processed_all_cusips"] = True
    else:
        processed = _normalized_filter_universe(processed_cusips)
        digest = _filter_universe_sha256(processed) if processed else None
        if digest is not None:
            profiles[digest] = {"cusips": processed, "count": len(processed)}
        migrated["ftd_processed_filter_universe_sha256"] = digest
        migrated["ftd_processed_filter_universe_count"] = len(processed)
        migrated.pop("ftd_processed_all_cusips", None)
    _prune_filter_universe_profiles(migrated)
    return migrated


def _migrate_v3_source_state(state: Mapping[str, Any]) -> dict[str, Any]:
    """Add v4 boundary-proof slots without fabricating historical evidence.

    Ordinary archives need only an empty slot.  A pre-v4 Q1/Q2 inventory
    cannot prove the complete raw April 1 multiset, so it is marked incomplete
    and the refresh planner will refetch it before any master rebuild.
    """

    migrated = json.loads(json.dumps(state))
    migrated["schema_version"] = SOURCE_STATE_SCHEMA_VERSION
    for url, source in migrated.get("sources", {}).items():
        if not isinstance(source, dict) or source.get("kind") != "sec_ftd_archive":
            continue
        if "boundary_date_proofs" in source:
            continue
        source["boundary_date_proofs"] = []
        if _ftd_archive_period_key(str(url)) in {
            _FTD_2004_Q1_PERIOD,
            _FTD_2004_Q2_PERIOD,
        }:
            source["date_inventory_complete"] = False
    return migrated


def _normalize_source_state(state: Mapping[str, Any]) -> dict[str, Any]:
    _reject_sensitive_source_state_metadata(state)
    version = state.get("schema_version")
    if version == LEGACY_SOURCE_STATE_SCHEMA_VERSION:
        return _migrate_v2_source_state(_migrate_v1_source_state(state))
    if version == COMPACT_SOURCE_STATE_SCHEMA_VERSION:
        return _migrate_v2_source_state(state)
    if version == TIMELINE_SOURCE_STATE_SCHEMA_VERSION:
        return _migrate_v3_source_state(state)
    if version != SOURCE_STATE_SCHEMA_VERSION:
        raise SecurityMasterError("unsupported SEC source-state schema version")
    normalized = json.loads(json.dumps(state))
    # Test fixtures and interrupted development snapshots may already declare
    # the current version while still carrying the v2 per-archive shape.
    # Normalize them through the same deterministic migration, but never merge
    # it with an existing bounded timeline.
    if any(
        isinstance(source, dict)
        and source.get("kind") == "sec_ftd_archive"
        and "records" in source
        for source in normalized.get("sources", {}).values()
    ):
        if normalized.get("ftd_timeline"):
            raise SecurityMasterError(
                "SEC source state mixes v2 archive rows with an FTD timeline"
            )
        normalized["schema_version"] = COMPACT_SOURCE_STATE_SCHEMA_VERSION
        return _migrate_v2_source_state(normalized)
    normalized.setdefault("ftd_processed_filter_universe_sha256", None)
    normalized.setdefault("ftd_processed_filter_universe_count", 0)
    normalized.setdefault("ftd_filter_cusips", [])
    normalized.setdefault("ftd_timeline", {})
    normalized.setdefault("ftd_mutable_tail", {})
    return normalized


def _validate_compact_ftd_records(
    records: object,
    *,
    source_url: str,
) -> None:
    if not isinstance(records, list):
        raise SecurityMasterError("FTD archive entry has no records list")
    prior_key: tuple[str, str, str] | None = None
    for record in records:
        if not isinstance(record, dict) or set(record) != _FTD_COMPACT_RECORD_FIELDS:
            raise SecurityMasterError(
                f"FTD archive has malformed compact record: {source_url}"
            )
        cusip = normalize_cusip(record.get("cusip"))
        symbol = _normalize_symbol(record.get("symbol"))
        description = record.get("description")
        dates = record.get("observation_dates")
        if (
            record.get("record_schema_version")
            != FTD_COMPACT_RECORD_SCHEMA_VERSION
            or not cusip
            or cusip != record.get("cusip")
            or not symbol
            or symbol != record.get("symbol")
            or not isinstance(description, str)
            or not isinstance(dates, list)
            or not dates
            or dates != sorted(set(dates))
            or any(_parse_settlement_date(item) != item for item in dates)
            or record.get("first_settlement_date") != dates[0]
            or record.get("last_settlement_date") != dates[-1]
            or record.get("distinct_settlement_date_count") != len(dates)
            or type(record.get("row_count")) is not int
            or record["row_count"] < len(dates)
        ):
            raise SecurityMasterError(
                f"FTD archive has invalid compact date proof: {source_url}"
            )
        key = (cusip, symbol, description)
        if prior_key is not None and key <= prior_key:
            raise SecurityMasterError(
                f"FTD compact records are not unique and ordered: {source_url}"
            )
        prior_key = key
    _validate_ftd_archive_dates(records, source_url=source_url)


def _validate_ftd_boundary_proofs_for_source(
    source: Mapping[str, Any],
    *,
    source_url: str,
) -> dict[str, Any] | None:
    """Validate the row-free proof attached to the audited 2004 cutover."""

    proofs = source.get("boundary_date_proofs")
    if not isinstance(proofs, list):
        raise SourceSchemaError(
            f"FTD archive has invalid boundary-date proofs: {source_url}"
        )
    expected_date = (
        _FTD_2004_BOUNDARY_DATE.isoformat()
        if _ftd_archive_period_key(source_url)
        in {_FTD_2004_Q1_PERIOD, _FTD_2004_Q2_PERIOD}
        else None
    )
    if len(proofs) > 1:
        raise SourceSchemaError(
            f"FTD archive has unexpected boundary-date proofs: {source_url}"
        )
    proof = proofs[0] if proofs else None
    if proof is not None and (
        not isinstance(proof, dict)
        or set(proof) != _FTD_BOUNDARY_DATE_PROOF_FIELDS
        or proof.get("date") != expected_date
        or type(proof.get("row_count")) is not int
        or proof["row_count"] < 1
        or not _SHA256_RE.fullmatch(
            str(proof.get("row_multiset_sha256") or "")
        )
    ):
        raise SourceSchemaError(
            f"FTD archive has malformed boundary-date proof: {source_url}"
        )
    if expected_date is None and proof is not None:
        raise SourceSchemaError(
            f"FTD archive has an undeclared boundary-date proof: {source_url}"
        )
    if source.get("date_inventory_complete") is True and (
        expected_date is not None and proof is None
    ):
        raise SourceSchemaError(
            f"FTD archive is missing its required boundary-date proof: {source_url}"
        )
    return proof


def _validate_ftd_archive_inventory(
    source: Mapping[str, Any],
    *,
    source_url: str,
) -> None:
    """Validate a row-free archive inventory retained by current source state."""

    if "records" in source:
        raise SecurityMasterError(
            f"FTD archive must not retain per-security rows: {source_url}"
        )
    raw_count = source.get("raw_record_count")
    matched_count = source.get("matched_record_count")
    matched_cusips = source.get("matched_cusip_count")
    record_count = source.get("record_count")
    if (
        type(raw_count) is not int
        or raw_count < 1
        or type(matched_count) is not int
        or not 0 <= matched_count <= raw_count
        or type(matched_cusips) is not int
        or not 0 <= matched_cusips <= matched_count
        or record_count != matched_count
    ):
        raise SecurityMasterError(
            f"FTD archive has invalid compact inventory counts: {source_url}"
        )
    first = source.get("first_settlement_date")
    last = source.get("last_settlement_date")
    months = source.get("observed_months")
    complete = source.get("date_inventory_complete")
    if (
        complete not in {True, False}
        or not isinstance(months, list)
        or months != sorted(set(months))
        or any(re.fullmatch(r"\d{4}-\d{2}", str(item)) is None for item in months)
        or ((first is None) != (last is None))
        or (
            first is not None
            and (
                _parse_settlement_date(first) != first
                or _parse_settlement_date(last) != last
                or first > last
            )
        )
    ):
        raise SecurityMasterError(
            f"FTD archive has invalid settlement-date inventory: {source_url}"
        )
    boundary_proof = _validate_ftd_boundary_proofs_for_source(
        source,
        source_url=source_url,
    )
    period_start, period_end = _ftd_archive_raw_date_bounds(source_url)
    if first is not None and not (
        period_start.isoformat() <= first <= last <= period_end.isoformat()
    ):
        raise SecurityMasterError(
            f"FTD archive inventory falls outside its URL period: {source_url}"
        )
    if complete:
        expected_months = sorted({
            f"{period_start.year:04d}-{month:02d}"
            for month in range(period_start.month, period_end.month + 1)
        })
        if months != expected_months or first is None:
            raise SecurityMasterError(
                f"FTD archive has incomplete settlement-month inventory: {source_url}"
            )
        if boundary_proof is not None and not (
            first <= boundary_proof["date"] <= last
        ):
            raise SourceSchemaError(
                "FTD boundary-date proof falls outside the complete raw "
                f"inventory: {source_url}"
            )


def _validate_ftd_boundary_duplicate_proofs(
    state: Mapping[str, Any],
    *,
    require_complete: bool = False,
) -> None:
    """Bind the SEC's duplicate 2004-04-01 Q1 rows to Q2 exactly.

    Ordinary state checkpoints may contain the chronological Q1 prefix before
    Q2 has downloaded. A rebuild suitable for publication must contain both
    complete source inventories and equal full-row multiset proofs whenever
    either historical boundary period is in scope.
    """

    sources = state.get("sources", {})
    if not isinstance(sources, Mapping):
        raise SecurityMasterError("SEC source state must contain sources")
    by_period: dict[tuple[Any, ...], Mapping[str, Any]] = {}
    for url, source in sources.items():
        if not isinstance(source, Mapping) or source.get("kind") != "sec_ftd_archive":
            continue
        period = _ftd_archive_period_key(str(url))
        if period in by_period:
            raise SourceSchemaError(
                "SEC FTD source state contains duplicate archive periods"
            )
        by_period[period] = source

    boundary_periods = (_FTD_2004_Q1_PERIOD, _FTD_2004_Q2_PERIOD)
    required_periods = {
        _ftd_archive_period_key(str(url))
        for url in state.get("required_filter_coverage_urls", [])
        if isinstance(url, str)
    }
    boundary_in_scope = bool(
        set(boundary_periods) & (set(by_period) | required_periods)
    )
    proofs: dict[tuple[Any, ...], dict[str, Any]] = {}
    for period in boundary_periods:
        source = by_period.get(period)
        if source is None:
            continue
        source_url = str(source.get("url") or "")
        proof = _validate_ftd_boundary_proofs_for_source(
            source,
            source_url=source_url,
        )
        if proof is not None:
            proofs[period] = proof

    if len(proofs) == 2:
        q1_proof = proofs[_FTD_2004_Q1_PERIOD]
        q2_proof = proofs[_FTD_2004_Q2_PERIOD]
        if q1_proof != q2_proof:
            raise SourceSchemaError(
                "SEC FTD 2004Q1 boundary duplicate does not exactly match "
                "the 2004Q2 canonical rows"
            )

    if require_complete and boundary_in_scope:
        incomplete = [
            _ftd_period_label(period)
            for period in boundary_periods
            if period not in by_period
            or by_period[period].get("date_inventory_complete") is not True
            or period not in proofs
        ]
        if incomplete:
            raise SourceSchemaError(
                "SEC FTD 2004 boundary proof is incomplete for: "
                + ", ".join(incomplete)
            )


def _ftd_boundary_proof_refresh_needed(
    source: Mapping[str, Any],
    *,
    source_url: str,
) -> bool:
    """Return whether a migrated boundary archive needs a full-row refetch."""

    if _ftd_archive_period_key(source_url) not in {
        _FTD_2004_Q1_PERIOD,
        _FTD_2004_Q2_PERIOD,
    }:
        return False
    proofs = source.get("boundary_date_proofs")
    return (
        source.get("date_inventory_complete") is not True
        or not isinstance(proofs, list)
        or len(proofs) != 1
    )


def _validate_reparsed_ftd_timeline_witnesses(
    state: Mapping[str, Any],
    *,
    source_url: str,
    source_sha256: str,
    compact_records: Iterable[Mapping[str, Any]],
) -> None:
    """Prove retained timeline witnesses survive a metadata-only reparse.

    The bounded timeline intentionally discards most old exact observations,
    so an immutable archive cannot be subtracted and replayed in place.  A
    same-checksum schema migration may leave the timeline untouched only when
    every retained witness attributed to that archive is reproduced exactly by
    the current canonical parser.
    """

    expected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for cusip, observations in _ftd_observations_from_archive_records(
        compact_records,
        source_url=source_url,
        source_sha256=source_sha256,
    ).items():
        for observation in observations:
            key = (
                cusip,
                str(observation.get("settlement_date") or ""),
                str(observation.get("symbol") or ""),
            )
            expected[key] = observation

    source_ref = {"url": source_url, "sha256": source_sha256}
    for cusip, intervals in state.get("ftd_timeline", {}).items():
        if not isinstance(intervals, list):
            continue
        for interval in intervals:
            if not isinstance(interval, Mapping):
                continue
            for observation in interval.get("observations", []):
                if not isinstance(observation, Mapping):
                    continue
                raw_sources = observation.get("sources")
                if not isinstance(raw_sources, list) or source_ref not in raw_sources:
                    continue
                key = (
                    str(cusip),
                    str(observation.get("settlement_date") or ""),
                    str(observation.get("symbol") or ""),
                )
                # Canonical FTD archive ownership is disjoint.  More than one
                # source on a boundary witness cannot be safely apportioned
                # during a row-free migration.
                if raw_sources != [source_ref] or dict(observation) != expected.get(key):
                    raise SourceSchemaError(
                        "reparsed FTD boundary archive does not reproduce its "
                        f"retained timeline evidence: {source_url}"
                    )


def _upgrade_ftd_boundary_inventory(
    state: Mapping[str, Any],
    *,
    prior: Mapping[str, Any],
    parsed: Mapping[str, Any],
    source_url: str,
    source_sha256: str,
    accepted_at: str,
) -> dict[str, Any]:
    """Add v4 raw-boundary proof without replaying a v3 timeline source."""

    compact_records = parsed.get("compact_records")
    if not isinstance(compact_records, list):
        raise SourceSchemaError(
            f"FTD boundary reparse returned no compact records: {source_url}"
        )
    parsed_cusip_count = len({
        str(record.get("cusip") or "")
        for record in compact_records
        if isinstance(record, Mapping) and record.get("cusip")
    })
    if (
        prior.get("sha256") != source_sha256
        or parsed.get("raw_record_count") != prior.get("raw_record_count")
        or len(compact_records) != prior.get("record_count")
        or len(compact_records) != prior.get("matched_record_count")
        or parsed_cusip_count != prior.get("matched_cusip_count")
    ):
        raise SourceSchemaError(
            "reparsed FTD boundary archive differs from the retained v3 "
            f"projection; run a clean security-master rebuild: {source_url}"
        )

    _validate_reparsed_ftd_timeline_witnesses(
        state,
        source_url=source_url,
        source_sha256=source_sha256,
        compact_records=compact_records,
    )
    replacement = copy.deepcopy(dict(prior))
    replacement["accepted_at"] = accepted_at
    for field in (
        "raw_record_count",
        "first_settlement_date",
        "last_settlement_date",
        "observed_months",
        "boundary_date_proofs",
    ):
        replacement[field] = copy.deepcopy(parsed.get(field))
    replacement["date_inventory_complete"] = True
    _validate_ftd_archive_inventory(replacement, source_url=source_url)
    return replacement


def _validate_ftd_timeline(state: Mapping[str, Any]) -> None:
    """Validate compact intervals and bind every exact witness to an archive."""

    timeline = state.get("ftd_timeline")
    if not isinstance(timeline, dict) or list(timeline) != sorted(timeline):
        raise SecurityMasterError("SEC source state has an invalid FTD timeline")
    mutable_tail = state.get("ftd_mutable_tail")
    if (
        not isinstance(mutable_tail, dict)
        or list(mutable_tail)
        != sorted(mutable_tail, key=_ftd_url_sort_key)
        or len(mutable_tail) > 2
    ):
        raise SecurityMasterError("SEC source state has an invalid FTD mutable tail")
    for url, contribution in mutable_tail.items():
        source = state.get("sources", {}).get(url)
        if (
            not isinstance(contribution, dict)
            or set(contribution) != {"sha256", "records"}
            or not isinstance(source, Mapping)
            or source.get("kind") != "sec_ftd_archive"
            or contribution.get("sha256") != source.get("sha256")
        ):
            raise SecurityMasterError(
                f"FTD mutable tail is not bound to its archive: {url}"
            )
        _validate_compact_ftd_records(contribution.get("records"), source_url=url)
        covered = _archive_filter_universe(source, state)
        if covered is not None and any(
            record.get("cusip") not in covered
            for record in contribution["records"]
        ):
            raise SecurityMasterError(
                f"FTD mutable tail exceeds archive filter coverage: {url}"
            )
    source_references = {
        (url, str(source.get("sha256") or "")): source.get("kind")
        for url, source in state.get("sources", {}).items()
        if isinstance(source, Mapping)
    }
    for cusip, intervals in timeline.items():
        if normalize_cusip(cusip) != cusip or not cusip:
            raise SecurityMasterError("FTD timeline has an invalid CUSIP key")
        if not isinstance(intervals, list) or not intervals:
            raise SecurityMasterError(f"FTD timeline has no intervals: {cusip}")
        prior_last: str | None = None
        prior_symbols: list[str] | None = None
        for interval in intervals:
            if (
                not isinstance(interval, dict)
                or set(interval) != _FTD_TIMELINE_INTERVAL_FIELDS
                or interval.get("timeline_schema_version")
                != FTD_TIMELINE_SCHEMA_VERSION
            ):
                raise SecurityMasterError(
                    f"FTD timeline has a malformed interval: {cusip}"
                )
            symbols = interval.get("symbols")
            symbol = interval.get("symbol")
            first = interval.get("first_seen")
            last = interval.get("last_seen")
            dates = interval.get("observation_dates")
            observations = interval.get("observations")
            descriptions = interval.get("descriptions")
            symbol_descriptions = interval.get("symbol_descriptions")
            if (
                not isinstance(symbols, list)
                or not symbols
                or symbols != sorted(set(symbols))
                or any(_normalize_symbol(item) != item for item in symbols)
                or symbol != (symbols[0] if len(symbols) == 1 else None)
                or _parse_settlement_date(first) != first
                or _parse_settlement_date(last) != last
                or first > last
                or (prior_last is not None and first <= prior_last)
                or prior_symbols == symbols
                or type(interval.get("observation_date_count")) is not int
                or interval["observation_date_count"] < 1
                or type(interval.get("observation_count")) is not int
                or interval["observation_count"]
                < interval["observation_date_count"]
                or not isinstance(dates, list)
                or not dates
                or dates != sorted(set(dates))
                or len(dates) > FTD_MAX_RECENT_EXACT_DATES + 1
                or dates[0] != first
                or dates[-1] != last
                or interval["observation_date_count"] < len(dates)
                or not isinstance(observations, list)
                or not observations
                or not isinstance(descriptions, list)
                or descriptions != sorted(set(descriptions))
                or not isinstance(symbol_descriptions, dict)
                or list(symbol_descriptions) != sorted(symbol_descriptions)
                or set(symbol_descriptions) - set(symbols)
            ):
                raise SecurityMasterError(
                    f"FTD timeline has an invalid interval: {cusip}"
                )
            normalized_descriptions: set[str] = set()
            for described_symbol, values in symbol_descriptions.items():
                if (
                    _normalize_symbol(described_symbol) != described_symbol
                    or not isinstance(values, list)
                    or values != sorted(set(values))
                    or any(
                        not isinstance(value, str)
                        or not value
                        or " ".join(value.split()) != value
                        for value in values
                    )
                ):
                    raise SecurityMasterError(
                        f"FTD timeline has invalid descriptions: {cusip}"
                    )
                normalized_descriptions.update(values)
            if sorted(normalized_descriptions) != descriptions:
                raise SecurityMasterError(
                    f"FTD timeline description projection differs: {cusip}"
                )
            observed_dates: set[str] = set()
            observed_sources: dict[tuple[str, str], dict[str, str]] = {}
            prior_observation_key: tuple[str, str] | None = None
            for observation in observations:
                if (
                    not isinstance(observation, dict)
                    or set(observation) != _FTD_EXACT_OBSERVATION_FIELDS
                ):
                    raise SecurityMasterError(
                        f"FTD timeline has malformed exact evidence: {cusip}"
                    )
                observed_date = observation.get("settlement_date")
                observed_symbol = observation.get("symbol")
                key = (str(observed_date or ""), str(observed_symbol or ""))
                if (
                    _parse_settlement_date(observed_date) != observed_date
                    or observed_symbol not in symbols
                    or observed_date not in dates
                    or not first <= observed_date <= last
                    or type(observation.get("observation_count")) is not int
                    or observation["observation_count"] < 1
                    or prior_observation_key is not None
                    and key <= prior_observation_key
                ):
                    raise SecurityMasterError(
                        f"FTD timeline has invalid exact evidence: {cusip}"
                    )
                prior_observation_key = key
                observed_dates.add(observed_date)
                raw_descriptions = observation.get("descriptions")
                raw_sources = observation.get("sources")
                if (
                    not isinstance(raw_descriptions, list)
                    or raw_descriptions != sorted(set(raw_descriptions))
                    or any(item not in descriptions for item in raw_descriptions)
                    or not isinstance(raw_sources, list)
                    or not raw_sources
                ):
                    raise SecurityMasterError(
                        f"FTD timeline has invalid exact witness data: {cusip}"
                    )
                source_keys: list[tuple[str, str]] = []
                for source in raw_sources:
                    if not isinstance(source, dict) or set(source) != {"url", "sha256"}:
                        raise SecurityMasterError(
                            f"FTD timeline has malformed source proof: {cusip}"
                        )
                    source_key = (
                        normalize_sec_url(str(source.get("url") or "")),
                        str(source.get("sha256") or ""),
                    )
                    owned_start, owned_end = _ftd_archive_date_bounds(
                        source_key[0]
                    )
                    if (
                        source.get("url") != source_key[0]
                        or not _SHA256_RE.fullmatch(source_key[1])
                        or source_references.get(source_key) != "sec_ftd_archive"
                        or source_key[0] in mutable_tail
                        or not (
                            owned_start.isoformat()
                            <= observed_date
                            <= owned_end.isoformat()
                        )
                    ):
                        raise SecurityMasterError(
                            f"FTD timeline witness is not bound to an SEC archive: {cusip}"
                        )
                    source_keys.append(source_key)
                    observed_sources[source_key] = {
                        "url": source_key[0],
                        "sha256": source_key[1],
                    }
                if source_keys != sorted(set(source_keys)):
                    raise SecurityMasterError(
                        f"FTD timeline witness sources are not ordered: {cusip}"
                    )
            if observed_dates != set(dates) or interval.get("sources") != [
                observed_sources[key] for key in sorted(observed_sources)
            ]:
                raise SecurityMasterError(
                    f"FTD timeline bounded projections differ: {cusip}"
                )
            prior_last = last
            prior_symbols = symbols


def _validate_official_13f_records(
    entry: Mapping[str, Any],
    *,
    source_url: str,
) -> None:
    """Validate the complete normalized contract for one official-list file."""

    records = entry.get("records")
    if not isinstance(records, list):
        raise SecurityMasterError(
            "official 13F-list entry has no records list"
        )
    match = _OFFICIAL_13F_LIST_RE.fullmatch(
        Path(urlparse(source_url).path).name
    )
    expected_period = (
        f"{match.group('year')}Q{match.group('quarter')}" if match else None
    )
    if expected_period is None or entry.get("list_period") != expected_period:
        raise SecurityMasterError(
            f"official 13F-list period does not match its URL: {source_url}"
        )
    if type(entry.get("record_count")) is not int or entry.get(
        "record_count"
    ) != len(records):
        raise SecurityMasterError(
            f"official 13F-list record count mismatch: {source_url}"
        )

    prior_key: tuple[str, str, str, str, str] | None = None
    for record in records:
        if not isinstance(record, dict) or set(record) != _OFFICIAL_13F_RECORD_FIELDS:
            raise SecurityMasterError(
                f"official 13F-list has a malformed record: {source_url}"
            )
        cusip = normalize_cusip(record.get("cusip"))
        issuer = record.get("issuer")
        description = record.get("description")
        option_indicator = record.get("option_indicator")
        status = record.get("status")
        if (
            not cusip
            or cusip != record.get("cusip")
            or not isinstance(issuer, str)
            or not issuer
            or " ".join(issuer.split()) != issuer
            or not isinstance(description, str)
            or " ".join(description.split()) != description
            or option_indicator not in {"", "*"}
            or status not in {"", "*A*", "*D*"}
        ):
            raise SecurityMasterError(
                f"official 13F-list has invalid normalized metadata: {source_url}"
            )
        record_key = (
            cusip,
            description,
            issuer,
            status,
            option_indicator,
        )
        if prior_key is not None and record_key <= prior_key:
            raise SecurityMasterError(
                f"official 13F-list records are not unique and ordered: {source_url}"
            )
        prior_key = record_key


def _validate_symbol_metadata_entry(
    entry: Mapping[str, Any],
    *,
    source_url: str,
) -> None:
    """Validate current-symbol membership and its normalized issuer metadata."""

    symbols = entry.get("symbols")
    if (
        not isinstance(symbols, list)
        or not symbols
        or symbols != sorted(set(symbols))
        or any(_normalize_symbol(symbol) != symbol for symbol in symbols)
        or type(entry.get("symbol_count")) is not int
        or entry.get("symbol_count") != len(symbols)
    ):
        raise SecurityMasterError(
            f"SEC validation entry has invalid symbol membership: {source_url}"
        )
    symbol_set = set(symbols)
    for field, label in (
        ("symbol_titles", "title"),
        ("symbol_exchanges", "exchange"),
    ):
        metadata = entry.get(field)
        if not isinstance(metadata, dict):
            raise SecurityMasterError(
                f"SEC validation entry has invalid symbol-{label} metadata"
            )
        for raw_symbol, raw_values in metadata.items():
            symbol = _normalize_symbol(raw_symbol)
            if (
                symbol != raw_symbol
                or symbol not in symbol_set
                or not isinstance(raw_values, list)
                or not raw_values
                or raw_values != sorted(set(raw_values))
                or any(
                    not isinstance(value, str)
                    or not value
                    or " ".join(value.split()) != value
                    for value in raw_values
                )
            ):
                raise SecurityMasterError(
                    f"SEC validation entry has invalid symbol-{label} metadata: "
                    f"{source_url}"
                )

    if entry.get("kind") == "sec_fund_tickers":
        fund_records = entry.get("fund_records")
        if "fund_records" in entry:
            expected: list[tuple[str, str, str, str]] = []
            for record in fund_records if isinstance(fund_records, list) else []:
                if not isinstance(record, dict) or set(record) != {
                    "symbol",
                    "cik",
                    "series_id",
                    "class_id",
                }:
                    raise SecurityMasterError(
                        "SEC fund-symbol entry has malformed series/class records"
                    )
                symbol = _normalize_symbol(record.get("symbol"))
                cik = str(record.get("cik") or "")
                series_id = str(record.get("series_id") or "")
                class_id = str(record.get("class_id") or "")
                if (
                    symbol != record.get("symbol")
                    or symbol not in symbol_set
                    or not re.fullmatch(r"\d{10}", cik)
                    or int(cik) < 1
                    or not re.fullmatch(r"S\d+", series_id)
                    or not re.fullmatch(r"C\d+", class_id)
                ):
                    raise SecurityMasterError(
                        "SEC fund-symbol entry has invalid series/class records"
                    )
                expected.append((symbol, cik, series_id, class_id))
            if not expected or expected != sorted(set(expected)):
                raise SecurityMasterError(
                    "SEC fund-symbol series/class records are not unique and ordered"
                )


def sec_fund_series_url(cik: object | None) -> str:
    """Return the canonical SEC registrant series/class page URL."""

    raw = str(cik or "").strip()
    if not raw.isdigit() or int(raw) < 1:
        raise SecurityMasterError("SEC fund-series CIK must be a positive integer")
    normalized = raw.zfill(10)
    return (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={normalized}&scd=series"
    )


def _validate_fund_series_source(
    entry: Mapping[str, Any],
    *,
    source_url: str,
) -> None:
    cik = str(entry.get("cik") or "")
    if not re.fullmatch(r"\d{10}", cik) or int(cik) < 1:
        raise SecurityMasterError("SEC fund-series source has invalid CIK")
    if source_url != sec_fund_series_url(cik):
        raise SecurityMasterError("SEC fund-series source URL does not match CIK")
    for field, prefix in (("series_names", "S"), ("class_names", "C")):
        names = entry.get(field)
        if not isinstance(names, dict):
            raise SecurityMasterError(f"SEC fund-series source lacks {field}")
        if list(names) != sorted(names):
            raise SecurityMasterError(f"SEC fund-series {field} is not ordered")
        for identifier, name in names.items():
            if (
                not re.fullmatch(rf"{prefix}\d+", str(identifier))
                or not isinstance(name, str)
                or not name
                or " ".join(name.split()) != name
            ):
                raise SecurityMasterError(
                    f"SEC fund-series source has invalid {field}"
                )
    if not entry.get("series_names"):
        raise SecurityMasterError("SEC fund-series source has no series names")


def _edgar_discovery_successful_check_at(
    record: Mapping[str, Any],
    *,
    schema_version: int,
) -> str | None:
    """Return the last complete EDGAR check without upgrading persisted state."""

    if schema_version >= EDGAR_DISCOVERY_SCHEMA_VERSION:
        value = record.get("last_successful_check_at")
        return str(value) if _is_canonical_utc_timestamp(value) else None
    checked_at = record.get("checked_at")
    if (
        record.get("terminal") is True
        and record.get("status") != "transient_error"
        and _is_canonical_utc_timestamp(checked_at)
    ):
        return str(checked_at)
    return None


def _validate_edgar_discovery_state(discovery: object) -> None:
    """Validate embedded v1/v2 EDGAR diagnostics without read-time mutation."""

    if discovery == {}:
        return
    if not isinstance(discovery, dict):
        raise SecurityMasterError("SEC source state has invalid EDGAR diagnostics")
    schema_version = discovery.get("schema_version")
    if schema_version is None:
        # Early source-state v2 snapshots admitted opaque diagnostics before
        # the embedded discovery contract was versioned. Keep those snapshots
        # byte-stable and readable, but do not treat any opaque clock as a
        # successful revalidation checkpoint.
        return
    if schema_version not in {
        LEGACY_EDGAR_DISCOVERY_SCHEMA_VERSION,
        EDGAR_DISCOVERY_SCHEMA_VERSION,
    }:
        raise SecurityMasterError(
            "unsupported SEC EDGAR discovery schema version"
        )
    records = discovery.get("records")
    fetched_sources = discovery.get("fetched_sources")
    if (
        not isinstance(records, dict)
        or list(records) != sorted(records)
        or not isinstance(fetched_sources, dict)
        or list(fetched_sources) != sorted(fetched_sources)
    ):
        raise SecurityMasterError("malformed SEC EDGAR discovery state")

    required_record_fields = {
        "cusip",
        "status",
        "terminal",
        "reason",
        "issuer_cik",
        "security_class",
        "schedule_candidate_count",
        "exact_schedule_count",
        "periodic_candidate_count",
        "source_accessions",
        "record_sha256",
        "checked_at",
    }
    if schema_version >= EDGAR_DISCOVERY_SCHEMA_VERSION:
        required_record_fields.add("last_successful_check_at")
    valid_statuses = {"sources_found", "no_evidence", "conflict", "transient_error"}
    for cusip, record in records.items():
        if (
            not isinstance(record, dict)
            or set(record) != required_record_fields
            or cusip_quarantine_reason(cusip) is not None
            or record.get("cusip") != cusip
            or record.get("status") not in valid_statuses
            or type(record.get("terminal")) is not bool
            or record.get("terminal")
            != (record.get("status") != "transient_error")
            or not isinstance(record.get("reason"), str)
            or not record.get("reason")
            or not _SHA256_RE.fullmatch(str(record.get("record_sha256") or ""))
            or not _is_canonical_utc_timestamp(record.get("checked_at"))
        ):
            raise SecurityMasterError(
                f"invalid SEC EDGAR discovery record: {cusip}"
            )
        for field in (
            "schedule_candidate_count",
            "exact_schedule_count",
            "periodic_candidate_count",
        ):
            if type(record.get(field)) is not int or record[field] < 0:
                raise SecurityMasterError(
                    f"invalid SEC EDGAR discovery count: {cusip}"
                )
        issuer_cik = record.get("issuer_cik")
        security_class = record.get("security_class")
        accessions = record.get("source_accessions")
        if (
            issuer_cik is not None
            and not re.fullmatch(r"\d{10}", str(issuer_cik))
        ) or (
            security_class is not None
            and (
                not isinstance(security_class, str)
                or not security_class
                or " ".join(security_class.split()) != security_class
            )
        ) or (
            not isinstance(accessions, list)
            or accessions != sorted(set(accessions))
            or any(not _SEC_ACCESSION_RE.fullmatch(str(item)) for item in accessions)
        ):
            raise SecurityMasterError(
                f"invalid SEC EDGAR discovery identity metadata: {cusip}"
            )
        if schema_version >= EDGAR_DISCOVERY_SCHEMA_VERSION:
            successful_at = record.get("last_successful_check_at")
            if successful_at is not None and (
                not _is_canonical_utc_timestamp(successful_at)
                or str(successful_at) > str(record["checked_at"])
            ):
                raise SecurityMasterError(
                    f"invalid SEC EDGAR successful-check timestamp: {cusip}"
                )

    from sec_edgar_evidence import normalize_sec_discovery_url

    valid_fetch_kinds = {
        "sec_cusip_search",
        "sec_submissions",
        "schedule_13dg",
        "periodic_ixbrl",
    }
    for url, fetched in fetched_sources.items():
        try:
            canonical_url = normalize_sec_discovery_url(str(url))
        except Exception as exc:
            raise SecurityMasterError(
                f"invalid SEC EDGAR discovery URL: {url}"
            ) from exc
        outcome = fetched.get("outcome") if isinstance(fetched, dict) else None
        sha256 = fetched.get("sha256") if isinstance(fetched, dict) else None
        if (
            canonical_url != url
            or not isinstance(fetched, dict)
            or set(fetched) != {"kind", "url", "outcome", "sha256"}
            or fetched.get("url") != url
            or fetched.get("kind") not in valid_fetch_kinds
            or outcome not in {"fetched", "transient_error"}
            or (
                outcome == "fetched"
                and not _SHA256_RE.fullmatch(str(sha256 or ""))
            )
            or (outcome == "transient_error" and sha256 is not None)
        ):
            raise SecurityMasterError(
                f"invalid SEC EDGAR discovery fetch record: {url}"
            )


def _validate_source_state(state: Mapping[str, Any]) -> None:
    required_fields = set(empty_source_state())
    allowed_fields = required_fields | {"ftd_processed_all_cusips"}
    actual_fields = set(state)
    if actual_fields != required_fields and not (
        actual_fields == allowed_fields
        and state.get("ftd_processed_all_cusips") is True
    ):
        raise SecurityMasterError(
            "SEC source state has unexpected or missing top-level fields"
        )
    if state.get("schema_version") != SOURCE_STATE_SCHEMA_VERSION:
        raise SecurityMasterError("unsupported SEC source-state schema version")
    updated_at = state.get("updated_at")
    if updated_at is not None and not _is_canonical_utc_timestamp(updated_at):
        raise SecurityMasterError("SEC source state has an invalid updated_at")
    filter_universes = state.get("filter_universes")
    if not isinstance(filter_universes, dict):
        raise SecurityMasterError(
            "SEC source state must contain filter-universe profiles"
        )
    normalized_profiles: dict[str, set[str]] = {}
    for digest, profile in filter_universes.items():
        if (
            not _SHA256_RE.fullmatch(str(digest))
            or not isinstance(profile, dict)
            or set(profile) != {"cusips", "count"}
            or not isinstance(profile.get("cusips"), list)
            or profile.get("cusips")
            != _normalized_filter_universe(profile.get("cusips", []))
            or profile.get("count") != len(profile.get("cusips", []))
            or _filter_universe_sha256(profile.get("cusips", [])) != digest
        ):
            raise SecurityMasterError(
                f"SEC source state has invalid filter-universe profile: {digest}"
            )
        normalized_profiles[digest] = set(profile["cusips"])
    filter_sequence = state.get("ftd_filter_cusips")
    if (
        not isinstance(filter_sequence, list)
        or any(
            normalize_cusip(cusip) != cusip or not cusip
            for cusip in filter_sequence
        )
        or len(filter_sequence) != len(set(filter_sequence))
    ):
        raise SecurityMasterError("SEC source state has an invalid FTD filter log")
    current_digest = state.get("current_filter_universe_sha256")
    current_count = state.get("current_filter_universe_count")
    if current_digest is None:
        if current_count != 0:
            raise SecurityMasterError(
                "empty current filter universe has a non-zero count"
            )
    elif (
        not isinstance(current_digest, str)
        or current_digest not in normalized_profiles
        or current_count != len(normalized_profiles[current_digest])
    ):
        raise SecurityMasterError("current filter universe is not registered")
    processed_all = state.get("ftd_processed_all_cusips")
    processed_digest = state.get("ftd_processed_filter_universe_sha256")
    processed_count = state.get("ftd_processed_filter_universe_count")
    if processed_all is True:
        if processed_digest is not None or processed_count != 0:
            raise SecurityMasterError(
                "unfiltered FTD timeline has a processed-universe digest"
            )
    elif processed_all is not None:
        raise SecurityMasterError("invalid FTD processed-universe mode")
    elif processed_digest is None:
        if processed_count != 0:
            raise SecurityMasterError(
                "empty FTD processed universe has a non-zero count"
            )
    elif (
        not isinstance(processed_digest, str)
        or processed_digest not in normalized_profiles
        or processed_count != len(normalized_profiles[processed_digest])
    ):
        raise SecurityMasterError("FTD processed universe is not registered")
    required_urls = state.get("required_filter_coverage_urls")
    if (
        not isinstance(required_urls, list)
        or any(not isinstance(url, str) for url in required_urls)
        or required_urls
        != sorted(set(required_urls), key=_ftd_url_sort_key)
    ):
        raise SecurityMasterError(
            "SEC source state has invalid required filter-coverage URLs"
        )
    for required_url in required_urls:
        normalize_sec_url(str(required_url))
        if not _FTD_ARCHIVE_RE.fullmatch(
            Path(urlparse(required_url).path).name
        ):
            raise SecurityMasterError(
                "required filter-coverage URL is not an FTD archive"
            )
    edgar_evidence = state.get("edgar_evidence", {})
    if not isinstance(edgar_evidence, dict):
        raise SecurityMasterError("SEC source state has invalid EDGAR evidence")
    if edgar_evidence:
        try:
            from sec_edgar_evidence import validate_sec_edgar_evidence_cache

            validate_sec_edgar_evidence_cache(edgar_evidence)
        except Exception as exc:
            raise SecurityMasterError(
                f"SEC source state has invalid EDGAR evidence: {exc}"
            ) from exc
    _validate_edgar_discovery_state(state.get("edgar_discovery", {}))

    sources = state.get("sources")
    if not isinstance(sources, dict):
        raise SecurityMasterError("SEC source state must contain a sources object")
    ftd_archive_urls: list[str] = []
    for url, entry in sources.items():
        canonical_url = normalize_sec_url(str(url))
        if canonical_url != url or not isinstance(entry, dict):
            raise SecurityMasterError("SEC source state has a malformed source entry")
        if entry.get("url") != url:
            raise SecurityMasterError(f"SEC source entry URL mismatch: {url}")
        if entry.get("kind") not in _SOURCE_KINDS:
            raise SecurityMasterError(f"SEC source entry has invalid kind: {url}")
        kind = entry["kind"]
        base_fields = {"url", "kind", "sha256", "accepted_at"}
        payload_fields = {
            "sec_ftd_index": {"discovered_urls"},
            "sec_13f_list_index": {"discovered_urls"},
            "sec_13f_list": {"records", "record_count", "list_period"},
            "sec_company_tickers": {
                "symbols",
                "symbol_titles",
                "symbol_exchanges",
                "symbol_count",
            },
            "sec_company_exchange_tickers": {
                "symbols",
                "symbol_titles",
                "symbol_exchanges",
                "symbol_count",
            },
            "sec_fund_tickers": {
                "symbols",
                "symbol_titles",
                "symbol_exchanges",
                "symbol_count",
            },
            "sec_fund_series": {"cik", "series_names", "class_names"},
        }
        if kind == "sec_ftd_archive":
            if entry.get("filter_all_cusips") is True:
                required_entry_fields = (
                    set(_FTD_ARCHIVE_INVENTORY_FIELDS)
                    - {"filter_universe_sha256", "filter_universe_count"}
                ) | {"filter_all_cusips"}
            else:
                required_entry_fields = set(_FTD_ARCHIVE_INVENTORY_FIELDS)
        else:
            required_entry_fields = base_fields | payload_fields[kind]
        optional_entry_fields: set[str] = set()
        if kind in _REQUIRED_CURRENT_SOURCE_KINDS | {"sec_fund_series"}:
            optional_entry_fields.add("last_successful_check_at")
        if kind == "sec_fund_tickers":
            optional_entry_fields.add("fund_records")
        if (
            not required_entry_fields.issubset(entry)
            or set(entry) - required_entry_fields - optional_entry_fields
        ):
            raise SecurityMasterError(
                f"SEC source entry has unexpected or missing fields: {url}"
            )
        if not _SHA256_RE.fullmatch(str(entry.get("sha256") or "")):
            raise SecurityMasterError(f"SEC source entry has invalid sha256: {url}")
        expected_fixed_url = {
            "sec_ftd_index": FTD_PAGE_URL,
            "sec_13f_list_index": OFFICIAL_13F_LIST_PAGE_URL,
            "sec_company_tickers": SEC_COMPANY_TICKERS_URL,
            "sec_company_exchange_tickers": SEC_COMPANY_EXCHANGE_TICKERS_URL,
            "sec_fund_tickers": SEC_FUND_TICKERS_URL,
        }.get(kind)
        if expected_fixed_url is not None and url != expected_fixed_url:
            raise SecurityMasterError(
                f"SEC source kind does not match its canonical URL: {url}"
            )
        if not _is_canonical_utc_timestamp(entry.get("accepted_at")):
            raise SecurityMasterError(
                f"SEC source entry has invalid accepted_at: {url}"
            )
        last_successful_check_at = entry.get("last_successful_check_at")
        if "last_successful_check_at" in entry and not (
            _is_canonical_utc_timestamp(last_successful_check_at)
        ):
            raise SecurityMasterError(
                f"SEC source entry has invalid successful-check timestamp: {url}"
            )
        if kind in {"sec_ftd_index", "sec_13f_list_index"}:
            discovered_urls = entry.get("discovered_urls")
            if not isinstance(discovered_urls, list) or not discovered_urls:
                raise SecurityMasterError(
                    "SEC discovery entry has no discovered URL list"
                )
            normalized_discovered = [
                normalize_sec_url(str(discovered_url))
                for discovered_url in discovered_urls
            ]
            if kind == "sec_ftd_index":
                if (
                    discovered_urls
                    != sorted(set(normalized_discovered), key=_ftd_url_sort_key)
                    or any(
                        not _FTD_ARCHIVE_RE.fullmatch(
                            Path(urlparse(discovered_url).path).name
                        )
                        for discovered_url in discovered_urls
                    )
                ):
                    raise SecurityMasterError(
                        "SEC FTD discovery URLs are invalid or not ordered"
                    )
                _ftd_urls_by_period(
                    discovered_urls,
                    context="SEC FTD discovery state",
                )
            elif (
                len(discovered_urls) != 1
                or discovered_urls != normalized_discovered
                or not _OFFICIAL_13F_LIST_RE.fullmatch(
                    Path(urlparse(discovered_urls[0]).path).name
                )
            ):
                raise SecurityMasterError(
                    "SEC official-list discovery URL is invalid"
                )
        elif kind == "sec_ftd_archive":
            if not _FTD_ARCHIVE_RE.fullmatch(Path(urlparse(url).path).name):
                raise SecurityMasterError(
                    f"SEC FTD archive kind has an invalid URL: {url}"
                )
            ftd_archive_urls.append(url)
            _validate_ftd_archive_inventory(entry, source_url=url)
            if entry.get("filter_all_cusips") is True:
                if entry.get("filter_universe_sha256") is not None:
                    raise SecurityMasterError(
                        f"unfiltered FTD archive has a filter digest: {url}"
                    )
            else:
                digest = entry.get("filter_universe_sha256")
                coverage_count = entry.get("filter_universe_count")
                covered_prefix = (
                    filter_sequence[:coverage_count]
                    if type(coverage_count) is int
                    and 0 <= coverage_count <= len(filter_sequence)
                    else None
                )
                if (
                    covered_prefix is None
                    or digest != _filter_universe_sha256(covered_prefix)
                ):
                    raise SecurityMasterError(
                        f"FTD archive has invalid filter coverage: {url}"
                    )
        elif kind == "sec_13f_list":
            _validate_official_13f_records(entry, source_url=url)
        elif kind == "sec_fund_series":
            _validate_fund_series_source(entry, source_url=url)
        else:
            _validate_symbol_metadata_entry(entry, source_url=url)
    _ftd_urls_by_period(
        ftd_archive_urls,
        context="SEC FTD source state",
    )
    _validate_ftd_boundary_duplicate_proofs(state)
    _validate_ftd_timeline(state)


def _load_source_state_unlocked(path: Path) -> dict[str, Any]:
    payload = _read_json_object(path)
    if not payload:
        return empty_source_state()
    normalized = _normalize_source_state(payload)
    _validate_source_state(normalized)
    return normalized


def load_source_state(
    path: Path = DEFAULT_SOURCE_STATE_PATH,
) -> dict[str, Any]:
    """Load source evidence after recovering any interrupted pair publish."""

    path = Path(path)
    _recover_security_master_pair_for_path(path)
    return _load_source_state_unlocked(path)


def save_source_state(
    state: Mapping[str, Any],
    path: Path = DEFAULT_SOURCE_STATE_PATH,
) -> None:
    """Save a non-publishable staging/checkpoint state file.

    Authoritative state must be published with :func:`save_security_master_pair`.
    """

    normalized = _normalize_source_state(state)
    _validate_source_state(normalized)
    _atomic_write_json(Path(path), normalized)


def source_state_sha256(state: Mapping[str, Any]) -> str:
    """Return the canonical digest used to bind a master to its source state."""

    normalized = _normalize_source_state(state)
    _validate_source_state(normalized)
    return _mapping_sha256(normalized)


def _validate_master_sources(
    master: Mapping[str, Any],
) -> tuple[dict[tuple[str, str], str], set[str]]:
    """Return cryptographic source references after validating provenance."""

    raw_sources = master.get("sources")
    if not isinstance(raw_sources, list):
        raise SecurityMasterError("security master must contain a sources list")
    expected_order = sorted(
        raw_sources,
        key=lambda source: (
            str(source.get("url") or "")
            if isinstance(source, Mapping)
            else "",
            str(source.get("kind") or "")
            if isinstance(source, Mapping)
            else "",
            str(source.get("sha256") or "")
            if isinstance(source, Mapping)
            else "",
        ),
    )
    if raw_sources != expected_order:
        raise SecurityMasterError("master source provenance is not ordered")

    references: dict[tuple[str, str], str] = {}
    kinds: set[str] = set()
    urls: set[str] = set()
    ftd_archive_urls: list[str] = []
    for source in raw_sources:
        if not isinstance(source, dict):
            raise SecurityMasterError("master source provenance must be an object")
        kind = source.get("kind")
        expected_fields = (
            {"url", "sha256", "kind", "schema_sha256"}
            if kind in _SOURCE_KINDS
            else {"url", "sha256", "kind"}
        )
        url = normalize_sec_url(str(source.get("url") or ""))
        sha256 = str(source.get("sha256") or "")
        if (
            kind not in _MASTER_PROVENANCE_SOURCE_KINDS
            or set(source) != expected_fields
            or source.get("url") != url
            or not _SHA256_RE.fullmatch(sha256)
            or url in urls
        ):
            raise SecurityMasterError("master source provenance is invalid")
        if kind in _SOURCE_KINDS and not _SHA256_RE.fullmatch(
            str(source.get("schema_sha256") or "")
        ):
            raise SecurityMasterError(
                "master source provenance has an invalid schema hash"
            )
        if kind == "sec_ftd_archive" and not _FTD_ARCHIVE_RE.fullmatch(
            Path(urlparse(url).path).name
        ):
            raise SecurityMasterError(
                "master FTD source provenance has an invalid archive URL"
            )
        if kind == "sec_ftd_archive":
            ftd_archive_urls.append(url)
        references[(url, sha256)] = str(kind)
        kinds.add(str(kind))
        urls.add(url)
    _ftd_urls_by_period(
        ftd_archive_urls,
        context="security-master FTD source provenance",
    )
    return references, kinds


def _validate_symbol_evidence(
    symbol_evidence: list[Any],
    *,
    key: str,
    source_references: Mapping[tuple[str, str], str],
) -> None:
    expected_fields = {
        "settlement_date",
        "symbol",
        "observation_count",
        "descriptions",
        "sources",
    }
    prior_key: tuple[str, str] | None = None
    for item in symbol_evidence:
        if not isinstance(item, dict) or set(item) != expected_fields:
            raise SecurityMasterError(
                f"security-master symbol evidence is malformed: {key}"
            )
        settlement_date = item.get("settlement_date")
        symbol = item.get("symbol")
        descriptions = item.get("descriptions")
        sources = item.get("sources")
        evidence_key = (str(settlement_date or ""), str(symbol or ""))
        if (
            _parse_settlement_date(settlement_date) != settlement_date
            or _normalize_symbol(symbol) != symbol
            or type(item.get("observation_count")) is not int
            or item.get("observation_count") < 1
            or not isinstance(descriptions, list)
            or descriptions != sorted(set(descriptions))
            or any(
                not isinstance(description, str)
                or not description
                or " ".join(description.split()) != description
                for description in descriptions
            )
            or not isinstance(sources, list)
            or not sources
        ):
            raise SecurityMasterError(
                f"security-master symbol evidence is invalid: {key}"
            )
        if prior_key is not None and evidence_key <= prior_key:
            raise SecurityMasterError(
                f"security-master symbol evidence is not unique and ordered: {key}"
            )
        prior_key = evidence_key
        source_keys: list[tuple[str, str]] = []
        for source in sources:
            if not isinstance(source, dict) or set(source) != {"url", "sha256"}:
                raise SecurityMasterError(
                    f"security-master symbol evidence source is malformed: {key}"
                )
            url = normalize_sec_url(str(source.get("url") or ""))
            sha256 = str(source.get("sha256") or "")
            source_key = (url, sha256)
            if (
                source.get("url") != url
                or not _SHA256_RE.fullmatch(sha256)
                or source_references.get(source_key) != "sec_ftd_archive"
            ):
                raise SecurityMasterError(
                    f"security-master symbol evidence is not bound to an SEC FTD "
                    f"source: {key}"
                )
            source_keys.append(source_key)
        if source_keys != sorted(set(source_keys)):
            raise SecurityMasterError(
                f"security-master symbol evidence sources are not unique and "
                f"ordered: {key}"
            )


def _validate_master_symbol_intervals(
    intervals: object,
    *,
    key: str,
    symbol_evidence: list[Any],
    source_references: Mapping[tuple[str, str], str],
) -> None:
    if not isinstance(intervals, list):
        raise SecurityMasterError(
            f"security-master symbol intervals are not a list: {key}"
        )
    evidence_keys = {
        (item.get("settlement_date"), item.get("symbol"))
        for item in symbol_evidence
        if isinstance(item, Mapping)
    }
    prior_last: str | None = None
    prior_symbols: list[str] | None = None
    interval_evidence_keys: set[tuple[object, object]] = set()
    for interval in intervals:
        if not isinstance(interval, dict) or set(interval) not in {
            _FTD_MASTER_INTERVAL_FIELDS,
            _LEGACY_FTD_MASTER_INTERVAL_FIELDS,
        }:
            raise SecurityMasterError(
                f"security-master symbol interval is malformed: {key}"
            )
        legacy_interval = set(interval) == _LEGACY_FTD_MASTER_INTERVAL_FIELDS
        symbols = (
            [interval.get("symbol")]
            if legacy_interval
            else interval.get("symbols")
        )
        symbol = interval.get("symbol")
        first = interval.get("first_seen")
        last = interval.get("last_seen")
        dates = interval.get("observation_dates")
        descriptions = interval.get("descriptions")
        symbol_descriptions = (
            {str(symbol): interval.get("descriptions", [])}
            if legacy_interval and symbol
            else interval.get("symbol_descriptions")
        )
        if (
            not isinstance(symbols, list)
            or not symbols
            or symbols != sorted(set(symbols))
            or any(_normalize_symbol(item) != item for item in symbols)
            or symbol != (symbols[0] if len(symbols) == 1 else None)
            or _parse_settlement_date(first) != first
            or _parse_settlement_date(last) != last
            or first > last
            or prior_last is not None
            and first <= prior_last
            or prior_symbols == symbols
            or type(interval.get("observation_date_count")) is not int
            or interval["observation_date_count"] < 1
            or type(interval.get("observation_count")) is not int
            or interval["observation_count"] < interval["observation_date_count"]
            or not isinstance(dates, list)
            or not dates
            or dates != sorted(set(dates))
            or (
                not legacy_interval
                and len(dates) > FTD_MAX_RECENT_EXACT_DATES + 1
            )
            or dates[0] != first
            or dates[-1] != last
            or interval["observation_date_count"] < len(dates)
            or not isinstance(descriptions, list)
            or descriptions != sorted(set(descriptions))
            or not isinstance(symbol_descriptions, dict)
            or list(symbol_descriptions) != sorted(symbol_descriptions)
            or set(symbol_descriptions) - set(symbols)
        ):
            raise SecurityMasterError(
                f"security-master symbol interval is invalid: {key}"
            )
        flattened_descriptions: set[str] = set()
        for described_symbol, values in symbol_descriptions.items():
            if (
                _normalize_symbol(described_symbol) != described_symbol
                or not isinstance(values, list)
                or values != sorted(set(values))
                or any(
                    not isinstance(value, str)
                    or not value
                    or " ".join(value.split()) != value
                    for value in values
                )
            ):
                raise SecurityMasterError(
                    f"security-master interval descriptions are invalid: {key}"
                )
            flattened_descriptions.update(values)
        if descriptions != sorted(flattened_descriptions):
            raise SecurityMasterError(
                f"security-master interval description projection differs: {key}"
            )
        raw_sources = interval.get("sources")
        if not isinstance(raw_sources, list) or not raw_sources:
            raise SecurityMasterError(
                f"security-master interval lacks source evidence: {key}"
            )
        source_keys: list[tuple[str, str]] = []
        for source in raw_sources:
            if not isinstance(source, dict) or set(source) != {"url", "sha256"}:
                raise SecurityMasterError(
                    f"security-master interval source is malformed: {key}"
                )
            source_key = (
                normalize_sec_url(str(source.get("url") or "")),
                str(source.get("sha256") or ""),
            )
            if (
                source.get("url") != source_key[0]
                or not _SHA256_RE.fullmatch(source_key[1])
                or source_references.get(source_key) != "sec_ftd_archive"
            ):
                raise SecurityMasterError(
                    f"security-master interval is not bound to SEC FTD: {key}"
                )
            source_keys.append(source_key)
        if source_keys != sorted(set(source_keys)):
            raise SecurityMasterError(
                f"security-master interval sources are not ordered: {key}"
            )
        for settlement_date in dates:
            for observed_symbol in symbols:
                evidence_key = (settlement_date, observed_symbol)
                if evidence_key in evidence_keys:
                    interval_evidence_keys.add(evidence_key)
        prior_last = last
        prior_symbols = symbols
    if not evidence_keys.issubset(interval_evidence_keys):
        raise SecurityMasterError(
            f"security-master exact evidence is outside compact intervals: {key}"
        )


def _validate_ftd_resolution_proof(
    entry: Mapping[str, Any],
    *,
    key: str,
    master: Mapping[str, Any],
    source_kinds: set[str],
) -> None:
    ticker = str(entry["ticker"])
    ticker_as_of = str(entry["ticker_as_of"])
    confirmation_dates = entry.get("confirmation_dates")
    validation_sources = entry.get("symbol_validation_sources")
    validation_titles = entry.get("symbol_validation_titles")
    minimum_dates = master.get("policy", {}).get(
        "min_confirmation_dates",
        DEFAULT_MIN_CONFIRMATION_DATES,
    )
    if (
        entry.get("mapping_method")
        != "exact_ftd_symbol_with_sec_metadata_validation"
        or type(minimum_dates) is not int
        or minimum_dates < 1
        or not isinstance(confirmation_dates, list)
        or len(confirmation_dates) < minimum_dates
        or confirmation_dates != sorted(set(confirmation_dates))
        or any(_parse_settlement_date(value) != value for value in confirmation_dates)
        or confirmation_dates[-1] != ticker_as_of
        or not isinstance(validation_sources, list)
        or not validation_sources
        or validation_sources != sorted(set(validation_sources))
        or any(kind not in _VALIDATION_SOURCE_KINDS for kind in validation_sources)
        or not set(validation_sources).issubset(source_kinds)
        or not isinstance(validation_titles, list)
        or not validation_titles
        or validation_titles != sorted(set(validation_titles))
        or any(
            not isinstance(title, str)
            or not title
            or " ".join(title.split()) != title
            for title in validation_titles
        )
    ):
        raise SecurityMasterError(
            f"resolved FTD record lacks exact SEC validation proof: {key}"
        )
    evidence_by_date = {
        item["settlement_date"]: item
        for item in entry.get("symbol_evidence", [])
        if item.get("symbol") == ticker
    }
    if any(value not in evidence_by_date for value in confirmation_dates):
        raise SecurityMasterError(
            f"resolved FTD confirmations lack cryptographic evidence: {key}"
        )
    active_intervals = [
        interval
        for interval in entry.get("symbol_intervals", [])
        if interval.get("symbol") == ticker
        and ticker_as_of in interval.get("observation_dates", [])
    ]
    if (
        len(active_intervals) != 1
        or entry.get("effective_from") != active_intervals[0].get("first_seen")
    ):
        raise SecurityMasterError(
            f"resolved FTD effective date is not the active symbol interval: {key}"
        )


def _sec_edgar_normalized_text(value: object | None) -> str:
    """Mirror the EDGAR parser's presentation-only text normalization."""

    text = unicodedata.normalize("NFKC", unescape(str(value or "")))
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _sec_edgar_issuer_names_compatible(
    left: object | None,
    right: object | None,
) -> bool:
    """Apply the same conservative issuer comparison as the EDGAR bridge."""

    left_key = re.sub(
        r"[^a-z0-9]+",
        "",
        _sec_edgar_normalized_text(left).casefold(),
    )
    right_key = re.sub(
        r"[^a-z0-9]+",
        "",
        _sec_edgar_normalized_text(right).casefold(),
    )
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    shorter, longer = sorted((left_key, right_key), key=len)
    return len(shorter) >= 10 and longer.startswith(shorter)


def _sec_edgar_class_profile(
    value: object | None,
) -> tuple[str | None, frozenset[str]]:
    """Return the exact broad family and explicit class designators."""

    normalized = re.sub(
        r"[^A-Z0-9]+",
        " ",
        _sec_edgar_normalized_text(value).upper(),
    ).strip()
    tokens = set(normalized.split())
    family = None
    if _PREFERRED_CLASS_RE.search(normalized):
        family = "PREF"
    elif _WARRANT_CLASS_RE.search(normalized):
        family = "WARRANT"
    elif _DEBT_CLASS_RE.search(normalized):
        family = "DEBT"
    elif _OPTION_CLASS_RE.search(normalized):
        family = "OPTION"
    elif tokens & {"COM", "COMMON", "ORD", "ORDINARY"} or (
        "CLASS" in tokens and tokens & {"SHARE", "SHARES", "STOCK"}
    ) or tokens & {"ADR", "ADRS", "ADS", "GDR", "GDRS"} or re.search(
        r"\b(?:AMERICAN|GLOBAL)\s+DEPOSITARY\s+"
        r"(?:SHARE|SHARES|RECEIPT|RECEIPTS)\b",
        normalized,
    ):
        family = "EQUITY"
    designators = frozenset(
        match.group(1)
        for match in re.finditer(
            r"\b(?:CLASS|CL)\s+([A-Z0-9]+)\b",
            normalized,
        )
    )
    return family, designators


def _validate_edgar_master_identity(
    entry: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    key: str,
) -> None:
    """Bind a resolved EDGAR proof to the exact master identity fields."""

    instrument_type = str(entry.get("instrument_type") or "").upper()
    if instrument_type not in FTD_ELIGIBLE_INSTRUMENT_TYPES:
        raise SecurityMasterError(
            f"resolved iXBRL record has an ineligible instrument type: {key}"
        )

    candidate_family, candidate_designators = _sec_edgar_class_profile(
        evidence.get("security_class")
    )
    if candidate_family != instrument_type:
        raise SecurityMasterError(
            f"resolved iXBRL class conflicts with its instrument type: {key}"
        )

    reported_classes: list[object] = []
    for field in ("reported_class", "class"):
        if _sec_edgar_normalized_text(entry.get(field)):
            reported_classes.append(entry[field])
    raw_classes = entry.get("reported_classes")
    if raw_classes is not None and not isinstance(raw_classes, list):
        raise SecurityMasterError(
            f"resolved iXBRL record has malformed reported classes: {key}"
        )
    if isinstance(raw_classes, list):
        reported_classes.extend(raw_classes)
    for reported_class in reported_classes:
        reported_family, reported_designators = _sec_edgar_class_profile(
            reported_class
        )
        if reported_family and reported_family != candidate_family:
            raise SecurityMasterError(
                f"resolved iXBRL proof conflicts with an as-filed class: {key}"
            )
        if (
            reported_designators
            and candidate_designators
            and reported_designators != candidate_designators
        ):
            raise SecurityMasterError(
                f"resolved iXBRL proof conflicts with an as-filed class: {key}"
            )

    candidate_cik = str(evidence.get("issuer_cik") or "")
    master_cik = str(entry.get("issuer_cik") or "")
    if master_cik and master_cik.zfill(10) != candidate_cik:
        raise SecurityMasterError(
            f"resolved iXBRL proof conflicts with the master issuer CIK: {key}"
        )

    reported_issuers: list[object] = []
    if _sec_edgar_normalized_text(entry.get("reported_issuer")):
        reported_issuers.append(entry["reported_issuer"])
    raw_issuers = entry.get("reported_issuers")
    if raw_issuers is not None and not isinstance(raw_issuers, list):
        raise SecurityMasterError(
            f"resolved iXBRL record has malformed reported issuers: {key}"
        )
    if isinstance(raw_issuers, list):
        reported_issuers.extend(raw_issuers)
    if reported_issuers and any(
        not _sec_edgar_issuer_names_compatible(
            reported_issuer,
            evidence.get("issuer_name"),
        )
        for reported_issuer in reported_issuers
    ):
        raise SecurityMasterError(
            f"resolved iXBRL proof conflicts with an as-filed issuer: {key}"
        )


def _validate_edgar_resolution_proof(
    entry: Mapping[str, Any],
    *,
    key: str,
    source_references: Mapping[tuple[str, str], str],
) -> None:
    evidence = entry.get("sec_edgar_evidence")
    if (
        entry.get("mapping_method")
        != "exact_schedule_13dg_ixbrl_class_bridge"
        or not isinstance(evidence, dict)
        or evidence.get("status") != "accepted"
        or set(evidence)
        != {
            "status",
            "issuer_cik",
            "issuer_name",
            "security_class",
            "exchange",
            "exchanges",
            "schedule_13dg",
            "ixbrl",
        }
    ):
        raise SecurityMasterError(
            f"resolved iXBRL record lacks exact SEC bridge proof: {key}"
        )
    schedule = evidence.get("schedule_13dg")
    ixbrl = evidence.get("ixbrl")
    if (
        not isinstance(schedule, dict)
        or set(schedule) != {"accession", "url", "as_of", "sha256"}
        or not isinstance(ixbrl, dict)
        or set(ixbrl)
        != {"accession", "url", "as_of", "sha256", "context_ids"}
    ):
        raise SecurityMasterError(
            f"resolved iXBRL record has malformed SEC bridge proof: {key}"
        )
    for proof, expected_kind in (
        (schedule, "schedule_13dg"),
        (ixbrl, "periodic_ixbrl"),
    ):
        accession = proof.get("accession")
        url = normalize_sec_url(str(proof.get("url") or ""))
        sha256 = str(proof.get("sha256") or "")
        as_of = proof.get("as_of")
        if (
            not _SEC_ACCESSION_RE.fullmatch(str(accession or ""))
            or proof.get("url") != url
            or not _SHA256_RE.fullmatch(sha256)
            or _parse_settlement_date(as_of) != as_of
            or source_references.get((url, sha256)) != expected_kind
        ):
            raise SecurityMasterError(
                f"resolved iXBRL proof is not cryptographically bound to SEC: {key}"
            )
    context_ids = ixbrl.get("context_ids")
    if (
        not isinstance(context_ids, list)
        or not context_ids
        or context_ids != sorted(set(context_ids))
        or any(not isinstance(value, str) or not value for value in context_ids)
        or ixbrl.get("as_of") != entry.get("ticker_as_of")
        or entry.get("effective_from") != entry.get("ticker_as_of")
        or evidence.get("issuer_name") != entry.get("issuer")
        or evidence.get("security_class") != entry.get("security_class")
        or evidence.get("exchange") != entry.get("exchange")
        or evidence.get("exchanges") != entry.get("exchanges")
        or not re.fullmatch(r"\d{10}", str(evidence.get("issuer_cik") or ""))
    ):
        raise SecurityMasterError(
            f"resolved iXBRL record does not match its SEC bridge proof: {key}"
        )
    _validate_edgar_master_identity(entry, evidence, key=key)


def _validate_fund_series_record_evidence(
    entry: Mapping[str, Any],
    *,
    key: str,
    source_references: Mapping[tuple[str, str], str],
) -> None:
    evidence = entry.get("fund_series_evidence")
    if not isinstance(evidence, dict) or set(evidence) != {
        "symbol",
        "name",
        "cik",
        "series_id",
        "class_id",
        "url",
        "sha256",
        "verified_at",
    }:
        raise SecurityMasterError(
            f"security-master fund-series evidence is malformed: {key}"
        )
    url = normalize_sec_url(str(evidence.get("url") or ""))
    sha256 = str(evidence.get("sha256") or "")
    name = str(evidence.get("name") or "")
    if (
        entry.get("mapping_status") != "resolved"
        or evidence.get("symbol") != entry.get("ticker")
        or entry.get("fund_series_name") != name
        or not name
        or " ".join(name.split()) != name
        or not re.fullmatch(r"\d{10}", str(evidence.get("cik") or ""))
        or not re.fullmatch(r"S\d+", str(evidence.get("series_id") or ""))
        or not re.fullmatch(r"C\d+", str(evidence.get("class_id") or ""))
        or evidence.get("url") != url
        or not _SHA256_RE.fullmatch(sha256)
        or source_references.get((url, sha256)) != "sec_fund_series"
        or not _is_canonical_utc_timestamp(evidence.get("verified_at"))
    ):
        raise SecurityMasterError(
            f"security-master fund-series evidence is not bound to SEC: {key}"
        )


def _fund_series_source_checkpoints(
    records: Mapping[str, Any],
) -> dict[str, str]:
    """Return the exact successful-check clock for each used fund page."""

    checkpoints: dict[str, str] = {}
    for record in records.values():
        if not isinstance(record, Mapping):
            continue
        evidence = record.get("fund_series_evidence")
        if not isinstance(evidence, Mapping):
            continue
        url = str(evidence.get("url") or "")
        checked_at = str(evidence.get("verified_at") or "")
        prior = checkpoints.get(url)
        if prior is not None and prior != checked_at:
            raise SecurityMasterError(
                "security-master fund-series page has conflicting checkpoints"
            )
        checkpoints[url] = checked_at
    return {url: checkpoints[url] for url in sorted(checkpoints)}


def _edgar_successful_checkpoints_by_cusip(
    state: Mapping[str, Any],
) -> dict[str, str | None]:
    """Project embedded discovery success clocks without mutating legacy v1."""

    discovery = state.get("edgar_discovery", {})
    if not isinstance(discovery, Mapping) or not discovery:
        return {}
    schema_version = discovery.get("schema_version")
    if schema_version not in {
        LEGACY_EDGAR_DISCOVERY_SCHEMA_VERSION,
        EDGAR_DISCOVERY_SCHEMA_VERSION,
    }:
        return {}
    records = discovery.get("records", {})
    if not isinstance(records, Mapping):
        return {}
    return {
        str(cusip): _edgar_discovery_successful_check_at(
            record,
            schema_version=int(schema_version),
        )
        for cusip, record in sorted(records.items())
        if isinstance(record, Mapping)
    }


def _sec_ixbrl_source_checkpoints(
    records: Mapping[str, Any],
    state: Mapping[str, Any],
) -> dict[str, str | None]:
    """Return the last complete revalidation for every resolved iXBRL key."""

    by_cusip = _edgar_successful_checkpoints_by_cusip(state)
    return {
        key: by_cusip.get(normalize_cusip(record.get("cusip")))
        for key, record in sorted(records.items())
        if isinstance(record, Mapping)
        and record.get("mapping_status") == "resolved"
        and record.get("ticker_source") == "sec_ixbrl"
    }


def _validate_reported_identity_evidence(
    entry: Mapping[str, Any],
    *,
    key: str,
) -> None:
    identities = entry.get("reported_identities", [])
    evidence = entry.get("reported_identity_evidence", [])
    identity_fields = {"reported_cusip", "reported_issuer", "reported_class"}
    evidence_fields = identity_fields | {
        "accession",
        "report_date",
        "url",
        "sha256",
    }
    if not isinstance(identities, list) or not isinstance(evidence, list):
        raise SecurityMasterError(
            f"reported-identity evidence is not a list: {key}"
        )
    identity_payloads: list[str] = []
    identity_keys: set[tuple[str, str, str]] = set()
    for identity in identities:
        if not isinstance(identity, dict) or set(identity) != identity_fields:
            raise SecurityMasterError(
                f"malformed reported identity metadata: {key}"
            )
        identity_key = (
            normalize_cusip(identity.get("reported_cusip")),
            " ".join(str(identity.get("reported_issuer") or "").split()),
            " ".join(str(identity.get("reported_class") or "").split()),
        )
        if (
            identity_key[0] != entry.get("cusip")
            or identity.get("reported_cusip") != identity_key[0]
            or identity.get("reported_issuer") != identity_key[1]
            or identity.get("reported_class") != identity_key[2]
        ):
            raise SecurityMasterError(
                f"reported identity conflicts with security-master row: {key}"
            )
        identity_keys.add(identity_key)
        identity_payloads.append(
            json.dumps(identity, sort_keys=True, separators=(",", ":"))
        )
    if identity_payloads != sorted(set(identity_payloads)):
        raise SecurityMasterError(
            f"reported identities are not unique and ordered: {key}"
        )

    evidence_payloads: list[str] = []
    for source in evidence:
        if not isinstance(source, dict) or set(source) != evidence_fields:
            raise SecurityMasterError(
                f"malformed reported-identity source evidence: {key}"
            )
        source_identity = (
            normalize_cusip(source.get("reported_cusip")),
            " ".join(str(source.get("reported_issuer") or "").split()),
            " ".join(str(source.get("reported_class") or "").split()),
        )
        source_url = _normalize_reported_identity_source_url(
            source.get("url"),
            accession=source.get("accession"),
        )
        if (
            source_identity not in identity_keys
            or source["reported_cusip"] != source_identity[0]
            or source["reported_issuer"] != source_identity[1]
            or source["reported_class"] != source_identity[2]
            or not _SEC_ACCESSION_RE.fullmatch(str(source.get("accession") or ""))
            or _parse_settlement_date(source.get("report_date"))
            != source.get("report_date")
            or source.get("url") != source_url
            or not _SHA256_RE.fullmatch(str(source.get("sha256") or ""))
        ):
            raise SecurityMasterError(
                f"invalid reported-identity source evidence: {key}"
            )
        evidence_payloads.append(
            json.dumps(source, sort_keys=True, separators=(",", ":"))
        )
    if evidence_payloads != sorted(set(evidence_payloads)):
        raise SecurityMasterError(
            f"reported-identity source evidence is not unique and ordered: {key}"
        )


def _reported_identity_evidence_counts(
    records: Mapping[str, Any],
) -> tuple[int, int]:
    required: set[tuple[str, str, str, str]] = set()
    evidenced: set[tuple[str, str, str, str]] = set()
    for key, record in records.items():
        if not isinstance(record, Mapping):
            continue
        for identity in record.get("reported_identities", []) or []:
            if isinstance(identity, Mapping):
                required.add((
                    str(key),
                    str(identity.get("reported_cusip") or ""),
                    str(identity.get("reported_issuer") or ""),
                    str(identity.get("reported_class") or ""),
                ))
        for source in record.get("reported_identity_evidence", []) or []:
            if isinstance(source, Mapping):
                evidenced.add((
                    str(key),
                    str(source.get("reported_cusip") or ""),
                    str(source.get("reported_issuer") or ""),
                    str(source.get("reported_class") or ""),
                ))
    return len(required), len(required.intersection(evidenced))


def _validate_security_master(master: Mapping[str, Any]) -> None:
    if master.get("schema_version") != MASTER_SCHEMA_VERSION:
        raise SecurityMasterError("unsupported SEC security-master schema version")
    policy = master.get("policy")
    if not isinstance(policy, dict):
        raise SecurityMasterError("security master policy must be an object")
    source_references, source_kinds = _validate_master_sources(master)
    source_state_version = master.get("source_state_schema_version")
    if source_state_version is not None and source_state_version not in {
        LEGACY_SOURCE_STATE_SCHEMA_VERSION,
        COMPACT_SOURCE_STATE_SCHEMA_VERSION,
        TIMELINE_SOURCE_STATE_SCHEMA_VERSION,
        SOURCE_STATE_SCHEMA_VERSION,
    }:
        raise SecurityMasterError(
            "security master references an unsupported source-state schema"
        )
    records = master.get("records")
    if not isinstance(records, dict):
        raise SecurityMasterError("SEC security master must contain a records object")
    computed_summary = {
        status: 0 for status in sorted(VALID_MAPPING_STATUSES)
    }
    expected_quarantine: dict[str, dict[str, str]] = {}
    for key, entry in records.items():
        if not isinstance(entry, dict):
            raise SecurityMasterError(f"security-master record is not an object: {key}")
        expected_key = security_key(entry.get("cusip"), entry.get("instrument_type"))
        if key != expected_key:
            raise SecurityMasterError(f"security-master key mismatch: {key}")
        _validate_reported_identity_evidence(entry, key=key)
        status = entry.get("mapping_status")
        if status not in VALID_MAPPING_STATUSES:
            raise SecurityMasterError(f"invalid mapping status for {key}: {status}")
        computed_summary[status] += 1
        quarantine_reason = cusip_quarantine_reason(entry.get("cusip"))
        if quarantine_reason is not None:
            if (
                status != "malformed_as_filed"
                or entry.get("resolution_reason") != quarantine_reason
            ):
                raise SecurityMasterError(
                    f"malformed identifier is not quarantined exactly: {key}"
                )
            expected_quarantine[key] = {
                "cusip": entry["cusip"],
                "instrument_type": entry["instrument_type"],
                "reason": quarantine_reason,
            }
        elif status == "malformed_as_filed":
            raise SecurityMasterError(
                f"valid identifier is incorrectly quarantined: {key}"
            )
        ticker = entry.get("ticker")
        ticker_source = entry.get("ticker_source")
        ticker_as_of = entry.get("ticker_as_of")
        mapping_method = entry.get("mapping_method")
        effective_from = entry.get("effective_from")
        effective_to = entry.get("effective_to")
        last_verification_date = entry.get("last_verification_date")
        if (
            last_verification_date is not None
            and _parse_settlement_date(last_verification_date)
            != last_verification_date
        ):
            raise SecurityMasterError(
                f"security-master record has invalid verification date: {key}"
            )
        symbol_evidence = entry.get("symbol_evidence", [])
        if not isinstance(symbol_evidence, list):
            raise SecurityMasterError(
                f"security-master symbol evidence is not a list: {key}"
            )
        _validate_symbol_evidence(
            symbol_evidence,
            key=key,
            source_references=source_references,
        )
        symbol_intervals = entry.get("symbol_intervals")
        if symbol_evidence:
            _validate_master_symbol_intervals(
                symbol_intervals,
                key=key,
                symbol_evidence=symbol_evidence,
                source_references=source_references,
            )
        elif symbol_intervals not in (None, []):
            _validate_master_symbol_intervals(
                symbol_intervals,
                key=key,
                symbol_evidence=[],
                source_references=source_references,
            )
        if status == "resolved":
            if (
                _normalize_symbol(ticker) != ticker
                or ticker_source not in VALID_TICKER_SOURCES
                or _parse_settlement_date(ticker_as_of) != ticker_as_of
                or (
                    "last_verification_date" in entry
                    and last_verification_date != ticker_as_of
                )
            ):
                raise SecurityMasterError(
                    f"resolved record lacks valid ticker provenance: {key}"
                )
        elif any(value is not None for value in (ticker, ticker_source, ticker_as_of)):
            raise SecurityMasterError(
                f"non-resolved record carries ticker provenance: {key}"
            )
        mapping_interval_fields = (
            "mapping_method",
            "effective_from",
            "effective_to",
        )
        if status == "resolved" and any(
            field not in entry for field in mapping_interval_fields
        ):
            raise SecurityMasterError(
                f"resolved record lacks a mapping interval: {key}"
            )
        if any(field in entry for field in mapping_interval_fields):
            if any(field not in entry for field in mapping_interval_fields):
                raise SecurityMasterError(
                    f"security-master record has incomplete mapping interval: {key}"
                )
            if status == "resolved":
                parsed_from = _parse_settlement_date(effective_from)
                parsed_to = (
                    _parse_settlement_date(effective_to)
                    if effective_to is not None
                    else None
                )
                if (
                    mapping_method not in VALID_MAPPING_METHODS
                    or not isinstance(effective_from, str)
                    or parsed_from != effective_from
                    or (
                        effective_to is not None
                        and (
                            not isinstance(effective_to, str)
                            or parsed_to != effective_to
                        )
                    )
                    or effective_from > ticker_as_of
                    or (
                        effective_to is not None
                        and (
                            effective_to < effective_from
                            or ticker_as_of > effective_to
                        )
                    )
                ):
                    raise SecurityMasterError(
                        f"resolved record has invalid mapping interval: {key}"
                    )
            elif any(
                value is not None
                for value in (mapping_method, effective_from, effective_to)
            ):
                raise SecurityMasterError(
                    f"non-resolved record carries a mapping interval: {key}"
                )
        if "exchanges" in entry:
            exchanges = entry.get("exchanges")
            if (
                not isinstance(exchanges, list)
                or not exchanges
                or any(
                    not isinstance(exchange, str) or not exchange.strip()
                    for exchange in exchanges
                )
                or exchanges != sorted(set(exchanges))
            ):
                raise SecurityMasterError(
                    f"security-master record has invalid exchanges: {key}"
                )
            exchange = entry.get("exchange")
            if exchange is not None and exchange not in exchanges:
                raise SecurityMasterError(
                    f"security-master primary exchange is not in exchanges: {key}"
                )
        elif entry.get("exchange") is not None:
            raise SecurityMasterError(
                f"security-master primary exchange lacks exchange set: {key}"
            )
        if status == "resolved" and ticker_source == "sec_ftd":
            if entry.get("instrument_type") not in FTD_ELIGIBLE_INSTRUMENT_TYPES:
                raise SecurityMasterError(
                    f"resolved FTD record has an ineligible instrument type: {key}"
                )
            _validate_ftd_resolution_proof(
                entry,
                key=key,
                master=master,
                source_kinds=source_kinds,
            )
            _validate_ftd_master_identity(entry, key=key)
        elif status == "resolved" and ticker_source == "sec_ixbrl":
            _validate_edgar_resolution_proof(
                entry,
                key=key,
                source_references=source_references,
            )
        if "fund_series_name" in entry or "fund_series_evidence" in entry:
            if not (
                "fund_series_name" in entry
                and "fund_series_evidence" in entry
            ):
                raise SecurityMasterError(
                    f"security-master record has incomplete fund-series evidence: {key}"
                )
            _validate_fund_series_record_evidence(
                entry,
                key=key,
                source_references=source_references,
            )
    if master.get("summary") != computed_summary:
        raise SecurityMasterError(
            "security-master summary does not match record statuses"
        )
    if master.get("quarantine") != expected_quarantine:
        raise SecurityMasterError(
            "security-master quarantine does not match malformed records"
        )
    audit = master.get("audit")
    if audit is not None:
        if not isinstance(audit, dict):
            raise SecurityMasterError("security-master audit must be an object")
        if audit:
            audit_schema_version = audit.get("schema_version")
            if audit_schema_version not in {
                LEGACY_MASTER_AUDIT_SCHEMA_VERSION,
                FILTER_COVERAGE_MASTER_AUDIT_SCHEMA_VERSION,
                CURRENT_SOURCE_MASTER_AUDIT_SCHEMA_VERSION,
                IXBRL_MASTER_AUDIT_SCHEMA_VERSION,
                MASTER_AUDIT_SCHEMA_VERSION,
            }:
                raise SecurityMasterError(
                    "unsupported security-master audit schema version"
                )
            count_fields = (
                "active_non_option_official_cusip_count",
                "malformed_active_official_cusip_count",
                "ftd_evidenced_official_cusip_count",
            )
            for field in count_fields:
                value = audit.get(field)
                if type(value) is not int or value < 0:
                    raise SecurityMasterError(
                        f"invalid security-master audit count: {field}"
                    )
            active_count = audit["active_non_option_official_cusip_count"]
            malformed_count = audit["malformed_active_official_cusip_count"]
            evidenced_count = audit["ftd_evidenced_official_cusip_count"]
            if malformed_count > active_count or evidenced_count > active_count:
                raise SecurityMasterError(
                    "security-master audit count exceeds official-list total"
                )
            coverage = audit.get("ftd_coverage_ratio")
            if (
                isinstance(coverage, bool)
                or not isinstance(coverage, (int, float))
                or not 0 <= float(coverage) <= 1
            ):
                raise SecurityMasterError(
                    "invalid security-master FTD coverage ratio"
                )
            expected_coverage = (
                round(evidenced_count / active_count, 8)
                if active_count
                else 0.0
            )
            if abs(float(coverage) - expected_coverage) > 1e-9:
                raise SecurityMasterError(
                    "security-master FTD coverage ratio does not match counts"
                )
            active_hash = str(
                audit.get("active_non_option_official_cusips_sha256") or ""
            )
            if not _SHA256_RE.fullmatch(active_hash):
                raise SecurityMasterError(
                    "invalid official-list CUSIP-set audit hash"
                )
            audit_as_of = audit.get("as_of")
            if (
                audit_as_of is not None
                and _parse_settlement_date(audit_as_of) != audit_as_of
            ):
                raise SecurityMasterError("invalid security-master audit as-of")
            latest_ftd = audit.get("latest_ftd_settlement_date")
            if (
                latest_ftd is not None
                and _parse_settlement_date(latest_ftd) != latest_ftd
            ):
                raise SecurityMasterError(
                    "invalid latest FTD settlement date in audit"
                )
            age_days = audit.get("ftd_source_age_days")
            if age_days is not None and (
                type(age_days) is not int or age_days < 0
            ):
                raise SecurityMasterError("invalid FTD source age in audit")
            threshold = audit.get("source_staleness_threshold_days")
            if type(threshold) is not int or threshold < 0:
                raise SecurityMasterError(
                    "invalid source-staleness threshold in audit"
                )
            source_stale = audit.get("source_stale")
            if type(source_stale) is not bool or source_stale != (
                age_days is None or age_days > threshold
            ):
                raise SecurityMasterError(
                    "security-master source-staleness flag is inconsistent"
                )
            schemas = audit.get("source_schema_sha256_by_kind")
            if not isinstance(schemas, dict) or any(
                not isinstance(kind, str)
                or not kind
                or not _SHA256_RE.fullmatch(str(fingerprint))
                for kind, fingerprint in schemas.items()
            ):
                raise SecurityMasterError(
                    "invalid security-master source-schema fingerprints"
                )
            if (
                audit_schema_version
                >= FILTER_COVERAGE_MASTER_AUDIT_SCHEMA_VERSION
            ):
                filter_digest = audit.get("ftd_filter_universe_sha256")
                filter_count = audit.get("ftd_filter_universe_count")
                required_count = audit.get("required_filtered_archive_count")
                covered_count = audit.get("covered_filtered_archive_count")
                incomplete_urls = audit.get(
                    "incomplete_filtered_archive_urls"
                )
                coverage_complete = audit.get(
                    "filter_universe_coverage_complete"
                )
                if (
                    (filter_digest is not None and not _SHA256_RE.fullmatch(
                        str(filter_digest)
                    ))
                    or type(filter_count) is not int
                    or filter_count < 0
                    or type(required_count) is not int
                    or required_count < 0
                    or type(covered_count) is not int
                    or not 0 <= covered_count <= required_count
                    or not isinstance(incomplete_urls, list)
                    or any(not isinstance(url, str) for url in incomplete_urls)
                    or incomplete_urls != sorted(set(incomplete_urls))
                    or len(incomplete_urls) != required_count - covered_count
                    or type(coverage_complete) is not bool
                    or coverage_complete != (not incomplete_urls)
                ):
                    raise SecurityMasterError(
                        "invalid FTD filter-universe audit metadata"
                    )
                for incomplete_url in incomplete_urls:
                    normalize_sec_url(incomplete_url)
            if (
                audit_schema_version
                >= CURRENT_SOURCE_MASTER_AUDIT_SCHEMA_VERSION
            ):
                checkpoint_days = audit.get(
                    "successful_check_checkpoint_days"
                )
                checkpoints = audit.get(
                    "required_current_source_checkpoints"
                )
                if (
                    type(checkpoint_days) is not int
                    or not 1 <= checkpoint_days <= 30
                    or not isinstance(checkpoints, dict)
                    or set(checkpoints) != _REQUIRED_CURRENT_SOURCE_KINDS
                ):
                    raise SecurityMasterError(
                        "invalid required-current-source checkpoint metadata"
                    )
                for kind, checkpoint in checkpoints.items():
                    if not isinstance(checkpoint, dict) or set(checkpoint) != {
                        "url",
                        "last_successful_check_at",
                    }:
                        raise SecurityMasterError(
                            "invalid required-current-source checkpoint entry"
                        )
                    source_url = checkpoint.get("url")
                    checked_at = checkpoint.get("last_successful_check_at")
                    if source_url is not None:
                        normalize_sec_url(str(source_url))
                    if checked_at is not None and (
                        not isinstance(checked_at, str)
                        or _parse_calendar_date(checked_at) is None
                    ):
                        raise SecurityMasterError(
                            "invalid required-current-source check timestamp"
                        )
                fund_checkpoints = audit.get(
                    "fund_series_source_checkpoints",
                    {},
                )
                if (
                    not isinstance(fund_checkpoints, dict)
                    or list(fund_checkpoints) != sorted(fund_checkpoints)
                    or fund_checkpoints
                    != _fund_series_source_checkpoints(
                        master.get("records", {})
                    )
                ):
                    raise SecurityMasterError(
                        "invalid fund-series checkpoint metadata"
                    )
                for source_url, checked_at in fund_checkpoints.items():
                    normalized_url = normalize_sec_url(source_url)
                    if (
                        normalized_url != source_url
                        or not _is_canonical_utc_timestamp(checked_at)
                        or not any(
                            source.get("url") == source_url
                            and source.get("kind") == "sec_fund_series"
                            for source in master.get("sources", [])
                            if isinstance(source, dict)
                        )
                    ):
                        raise SecurityMasterError(
                            "invalid fund-series checkpoint entry"
                        )
            if audit_schema_version >= IXBRL_MASTER_AUDIT_SCHEMA_VERSION:
                ixbrl_checkpoints = audit.get(
                    "sec_ixbrl_source_checkpoints"
                )
                expected_ixbrl_keys = {
                    key
                    for key, record in records.items()
                    if isinstance(record, Mapping)
                    and record.get("mapping_status") == "resolved"
                    and record.get("ticker_source") == "sec_ixbrl"
                }
                if (
                    not isinstance(ixbrl_checkpoints, dict)
                    or list(ixbrl_checkpoints) != sorted(ixbrl_checkpoints)
                    or set(ixbrl_checkpoints) != expected_ixbrl_keys
                    or any(
                        checked_at is not None
                        and not _is_canonical_utc_timestamp(checked_at)
                        for checked_at in ixbrl_checkpoints.values()
                    )
                ):
                    raise SecurityMasterError(
                        "invalid SEC iXBRL successful-check metadata"
                    )
            if audit_schema_version >= MASTER_AUDIT_SCHEMA_VERSION:
                populations = audit.get("current_symbol_population_by_kind")
                title_populations = audit.get(
                    "current_symbol_title_population_by_kind"
                )
                resolved_counts = audit.get(
                    "resolved_mapping_count_by_ticker_source"
                )
                reported_identity_count = audit.get(
                    "reported_identity_count"
                )
                evidenced_reported_identity_count = audit.get(
                    "evidenced_reported_identity_count"
                )
                if (
                    not isinstance(populations, dict)
                    or set(populations) != _VALIDATION_SOURCE_KINDS
                    or not isinstance(title_populations, dict)
                    or set(title_populations) != _VALIDATION_SOURCE_KINDS
                    or any(
                        type(populations[kind]) is not int
                        or populations[kind] < 0
                        or type(title_populations[kind]) is not int
                        or not 0 <= title_populations[kind] <= populations[kind]
                        for kind in _VALIDATION_SOURCE_KINDS
                    )
                    or not isinstance(resolved_counts, dict)
                    or set(resolved_counts) != VALID_TICKER_SOURCES
                    or any(
                        type(count) is not int or count < 0
                        for count in resolved_counts.values()
                    )
                    or resolved_counts != _resolved_mapping_counts(records)
                    or type(reported_identity_count) is not int
                    or reported_identity_count < 0
                    or type(evidenced_reported_identity_count) is not int
                    or not 0
                    <= evidenced_reported_identity_count
                    <= reported_identity_count
                    or (
                        reported_identity_count,
                        evidenced_reported_identity_count,
                    )
                    != _reported_identity_evidence_counts(records)
                ):
                    raise SecurityMasterError(
                        "invalid current-source population audit metadata"
                    )
def validate_security_master(master: Mapping[str, Any]) -> None:
    """Validate a master, including fail-closed ticker provenance invariants."""

    _validate_security_master(master)


def _empty_security_master() -> dict[str, Any]:
    return {
        "schema_version": MASTER_SCHEMA_VERSION,
        "generated_at": None,
        "source_state_sha256": None,
        "universe_sha256": None,
        "policy": {},
        "sources": [],
        "records": {},
        "quarantine": {},
        "summary": {status: 0 for status in sorted(VALID_MAPPING_STATUSES)},
    }


def _load_security_master_unlocked(path: Path) -> dict[str, Any]:
    payload = _read_json_object(path)
    if not payload:
        return _empty_security_master()
    _validate_security_master(payload)
    return payload


def load_security_master(
    path: Path = DEFAULT_MASTER_PATH,
) -> dict[str, Any]:
    """Load the master after recovering any interrupted pair publish."""

    path = Path(path)
    _recover_security_master_pair_for_path(path)
    return _load_security_master_unlocked(path)


def save_security_master(
    master: Mapping[str, Any],
    path: Path = DEFAULT_MASTER_PATH,
) -> None:
    """Save a standalone staging/migration master, not a published pair."""

    _validate_security_master(master)
    _atomic_write_json(Path(path), master)


def _absolute_managed_path(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(Path(path))))


def _validate_security_master_pair_paths(
    master_path: Path,
    source_state_path: Path,
    *,
    create_parent: bool,
) -> tuple[Path, Path, Path]:
    master_target = _absolute_managed_path(master_path)
    state_target = _absolute_managed_path(source_state_path)
    if master_target == state_target:
        raise SecurityMasterError(
            "security-master pair targets must be distinct files"
        )
    if master_target.parent != state_target.parent:
        raise SecurityMasterError(
            "security-master pair targets must share one parent directory"
        )
    parent = master_target.parent
    if create_parent:
        parent.mkdir(parents=True, exist_ok=True)
    try:
        parent_metadata = parent.lstat()
    except FileNotFoundError:
        raise SecurityMasterError(
            f"security-master pair parent is missing: {parent}"
        ) from None
    except OSError as exc:
        raise SecurityMasterError(
            f"cannot inspect security-master pair parent {parent}: {exc}"
        ) from exc
    if stat.S_ISLNK(parent_metadata.st_mode) or not stat.S_ISDIR(
        parent_metadata.st_mode
    ):
        raise SecurityMasterError(
            "security-master pair parent must be a real directory"
        )
    for target in (master_target, state_target):
        if target.name.startswith(_PAIR_ARTIFACT_PREFIX):
            raise SecurityMasterError(
                "security-master pair target uses a reserved artifact name"
            )
        _reject_symlink_or_nonregular(target, allow_missing=True)
    if master_target.exists() and state_target.exists():
        master_metadata = master_target.stat(follow_symlinks=False)
        state_metadata = state_target.stat(follow_symlinks=False)
        if (
            master_metadata.st_dev,
            master_metadata.st_ino,
        ) == (
            state_metadata.st_dev,
            state_metadata.st_ino,
        ):
            raise SecurityMasterError(
                "security-master pair targets must use distinct inodes"
            )
    return master_target, state_target, parent


def _pair_process_lock(parent: Path) -> threading.RLock:
    key = os.fspath(parent)
    with _PAIR_LOCK_REGISTRY_GUARD:
        return _PAIR_PROCESS_LOCKS.setdefault(key, threading.RLock())


@contextmanager
def _security_master_pair_file_lock(parent: Path) -> Iterator[None]:
    """Take the directory-scoped flock, reentrantly in the current thread."""

    key = os.fspath(parent)
    process_lock = _pair_process_lock(parent)
    with process_lock:
        held = getattr(_PAIR_LOCK_LOCAL, "held", None)
        if held is None:
            held = {}
            _PAIR_LOCK_LOCAL.held = held
        active = held.get(key)
        if active is not None:
            active[1] += 1
            try:
                yield
            finally:
                active[1] -= 1
            return

        lock_path = parent / _PAIR_LOCK_NAME
        _reject_symlink_or_nonregular(lock_path, allow_missing=True)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as exc:
            raise SecurityMasterError(
                f"cannot open security-master pair lock: {exc}"
            ) from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise SecurityMasterError(
                    "security-master pair lock must be a regular file"
                )
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            held[key] = [descriptor, 1]
            try:
                yield
            finally:
                held.pop(key, None)
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except OSError:
                    # Closing the descriptor also releases flock. Once the
                    # protected body completed, a cleanup-only unlock error
                    # must not report publication failure after commit.
                    pass
        finally:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _file_sha256(path: Path) -> str:
    _reject_symlink_or_nonregular(path, allow_missing=False)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise SecurityMasterError(
                f"managed path must be a regular file: {path.name}"
            )
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _write_pair_new_keeper(
    path: Path,
    payload: Mapping[str, Any],
) -> str:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as out:
            descriptor = -1
            json.dump(
                payload,
                out,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            out.write("\n")
            out.flush()
            os.fsync(out.fileno())
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            path.unlink()
        except BaseException:
            pass
        raise
    return _file_sha256(path)


def _pair_artifact_name(
    transaction_id: str,
    member: str,
    role: str,
) -> str:
    return f"{_PAIR_ARTIFACT_PREFIX}{transaction_id}.{member}.{role}"


_PAIR_RUN_ARTIFACT_RE = re.compile(
    r"^\.sec-security-master-pair\.[0-9a-f]{32}\."
    r"(?:master|state)\.(?:new|old|install|restore)$"
)
_PAIR_MARKER_TEMP_RE = re.compile(
    r"^\.sec-security-master-pair\.transaction\.json\..+\.tmp$"
)


def _cleanup_orphan_pair_artifacts_locked(parent: Path) -> None:
    """Remove only reserved transaction residue while no marker is active."""

    marker_path = parent / _PAIR_MARKER_NAME
    if marker_path.exists() or marker_path.is_symlink():
        return
    for candidate in parent.iterdir():
        if not (
            _PAIR_RUN_ARTIFACT_RE.fullmatch(candidate.name)
            or _PAIR_MARKER_TEMP_RE.fullmatch(candidate.name)
        ):
            continue
        _reject_symlink_or_nonregular(candidate, allow_missing=True)
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass
    _fsync_directory(parent)


def _validate_pair_member_marker(
    raw: object,
    *,
    transaction_id: str,
    member: str,
    target_name: str,
) -> dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) != {
        "target",
        "new",
        "old",
        "old_exists",
        "new_sha256",
        "old_sha256",
    }:
        raise SecurityMasterError(
            "security-master pair transaction marker is malformed"
        )
    expected_new = _pair_artifact_name(transaction_id, member, "new")
    expected_old = _pair_artifact_name(transaction_id, member, "old")
    old_exists = raw.get("old_exists")
    if (
        raw.get("target") != target_name
        or raw.get("new") != expected_new
        or type(old_exists) is not bool
        or not _SHA256_RE.fullmatch(str(raw.get("new_sha256") or ""))
    ):
        raise SecurityMasterError(
            "security-master pair transaction marker is malformed"
        )
    if old_exists:
        if (
            raw.get("old") != expected_old
            or not _SHA256_RE.fullmatch(str(raw.get("old_sha256") or ""))
        ):
            raise SecurityMasterError(
                "security-master pair transaction marker is malformed"
            )
    elif raw.get("old") is not None or raw.get("old_sha256") is not None:
        raise SecurityMasterError(
            "security-master pair transaction marker is malformed"
        )
    return dict(raw)


def _load_pair_marker_locked(
    parent: Path,
    *,
    master_name: str | None = None,
    state_name: str | None = None,
) -> dict[str, Any] | None:
    marker_path = parent / _PAIR_MARKER_NAME
    marker = _read_json_object(marker_path)
    if not marker:
        return None
    if set(marker) != {
        "schema_version",
        "transaction_id",
        "master",
        "source_state",
    } or marker.get("schema_version") != _PAIR_TRANSACTION_SCHEMA_VERSION:
        raise SecurityMasterError(
            "security-master pair transaction marker is malformed"
        )
    transaction_id = marker.get("transaction_id")
    if not isinstance(transaction_id, str) or not re.fullmatch(
        r"[0-9a-f]{32}", transaction_id
    ):
        raise SecurityMasterError(
            "security-master pair transaction marker is malformed"
        )
    raw_master = marker.get("master")
    raw_state = marker.get("source_state")
    inferred_master_name = (
        master_name
        if master_name is not None
        else str(raw_master.get("target") if isinstance(raw_master, dict) else "")
    )
    inferred_state_name = (
        state_name
        if state_name is not None
        else str(raw_state.get("target") if isinstance(raw_state, dict) else "")
    )
    if (
        not inferred_master_name
        or not inferred_state_name
        or Path(inferred_master_name).name != inferred_master_name
        or Path(inferred_state_name).name != inferred_state_name
        or inferred_master_name == inferred_state_name
        or inferred_master_name.startswith(_PAIR_ARTIFACT_PREFIX)
        or inferred_state_name.startswith(_PAIR_ARTIFACT_PREFIX)
    ):
        raise SecurityMasterError(
            "security-master pair transaction marker is malformed"
        )
    return {
        "schema_version": _PAIR_TRANSACTION_SCHEMA_VERSION,
        "transaction_id": transaction_id,
        "master": _validate_pair_member_marker(
            raw_master,
            transaction_id=transaction_id,
            member="master",
            target_name=inferred_master_name,
        ),
        "source_state": _validate_pair_member_marker(
            raw_state,
            transaction_id=transaction_id,
            member="state",
            target_name=inferred_state_name,
        ),
    }


def _path_matches_sha256(path: Path, expected: str) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise SecurityMasterError(
            f"cannot inspect managed path {path.name}: {exc}"
        ) from exc
    try:
        return _file_sha256(path) == expected
    except SecurityMasterError:
        raise
    except OSError as exc:
        if isinstance(exc, FileNotFoundError):
            return False
        raise SecurityMasterError(
            f"cannot verify managed path {path.name}: {exc}"
        ) from exc


def _link_regular(source: Path, destination: Path) -> None:
    _reject_symlink_or_nonregular(source, allow_missing=False)
    _reject_symlink_or_nonregular(destination, allow_missing=True)
    try:
        os.link(source, destination, follow_symlinks=False)
    except OSError as exc:
        raise SecurityMasterError(
            f"cannot retain security-master pair inode {source.name}: {exc}"
        ) from exc


def _replace_pair_member(
    keeper: Path,
    target: Path,
    *,
    transaction_id: str,
    member: str,
    role: str,
) -> None:
    """Install one retained inode and durably record the directory change."""

    install_path = target.parent / _pair_artifact_name(
        transaction_id,
        member,
        role,
    )
    _reject_symlink_or_nonregular(install_path, allow_missing=True)
    try:
        install_path.unlink()
    except FileNotFoundError:
        pass
    _link_regular(keeper, install_path)
    _fsync_directory(target.parent)
    _reject_symlink_or_nonregular(target, allow_missing=True)
    os.replace(install_path, target)
    os.chmod(target, 0o600, follow_symlinks=False)
    _fsync_directory(target.parent)


def _remove_marker_then_cleanup_locked(
    parent: Path,
    marker: Mapping[str, Any],
) -> None:
    marker_path = parent / _PAIR_MARKER_NAME
    try:
        marker_path.unlink()
    except FileNotFoundError:
        pass
    _fsync_directory(parent)
    # Once the durable marker is gone the committed/restored pair is the sole
    # authority. Cleanup cannot be allowed to turn success into a false error.
    try:
        for raw_member in (marker["master"], marker["source_state"]):
            for field in ("new", "old"):
                raw_name = raw_member.get(field)
                if not raw_name:
                    continue
                artifact = parent / str(raw_name)
                _reject_symlink_or_nonregular(artifact, allow_missing=True)
                try:
                    artifact.unlink()
                except FileNotFoundError:
                    pass
        _cleanup_orphan_pair_artifacts_locked(parent)
    except BaseException:
        pass


def _force_restore_old_security_master_pair_locked(
    master_path: Path,
    source_state_path: Path,
    parent: Path,
    marker: Mapping[str, Any],
) -> bool:
    """Restore the pre-transaction pair regardless of landed new members."""

    members = (
        ("master", master_path, marker["master"]),
        ("state", source_state_path, marker["source_state"]),
    )
    # Validate every required old keeper before changing either target. This
    # keeps recovery all-or-nothing even if an artifact was damaged manually.
    for _member, _target, raw in members:
        if not raw["old_exists"]:
            continue
        old_path = parent / str(raw["old"])
        if not _path_matches_sha256(old_path, str(raw["old_sha256"])):
            raise SecurityMasterError(
                "cannot recover security-master pair: retained old inode "
                f"is missing or corrupt for {raw['target']}"
            )

    transaction_id = str(marker["transaction_id"])
    for member, target, raw in members:
        if raw["old_exists"]:
            _replace_pair_member(
                parent / str(raw["old"]),
                target,
                transaction_id=transaction_id,
                member=member,
                role="restore",
            )
        else:
            _reject_symlink_or_nonregular(target, allow_missing=True)
            try:
                target.unlink()
            except FileNotFoundError:
                pass
            _fsync_directory(parent)

    for _member, target, raw in members:
        if raw["old_exists"]:
            if not _path_matches_sha256(target, str(raw["old_sha256"])):
                raise SecurityMasterError(
                    "security-master pair rollback verification failed"
                )
        elif target.exists() or target.is_symlink():
            raise SecurityMasterError(
                "security-master pair rollback could not restore absence"
            )
    _remove_marker_then_cleanup_locked(parent, marker)
    return True


def _recover_security_master_pair_locked(
    master_path: Path,
    source_state_path: Path,
    parent: Path,
) -> bool:
    marker = _load_pair_marker_locked(
        parent,
        master_name=master_path.name,
        state_name=source_state_path.name,
    )
    if marker is None:
        _cleanup_orphan_pair_artifacts_locked(parent)
        return False

    members = (
        ("master", master_path, marker["master"]),
        ("state", source_state_path, marker["source_state"]),
    )
    if all(
        _path_matches_sha256(target, str(raw["new_sha256"]))
        for _member, target, raw in members
    ):
        _remove_marker_then_cleanup_locked(parent, marker)
        return True
    return _force_restore_old_security_master_pair_locked(
        master_path,
        source_state_path,
        parent,
        marker,
    )


def _load_security_master_pair_locked(
    master_path: Path,
    source_state_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    master_exists = master_path.exists()
    state_exists = source_state_path.exists()
    _reject_symlink_or_nonregular(master_path, allow_missing=True)
    _reject_symlink_or_nonregular(source_state_path, allow_missing=True)
    if not master_exists and not state_exists:
        return _empty_security_master(), empty_source_state()
    if master_exists != state_exists:
        raise SecurityMasterError(
            "security-master pair is incomplete: both files are required"
        )
    state = _load_source_state_unlocked(source_state_path)
    loaded_master = _load_security_master_unlocked(master_path)
    expected_digest = source_state_sha256(state)
    if loaded_master.get("source_state_sha256") != expected_digest:
        raise SecurityMasterError(
            "security-master pair source-state digest does not match"
        )
    return loaded_master, state


@contextmanager
def security_master_pair_lock(
    *,
    master_path: Path = DEFAULT_MASTER_PATH,
    source_state_path: Path = DEFAULT_SOURCE_STATE_PATH,
) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    """Hold a reentrant pair lock and yield a recovered, validated pair."""

    master_target, state_target, parent = _validate_security_master_pair_paths(
        master_path,
        source_state_path,
        create_parent=True,
    )
    with _security_master_pair_file_lock(parent):
        _recover_security_master_pair_locked(master_target, state_target, parent)
        yield _load_security_master_pair_locked(master_target, state_target)


def recover_security_master_pair(
    *,
    master_path: Path = DEFAULT_MASTER_PATH,
    source_state_path: Path = DEFAULT_SOURCE_STATE_PATH,
) -> bool:
    """Recover an interrupted pair transaction, returning whether one existed."""

    master_target, state_target, parent = _validate_security_master_pair_paths(
        master_path,
        source_state_path,
        create_parent=True,
    )
    with _security_master_pair_file_lock(parent):
        return _recover_security_master_pair_locked(
            master_target,
            state_target,
            parent,
        )


def _recover_security_master_pair_for_path(path: Path) -> None:
    """Recover the directory's active pair before an individual file load."""

    target = _absolute_managed_path(path)
    parent = target.parent
    try:
        metadata = parent.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise SecurityMasterError(
            "security-master pair parent must be a real directory"
        )
    with _security_master_pair_file_lock(parent):
        marker = _load_pair_marker_locked(parent)
        if marker is None:
            return
        marker_master = parent / str(marker["master"]["target"])
        marker_state = parent / str(marker["source_state"]["target"])
        _recover_security_master_pair_locked(
            marker_master,
            marker_state,
            parent,
        )


def load_security_master_pair(
    *,
    master_path: Path = DEFAULT_MASTER_PATH,
    source_state_path: Path = DEFAULT_SOURCE_STATE_PATH,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Atomically load and validate the current master/source-state pair."""

    with security_master_pair_lock(
        master_path=master_path,
        source_state_path=source_state_path,
    ) as pair:
        return pair


def save_security_master_pair(
    master: Mapping[str, Any],
    source_state: Mapping[str, Any],
    *,
    master_path: Path = DEFAULT_MASTER_PATH,
    source_state_path: Path = DEFAULT_SOURCE_STATE_PATH,
) -> None:
    """Crash-safely publish a digest-bound source-state/master pair."""

    normalized_state = _normalize_source_state(source_state)
    _validate_source_state(normalized_state)
    _validate_security_master(master)
    expected_digest = source_state_sha256(normalized_state)
    if master.get("source_state_sha256") != expected_digest:
        raise SecurityMasterError(
            "security master is not bound to the supplied SEC source state"
        )

    master_target, state_target, parent = _validate_security_master_pair_paths(
        master_path,
        source_state_path,
        create_parent=True,
    )
    transaction_id = secrets.token_hex(16)
    master_new = parent / _pair_artifact_name(
        transaction_id, "master", "new"
    )
    state_new = parent / _pair_artifact_name(
        transaction_id, "state", "new"
    )
    marker_path = parent / _PAIR_MARKER_NAME

    with _security_master_pair_file_lock(parent):
        _recover_security_master_pair_locked(master_target, state_target, parent)
        marker_written = False
        marker: dict[str, Any] | None = None
        try:
            master_new_sha256 = _write_pair_new_keeper(master_new, master)
            state_new_sha256 = _write_pair_new_keeper(
                state_new,
                normalized_state,
            )
            _fsync_directory(parent)

            marker = {
                "schema_version": _PAIR_TRANSACTION_SCHEMA_VERSION,
                "transaction_id": transaction_id,
            }
            for member, target, new_path, new_sha256 in (
                ("master", master_target, master_new, master_new_sha256),
                ("state", state_target, state_new, state_new_sha256),
            ):
                _reject_symlink_or_nonregular(target, allow_missing=True)
                old_exists = target.exists()
                old_name = (
                    _pair_artifact_name(transaction_id, member, "old")
                    if old_exists
                    else None
                )
                old_sha256 = None
                if old_exists:
                    os.chmod(target, 0o600, follow_symlinks=False)
                    old_path = parent / str(old_name)
                    _link_regular(target, old_path)
                    old_sha256 = _file_sha256(old_path)
                marker["source_state" if member == "state" else member] = {
                    "target": target.name,
                    "new": new_path.name,
                    "old": old_name,
                    "old_exists": old_exists,
                    "new_sha256": new_sha256,
                    "old_sha256": old_sha256,
                }
            _fsync_directory(parent)
            _atomic_write_json(marker_path, marker)
            marker_written = True

            _replace_pair_member(
                state_new,
                state_target,
                transaction_id=transaction_id,
                member="state",
                role="install",
            )
            _replace_pair_member(
                master_new,
                master_target,
                transaction_id=transaction_id,
                member="master",
                role="install",
            )
            _recover_security_master_pair_locked(
                master_target,
                state_target,
                parent,
            )
        except BaseException:
            # A live writer exception is an abort, even if both new inodes
            # already landed. Crash-time recovery may commit a complete pair,
            # but a caught exception must restore the exact prior pair before
            # the primary failure is re-raised.
            try:
                if marker is not None and (
                    marker_written
                    or marker_path.exists()
                    or marker_path.is_symlink()
                ):
                    # The validated in-memory recipe is authoritative for this
                    # live writer. Reading a possibly half-cleaned marker here
                    # would introduce a second failure mode before rollback.
                    _force_restore_old_security_master_pair_locked(
                        master_target,
                        state_target,
                        parent,
                        marker,
                    )
                else:
                    _cleanup_orphan_pair_artifacts_locked(parent)
            except BaseException:
                # Cleanup/rollback diagnostics must never mask the original
                # BaseException. All recovery inputs remain 0600 and durable
                # for the next authoritative load if the OS is still failing.
                pass
            raise


def normalized_security_master_bytes(master: Mapping[str, Any]) -> bytes:
    """Return deterministic mapping output without fetch-time audit clocks.

    SEC downloads performed at different wall-clock times legitimately produce
    different successful-check timestamps. Cutover reproducibility compares
    this normalized representation, which retains every identity, decision,
    evidence hash, source URL, and settlement/filing date while excluding only
    operational fetch/check clocks and the digest derived from those clocks.
    """

    _validate_security_master(master)
    normalized = copy.deepcopy(dict(master))
    normalized.pop("generated_at", None)
    normalized.pop("source_state_sha256", None)
    audit = normalized.get("audit")
    if isinstance(audit, dict):
        for field in (
            "as_of",
            "ftd_source_age_days",
            "source_stale",
            "required_current_source_checkpoints",
            "fund_series_source_checkpoints",
            "sec_ixbrl_source_checkpoints",
        ):
            audit.pop(field, None)
    for record in normalized.get("records", {}).values():
        if isinstance(record, dict):
            evidence = record.get("fund_series_evidence")
            if isinstance(evidence, dict):
                evidence.pop("verified_at", None)
    return _canonical_json_bytes(normalized)


def normalized_source_state_evidence_bytes(
    state: Mapping[str, Any],
) -> bytes:
    """Return SEC source/evidence identity without operational fetch clocks.

    Reproducibility comparisons must retain every SEC URL, content checksum,
    parsed record, filter boundary, and EDGAR decision.  Only the exact fields
    below record when already-validated content was fetched, checked, or
    accepted; removing keys recursively by name would be unsafe because a
    future schema could give the same name a substantive meaning elsewhere.
    """

    normalized = _normalize_source_state(state)
    _validate_source_state(normalized)
    evidence = copy.deepcopy(normalized)
    evidence.pop("updated_at", None)

    for source in evidence["sources"].values():
        source.pop("accepted_at", None)
        source.pop("last_successful_check_at", None)

    edgar_evidence = evidence.get("edgar_evidence")
    if isinstance(edgar_evidence, dict) and edgar_evidence:
        edgar_evidence.pop("generated_at", None)

    edgar_discovery = evidence.get("edgar_discovery")
    if isinstance(edgar_discovery, dict):
        records = edgar_discovery.get("records")
        if isinstance(records, dict):
            for record in records.values():
                if isinstance(record, dict):
                    record.pop("checked_at", None)
                    record.pop("last_successful_check_at", None)

    return _canonical_json_bytes(evidence)


def _normalize_security_universe(
    securities: (
        Mapping[str, str | Iterable[str] | Mapping[str, Any]]
        | Iterable[tuple[str, str] | Mapping[str, Any]]
        | None
    ),
    state: Mapping[str, Any],
) -> list[dict[str, Any]]:
    collected: dict[tuple[str, str], dict[str, Any]] = {}

    evidence_fields = {
        "reported_cusip",
        "reported_issuer",
        "reported_class",
        "accession",
        "report_date",
        "url",
        "sha256",
    }

    def normalize_evidence(
        raw: object,
        *,
        cusip: str,
        issuer: str,
        security_class: str,
    ) -> dict[str, str]:
        if not isinstance(raw, Mapping) or set(raw) != evidence_fields:
            raise SecurityMasterError("invalid reported-identity evidence record")
        raw_url = str(raw.get("url") or "").strip()
        normalized = {
            "reported_cusip": normalize_cusip(raw.get("reported_cusip")),
            "reported_issuer": " ".join(
                str(raw.get("reported_issuer") or "").split()
            ),
            "reported_class": " ".join(
                str(raw.get("reported_class") or "").split()
            ),
            "accession": str(raw.get("accession") or "").strip(),
            "report_date": str(raw.get("report_date") or "").strip(),
            "url": _normalize_reported_identity_source_url(
                raw_url,
                accession=raw.get("accession"),
            ),
            "sha256": str(raw.get("sha256") or ""),
        }
        if (
            normalized["reported_cusip"] != cusip
            or normalized["reported_issuer"] != issuer
            or normalized["reported_class"] != security_class
            or not _SEC_ACCESSION_RE.fullmatch(normalized["accession"])
            or _parse_settlement_date(normalized["report_date"])
            != normalized["report_date"]
            or normalized["url"] != raw_url
            or not _SHA256_RE.fullmatch(normalized["sha256"])
        ):
            raise SecurityMasterError("reported-identity evidence conflicts with row")
        return normalized

    def add(
        cusip: object | None,
        instrument_type: object | None,
        issuer: object | None = None,
        security_class: object | None = None,
        reported_identity_evidence: object | None = None,
    ) -> None:
        normalized_cusip = normalize_cusip(cusip)
        if not normalized_cusip:
            return
        normalized_type = normalize_instrument_type(instrument_type)
        values = collected.setdefault(
            (normalized_cusip, normalized_type),
            {
                "issuers": set(),
                "classes": set(),
                "identities": set(),
                "evidence": {},
            },
        )
        normalized_issuer = " ".join(str(issuer or "").split())
        normalized_class = " ".join(str(security_class or "").split())
        if normalized_issuer:
            values["issuers"].add(normalized_issuer)
        if normalized_class:
            values["classes"].add(normalized_class)
        if issuer is not None or security_class is not None:
            values["identities"].add(
                (normalized_cusip, normalized_issuer, normalized_class)
            )
        if reported_identity_evidence is not None:
            if not isinstance(reported_identity_evidence, list):
                raise SecurityMasterError(
                    "reported-identity evidence must be a list"
                )
            for raw in reported_identity_evidence:
                evidence = normalize_evidence(
                    raw,
                    cusip=normalized_cusip,
                    issuer=normalized_issuer,
                    security_class=normalized_class,
                )
                digest = json.dumps(
                    evidence,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                values["evidence"][digest] = evidence

    if securities is None:
        for cusip in state.get("ftd_timeline", {}):
            add(cusip, "EQUITY")
        for contribution in state.get("ftd_mutable_tail", {}).values():
            if not isinstance(contribution, Mapping):
                continue
            for record in contribution.get("records", []):
                if isinstance(record, Mapping) and record.get("cusip"):
                    add(record["cusip"], "EQUITY")
        securities = []

    if isinstance(securities, Mapping):
        iterable: Iterable[Any] = securities.items()
        for cusip, raw_value in iterable:
            if isinstance(raw_value, Mapping):
                type_values: Any = raw_value.get("instrument_type", "EQUITY")
                issuer = raw_value.get("reported_issuer", raw_value.get("issuer"))
                security_class = raw_value.get(
                    "reported_class",
                    raw_value.get("security_class", raw_value.get("class")),
                )
                reported_identity_evidence = raw_value.get(
                    "reported_identity_evidence"
                )
            else:
                type_values = raw_value
                issuer = None
                security_class = None
                reported_identity_evidence = None
            if isinstance(type_values, str):
                type_values = [type_values]
            try:
                for instrument_type in type_values:
                    add(
                        cusip,
                        instrument_type,
                        issuer,
                        security_class,
                        reported_identity_evidence,
                    )
            except TypeError as exc:
                raise SecurityMasterError(
                    "security universe instrument types must be strings or iterables"
                ) from exc
    else:
        for item in securities:
            if isinstance(item, Mapping):
                add(
                    item.get("cusip"),
                    item.get("instrument_type", "EQUITY"),
                    item.get("reported_issuer", item.get("issuer")),
                    item.get(
                        "reported_class",
                        item.get("security_class", item.get("class")),
                    ),
                    item.get("reported_identity_evidence"),
                )
            else:
                try:
                    cusip, instrument_type = item
                except (TypeError, ValueError) as exc:
                    raise SecurityMasterError(
                        "security universe entries must be pairs or objects"
                    ) from exc
                add(cusip, instrument_type)

    # The official list is an identity source, not merely a validation list.
    # Always include every active non-option security under the instrument type
    # proved by its exact class description, even when callers also supplied a
    # repository-observed universe. This lets an official preferred, warrant,
    # or convertible-note class correct a legacy broad EQUITY parse without
    # overwriting the as-filed row.
    for entry in state.get("sources", {}).values():
        if entry.get("kind") != "sec_13f_list":
            continue
        for record in entry.get("records", []):
            if not isinstance(record, dict) or record.get("status") == "*D*":
                continue
            description = " ".join(
                str(record.get("description") or "").upper().split()
            )
            if not record.get("cusip") or description in {"CALL", "PUT"}:
                continue
            instrument_type = "EQUITY"
            if _DEBT_CLASS_RE.search(description):
                instrument_type = "NOTE"
            elif _PREFERRED_CLASS_RE.search(description):
                instrument_type = "PREF"
            elif _WARRANT_CLASS_RE.search(description):
                instrument_type = "WARRANT"
            # Only union the official identity into the caller-supplied
            # universe.  Official-list issuer/class values are authoritative
            # evidence consumed separately by ``_official_13f_index`` below;
            # treating them as filer-reported metadata would manufacture a
            # conflict whenever they legitimately correct a broad or option
            # parse (for example reported ``CALL`` versus official ``COM``).
            add(record["cusip"], instrument_type)

    normalized: list[dict[str, Any]] = []
    for (cusip, instrument_type), values in sorted(collected.items()):
        issuers = sorted(values["issuers"])
        classes = sorted(values["classes"])
        record: dict[str, Any] = {
            "cusip": cusip,
            "instrument_type": instrument_type,
        }
        if issuers:
            record["reported_issuer"] = issuers[0]
            record["reported_issuers"] = issuers
        if classes:
            record["reported_class"] = classes[0]
            record["reported_classes"] = classes
        if len(issuers) > 1 or len(classes) > 1:
            record["universe_metadata_conflict"] = True
        identities = [{
            "reported_cusip": identity_cusip,
            "reported_issuer": issuer,
            "reported_class": security_class,
        } for identity_cusip, issuer, security_class in values["identities"]]
        identities.sort(
            key=lambda identity: json.dumps(
                identity,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        if identities:
            record["reported_identities"] = identities
        if values["evidence"]:
            witnesses: dict[tuple[str, str, str], tuple[str, dict[str, str]]] = {}
            for digest, evidence in values["evidence"].items():
                identity = (
                    evidence["reported_cusip"],
                    evidence["reported_issuer"],
                    evidence["reported_class"],
                )
                prior = witnesses.get(identity)
                if prior is None or digest < prior[0]:
                    witnesses[identity] = (digest, evidence)
            record["reported_identity_evidence"] = sorted(
                (item[1] for item in witnesses.values()),
                key=lambda evidence: json.dumps(
                    evidence,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        normalized.append(record)
    return normalized


def _aggregate_ftd_evidence(
    state: Mapping[str, Any],
    target_cusips: Iterable[object] | None = None,
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
    date | None,
]:
    """Project stable intervals plus the two reversible tail archives.

    Only a bounded set of latest exact observations enters the master. The
    complete time-versioned history remains represented by compact interval
    counts and boundaries, so memory and JSON size scale with symbol changes
    rather than every FTD settlement date.
    """

    target = (
        set(_normalized_filter_universe(target_cusips))
        if target_cusips is not None
        else None
    )
    stable = state.get("ftd_timeline", {})
    tail_records: dict[str, list[tuple[str, str, Mapping[str, Any]]]] = (
        defaultdict(list)
    )
    for url, contribution in state.get("ftd_mutable_tail", {}).items():
        if not isinstance(contribution, Mapping):
            continue
        sha256 = str(contribution.get("sha256") or "")
        for record in contribution.get("records", []):
            if not isinstance(record, Mapping):
                continue
            cusip = normalize_cusip(record.get("cusip"))
            if cusip and (target is None or cusip in target):
                tail_records[cusip].append((url, sha256, record))

    universe = set(tail_records)
    universe.update(
        cusip
        for cusip in stable
        if target is None or cusip in target
    )
    evidence_by_cusip: dict[str, list[dict[str, Any]]] = {}
    intervals_by_cusip: dict[str, list[dict[str, Any]]] = {}
    for cusip in sorted(universe):
        projected = {
            cusip: json.loads(json.dumps(stable.get(cusip, [])))
        }
        records_by_source: dict[tuple[str, str], list[Mapping[str, Any]]] = (
            defaultdict(list)
        )
        for url, sha256, record in tail_records.get(cusip, []):
            records_by_source[(url, sha256)].append(record)
        for (url, sha256), records in sorted(
            records_by_source.items(), key=lambda item: _ftd_url_sort_key(item[0][0])
        ):
            observations = _ftd_observations_from_archive_records(
                records,
                source_url=url,
                source_sha256=sha256,
            )
            _append_ftd_observations_to_timeline(projected, observations)
        intervals = projected.get(cusip, [])
        master_intervals = [
            {
                field: json.loads(json.dumps(interval[field]))
                for field in (
                    "symbols",
                    "symbol",
                    "first_seen",
                    "last_seen",
                    "observation_dates",
                    "observation_date_count",
                    "observation_count",
                    "sources",
                    "descriptions",
                    "symbol_descriptions",
                )
            }
            for interval in intervals
        ]
        intervals_by_cusip[cusip] = master_intervals
        exact = [
            observation
            for interval in intervals
            for observation in interval.get("observations", [])
        ]
        exact_dates = sorted({item["settlement_date"] for item in exact})
        retained_dates = set(exact_dates[-FTD_MAX_RECENT_EXACT_DATES:])
        evidence_by_cusip[cusip] = sorted(
            (
                json.loads(json.dumps(item))
                for item in exact
                if item["settlement_date"] in retained_dates
            ),
            key=lambda item: (item["settlement_date"], item["symbol"]),
        )

    latest_values = [
        _parse_settlement_date(source.get("last_settlement_date"))
        for source in state.get("sources", {}).values()
        if isinstance(source, Mapping)
        and source.get("kind") == "sec_ftd_archive"
    ]
    global_latest_text = max((item for item in latest_values if item), default=None)
    return (
        evidence_by_cusip,
        intervals_by_cusip,
        date.fromisoformat(global_latest_text) if global_latest_text else None,
    )


def project_ftd_master_evidence(
    state: Mapping[str, Any],
    target_cusips: Iterable[object] | None = None,
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
    date | None,
]:
    """Return the bounded deterministic master projection for validation."""

    normalized = _normalize_source_state(state)
    _validate_source_state(normalized)
    return _aggregate_ftd_evidence(normalized, target_cusips)


def _build_symbol_intervals(
    evidence: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build bounded symbol-set intervals from exact observations."""

    by_cusip = {"_": list(evidence)}
    timeline: dict[str, list[dict[str, Any]]] = {}
    _append_ftd_observations_to_timeline(timeline, by_cusip)
    return [
        {
            field: json.loads(json.dumps(interval[field]))
            for field in (
                "symbols",
                "symbol",
                "first_seen",
                "last_seen",
                "observation_dates",
                "observation_date_count",
                "observation_count",
                "sources",
                "descriptions",
                "symbol_descriptions",
            )
        }
        for interval in timeline.get("_", [])
    ]


def _validation_symbol_metadata(
    state: Mapping[str, Any],
) -> tuple[
    dict[str, list[str]],
    dict[str, list[str]],
    dict[str, list[str]],
]:
    symbol_sources: dict[str, set[str]] = defaultdict(set)
    symbol_titles: dict[str, set[str]] = defaultdict(set)
    symbol_exchanges: dict[str, set[str]] = defaultdict(set)
    for source in state.get("sources", {}).values():
        kind = source.get("kind")
        if kind not in _VALIDATION_SOURCE_KINDS:
            continue
        for raw_symbol in source.get("symbols", []):
            symbol = _normalize_symbol(raw_symbol)
            if symbol:
                symbol_sources[symbol].add(kind)
        for raw_symbol, raw_titles in source.get("symbol_titles", {}).items():
            symbol = _normalize_symbol(raw_symbol)
            if not symbol or not isinstance(raw_titles, list):
                continue
            for raw_title in raw_titles:
                title = " ".join(str(raw_title or "").split())
                if title:
                    symbol_titles[symbol].add(title)
        for raw_symbol, raw_exchanges in source.get(
            "symbol_exchanges", {}
        ).items():
            symbol = _normalize_symbol(raw_symbol)
            if not symbol or not isinstance(raw_exchanges, list):
                continue
            for raw_exchange in raw_exchanges:
                exchange = " ".join(str(raw_exchange or "").split())
                if exchange:
                    symbol_exchanges[symbol].add(exchange)
    return ({
        symbol: sorted(kinds)
        for symbol, kinds in sorted(symbol_sources.items())
    }, {
        symbol: sorted(titles)
        for symbol, titles in sorted(symbol_titles.items())
    }, {
        symbol: sorted(exchanges)
        for symbol, exchanges in sorted(symbol_exchanges.items())
    })


def _fund_series_name_evidence(
    state: Mapping[str, Any],
) -> dict[str, dict[str, str]]:
    """Resolve unique SEC fund symbols to checksummed series/class names."""

    fund_records: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    pages_by_cik: dict[str, tuple[str, Mapping[str, Any]]] = {}
    for url, source in state.get("sources", {}).items():
        if source.get("kind") == "sec_fund_tickers":
            for record in source.get("fund_records", []):
                if not isinstance(record, Mapping):
                    continue
                symbol = _normalize_symbol(record.get("symbol"))
                cik = str(record.get("cik") or "")
                series_id = str(record.get("series_id") or "")
                class_id = str(record.get("class_id") or "")
                if symbol:
                    fund_records[symbol].add((cik, series_id, class_id))
        elif source.get("kind") == "sec_fund_series":
            cik = str(source.get("cik") or "")
            pages_by_cik[cik] = (url, source)

    resolved: dict[str, dict[str, str]] = {}
    for symbol, candidates in sorted(fund_records.items()):
        if len(candidates) != 1:
            continue
        cik, series_id, class_id = next(iter(candidates))
        page = pages_by_cik.get(cik)
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
        verified_at = _source_successful_check_at(source)
        if verified_at is None:
            continue
        resolved[symbol] = {
            "symbol": symbol,
            "name": name,
            "cik": cik,
            "series_id": series_id,
            "class_id": class_id,
            "url": url,
            "sha256": str(source.get("sha256") or ""),
            "verified_at": verified_at,
        }
    return resolved


def _official_13f_index(
    state: Mapping[str, Any],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str] | None]:
    candidates: list[tuple[str, str, Mapping[str, Any]]] = []
    for url, source in state.get("sources", {}).items():
        if source.get("kind") != "sec_13f_list":
            continue
        period = str(source.get("list_period") or "")
        candidates.append((period, url, source))
    if not candidates:
        return {}, None
    period, url, source = max(candidates, key=lambda item: (item[0], item[1]))
    by_cusip: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw_record in source.get("records", []):
        if not isinstance(raw_record, dict):
            continue
        cusip = normalize_cusip(raw_record.get("cusip"))
        if cusip:
            by_cusip[cusip].append(dict(raw_record))
    for records in by_cusip.values():
        records.sort(
            key=lambda item: (
                item.get("description") or "",
                item.get("issuer") or "",
                item.get("status") or "",
            )
        )
    return dict(by_cusip), {
        "period": period,
        "url": url,
        "sha256": str(source.get("sha256") or ""),
    }


def _required_current_source_checkpoints(
    state: Mapping[str, Any],
    *,
    official_source: Mapping[str, str] | None,
) -> dict[str, dict[str, str | None]]:
    """Return the successful-check proof used by the 45-day source gate."""

    sources = state.get("sources", {})
    expected_urls = {
        "sec_company_tickers": SEC_COMPANY_TICKERS_URL,
        "sec_company_exchange_tickers": SEC_COMPANY_EXCHANGE_TICKERS_URL,
        "sec_fund_tickers": SEC_FUND_TICKERS_URL,
        "sec_ftd_index": FTD_PAGE_URL,
        "sec_13f_list_index": OFFICIAL_13F_LIST_PAGE_URL,
        "sec_13f_list": (
            str(official_source.get("url") or "")
            if official_source is not None
            else None
        ),
    }

    checkpoints: dict[str, dict[str, str | None]] = {}
    for kind in sorted(_REQUIRED_CURRENT_SOURCE_KINDS):
        expected_url = expected_urls[kind]
        source: Mapping[str, Any] | None = None
        source_url: str | None = None
        if expected_url and isinstance(sources.get(expected_url), Mapping):
            source_url = expected_url
            source = sources[expected_url]
        else:
            candidates = [
                (url, entry)
                for url, entry in sources.items()
                if isinstance(entry, Mapping) and entry.get("kind") == kind
            ]
            if candidates:
                source_url, source = max(
                    candidates,
                    key=lambda item: (
                        _source_successful_check_at(item[1]) or "",
                        item[0],
                    ),
                )
        checkpoints[kind] = {
            "url": source_url or expected_url,
            "last_successful_check_at": (
                _source_successful_check_at(source) if source is not None else None
            ),
        }
    return checkpoints


def _current_symbol_source_populations(
    state: Mapping[str, Any],
) -> tuple[dict[str, int], dict[str, int]]:
    populations = {kind: 0 for kind in sorted(_VALIDATION_SOURCE_KINDS)}
    title_populations = {kind: 0 for kind in sorted(_VALIDATION_SOURCE_KINDS)}
    for source in state.get("sources", {}).values():
        if not isinstance(source, Mapping):
            continue
        kind = str(source.get("kind") or "")
        if kind not in _VALIDATION_SOURCE_KINDS:
            continue
        symbols = source.get("symbols", [])
        titles = source.get("symbol_titles", {})
        if not isinstance(symbols, list) or not isinstance(titles, Mapping):
            continue
        symbol_set = set(symbols)
        populations[kind] = max(populations[kind], len(symbol_set))
        if kind == "sec_fund_tickers":
            # company_tickers_mf.json identifies the exact series/class but
            # does not consistently carry a human-readable title. Treat a
            # complete series/class identity as its contract-level metadata
            # sanity check; fund-series pages supply names separately.
            fund_records = source.get("fund_records", [])
            identified_symbols = {
                str(record.get("symbol") or "").strip().upper()
                for record in fund_records
                if isinstance(record, Mapping)
                and str(record.get("cik") or "").strip()
                and str(record.get("series_id") or "").strip()
                and str(record.get("class_id") or "").strip()
            }
            titled = max(
                len(symbol_set.intersection(identified_symbols)),
                sum(
                    1
                    for symbol in symbol_set
                    if isinstance(titles.get(symbol), list)
                    and any(
                        str(title or "").strip()
                        for title in titles[symbol]
                    )
                ),
            )
        else:
            titled = sum(
                1
                for symbol in symbol_set
                if isinstance(titles.get(symbol), list)
                and any(str(title or "").strip() for title in titles[symbol])
            )
        title_populations[kind] = max(title_populations[kind], titled)
    return populations, title_populations


def _resolved_mapping_counts(
    records: Mapping[str, Any],
) -> dict[str, int]:
    return {
        source: sum(
            1
            for record in records.values()
            if isinstance(record, Mapping)
            and record.get("mapping_status") == "resolved"
            and record.get("ticker_source") == source
        )
        for source in sorted(VALID_TICKER_SOURCES)
    }


def _build_master_audit(
    state: Mapping[str, Any],
    *,
    evidence_by_cusip: Mapping[str, list[dict[str, Any]]],
    global_latest_date: date | None,
    official_index: Mapping[str, list[dict[str, Any]]],
    official_source: Mapping[str, str] | None,
) -> dict[str, Any]:
    """Build deterministic coverage and freshness metadata from private state."""

    active_cusips = sorted({
        cusip
        for cusip, rows in official_index.items()
        if any(
            row.get("status") != "*D*"
            and str(row.get("description") or "").strip().upper()
            not in {"CALL", "PUT"}
            for row in rows
        )
    })
    coverage_cutoff = (
        global_latest_date - timedelta(days=DEFAULT_FTD_COVERAGE_WINDOW_DAYS)
        if global_latest_date is not None
        else None
    )
    evidenced_cusips = sorted(
        cusip
        for cusip in active_cusips
        if coverage_cutoff is not None
        and any(
            date.fromisoformat(item["settlement_date"]) >= coverage_cutoff
            for item in evidence_by_cusip.get(cusip, [])
        )
    )
    active_count = len(active_cusips)
    evidenced_count = len(evidenced_cusips)
    coverage_ratio = (
        round(evidenced_count / active_count, 8) if active_count else 0.0
    )
    audit_as_of = _parse_calendar_date(state.get("updated_at"))
    ftd_age_days = (
        max(0, (audit_as_of - global_latest_date).days)
        if audit_as_of is not None and global_latest_date is not None
        else None
    )
    malformed_active_count = sum(
        1 for cusip in active_cusips if cusip_quarantine_reason(cusip)
    )
    filter_digest = state.get("current_filter_universe_sha256")
    filter_profile = state.get("filter_universes", {}).get(filter_digest, {})
    filter_cusips = set(filter_profile.get("cusips", []))
    required_filter_urls = list(
        state.get("required_filter_coverage_urls", [])
    )
    covered_filter_urls = [
        url
        for url in required_filter_urls
        if isinstance(state.get("sources", {}).get(url), Mapping)
        and _archive_filter_covers(
            state["sources"][url],
            state,
            filter_cusips,
        )
    ]
    incomplete_filter_urls = sorted(
        set(required_filter_urls) - set(covered_filter_urls)
    )
    symbol_populations, title_populations = _current_symbol_source_populations(
        state
    )
    return {
        "schema_version": MASTER_AUDIT_SCHEMA_VERSION,
        "as_of": audit_as_of.isoformat() if audit_as_of else None,
        "official_13f_period": (
            official_source.get("period") if official_source else None
        ),
        "active_non_option_official_cusip_count": active_count,
        # Publish the set's digest, not the SEC's complete official list.
        "active_non_option_official_cusips_sha256": _payload_sha256(
            "\n".join(active_cusips).encode("ascii")
        ),
        "malformed_active_official_cusip_count": malformed_active_count,
        "ftd_evidenced_official_cusip_count": evidenced_count,
        "ftd_coverage_ratio": coverage_ratio,
        "ftd_coverage_window_days": DEFAULT_FTD_COVERAGE_WINDOW_DAYS,
        "ftd_coverage_cutoff_date": (
            coverage_cutoff.isoformat() if coverage_cutoff else None
        ),
        "latest_ftd_settlement_date": (
            global_latest_date.isoformat() if global_latest_date else None
        ),
        "ftd_source_age_days": ftd_age_days,
        "source_staleness_threshold_days": DEFAULT_SOURCE_STALENESS_DAYS,
        "source_stale": (
            ftd_age_days is None
            or ftd_age_days > DEFAULT_SOURCE_STALENESS_DAYS
        ),
        "successful_check_checkpoint_days": (
            DEFAULT_SUCCESSFUL_CHECK_CHECKPOINT_DAYS
        ),
        "required_current_source_checkpoints": (
            _required_current_source_checkpoints(
                state,
                official_source=official_source,
            )
        ),
        "ftd_filter_universe_sha256": filter_digest,
        "ftd_filter_universe_count": len(filter_cusips),
        "required_filtered_archive_count": len(required_filter_urls),
        "covered_filtered_archive_count": len(covered_filter_urls),
        "incomplete_filtered_archive_urls": incomplete_filter_urls,
        "filter_universe_coverage_complete": not incomplete_filter_urls,
        "source_schema_sha256_by_kind": _source_schema_fingerprints_by_kind(
            state
        ),
        "current_symbol_population_by_kind": symbol_populations,
        "current_symbol_title_population_by_kind": title_populations,
        # Replaced from the final record set immediately before validation.
        "resolved_mapping_count_by_ticker_source": {
            source: 0 for source in sorted(VALID_TICKER_SOURCES)
        },
    }


def project_master_audit(
    master: Mapping[str, Any],
    source_state: Mapping[str, Any],
) -> dict[str, Any]:
    """Recompute every audit claim from the retained SEC evidence.

    The audit object is a publication contract, not an independent source of
    truth. Reprojecting it here prevents plausible, internally consistent
    counts from bypassing the production population and coverage gates.
    """

    current = dict(master)
    _validate_security_master(current)
    state = _normalize_source_state(source_state)
    _validate_source_state(state)
    official_index, official_source = _official_13f_index(state)
    evidence_by_cusip, _intervals_by_cusip, global_latest_date = (
        _aggregate_ftd_evidence(state, official_index)
    )
    projected = _build_master_audit(
        state,
        evidence_by_cusip=evidence_by_cusip,
        global_latest_date=global_latest_date,
        official_index=official_index,
        official_source=official_source,
    )
    records = current.get("records", {})
    projected["fund_series_source_checkpoints"] = (
        _fund_series_source_checkpoints(records)
    )
    projected["sec_ixbrl_source_checkpoints"] = (
        _sec_ixbrl_source_checkpoints(records, state)
    )
    projected["resolved_mapping_count_by_ticker_source"] = (
        _resolved_mapping_counts(records)
    )
    (
        projected["reported_identity_count"],
        projected["evidenced_reported_identity_count"],
    ) = _reported_identity_evidence_counts(records)
    return projected


_ISSUER_TRAILING_CLASS_RE = re.compile(
    r"\b(?:CLASS|CL)\s+[A-Z0-9]+$|"
    r"\b(?:COM(?:MON)?(?:\s+STOCK)?|ORD(?:INARY)?(?:\s+SHS?)?|"
    r"SHS?|STOCK|ETF|ETN|ADR|ADS|WARRANTS?|WTS?)$"
)
_ISSUER_SEMICOLON_CLASS_RE = re.compile(
    r";\s*(?:COM(?:MON)?|ORD(?:INARY)?|CL(?:ASS)?|PFD|PREF(?:ERRED)?|"
    r"WARRANTS?|WTS?|ADRS?|ADS|SHS?|UNITS?)\b.*$"
)
_ISSUER_SUFFIXES = frozenset({
    "CO",
    "COMPANY",
    "CORP",
    "CORPORATION",
    "INC",
    "INCORPORATED",
    "HOLDING",
    "HOLDINGS",
    "LTD",
    "LIMITED",
    "LLC",
    "LP",
    "PLC",
})


def _issuer_proof_key(value: object | None) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = text.upper().replace("&", " AND ")
    # FTD DESCRIPTION commonly appends a security class after a semicolon
    # (for example ``APPLE INC;COM NPV``).  Strip only a recognized class
    # suffix; the issuer portion remains an exact, deterministic comparison.
    text = _ISSUER_SEMICOLON_CLASS_RE.sub("", text).strip()
    text = _ISSUER_TRAILING_CLASS_RE.sub("", text).strip()
    tokens = re.findall(r"[A-Z0-9]+", text)
    if tokens and tokens[0] == "THE":
        tokens = tokens[1:]
    while tokens and tokens[-1] in _ISSUER_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def _issuer_compatible(left: object | None, right: object | None) -> bool:
    left_key = _issuer_proof_key(left)
    right_key = _issuer_proof_key(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    # SEC ticker metadata can use a successor holding-company title while the
    # 13F list and FTD archive retain the operating-company spelling.  Treat
    # spacing-only variants as exact after removing corporate suffixes; this
    # is deterministic and deliberately does not admit fuzzy name matches.
    if left_key.replace(" ", "") == right_key.replace(" ", ""):
        return True
    shorter, longer = sorted((left_key, right_key), key=len)
    # Official-list and FTD issuer fields are fixed-width and can truncate a
    # longer name.  Allow only a substantial word-boundary prefix.
    return (
        len(shorter) >= 12
        and len(shorter.split()) >= 2
        and longer.startswith(shorter)
        and (len(longer) == len(shorter) or longer[len(shorter)] == " ")
    )


def _issuer_conflict_reason(
    *,
    reported_issuers: list[str],
    ftd_descriptions: list[str],
    sec_titles: list[str],
    official_issuers: list[str],
) -> str | None:
    authoritative = reported_issuers or official_issuers
    comparison_groups = [
        ("ftd_description", ftd_descriptions),
        ("sec_company_title", sec_titles),
        ("official_13f_issuer", official_issuers),
    ]
    if authoritative:
        for group_name, values in comparison_groups:
            if values and any(
                not any(
                    _issuer_compatible(expected, observed)
                    for expected in authoritative
                )
                for observed in values
            ):
                return f"issuer_conflict_with_{group_name}"
    elif ftd_descriptions and sec_titles and not any(
        _issuer_compatible(description, title)
        for description in ftd_descriptions
        for title in sec_titles
    ):
        return "issuer_conflict_between_ftd_and_sec_company_title"
    return None


def _class_conflict_reason(
    instrument_type: str,
    reported_classes: list[str],
    official_descriptions: list[str],
) -> str | None:
    reported_values = [
        " ".join(str(value or "").upper().split())
        for value in reported_classes
        if str(value or "").strip()
    ]
    official_values = [
        " ".join(str(value or "").upper().split())
        for value in official_descriptions
        if str(value or "").strip()
    ]
    official_types = set()
    for value in official_values:
        if _DEBT_CLASS_RE.search(value):
            official_types.add("NOTE")
        elif _PREFERRED_CLASS_RE.search(value):
            official_types.add("PREF")
        elif _WARRANT_CLASS_RE.search(value):
            official_types.add("WARRANT")
        else:
            official_types.add("EQUITY")
    if official_types and official_types != {instrument_type}:
        return "official_13f_class_conflicts_with_instrument_type"
    # Form 13F option rows use the underlying CUSIP.  Pipeline integration may
    # therefore create a companion EQUITY identity from a CALL/PUT position.
    # Permit that companion only when the exact official-list CUSIP independently
    # identifies a non-option security class.
    derived_option_underlying = (
        instrument_type == "EQUITY"
        and reported_values
        and all(_OPTION_CLASS_RE.search(value) for value in reported_values)
        and bool(official_values)
        and not any(_OPTION_CLASS_RE.search(value) for value in official_values)
    )
    if official_values and reported_values and not derived_option_underlying:
        def reported_type(value: str) -> str:
            if _OPTION_CLASS_RE.search(value):
                return "OPTION"
            if _DEBT_CLASS_RE.search(value):
                return "NOTE"
            if _PREFERRED_CLASS_RE.search(value):
                return "PREF"
            if _WARRANT_CLASS_RE.search(value):
                return "WARRANT"
            return "EQUITY"

        reported_types = [reported_type(value) for value in reported_values]
        if instrument_type not in reported_types:
            if set(reported_types) == {"NOTE"}:
                return "debt_class_not_eligible_for_ftd_symbol"
            if set(reported_types) == {"OPTION"}:
                return "option_class_not_eligible_for_direct_ftd_symbol"
            return "reported_class_conflicts_with_official_13f_identity"

        reported_designators = {
            designator
            for value in reported_values
            for designator in _sec_edgar_class_profile(value)[1]
        }
        official_designators = {
            designator
            for value in official_values
            for designator in _sec_edgar_class_profile(value)[1]
        }
        if (
            reported_designators
            and official_designators
            and reported_designators.isdisjoint(official_designators)
        ):
            return "official_13f_class_designator_conflicts_with_reported_class"

    # Once one retained filer descriptor is compatible with the exact active
    # official identity, conflicting historical outliers remain evidence but
    # do not overrule the SEC's current CUSIP/class record.  Without an active
    # official row, retain the stricter all-reported-values policy.
    values = official_values if official_values else reported_values
    if any(_OPTION_CLASS_RE.search(value) for value in values):
        return "option_class_not_eligible_for_direct_ftd_symbol"
    if any(_DEBT_CLASS_RE.search(value) for value in values):
        return "debt_class_not_eligible_for_ftd_symbol"
    if instrument_type == "PREF" and reported_classes and not any(
        _PREFERRED_CLASS_RE.search(value) for value in values
    ):
        return "preferred_instrument_lacks_preferred_class_evidence"
    if instrument_type == "WARRANT" and reported_classes and not any(
        _WARRANT_CLASS_RE.search(value) for value in values
    ):
        return "warrant_instrument_lacks_warrant_class_evidence"
    return None


def _master_reported_text_values(
    entry: Mapping[str, Any],
    *,
    singular: str,
    plural: str,
    key: str,
) -> list[str]:
    """Return normalized immutable filing values or reject malformed state."""

    values: list[str] = []
    raw_singular = entry.get(singular)
    if raw_singular is not None:
        if not isinstance(raw_singular, str):
            raise SecurityMasterError(
                f"resolved record has malformed {singular}: {key}"
            )
        normalized = " ".join(raw_singular.split())
        if normalized:
            values.append(normalized)
    raw_plural = entry.get(plural)
    if raw_plural is not None:
        if not isinstance(raw_plural, list) or any(
            not isinstance(value, str) for value in raw_plural
        ):
            raise SecurityMasterError(
                f"resolved record has malformed {plural}: {key}"
            )
        values.extend(
            normalized
            for value in raw_plural
            if (normalized := " ".join(value.split()))
        )
    return sorted(set(values))


def _validate_ftd_master_identity(
    entry: Mapping[str, Any],
    *,
    key: str,
) -> None:
    """Replay the exact as-filed identity checks for one resolved FTD row."""

    reported_issuers = _master_reported_text_values(
        entry,
        singular="reported_issuer",
        plural="reported_issuers",
        key=key,
    )
    reported_classes = _master_reported_text_values(
        entry,
        singular="reported_class",
        plural="reported_classes",
        key=key,
    )

    official = entry.get("official_13f")
    if official is None:
        official_rows: list[Mapping[str, Any]] = []
    elif not isinstance(official, Mapping) or not isinstance(
        official.get("records"), list
    ) or any(
        not isinstance(row, Mapping) for row in official.get("records", [])
    ):
        raise SecurityMasterError(
            f"resolved FTD record has malformed official-list evidence: {key}"
        )
    else:
        official_rows = official["records"]
    active_official_rows = [
        row
        for row in official_rows
        if row.get("status") != "*D*"
        and str(row.get("description") or "").strip().upper()
        not in {"CALL", "PUT"}
    ]
    official_issuers = sorted({
        " ".join(str(row.get("issuer") or "").split())
        for row in active_official_rows
        if str(row.get("issuer") or "").strip()
    })
    official_descriptions = sorted({
        " ".join(str(row.get("description") or "").split())
        for row in active_official_rows
        if str(row.get("description") or "").strip()
    })
    if active_official_rows and (
        len(official_issuers) != 1 or len(official_descriptions) != 1
    ):
        raise SecurityMasterError(
            f"resolved FTD record has conflicting active official-list "
            f"identity evidence: {key}"
        )

    class_conflict = _class_conflict_reason(
        str(entry.get("instrument_type") or ""),
        reported_classes,
        official_descriptions,
    )
    if class_conflict:
        raise SecurityMasterError(
            f"resolved FTD proof conflicts with as-filed class evidence "
            f"({class_conflict}): {key}"
        )

    reported_designators = {
        designator
        for value in reported_classes
        for designator in _sec_edgar_class_profile(value)[1]
    }
    official_designators = {
        designator
        for value in official_descriptions
        for designator in _sec_edgar_class_profile(value)[1]
    }
    if (
        reported_designators
        and official_designators
        and reported_designators.isdisjoint(official_designators)
    ):
        raise SecurityMasterError(
            f"resolved FTD proof conflicts with as-filed class designators: {key}"
        )

    confirmation_dates = set(entry.get("confirmation_dates", []))
    ticker = entry.get("ticker")
    recent_descriptions = sorted({
        description
        for item in entry.get("symbol_evidence", [])
        if item.get("symbol") == ticker
        and item.get("settlement_date") in confirmation_dates
        for description in item.get("descriptions", [])
        if isinstance(description, str) and description
    })
    issuer_conflict = _issuer_conflict_reason(
        reported_issuers=reported_issuers,
        ftd_descriptions=recent_descriptions,
        sec_titles=list(entry.get("symbol_validation_titles", [])),
        official_issuers=official_issuers,
    )
    if issuer_conflict:
        raise SecurityMasterError(
            f"resolved FTD proof conflicts with as-filed issuer evidence "
            f"({issuer_conflict}): {key}"
        )


def _withdraw_superseded_current_symbol(
    record: dict[str, Any],
    *,
    newer_cusips: Iterable[str],
) -> None:
    """Retain dated evidence but withdraw an older CUSIP's current ticker."""

    record.update(
        {
            "mapping_status": "no_listed_symbol",
            "ticker": None,
            "ticker_source": None,
            "ticker_as_of": None,
            "mapping_method": None,
            "effective_from": None,
            "effective_to": None,
            "exchange": None,
            "resolution_reason": "current_symbol_observed_on_newer_cusip",
            "superseded_by_cusips": sorted(set(newer_cusips)),
        }
    )
    record.pop("exchanges", None)
    edgar_evidence = record.get("sec_edgar_evidence")
    if isinstance(edgar_evidence, dict):
        edgar_evidence["status"] = "historical"
        edgar_evidence["reason"] = record["resolution_reason"]


def _reconcile_current_symbol_cusips(
    records: Mapping[str, dict[str, Any]],
    *,
    concurrent_window_days: int,
) -> None:
    """Fail closed when a symbol has moved to a materially newer CUSIP.

    Exact observations for distinct CUSIPs inside the recent window are kept:
    they can coexist during a settlement/corporate-action transition and each
    remains independently SEC-proven. Once one CUSIP's latest proof is more
    than the recent window behind another's, only the newer claim may remain a
    current mapping. Historical intervals and evidence stay on the older row.
    """

    # A newer exact FTD observation is a conflict even before it accumulates
    # enough distinct settlement dates to publish a ticker on the new CUSIP.
    # Keeping only resolved claims here would let the materially older CUSIP
    # remain current during that confirmation gap.  Candidate claims therefore
    # participate in supersession, but never become published mappings.
    by_ticker: dict[str, list[tuple[dict[str, Any], date, bool]]] = defaultdict(
        list
    )
    for record in records.values():
        if record.get("mapping_status") == "resolved":
            ticker = _normalize_symbol(record.get("ticker"))
            ticker_as_of = _parse_settlement_date(record.get("ticker_as_of"))
            if ticker is not None and ticker_as_of is not None:
                by_ticker[ticker].append(
                    (record, date.fromisoformat(ticker_as_of), True)
                )
        candidate = _normalize_symbol(record.get("candidate_ticker"))
        candidate_as_of = _parse_settlement_date(record.get("candidate_as_of"))
        candidate_has_exact_ftd_proof = bool(
            candidate is not None
            and candidate_as_of is not None
            and any(
                item.get("symbol") == candidate
                and item.get("settlement_date") == candidate_as_of
                and item.get("sources")
                for item in record.get("symbol_evidence", [])
                if isinstance(item, Mapping)
            )
        )
        if candidate_has_exact_ftd_proof:
            by_ticker[candidate].append(
                (record, date.fromisoformat(candidate_as_of), False)
            )

    for claims in by_ticker.values():
        claim_cusips = {
            str(record.get("cusip") or "")
            for record, _date, _resolved in claims
        }
        if len(claim_cusips) < 2:
            continue
        newest_as_of = max(
            claim_date for _record, claim_date, _resolved in claims
        )
        current_claims = [
            (record, claim_date)
            for record, claim_date, _resolved in claims
            if (newest_as_of - claim_date).days <= concurrent_window_days
        ]
        current_cusips = sorted(
            {str(record.get("cusip") or "") for record, _date in current_claims}
        )
        for record, claim_as_of, is_resolved in claims:
            newer_cusips = [
                cusip
                for cusip in current_cusips
                if cusip != record.get("cusip")
            ]
            if (
                is_resolved
                and newer_cusips
                and (newest_as_of - claim_as_of).days > concurrent_window_days
            ):
                _withdraw_superseded_current_symbol(
                    record,
                    newer_cusips=newer_cusips,
                )


def _apply_fund_series_names(
    records: Mapping[str, dict[str, Any]],
    state: Mapping[str, Any],
) -> None:
    """Attach only exact, checksummed SEC series/class evidence by symbol."""

    evidence_by_symbol = _fund_series_name_evidence(state)
    for record in records.values():
        record.pop("fund_series_name", None)
        record.pop("fund_series_evidence", None)
        if record.get("mapping_status") != "resolved":
            continue
        symbol = _normalize_symbol(record.get("ticker"))
        evidence = evidence_by_symbol.get(symbol or "")
        if evidence is None:
            continue
        record["fund_series_name"] = evidence["name"]
        record["fund_series_evidence"] = evidence


def rebuild_security_master(
    source_state: Mapping[str, Any] | Path = DEFAULT_SOURCE_STATE_PATH,
    securities: (
        Mapping[str, str | Iterable[str] | Mapping[str, Any]]
        | Iterable[tuple[str, str] | Mapping[str, Any]]
        | None
    ) = None,
    *,
    recent_window_days: int = DEFAULT_RECENT_WINDOW_DAYS,
    max_evidence_age_days: int = DEFAULT_MAX_EVIDENCE_AGE_DAYS,
    min_confirmation_dates: int = DEFAULT_MIN_CONFIRMATION_DATES,
) -> dict[str, Any]:
    """Deterministically rebuild the master from accepted SEC source state."""

    if isinstance(source_state, (str, os.PathLike, Path)):
        state = load_source_state(Path(source_state))
    else:
        state = _normalize_source_state(source_state)
        _validate_source_state(state)
    _validate_ftd_boundary_duplicate_proofs(state, require_complete=True)
    if recent_window_days < 0 or max_evidence_age_days < 0:
        raise SecurityMasterError("evidence day windows cannot be negative")
    if recent_window_days > FTD_MAX_RECENT_EXACT_DATES - 1:
        raise SecurityMasterError(
            "recent_window_days exceeds the exact FTD witness retention "
            f"limit of {FTD_MAX_RECENT_EXACT_DATES - 1} days"
        )
    if min_confirmation_dates < 1:
        raise SecurityMasterError("min_confirmation_dates must be positive")

    universe = _normalize_security_universe(securities, state)
    universe_cusips = {record["cusip"] for record in universe}
    (
        evidence_by_cusip,
        intervals_by_cusip,
        global_latest_date,
    ) = _aggregate_ftd_evidence(state, universe_cusips)
    (
        validation_sources,
        validation_titles,
        validation_exchanges,
    ) = _validation_symbol_metadata(state)
    official_index, official_source = _official_13f_index(state)
    records: dict[str, dict[str, Any]] = {}
    quarantine: dict[str, dict[str, str]] = {}
    status_counts = {status: 0 for status in sorted(VALID_MAPPING_STATUSES)}

    for security in universe:
        cusip = security["cusip"]
        instrument_type = security["instrument_type"]
        key = security_key(cusip, instrument_type)
        evidence = evidence_by_cusip.get(cusip, [])
        symbol_intervals = intervals_by_cusip.get(cusip, [])
        reported_issuers = list(security.get("reported_issuers", []))
        reported_classes = list(security.get("reported_classes", []))
        official_rows = official_index.get(cusip, [])
        active_official_rows = [
            row
            for row in official_rows
            if row.get("status") != "*D*"
            and str(row.get("description") or "").strip().upper()
            not in {"CALL", "PUT"}
        ]
        official_issuers = sorted({
            str(row.get("issuer") or "").strip()
            for row in active_official_rows
            if str(row.get("issuer") or "").strip()
        })
        official_descriptions = sorted({
            str(row.get("description") or "").strip()
            for row in active_official_rows
            if str(row.get("description") or "").strip()
        })
        entry: dict[str, Any] = {
            "cusip": cusip,
            "instrument_type": instrument_type,
            "issuer": None,
            "security_class": None,
            "mapping_status": "unresolved",
            "ticker": None,
            "ticker_source": None,
            "ticker_as_of": None,
            "mapping_method": None,
            "effective_from": None,
            "effective_to": None,
            "exchange": None,
            "last_verification_date": None,
            "resolution_reason": "no_ftd_symbol_evidence",
            "symbol_evidence": evidence,
            "symbol_intervals": symbol_intervals,
        }
        for field in (
            "reported_issuer",
            "reported_issuers",
            "reported_class",
            "reported_classes",
            "reported_identities",
            "reported_identity_evidence",
        ):
            if field in security:
                entry[field] = security[field]
        if official_rows and official_source:
            official_status = (
                "active"
                if active_official_rows
                else "deleted"
                if all(row.get("status") == "*D*" for row in official_rows)
                else "option_only"
            )
            entry["official_13f_status"] = official_status
            entry["official_13f_as_of"] = official_source["period"]
            entry["official_13f"] = {
                **official_source,
                "status": official_status,
                "records": active_official_rows or official_rows,
            }
        if active_official_rows:
            label_row = active_official_rows[0]
            label = str(label_row.get("issuer") or "").strip()
            description = str(label_row.get("description") or "").strip()
            if description and description.upper() not in label.upper():
                label = f"{label} — {description}"
            entry["security_label"] = label
            entry["security_label_source"] = "sec_13f_list"
        elif reported_issuers:
            label = reported_issuers[0]
            if reported_classes:
                label = f"{label} — {reported_classes[0]}"
            entry["security_label"] = label
            entry["security_label_source"] = "sec_13f_filer_consensus"

        canonical_issuers = official_issuers or reported_issuers
        canonical_classes = official_descriptions or reported_classes
        if len(canonical_issuers) == 1:
            entry["issuer"] = canonical_issuers[0]
        if len(canonical_classes) == 1:
            entry["security_class"] = canonical_classes[0]

        active_official_identity_conflict = bool(
            active_official_rows
            and (
                len(official_issuers) != 1
                or len(official_descriptions) != 1
            )
        )
        quarantine_reason = cusip_quarantine_reason(cusip)
        if quarantine_reason:
            entry["mapping_status"] = "malformed_as_filed"
            entry["resolution_reason"] = quarantine_reason
            quarantine[key] = {
                "cusip": cusip,
                "instrument_type": instrument_type,
                "reason": quarantine_reason,
            }
        elif instrument_type not in FTD_ELIGIBLE_INSTRUMENT_TYPES:
            entry["mapping_status"] = "no_listed_symbol"
            entry["resolution_reason"] = "instrument_type_not_ftd_eligible"
        elif active_official_identity_conflict:
            entry["mapping_status"] = "ambiguous"
            entry["resolution_reason"] = "conflicting_active_official_13f_identity"
        elif (
            security.get("universe_metadata_conflict")
            and not active_official_rows
        ):
            entry["mapping_status"] = "ambiguous"
            entry["resolution_reason"] = "conflicting_reported_issuer_or_class"
        elif official_rows and not active_official_rows:
            entry["mapping_status"] = "no_listed_symbol"
            entry["resolution_reason"] = (
                "official_13f_security_deleted_or_option_only"
            )
        elif (
            class_conflict := _class_conflict_reason(
                instrument_type,
                reported_classes,
                official_descriptions,
            )
        ):
            entry["mapping_status"] = "no_listed_symbol"
            entry["resolution_reason"] = class_conflict
        elif (
            official_issuer_conflict := _issuer_conflict_reason(
                reported_issuers=reported_issuers,
                ftd_descriptions=[],
                sec_titles=[],
                official_issuers=official_issuers,
            )
        ):
            entry["mapping_status"] = "ambiguous"
            entry["resolution_reason"] = official_issuer_conflict
        elif symbol_intervals:
            latest_for_security = date.fromisoformat(
                symbol_intervals[-1]["last_seen"]
            )
            entry["last_verification_date"] = latest_for_security.isoformat()
            if (
                global_latest_date is not None
                and (global_latest_date - latest_for_security).days
                > max_evidence_age_days
            ):
                entry["resolution_reason"] = "ftd_evidence_is_stale"
                entry["candidate_as_of"] = latest_for_security.isoformat()
            else:
                cutoff = latest_for_security - timedelta(days=recent_window_days)
                recent = [
                    item
                    for item in evidence
                    if date.fromisoformat(item["settlement_date"]) >= cutoff
                ]
                recent_intervals = [
                    interval
                    for interval in symbol_intervals
                    if date.fromisoformat(interval["last_seen"]) >= cutoff
                ]
                recent_symbols = sorted({
                    symbol
                    for interval in recent_intervals
                    for symbol in interval["symbols"]
                })
                recent_dates_by_symbol = {
                    symbol: sorted({
                        item["settlement_date"]
                        for item in recent
                        if item["symbol"] == symbol
                    })
                    for symbol in recent_symbols
                }
                # A single uncorroborated FTD symbol typo must not withdraw a
                # repeatedly observed symbol that is present in the current
                # SEC ticker metadata.  A competing current SEC symbol or a
                # symbol repeated on two settlement dates remains a credible
                # conflict and fails closed.  Every raw observation is still
                # retained in the dated evidence and interval history.
                credible_recent_symbols = sorted(
                    symbol
                    for symbol in recent_symbols
                    if (
                        symbol in validation_sources
                        or len(recent_dates_by_symbol[symbol])
                        >= min_confirmation_dates
                    )
                )
                if (
                    len(recent_symbols) > 1
                    and len(credible_recent_symbols) != 1
                ):
                    entry["mapping_status"] = "ambiguous"
                    entry["resolution_reason"] = "conflicting_recent_ftd_symbols"
                    entry["candidate_symbols"] = recent_symbols
                    entry["candidate_as_of"] = latest_for_security.isoformat()
                else:
                    candidate = (
                        credible_recent_symbols[0]
                        if credible_recent_symbols
                        else recent_symbols[0]
                    )
                    candidate_dates = recent_dates_by_symbol[candidate]
                    candidate_interval = next(
                        (
                            interval for interval in reversed(symbol_intervals)
                            if interval.get("symbols") == [candidate]
                            and interval.get("last_seen") == candidate_dates[-1]
                        ),
                        None,
                    )
                    if candidate_interval is None:
                        raise SecurityMasterError(
                            f"active FTD symbol interval is missing for {key}"
                        )
                    entry["candidate_ticker"] = candidate
                    entry["candidate_as_of"] = candidate_dates[-1]
                    entry["confirmation_dates"] = candidate_dates
                    entry["symbol_validation_sources"] = validation_sources.get(
                        candidate, []
                    )
                    entry["symbol_validation_titles"] = validation_titles.get(
                        candidate, []
                    )
                    candidate_exchanges = validation_exchanges.get(candidate, [])
                    entry["symbol_validation_exchanges"] = candidate_exchanges
                    recent_descriptions = sorted({
                        description
                        for item in recent
                        if item.get("symbol") == candidate
                        for description in item.get("descriptions", [])
                        if isinstance(description, str) and description
                    })
                    if (
                        "security_label" not in entry
                        and recent_descriptions
                    ):
                        entry["security_label"] = recent_descriptions[0]
                        entry["security_label_source"] = "sec_ftd"
                    if len(candidate_dates) < min_confirmation_dates:
                        entry["resolution_reason"] = (
                            "insufficient_distinct_ftd_settlement_dates"
                        )
                    elif candidate not in validation_sources:
                        entry["resolution_reason"] = (
                            "symbol_absent_from_current_sec_validation_inputs"
                        )
                    elif not validation_titles.get(candidate):
                        entry["resolution_reason"] = (
                            "symbol_lacks_current_sec_issuer_metadata"
                        )
                    elif (
                        issuer_conflict := _issuer_conflict_reason(
                            reported_issuers=reported_issuers,
                            ftd_descriptions=recent_descriptions,
                            sec_titles=validation_titles.get(candidate, []),
                            official_issuers=official_issuers,
                        )
                    ):
                        entry["mapping_status"] = "ambiguous"
                        entry["resolution_reason"] = issuer_conflict
                    else:
                        entry.update({
                            "mapping_status": "resolved",
                            "ticker": candidate,
                            "ticker_source": "sec_ftd",
                            "ticker_as_of": candidate_dates[-1],
                            "last_verification_date": candidate_dates[-1],
                            "mapping_method": (
                                "exact_ftd_symbol_with_sec_metadata_validation"
                            ),
                            "effective_from": candidate_interval["first_seen"],
                            "effective_to": None,
                            "resolution_reason": (
                                "recent_repeated_ftd_symbol_validated_by_sec"
                            ),
                        })
                        if candidate_exchanges:
                            entry["exchanges"] = candidate_exchanges
                            entry["exchange"] = (
                                candidate_exchanges[0]
                                if len(candidate_exchanges) == 1
                                else None
                            )

        records[key] = entry

    _reconcile_current_symbol_cusips(
        records,
        concurrent_window_days=recent_window_days,
    )
    _apply_fund_series_names(records, state)
    status_counts = {status: 0 for status in sorted(VALID_MAPPING_STATUSES)}
    for entry in records.values():
        status_counts[entry["mapping_status"]] += 1

    source_provenance = [
        {
            "url": url,
            "sha256": entry["sha256"],
            "kind": entry["kind"],
            "schema_sha256": _source_schema_fingerprint(entry),
        }
        for url, entry in sorted(state.get("sources", {}).items())
    ]
    universe_payload = {"securities": universe}
    audit = _build_master_audit(
        state,
        evidence_by_cusip=evidence_by_cusip,
        global_latest_date=global_latest_date,
        official_index=official_index,
        official_source=official_source,
    )
    audit["fund_series_source_checkpoints"] = (
        _fund_series_source_checkpoints(records)
    )
    audit["sec_ixbrl_source_checkpoints"] = {}
    master: dict[str, Any] = {
        "schema_version": MASTER_SCHEMA_VERSION,
        "source_state_schema_version": state.get("schema_version"),
        # State timestamps change only for accepted source content or a
        # bounded successful-check checkpoint. Rebuilding a given state and
        # universe is therefore deterministic without daily timestamp churn.
        "generated_at": state.get("updated_at"),
        "source_state_sha256": _mapping_sha256(state),
        "universe_sha256": _mapping_sha256(universe_payload),
        "policy": {
            "recent_window_days": recent_window_days,
            "max_evidence_age_days": max_evidence_age_days,
            "min_confirmation_dates": min_confirmation_dates,
            "ftd_eligible_instrument_types": sorted(
                FTD_ELIGIBLE_INSTRUMENT_TYPES
            ),
        },
        "sources": source_provenance,
        "records": {key: records[key] for key in sorted(records)},
        "quarantine": {
            key: quarantine[key] for key in sorted(quarantine)
        },
        "summary": status_counts,
        "audit": audit,
    }
    master["audit"]["resolved_mapping_count_by_ticker_source"] = (
        _resolved_mapping_counts(master["records"])
    )
    (
        master["audit"]["reported_identity_count"],
        master["audit"]["evidenced_reported_identity_count"],
    ) = _reported_identity_evidence_counts(master["records"])
    _validate_security_master(master)
    edgar_evidence = state.get("edgar_evidence", {})
    if edgar_evidence:
        # Late import avoids a module cycle: the EDGAR bridge itself calls the
        # public master validator.  It never replaces an already resolved FTD
        # record and only applies exact Schedule 13D/G -> iXBRL class bridges.
        from sec_edgar_evidence import apply_sec_edgar_evidence

        master = apply_sec_edgar_evidence(
            master,
            edgar_evidence,
            successful_checkpoints=(
                _edgar_successful_checkpoints_by_cusip(state)
            ),
        )
        _apply_fund_series_names(master["records"], state)
        master["audit"]["fund_series_source_checkpoints"] = (
            _fund_series_source_checkpoints(master["records"])
        )
        master["audit"]["sec_ixbrl_source_checkpoints"] = (
            _sec_ixbrl_source_checkpoints(master["records"], state)
        )
        master["audit"]["resolved_mapping_count_by_ticker_source"] = (
            _resolved_mapping_counts(master["records"])
        )
        (
            master["audit"]["reported_identity_count"],
            master["audit"]["evidenced_reported_identity_count"],
        ) = _reported_identity_evidence_counts(master["records"])
        _validate_security_master(master)
    return master


def _retain_prior_mappings_with_unresolved_extensions(
    prior_master: Mapping[str, Any],
    source_state: Mapping[str, Any],
    securities: (
        Mapping[str, str | Iterable[str] | Mapping[str, Any]]
        | Iterable[tuple[str, str] | Mapping[str, Any]]
        | None
    ),
) -> dict[str, Any]:
    """Extend a verified master without using evidence from a failed refresh.

    Existing records remain byte-for-byte decisions from the prior master. New
    valid identities may be added for downstream display coverage, but a source
    outage prevents them from publishing a ticker until a complete refresh
    succeeds.
    """

    normalized_state = _normalize_source_state(source_state)
    _validate_source_state(normalized_state)
    _validate_security_master(prior_master)
    if prior_master.get("source_state_sha256") != _mapping_sha256(
        normalized_state
    ):
        raise SecurityMasterError(
            "cannot retain mappings from a master that is not bound to the "
            "prior SEC source state"
        )
    prior_records = prior_master.get("records")
    if not isinstance(prior_records, Mapping) or not prior_master.get("audit"):
        raise SecurityMasterError(
            "incremental SEC source failure has no verified prior master"
        )

    combined_universe = _normalize_security_universe(securities, normalized_state)
    combined_keys = {
        security_key(item.get("cusip"), item.get("instrument_type"))
        for item in combined_universe
    }
    for record in prior_records.values():
        if not isinstance(record, Mapping):
            continue
        key = security_key(record.get("cusip"), record.get("instrument_type"))
        if key in combined_keys:
            continue
        combined_universe.append({
            "cusip": record.get("cusip"),
            "instrument_type": record.get("instrument_type"),
            "reported_issuer": record.get(
                "reported_issuer", record.get("issuer")
            ),
            "reported_class": record.get(
                "reported_class", record.get("security_class")
            ),
        })
        combined_keys.add(key)

    policy = prior_master.get("policy", {})
    fallback = rebuild_security_master(
        normalized_state,
        combined_universe,
        recent_window_days=int(
            policy.get("recent_window_days", DEFAULT_RECENT_WINDOW_DAYS)
        ),
        max_evidence_age_days=int(
            policy.get("max_evidence_age_days", DEFAULT_MAX_EVIDENCE_AGE_DAYS)
        ),
        min_confirmation_dates=int(
            policy.get("min_confirmation_dates", DEFAULT_MIN_CONFIRMATION_DATES)
        ),
    )
    fallback_records = fallback["records"]
    prior_keys = set(prior_records)
    for key, record in prior_records.items():
        fallback_records[key] = copy.deepcopy(record)
    for key in sorted(set(fallback_records) - prior_keys):
        record = fallback_records[key]
        if record.get("mapping_status") not in {
            "malformed_as_filed",
            "no_listed_symbol",
        }:
            record.update({
                "mapping_status": "unresolved",
                "ticker": None,
                "ticker_source": None,
                "ticker_as_of": None,
                "mapping_method": None,
                "effective_from": None,
                "effective_to": None,
                "exchange": None,
                "resolution_reason": (
                    "sec_source_refresh_failed_new_identity_deferred"
                ),
            })
            record.pop("exchanges", None)
            record.pop("sec_edgar_evidence", None)
            record.pop("fund_series_name", None)
            record.pop("fund_series_evidence", None)

    summary = {status: 0 for status in sorted(VALID_MAPPING_STATUSES)}
    for record in fallback_records.values():
        summary[record["mapping_status"]] += 1
    fallback["records"] = {
        key: fallback_records[key] for key in sorted(fallback_records)
    }
    fallback["summary"] = summary
    _validate_security_master(fallback)
    return fallback


def audit_security_master(
    master: Mapping[str, Any] | Path,
    *,
    prior_master: Mapping[str, Any] | Path | None = None,
    minimum_ftd_coverage_ratio: float = DEFAULT_MIN_FTD_COVERAGE_RATIO,
    max_unexplained_regression_percentage_points: float = (
        DEFAULT_MAX_UNEXPLAINED_REGRESSION_PERCENTAGE_POINTS
    ),
    source_staleness_days: int = DEFAULT_SOURCE_STALENESS_DAYS,
    enforce_fund_series_freshness: bool = True,
    enforce_sec_ixbrl_freshness: bool = True,
    minimum_current_symbol_population_by_kind: Mapping[str, int] | None = None,
    minimum_current_symbol_title_ratio: float | None = None,
    minimum_active_official_cusip_count: int = 0,
    max_source_population_regression_ratio: float = (
        DEFAULT_MAX_SOURCE_POPULATION_REGRESSION_RATIO
    ),
    max_resolved_mapping_regression_ratio: float = (
        DEFAULT_MAX_RESOLVED_MAPPING_REGRESSION_RATIO
    ),
    enforce_latest_completed_official_period: bool = False,
    enforce_reported_identity_evidence: bool = False,
    regression_explanation: str | None = None,
    as_of: date | datetime | str | None = None,
) -> dict[str, Any]:
    """Evaluate deterministic acceptance gates for one rebuilt master.

    Coverage is measured across the latest active, non-option official 13F
    CUSIP set.  A decline greater than one percentage point fails unless a
    caller supplies a non-empty explanation; an explanation never waives the
    absolute coverage or freshness gates.  Passing ``as_of`` makes later
    staleness checks reproducible without rebuilding the source state.
    """

    if not 0 <= minimum_ftd_coverage_ratio <= 1:
        raise SecurityMasterError(
            "minimum_ftd_coverage_ratio must be between zero and one"
        )
    if max_unexplained_regression_percentage_points < 0:
        raise SecurityMasterError(
            "max unexplained regression cannot be negative"
        )
    if type(source_staleness_days) is not int or source_staleness_days < 0:
        raise SecurityMasterError(
            "source_staleness_days must be a non-negative integer"
        )
    minimum_populations = dict(minimum_current_symbol_population_by_kind or {})
    if set(minimum_populations) - _VALIDATION_SOURCE_KINDS or any(
        type(value) is not int or value < 0
        for value in minimum_populations.values()
    ):
        raise SecurityMasterError("invalid current-symbol minimum populations")
    if minimum_current_symbol_title_ratio is not None and not (
        isinstance(minimum_current_symbol_title_ratio, (int, float))
        and not isinstance(minimum_current_symbol_title_ratio, bool)
        and 0 <= float(minimum_current_symbol_title_ratio) <= 1
    ):
        raise SecurityMasterError("current-symbol title ratio must be in [0, 1]")
    if (
        type(minimum_active_official_cusip_count) is not int
        or minimum_active_official_cusip_count < 0
    ):
        raise SecurityMasterError("official-list minimum count cannot be negative")
    for value, label in (
        (max_source_population_regression_ratio, "source population regression"),
        (
            max_resolved_mapping_regression_ratio,
            "resolved mapping regression",
        ),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not 0 <= float(value) <= 1
        ):
            raise SecurityMasterError(f"{label} ratio must be in [0, 1]")

    if isinstance(master, (str, os.PathLike, Path)):
        current = load_security_master(Path(master))
    else:
        current = dict(master)
        _validate_security_master(current)
    current_audit = current.get("audit")
    if not isinstance(current_audit, dict) or not current_audit:
        raise SecurityMasterError(
            "security master has no deterministic acceptance-audit metadata"
        )

    prior: dict[str, Any] | None = None
    if prior_master is not None:
        if isinstance(prior_master, (str, os.PathLike, Path)):
            prior = load_security_master(Path(prior_master))
        else:
            prior = dict(prior_master)
            _validate_security_master(prior)
    prior_audit = prior.get("audit") if prior else None
    if not isinstance(prior_audit, dict) or not prior_audit:
        prior_audit = None

    coverage = float(current_audit["ftd_coverage_ratio"])
    coverage_gate_passed = coverage >= minimum_ftd_coverage_ratio
    prior_coverage = (
        float(prior_audit["ftd_coverage_ratio"])
        if prior_audit is not None
        else None
    )
    coverage_change_points = (
        round((coverage - prior_coverage) * 100, 6)
        if prior_coverage is not None
        else None
    )
    regression_points = (
        round(max(0.0, -coverage_change_points), 6)
        if coverage_change_points is not None
        else 0.0
    )
    explanation = str(regression_explanation or "").strip() or None
    material_regression = (
        prior_coverage is not None
        and regression_points
        > max_unexplained_regression_percentage_points + 1e-12
    )
    regression_gate_passed = not material_regression or explanation is not None

    effective_as_of = (
        _parse_calendar_date(as_of)
        if as_of is not None
        else _parse_calendar_date(current_audit.get("as_of"))
    )
    if as_of is not None and effective_as_of is None:
        raise SecurityMasterError("as_of must be a valid date or ISO timestamp")
    latest_ftd = _parse_calendar_date(
        current_audit.get("latest_ftd_settlement_date")
    )
    age_days = (
        max(0, (effective_as_of - latest_ftd).days)
        if effective_as_of is not None and latest_ftd is not None
        else None
    )
    source_staleness_gate_passed = (
        age_days is not None and age_days <= source_staleness_days
    )
    required_source_checkpoints = current_audit.get(
        "required_current_source_checkpoints"
    )
    required_source_freshness_available = isinstance(
        required_source_checkpoints,
        dict,
    )
    required_source_age_days_by_kind: dict[str, int | None] = {}
    missing_required_source_kinds: list[str] = []
    stale_required_source_kinds: list[str] = []
    if required_source_freshness_available:
        for kind in sorted(_REQUIRED_CURRENT_SOURCE_KINDS):
            checkpoint = required_source_checkpoints.get(kind)
            checked_date = (
                _parse_calendar_date(checkpoint.get("last_successful_check_at"))
                if isinstance(checkpoint, Mapping)
                else None
            )
            source_age_days = (
                max(0, (effective_as_of - checked_date).days)
                if effective_as_of is not None and checked_date is not None
                else None
            )
            required_source_age_days_by_kind[kind] = source_age_days
            if source_age_days is None:
                missing_required_source_kinds.append(kind)
            elif source_age_days > source_staleness_days:
                stale_required_source_kinds.append(kind)
        required_source_freshness_gate_passed = not (
            missing_required_source_kinds or stale_required_source_kinds
        )
    else:
        # Audit schemas 1 and 2 predate successful-check checkpoints.  They
        # remain readable and auditable under their original FTD-only gate;
        # every newly rebuilt schema-3 master receives the stricter gate.
        required_source_freshness_gate_passed = True

    fund_series_checkpoints = current_audit.get(
        "fund_series_source_checkpoints",
        {},
    )
    stale_fund_series_source_urls: list[str] = []
    fund_series_source_age_days: dict[str, int | None] = {}
    if not isinstance(fund_series_checkpoints, dict):
        raise SecurityMasterError("invalid fund-series checkpoint metadata")
    for source_url, checkpoint in sorted(fund_series_checkpoints.items()):
        checked_date = _parse_calendar_date(checkpoint)
        source_age_days = (
            max(0, (effective_as_of - checked_date).days)
            if effective_as_of is not None and checked_date is not None
            else None
        )
        fund_series_source_age_days[source_url] = source_age_days
        if source_age_days is None or source_age_days > source_staleness_days:
            stale_fund_series_source_urls.append(source_url)
    fund_series_freshness_gate_passed = (
        not enforce_fund_series_freshness
        or not stale_fund_series_source_urls
    )

    ixbrl_checkpoints = current_audit.get("sec_ixbrl_source_checkpoints")
    ixbrl_freshness_available = isinstance(ixbrl_checkpoints, dict)
    ixbrl_source_age_days: dict[str, int | None] = {}
    missing_ixbrl_security_keys: list[str] = []
    stale_ixbrl_security_keys: list[str] = []
    if ixbrl_freshness_available:
        for key, checkpoint in sorted(ixbrl_checkpoints.items()):
            checked_date = _parse_calendar_date(checkpoint)
            source_age_days = (
                max(0, (effective_as_of - checked_date).days)
                if effective_as_of is not None and checked_date is not None
                else None
            )
            ixbrl_source_age_days[key] = source_age_days
            if source_age_days is None:
                missing_ixbrl_security_keys.append(key)
            elif source_age_days > source_staleness_days:
                stale_ixbrl_security_keys.append(key)
        ixbrl_evidence_fresh = not (
            missing_ixbrl_security_keys or stale_ixbrl_security_keys
        )
    else:
        # Master-audit schemas 1-3 predate the per-security EDGAR success
        # clock. They remain readable under their original gates; every newly
        # rebuilt schema-4 master receives and enforces the stricter contract.
        ixbrl_evidence_fresh = True
    sec_ixbrl_freshness_gate_passed = (
        not enforce_sec_ixbrl_freshness or ixbrl_evidence_fresh
    )

    current_populations = dict(
        current_audit.get("current_symbol_population_by_kind", {})
    )
    current_title_populations = dict(
        current_audit.get("current_symbol_title_population_by_kind", {})
    )
    below_minimum_symbol_kinds = sorted(
        kind
        for kind, minimum in minimum_populations.items()
        if int(current_populations.get(kind, 0)) < minimum
    )
    insufficient_title_symbol_kinds: list[str] = []
    if minimum_current_symbol_title_ratio is not None:
        insufficient_title_symbol_kinds = sorted(
            kind
            for kind in _VALIDATION_SOURCE_KINDS
            if int(current_populations.get(kind, 0)) == 0
            or (
                int(current_title_populations.get(kind, 0))
                / int(current_populations.get(kind, 0))
            )
            < float(minimum_current_symbol_title_ratio)
        )

    prior_populations = (
        dict(prior_audit.get("current_symbol_population_by_kind", {}))
        if prior_audit is not None
        and int(prior_audit.get("schema_version", 0))
        >= MASTER_AUDIT_SCHEMA_VERSION
        else {}
    )
    regressed_symbol_population_kinds = sorted(
        kind
        for kind, prior_count in prior_populations.items()
        if int(prior_count) > 0
        and int(current_populations.get(kind, 0))
        < int(prior_count) * (1 - max_source_population_regression_ratio)
    )
    symbol_population_gate_passed = not (
        below_minimum_symbol_kinds
        or insufficient_title_symbol_kinds
        or regressed_symbol_population_kinds
    )

    current_official_count = int(
        current_audit["active_non_option_official_cusip_count"]
    )
    prior_official_count = (
        int(prior_audit["active_non_option_official_cusip_count"])
        if prior_audit is not None
        else None
    )
    official_population_regressed = bool(
        prior_official_count
        and current_official_count
        < prior_official_count * (1 - max_source_population_regression_ratio)
    )
    official_population_gate_passed = (
        current_official_count >= minimum_active_official_cusip_count
        and not official_population_regressed
    )

    expected_official_period = None
    if effective_as_of is not None:
        # The SEC may publish a quarter's official list after that quarter has
        # ended.  Apply the same 45-day (configurable) grace used for source
        # staleness before making the newly completed quarter mandatory.  The
        # completed-quarter calculation is intentionally strict at a quarter
        # end: on day 45 the prior list is still accepted; on day 46 the new
        # list is required.
        official_period_cutoff = effective_as_of - timedelta(
            days=source_staleness_days
        )
        current_quarter = (official_period_cutoff.month - 1) // 3 + 1
        completed_year = official_period_cutoff.year
        completed_quarter = current_quarter - 1
        if completed_quarter == 0:
            completed_year -= 1
            completed_quarter = 4
        expected_official_period = f"{completed_year:04d}Q{completed_quarter}"
    current_official_period = str(current_audit.get("official_13f_period") or "")
    official_period_gate_passed = (
        not enforce_latest_completed_official_period
        or (
            expected_official_period is not None
            and re.fullmatch(r"\d{4}Q[1-4]", current_official_period) is not None
            and current_official_period >= expected_official_period
        )
    )

    current_resolved_counts = dict(
        current_audit.get("resolved_mapping_count_by_ticker_source", {})
    )
    prior_resolved_counts = (
        dict(prior_audit.get("resolved_mapping_count_by_ticker_source", {}))
        if prior_audit is not None
        and int(prior_audit.get("schema_version", 0))
        >= MASTER_AUDIT_SCHEMA_VERSION
        else {}
    )
    resolved_mapping_regressed_sources = sorted(
        source
        for source, prior_count in prior_resolved_counts.items()
        if int(prior_count) > 0
        and int(current_resolved_counts.get(source, 0))
        < int(prior_count) * (1 - max_resolved_mapping_regression_ratio)
    )
    resolved_mapping_population_gate_passed = not (
        resolved_mapping_regressed_sources
    )
    reported_identity_count = int(
        current_audit.get("reported_identity_count", 0)
    )
    evidenced_reported_identity_count = int(
        current_audit.get("evidenced_reported_identity_count", 0)
    )
    reported_identity_evidence_gate_passed = (
        not enforce_reported_identity_evidence
        or evidenced_reported_identity_count == reported_identity_count
    )

    current_schemas = dict(
        current_audit.get("source_schema_sha256_by_kind", {})
    )
    if prior_audit is not None:
        prior_schemas = dict(
            prior_audit.get("source_schema_sha256_by_kind", {})
        )
        shared_kinds = set(current_schemas).intersection(prior_schemas)
        changed_kinds = sorted(
            kind
            for kind in shared_kinds
            if current_schemas[kind] != prior_schemas[kind]
        )
        added_kinds = sorted(set(current_schemas) - set(prior_schemas))
        removed_kinds = sorted(set(prior_schemas) - set(current_schemas))
        schema_comparison_available = True
    else:
        changed_kinds = []
        added_kinds = []
        removed_kinds = []
        schema_comparison_available = False
    source_state_schema_upgrade = bool(
        prior_audit is not None
        and current.get("source_state_schema_version")
        != prior.get("source_state_schema_version")
        and current.get("source_state_schema_version")
        == SOURCE_STATE_SCHEMA_VERSION
    )
    schema_change_gate_passed = not changed_kinds or source_state_schema_upgrade
    filter_universe_gate_passed = bool(
        current_audit.get("filter_universe_coverage_complete", True)
    )

    issues: list[str] = []
    if not coverage_gate_passed:
        issues.append("ftd_coverage_below_minimum")
    if not regression_gate_passed:
        issues.append("unexplained_material_ftd_coverage_regression")
    if not source_staleness_gate_passed:
        issues.append(
            "ftd_source_date_unavailable"
            if age_days is None
            else "ftd_source_is_stale"
        )
    if missing_required_source_kinds:
        issues.append("required_current_sec_source_date_unavailable")
    if stale_required_source_kinds:
        issues.append("required_current_sec_source_is_stale")
    if not fund_series_freshness_gate_passed:
        issues.append("fund_series_source_is_stale")
    if enforce_sec_ixbrl_freshness and missing_ixbrl_security_keys:
        issues.append("sec_ixbrl_source_date_unavailable")
    if enforce_sec_ixbrl_freshness and stale_ixbrl_security_keys:
        issues.append("sec_ixbrl_source_is_stale")
    if not schema_change_gate_passed:
        issues.append("source_schema_change_detected")
    if not filter_universe_gate_passed:
        issues.append("ftd_filter_universe_incomplete")
    if not symbol_population_gate_passed:
        issues.append("current_symbol_source_population_regressed")
    if not official_population_gate_passed:
        issues.append("official_13f_population_regressed")
    if not official_period_gate_passed:
        issues.append("official_13f_period_is_stale")
    if not resolved_mapping_population_gate_passed:
        issues.append("resolved_mapping_population_regressed")
    if not reported_identity_evidence_gate_passed:
        issues.append("reported_identity_evidence_incomplete")

    gates = {
        "minimum_ftd_coverage": coverage_gate_passed,
        "unexplained_material_regression": regression_gate_passed,
        "ftd_source_freshness": source_staleness_gate_passed,
        "required_current_sec_source_freshness": (
            required_source_freshness_gate_passed
        ),
        "fund_series_source_freshness": fund_series_freshness_gate_passed,
        "sec_ixbrl_source_freshness": sec_ixbrl_freshness_gate_passed,
        "source_schema_stability": schema_change_gate_passed,
        "ftd_filter_universe_coverage": filter_universe_gate_passed,
        "current_symbol_source_population": symbol_population_gate_passed,
        "official_13f_population": official_population_gate_passed,
        "official_13f_period": official_period_gate_passed,
        "resolved_mapping_population": (
            resolved_mapping_population_gate_passed
        ),
        "reported_identity_evidence": reported_identity_evidence_gate_passed,
    }
    return {
        "schema_version": 1,
        "as_of": effective_as_of.isoformat() if effective_as_of else None,
        "active_non_option_official_cusip_count": current_audit[
            "active_non_option_official_cusip_count"
        ],
        "ftd_evidenced_official_cusip_count": current_audit[
            "ftd_evidenced_official_cusip_count"
        ],
        "ftd_coverage_ratio": coverage,
        "minimum_ftd_coverage_ratio": minimum_ftd_coverage_ratio,
        "coverage_gate_passed": coverage_gate_passed,
        "prior_ftd_coverage_ratio": prior_coverage,
        "coverage_change_percentage_points": coverage_change_points,
        "coverage_regression_percentage_points": regression_points,
        "max_unexplained_regression_percentage_points": (
            max_unexplained_regression_percentage_points
        ),
        "material_regression": material_regression,
        "regression_explanation": explanation,
        "regression_gate_passed": regression_gate_passed,
        "latest_ftd_settlement_date": (
            latest_ftd.isoformat() if latest_ftd else None
        ),
        "ftd_source_age_days": age_days,
        "source_staleness_days": source_staleness_days,
        "source_staleness_gate_passed": source_staleness_gate_passed,
        "required_current_source_freshness_available": (
            required_source_freshness_available
        ),
        "required_current_source_age_days_by_kind": (
            required_source_age_days_by_kind
        ),
        "missing_required_current_source_kinds": (
            missing_required_source_kinds
        ),
        "stale_required_current_source_kinds": stale_required_source_kinds,
        "required_current_source_freshness_gate_passed": (
            required_source_freshness_gate_passed
        ),
        "fund_series_source_age_days": fund_series_source_age_days,
        "stale_fund_series_source_urls": stale_fund_series_source_urls,
        "fund_series_source_freshness_gate_passed": (
            fund_series_freshness_gate_passed
        ),
        "sec_ixbrl_source_freshness_available": ixbrl_freshness_available,
        "sec_ixbrl_source_age_days": ixbrl_source_age_days,
        "missing_sec_ixbrl_security_keys": missing_ixbrl_security_keys,
        "stale_sec_ixbrl_security_keys": stale_ixbrl_security_keys,
        "sec_ixbrl_source_freshness_gate_passed": (
            sec_ixbrl_freshness_gate_passed
        ),
        "schema_comparison_available": schema_comparison_available,
        "schema_changed_kinds": changed_kinds,
        "source_added_kinds": added_kinds,
        "source_removed_kinds": removed_kinds,
        "schema_change_gate_passed": schema_change_gate_passed,
        "source_state_schema_upgrade": source_state_schema_upgrade,
        "filter_universe_gate_passed": filter_universe_gate_passed,
        "current_symbol_population_by_kind": current_populations,
        "current_symbol_title_population_by_kind": current_title_populations,
        "minimum_current_symbol_population_by_kind": minimum_populations,
        "minimum_current_symbol_title_ratio": (
            minimum_current_symbol_title_ratio
        ),
        "below_minimum_symbol_kinds": below_minimum_symbol_kinds,
        "insufficient_title_symbol_kinds": insufficient_title_symbol_kinds,
        "regressed_symbol_population_kinds": (
            regressed_symbol_population_kinds
        ),
        "symbol_population_gate_passed": symbol_population_gate_passed,
        "minimum_active_official_cusip_count": (
            minimum_active_official_cusip_count
        ),
        "prior_active_non_option_official_cusip_count": prior_official_count,
        "official_population_regressed": official_population_regressed,
        "official_population_gate_passed": official_population_gate_passed,
        "expected_latest_completed_official_period": expected_official_period,
        "official_period_gate_passed": official_period_gate_passed,
        "resolved_mapping_count_by_ticker_source": current_resolved_counts,
        "resolved_mapping_regressed_sources": (
            resolved_mapping_regressed_sources
        ),
        "resolved_mapping_population_gate_passed": (
            resolved_mapping_population_gate_passed
        ),
        "reported_identity_count": reported_identity_count,
        "evidenced_reported_identity_count": evidenced_reported_identity_count,
        "reported_identity_evidence_gate_passed": (
            reported_identity_evidence_gate_passed
        ),
        "incomplete_filtered_archive_urls": list(
            current_audit.get("incomplete_filtered_archive_urls", [])
        ),
        "gates": gates,
        "issues": issues,
        "ok": all(gates.values()),
    }


def resolve_security(
    master: Mapping[str, Any] | Path,
    cusip: object | None,
    instrument_type: object | None = "EQUITY",
) -> dict[str, Any]:
    """Return an exact identity-keyed resolution; never fall back by issuer."""

    if isinstance(master, (str, os.PathLike, Path)):
        loaded = load_security_master(Path(master))
    else:
        loaded = dict(master)
        _validate_security_master(loaded)
    normalized_cusip = normalize_cusip(cusip)
    normalized_type = normalize_instrument_type(instrument_type)
    key = security_key(normalized_cusip, normalized_type)
    entry = loaded.get("records", {}).get(key)
    if isinstance(entry, dict):
        return dict(entry)
    quarantine_reason = cusip_quarantine_reason(normalized_cusip)
    status = "malformed_as_filed" if quarantine_reason else "unresolved"
    return {
        "cusip": normalized_cusip,
        "instrument_type": normalized_type,
        "mapping_status": status,
        "ticker": None,
        "ticker_source": None,
        "ticker_as_of": None,
        "mapping_method": None,
        "effective_from": None,
        "effective_to": None,
        "exchange": None,
        "last_verification_date": None,
        "resolution_reason": quarantine_reason or "security_not_in_master",
        "symbol_evidence": [],
    }


def make_sec_fetcher(
    user_agent: str | None = None,
    *,
    timeout: float = 30.0,
    session: requests.Session | None = None,
    requests_per_second: float = 8.0,
    max_attempts: int = 5,
    max_retry_delay: float = 60.0,
    max_redirects: int = 5,
) -> Fetcher:
    """Create a paced, bounded-retry, SEC-only fetcher.

    The shared process lock spaces requests at no more than eight per second,
    including full historical FTD backfills.  Only SEC-request-throttling and
    temporary-unavailability responses are retried.
    """

    agent = str(user_agent or os.environ.get("SEC_USER_AGENT") or "").strip()
    if "@" not in agent:
        raise SecurityMasterError(
            "SEC_USER_AGENT with a contact email is required for SEC downloads"
        )
    http = session or requests.Session()
    if not 0 < requests_per_second <= 8:
        raise SecurityMasterError("SEC request rate must be in (0, 8] requests/sec")
    if max_attempts < 1:
        raise SecurityMasterError("max_attempts must be positive")
    if max_retry_delay < 0:
        raise SecurityMasterError("max_retry_delay cannot be negative")
    if max_redirects < 0:
        raise SecurityMasterError("max_redirects cannot be negative")
    interval = 1.0 / requests_per_second

    def pace() -> None:
        global _SEC_NEXT_REQUEST_AT
        with _SEC_FETCH_LOCK:
            current_monotonic = time.monotonic()
            wait = max(0.0, _SEC_NEXT_REQUEST_AT - current_monotonic)
            if wait:
                time.sleep(wait)
                current_monotonic = time.monotonic()
            _SEC_NEXT_REQUEST_AT = max(
                current_monotonic,
                _SEC_NEXT_REQUEST_AT,
            ) + interval

    def retry_delay(
        response: requests.Response | None,
        attempt: int,
    ) -> float:
        raw_retry_after = str(
            (response.headers.get("Retry-After") or "")
            if response is not None
            else ""
        ).strip()
        parsed_delay: float | None = None
        if raw_retry_after:
            try:
                parsed_delay = max(0.0, float(raw_retry_after))
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(raw_retry_after)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    parsed_delay = max(
                        0.0,
                        (retry_at - datetime.now(timezone.utc)).total_seconds(),
                    )
                except (TypeError, ValueError, OverflowError):
                    parsed_delay = None
        if parsed_delay is None:
            parsed_delay = min(float(2**attempt), 16.0)
        return min(parsed_delay, max_retry_delay)

    def fetch(url: str) -> bytes:
        canonical_url = normalize_sec_url(url)
        for attempt in range(max_attempts):
            response: requests.Response | None = None
            request_url = canonical_url
            try:
                for redirect_count in range(max_redirects + 1):
                    pace()
                    response = http.get(
                        request_url,
                        headers={
                            "User-Agent": agent,
                            "Accept-Encoding": "gzip, deflate",
                        },
                        timeout=timeout,
                        allow_redirects=False,
                    )
                    response_url = normalize_sec_url(
                        str(response.url or request_url)
                    )
                    if response.status_code not in {301, 302, 303, 307, 308}:
                        break
                    if redirect_count >= max_redirects:
                        raise SecurityMasterError(
                            "SEC response exceeded the redirect limit"
                        )
                    location = str(
                        response.headers.get("Location") or ""
                    ).strip()
                    if not location:
                        raise SourceParseError(
                            "SEC redirect response has no Location header"
                        )
                    # Validate before making the redirected request; requests
                    # never receives a non-SEC target.
                    request_url = normalize_sec_url(
                        urljoin(response_url, location)
                    )
            except (requests.ConnectionError, requests.Timeout):
                if attempt + 1 >= max_attempts:
                    raise
                delay = retry_delay(None, attempt)
                if delay:
                    time.sleep(delay)
                continue
            assert response is not None
            if response.status_code not in {403, 429, 500, 502, 503, 504}:
                response.raise_for_status()
                return bytes(response.content)
            if attempt + 1 < max_attempts:
                delay = retry_delay(response, attempt)
                if delay:
                    time.sleep(delay)
                continue
            response.raise_for_status()
        raise SecurityMasterError("unreachable SEC fetch state")

    return fetch


def _fetch_bytes(fetcher: Fetcher, url: str) -> bytes:
    canonical_url = normalize_sec_url(url)
    payload = fetcher(canonical_url)
    if not isinstance(payload, (bytes, bytearray)):
        raise SourceParseError("fetcher must return bytes")
    return bytes(payload)


def _accepted_source_entry(
    *,
    url: str,
    kind: str,
    sha256: str,
    accepted_at: str,
    parsed: Any,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "url": url,
        "kind": kind,
        "sha256": sha256,
        "accepted_at": accepted_at,
    }
    if kind in _REQUIRED_CURRENT_SOURCE_KINDS:
        entry["last_successful_check_at"] = accepted_at
    if kind in {"sec_ftd_index", "sec_13f_list_index"}:
        entry["discovered_urls"] = parsed
    elif kind == "sec_ftd_archive":
        if not isinstance(parsed, dict) or not isinstance(
            parsed.get("compact_records"), list
        ):
            raise SourceParseError("FTD compact parser returned invalid metadata")
        entry["record_count"] = len(parsed["compact_records"])
        entry["raw_record_count"] = parsed.get("raw_record_count")
        entry["matched_record_count"] = len(parsed["compact_records"])
        entry["matched_cusip_count"] = len({
            record["cusip"] for record in parsed["compact_records"]
        })
        entry["first_settlement_date"] = parsed.get(
            "first_settlement_date"
        )
        entry["last_settlement_date"] = parsed.get("last_settlement_date")
        entry["observed_months"] = parsed.get("observed_months")
        entry["date_inventory_complete"] = True
        entry["boundary_date_proofs"] = parsed.get(
            "boundary_date_proofs"
        )
        entry["filter_universe_sha256"] = parsed.get(
            "filter_universe_sha256"
        )
        entry["filter_universe_count"] = parsed.get("filter_universe_count")
    elif kind == "sec_13f_list":
        entry["records"] = parsed
        entry["record_count"] = len(parsed)
        if kind == "sec_13f_list":
            match = _OFFICIAL_13F_LIST_RE.fullmatch(
                Path(urlparse(url).path).name
            )
            if not match:
                raise SourceParseError("official 13F-list URL has no period")
            entry["list_period"] = (
                f"{match.group('year')}Q{match.group('quarter')}"
            )
    elif kind == "sec_fund_series":
        if not isinstance(parsed, dict):
            raise SourceParseError(
                "SEC fund-series parser did not return metadata"
            )
        entry["cik"] = parsed["cik"]
        entry["series_names"] = parsed["series_names"]
        entry["class_names"] = parsed["class_names"]
    else:
        if not isinstance(parsed, dict):
            raise SourceParseError("SEC symbol parser did not return metadata")
        entry["symbols"] = parsed["symbols"]
        entry["symbol_titles"] = parsed.get("symbol_titles", {})
        entry["symbol_exchanges"] = parsed.get("symbol_exchanges", {})
        entry["symbol_count"] = len(entry["symbols"])
        if kind == "sec_fund_tickers":
            entry["fund_records"] = parsed.get("fund_records", [])
    return entry


def _active_official_cusips(state: Mapping[str, Any]) -> set[str]:
    official_index, _source = _official_13f_index(state)
    return {
        cusip
        for cusip, rows in official_index.items()
        if any(
            row.get("status") != "*D*"
            and str(row.get("description") or "").strip().upper()
            not in {"CALL", "PUT"}
            for row in rows
        )
    }


def _archive_filter_covers(
    source: Mapping[str, Any],
    state: Mapping[str, Any],
    target_cusips: set[str],
) -> bool:
    if source.get("kind") != "sec_ftd_archive":
        return False
    covered = _archive_filter_universe(source, state)
    if covered is None:
        return True
    return target_cusips.issubset(covered)


def _archive_filter_universe(
    source: Mapping[str, Any],
    state: Mapping[str, Any],
) -> set[str] | None:
    """Return a filtered archive's shared universe, or ``None`` for all."""

    if source.get("filter_all_cusips") is True:
        return None
    count = source.get("filter_universe_count")
    filter_sequence = state.get("ftd_filter_cusips", [])
    if (
        type(count) is not int
        or not isinstance(filter_sequence, list)
        or not 0 <= count <= len(filter_sequence)
    ):
        return set()
    covered = filter_sequence[:count]
    if source.get("filter_universe_sha256") != _filter_universe_sha256(covered):
        return set()
    return set(covered)


def _merge_compact_ftd_records(
    existing: Iterable[Mapping[str, Any]],
    added: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Merge disjoint filter-delta rows for one immutable archive."""

    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for raw in (*list(existing), *list(added)):
        if not isinstance(raw, Mapping):
            continue
        record = json.loads(json.dumps(raw))
        key = (
            str(record.get("cusip") or ""),
            str(record.get("symbol") or ""),
            str(record.get("description") or ""),
        )
        prior = merged.get(key)
        if prior is not None and prior != record:
            raise SecurityMasterError(
                "conflicting compact rows for one immutable FTD archive"
            )
        merged[key] = record
    return [merged[key] for key in sorted(merged)]


def _promote_ftd_tail_contribution(
    state: dict[str, Any],
    source_url: str,
) -> None:
    """Fold one no-longer-mutable archive into the stable base timeline."""

    contribution = state.get("ftd_mutable_tail", {}).get(source_url)
    source = state.get("sources", {}).get(source_url)
    if not isinstance(contribution, Mapping) or not isinstance(source, Mapping):
        raise SecurityMasterError(
            f"cannot promote missing FTD mutable-tail contribution: {source_url}"
        )
    if contribution.get("sha256") != source.get("sha256"):
        raise SecurityMasterError(
            f"cannot promote unbound FTD mutable-tail contribution: {source_url}"
        )
    observations = _ftd_observations_from_archive_records(
        contribution.get("records", []),
        source_url=source_url,
        source_sha256=str(source["sha256"]),
    )
    _append_ftd_observations_atomically(
        state["ftd_timeline"],
        observations,
    )
    del state["ftd_mutable_tail"][source_url]


def _canonicalize_ftd_checkpoint_order(state: dict[str, Any]) -> None:
    """Order incrementally built FTD maps before validation or persistence.

    Each archive is internally processed by sorted CUSIP, but a security first
    observed in a later archive can sort before an older dictionary key.
    Reordering only at bounded checkpoints avoids repeatedly sorting the whole
    historical timeline while keeping every durable state byte-deterministic.
    """

    timeline = state.get("ftd_timeline")
    if isinstance(timeline, dict):
        state["ftd_timeline"] = {
            cusip: timeline[cusip] for cusip in sorted(timeline)
        }
    mutable_tail = state.get("ftd_mutable_tail")
    if isinstance(mutable_tail, dict):
        state["ftd_mutable_tail"] = {
            url: mutable_tail[url]
            for url in sorted(mutable_tail, key=_ftd_url_sort_key)
        }


def _prune_filter_universe_profiles(state: dict[str, Any]) -> None:
    """Drop only unreferenced shared CUSIP profiles after a complete refresh."""

    referenced = {
        digest
        for digest in (
            state.get("current_filter_universe_sha256"),
            state.get("ftd_processed_filter_universe_sha256"),
        )
        if isinstance(digest, str) and digest
    }
    profiles = state.get("filter_universes", {})
    state["filter_universes"] = {
        digest: profiles[digest]
        for digest in sorted(referenced)
        if digest in profiles
    }


def _compact_ftd_payload(
    payload: bytes,
    *,
    source_url: str,
    target_cusips: set[str] | None,
    filter_universe_sha256: str,
) -> dict[str, Any]:
    period = _ftd_archive_period_key(source_url)
    period_start, period_end = _ftd_archive_date_bounds(source_url)
    raw_start, raw_end = _ftd_archive_raw_date_bounds(source_url)
    observed_dates: set[str] = set()
    observed_months: set[tuple[int, int]] = set()
    owned_months: set[tuple[int, int]] = set()
    boundary_rows: list[str] = []
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    raw_record_count = 0
    for raw_record in _iter_ftd_zip_records(payload):
        raw_record_count += 1
        settlement_date = _parse_settlement_date(
            raw_record.get("settlement_date")
        )
        if settlement_date is None:
            raise SourceParseError(
                "FTD archive contains an invalid settlement date"
            )
        observed_at = date.fromisoformat(settlement_date)
        if not raw_start <= observed_at <= raw_end:
            raise SourceSchemaError(
                "FTD settlement date falls outside archive period: "
                f"{settlement_date} not in {raw_start.isoformat()}.."
                f"{raw_end.isoformat()} for {source_url}"
            )
        observed_dates.add(settlement_date)
        observed_months.add((observed_at.year, observed_at.month))
        is_boundary_date = observed_at == _FTD_2004_BOUNDARY_DATE
        if period in {_FTD_2004_Q1_PERIOD, _FTD_2004_Q2_PERIOD} and (
            is_boundary_date
        ):
            # This proof is computed before CUSIP filtering so target-universe
            # expansion cannot change or weaken the archive equality check.
            boundary_rows.append(_canonical_ftd_row_json(raw_record))
        if not period_start <= observed_at <= period_end:
            if period == _FTD_2004_Q1_PERIOD and is_boundary_date:
                # Q2 canonically owns April 1. The Q1 copy is retained only as
                # a row-free multiset proof and never enters ticker evidence.
                continue
            raise SourceSchemaError(
                "FTD settlement date has no canonical archive owner: "
                f"{settlement_date} for {source_url}"
            )
        owned_months.add((observed_at.year, observed_at.month))

        cusip = normalize_cusip(raw_record.get("cusip"))
        if target_cusips is not None and cusip not in target_cusips:
            continue
        symbol = _normalize_symbol(raw_record.get("symbol"))
        if not cusip or symbol is None:
            continue
        description = " ".join(
            str(raw_record.get("description") or "").split()
        )
        bucket = buckets.setdefault(
            (cusip, symbol, description),
            {"dates": set(), "row_count": 0},
        )
        bucket["dates"].add(settlement_date)
        bucket["row_count"] += 1

    filename = Path(urlparse(normalize_sec_url(source_url)).path).name
    if _FTD_QUARTERLY_ARCHIVE_RE.fullmatch(filename):
        expected_months = {
            (period_start.year, month)
            for month in range(period_start.month, period_end.month + 1)
        }
        missing_months = sorted(expected_months - owned_months)
        if missing_months:
            formatted = ", ".join(
                f"{year:04d}-{month:02d}" for year, month in missing_months
            )
            raise SourceParseError(
                "quarterly FTD archive is missing settlement-month coverage: "
                f"{formatted} for {source_url}"
            )

    boundary_date_proofs: list[dict[str, Any]] = []
    if period in {_FTD_2004_Q1_PERIOD, _FTD_2004_Q2_PERIOD}:
        boundary_proof = _ftd_boundary_date_proof(
            boundary_rows,
            settlement_date=_FTD_2004_BOUNDARY_DATE,
        )
        if boundary_proof["row_count"] < 1:
            raise SourceSchemaError(
                "FTD archive is missing the declared 2004-04-01 boundary "
                f"rows: {source_url}"
            )
        boundary_date_proofs.append(boundary_proof)

    compact_records: list[dict[str, Any]] = []
    for (cusip, symbol, description), bucket in sorted(buckets.items()):
        dates = sorted(bucket["dates"])
        compact_records.append({
            "record_schema_version": FTD_COMPACT_RECORD_SCHEMA_VERSION,
            "cusip": cusip,
            "symbol": symbol,
            "description": description,
            "first_settlement_date": dates[0],
            "last_settlement_date": dates[-1],
            "observation_dates": dates,
            "distinct_settlement_date_count": len(dates),
            "row_count": bucket["row_count"],
        })

    settlement_dates = sorted(observed_dates)
    return {
        "compact_records": compact_records,
        "raw_record_count": raw_record_count,
        "first_settlement_date": settlement_dates[0],
        "last_settlement_date": settlement_dates[-1],
        "observed_months": sorted({item[:7] for item in settlement_dates}),
        "boundary_date_proofs": boundary_date_proofs,
        "filter_universe_sha256": filter_universe_sha256,
        "filter_universe_count": (
            len(target_cusips) if target_cusips is not None else 0
        ),
    }


def refresh_security_master(
    securities: (
        Mapping[str, str | Iterable[str] | Mapping[str, Any]]
        | Iterable[tuple[str, str] | Mapping[str, Any]]
        | None
    ) = None,
    *,
    master_path: Path = DEFAULT_MASTER_PATH,
    source_state_path: Path = DEFAULT_SOURCE_STATE_PATH,
    fetcher: Fetcher | None = None,
    now: datetime | None = None,
    ftd_page_url: str = FTD_PAGE_URL,
    official_13f_page_url: str = OFFICIAL_13F_LIST_PAGE_URL,
    lookback_months: int | None = DEFAULT_FTD_LOOKBACK_MONTHS,
    recheck_recent_archives: int = 2,
    recent_window_days: int = DEFAULT_RECENT_WINDOW_DAYS,
    max_evidence_age_days: int = DEFAULT_MAX_EVIDENCE_AGE_DAYS,
    min_confirmation_dates: int = DEFAULT_MIN_CONFIRMATION_DATES,
    minimum_ftd_coverage_ratio: float = DEFAULT_MIN_FTD_COVERAGE_RATIO,
    max_unexplained_regression_percentage_points: float = (
        DEFAULT_MAX_UNEXPLAINED_REGRESSION_PERCENTAGE_POINTS
    ),
    source_staleness_days: int = DEFAULT_SOURCE_STALENESS_DAYS,
    minimum_current_symbol_population_by_kind: Mapping[str, int] | None = None,
    minimum_current_symbol_title_ratio: float | None = None,
    minimum_active_official_cusip_count: int = 0,
    max_source_population_regression_ratio: float = (
        DEFAULT_MAX_SOURCE_POPULATION_REGRESSION_RATIO
    ),
    max_resolved_mapping_regression_ratio: float = (
        DEFAULT_MAX_RESOLVED_MAPPING_REGRESSION_RATIO
    ),
    enforce_latest_completed_official_period: bool = False,
    enforce_reported_identity_evidence: bool = False,
    regression_explanation: str | None = None,
) -> RefreshResult:
    """Incrementally refresh SEC sources, preserving every failed source's LKG.

    New FTD URLs in the rolling window are fetched once.  The latest archives
    are rechecked by SHA-256 because the SEC can replace a posted ZIP.  Current
    SEC validation files and the discovery page are rechecked every run.
    Successful unchanged checks receive a durable checkpoint at most every 30
    days, avoiding daily output churn while proving the required current SEC
    inputs were reachable within the 45-day gate. Incremental publication is a
    crash-recoverable source-state/master pair transaction; acceptance or
    schema failure leaves the exact last-good pair in place. A clean historical
    rebuild may still checkpoint source-only progress in its private staging
    directory before it produces a publishable pair.
    """

    if recheck_recent_archives < 0:
        raise SecurityMasterError("recheck_recent_archives cannot be negative")
    if securities is not None and not isinstance(securities, Mapping):
        securities = list(securities)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    accepted_at = _utc_timestamp(current)
    fetch = fetcher or make_sec_fetcher()
    recover_security_master_pair(
        master_path=Path(master_path),
        source_state_path=Path(source_state_path),
    )
    persisted_state = _read_json_object(Path(source_state_path))
    persisted_schema_version = persisted_state.get("schema_version")
    persisted_state_requires_migration = bool(persisted_state) and (
        persisted_schema_version != SOURCE_STATE_SCHEMA_VERSION
    )
    del persisted_state
    if lookback_months is None or persisted_state_requires_migration:
        # A full-rebuild workspace is deliberately non-publishable while its
        # source-only checkpoints are incomplete. Legacy state also has to be
        # normalized before its replacement pair can be bound and published.
        old_state = _load_source_state_unlocked(Path(source_state_path))
        old_master = _load_security_master_unlocked(Path(master_path))
    else:
        old_master, old_state = load_security_master_pair(
            master_path=Path(master_path),
            source_state_path=Path(source_state_path),
        )
    retained_boundary_periods = {
        _ftd_archive_period_key(str(url))
        for url, source in old_state.get("sources", {}).items()
        if isinstance(source, Mapping)
        and source.get("kind") == "sec_ftd_archive"
        and _ftd_archive_period_key(str(url))
        in {_FTD_2004_Q1_PERIOD, _FTD_2004_Q2_PERIOD}
    }
    boundary_migration_transaction = bool(
        persisted_schema_version
        in {TIMELINE_SOURCE_STATE_SCHEMA_VERSION, SOURCE_STATE_SCHEMA_VERSION}
        and retained_boundary_periods
        == {_FTD_2004_Q1_PERIOD, _FTD_2004_Q2_PERIOD}
        and any(
            isinstance(source, Mapping)
            and source.get("kind") == "sec_ftd_archive"
            and _ftd_boundary_proof_refresh_needed(
                source,
                source_url=str(url),
            )
            for url, source in old_state.get("sources", {}).items()
        )
    )
    state = json.loads(json.dumps(old_state))
    sources: dict[str, dict[str, Any]] = state["sources"]
    errors: list[str] = []
    schema_errors: list[str] = []
    refreshed_urls: list[str] = []
    retained_urls: list[str] = []
    state_changed = persisted_state_requires_migration

    def refresh_one(
        url: str,
        kind: str,
        parser: Callable[[bytes], Any],
        *,
        force_parse: bool = False,
    ) -> bool:
        nonlocal state_changed
        canonical_url = normalize_sec_url(url)
        prior = sources.get(canonical_url)
        try:
            payload = _fetch_bytes(fetch, canonical_url)
            digest = _payload_sha256(payload)
            if (
                not force_parse
                and isinstance(prior, dict)
                and prior.get("sha256") == digest
            ):
                if (
                    kind in _REQUIRED_CURRENT_SOURCE_KINDS
                    and _successful_check_checkpoint_due(
                        prior,
                        checked_at=current,
                    )
                    and prior.get("last_successful_check_at") != accepted_at
                ):
                    prior["last_successful_check_at"] = accepted_at
                    state_changed = True
                retained_urls.append(canonical_url)
                return False
            parsed = parser(payload)
            replacement = _accepted_source_entry(
                url=canonical_url,
                kind=kind,
                sha256=digest,
                accepted_at=accepted_at,
                parsed=parsed,
            )
            if replacement != prior:
                sources[canonical_url] = replacement
                state_changed = True
                changed = True
            else:
                changed = False
            refreshed_urls.append(canonical_url)
            return changed
        except Exception as exc:  # Per-source last-good boundary.
            if isinstance(exc, KeyboardInterrupt):
                raise
            detail = f"{kind} {canonical_url}: {type(exc).__name__}: {exc}"
            errors.append(detail)
            if isinstance(exc, SourceSchemaError):
                schema_errors.append(detail)
            if prior is not None:
                retained_urls.append(canonical_url)
            return False

    validation_sources: tuple[
        tuple[str, str, Callable[[Any], dict[str, Any]]], ...
    ] = (
        (
            SEC_COMPANY_TICKERS_URL,
            "sec_company_tickers",
            lambda payload: _symbol_metadata_from_payload(
                _parse_json_payload(payload)
            ),
        ),
        (
            SEC_COMPANY_EXCHANGE_TICKERS_URL,
            "sec_company_exchange_tickers",
            lambda payload: _symbol_metadata_from_payload(
                _parse_json_payload(payload)
            ),
        ),
        (
            SEC_FUND_TICKERS_URL,
            "sec_fund_tickers",
            lambda payload: _fund_symbol_metadata_from_payload(
                _parse_json_payload(payload)
            ),
        ),
    )
    for url, kind, parser in validation_sources:
        prior = sources.get(normalize_sec_url(url))
        refresh_one(
            url,
            kind,
            lambda payload, parser=parser: parser(payload),
            force_parse=(
                kind == "sec_fund_tickers"
                and isinstance(prior, dict)
                and not isinstance(prior.get("fund_records"), list)
            ),
        )

    # The official list must be accepted before FTD filtering so the archive
    # target is the repo universe UNION every active non-option official CUSIP.
    canonical_13f_page_url = normalize_sec_url(official_13f_page_url)
    refresh_one(
        canonical_13f_page_url,
        "sec_13f_list_index",
        lambda payload: [
            discover_latest_13f_list_url(
                payload,
                page_url=canonical_13f_page_url,
            )
        ],
    )
    official_index_entry = sources.get(canonical_13f_page_url) or {}
    official_urls = official_index_entry.get("discovered_urls")
    if isinstance(official_urls, list) and official_urls:
        latest_official_url = max(
            (normalize_sec_url(url) for url in official_urls),
            key=lambda url: (
                Path(urlparse(url).path).name,
                url,
            ),
        )
        refresh_one(latest_official_url, "sec_13f_list", parse_official_13f_list)

    repo_universe = _normalize_security_universe(securities, state)
    target_cusips = {
        record["cusip"] for record in repo_universe if record.get("cusip")
    }
    target_cusips.update(_active_official_cusips(state))
    normalized_target = sorted(target_cusips)
    target_digest = _filter_universe_sha256(normalized_target)
    target_profile = {
        "cusips": normalized_target,
        "count": len(normalized_target),
    }
    filter_universes = state.setdefault("filter_universes", {})
    if filter_universes.get(target_digest) != target_profile:
        filter_universes[target_digest] = target_profile
        state_changed = True
    if (
        state.get("current_filter_universe_sha256") != target_digest
        or state.get("current_filter_universe_count") != len(normalized_target)
    ):
        state["current_filter_universe_sha256"] = target_digest
        state["current_filter_universe_count"] = len(normalized_target)
        state_changed = True
    filter_sequence = state.setdefault("ftd_filter_cusips", [])
    filter_seen = set(filter_sequence)
    filter_additions = sorted(target_cusips - filter_seen)
    if filter_additions:
        filter_sequence.extend(filter_additions)
        filter_seen.update(filter_additions)
        state_changed = True

    canonical_page_url = normalize_sec_url(ftd_page_url)
    prior_ftd_index = sources.get(canonical_page_url)
    prior_ftd_urls = (
        prior_ftd_index.get("discovered_urls", [])
        if isinstance(prior_ftd_index, Mapping)
        else []
    )
    refresh_one(
        canonical_page_url,
        "sec_ftd_index",
        lambda payload: _validate_ftd_archive_discovery(
            _require_discovered_ftd_urls(
                discover_ftd_urls(payload, page_url=canonical_page_url)
            ),
            as_of=current.date(),
            require_full_history=lookback_months is None,
            prior_urls=prior_ftd_urls,
        ),
    )
    index_entry = sources.get(canonical_page_url) or {}
    discovered_urls = index_entry.get("discovered_urls")
    if not isinstance(discovered_urls, list):
        # A pre-index state can still rebuild and retry its known archives.
        discovered_urls = [
            url
            for url, entry in sources.items()
            if entry.get("kind") == "sec_ftd_archive"
        ]
    if lookback_months is None:
        selected_urls = sorted(
            {normalize_sec_url(url) for url in discovered_urls},
            key=_ftd_url_sort_key,
        )
    else:
        selected_urls = select_recent_ftd_urls(
            discovered_urls,
            as_of=current.date(),
            lookback_months=lookback_months,
        )
    if state.get("required_filter_coverage_urls") != selected_urls:
        state["required_filter_coverage_urls"] = selected_urls
        state_changed = True
    mutable_tail_urls = set(selected_urls[-2:])

    boundary_migration_urls: list[str] = []
    if boundary_migration_transaction:
        boundary_urls_by_period: dict[tuple[Any, ...], str] = {}
        for raw_url in discovered_urls:
            canonical_url = normalize_sec_url(str(raw_url))
            period = _ftd_archive_period_key(canonical_url)
            if period not in {_FTD_2004_Q1_PERIOD, _FTD_2004_Q2_PERIOD}:
                continue
            if period in boundary_urls_by_period:
                raise SourceSchemaChangeError([
                    "SEC FTD discovery returned duplicate 2004 boundary periods"
                ])
            boundary_urls_by_period[period] = canonical_url
        missing_periods = [
            _ftd_period_label(period)
            for period in (_FTD_2004_Q1_PERIOD, _FTD_2004_Q2_PERIOD)
            if period not in boundary_urls_by_period
            or boundary_urls_by_period[period] not in sources
        ]
        if missing_periods:
            raise SourceSchemaChangeError([
                "published v3 FTD state cannot migrate without both retained "
                "2004 boundary archives: " + ", ".join(missing_periods)
            ])
        boundary_migration_urls = [
            boundary_urls_by_period[_FTD_2004_Q1_PERIOD],
            boundary_urls_by_period[_FTD_2004_Q2_PERIOD],
        ]

        boundary_replacements: dict[str, dict[str, Any]] = {}
        try:
            for canonical_url in boundary_migration_urls:
                prior = sources[canonical_url]
                payload = _fetch_bytes(fetch, canonical_url)
                digest = _payload_sha256(payload)
                if prior.get("sha256") != digest:
                    raise SourceSchemaError(
                        "SEC replaced a 2004 boundary archive already merged "
                        "into the v3 timeline; run a clean security-master "
                        f"rebuild: {canonical_url}"
                    )
                prior_coverage = _archive_filter_universe(prior, state)
                parsed = _compact_ftd_payload(
                    payload,
                    source_url=canonical_url,
                    # A legacy unfiltered source must remain unfiltered.  For
                    # filtered sources, reparse the exact retained prefix,
                    # never the newly expanded current universe.
                    target_cusips=(
                        None
                        if prior_coverage is None
                        else set(prior_coverage)
                    ),
                    filter_universe_sha256=str(
                        prior.get("filter_universe_sha256") or ""
                    ),
                )
                boundary_replacements[canonical_url] = (
                    _upgrade_ftd_boundary_inventory(
                        state,
                        prior=prior,
                        parsed=parsed,
                        source_url=canonical_url,
                        source_sha256=digest,
                        accepted_at=accepted_at,
                    )
                )
            prospective_state = dict(state)
            prospective_sources = dict(sources)
            prospective_sources.update(boundary_replacements)
            prospective_state["sources"] = prospective_sources
            _validate_ftd_boundary_duplicate_proofs(
                prospective_state,
                require_complete=True,
            )
            _validate_ftd_timeline(prospective_state)
        except Exception as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise
            detail = (
                "sec_ftd_archive 2004 boundary migration: "
                f"{type(exc).__name__}: {exc}"
            )
            if isinstance(exc, SourceSchemaError):
                raise SourceSchemaChangeError([detail]) from exc
            raise SecurityMasterError(
                "SEC FTD 2004 boundary migration failed; retained the exact "
                "v3 source state and security master"
            ) from exc

        sources.update(boundary_replacements)
        state_changed = True
        refreshed_urls.extend(boundary_migration_urls)

    for promoted_url in sorted(
        set(state["ftd_mutable_tail"]) - mutable_tail_urls,
        key=_ftd_url_sort_key,
    ):
        _promote_ftd_tail_contribution(state, promoted_url)
        state_changed = True
    new_urls = [url for url in selected_urls if url not in sources]
    coverage_gap_urls = [
        url
        for url in selected_urls
        if url in sources
        and not _archive_filter_covers(sources[url], state, target_cusips)
    ]
    boundary_proof_gap_urls = [
        url
        for url in selected_urls
        if url in sources
        and _ftd_boundary_proof_refresh_needed(
            sources[url],
            source_url=url,
        )
    ]
    recheck_urls = (
        selected_urls[-recheck_recent_archives:]
        if recheck_recent_archives
        else []
    )
    urls_to_fetch = (
        set(new_urls)
        | set(coverage_gap_urls)
        | set(boundary_proof_gap_urls)
        | set(recheck_urls)
    ) - set(boundary_migration_urls)
    boundary_proof_gap_url_set = set(boundary_proof_gap_urls)
    archives_since_checkpoint = 0
    for url in sorted(urls_to_fetch, key=_ftd_url_sort_key):
        canonical_url = normalize_sec_url(url)
        prior = sources.get(canonical_url)
        prior_coverage = (
            _archive_filter_universe(prior, state)
            if isinstance(prior, Mapping)
            else set()
        )
        missing_cusips = (
            set()
            if prior_coverage is None
            else filter_seen - prior_coverage
        )
        try:
            payload = _fetch_bytes(fetch, canonical_url)
            digest = _payload_sha256(payload)
            checksum_changed = bool(
                isinstance(prior, Mapping) and prior.get("sha256") != digest
            )
            is_mutable = canonical_url in mutable_tail_urls
            if checksum_changed and not is_mutable:
                raise SourceSchemaError(
                    "SEC replaced an FTD archive already merged into the "
                    "compact timeline; a clean security-master rebuild is "
                    f"required: {canonical_url}"
                )
            if (
                isinstance(prior, Mapping)
                and not checksum_changed
                and not missing_cusips
                and canonical_url not in boundary_proof_gap_url_set
            ):
                retained_urls.append(canonical_url)
                continue

            # Boundary proofs hash the complete raw April 1 multiset before
            # filtering, so a same-checksum proof refresh needs to compact only
            # genuinely missing CUSIPs. Re-appending all already-folded v3
            # records would overlap the stable timeline during v4 migration.
            parse_cusips = (
                set(filter_sequence) if checksum_changed else missing_cusips
            )
            combined_coverage = list(filter_sequence)
            combined_digest = _filter_universe_sha256(combined_coverage)
            parsed = _compact_ftd_payload(
                payload,
                source_url=canonical_url,
                target_cusips=parse_cusips,
                filter_universe_sha256=combined_digest,
            )
            parsed["filter_universe_count"] = len(combined_coverage)
            if (
                is_mutable
                and not checksum_changed
                and canonical_url in state["ftd_mutable_tail"]
            ):
                parsed["compact_records"] = _merge_compact_ftd_records(
                    state["ftd_mutable_tail"][canonical_url]["records"],
                    parsed["compact_records"],
                )
            replacement = _accepted_source_entry(
                url=canonical_url,
                kind="sec_ftd_archive",
                sha256=digest,
                accepted_at=accepted_at,
                parsed=parsed,
            )
            if _ftd_archive_period_key(canonical_url) in {
                _FTD_2004_Q1_PERIOD,
                _FTD_2004_Q2_PERIOD,
            }:
                prospective_state = dict(state)
                prospective_sources = dict(sources)
                prospective_sources[canonical_url] = replacement
                prospective_state["sources"] = prospective_sources
                _validate_ftd_boundary_duplicate_proofs(prospective_state)
            if (
                not is_mutable
                and isinstance(prior, Mapping)
                and not checksum_changed
            ):
                replacement["record_count"] += int(
                    prior.get("record_count") or 0
                )
                replacement["matched_record_count"] = replacement[
                    "record_count"
                ]
                replacement["matched_cusip_count"] += int(
                    prior.get("matched_cusip_count") or 0
                )
            if is_mutable:
                state["ftd_mutable_tail"][canonical_url] = {
                    "sha256": digest,
                    "records": parsed["compact_records"],
                }
            else:
                observations = _ftd_observations_from_archive_records(
                    parsed["compact_records"],
                    source_url=canonical_url,
                    source_sha256=digest,
                )
                _append_ftd_observations_atomically(
                    state["ftd_timeline"],
                    observations,
                )
            sources[canonical_url] = replacement
            state_changed = True
            refreshed_urls.append(canonical_url)
            archives_since_checkpoint += 1
            if (
                not boundary_migration_transaction
                and lookback_months is None
                and archives_since_checkpoint >= FTD_CHECKPOINT_ARCHIVE_INTERVAL
            ):
                # This path is a non-publishable full-rebuild workspace. The
                # authoritative incremental pair never checkpoints one member.
                state["updated_at"] = accepted_at
                _canonicalize_ftd_checkpoint_order(state)
                save_source_state(state, Path(source_state_path))
                archives_since_checkpoint = 0
        except Exception as exc:  # Per-source last-good boundary.
            if isinstance(exc, KeyboardInterrupt):
                raise
            detail = (
                f"sec_ftd_archive {canonical_url}: "
                f"{type(exc).__name__}: {exc}"
            )
            errors.append(detail)
            if isinstance(exc, SourceSchemaError):
                schema_errors.append(detail)
            if prior is not None:
                retained_urls.append(canonical_url)
            # Keep checkpoints as a contiguous chronological prefix. Applying
            # a later archive for the same new CUSIP would make the missing
            # older interval impossible to prepend on resume.
            break

    if (
        not boundary_migration_transaction
        and lookback_months is None
        and archives_since_checkpoint
    ):
        state["updated_at"] = accepted_at
        _canonicalize_ftd_checkpoint_order(state)
        save_source_state(state, Path(source_state_path))

    if not errors:
        if state.get("ftd_processed_all_cusips") is not True:
            prior_processed_digest = state.get(
                "ftd_processed_filter_universe_sha256"
            )
            prior_processed_profile = filter_universes.get(
                prior_processed_digest, {}
            )
            processed_cusips = set(
                prior_processed_profile.get("cusips", [])
                if isinstance(prior_processed_profile, Mapping)
                else []
            )
            processed_cusips.update(filter_seen)
            processed = _normalized_filter_universe(processed_cusips)
            processed_digest = _filter_universe_sha256(processed)
            processed_profile = {"cusips": processed, "count": len(processed)}
            if filter_universes.get(processed_digest) != processed_profile:
                filter_universes[processed_digest] = processed_profile
            if (
                state.get("ftd_processed_filter_universe_sha256")
                != processed_digest
                or state.get("ftd_processed_filter_universe_count")
                != len(processed)
            ):
                state["ftd_processed_filter_universe_sha256"] = processed_digest
                state["ftd_processed_filter_universe_count"] = len(processed)
                state_changed = True
        _prune_filter_universe_profiles(state)

    if schema_errors:
        raise SourceSchemaChangeError(schema_errors)

    if errors:
        if boundary_migration_transaction:
            raise SecurityMasterError(
                "SEC source refresh could not complete the v3 to v4 FTD "
                "boundary migration; retained the exact prior source state "
                "and security master: " + "; ".join(errors)
            )
        if lookback_months is None:
            # A clean historical rebuild cannot mix a partially refreshed
            # source set with last-good inputs. Accepted archives remain
            # checkpointed for the next full-rebuild attempt, but no master is
            # rebuilt or published from the incomplete evidence set.
            raise SecurityMasterError(
                "full SEC security-master rebuild had source failures: "
                + "; ".join(errors)
            )

        # Incremental updates are atomic at the complete source-set boundary.
        # Roll back accepted/checkpointed source changes, retain every prior
        # verified mapping, and expose only null-ticker rows for new identities.
        fallback_master = _retain_prior_mappings_with_unresolved_extensions(
            old_master,
            old_state,
            securities,
        )
        fallback_acceptance = audit_security_master(
            fallback_master,
            prior_master=old_master,
            minimum_ftd_coverage_ratio=minimum_ftd_coverage_ratio,
            max_unexplained_regression_percentage_points=(
                max_unexplained_regression_percentage_points
            ),
            source_staleness_days=source_staleness_days,
            enforce_fund_series_freshness=False,
            enforce_sec_ixbrl_freshness=False,
            minimum_current_symbol_population_by_kind=(
                minimum_current_symbol_population_by_kind
            ),
            minimum_current_symbol_title_ratio=(
                minimum_current_symbol_title_ratio
            ),
            minimum_active_official_cusip_count=(
                minimum_active_official_cusip_count
            ),
            max_source_population_regression_ratio=(
                max_source_population_regression_ratio
            ),
            max_resolved_mapping_regression_ratio=(
                max_resolved_mapping_regression_ratio
            ),
            enforce_latest_completed_official_period=(
                enforce_latest_completed_official_period
            ),
            enforce_reported_identity_evidence=(
                enforce_reported_identity_evidence
            ),
            regression_explanation=regression_explanation,
            as_of=current,
        )
        if not fallback_acceptance["ok"]:
            raise SecurityMasterAcceptanceError(fallback_acceptance)
        master_changed = _canonical_json_bytes(
            fallback_master
        ) != _canonical_json_bytes(old_master)
        if master_changed:
            save_security_master_pair(
                fallback_master,
                old_state,
                master_path=Path(master_path),
                source_state_path=Path(source_state_path),
            )
        return RefreshResult(
            master=fallback_master,
            state=old_state,
            changed=master_changed,
            refreshed_urls=(),
            retained_urls=tuple(sorted(old_state.get("sources", {}))),
            errors=tuple(errors),
            acceptance=fallback_acceptance,
        )

    if state_changed:
        state["updated_at"] = accepted_at
        _canonicalize_ftd_checkpoint_order(state)

    master = rebuild_security_master(
        state,
        securities,
        recent_window_days=recent_window_days,
        max_evidence_age_days=max_evidence_age_days,
        min_confirmation_dates=min_confirmation_dates,
    )
    acceptance = audit_security_master(
        master,
        prior_master=(old_master if old_master.get("audit") else None),
        minimum_ftd_coverage_ratio=minimum_ftd_coverage_ratio,
        max_unexplained_regression_percentage_points=(
            max_unexplained_regression_percentage_points
        ),
        source_staleness_days=source_staleness_days,
        enforce_fund_series_freshness=False,
        enforce_sec_ixbrl_freshness=False,
        minimum_current_symbol_population_by_kind=(
            minimum_current_symbol_population_by_kind
        ),
        minimum_current_symbol_title_ratio=minimum_current_symbol_title_ratio,
        minimum_active_official_cusip_count=(
            minimum_active_official_cusip_count
        ),
        max_source_population_regression_ratio=(
            max_source_population_regression_ratio
        ),
        max_resolved_mapping_regression_ratio=(
            max_resolved_mapping_regression_ratio
        ),
        enforce_latest_completed_official_period=(
            enforce_latest_completed_official_period
        ),
        enforce_reported_identity_evidence=enforce_reported_identity_evidence,
        regression_explanation=regression_explanation,
        as_of=current,
    )
    if not acceptance["ok"]:
        raise SecurityMasterAcceptanceError(acceptance)
    master_changed = _canonical_json_bytes(master) != _canonical_json_bytes(old_master)
    if state_changed or master_changed:
        save_security_master_pair(
            master,
            state,
            master_path=Path(master_path),
            source_state_path=Path(source_state_path),
        )

    return RefreshResult(
        master=master,
        state=state,
        changed=state_changed or master_changed,
        refreshed_urls=tuple(sorted(set(refreshed_urls))),
        retained_urls=tuple(sorted(set(retained_urls))),
        errors=tuple(errors),
        acceptance=acceptance,
    )


# Short aliases make the integration surface explicit without hiding the
# descriptive function names used in tests and diagnostics.
load = load_security_master
save = save_security_master
refresh = refresh_security_master
rebuild = rebuild_security_master
resolve = resolve_security
audit = audit_security_master


__all__ = [
    "DEFAULT_MASTER_PATH",
    "DEFAULT_SOURCE_STATE_PATH",
    "DEFAULT_SOURCE_STALENESS_DAYS",
    "DEFAULT_SUCCESSFUL_CHECK_CHECKPOINT_DAYS",
    "DEFAULT_FTD_COVERAGE_WINDOW_DAYS",
    "DEFAULT_MIN_FTD_COVERAGE_RATIO",
    "DEFAULT_MAX_UNEXPLAINED_REGRESSION_PERCENTAGE_POINTS",
    "FTD_PAGE_URL",
    "OFFICIAL_13F_LIST_PAGE_URL",
    "SEC_COMPANY_TICKERS_URL",
    "SEC_COMPANY_EXCHANGE_TICKERS_URL",
    "SEC_FUND_TICKERS_URL",
    "FTD_COMPACT_RECORD_SCHEMA_VERSION",
    "FTD_TIMELINE_SCHEMA_VERSION",
    "COMPACT_SOURCE_STATE_SCHEMA_VERSION",
    "LEGACY_SOURCE_STATE_SCHEMA_VERSION",
    "MASTER_SCHEMA_VERSION",
    "SOURCE_STATE_SCHEMA_VERSION",
    "TIMELINE_SOURCE_STATE_SCHEMA_VERSION",
    "VALID_MAPPING_STATUSES",
    "VALID_MAPPING_METHODS",
    "VALID_TICKER_SOURCES",
    "RefreshResult",
    "SecurityMasterAcceptanceError",
    "SecurityMasterError",
    "SourceSchemaChangeError",
    "SourceSchemaError",
    "SourceParseError",
    "NonSECURL",
    "calculate_cusip_check_digit",
    "compact_ftd_records",
    "audit",
    "audit_security_master",
    "cusip_quarantine_reason",
    "discover_ftd_urls",
    "discover_latest_13f_list_url",
    "empty_source_state",
    "is_valid_cusip",
    "load",
    "load_security_master",
    "load_security_master_pair",
    "load_source_state",
    "make_sec_fetcher",
    "normalize_sec_url",
    "normalized_security_master_bytes",
    "normalized_source_state_evidence_bytes",
    "parse_ftd_pipe",
    "parse_ftd_zip",
    "parse_official_13f_list",
    "parse_sec_company_exchange_symbols",
    "parse_sec_company_symbols",
    "parse_sec_fund_symbols",
    "project_ftd_master_evidence",
    "project_master_audit",
    "rebuild",
    "rebuild_security_master",
    "refresh",
    "refresh_security_master",
    "resolve",
    "resolve_security",
    "save",
    "save_security_master",
    "save_security_master_pair",
    "save_source_state",
    "security_master_pair_lock",
    "recover_security_master_pair",
    "sec_fund_series_url",
    "security_key",
    "source_state_sha256",
    "select_recent_ftd_urls",
    "validate_security_master",
]
