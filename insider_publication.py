"""Bounded public projections for private normalized Section 16 filings.

This is the only module allowed to cross the private/public insider boundary.
It validates and re-binds immutable normalized records to issuer state, selects
current amendment versions, then copies a strict allowlist into per-security
and per-accession payloads.  Raw XML/HTML, raw parser trees, owner addresses,
parser warnings/errors, private paths, and private artifact hashes never enter
its output.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Iterable, Mapping
from urllib.parse import urlsplit

from data_contract import DATA_CONTRACT_VERSION
from insider_contract import (
    InsiderContractError,
    canonical_insider_json_bytes,
    classify_transaction_code,
    validate_insider_filing,
)
from insider_metrics import (
    MAX_PUBLIC_HOLDING_ROWS,
    MAX_PUBLIC_TRANSACTION_ROWS,
    InsiderMetricsError,
    build_insider_metric_projection,
)
from insider_pipeline import (
    InsiderDiscoveryError,
    issuer_record_from_normalized,
    reduce_issuer_state,
)
from security_identity import parse_stock_lookup_id, stock_file_stem


INSIDER_PUBLIC_CONTRACT_VERSION = 1
MAX_PUBLIC_FILINGS_PER_ISSUER = 5_000
MAX_PUBLIC_ISSUERS = 1_000
MAX_PUBLIC_SECURITY_MAPPINGS = 1_000
MAX_PUBLIC_FILING_REFS_PER_SECURITY = 1_000
MAX_PUBLIC_SECURITY_PAYLOAD_BYTES = 5_000_000
MAX_PUBLIC_FILING_DETAIL_BYTES = 1_000_000
MAX_PUBLIC_SECURITY_FILES = 10_000
MAX_PUBLIC_FILING_FILES = 15_000
MAX_PUBLIC_TOTAL_FILES = 25_001
MAX_PUBLIC_TOTAL_BYTES = 250_000_000
MAX_PUBLIC_TEXT_CHARS = 100_000
MAX_PUBLIC_FOOTNOTE_CHARS = 50_000
_PUBLIC_METHODOLOGY_TEXT = (
    "Showing reported purchase and sale transactions coded P or S. "
    "Missing values remain unknown; superseded amendments are excluded. "
    "Owner counts group identical names as reported in each filing; "
    "private owner identifiers are not published."
)
_PUBLIC_SECURITY_FIELDS = frozenset(
    {
        "companyName",
        "cusip",
        "fileStem",
        "id",
        "instrumentType",
        "issuerCik",
        "primary",
        "securityType",
        "securityTypeLabel",
        "ticker",
    }
)
_PUBLIC_PERSONAL_TEXT_RE = re.compile(
    r"(?:https?://|www\.|@|(?:^|\b)(?:address|avenue|boulevard|city|drive|email|"
    r"phone|postal|road|street|suite|zip)(?:\b|$)|\b[0-9]{5}(?:-[0-9]{4})?\b|"
    r"(?<![0-9])(?:\+?1[ .-]?)?\(?[0-9]{3}\)?[ .-][0-9]{3}[ .-][0-9]{4}(?![0-9]))",
    re.IGNORECASE,
)
_PUBLIC_CONTACT_TEXT_RE = re.compile(
    r"(?:https?://|www\.|@|"
    r"(?<![0-9])(?:\+?1[ .-]?)?\(?[0-9]{3}\)?[ .-][0-9]{3}[ .-][0-9]{4}(?![0-9])|"
    r"\b(?:address|contact|e[ -]?mail|facsimile|fax|home\s*page|homepage|"
    r"internet|mobile|cell|phone|telephone|tel|site|uri|url|web\s*site|"
    r"website|web)\s*[:=#-]|"
    r"\b(?:address|contact|e[ -]?mail|facsimile|fax|home\s*page|homepage|"
    r"internet|mobile|cell|phone|telephone|tel|site|uri|url|web\s*site|"
    r"website|web)\s+(?:https?://|www\.|[A-Z0-9._%+-]+@[A-Z0-9.-]+|"
    r"(?:[A-Z0-9-]+\.)+[A-Z0-9-]{2,63}))",
    re.IGNORECASE,
)
_PUBLIC_URI_SCHEME_RE = re.compile(
    r"\b[A-Z][A-Z0-9+.-]{1,31}:(?://|\S)",
    re.IGNORECASE,
)
_PUBLIC_BARE_DOMAIN_RE = re.compile(
    r"(?<![A-Z0-9_-])"
    r"(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+"
    r"(?:[A-Z]{2,63}|XN--[A-Z0-9-]{2,59})"
    r"(?![A-Z0-9_-])",
    re.IGNORECASE,
)
_PUBLIC_IP_ADDRESS_RE = re.compile(
    r"(?<![0-9.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9.])|"
    r"(?<![A-F0-9:])(?:[A-F0-9]{0,4}:){2,7}[A-F0-9]{0,4}(?![A-F0-9:])",
    re.IGNORECASE,
)
_PUBLIC_ADDRESS_TEXT_RE = re.compile(
    r"(?:"
    r"\b(?:P\.?\s*O\.?|POST\s+OFFICE)\s*(?:BOX|DRAWER)\s*#?\s*[A-Z0-9-]+\b|"
    r"\bP\.?\s*O\.?\s*B\.?\s*#?\s*[A-Z0-9-]+\b|"
    r"\bPOSTAL\s+(?:BOX|MAILBOX|DRAWER)\s*#?\s*[A-Z0-9-]+\b|"
    r"\b(?:BOX|MAILBOX|DRAWER)\s*#?\s*"
    r"(?:[0-9][A-Z0-9-]*|[A-Z]{1,2}[0-9][A-Z0-9-]*|[A-Z]{1,2})\b|"
    r"\b(?:RURAL\s+ROUTE|R\.?\s*R\.?|RR|RFD|HIGHWAY\s+CONTRACT|"
    r"H\.?\s*C\.?|HC|STAR\s+ROUTE)\s*#?\s*[A-Z0-9-]+\b|"
    r"\b(?:GENERAL\s+DELIVERY|POSTE?\s+RESTANTE|PRIVATE\s+BAG|LOCKED\s+BAG)\b|"
    r"\b(?:C(?:\s*[/\\.-]\s*|\s+)O|CARE(?:\s*[-./]\s*|\s+)OF)\b|"
    r"\b(?:ATTN|ATTENTION)\b|"
    r"\b[0-9]{1,6}[A-Z]?(?:[-/][0-9]{1,6}[A-Z]?)?\s+"
    r"(?:[^\s,;]+\s+){0,6}"
    r"(?:ALLEY|ALLY|ALY|ANNEX|ANX|ARCADE|ARC|AVENUE|AVE|BOULEVARD|BLVD|"
    r"CIRCLE|CIR|COURT|CT|DRIVE|DR|EXPRESSWAY|EXPY|HIGHWAY|HWY|LANE|LN|"
    r"PARKWAY|PKWY|PLACE|PL|PLAZA|PLZ|ROAD|RD|ROUTE|RTE|SQUARE|SQ|"
    r"STREET|ST|TERRACE|TER|TRAIL|TRL|WAY)\.?(?=$|[\s,;:#()])|"
    r"\b(?:APARTMENT|APT|UNIT|SUITE|STE|PMB|FLOOR|FL)\.?\s*#?\s*[A-Z0-9-]+\b|"
    r"(?<![0-9])[0-9]{5}(?:-[0-9]{4})?(?![0-9])|"
    r"\b[A-Z][0-9][A-Z][ -]?[0-9][A-Z][0-9]\b)",
    re.IGNORECASE,
)
_PUBLIC_CIK_TOKEN_RE = re.compile(r"(?<![0-9])[0-9]{10}(?![0-9])")
_PUBLIC_PRIVATE_CORRELATOR_RE = re.compile(
    r"(?<![A-F0-9])[A-F0-9]{64}(?![A-F0-9])",
    re.IGNORECASE,
)
_PUBLIC_LOCAL_PATH_RE = re.compile(
    r"(?:^[\\/]|^[A-Za-z]:[\\/]|^file://|(?:^|[\\/])\.\.(?:[\\/]|$)|"
    r"(?:^|[\s;])/(?:Users|etc|home|private|tmp|var)(?:/|$))",
    re.IGNORECASE,
)
_PUBLIC_COMPANY_TITLES = frozenset(
    {
        "10% Owner",
        "Advisor",
        "Chair",
        "Chief Executive Officer",
        "Chief Financial Officer",
        "Chief Investment Officer",
        "Chief Marketing Officer",
        "Chief Operating Officer",
        "Chief Technology Officer",
        "Controller",
        "Director",
        "Executive Vice President",
        "Founder",
        "General Counsel",
        "Manager",
        "Officer",
        "Partner",
        "President",
        "Principal",
        "Reporting Owner",
        "Secretary",
        "Senior Vice President",
        "Treasurer",
        "Trustee",
        "Vice President",
    }
)
_PUBLIC_COMPANY_TITLE_PATTERNS = (
    (
        re.compile(r"\b(?:chief executive(?: officer)?|ceo)\b", re.IGNORECASE),
        "Chief Executive Officer",
    ),
    (
        re.compile(r"\b(?:chief financial(?: officer)?|cfo|pfo)\b", re.IGNORECASE),
        "Chief Financial Officer",
    ),
    (
        re.compile(r"\b(?:chief operating(?: officer)?|coo)\b", re.IGNORECASE),
        "Chief Operating Officer",
    ),
    (
        re.compile(r"\b(?:chief investment(?: officer)?|cio)\b", re.IGNORECASE),
        "Chief Investment Officer",
    ),
    (
        re.compile(r"\b(?:chief technology(?: officer)?|cto)\b", re.IGNORECASE),
        "Chief Technology Officer",
    ),
    (
        re.compile(r"\b(?:chief marketing(?: officer)?|cmo)\b", re.IGNORECASE),
        "Chief Marketing Officer",
    ),
    (re.compile(r"\bgeneral counsel\b", re.IGNORECASE), "General Counsel"),
    (re.compile(r"\bcontroller\b", re.IGNORECASE), "Controller"),
    (re.compile(r"\btreasurer\b", re.IGNORECASE), "Treasurer"),
    (re.compile(r"\bsecretary\b", re.IGNORECASE), "Secretary"),
    (
        re.compile(r"\b(?:executive vice president|evp)\b", re.IGNORECASE),
        "Executive Vice President",
    ),
    (
        re.compile(r"\b(?:senior vice president|svp)\b", re.IGNORECASE),
        "Senior Vice President",
    ),
    (re.compile(r"\b(?:vice president|vp)\b", re.IGNORECASE), "Vice President"),
    (re.compile(r"\bpresident\b", re.IGNORECASE), "President"),
    (re.compile(r"\bchair(?:man|woman)?\b", re.IGNORECASE), "Chair"),
    (re.compile(r"\bfounder\b", re.IGNORECASE), "Founder"),
    (re.compile(r"\bpartner\b", re.IGNORECASE), "Partner"),
    (re.compile(r"\bprincipal\b", re.IGNORECASE), "Principal"),
    (re.compile(r"\bmanager\b", re.IGNORECASE), "Manager"),
    (re.compile(r"\btrustee\b", re.IGNORECASE), "Trustee"),
    (re.compile(r"\badvisor\b", re.IGNORECASE), "Advisor"),
    (re.compile(r"\bdirector\b", re.IGNORECASE), "Director"),
    (re.compile(r"\bofficer\b", re.IGNORECASE), "Officer"),
)
_ACCESSION_RE = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_SEC_ARCHIVE_URL_PREFIX_RE = re.compile(r"https://www\.sec\.gov(?::443)?/")
_SEC_ARCHIVE_PATH_RE = re.compile(
    r"/Archives/edgar/data/(0|[1-9][0-9]*)/([0-9]{18})/"
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}"
    r"(?:/[A-Za-z0-9][A-Za-z0-9._-]{0,255})*"
)
_SECURITY_STEM_RE = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,159}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_ISO_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_ISO_UTC_TIMESTAMP_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z"
)
_PUBLIC_SYMBOL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}")
_PUBLIC_FORM_TYPES = frozenset({"3", "3/A", "4", "4/A", "5", "5/A"})
_PUBLIC_TRANSACTION_FORM_TYPES = frozenset({"3", "4", "5"})
_PUBLIC_PLAN_STATES = frozenset({"filing_marked", "not_marked", "unknown"})
_PUBLIC_VALUE_METHODS = frozenset(
    {"reported_total", "calculated_shares_times_price", "unavailable"}
)
_FORBIDDEN_PUBLIC_KEYS = frozenset(
    {
        "attributes",
        "children",
        "field_sources",
        "has_restricted_address_source",
        "normalized_sha256",
        "ownerAggregationSlot",
        "parse_error",
        "privacy",
        "raw_document",
        "raw_footnote",
        "raw_owner",
        "raw_row",
        "raw_signature",
        "restricted_address",
        "source_path",
        "unknown_elements",
        "warnings",
    }
)


class InsiderPublicationError(ValueError):
    """Raised when private evidence cannot be projected safely and exactly."""


@dataclass(frozen=True)
class InsiderPublication:
    issuer_ciks: tuple[str, ...]
    security_payloads: dict[str, dict[str, object]]
    filing_payloads: dict[str, dict[str, object]]
    manifest: dict[str, object]


def _fail(label: str) -> InsiderPublicationError:
    return InsiderPublicationError(f"insider publication is invalid: {label}")


def _plain_text(
    value: object,
    label: str,
    *,
    nullable: bool = False,
    maximum: int = MAX_PUBLIC_TEXT_CHARS,
) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or len(value) > maximum:
        raise _fail(label)
    normalized = value.replace("\r\n", "\n").replace("\r", "\n")
    if any(
        (ord(character) < 32 and character not in {"\n", "\t"}) or ord(character) == 127
        for character in normalized
    ):
        raise _fail(label)
    if not normalized and not nullable:
        raise _fail(label)
    return normalized


def _safe_atom(
    value: object,
    label: str,
    *,
    nullable: bool = False,
    maximum: int = 512,
) -> str | None:
    text = _plain_text(value, label, nullable=nullable, maximum=maximum)
    if text is not None and (
        text != text.strip()
        or "\n" in text
        or "\t" in text
        or _PUBLIC_LOCAL_PATH_RE.search(text) is not None
    ):
        raise _fail(label)
    return text


def _safe_public_name(value: object, label: str, *, maximum: int) -> str:
    text = _safe_atom(value, label, maximum=maximum)
    assert isinstance(text, str)
    if (
        _PUBLIC_CONTACT_TEXT_RE.search(text) is not None
        or _PUBLIC_URI_SCHEME_RE.search(text) is not None
        or _PUBLIC_BARE_DOMAIN_RE.search(text) is not None
        or _PUBLIC_IP_ADDRESS_RE.search(text) is not None
        or _PUBLIC_ADDRESS_TEXT_RE.search(text) is not None
        or _PUBLIC_CIK_TOKEN_RE.search(text) is not None
        or _PUBLIC_PRIVATE_CORRELATOR_RE.search(text) is not None
    ):
        raise _fail(label)
    return text


def _safe_public_symbol(
    value: object,
    label: str,
    *,
    nullable: bool = False,
) -> str | None:
    text = _safe_atom(value, label, nullable=nullable, maximum=64)
    if text is not None:
        if (
            _PUBLIC_CONTACT_TEXT_RE.search(text) is not None
            or _PUBLIC_URI_SCHEME_RE.search(text) is not None
            or _PUBLIC_BARE_DOMAIN_RE.search(text) is not None
            or _PUBLIC_IP_ADDRESS_RE.search(text) is not None
            or _PUBLIC_ADDRESS_TEXT_RE.search(text) is not None
            or _PUBLIC_CIK_TOKEN_RE.search(text) is not None
            or _PUBLIC_PRIVATE_CORRELATOR_RE.search(text) is not None
            or _PUBLIC_SYMBOL_RE.fullmatch(text) is None
        ):
            raise _fail(label)
    return text


def _safe_iso_date(
    value: object,
    label: str,
    *,
    nullable: bool = False,
) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or _ISO_DATE_RE.fullmatch(value) is None:
        raise _fail(label)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise _fail(label) from error
    if parsed.isoformat() != value:
        raise _fail(label)
    return value


def _safe_utc_timestamp(
    value: object,
    label: str,
    *,
    nullable: bool = False,
) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or _ISO_UTC_TIMESTAMP_RE.fullmatch(value) is None:
        raise _fail(label)
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise _fail(label) from error
    if parsed.utcoffset() is None:
        raise _fail(label)
    return value


def _is_canonical_decimal_text(value: object) -> bool:
    if (
        type(value) is not str
        or len(value) > 512
        or re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value) is None
    ):
        return False
    try:
        number = Decimal(value)
    except InvalidOperation:
        return False
    if not number.is_finite():
        return False
    rendered = format(number, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    if number == 0:
        rendered = "0"
    return rendered == value


def _safe_sec_url(value: object, label: str) -> str:
    url = _safe_atom(value, label, maximum=512)
    assert isinstance(url, str)
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise _fail(label) from error
    if (
        _SEC_ARCHIVE_URL_PREFIX_RE.match(url) is None
        or parsed.scheme != "https"
        or parsed.hostname != "www.sec.gov"
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or _SEC_ARCHIVE_PATH_RE.fullmatch(parsed.path) is None
    ):
        raise _fail(label)
    return url


def _safe_bound_sec_url(
    value: object,
    label: str,
    *,
    issuer_cik: object,
    accession: str,
) -> str:
    url = _safe_sec_url(value, label)
    if (
        type(issuer_cik) is not str
        or re.fullmatch(r"[0-9]{10}", issuer_cik) is None
        or type(accession) is not str
        or _ACCESSION_RE.fullmatch(accession) is None
    ):
        raise _fail(label)
    path_match = _SEC_ARCHIVE_PATH_RE.fullmatch(urlsplit(url).path)
    assert path_match is not None
    if path_match.group(1) != str(int(issuer_cik)) or path_match.group(
        2
    ) != accession.replace("-", ""):
        raise _fail(label)
    return url


def _validate_public_value(value: object, path: str = "payload") -> None:
    if value is None or type(value) in {bool, int, str}:
        if type(value) is str:
            if len(value) > MAX_PUBLIC_TEXT_CHARS:
                raise _fail(f"{path} string limit")
            if _PUBLIC_LOCAL_PATH_RE.search(value) is not None:
                raise _fail(f"{path} internal path")
            if any(
                (ord(character) < 32 and character not in {"\n", "\t"})
                or ord(character) == 127
                for character in value
            ):
                raise _fail(f"{path} control characters")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_public_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if (
                type(key) is not str
                or key in _FORBIDDEN_PUBLIC_KEYS
                or key.startswith(("raw_", "private_", "internal_"))
            ):
                raise _fail(f"{path} forbidden field {key!r}")
            _validate_public_value(item, f"{path}.{key}")
        return
    raise _fail(f"{path} JSON type")


def canonical_public_json_bytes(payload: object) -> bytes:
    """Validate and deterministically serialize one public insider payload."""

    _validate_public_value(payload)
    try:
        rendered = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, RecursionError) as error:
        raise _fail("JSON serialization") from error
    return (rendered + "\n").encode("utf-8")


def _bounded_validated_filings(
    filings: Iterable[Mapping[str, object]],
) -> list[dict[str, object]]:
    if isinstance(filings, (str, bytes, Mapping)):
        raise _fail("filings")
    try:
        iterator = iter(filings)
    except TypeError as error:
        raise _fail("filings") from error
    by_accession: dict[str, dict[str, object]] = {}
    for filing in iterator:
        if len(by_accession) >= MAX_PUBLIC_FILINGS_PER_ISSUER:
            raise _fail("filing limit")
        try:
            validated = validate_insider_filing(filing)
            detached = json.loads(canonical_insider_json_bytes(validated))
        except (
            InsiderContractError,
            json.JSONDecodeError,
            RecursionError,
            TypeError,
            ValueError,
        ) as error:
            raise _fail("private normalized filing") from error
        accession = detached["accession_number"]
        if type(accession) is not str or not _ACCESSION_RE.fullmatch(accession):
            raise _fail("filing accession")
        if accession in by_accession:
            raise _fail("duplicate filing accession")
        remarks = detached["remarks"]
        _plain_text(remarks, "remarks", nullable=True)
        for footnote in detached["footnotes"]:
            _plain_text(
                footnote["text"],
                "footnote text",
                maximum=MAX_PUBLIC_FOOTNOTE_CHARS,
            )
        by_accession[accession] = detached
    if not by_accession:
        raise _fail("filings empty")
    result = [by_accession[accession] for accession in sorted(by_accession)]
    issuer_ciks = {filing["issuer"]["cik"] for filing in result}
    if len(issuer_ciks) != 1:
        raise _fail("issuer scope")
    return result


def _bind_issuer_state(
    filings: list[dict[str, object]],
    state: object,
) -> tuple[dict[str, object], dict[str, str]]:
    try:
        records = [
            issuer_record_from_normalized(
                filing,
                parser_version=filing["parser_version"],
            )
            for filing in filings
        ]
        issuer_cik = filings[0]["issuer"]["cik"]
        reduced = reduce_issuer_state(
            issuer_cik=issuer_cik,
            records=records,
        )
    except (InsiderDiscoveryError, TypeError, ValueError) as error:
        raise _fail("issuer reduction") from error
    if type(state) is not dict or state != reduced.issuer_state:
        raise _fail("issuer state binding")
    owner_keys = {record.accession_number: record.owner_group_key for record in records}
    if len(owner_keys) != len(records):
        raise _fail("issuer owner-group binding")
    return reduced.issuer_state, owner_keys


def _validate_security_metadata(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _fail("security mapping metadata")
    expected = {
        "companyName",
        "cusip",
        "fileStem",
        "primary",
        "securityType",
        "securityTypeLabel",
        "stockId",
        "ticker",
    }
    if set(value) != expected:
        raise _fail("security mapping metadata fields")
    stock_id = _safe_atom(value["stockId"], "stockId", maximum=128)
    assert isinstance(stock_id, str)
    base, instrument_type = parse_stock_lookup_id(stock_id)
    if not base or stock_file_stem(stock_id) != value["fileStem"]:
        raise _fail("security mapping file stem")
    ticker = _safe_public_symbol(value["ticker"], "ticker", nullable=True)
    company_name = _safe_public_name(
        value["companyName"],
        "companyName",
        maximum=256,
    )
    security_type = _safe_public_name(
        value["securityType"],
        "securityType",
        maximum=160,
    )
    security_label = _safe_public_name(
        value["securityTypeLabel"],
        "securityTypeLabel",
        maximum=160,
    )
    cusip = _safe_atom(value["cusip"], "cusip", nullable=True, maximum=32)
    if cusip is not None and re.fullmatch(r"[A-Za-z0-9*@#]{6,32}", cusip) is None:
        raise _fail("cusip")
    if type(value["primary"]) is not bool:
        raise _fail("security mapping primary")
    return {
        "id": stock_id,
        "ticker": ticker,
        "companyName": company_name,
        "securityType": security_type,
        "securityTypeLabel": security_label,
        "cusip": cusip,
        "instrumentType": instrument_type,
        "fileStem": value["fileStem"],
        "primary": value["primary"],
    }


def _validate_security_mappings(
    mappings: object,
    state: dict[str, object],
) -> dict[str, dict[str, object]]:
    if not isinstance(mappings, dict) or not mappings:
        raise _fail("security mappings")
    if len(mappings) > MAX_PUBLIC_SECURITY_MAPPINGS:
        raise _fail("security mapping limit")
    state_classes = {
        item["security_class_key"]
        for item in state["security_classes"]
        if isinstance(item, dict)
    }
    canonical: dict[str, dict[str, object]] = {}
    metadata_by_stock: dict[str, dict[str, object]] = {}
    for class_key in sorted(mappings):
        if (
            type(class_key) is not str
            or not _SHA256_RE.fullmatch(class_key)
            or class_key not in state_classes
        ):
            raise _fail("security mapping key")
        metadata = _validate_security_metadata(mappings[class_key])
        stock_id = metadata["id"]
        assert isinstance(stock_id, str)
        prior = metadata_by_stock.setdefault(stock_id, metadata)
        if prior != metadata:
            raise _fail("security mapping collision")
        canonical[class_key] = metadata
    return canonical


def _owner_roles(owner: Mapping[str, object]) -> list[str]:
    roles: list[str] = []
    if owner["is_officer"] is True:
        roles.append("Officer")
    if owner["is_director"] is True:
        roles.append("Director")
    if owner["is_ten_percent_owner"] is True:
        roles.append("TenPercentOwner")
    if owner["is_other"] is True:
        roles.append("Other")
    return roles or ["Other"]


def _fallback_company_title(roles: Iterable[str]) -> str:
    role_set = set(roles)
    if "Officer" in role_set:
        return "Officer"
    if "Director" in role_set:
        return "Director"
    if "TenPercentOwner" in role_set:
        return "10% Owner"
    return "Reporting Owner"


def _safe_company_title(value: object, *, roles: Iterable[str]) -> str:
    """Return only a title from the fixed public company-role vocabulary."""

    title = _safe_atom(value, "officer title", nullable=True, maximum=96)
    if title is None or _PUBLIC_PERSONAL_TEXT_RE.search(title) is not None:
        return _fallback_company_title(roles)
    for pattern, canonical in _PUBLIC_COMPANY_TITLE_PATTERNS:
        if pattern.search(title) is not None:
            return canonical
    return _fallback_company_title(roles)


def _owner_group(filing: Mapping[str, object]) -> dict[str, object]:
    owners = filing["owners"]
    assert isinstance(owners, list)
    names = [
        _safe_public_name(owner["name_as_filed"], "owner name", maximum=256)
        for owner in owners
    ]
    roles = {role for owner in owners for role in _owner_roles(owner)}
    role_order = {"Officer": 0, "Director": 1, "TenPercentOwner": 2, "Other": 3}
    sorted_roles = sorted(roles, key=role_order.__getitem__)
    officer_owners = [owner for owner in owners if owner["is_officer"] is True]
    primary_title = (
        _safe_company_title(
            officer_owners[0]["officer_title"],
            roles=_owner_roles(officer_owners[0]),
        )
        if officer_owners
        else _fallback_company_title(sorted_roles)
    )
    return {
        "displayName": " / ".join(str(name) for name in names),
        "ownerCount": len(owners),
        "roles": sorted_roles,
        "primaryTitle": primary_title,
        "isJoint": len(owners) > 1,
    }


def _owner_company_title(owner: Mapping[str, object]) -> str:
    roles = _owner_roles(owner)
    return _safe_company_title(owner["officer_title"], roles=roles)


def _public_owner(owner: Mapping[str, object]) -> dict[str, object]:
    return {
        "nameAsFiled": _safe_public_name(
            owner["name_as_filed"],
            "owner name",
            maximum=256,
        ),
        "companyTitle": _owner_company_title(owner),
        "roles": _owner_roles(owner),
    }


def _row_footnote_ids(row: Mapping[str, object]) -> list[str]:
    field_footnotes = row["field_footnotes"]
    assert isinstance(field_footnotes, dict)
    return sorted(
        {
            footnote_id
            for references in field_footnotes.values()
            for footnote_id in references
        }
    )


def _private_display_group_digest(
    row: Mapping[str, object],
    detail: Mapping[str, object],
    *,
    private_owner_group_key: str,
) -> str:
    material = {
        "accessionNumber": detail["accessionNumber"],
        "privateOwnerGroupKey": private_owner_group_key,
        "transactionDate": detail["transactionDate"],
        "securityId": detail["normalizedSecurityId"],
        "privateSourceTable": row["source_table"],
        "transactionCode": detail["transactionCode"],
        "acquiredDisposedCode": detail["acquiredDisposedCode"],
        "directIndirectOwnership": detail["directIndirectOwnership"],
        "pricePerShare": detail["pricePerShare"],
        "privateFootnoteIds": _row_footnote_ids(row),
    }
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(b"sis-insider-display-group-v1\0" + encoded).hexdigest()


def _private_public_display_group_key(accession: object, ordinal: object) -> str:
    """Bind one public detail group without exposing the private owner identity."""

    if type(accession) is not str or _ACCESSION_RE.fullmatch(accession) is None:
        raise _fail("public detail display-group accession")
    if type(ordinal) is not int or type(ordinal) is bool or ordinal < 1:
        raise _fail("public detail display-group ordinal")
    return hashlib.sha256(
        b"sis-public-detail-display-group-v1\0"
        + accession.encode("ascii")
        + b"\0"
        + str(ordinal).encode("ascii")
    ).hexdigest()


def _private_public_reconciliation_owner_key(accession: object) -> str:
    """Create a filing-local, in-memory owner key for public reconciliation only."""

    if type(accession) is not str or _ACCESSION_RE.fullmatch(accession) is None:
        raise _fail("public detail reconciliation accession")
    return hashlib.sha256(
        b"sis-public-filing-owner-reconciliation-v1\0" + accession.encode("ascii")
    ).hexdigest()


def _weighted_average_price(
    row: Mapping[str, object],
    footnotes_by_id: Mapping[str, str],
) -> bool:
    references = row["field_footnotes"].get("price_per_share", [])
    return any(
        "weighted average"
        in re.sub(
            r"[-\u2010-\u2015]+",
            " ",
            footnotes_by_id.get(footnote_id, "").casefold(),
        )
        for footnote_id in references
    )


def _detail_transaction(
    row: Mapping[str, object],
    *,
    filing: Mapping[str, object],
    effective: bool | None,
    owner_group: dict[str, object],
    mappings: Mapping[str, dict[str, object]],
    footnotes_by_id: Mapping[str, str],
) -> dict[str, object]:
    metadata = mappings.get(row["security_class_key"])
    underlying_metadata = mappings.get(row["underlying_security_class_key"])
    return {
        "normalizedSecurityId": None if metadata is None else metadata["id"],
        "transactionDate": row["transaction_date"],
        "deemedExecutionDate": row["deemed_execution_date"],
        "transactionFormType": row["transaction_form_type"],
        "transactionCode": row["transaction_code"],
        "transactionLabel": row["transaction_label"],
        "normalizedCategory": row["normalized_category"],
        "isMeaningfulPS": row["is_meaningful_ps"],
        "equitySwapInvolved": row["equity_swap_involved"],
        "transactionTimeliness": row["transaction_timeliness"],
        "shares": row["shares"],
        "pricePerShare": row["price_per_share"],
        "priceIsWeightedAverage": _weighted_average_price(row, footnotes_by_id),
        "reportedTotalValue": row["reported_total_value"],
        "calculatedValue": row["calculated_value"],
        "value": row["transaction_value"],
        "valueMethod": row["value_method"],
        "acquiredDisposedCode": row["acquired_disposed_code"],
        "postTransactionShares": row["post_transaction_shares"],
        "postTransactionValue": row["post_transaction_value"],
        "directIndirectOwnership": row["direct_indirect_ownership"],
        "conversionOrExercisePrice": row["conversion_or_exercise_price"],
        "exerciseDate": row["exercise_date"],
        "expirationDate": row["expiration_date"],
        "underlyingNormalizedSecurityId": (
            None if underlying_metadata is None else underlying_metadata["id"]
        ),
        "underlyingShares": row["underlying_shares"],
        "underlyingValue": row["underlying_value"],
        "planStatus": row["plan_status"],
        "ownerGroup": owner_group,
        "isAmended": filing["is_amendment"],
        "isSuperseded": None if effective is None else not effective,
        "formType": filing["form_type"],
        "filingDate": filing["filing_date"],
        "acceptedAt": filing["accepted_at"],
        "accessionNumber": filing["accession_number"],
        "secDocumentUrl": _safe_sec_url(
            filing["source"]["document_url"],
            "source document URL",
        ),
    }


def _metric_transaction(
    row: Mapping[str, object],
    detail: Mapping[str, object],
    *,
    private_owner_group_key: str,
) -> dict[str, object]:
    if _SHA256_RE.fullmatch(private_owner_group_key) is None:
        raise _fail("metric owner-group binding")
    return {
        key: detail[key]
        for key in (
            "acceptedAt",
            "accessionNumber",
            "acquiredDisposedCode",
            "deemedExecutionDate",
            "directIndirectOwnership",
            "filingDate",
            "formType",
            "isAmended",
            "isSuperseded",
            "normalizedCategory",
            "ownerGroup",
            "planStatus",
            "postTransactionShares",
            "priceIsWeightedAverage",
            "pricePerShare",
            "secDocumentUrl",
            "shares",
            "transactionCode",
            "transactionDate",
            "transactionLabel",
            "transactionTimeliness",
            "value",
            "valueMethod",
        )
    } | {
        "privateDisplayGroupKeyOverride": _private_public_display_group_key(
            detail["accessionNumber"],
            detail["displayGroupOrdinal"],
        ),
        "privateFootnoteIds": _row_footnote_ids(row),
        "privateOwnerGroupKey": private_owner_group_key,
        "privateRowKey": row["row_key"],
        "privateSourceRowIndex": row["source_row_index"],
        "privateSourceTable": row["source_table"],
        "securityId": detail["normalizedSecurityId"],
    }


def _detail_holding(
    row: Mapping[str, object],
    *,
    filing: Mapping[str, object],
    owner_group: dict[str, object],
    mappings: Mapping[str, dict[str, object]],
) -> dict[str, object]:
    metadata = mappings.get(row["security_class_key"])
    underlying_metadata = mappings.get(row["underlying_security_class_key"])
    return {
        "normalizedSecurityId": None if metadata is None else metadata["id"],
        "sharesOwned": row["shares_owned"],
        "valueOwned": row["value_owned"],
        "directIndirectOwnership": row["direct_indirect_ownership"],
        "conversionOrExercisePrice": row["conversion_or_exercise_price"],
        "exerciseDate": row["exercise_date"],
        "expirationDate": row["expiration_date"],
        "underlyingNormalizedSecurityId": (
            None if underlying_metadata is None else underlying_metadata["id"]
        ),
        "underlyingShares": row["underlying_shares"],
        "underlyingValue": row["underlying_value"],
        "ownerGroup": owner_group,
        "asOfDate": filing["period_of_report"] or filing["filing_date"],
        "formType": filing["form_type"],
        "filingDate": filing["filing_date"],
        "acceptedAt": filing["accepted_at"],
        "accessionNumber": filing["accession_number"],
    }


def _metric_holding(
    detail: Mapping[str, object],
    *,
    private_owner_group_key: str,
) -> dict[str, object]:
    if _SHA256_RE.fullmatch(private_owner_group_key) is None:
        raise _fail("metric holding owner-group binding")
    return {
        "asOfDate": detail["asOfDate"],
        "directIndirectOwnership": detail["directIndirectOwnership"],
        "ownerGroup": detail["ownerGroup"],
        "privateOwnerGroupKey": private_owner_group_key,
        "securityId": detail["normalizedSecurityId"],
        "shares": detail["sharesOwned"],
    }


def _materialize_bounded(
    values: Iterable[Mapping[str, object]],
    *,
    limit: int,
    label: str,
) -> list[Mapping[str, object]]:
    if isinstance(values, (str, bytes, Mapping)):
        raise _fail(label)
    try:
        iterator = iter(values)
    except TypeError as error:
        raise _fail(label) from error
    result: list[Mapping[str, object]] = []
    for value in iterator:
        if len(result) >= limit:
            raise _fail(label)
        result.append(value)
    return result


def build_static_insider_metric_projection(
    rows: Iterable[Mapping[str, object]],
    *,
    security_id: str,
    as_of: str,
    holdings: Iterable[Mapping[str, object]] | None = None,
    quality: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Embed every bounded row while retaining a 100-row client page size."""

    canonical_rows = _materialize_bounded(
        rows,
        limit=MAX_PUBLIC_TRANSACTION_ROWS,
        label="static metric row limit",
    )
    canonical_holdings = (
        []
        if holdings is None
        else _materialize_bounded(
            holdings,
            limit=MAX_PUBLIC_HOLDING_ROWS,
            label="static metric holding limit",
        )
    )
    complete_query: dict[str, object] = {
        "limit": 100,
        "range": "all",
        "transactionScope": "all",
    }

    projection = build_insider_metric_projection(
        canonical_rows,
        security_id=security_id,
        as_of=as_of,
        query=complete_query,
        holdings=canonical_holdings,
        quality=quality,
    )
    transactions = projection.get("transactions")
    if not isinstance(transactions, dict):
        raise _fail("static metric transactions")
    items = transactions.get("items")
    total = transactions.get("total")
    cursor = transactions.get("nextCursor")
    if not isinstance(items, list) or type(total) is not int or type(total) is bool:
        raise _fail("static metric transactions")
    all_items = list(items)
    seen_cursors: set[str] = set()
    while cursor is not None:
        if type(cursor) is not str or cursor in seen_cursors:
            raise _fail("static metric cursor")
        seen_cursors.add(cursor)
        page = build_insider_metric_projection(
            canonical_rows,
            security_id=security_id,
            as_of=as_of,
            query={**complete_query, "cursor": cursor},
            holdings=canonical_holdings,
            quality=quality,
        )
        page_transactions = page.get("transactions")
        if not isinstance(page_transactions, dict):
            raise _fail("static metric page")
        page_items = page_transactions.get("items")
        if not isinstance(page_items, list) or page_transactions.get("total") != total:
            raise _fail("static metric page")
        for key, value in projection.items():
            if key != "transactions" and page.get(key) != value:
                raise _fail("static metric page reconciliation")
        all_items.extend(page_items)
        cursor = page_transactions.get("nextCursor")
    if len(all_items) != total:
        raise _fail("static metric pagination completeness")
    projection["transactions"] = {
        **transactions,
        "items": all_items,
        "nextCursor": None,
    }
    projection["staticPagination"] = {
        "itemCount": len(all_items),
        "mode": "client",
        "pageSize": 100,
    }
    return projection


def _require_private_metric_reconciliation(
    page: Mapping[str, object],
    *,
    rows: list[dict[str, object]],
    security_id: str,
    as_of: str,
    holdings: list[dict[str, object]],
    quality: Mapping[str, object],
) -> None:
    """Recompute every page metric from private owner-bound inputs before release."""

    try:
        expected = build_static_insider_metric_projection(
            rows,
            security_id=security_id,
            as_of=as_of,
            holdings=holdings,
            quality=quality,
        )
    except InsiderMetricsError as error:
        raise _fail("private metric reconciliation") from error
    if any(page.get(key) != value for key, value in expected.items()):
        raise _fail("private metric reconciliation")


def _effective_versions(
    filings: list[dict[str, object]],
    state: Mapping[str, object],
) -> tuple[dict[str, bool | None], dict[str, dict[str, object]]]:
    by_accession = {filing["accession_number"]: filing for filing in filings}
    resolution = {
        amendment["accession_number"]: amendment for amendment in state["amendments"]
    }
    effective: dict[str, bool | None] = {
        accession: None if filing["is_amendment"] else True
        for accession, filing in by_accession.items()
    }
    resolved_by_original: dict[str, list[str]] = {}
    for accession, amendment in resolution.items():
        original = amendment["amends_accession"]
        if amendment["confidence"] != "unresolved" and isinstance(original, str):
            resolved_by_original.setdefault(original, []).append(accession)
    for original, accessions in resolved_by_original.items():
        if original not in by_accession:
            raise _fail("amendment original binding")
        newest = max(
            accessions,
            key=lambda accession: (
                by_accession[accession]["filing_date"],
                by_accession[accession]["accepted_at"] or "",
                accession,
            ),
        )
        effective[original] = False
        for accession in accessions:
            effective[accession] = accession == newest
    return effective, resolution


def _filing_detail(
    filing: Mapping[str, object],
    *,
    effective: bool | None,
    mappings: Mapping[str, dict[str, object]],
    private_owner_group_key: str,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    owner_group = _owner_group(filing)
    filing_transactions = filing["transactions"]
    filing_holdings = filing["holdings"]
    issuer = filing["issuer"]
    source = filing["source"]
    accession = filing["accession_number"]
    assert isinstance(filing_transactions, list)
    assert isinstance(filing_holdings, list)
    assert isinstance(issuer, dict)
    assert isinstance(source, dict)
    assert isinstance(accession, str)
    footnotes_by_id = {
        footnote["id"]: footnote["text"] for footnote in filing["footnotes"]
    }
    transactions = [
        _detail_transaction(
            row,
            filing=filing,
            effective=effective,
            owner_group=owner_group,
            mappings=mappings,
            footnotes_by_id=footnotes_by_id,
        )
        for row in filing_transactions
    ]
    display_group_digests = [
        _private_display_group_digest(
            row,
            detail,
            private_owner_group_key=private_owner_group_key,
        )
        for row, detail in zip(filing_transactions, transactions, strict=True)
    ]
    display_group_ordinals = {
        digest: index
        for index, digest in enumerate(sorted(set(display_group_digests)), start=1)
    }
    for row, detail, digest in zip(
        filing_transactions,
        transactions,
        display_group_digests,
        strict=True,
    ):
        detail["displayGroupOrdinal"] = display_group_ordinals[digest]
        detail["transactionTable"] = row["source_table"]
    holdings = [
        _detail_holding(
            row,
            filing=filing,
            owner_group=owner_group,
            mappings=mappings,
        )
        for row in filing_holdings
    ]
    detail: dict[str, object] = {
        "data_contract_version": DATA_CONTRACT_VERSION,
        "insider_public_contract_version": INSIDER_PUBLIC_CONTRACT_VERSION,
        "payloadType": "insider_filing_detail",
        "accessionNumber": accession,
        "filing": {
            "baseFormType": filing["base_form_type"],
            "formType": filing["form_type"],
            "isAmendment": filing["is_amendment"],
            "isCurrentEffectiveVersion": effective,
            "filingDate": filing["filing_date"],
            "acceptedAt": filing["accepted_at"],
            "originalSubmissionDate": filing["original_submission_date"],
            "periodOfReport": filing["period_of_report"],
            "aff10b5One": filing["aff10b5_one"],
            "notSubjectToSection16": filing["not_subject_to_section16"],
            "noSecuritiesOwned": filing["no_securities_owned"],
            "form3HoldingsReported": filing["form3_holdings_reported"],
            "form4TransactionsReported": filing["form4_transactions_reported"],
        },
        "issuer": {
            "cik": issuer["cik"],
            "nameAsFiled": _safe_public_name(
                issuer["name_as_filed"],
                "issuer name",
                maximum=256,
            ),
            "tradingSymbolAsFiled": _safe_public_symbol(
                issuer["trading_symbol_as_filed"],
                "issuer symbol",
                nullable=True,
            ),
            "foreignTradingSymbolAsFiled": _safe_public_symbol(
                issuer["foreign_trading_symbol_as_filed"],
                "issuer foreign symbol",
                nullable=True,
            ),
        },
        "ownerGroup": owner_group,
        "owners": [_public_owner(owner) for owner in filing["owners"]],
        "transactions": transactions,
        "holdings": holdings,
        "source": {
            "indexUrl": _safe_bound_sec_url(
                source["index_url"],
                "source index URL",
                issuer_cik=issuer["cik"],
                accession=accession,
            ),
            "documentUrl": _safe_bound_sec_url(
                source["document_url"],
                "source document URL",
                issuer_cik=issuer["cik"],
                accession=accession,
            ),
        },
        "publicationSafeguards": {
            "filingNarrativesOmitted": True,
            "ownerCiksOmitted": True,
            "ownerDisplayLimitedToNameAndCompanyTitle": True,
            "plainTextOnly": True,
            "rawSourceOmitted": True,
            "restrictedOwnerAddressesOmitted": True,
            "parserDiagnosticsOmitted": True,
            "signaturesOmitted": True,
        },
    }
    return detail, transactions, holdings


def _publication_manifest(
    *,
    issuer_ciks: Iterable[str],
    as_of: str,
    security_payloads: Mapping[str, dict[str, object]],
) -> dict[str, object]:
    canonical_as_of = _safe_utc_timestamp(as_of, "manifest asOf")
    assert isinstance(canonical_as_of, str)
    canonical_issuer_ciks = tuple(sorted(set(issuer_ciks)))
    if (
        not canonical_issuer_ciks
        or len(canonical_issuer_ciks) > MAX_PUBLIC_ISSUERS
        or any(
            type(issuer_cik) is not str
            or re.fullmatch(r"[0-9]{10}", issuer_cik) is None
            for issuer_cik in canonical_issuer_ciks
        )
    ):
        raise _fail("manifest issuer CIKs")
    if not security_payloads or len(security_payloads) > MAX_PUBLIC_SECURITY_FILES:
        raise _fail("manifest security payload count")
    security_entries = []
    for stem, payload in sorted(security_payloads.items()):
        if type(stem) is not str or _SECURITY_STEM_RE.fullmatch(stem) is None:
            raise _fail("manifest security file stem")
        encoded = canonical_public_json_bytes(payload)
        security_entries.append(
            {
                "path": f"securities/{stem}.json",
                "bytes": len(encoded),
                "sha256": hashlib.sha256(encoded).hexdigest(),
            }
        )
    return {
        "data_contract_version": DATA_CONTRACT_VERSION,
        "insider_public_contract_version": INSIDER_PUBLIC_CONTRACT_VERSION,
        "payloadType": "insider_publication_manifest",
        "issuerCiks": list(canonical_issuer_ciks),
        "asOf": canonical_as_of,
        "securityPayloads": security_entries,
    }


def build_insider_publication(
    filings: Iterable[Mapping[str, object]],
    *,
    issuer_state: object,
    security_mappings: object,
    as_of: str,
    latest_successful_sync_at: str | None,
) -> InsiderPublication:
    """Build deterministic, privacy-minimized page and filing payloads."""

    canonical_as_of = _safe_utc_timestamp(as_of, "asOf")
    assert isinstance(canonical_as_of, str)
    normalized = _bounded_validated_filings(filings)
    state, private_owner_keys = _bind_issuer_state(normalized, issuer_state)
    mappings = _validate_security_mappings(security_mappings, state)
    effective, _ = _effective_versions(normalized, state)
    issuer_cik = normalized[0]["issuer"]["cik"]

    details: dict[str, dict[str, object]] = {}
    detail_accessions_by_stock: dict[str, set[str]] = {}
    metric_rows_by_stock: dict[str, list[dict[str, object]]] = {}
    metric_holdings_by_stock: dict[str, list[dict[str, object]]] = {}
    metadata_by_stock: dict[str, dict[str, object]] = {}
    unmapped_effective_rows = 0
    for filing in normalized:
        accession = filing["accession_number"]
        assert isinstance(accession, str)
        private_owner_key = private_owner_keys[accession]
        detail, detail_transactions, detail_holdings = _filing_detail(
            filing,
            effective=effective[accession],
            mappings=mappings,
            private_owner_group_key=private_owner_key,
        )
        encoded_detail = canonical_public_json_bytes(detail)
        if len(encoded_detail) > MAX_PUBLIC_FILING_DETAIL_BYTES:
            raise _fail("filing detail size limit")
        details[accession] = detail
        for row in (*detail_transactions, *detail_holdings):
            stock_id = row["normalizedSecurityId"]
            if stock_id is None:
                continue
            assert isinstance(stock_id, str)
            detail_accessions_by_stock.setdefault(stock_id, set()).add(accession)
        if effective[accession] is not True:
            continue
        for private_row, transaction in zip(
            filing["transactions"],
            detail_transactions,
            strict=True,
        ):
            stock_id = transaction["normalizedSecurityId"]
            if stock_id is None:
                unmapped_effective_rows += 1
                continue
            assert isinstance(stock_id, str)
            metric_rows_by_stock.setdefault(stock_id, []).append(
                _metric_transaction(
                    private_row,
                    transaction,
                    private_owner_group_key=private_owner_key,
                )
            )
        for private_row, holding in zip(
            filing["holdings"],
            detail_holdings,
            strict=True,
        ):
            stock_id = holding["normalizedSecurityId"]
            if stock_id is None:
                unmapped_effective_rows += 1
                continue
            assert isinstance(stock_id, str)
            metric_holdings_by_stock.setdefault(stock_id, []).append(
                _metric_holding(
                    holding,
                    private_owner_group_key=private_owner_key,
                )
            )

    for metadata in mappings.values():
        stock_id = metadata["id"]
        assert isinstance(stock_id, str)
        prior = metadata_by_stock.setdefault(stock_id, metadata)
        if prior != metadata:
            raise _fail("security metadata collision")

    unresolved_count = len(state["unresolved_ambiguities"])
    security_payloads: dict[str, dict[str, object]] = {}
    published_accessions: set[str] = set()
    for stock_id in sorted(metadata_by_stock):
        metadata = metadata_by_stock[stock_id]
        stock_accessions = sorted(detail_accessions_by_stock.get(stock_id, set()))
        if (
            not stock_accessions
            or len(stock_accessions) > MAX_PUBLIC_FILING_REFS_PER_SECURITY
        ):
            raise _fail("per-security filing reference limit")
        filing_refs: list[dict[str, object]] = []
        for accession in stock_accessions:
            detail = details[accession]
            encoded_detail = canonical_public_json_bytes(detail)
            filing_refs.append(
                {
                    "accessionNumber": accession,
                    "bytes": len(encoded_detail),
                    "path": f"filings/{accession}.json",
                    "sha256": hashlib.sha256(encoded_detail).hexdigest(),
                }
            )
        published_accessions.update(stock_accessions)
        metric_rows = metric_rows_by_stock.get(stock_id, [])
        metric_holdings = metric_holdings_by_stock.get(stock_id, [])
        metric_quality: dict[str, object] = {
            "latestSuccessfulSyncAt": latest_successful_sync_at,
            "unmappedSecurityRowCount": unmapped_effective_rows,
            "unresolvedAmendmentCount": unresolved_count,
        }
        try:
            metrics = build_static_insider_metric_projection(
                metric_rows,
                security_id=stock_id,
                as_of=canonical_as_of,
                holdings=metric_holdings,
                quality=metric_quality,
            )
        except InsiderMetricsError as error:
            raise _fail("canonical metrics") from error
        page: dict[str, object] = {
            "data_contract_version": DATA_CONTRACT_VERSION,
            "insider_public_contract_version": INSIDER_PUBLIC_CONTRACT_VERSION,
            "payloadType": "security_insider_activity",
            "security": {
                **metadata,
                "issuerCik": issuer_cik,
            },
            "methodologyBanner": {
                "tone": "informational",
                "text": _PUBLIC_METHODOLOGY_TEXT,
                "actionLabel": "Learn more",
            },
            "filingRefs": filing_refs,
            **metrics,
        }
        _require_private_metric_reconciliation(
            page,
            rows=metric_rows,
            security_id=stock_id,
            as_of=canonical_as_of,
            holdings=metric_holdings,
            quality=metric_quality,
        )
        encoded_page = canonical_public_json_bytes(page)
        if len(encoded_page) > MAX_PUBLIC_SECURITY_PAYLOAD_BYTES:
            raise _fail("security payload size limit")
        stem = metadata["fileStem"]
        assert isinstance(stem, str)
        if stem in security_payloads:
            raise _fail("security file collision")
        security_payloads[stem] = page

    manifest = _publication_manifest(
        issuer_ciks=(issuer_cik,),
        as_of=canonical_as_of,
        security_payloads=security_payloads,
    )
    canonical_public_json_bytes(manifest)
    public_details = {
        accession: details[accession] for accession in sorted(published_accessions)
    }
    publication = InsiderPublication(
        issuer_ciks=(issuer_cik,),
        security_payloads=security_payloads,
        filing_payloads=public_details,
        manifest=manifest,
    )
    _validate_publication_bounds(publication)
    return publication


def _bounded_publications(
    publications: Iterable[InsiderPublication],
) -> list[InsiderPublication]:
    if isinstance(publications, (str, bytes, Mapping)):
        raise _fail("publication corpus type")
    try:
        iterator = iter(publications)
    except TypeError as error:
        raise _fail("publication corpus type") from error
    values: list[InsiderPublication] = []
    for publication in iterator:
        if len(values) >= MAX_PUBLIC_ISSUERS:
            raise _fail("publication corpus size")
        if type(publication) is not InsiderPublication:
            raise _fail("publication corpus type")
        values.append(publication)
    if not values:
        raise _fail("publication corpus size")
    return values


def _validate_publication_bounds(publication: InsiderPublication) -> int:
    if (
        not publication.security_payloads
        or len(publication.security_payloads) > MAX_PUBLIC_SECURITY_FILES
        or not publication.filing_payloads
        or len(publication.filing_payloads) > MAX_PUBLIC_FILING_FILES
        or len(publication.security_payloads) + len(publication.filing_payloads) + 1
        > MAX_PUBLIC_TOTAL_FILES
    ):
        raise _fail("publication file count")
    total_bytes = len(canonical_public_json_bytes(publication.manifest))
    if total_bytes > MAX_PUBLIC_SECURITY_PAYLOAD_BYTES:
        raise _fail("publication manifest size")
    for stem, payload in publication.security_payloads.items():
        if type(stem) is not str or _SECURITY_STEM_RE.fullmatch(stem) is None:
            raise _fail("security file stem")
        encoded = canonical_public_json_bytes(payload)
        if len(encoded) > MAX_PUBLIC_SECURITY_PAYLOAD_BYTES:
            raise _fail("security payload size limit")
        total_bytes += len(encoded)
    for accession, payload in publication.filing_payloads.items():
        if type(accession) is not str or _ACCESSION_RE.fullmatch(accession) is None:
            raise _fail("filing accession")
        encoded = canonical_public_json_bytes(payload)
        if len(encoded) > MAX_PUBLIC_FILING_DETAIL_BYTES:
            raise _fail("filing detail size limit")
        total_bytes += len(encoded)
    if total_bytes > MAX_PUBLIC_TOTAL_BYTES:
        raise _fail("publication byte limit")
    return total_bytes


def combine_insider_publications(
    publications: Iterable[InsiderPublication],
) -> InsiderPublication:
    """Combine bounded issuer projections into one deterministic public corpus."""

    values = _bounded_publications(publications)
    for publication in values:
        _validate_publication_bounds(publication)

    issuer_ciks: set[str] = set()
    security_payloads: dict[str, dict[str, object]] = {}
    filing_payloads: dict[str, dict[str, object]] = {}
    as_of_values: set[str] = set()
    for publication in values:
        if issuer_ciks.intersection(publication.issuer_ciks):
            raise _fail("duplicate issuer publication")
        issuer_ciks.update(publication.issuer_ciks)
        as_of = publication.manifest.get("asOf")
        if type(as_of) is not str:
            raise _fail("publication corpus asOf")
        as_of_values.add(as_of)
        for stem, payload in publication.security_payloads.items():
            if stem in security_payloads:
                raise _fail("security file collision")
            security_payloads[stem] = payload
        for accession, payload in publication.filing_payloads.items():
            if accession in filing_payloads:
                raise _fail("filing file collision")
            filing_payloads[accession] = payload
    if len(as_of_values) != 1:
        raise _fail("publication corpus asOf")

    canonical_issuer_ciks = tuple(sorted(issuer_ciks))
    canonical_security_payloads = dict(sorted(security_payloads.items()))
    canonical_filing_payloads = dict(sorted(filing_payloads.items()))
    manifest = _publication_manifest(
        issuer_ciks=canonical_issuer_ciks,
        as_of=next(iter(as_of_values)),
        security_payloads=canonical_security_payloads,
    )
    canonical_public_json_bytes(manifest)
    combined = InsiderPublication(
        issuer_ciks=canonical_issuer_ciks,
        security_payloads=canonical_security_payloads,
        filing_payloads=canonical_filing_payloads,
        manifest=manifest,
    )
    _validate_publication_bounds(combined)
    return combined


def _render_public_errors(errors: list[str]) -> str:
    rendered = "; ".join(errors[:10])
    if len(errors) > 10:
        rendered += f"; ... {len(errors) - 10} more"
    return rendered


_PUBLICATION_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_PUBLICATION_RECORD_FLAGS = (
    os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)
_PUBLICATION_TRANSACTION_ID_RE = re.compile(r"[0-9a-f]{64}")
_PUBLICATION_TRANSACTION_RECORD_RE = re.compile(
    r"\.public\.transaction-([0-9a-f]{64})\.json"
)
_PUBLICATION_STAGE_RE = re.compile(r"\.public\.prepare-([0-9a-f]{64})")
_PUBLICATION_BACKUP_RE = re.compile(r"\.public\.backup-([0-9a-f]{64})")
_PUBLICATION_TREE_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_MAX_PUBLICATION_TRANSACTION_BYTES = 4_096


@dataclass(frozen=True)
class _PublicationDirectoryIdentity:
    name: str
    st_dev: int
    st_ino: int


@dataclass(frozen=True)
class _PublicationTreeEntrySeal:
    kind: str
    st_dev: int
    st_ino: int
    st_mode: int
    st_size: int
    st_mtime_ns: int
    st_ctime_ns: int
    sha256: str | None


@dataclass(frozen=True)
class _PublicationTransaction:
    transaction_id: str
    state: str
    stage: _PublicationDirectoryIdentity
    backup: _PublicationDirectoryIdentity | None
    tree_sha256: str
    stage_tree_seal_sha256: str
    backup_tree_sha256: str | None


class _PublicationRecordCommitUncertain(InsiderPublicationError):
    """Raised after the durable state marker may have become published."""


class _PublicationStageChanged(InsiderPublicationError):
    """Raised when a staged descendant no longer matches its admitted seal."""


def _publication_transaction_names(transaction_id: str) -> tuple[str, str, str]:
    if _PUBLICATION_TRANSACTION_ID_RE.fullmatch(transaction_id) is None:
        raise _fail("publication transaction id")
    return (
        f".public.transaction-{transaction_id}.json",
        f".public.prepare-{transaction_id}",
        f".public.backup-{transaction_id}",
    )


def _publication_directory_identity(
    directory_fd: int,
    *,
    name: str,
    label: str,
) -> _PublicationDirectoryIdentity:
    try:
        metadata = os.fstat(directory_fd)
    except OSError as error:
        raise _fail(label) from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or type(metadata.st_dev) is not int
        or type(metadata.st_ino) is not int
        or metadata.st_dev < 0
        or metadata.st_ino < 1
    ):
        raise _fail(label)
    return _PublicationDirectoryIdentity(
        name=name,
        st_dev=metadata.st_dev,
        st_ino=metadata.st_ino,
    )


def _publication_identity_matches(
    directory_fd: int,
    identity: _PublicationDirectoryIdentity,
) -> bool:
    try:
        metadata = os.fstat(directory_fd)
    except OSError as error:
        raise _fail("publication directory identity") from error
    return stat.S_ISDIR(metadata.st_mode) and (metadata.st_dev, metadata.st_ino) == (
        identity.st_dev,
        identity.st_ino,
    )


def _publication_transaction_payload(
    transaction: _PublicationTransaction,
) -> dict[str, object]:
    record_name, stage_name, backup_name = _publication_transaction_names(
        transaction.transaction_id
    )
    del record_name
    if transaction.state not in {"prepared", "published"}:
        raise _fail("publication transaction state")
    if transaction.stage.name != stage_name:
        raise _fail("publication transaction stage")
    if transaction.backup is not None and transaction.backup.name != backup_name:
        raise _fail("publication transaction backup")
    if _PUBLICATION_TREE_SHA256_RE.fullmatch(transaction.tree_sha256) is None:
        raise _fail("publication transaction tree digest")
    if (
        _PUBLICATION_TREE_SHA256_RE.fullmatch(transaction.stage_tree_seal_sha256)
        is None
    ):
        raise _fail("publication transaction stage tree seal")
    if (transaction.backup is None) != (transaction.backup_tree_sha256 is None):
        raise _fail("publication transaction backup tree digest")
    if (
        transaction.backup_tree_sha256 is not None
        and _PUBLICATION_TREE_SHA256_RE.fullmatch(transaction.backup_tree_sha256)
        is None
    ):
        raise _fail("publication transaction backup tree digest")

    def identity_payload(
        identity: _PublicationDirectoryIdentity,
    ) -> dict[str, object]:
        if (
            type(identity.st_dev) is not int
            or type(identity.st_ino) is not int
            or identity.st_dev < 0
            or identity.st_ino < 1
        ):
            raise _fail("publication transaction directory identity")
        return {
            "name": identity.name,
            "st_dev": identity.st_dev,
            "st_ino": identity.st_ino,
        }

    return {
        "backup": (
            None if transaction.backup is None else identity_payload(transaction.backup)
        ),
        "backupTreeSha256": transaction.backup_tree_sha256,
        "kind": "insider-publication-transaction",
        "stage": identity_payload(transaction.stage),
        "stageTreeSealSha256": transaction.stage_tree_seal_sha256,
        "state": transaction.state,
        "transactionId": transaction.transaction_id,
        "treeSha256": transaction.tree_sha256,
        "version": 1,
    }


def _canonical_publication_transaction_bytes(
    transaction: _PublicationTransaction,
) -> bytes:
    return (
        json.dumps(
            _publication_transaction_payload(transaction),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        + b"\n"
    )


def _fsync_publication_directory(directory_fd: int, label: str) -> None:
    try:
        metadata = os.fstat(directory_fd)
        if not stat.S_ISDIR(metadata.st_mode):
            raise _fail(label)
        os.fsync(directory_fd)
    except InsiderPublicationError:
        raise
    except OSError as error:
        raise _fail(f"{label} fsync") from error


def _open_publication_directory(path: Path, label: str) -> int:
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise _fail("secure directory-descriptor publication is unavailable")
    try:
        directory_fd = os.open(os.fspath(path), _PUBLICATION_DIRECTORY_FLAGS)
    except OSError as error:
        raise _fail(label) from error
    try:
        if not stat.S_ISDIR(os.fstat(directory_fd).st_mode):
            raise _fail(label)
        return directory_fd
    except BaseException:
        os.close(directory_fd)
        raise


def _open_publication_directory_at(
    parent_fd: int,
    name: str,
    label: str,
) -> int:
    try:
        directory_fd = os.open(
            name,
            _PUBLICATION_DIRECTORY_FLAGS,
            dir_fd=parent_fd,
        )
    except OSError as error:
        raise _fail(label) from error
    try:
        if not stat.S_ISDIR(os.fstat(directory_fd).st_mode):
            raise _fail(label)
        return directory_fd
    except BaseException:
        os.close(directory_fd)
        raise


def _ensure_publication_directory_at(
    parent_fd: int,
    name: str,
    label: str,
) -> int:
    created = False
    try:
        os.mkdir(name, mode=0o755, dir_fd=parent_fd)
        created = True
    except FileExistsError:
        pass
    except OSError as error:
        raise _fail(label) from error
    directory_fd = _open_publication_directory_at(parent_fd, name, label)
    try:
        if created:
            os.fchmod(directory_fd, 0o755)
            _fsync_publication_directory(directory_fd, label)
            _fsync_publication_directory(parent_fd, f"{label} parent")
        return directory_fd
    except BaseException:
        os.close(directory_fd)
        raise


def _require_named_directory_identity(
    parent_fd: int,
    name: str,
    directory_fd: int,
    label: str,
) -> None:
    try:
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        opened = os.fstat(directory_fd)
    except OSError as error:
        raise _fail(f"{label} changed during publication") from error
    if (
        not stat.S_ISDIR(named.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
    ):
        raise _fail(f"{label} changed during publication")


def _revalidate_publication_anchor(
    root_path: Path,
    root_fd: int,
    data_fd: int,
    insiders_fd: int,
) -> None:
    try:
        named = os.stat(root_path, follow_symlinks=False)
        opened = os.fstat(root_fd)
    except OSError as error:
        raise _fail("repository root changed during publication") from error
    if (
        not stat.S_ISDIR(named.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
    ):
        raise _fail("repository root changed during publication")
    _require_named_directory_identity(root_fd, "data", data_fd, "data root")
    _require_named_directory_identity(
        data_fd, "insiders", insiders_fd, "insider data root"
    )


def _publication_checkpoint(
    label: str,
    root_path: Path,
    root_fd: int,
    data_fd: int,
    insiders_fd: int,
) -> None:
    del label
    _revalidate_publication_anchor(root_path, root_fd, data_fd, insiders_fd)


def _open_optional_publication_directory_at(
    parent_fd: int,
    name: str,
    label: str,
) -> int | None:
    try:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise _fail(label) from error
    if not stat.S_ISDIR(before.st_mode):
        raise _fail(label)
    directory_fd = _open_publication_directory_at(parent_fd, name, label)
    try:
        opened = os.fstat(directory_fd)
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise _fail(f"{label} changed during publication")
        return directory_fd
    except BaseException:
        os.close(directory_fd)
        raise


def _open_exact_publication_directory_at(
    parent_fd: int,
    identity: _PublicationDirectoryIdentity,
    label: str,
) -> int | None:
    directory_fd = _open_optional_publication_directory_at(
        parent_fd,
        identity.name,
        label,
    )
    if directory_fd is None:
        return None
    try:
        if not _publication_identity_matches(directory_fd, identity):
            raise _fail(f"{label} identity mismatch")
        return directory_fd
    except BaseException:
        os.close(directory_fd)
        raise


def _strict_publication_transaction_object(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate publication transaction key")
        result[key] = value
    return result


def _parse_publication_directory_identity(
    value: object,
    *,
    expected_name: str,
    label: str,
) -> _PublicationDirectoryIdentity:
    if type(value) is not dict or set(value) != {"name", "st_dev", "st_ino"}:
        raise _fail(label)
    name = value.get("name")
    st_dev = value.get("st_dev")
    st_ino = value.get("st_ino")
    if (
        name != expected_name
        or type(st_dev) is not int
        or type(st_ino) is not int
        or st_dev < 0
        or st_ino < 1
    ):
        raise _fail(label)
    return _PublicationDirectoryIdentity(
        name=expected_name,
        st_dev=st_dev,
        st_ino=st_ino,
    )


def _parse_publication_transaction_bytes(
    encoded: bytes,
    *,
    expected_transaction_id: str,
) -> _PublicationTransaction:
    try:
        decoded = encoded.decode("ascii")
        payload = json.loads(
            decoded,
            object_pairs_hook=_strict_publication_transaction_object,
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise _fail("publication transaction record") from error
    if type(payload) is not dict or set(payload) != {
        "backup",
        "backupTreeSha256",
        "kind",
        "stage",
        "stageTreeSealSha256",
        "state",
        "transactionId",
        "treeSha256",
        "version",
    }:
        raise _fail("publication transaction record")
    if (
        payload.get("kind") != "insider-publication-transaction"
        or payload.get("version") != 1
        or type(payload.get("version")) is not int
        or payload.get("transactionId") != expected_transaction_id
        or payload.get("state") not in {"prepared", "published"}
        or type(payload.get("treeSha256")) is not str
        or _PUBLICATION_TREE_SHA256_RE.fullmatch(payload["treeSha256"]) is None
        or type(payload.get("stageTreeSealSha256")) is not str
        or _PUBLICATION_TREE_SHA256_RE.fullmatch(payload["stageTreeSealSha256"]) is None
    ):
        raise _fail("publication transaction record")
    _, stage_name, backup_name = _publication_transaction_names(expected_transaction_id)
    stage = _parse_publication_directory_identity(
        payload.get("stage"),
        expected_name=stage_name,
        label="publication transaction stage",
    )
    backup_value = payload.get("backup")
    backup = (
        None
        if backup_value is None
        else _parse_publication_directory_identity(
            backup_value,
            expected_name=backup_name,
            label="publication transaction backup",
        )
    )
    backup_tree_sha256 = payload.get("backupTreeSha256")
    if (backup is None) != (backup_tree_sha256 is None) or (
        backup_tree_sha256 is not None
        and (
            type(backup_tree_sha256) is not str
            or _PUBLICATION_TREE_SHA256_RE.fullmatch(backup_tree_sha256) is None
        )
    ):
        raise _fail("publication transaction backup tree digest")
    transaction = _PublicationTransaction(
        transaction_id=expected_transaction_id,
        state=payload["state"],
        stage=stage,
        backup=backup,
        tree_sha256=payload["treeSha256"],
        stage_tree_seal_sha256=payload["stageTreeSealSha256"],
        backup_tree_sha256=backup_tree_sha256,
    )
    if encoded != _canonical_publication_transaction_bytes(transaction):
        raise _fail("publication transaction record is not canonical")
    return transaction


def _require_named_publication_record_identity(
    parent_fd: int,
    name: str,
    record_fd: int,
) -> None:
    try:
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        opened = os.fstat(record_fd)
    except OSError as error:
        raise _fail("publication transaction record changed") from error
    if (
        not stat.S_ISREG(named.st_mode)
        or not stat.S_ISREG(opened.st_mode)
        or named.st_nlink != 1
        or opened.st_nlink != 1
        or stat.S_IMODE(named.st_mode) != 0o600
        or stat.S_IMODE(opened.st_mode) != 0o600
        or named.st_uid != os.geteuid()
        or opened.st_uid != os.geteuid()
        or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
    ):
        raise _fail("publication transaction record changed")


def _open_publication_transaction_at(
    parent_fd: int,
    record_name: str,
    transaction_id: str,
) -> tuple[int, _PublicationTransaction]:
    try:
        before = os.stat(record_name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as error:
        raise _fail("publication transaction record") from error
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_uid != os.geteuid()
        or before.st_size < 1
        or before.st_size > _MAX_PUBLICATION_TRANSACTION_BYTES
    ):
        raise _fail("publication transaction record")
    try:
        record_fd = os.open(
            record_name,
            _PUBLICATION_RECORD_FLAGS,
            dir_fd=parent_fd,
        )
    except OSError as error:
        raise _fail("publication transaction record") from error
    try:
        opened_before = os.fstat(record_fd)
        if (before.st_dev, before.st_ino) != (
            opened_before.st_dev,
            opened_before.st_ino,
        ) or opened_before.st_size != before.st_size:
            raise _fail("publication transaction record changed")
        chunks: list[bytes] = []
        remaining = _MAX_PUBLICATION_TRANSACTION_BYTES + 1
        while remaining > 0:
            chunk = os.read(record_fd, min(remaining, 4096))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        encoded = b"".join(chunks)
        if (
            len(encoded) != before.st_size
            or len(encoded) > _MAX_PUBLICATION_TRANSACTION_BYTES
        ):
            raise _fail("publication transaction record")
        opened_after = os.fstat(record_fd)
        if (
            opened_before.st_dev,
            opened_before.st_ino,
            opened_before.st_size,
            opened_before.st_mtime_ns,
            opened_before.st_ctime_ns,
        ) != (
            opened_after.st_dev,
            opened_after.st_ino,
            opened_after.st_size,
            opened_after.st_mtime_ns,
            opened_after.st_ctime_ns,
        ):
            raise _fail("publication transaction record changed")
        _require_named_publication_record_identity(parent_fd, record_name, record_fd)
        transaction = _parse_publication_transaction_bytes(
            encoded,
            expected_transaction_id=transaction_id,
        )
        return record_fd, transaction
    except BaseException:
        os.close(record_fd)
        raise


def _write_all_publication_record(record_fd: int, encoded: bytes) -> None:
    view = memoryview(encoded)
    while view:
        try:
            written = os.write(record_fd, view)
        except OSError as error:
            raise _fail("publication transaction record") from error
        if written < 1:
            raise _fail("publication transaction record")
        view = view[written:]


def _unlink_owned_publication_record_at(
    parent_fd: int,
    name: str,
    record_fd: int,
) -> None:
    _require_named_publication_record_identity(parent_fd, name, record_fd)
    try:
        os.unlink(name, dir_fd=parent_fd)
    except OSError as error:
        raise _fail("publication transaction record cleanup") from error
    _fsync_publication_directory(parent_fd, "publication transaction record parent")


def _create_publication_transaction_at(
    parent_fd: int,
    transaction: _PublicationTransaction,
) -> int:
    record_name, _, _ = _publication_transaction_names(transaction.transaction_id)
    encoded = _canonical_publication_transaction_bytes(transaction)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        record_fd = os.open(record_name, flags, 0o600, dir_fd=parent_fd)
    except OSError as error:
        raise _fail("publication transaction record") from error
    try:
        _write_all_publication_record(record_fd, encoded)
        os.fchmod(record_fd, 0o600)
        os.fsync(record_fd)
        _require_named_publication_record_identity(parent_fd, record_name, record_fd)
        _fsync_publication_directory(parent_fd, "publication transaction record parent")
        return record_fd
    except BaseException:
        try:
            _unlink_owned_publication_record_at(parent_fd, record_name, record_fd)
        except BaseException:
            pass
        os.close(record_fd)
        raise


def _replace_publication_transaction_at(
    parent_fd: int,
    old_record_fd: int,
    transaction: _PublicationTransaction,
) -> int:
    record_name, _, _ = _publication_transaction_names(transaction.transaction_id)
    temporary_name = (
        f".public.transaction-update-{transaction.transaction_id}-"
        f"{secrets.token_hex(16)}.tmp"
    )
    encoded = _canonical_publication_transaction_bytes(transaction)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        new_record_fd = os.open(temporary_name, flags, 0o600, dir_fd=parent_fd)
    except OSError as error:
        raise _fail("publication transaction record update") from error
    replaced = False
    try:
        _write_all_publication_record(new_record_fd, encoded)
        os.fchmod(new_record_fd, 0o600)
        os.fsync(new_record_fd)
        _require_named_publication_record_identity(
            parent_fd,
            temporary_name,
            new_record_fd,
        )
        _require_named_publication_record_identity(
            parent_fd, record_name, old_record_fd
        )
        try:
            os.replace(
                temporary_name,
                record_name,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
            )
            replaced = True
        except (NotImplementedError, TypeError) as error:
            raise _fail(
                "secure transaction record replacement is unavailable"
            ) from error
        except OSError as error:
            raise _fail("publication transaction record update") from error
        try:
            _fsync_publication_directory(
                parent_fd,
                "publication transaction record parent",
            )
            _require_named_publication_record_identity(
                parent_fd,
                record_name,
                new_record_fd,
            )
        except BaseException as error:
            raise _PublicationRecordCommitUncertain(
                "publication transaction state is uncertain"
            ) from error
        return new_record_fd
    except BaseException:
        if not replaced:
            try:
                _unlink_owned_publication_record_at(
                    parent_fd,
                    temporary_name,
                    new_record_fd,
                )
            except BaseException:
                pass
        os.close(new_record_fd)
        raise


def _create_publication_stage_at(
    parent_fd: int,
) -> tuple[str, str, str, int]:
    for _ in range(100):
        transaction_id = secrets.token_hex(32)
        record_name, stage_name, backup_name = _publication_transaction_names(
            transaction_id
        )
        try:
            os.mkdir(stage_name, mode=0o700, dir_fd=parent_fd)
        except FileExistsError:
            continue
        except OSError as error:
            raise _fail("publication staging directory") from error
        stage_fd: int | None = None
        try:
            stage_fd = _open_publication_directory_at(
                parent_fd,
                stage_name,
                "publication staging directory",
            )
            os.fchmod(stage_fd, 0o700)
            _fsync_publication_directory(stage_fd, "publication staging directory")
            _fsync_publication_directory(
                parent_fd,
                "publication staging directory parent",
            )
            return record_name, stage_name, backup_name, stage_fd
        except BaseException:
            if stage_fd is not None:
                try:
                    _require_named_directory_identity(
                        parent_fd,
                        stage_name,
                        stage_fd,
                        "publication staging directory",
                    )
                    os.rmdir(stage_name, dir_fd=parent_fd)
                    _fsync_publication_directory(
                        parent_fd,
                        "publication staging directory parent",
                    )
                except BaseException:
                    pass
                os.close(stage_fd)
            raise
    raise _fail("could not allocate publication staging directory")


def _write_public_file_at(
    directory_fd: int,
    name: str,
    payload: object,
) -> None:
    encoded = canonical_public_json_bytes(payload)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        file_fd = os.open(name, flags, 0o600, dir_fd=directory_fd)
    except OSError as error:
        raise _fail(f"public file {name}") from error
    try:
        view = memoryview(encoded)
        while view:
            written = os.write(file_fd, view)
            if written < 1:
                raise _fail(f"public file {name}")
            view = view[written:]
        os.fchmod(file_fd, 0o644)
        os.utime(file_fd, (0, 0))
        os.fsync(file_fd)
    except InsiderPublicationError:
        raise
    except OSError as error:
        raise _fail(f"public file {name}") from error
    finally:
        os.close(file_fd)


def _unlink_owned_publication_file_at(
    parent_fd: int,
    name: str,
    expected: os.stat_result,
    label: str,
) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        file_fd = os.open(name, flags, dir_fd=parent_fd)
    except OSError as error:
        raise _fail(f"{label} entry changed during cleanup") from error
    try:
        opened = os.fstat(file_fd)
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        expected_identity = (expected.st_dev, expected.st_ino)
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(named.st_mode)
            or opened.st_nlink != 1
            or named.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != expected_identity
            or (named.st_dev, named.st_ino) != expected_identity
        ):
            raise _fail(f"{label} entry changed during cleanup")
        try:
            os.unlink(name, dir_fd=parent_fd)
        except OSError as error:
            raise _fail(f"{label} entry") from error
    finally:
        os.close(file_fd)


def _remove_publication_tree_contents_at(
    directory_fd: int,
    label: str,
    *,
    remaining: list[int] | None = None,
    depth: int = 0,
) -> None:
    if remaining is None:
        remaining = [MAX_PUBLIC_TOTAL_FILES + 10]
    if depth > 4:
        raise _fail(f"{label} path depth")
    try:
        entries = sorted(os.scandir(directory_fd), key=lambda entry: entry.name)
    except OSError as error:
        raise _fail(label) from error
    remaining[0] -= len(entries)
    if remaining[0] < 0:
        raise _fail(f"{label} file count")
    for entry in entries:
        try:
            metadata = os.stat(
                entry.name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError as error:
            raise _fail(f"{label} entry") from error
        if stat.S_ISDIR(metadata.st_mode):
            child_fd = _open_publication_directory_at(
                directory_fd,
                entry.name,
                label,
            )
            try:
                _remove_publication_tree_contents_at(
                    child_fd,
                    label,
                    remaining=remaining,
                    depth=depth + 1,
                )
                _require_named_directory_identity(
                    directory_fd,
                    entry.name,
                    child_fd,
                    label,
                )
                os.rmdir(entry.name, dir_fd=directory_fd)
            finally:
                os.close(child_fd)
        elif stat.S_ISREG(metadata.st_mode):
            _unlink_owned_publication_file_at(
                directory_fd,
                entry.name,
                metadata,
                label,
            )
        else:
            raise _fail(f"{label} contains an unsupported entry")
    _fsync_publication_directory(directory_fd, label)


def _remove_owned_publication_directory_at(
    parent_fd: int,
    name: str,
    directory_fd: int,
    label: str,
    *,
    expected_tree_sha256: str | None = None,
    expected_stage_seal_sha256: str | None = None,
    expected_backup_tree_sha256: str | None = None,
) -> None:
    _require_named_directory_identity(parent_fd, name, directory_fd, label)
    if (expected_tree_sha256 is None) != (expected_stage_seal_sha256 is None):
        raise _fail(f"{label} removal seal")
    if expected_tree_sha256 is not None:
        assert expected_stage_seal_sha256 is not None
        _require_publication_transaction_stage_tree(
            directory_fd,
            expected_tree_sha256=expected_tree_sha256,
            expected_seal_sha256=expected_stage_seal_sha256,
            label=label,
        )
    if expected_backup_tree_sha256 is not None:
        _require_publication_transaction_backup_tree_digest(
            directory_fd,
            expected_sha256=expected_backup_tree_sha256,
            label=label,
        )
    _remove_publication_tree_contents_at(directory_fd, label)
    _require_named_directory_identity(parent_fd, name, directory_fd, label)
    try:
        os.rmdir(name, dir_fd=parent_fd)
    except OSError as error:
        raise _fail(label) from error
    _fsync_publication_directory(parent_fd, f"{label} parent")


def _require_publication_name_absent(
    parent_fd: int,
    name: str,
    label: str,
) -> None:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    except OSError as error:
        raise _fail(label) from error
    raise _fail(f"{label} already exists")


def _replace_publication_directory_at(
    parent_fd: int,
    source_name: str,
    target_name: str,
    label: str,
    *,
    source_fd: int | None = None,
    expected_tree_sha256: str | None = None,
    expected_stage_seal_sha256: str | None = None,
    expected_backup_tree_sha256: str | None = None,
) -> None:
    if (
        any(
            value is not None
            for value in (
                expected_tree_sha256,
                expected_stage_seal_sha256,
                expected_backup_tree_sha256,
            )
        )
        and source_fd is None
    ):
        raise _fail(f"{label} source verification")
    if source_fd is not None:
        _require_named_directory_identity(
            parent_fd,
            source_name,
            source_fd,
            label,
        )
    if (expected_tree_sha256 is None) != (expected_stage_seal_sha256 is None):
        raise _fail(f"{label} source seal")
    if expected_tree_sha256 is not None:
        assert source_fd is not None
        assert expected_stage_seal_sha256 is not None
        _require_publication_transaction_stage_tree(
            source_fd,
            expected_tree_sha256=expected_tree_sha256,
            expected_seal_sha256=expected_stage_seal_sha256,
            label=label,
        )
    if expected_backup_tree_sha256 is not None:
        assert source_fd is not None
        _require_publication_transaction_backup_tree_digest(
            source_fd,
            expected_sha256=expected_backup_tree_sha256,
            label=label,
        )
    try:
        os.replace(
            source_name,
            target_name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
    except (NotImplementedError, TypeError) as error:
        raise _fail("secure directory-descriptor publication is unavailable") from error
    except OSError as error:
        raise _fail(label) from error
    _fsync_publication_directory(parent_fd, f"{label} parent")


def _validate_publication_directory_fd(
    directory_fd: int,
    label: str,
) -> tuple[dict[str, bytes], list[str]]:
    snapshot, errors = _read_validated_insider_public_snapshot_fd(directory_fd)
    if errors:
        return {}, [f"{label}: {error}" for error in errors]
    return snapshot, []


def _publication_tree_sha256(snapshot: Mapping[str, bytes]) -> str:
    digest = hashlib.sha256()
    for relative in sorted(snapshot):
        payload = snapshot[relative]
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
    return digest.hexdigest()


def _publication_descendant_seal_sha256(
    seal: tuple[tuple[str, _PublicationTreeEntrySeal], ...],
) -> str:
    """Bind every descendant identity while allowing the root directory to be renamed."""

    digest = hashlib.sha256()
    for relative, entry in seal:
        if relative == ".":
            continue
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(entry.kind.encode("ascii"))
        digest.update(b"\0")
        for value in (
            entry.st_dev,
            entry.st_ino,
            entry.st_mode,
            entry.st_size,
            entry.st_mtime_ns,
            entry.st_ctime_ns,
        ):
            digest.update(str(value).encode("ascii"))
            digest.update(b"\0")
        digest.update((entry.sha256 or "").encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def _publication_backup_tree_sha256(directory_fd: int) -> str:
    """Digest a bounded prior tree, including descendant identities and bytes."""

    digest = hashlib.sha256()
    remaining_entries = [MAX_PUBLIC_TOTAL_FILES + 10]
    remaining_bytes = [MAX_PUBLIC_TOTAL_BYTES + MAX_PUBLIC_SECURITY_PAYLOAD_BYTES]

    def update_digest(
        kind: bytes,
        relative: str,
        metadata: os.stat_result,
        payload: bytes | None,
    ) -> None:
        try:
            encoded_relative = relative.encode("utf-8")
        except UnicodeEncodeError as error:
            raise _fail("publication backup path encoding") from error
        if not encoded_relative or len(encoded_relative) > 1_024:
            raise _fail("publication backup path length")
        digest.update(kind)
        digest.update(b"\0")
        digest.update(encoded_relative)
        digest.update(b"\0")
        for value in (
            *_publication_tree_metadata_identity(metadata),
            metadata.st_uid,
            metadata.st_nlink,
        ):
            digest.update(str(value).encode("ascii"))
            digest.update(b"\0")
        if payload is not None:
            digest.update(payload)
            digest.update(b"\0")

    def walk(current_fd: int, prefix: str, depth: int) -> None:
        if depth > 4:
            raise _fail("publication backup path depth")
        try:
            before = os.fstat(current_fd)
            if not stat.S_ISDIR(before.st_mode):
                raise _fail("publication backup entry")
            with os.scandir(current_fd) as scan:
                entries = sorted(scan, key=lambda entry: entry.name)
        except InsiderPublicationError:
            raise
        except OSError as error:
            raise _fail("publication backup tree") from error
        remaining_entries[0] -= len(entries)
        if remaining_entries[0] < 0:
            raise _fail("publication backup file count")
        for entry in entries:
            name = entry.name
            relative = f"{prefix}/{name}" if prefix else name
            try:
                metadata = os.stat(
                    name,
                    dir_fd=current_fd,
                    follow_symlinks=False,
                )
            except OSError as error:
                raise _fail("publication backup entry") from error
            if stat.S_ISDIR(metadata.st_mode):
                child_fd = _open_publication_directory_at(
                    current_fd,
                    name,
                    "publication backup directory",
                )
                try:
                    _require_named_directory_identity(
                        current_fd,
                        name,
                        child_fd,
                        "publication backup directory",
                    )
                    update_digest(b"D", relative, metadata, None)
                    walk(child_fd, relative, depth + 1)
                    _require_named_directory_identity(
                        current_fd,
                        name,
                        child_fd,
                        "publication backup directory",
                    )
                    try:
                        after = os.stat(
                            name,
                            dir_fd=current_fd,
                            follow_symlinks=False,
                        )
                    except OSError as error:
                        raise _fail("publication backup directory changed") from error
                    if _publication_tree_metadata_identity(
                        metadata
                    ) != _publication_tree_metadata_identity(after):
                        raise _fail("publication backup directory changed")
                finally:
                    os.close(child_fd)
                continue
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise _fail("publication backup contains an unsupported entry")
            maximum = min(MAX_PUBLIC_SECURITY_PAYLOAD_BYTES, remaining_bytes[0])
            if maximum < 1:
                raise _fail("publication backup byte count")
            errors: list[str] = []
            encoded = _read_regular_at(
                current_fd,
                name,
                relative=relative,
                label=f"publication backup {relative}",
                maximum=maximum,
                errors=errors,
                allow_empty=True,
            )
            if errors or encoded is None:
                raise _fail(
                    _render_public_errors(errors or ["publication backup entry"])
                )
            try:
                after = os.stat(name, dir_fd=current_fd, follow_symlinks=False)
            except OSError as error:
                raise _fail("publication backup file changed") from error
            if _publication_tree_metadata_identity(
                metadata
            ) != _publication_tree_metadata_identity(after):
                raise _fail("publication backup file changed")
            remaining_bytes[0] -= len(encoded)
            update_digest(b"F", relative, after, encoded)
        try:
            after = os.fstat(current_fd)
        except OSError as error:
            raise _fail("publication backup tree") from error
        if _publication_tree_metadata_identity(
            before
        ) != _publication_tree_metadata_identity(after):
            raise _fail("publication backup tree changed")

    walk(directory_fd, "", 0)
    return digest.hexdigest()


def _require_publication_transaction_tree_digest(
    directory_fd: int,
    *,
    expected_sha256: str,
    label: str,
) -> None:
    snapshot, errors = _validate_publication_directory_fd(directory_fd, label)
    if errors:
        raise _fail(_render_public_errors(errors))
    if _publication_tree_sha256(snapshot) != expected_sha256:
        raise _fail(f"{label} tree digest mismatch")


def _require_publication_transaction_stage_tree(
    directory_fd: int,
    *,
    expected_tree_sha256: str,
    expected_seal_sha256: str,
    label: str,
) -> None:
    snapshot, seal, errors = _read_validated_insider_public_snapshot_sealed_fd(
        directory_fd
    )
    if errors:
        raise _fail(_render_public_errors([f"{label}: {error}" for error in errors]))
    if _publication_tree_sha256(snapshot) != expected_tree_sha256:
        raise _fail(f"{label} tree digest mismatch")
    if _publication_descendant_seal_sha256(seal) != expected_seal_sha256:
        raise _fail(f"{label} tree seal mismatch")


def _require_publication_transaction_backup_tree_digest(
    directory_fd: int,
    *,
    expected_sha256: str,
    label: str,
) -> None:
    if _publication_backup_tree_sha256(directory_fd) != expected_sha256:
        raise _fail(f"{label} tree digest mismatch")


def _publication_directory_role(
    directory_fd: int | None,
    transaction: _PublicationTransaction,
) -> str | None:
    if directory_fd is None:
        return None
    if _publication_identity_matches(directory_fd, transaction.stage):
        return "stage"
    if transaction.backup is not None and _publication_identity_matches(
        directory_fd,
        transaction.backup,
    ):
        return "backup"
    return "unknown"


def _recover_publication_transaction_at(
    *,
    insiders_fd: int,
    record_name: str,
    record_fd: int,
    transaction: _PublicationTransaction,
) -> None:
    stage_fd = _open_exact_publication_directory_at(
        insiders_fd,
        transaction.stage,
        "publication staging directory",
    )
    backup_fd = (
        None
        if transaction.backup is None
        else _open_exact_publication_directory_at(
            insiders_fd,
            transaction.backup,
            "publication backup",
        )
    )
    public_fd = _open_optional_publication_directory_at(
        insiders_fd,
        "public",
        "public output root",
    )
    try:
        public_role = _publication_directory_role(public_fd, transaction)
        if public_role == "unknown":
            raise _fail("public output does not match publication transaction")
        if stage_fd is not None:
            _require_publication_transaction_stage_tree(
                stage_fd,
                expected_tree_sha256=transaction.tree_sha256,
                expected_seal_sha256=transaction.stage_tree_seal_sha256,
                label="publication staging directory",
            )
        if public_role == "stage":
            assert public_fd is not None
            _require_publication_transaction_stage_tree(
                public_fd,
                expected_tree_sha256=transaction.tree_sha256,
                expected_seal_sha256=transaction.stage_tree_seal_sha256,
                label="installed public tree",
            )
        if backup_fd is not None:
            assert transaction.backup_tree_sha256 is not None
            _require_publication_transaction_backup_tree_digest(
                backup_fd,
                expected_sha256=transaction.backup_tree_sha256,
                label="publication backup",
            )
        if public_role == "backup":
            assert public_fd is not None
            assert transaction.backup_tree_sha256 is not None
            _require_publication_transaction_backup_tree_digest(
                public_fd,
                expected_sha256=transaction.backup_tree_sha256,
                label="public backup generation",
            )

        if transaction.state == "published":
            if stage_fd is not None or public_fd is None or public_role != "stage":
                raise _fail("published publication transaction topology")
            if backup_fd is not None:
                assert transaction.backup is not None
                assert transaction.backup_tree_sha256 is not None
                _remove_owned_publication_directory_at(
                    insiders_fd,
                    transaction.backup.name,
                    backup_fd,
                    "publication backup",
                    expected_backup_tree_sha256=transaction.backup_tree_sha256,
                )
            _unlink_owned_publication_record_at(
                insiders_fd,
                record_name,
                record_fd,
            )
            return

        if transaction.state != "prepared":
            raise _fail("publication transaction state")

        if transaction.backup is None:
            if public_fd is None:
                if stage_fd is not None:
                    _remove_owned_publication_directory_at(
                        insiders_fd,
                        transaction.stage.name,
                        stage_fd,
                        "publication staging directory",
                        expected_tree_sha256=transaction.tree_sha256,
                        expected_stage_seal_sha256=transaction.stage_tree_seal_sha256,
                    )
                _unlink_owned_publication_record_at(
                    insiders_fd,
                    record_name,
                    record_fd,
                )
                return
            if public_role == "stage" and stage_fd is None:
                _remove_owned_publication_directory_at(
                    insiders_fd,
                    "public",
                    public_fd,
                    "new public output",
                    expected_tree_sha256=transaction.tree_sha256,
                    expected_stage_seal_sha256=transaction.stage_tree_seal_sha256,
                )
                _unlink_owned_publication_record_at(
                    insiders_fd,
                    record_name,
                    record_fd,
                )
                return
            raise _fail("prepared publication transaction topology")

        if public_role == "backup" and backup_fd is None:
            if stage_fd is not None:
                _remove_owned_publication_directory_at(
                    insiders_fd,
                    transaction.stage.name,
                    stage_fd,
                    "publication staging directory",
                    expected_tree_sha256=transaction.tree_sha256,
                    expected_stage_seal_sha256=transaction.stage_tree_seal_sha256,
                )
            _unlink_owned_publication_record_at(
                insiders_fd,
                record_name,
                record_fd,
            )
            return

        if public_fd is None and backup_fd is not None:
            assert transaction.backup is not None
            assert transaction.backup_tree_sha256 is not None
            _require_named_directory_identity(
                insiders_fd,
                transaction.backup.name,
                backup_fd,
                "publication backup",
            )
            _require_publication_name_absent(
                insiders_fd,
                "public",
                "publication backup recovery target",
            )
            _replace_publication_directory_at(
                insiders_fd,
                transaction.backup.name,
                "public",
                "publication backup recovery",
                source_fd=backup_fd,
                expected_backup_tree_sha256=transaction.backup_tree_sha256,
            )
            _require_named_directory_identity(
                insiders_fd,
                "public",
                backup_fd,
                "public output root",
            )
            if stage_fd is not None:
                _remove_owned_publication_directory_at(
                    insiders_fd,
                    transaction.stage.name,
                    stage_fd,
                    "publication staging directory",
                    expected_tree_sha256=transaction.tree_sha256,
                    expected_stage_seal_sha256=transaction.stage_tree_seal_sha256,
                )
            _unlink_owned_publication_record_at(
                insiders_fd,
                record_name,
                record_fd,
            )
            return

        if public_role == "stage" and stage_fd is None and backup_fd is not None:
            assert public_fd is not None
            assert transaction.backup is not None
            assert transaction.backup_tree_sha256 is not None
            _remove_owned_publication_directory_at(
                insiders_fd,
                "public",
                public_fd,
                "new public output",
                expected_tree_sha256=transaction.tree_sha256,
                expected_stage_seal_sha256=transaction.stage_tree_seal_sha256,
            )
            _require_named_directory_identity(
                insiders_fd,
                transaction.backup.name,
                backup_fd,
                "publication backup",
            )
            _require_publication_name_absent(
                insiders_fd,
                "public",
                "publication rollback target",
            )
            _replace_publication_directory_at(
                insiders_fd,
                transaction.backup.name,
                "public",
                "publication rollback",
                source_fd=backup_fd,
                expected_backup_tree_sha256=transaction.backup_tree_sha256,
            )
            _require_named_directory_identity(
                insiders_fd,
                "public",
                backup_fd,
                "public output root",
            )
            _unlink_owned_publication_record_at(
                insiders_fd,
                record_name,
                record_fd,
            )
            return

        raise _fail("prepared publication transaction topology")
    finally:
        if public_fd is not None:
            os.close(public_fd)
        if backup_fd is not None:
            os.close(backup_fd)
        if stage_fd is not None:
            os.close(stage_fd)


def _recover_public_transactions_at(
    *,
    root_path: Path,
    root_fd: int,
    data_fd: int,
    insiders_fd: int,
) -> None:
    try:
        entries = sorted(os.scandir(insiders_fd), key=lambda entry: entry.name)
    except OSError as error:
        raise _fail("insider data root") from error

    records: dict[str, str] = {}
    stages: dict[str, str] = {}
    backups: dict[str, str] = {}
    for entry in entries:
        name = entry.name
        record_match = _PUBLICATION_TRANSACTION_RECORD_RE.fullmatch(name)
        stage_match = _PUBLICATION_STAGE_RE.fullmatch(name)
        backup_match = _PUBLICATION_BACKUP_RE.fullmatch(name)
        if record_match is not None:
            transaction_id = record_match.group(1)
            if transaction_id in records:
                raise _fail("duplicate publication transaction record")
            records[transaction_id] = name
            continue
        if stage_match is not None:
            transaction_id = stage_match.group(1)
            if transaction_id in stages:
                raise _fail("duplicate publication staging directory")
            stages[transaction_id] = name
            continue
        if backup_match is not None:
            transaction_id = backup_match.group(1)
            if transaction_id in backups:
                raise _fail("duplicate publication backup")
            backups[transaction_id] = name
            continue
        if name.startswith(".public.prepare-"):
            raise _fail("unknown publication staging directory")
        if name.startswith(".public.backup"):
            raise _fail("unknown publication backup")
        if name.startswith(".public.transaction-"):
            raise _fail("unknown publication transaction record")

    if len(records) > 1:
        raise _fail("multiple publication transactions")
    for transaction_id in stages:
        if transaction_id not in records:
            raise _fail("unknown publication staging directory")
    for transaction_id in backups:
        if transaction_id not in records:
            raise _fail("unknown publication backup")
    if not records:
        return

    transaction_id, record_name = next(iter(records.items()))
    record_fd, transaction = _open_publication_transaction_at(
        insiders_fd,
        record_name,
        transaction_id,
    )
    try:
        if transaction.stage.name != stages.get(
            transaction_id,
            transaction.stage.name,
        ):
            raise _fail("publication transaction stage")
        if transaction_id in backups and (
            transaction.backup is None
            or transaction.backup.name != backups[transaction_id]
        ):
            raise _fail("publication transaction backup")
        _publication_checkpoint(
            "before_transaction_recovery",
            root_path,
            root_fd,
            data_fd,
            insiders_fd,
        )
        _recover_publication_transaction_at(
            insiders_fd=insiders_fd,
            record_name=record_name,
            record_fd=record_fd,
            transaction=transaction,
        )
    finally:
        os.close(record_fd)


def _publication_summary(
    publication: InsiderPublication,
    snapshot: Mapping[str, bytes],
) -> dict[str, object]:
    files = sorted(snapshot)
    total_bytes = sum(len(snapshot[relative]) for relative in files)
    return {
        "filingPayloadCount": len(publication.filing_payloads),
        "securityPayloadCount": len(publication.security_payloads),
        "publicBytes": total_bytes,
        "publicFileCount": len(files),
        "treeSha256": _publication_tree_sha256(snapshot),
    }


def _expected_publication_snapshot(
    publication: InsiderPublication,
) -> dict[str, bytes]:
    snapshot = {
        "manifest.json": canonical_public_json_bytes(publication.manifest),
    }
    snapshot.update(
        {
            f"securities/{stem}.json": canonical_public_json_bytes(payload)
            for stem, payload in sorted(publication.security_payloads.items())
        }
    )
    snapshot.update(
        {
            f"filings/{accession}.json": canonical_public_json_bytes(payload)
            for accession, payload in sorted(publication.filing_payloads.items())
        }
    )
    return dict(sorted(snapshot.items()))


def _read_admitted_publication_stage(
    stage_fd: int,
    expected_snapshot: Mapping[str, bytes],
) -> tuple[tuple[str, _PublicationTreeEntrySeal], ...]:
    snapshot, seal, errors = _read_validated_insider_public_snapshot_sealed_fd(stage_fd)
    if errors:
        raise _fail(_render_public_errors([f"staged public tree: {e}" for e in errors]))
    if snapshot != expected_snapshot:
        raise _fail("staged public tree does not match requested publication")
    return seal


def _require_admitted_publication_stage_unchanged(
    *,
    insiders_fd: int,
    stage_name: str,
    stage_fd: int,
    expected_snapshot: Mapping[str, bytes],
    expected_seal: tuple[tuple[str, _PublicationTreeEntrySeal], ...],
) -> None:
    try:
        _require_named_directory_identity(
            insiders_fd,
            stage_name,
            stage_fd,
            "publication staging directory",
        )
        snapshot, seal, errors = _read_validated_insider_public_snapshot_sealed_fd(
            stage_fd
        )
    except InsiderPublicationError as error:
        raise _PublicationStageChanged(
            "insider publication is invalid: staged public tree changed during publication"
        ) from error
    if errors or snapshot != expected_snapshot or seal != expected_seal:
        raise _PublicationStageChanged(
            "insider publication is invalid: staged public tree changed during publication"
        )


def _read_admitted_installed_publication(
    public_fd: int,
    expected_snapshot: Mapping[str, bytes],
    expected_seal: tuple[tuple[str, _PublicationTreeEntrySeal], ...],
) -> tuple[
    dict[str, bytes],
    tuple[tuple[str, _PublicationTreeEntrySeal], ...],
]:
    snapshot, seal, errors = _read_validated_insider_public_snapshot_sealed_fd(
        public_fd
    )
    if (
        errors
        or snapshot != expected_snapshot
        or _publication_descendant_seal_sha256(seal)
        != _publication_descendant_seal_sha256(expected_seal)
    ):
        raise _PublicationStageChanged(
            "insider publication is invalid: staged public tree changed during publication"
        )
    return snapshot, seal


def _require_installed_publication_unchanged(
    *,
    insiders_fd: int,
    public_fd: int,
    expected_snapshot: Mapping[str, bytes],
    expected_seal: tuple[tuple[str, _PublicationTreeEntrySeal], ...],
) -> None:
    try:
        _require_named_directory_identity(
            insiders_fd,
            "public",
            public_fd,
            "public output root",
        )
        snapshot, seal, errors = _read_validated_insider_public_snapshot_sealed_fd(
            public_fd
        )
    except InsiderPublicationError as error:
        raise _PublicationStageChanged(
            "insider publication is invalid: staged public tree changed during publication"
        ) from error
    if errors or snapshot != expected_snapshot or seal != expected_seal:
        raise _PublicationStageChanged(
            "insider publication is invalid: staged public tree changed during publication"
        )


def _write_insider_publication_unlocked(
    publication: InsiderPublication,
    *,
    root_path: Path,
    root_fd: int,
    data_fd: int,
    insiders_fd: int,
) -> dict[str, object]:
    """Replace ``data/insiders/public`` through a durable private journal."""

    expected_snapshot = _expected_publication_snapshot(publication)
    expected_tree_sha256 = _publication_tree_sha256(expected_snapshot)
    record_name, stage_name, backup_name, stage_fd = _create_publication_stage_at(
        insiders_fd
    )
    record_match = _PUBLICATION_TRANSACTION_RECORD_RE.fullmatch(record_name)
    if record_match is None:
        os.close(stage_fd)
        raise _fail("publication transaction record")
    transaction_id = record_match.group(1)
    stage_is_named = True
    record_fd: int | None = None
    transaction: _PublicationTransaction | None = None
    transaction_state: str | None = None
    backup_fd: int | None = None
    old_public_fd: int | None = None
    final_snapshot: dict[str, bytes] = {}
    stage_cleanup_safe = True
    staging_seal: tuple[tuple[str, _PublicationTreeEntrySeal], ...] | None = None
    stage_tree_seal_sha256: str | None = None
    try:
        securities_fd: int | None = None
        filings_fd: int | None = None
        try:
            securities_fd = _ensure_publication_directory_at(
                stage_fd,
                "securities",
                "publication securities directory",
            )
            filings_fd = _ensure_publication_directory_at(
                stage_fd,
                "filings",
                "publication filings directory",
            )
            for stem, payload in sorted(publication.security_payloads.items()):
                _write_public_file_at(securities_fd, f"{stem}.json", payload)
            for accession, payload in sorted(publication.filing_payloads.items()):
                _write_public_file_at(filings_fd, f"{accession}.json", payload)
            _write_public_file_at(stage_fd, "manifest.json", publication.manifest)
            for directory_fd, label in (
                (securities_fd, "publication securities directory"),
                (filings_fd, "publication filings directory"),
            ):
                os.fchmod(directory_fd, 0o755)
                os.utime(directory_fd, (0, 0))
                _fsync_publication_directory(directory_fd, label)
        finally:
            if filings_fd is not None:
                os.close(filings_fd)
            if securities_fd is not None:
                os.close(securities_fd)
        os.fchmod(stage_fd, 0o755)
        os.utime(stage_fd, (0, 0))
        _fsync_publication_directory(stage_fd, "publication staging directory")

        staging_seal = _read_admitted_publication_stage(stage_fd, expected_snapshot)
        stage_tree_seal_sha256 = _publication_descendant_seal_sha256(staging_seal)
        old_public_fd = _open_optional_publication_directory_at(
            insiders_fd,
            "public",
            "public output root",
        )
        backup_tree_sha256: str | None = None
        if old_public_fd is not None:
            backup_tree_sha256 = _publication_backup_tree_sha256(old_public_fd)
        _publication_checkpoint(
            "before_commit",
            root_path,
            root_fd,
            data_fd,
            insiders_fd,
        )
        _require_admitted_publication_stage_unchanged(
            insiders_fd=insiders_fd,
            stage_name=stage_name,
            stage_fd=stage_fd,
            expected_snapshot=expected_snapshot,
            expected_seal=staging_seal,
        )

        stage_identity = _publication_directory_identity(
            stage_fd,
            name=stage_name,
            label="publication staging directory",
        )
        backup_identity = (
            None
            if old_public_fd is None
            else _publication_directory_identity(
                old_public_fd,
                name=backup_name,
                label="public output root",
            )
        )
        transaction = _PublicationTransaction(
            transaction_id=transaction_id,
            state="prepared",
            stage=stage_identity,
            backup=backup_identity,
            tree_sha256=expected_tree_sha256,
            stage_tree_seal_sha256=stage_tree_seal_sha256,
            backup_tree_sha256=backup_tree_sha256,
        )
        _publication_checkpoint(
            "before_namespace_commit",
            root_path,
            root_fd,
            data_fd,
            insiders_fd,
        )
        _require_admitted_publication_stage_unchanged(
            insiders_fd=insiders_fd,
            stage_name=stage_name,
            stage_fd=stage_fd,
            expected_snapshot=expected_snapshot,
            expected_seal=staging_seal,
        )
        _publication_checkpoint(
            "before_publish",
            root_path,
            root_fd,
            data_fd,
            insiders_fd,
        )
        _require_admitted_publication_stage_unchanged(
            insiders_fd=insiders_fd,
            stage_name=stage_name,
            stage_fd=stage_fd,
            expected_snapshot=expected_snapshot,
            expected_seal=staging_seal,
        )

        record_fd = _create_publication_transaction_at(insiders_fd, transaction)
        transaction_state = "prepared"

        if old_public_fd is not None:
            assert transaction.backup_tree_sha256 is not None
            _require_publication_transaction_backup_tree_digest(
                old_public_fd,
                expected_sha256=transaction.backup_tree_sha256,
                label="existing public backup",
            )
            _require_named_directory_identity(
                insiders_fd,
                "public",
                old_public_fd,
                "public output root",
            )
            _require_publication_name_absent(
                insiders_fd,
                backup_name,
                "publication backup target",
            )
            _replace_publication_directory_at(
                insiders_fd,
                "public",
                backup_name,
                "publication backup",
                source_fd=old_public_fd,
                expected_backup_tree_sha256=transaction.backup_tree_sha256,
            )
            backup_fd = old_public_fd
            old_public_fd = None
            _require_named_directory_identity(
                insiders_fd,
                backup_name,
                backup_fd,
                "publication backup",
            )
        _require_admitted_publication_stage_unchanged(
            insiders_fd=insiders_fd,
            stage_name=stage_name,
            stage_fd=stage_fd,
            expected_snapshot=expected_snapshot,
            expected_seal=staging_seal,
        )
        _require_publication_name_absent(
            insiders_fd,
            "public",
            "public output publication target",
        )
        _replace_publication_directory_at(
            insiders_fd,
            stage_name,
            "public",
            "public output publication",
            source_fd=stage_fd,
            expected_tree_sha256=transaction.tree_sha256,
            expected_stage_seal_sha256=transaction.stage_tree_seal_sha256,
        )
        stage_is_named = False
        _require_named_directory_identity(
            insiders_fd,
            "public",
            stage_fd,
            "public output root",
        )
        final_snapshot, installed_seal = _read_admitted_installed_publication(
            stage_fd,
            expected_snapshot,
            staging_seal,
        )
        summary = _publication_summary(publication, final_snapshot)

        published_transaction = _PublicationTransaction(
            transaction_id=transaction.transaction_id,
            state="published",
            stage=transaction.stage,
            backup=transaction.backup,
            tree_sha256=transaction.tree_sha256,
            stage_tree_seal_sha256=transaction.stage_tree_seal_sha256,
            backup_tree_sha256=transaction.backup_tree_sha256,
        )
        try:
            published_record_fd = _replace_publication_transaction_at(
                insiders_fd,
                record_fd,
                published_transaction,
            )
        except _PublicationRecordCommitUncertain:
            transaction = published_transaction
            transaction_state = "published"
            raise
        old_record_fd = record_fd
        record_fd = published_record_fd
        transaction = published_transaction
        transaction_state = "published"
        os.close(old_record_fd)

        _publication_checkpoint(
            "before_backup_cleanup",
            root_path,
            root_fd,
            data_fd,
            insiders_fd,
        )
        _require_installed_publication_unchanged(
            insiders_fd=insiders_fd,
            public_fd=stage_fd,
            expected_snapshot=expected_snapshot,
            expected_seal=installed_seal,
        )
        _publication_checkpoint(
            "before_return",
            root_path,
            root_fd,
            data_fd,
            insiders_fd,
        )
        _require_installed_publication_unchanged(
            insiders_fd=insiders_fd,
            public_fd=stage_fd,
            expected_snapshot=expected_snapshot,
            expected_seal=installed_seal,
        )
        if backup_fd is not None:
            assert transaction.backup_tree_sha256 is not None
            _require_publication_transaction_backup_tree_digest(
                backup_fd,
                expected_sha256=transaction.backup_tree_sha256,
                label="publication backup",
            )
            _remove_owned_publication_directory_at(
                insiders_fd,
                backup_name,
                backup_fd,
                "publication backup",
                expected_backup_tree_sha256=transaction.backup_tree_sha256,
            )
            os.close(backup_fd)
            backup_fd = None
        _require_installed_publication_unchanged(
            insiders_fd=insiders_fd,
            public_fd=stage_fd,
            expected_snapshot=expected_snapshot,
            expected_seal=installed_seal,
        )
        _unlink_owned_publication_record_at(
            insiders_fd,
            record_name,
            record_fd,
        )
        transaction_state = None
        return summary
    except BaseException as error:
        if isinstance(error, _PublicationRecordCommitUncertain):
            transaction_state = "published"
        stage_changed = isinstance(error, _PublicationStageChanged)
        if stage_changed:
            stage_cleanup_safe = False
        rollback_error: BaseException | None = None
        try:
            if stage_changed:
                if not stage_is_named:
                    _require_named_directory_identity(
                        insiders_fd,
                        "public",
                        stage_fd,
                        "changed public output",
                    )
                    _require_publication_name_absent(
                        insiders_fd,
                        stage_name,
                        "changed publication quarantine target",
                    )
                    _replace_publication_directory_at(
                        insiders_fd,
                        "public",
                        stage_name,
                        "changed publication quarantine",
                        source_fd=stage_fd,
                    )
                    stage_is_named = True
                if backup_fd is not None:
                    assert transaction is not None
                    assert transaction.backup_tree_sha256 is not None
                    _require_publication_transaction_backup_tree_digest(
                        backup_fd,
                        expected_sha256=transaction.backup_tree_sha256,
                        label="publication backup",
                    )
                    _require_named_directory_identity(
                        insiders_fd,
                        backup_name,
                        backup_fd,
                        "publication backup",
                    )
                    _require_publication_name_absent(
                        insiders_fd,
                        "public",
                        "publication rollback target",
                    )
                    _replace_publication_directory_at(
                        insiders_fd,
                        backup_name,
                        "public",
                        "publication rollback",
                        source_fd=backup_fd,
                        expected_backup_tree_sha256=transaction.backup_tree_sha256,
                    )
                if record_fd is not None:
                    _unlink_owned_publication_record_at(
                        insiders_fd,
                        record_name,
                        record_fd,
                    )
                transaction_state = None
            elif (
                transaction_state == "prepared"
                and transaction is not None
                and record_fd is not None
            ):
                _recover_publication_transaction_at(
                    insiders_fd=insiders_fd,
                    record_name=record_name,
                    record_fd=record_fd,
                    transaction=transaction,
                )
                transaction_state = None
            elif transaction_state is None and stage_is_named and stage_cleanup_safe:
                if staging_seal is None or stage_tree_seal_sha256 is None:
                    stage_cleanup_safe = False
                else:
                    try:
                        _require_admitted_publication_stage_unchanged(
                            insiders_fd=insiders_fd,
                            stage_name=stage_name,
                            stage_fd=stage_fd,
                            expected_snapshot=expected_snapshot,
                            expected_seal=staging_seal,
                        )
                        _remove_owned_publication_directory_at(
                            insiders_fd,
                            stage_name,
                            stage_fd,
                            "publication staging directory",
                            expected_tree_sha256=expected_tree_sha256,
                            expected_stage_seal_sha256=stage_tree_seal_sha256,
                        )
                        stage_is_named = False
                    except _PublicationStageChanged:
                        stage_cleanup_safe = False
        except BaseException as caught:
            rollback_error = caught
        if rollback_error is not None:
            raise _fail(f"publication rollback failed: {rollback_error}") from error
        raise
    finally:
        if old_public_fd is not None:
            os.close(old_public_fd)
        if backup_fd is not None:
            os.close(backup_fd)
        if record_fd is not None:
            os.close(record_fd)
        os.close(stage_fd)


def write_insider_publication(
    publication: InsiderPublication,
    *,
    repository_root: Path,
) -> dict[str, object]:
    """Publish with a descriptor lock, validation, rollback, and recovery."""

    if type(publication) is not InsiderPublication:
        raise _fail("publication type")
    _validate_publication_bounds(publication)
    root_path = Path(os.path.abspath(os.fspath(repository_root)))
    root_fd = _open_publication_directory(root_path, "repository root")
    data_fd: int | None = None
    insiders_fd: int | None = None
    insiders_locked = False
    try:
        data_fd = _ensure_publication_directory_at(root_fd, "data", "data root")
        insiders_fd = _ensure_publication_directory_at(
            data_fd,
            "insiders",
            "insider data root",
        )
        _publication_checkpoint(
            "after_admission",
            root_path,
            root_fd,
            data_fd,
            insiders_fd,
        )
        try:
            fcntl.flock(insiders_fd, fcntl.LOCK_EX)
            insiders_locked = True
        except OSError as error:
            raise _fail("publication lock") from error
        _publication_checkpoint(
            "after_lock",
            root_path,
            root_fd,
            data_fd,
            insiders_fd,
        )
        _recover_public_transactions_at(
            root_path=root_path,
            root_fd=root_fd,
            data_fd=data_fd,
            insiders_fd=insiders_fd,
        )
        return _write_insider_publication_unlocked(
            publication,
            root_path=root_path,
            root_fd=root_fd,
            data_fd=data_fd,
            insiders_fd=insiders_fd,
        )
    finally:
        if insiders_locked and insiders_fd is not None:
            try:
                fcntl.flock(insiders_fd, fcntl.LOCK_UN)
            except OSError:
                pass
        if insiders_fd is not None:
            os.close(insiders_fd)
        if data_fd is not None:
            os.close(data_fd)
        os.close(root_fd)


class _DuplicatePublicKeyError(ValueError):
    pass


def _unique_public_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicatePublicKeyError(key)
        result[key] = value
    return result


def _load_public_payload(
    path: Path,
    *,
    maximum: int,
    errors: list[str],
) -> dict[str, object] | None:
    label = path.as_posix()
    if path.is_symlink():
        errors.append(f"{label} must not be a symlink")
        return None
    if not path.is_file():
        errors.append(f"{label} is missing")
        return None
    try:
        encoded = path.read_bytes()
    except OSError as error:
        errors.append(f"{label} cannot be read: {error}")
        return None
    if not encoded or len(encoded) > maximum:
        errors.append(f"{label} exceeds its bounded public size")
        return None
    try:
        payload = json.loads(
            encoded,
            object_pairs_hook=_unique_public_object,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        _DuplicatePublicKeyError,
        ValueError,
    ) as error:
        errors.append(f"{label} is invalid JSON: {error}")
        return None
    if not isinstance(payload, dict):
        errors.append(f"{label} must contain a JSON object")
        return None
    try:
        canonical = canonical_public_json_bytes(payload)
    except InsiderPublicationError as error:
        errors.append(f"{label} violates the public contract: {error}")
        return payload
    if encoded != canonical:
        errors.append(f"{label} is not canonical public JSON")
    return payload


def _manifest_entries(
    manifest: Mapping[str, object],
    key: str,
    *,
    path_pattern: re.Pattern[str],
    errors: list[str],
) -> list[dict[str, object]]:
    raw = manifest.get(key)
    if not isinstance(raw, list):
        errors.append(f"insider manifest {key} must be a list")
        return []
    entries: list[dict[str, object]] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict) or set(item) != {"bytes", "path", "sha256"}:
            errors.append(f"insider manifest {key}[{index}] fields are invalid")
            continue
        relative = item.get("path")
        byte_count = item.get("bytes")
        digest = item.get("sha256")
        if (
            type(relative) is not str
            or path_pattern.fullmatch(relative) is None
            or relative in seen
            or type(byte_count) is not int
            or type(byte_count) is bool
            or byte_count < 1
            or type(digest) is not str
            or _SHA256_RE.fullmatch(digest) is None
        ):
            errors.append(f"insider manifest {key}[{index}] values are invalid")
            continue
        seen.add(relative)
        entries.append(item)
    if [entry["path"] for entry in entries] != sorted(
        entry["path"] for entry in entries
    ):
        errors.append(f"insider manifest {key} must be sorted")
    return entries


def _filing_references(
    page: Mapping[str, object],
    relative: str,
    errors: list[str],
) -> list[dict[str, object]]:
    raw = page.get("filingRefs")
    if (
        not isinstance(raw, list)
        or not raw
        or len(raw) > MAX_PUBLIC_FILING_REFS_PER_SECURITY
    ):
        errors.append(f"{relative} filingRefs are invalid")
        return []
    entries: list[dict[str, object]] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict) or set(item) != {
            "accessionNumber",
            "bytes",
            "path",
            "sha256",
        }:
            errors.append(f"{relative} filingRefs[{index}] fields are invalid")
            continue
        accession = item.get("accessionNumber")
        path = item.get("path")
        byte_count = item.get("bytes")
        digest = item.get("sha256")
        expected_path = f"filings/{accession}.json" if type(accession) is str else None
        if (
            type(accession) is not str
            or _ACCESSION_RE.fullmatch(accession) is None
            or path != expected_path
            or path in seen
            or type(byte_count) is not int
            or type(byte_count) is bool
            or not 1 <= byte_count <= MAX_PUBLIC_FILING_DETAIL_BYTES
            or type(digest) is not str
            or _SHA256_RE.fullmatch(digest) is None
        ):
            errors.append(f"{relative} filingRefs[{index}] values are invalid")
            continue
        assert isinstance(path, str)
        seen.add(path)
        entries.append(item)
    if [entry["accessionNumber"] for entry in entries] != sorted(
        entry["accessionNumber"] for entry in entries
    ):
        errors.append(f"{relative} filingRefs must be sorted")
    return entries


def _validate_manifest_file(
    public_root: Path,
    entry: Mapping[str, object],
    errors: list[str],
) -> None:
    relative = entry["path"]
    assert isinstance(relative, str)
    path = public_root / relative
    if path.is_symlink():
        errors.append(f"{relative} must not be a symlink")
        return
    if not path.is_file():
        errors.append(f"insider manifest payload is missing: {relative}")
        return
    try:
        encoded = path.read_bytes()
    except OSError as error:
        errors.append(f"insider manifest payload cannot be read: {relative}: {error}")
        return
    if len(encoded) != entry["bytes"]:
        errors.append(f"insider manifest byte count does not match {relative}")
    if hashlib.sha256(encoded).hexdigest() != entry["sha256"]:
        errors.append(f"insider manifest hash does not match {relative}")


_PUBLIC_OWNER_ROLE_ORDER = {
    "Officer": 0,
    "Director": 1,
    "TenPercentOwner": 2,
    "Other": 3,
}
_PUBLIC_OWNER_GROUP_FIELDS = {
    "displayName",
    "isJoint",
    "ownerCount",
    "primaryTitle",
    "roles",
}


def _validate_public_company_title_value(
    value: object,
    *,
    label: str,
) -> None:
    title = _safe_atom(value, label, maximum=96)
    if title not in _PUBLIC_COMPANY_TITLES:
        raise _fail(label)


def _validate_public_owner_group_shape(
    accession: str,
    owner_group: object,
    errors: list[str],
) -> None:
    if (
        not isinstance(owner_group, dict)
        or set(owner_group) != _PUBLIC_OWNER_GROUP_FIELDS
    ):
        errors.append(f"filing {accession} owner group fields are invalid")
        return
    try:
        _safe_public_name(
            owner_group.get("displayName"),
            f"filing {accession} owner group display name",
            maximum=512,
        )
        _validate_public_company_title_value(
            owner_group.get("primaryTitle"),
            label=f"filing {accession} owner group title",
        )
    except InsiderPublicationError as error:
        errors.append(str(error))
    owner_count = owner_group.get("ownerCount")
    is_joint = owner_group.get("isJoint")
    roles = owner_group.get("roles")
    if (
        type(owner_count) is not int
        or type(owner_count) is bool
        or not 1 <= owner_count <= 100
        or type(is_joint) is not bool
        or is_joint != (owner_count > 1)
        or not isinstance(roles, list)
        or not roles
        or any(
            type(role) is not str or role not in _PUBLIC_OWNER_ROLE_ORDER
            for role in roles
        )
        or roles != sorted(set(roles), key=_PUBLIC_OWNER_ROLE_ORDER.__getitem__)
    ):
        errors.append(f"filing {accession} owner group values are invalid")


def _validate_public_owners(
    accession: str,
    owners: object,
    errors: list[str],
) -> None:
    if not isinstance(owners, list) or not owners or len(owners) > 100:
        errors.append(f"filing {accession} owners are invalid")
        return
    expected_fields = {"companyTitle", "nameAsFiled", "roles"}
    for index, owner in enumerate(owners):
        if not isinstance(owner, dict) or set(owner) != expected_fields:
            errors.append(f"filing {accession} owner {index} fields are invalid")
            continue
        try:
            _safe_public_name(
                owner.get("nameAsFiled"),
                f"filing {accession} owner {index} name",
                maximum=256,
            )
            _validate_public_company_title_value(
                owner.get("companyTitle"),
                label=f"filing {accession} owner {index} title",
            )
        except InsiderPublicationError as error:
            errors.append(str(error))
        roles = owner.get("roles")
        if (
            not isinstance(roles, list)
            or not roles
            or any(
                type(role) is not str or role not in _PUBLIC_OWNER_ROLE_ORDER
                for role in roles
            )
            or roles != sorted(set(roles), key=_PUBLIC_OWNER_ROLE_ORDER.__getitem__)
        ):
            errors.append(f"filing {accession} owner {index} roles are invalid")


def _validate_public_detail_row_semantics(
    accession: str,
    collection_name: str,
    row: Mapping[str, object],
    row_index: int,
    *,
    filing: object,
    source: object,
    errors: list[str],
) -> None:
    label = f"filing {accession} {collection_name}[{row_index}]"
    filing_metadata = filing if isinstance(filing, dict) else {}
    source_metadata = source if isinstance(source, dict) else {}
    if (
        row.get("accessionNumber") != accession
        or row.get("filingDate") != filing_metadata.get("filingDate")
        or row.get("acceptedAt") != filing_metadata.get("acceptedAt")
        or row.get("formType") != filing_metadata.get("formType")
    ):
        errors.append(f"{label} filing identity is invalid")
    try:
        _safe_iso_date(row.get("filingDate"), f"{label}.filingDate")
        _safe_utc_timestamp(
            row.get("acceptedAt"),
            f"{label}.acceptedAt",
            nullable=True,
        )
        for field in ("normalizedSecurityId", "underlyingNormalizedSecurityId"):
            value = row.get(field)
            if value is not None:
                _safe_atom(value, f"{label}.{field}", maximum=160)
    except InsiderPublicationError as error:
        errors.append(str(error))

    direct_indirect = row.get("directIndirectOwnership")
    if direct_indirect not in {None, "D", "I"}:
        errors.append(f"{label} direct/indirect ownership is invalid")

    if collection_name == "holdings":
        try:
            _safe_iso_date(row.get("asOfDate"), f"{label}.asOfDate")
            for field in ("exerciseDate", "expirationDate"):
                _safe_iso_date(row.get(field), f"{label}.{field}", nullable=True)
        except InsiderPublicationError as error:
            errors.append(str(error))
        return

    try:
        _safe_iso_date(row.get("transactionDate"), f"{label}.transactionDate")
        for field in ("deemedExecutionDate", "exerciseDate", "expirationDate"):
            _safe_iso_date(row.get(field), f"{label}.{field}", nullable=True)
    except InsiderPublicationError as error:
        errors.append(str(error))

    transaction_code = row.get("transactionCode")
    code_is_safe = transaction_code is None or (
        type(transaction_code) is str
        and re.fullmatch(r"[A-Z0-9]{1,8}", transaction_code) is not None
    )
    if not code_is_safe:
        errors.append(f"{label} transaction code is invalid")
    else:
        classification = classify_transaction_code(transaction_code)
        if any(
            row.get(field) != classification[expected]
            for field, expected in (
                ("transactionLabel", "label"),
                ("normalizedCategory", "normalized_category"),
                ("isMeaningfulPS", "is_meaningful_ps"),
            )
        ):
            errors.append(f"{label} transaction classification is invalid")

    if row.get("transactionFormType") not in {
        None,
        *_PUBLIC_TRANSACTION_FORM_TYPES,
    }:
        errors.append(f"{label} transaction form type is invalid")
    if row.get("transactionTimeliness") not in {None, "E", "L"}:
        errors.append(f"{label} transaction timeliness is invalid")
    if row.get("acquiredDisposedCode") not in {None, "A", "D"}:
        errors.append(f"{label} acquired/disposed code is invalid")
    if row.get("planStatus") not in _PUBLIC_PLAN_STATES:
        errors.append(f"{label} plan status is invalid")
    for field in (
        "equitySwapInvolved",
        "isAmended",
        "isMeaningfulPS",
        "priceIsWeightedAverage",
    ):
        if type(row.get(field)) is not bool:
            errors.append(f"{label} {field} must be boolean")
    if row.get("isAmended") != filing_metadata.get("isAmendment"):
        errors.append(f"{label} amendment state is invalid")
    current = filing_metadata.get("isCurrentEffectiveVersion")
    expected_superseded = None if current is None else not current
    if row.get("isSuperseded") != expected_superseded:
        errors.append(f"{label} superseded state is invalid")

    value_method = row.get("valueMethod")
    if value_method not in _PUBLIC_VALUE_METHODS:
        errors.append(f"{label} value method is invalid")
    else:
        expected_value = (
            row.get("reportedTotalValue")
            if value_method == "reported_total"
            else row.get("calculatedValue")
            if value_method == "calculated_shares_times_price"
            else None
        )
        if row.get("value") != expected_value:
            errors.append(f"{label} value method does not reconcile")
    if row.get("secDocumentUrl") != source_metadata.get("documentUrl"):
        errors.append(f"{label} SEC document source is invalid")


def _validate_public_detail_shape(
    accession: str,
    detail: Mapping[str, object],
    errors: list[str],
) -> None:
    expected = {
        "accessionNumber",
        "data_contract_version",
        "filing",
        "holdings",
        "insider_public_contract_version",
        "issuer",
        "ownerGroup",
        "owners",
        "payloadType",
        "publicationSafeguards",
        "source",
        "transactions",
    }
    if set(detail) != expected:
        errors.append(f"filing {accession} public fields are invalid")
    if detail.get("accessionNumber") != accession:
        errors.append(f"filing {accession} accession does not match its filename")
    if detail.get("payloadType") != "insider_filing_detail":
        errors.append(f"filing {accession} payload type is invalid")
    if (
        detail.get("data_contract_version") != DATA_CONTRACT_VERSION
        or detail.get("insider_public_contract_version")
        != INSIDER_PUBLIC_CONTRACT_VERSION
    ):
        errors.append(f"filing {accession} contract version is invalid")
    source = detail.get("source")
    if not isinstance(source, dict) or set(source) != {"documentUrl", "indexUrl"}:
        errors.append(f"filing {accession} source fields are invalid")
    else:
        for key in ("indexUrl", "documentUrl"):
            try:
                _safe_sec_url(source.get(key), f"filing {accession} source {key}")
            except InsiderPublicationError as error:
                errors.append(f"filing {accession} SEC source is invalid: {error}")

    issuer = detail.get("issuer")
    expected_issuer_fields = {
        "cik",
        "foreignTradingSymbolAsFiled",
        "nameAsFiled",
        "tradingSymbolAsFiled",
    }
    if not isinstance(issuer, dict) or set(issuer) != expected_issuer_fields:
        errors.append(f"filing {accession} issuer fields are invalid")
    else:
        if (
            type(issuer.get("cik")) is not str
            or re.fullmatch(r"[0-9]{10}", issuer["cik"]) is None
        ):
            errors.append(f"filing {accession} issuer CIK is invalid")
        try:
            _safe_public_name(
                issuer.get("nameAsFiled"),
                f"filing {accession} issuer nameAsFiled",
                maximum=256,
            )
            for key in ("tradingSymbolAsFiled", "foreignTradingSymbolAsFiled"):
                _safe_public_symbol(
                    issuer.get(key),
                    f"filing {accession} issuer {key}",
                    nullable=True,
                )
        except InsiderPublicationError as error:
            errors.append(str(error))

    filing = detail.get("filing")
    if not isinstance(filing, dict):
        errors.append(f"filing {accession} metadata is invalid")
    else:
        expected_filing_fields = {
            "acceptedAt",
            "aff10b5One",
            "baseFormType",
            "filingDate",
            "form3HoldingsReported",
            "form4TransactionsReported",
            "formType",
            "isAmendment",
            "isCurrentEffectiveVersion",
            "noSecuritiesOwned",
            "notSubjectToSection16",
            "originalSubmissionDate",
            "periodOfReport",
        }
        if set(filing) != expected_filing_fields:
            errors.append(f"filing {accession} metadata fields are invalid")
        form_type = filing.get("formType")
        base_form_type = filing.get("baseFormType")
        is_amendment = filing.get("isAmendment")
        if (
            form_type not in _PUBLIC_FORM_TYPES
            or base_form_type not in {"3", "4", "5"}
            or base_form_type != str(form_type).removesuffix("/A")
            or type(is_amendment) is not bool
            or is_amendment != str(form_type).endswith("/A")
        ):
            errors.append(f"filing {accession} form metadata is invalid")
        try:
            _safe_iso_date(filing.get("filingDate"), f"filing {accession} filingDate")
            _safe_iso_date(
                filing.get("originalSubmissionDate"),
                f"filing {accession} originalSubmissionDate",
                nullable=True,
            )
            _safe_iso_date(
                filing.get("periodOfReport"),
                f"filing {accession} periodOfReport",
                nullable=True,
            )
            _safe_utc_timestamp(
                filing.get("acceptedAt"),
                f"filing {accession} acceptedAt",
                nullable=True,
            )
        except InsiderPublicationError as error:
            errors.append(str(error))
        for key in (
            "aff10b5One",
            "form3HoldingsReported",
            "form4TransactionsReported",
            "noSecuritiesOwned",
            "notSubjectToSection16",
        ):
            if filing.get(key) is not None and type(filing.get(key)) is not bool:
                errors.append(f"filing {accession} {key} must be tri-state")
        if (
            filing.get("isCurrentEffectiveVersion") is not None
            and type(filing.get("isCurrentEffectiveVersion")) is not bool
        ):
            errors.append(
                f"filing {accession} isCurrentEffectiveVersion must be tri-state"
            )

        if isinstance(source, dict) and isinstance(issuer, dict):
            for key in ("indexUrl", "documentUrl"):
                try:
                    _safe_bound_sec_url(
                        source.get(key),
                        f"filing {accession} source {key}",
                        issuer_cik=issuer.get("cik"),
                        accession=accession,
                    )
                except InsiderPublicationError as error:
                    errors.append(f"filing {accession} SEC source is invalid: {error}")

    _validate_public_owners(accession, detail.get("owners"), errors)
    detail_owner_group = detail.get("ownerGroup")
    _validate_public_owner_group_shape(accession, detail_owner_group, errors)
    owners = detail.get("owners")
    if isinstance(owners, list) and isinstance(detail_owner_group, dict):
        if all(isinstance(owner, dict) for owner in owners):
            public_owners = [owner for owner in owners if isinstance(owner, dict)]
            roles = {
                role
                for owner in public_owners
                for role in owner.get("roles", [])
                if type(role) is str and role in _PUBLIC_OWNER_ROLE_ORDER
            }
            canonical_roles = sorted(roles, key=_PUBLIC_OWNER_ROLE_ORDER.__getitem__)
            officer_owners = [
                owner for owner in public_owners if "Officer" in owner.get("roles", [])
            ]
            primary_title = (
                officer_owners[0].get("companyTitle")
                if officer_owners
                else _fallback_company_title(canonical_roles)
            )
            expected_owner_group = {
                "displayName": " / ".join(
                    str(owner.get("nameAsFiled")) for owner in public_owners
                ),
                "isJoint": len(public_owners) > 1,
                "ownerCount": len(public_owners),
                "primaryTitle": primary_title,
                "roles": canonical_roles,
            }
            if detail_owner_group != expected_owner_group:
                errors.append(f"filing {accession} owner group does not reconcile")
        elif detail_owner_group.get("ownerCount") != len(owners):
            errors.append(f"filing {accession} owner group count is invalid")

    safeguards = detail.get("publicationSafeguards")
    expected_safeguards = {
        "filingNarrativesOmitted": True,
        "ownerCiksOmitted": True,
        "ownerDisplayLimitedToNameAndCompanyTitle": True,
        "parserDiagnosticsOmitted": True,
        "plainTextOnly": True,
        "rawSourceOmitted": True,
        "restrictedOwnerAddressesOmitted": True,
        "signaturesOmitted": True,
    }
    if safeguards != expected_safeguards:
        errors.append(f"filing {accession} publication safeguards are invalid")

    decimal_fields = {
        "calculatedValue",
        "conversionOrExercisePrice",
        "postTransactionShares",
        "postTransactionValue",
        "pricePerShare",
        "reportedTotalValue",
        "shares",
        "underlyingShares",
        "underlyingValue",
        "value",
        "valueOwned",
        "sharesOwned",
    }
    expected_collection_fields = {
        "transactions": {
            "acceptedAt",
            "accessionNumber",
            "acquiredDisposedCode",
            "calculatedValue",
            "conversionOrExercisePrice",
            "deemedExecutionDate",
            "directIndirectOwnership",
            "displayGroupOrdinal",
            "equitySwapInvolved",
            "exerciseDate",
            "expirationDate",
            "filingDate",
            "formType",
            "isAmended",
            "isMeaningfulPS",
            "isSuperseded",
            "normalizedCategory",
            "normalizedSecurityId",
            "ownerGroup",
            "planStatus",
            "postTransactionShares",
            "postTransactionValue",
            "priceIsWeightedAverage",
            "pricePerShare",
            "reportedTotalValue",
            "secDocumentUrl",
            "shares",
            "transactionCode",
            "transactionDate",
            "transactionFormType",
            "transactionLabel",
            "transactionTable",
            "transactionTimeliness",
            "underlyingNormalizedSecurityId",
            "underlyingShares",
            "underlyingValue",
            "value",
            "valueMethod",
        },
        "holdings": {
            "acceptedAt",
            "accessionNumber",
            "asOfDate",
            "conversionOrExercisePrice",
            "directIndirectOwnership",
            "exerciseDate",
            "expirationDate",
            "filingDate",
            "formType",
            "normalizedSecurityId",
            "ownerGroup",
            "sharesOwned",
            "underlyingNormalizedSecurityId",
            "underlyingShares",
            "underlyingValue",
            "valueOwned",
        },
    }
    display_group_ordinals: set[int] = set()
    for collection_name in ("transactions", "holdings"):
        collection = detail.get(collection_name)
        if not isinstance(collection, list):
            errors.append(f"filing {accession} {collection_name} must be a list")
            continue
        for row_index, row in enumerate(collection):
            if not isinstance(row, dict):
                errors.append(
                    f"filing {accession} {collection_name}[{row_index}] is invalid"
                )
                continue
            if set(row) != expected_collection_fields[collection_name]:
                errors.append(
                    f"filing {accession} {collection_name}[{row_index}] fields are invalid"
                )
                continue
            if (
                collection_name == "transactions"
                and row.get("isSuperseded") is not None
                and type(row.get("isSuperseded")) is not bool
            ):
                errors.append(
                    f"filing {accession} transactions[{row_index}] "
                    "isSuperseded must be tri-state"
                )
            _validate_public_owner_group_shape(
                accession,
                row.get("ownerGroup"),
                errors,
            )
            if row.get("ownerGroup") != detail_owner_group:
                errors.append(
                    f"filing {accession} {collection_name}[{row_index}] "
                    "owner group does not match filing"
                )
            if collection_name == "transactions":
                group_ordinal = row.get("displayGroupOrdinal")
                if (
                    type(group_ordinal) is not int
                    or type(group_ordinal) is bool
                    or not 1 <= group_ordinal <= len(collection)
                ):
                    errors.append(
                        f"filing {accession} transactions[{row_index}] "
                        "display group ordinal is invalid"
                    )
                else:
                    display_group_ordinals.add(group_ordinal)
                if row.get("transactionTable") not in {
                    "non_derivative",
                    "derivative",
                }:
                    errors.append(
                        f"filing {accession} transactions[{row_index}] "
                        "transaction table is invalid"
                    )
            _validate_public_detail_row_semantics(
                accession,
                collection_name,
                row,
                row_index,
                filing=filing,
                source=source,
                errors=errors,
            )
            for field in decimal_fields & set(row):
                value = row[field]
                if value is None:
                    continue
                if not _is_canonical_decimal_text(value):
                    errors.append(
                        f"filing {accession} {collection_name}[{row_index}].{field} "
                        "must be a canonical exact decimal string or null"
                    )
    if display_group_ordinals and display_group_ordinals != set(
        range(1, len(display_group_ordinals) + 1)
    ):
        errors.append(f"filing {accession} display group ordinals are invalid")


_PUBLIC_TRANSACTION_GROUP_FIELDS = frozenset(
    {
        "acceptedAt",
        "accessionNumber",
        "acquiredDisposedCode",
        "directIndirectOwnership",
        "filingDate",
        "formType",
        "isAmended",
        "isSuperseded",
        "normalizedCategory",
        "ownerGroup",
        "percentChange",
        "percentChangeState",
        "planStatus",
        "postTransactionShares",
        "priceIsWeightedAverage",
        "pricePerShare",
        "secDocumentUrl",
        "securityId",
        "shares",
        "transactionCode",
        "transactionDate",
        "transactionLabel",
        "transactionLegCount",
        "transactionTimeliness",
        "value",
        "valueCoverage",
        "valueMethod",
    }
)
_PUBLIC_CHART_EVENT_FIELDS = frozenset(
    {
        "accessionNumber",
        "category",
        "code",
        "filingDate",
        "formType",
        "marker",
        "ownerGroupDisplayName",
        "planStatus",
        "postTransactionShares",
        "pricePerShare",
        "roleLabel",
        "shares",
        "transactionDate",
        "value",
    }
)
_PUBLIC_SIDEBAR_FIELDS = frozenset(
    {"latestReportedHoldings", "rule10b51", "topBuyers", "topSellers", "window"}
)
_PUBLIC_RANKING_FIELDS = frozenset(
    {
        "displayName",
        "displayValue",
        "incompleteCount",
        "planMarkedKnownValuePercentage",
        "rank",
        "roleLabel",
        "transactionCount",
        "value",
    }
)
_PUBLIC_HOLDING_GROUP_FIELDS = frozenset(
    {"officersAndDirectors", "tenPercentOwnersAndEntities"}
)
_PUBLIC_HOLDING_ITEM_FIELDS = frozenset(
    {
        "asOfDate",
        "displayName",
        "ownershipPercentage",
        "roleLabel",
        "roles",
        "shares",
    }
)
_PUBLIC_RULE_10B51_FIELDS = frozenset(
    {
        "distinctOwnerGroupCount",
        "latestPlanAdoptionDate",
        "missingValueCount",
        "planMarkedSalesDisplayValue",
        "planMarkedSalesValue",
    }
)


def _public_decimal_text(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if value == 0 else rendered


def _public_compact_money(value: Decimal) -> str:
    negative = value < 0
    absolute = -value if negative else value
    divisor = Decimal(1)
    suffix = ""
    for threshold, candidate_divisor, candidate_suffix in (
        (Decimal("1000000000000"), Decimal("1000000000000"), "T"),
        (Decimal("1000000000"), Decimal("1000000000"), "B"),
        (Decimal("1000000"), Decimal("1000000"), "M"),
        (Decimal("1000"), Decimal("1000"), "K"),
    ):
        if absolute >= threshold:
            divisor = candidate_divisor
            suffix = candidate_suffix
            break
    with localcontext() as context:
        context.prec = 80
        scaled = absolute / divisor
        places = (
            Decimal("0.01")
            if scaled < 10
            else Decimal("0.1")
            if scaled < 100
            else Decimal("1")
        )
        rendered = _public_decimal_text(scaled.quantize(places))
    return f"{'-' if negative else ''}${rendered}{suffix}"


def _metric_row_from_public_detail(
    row: Mapping[str, object],
    *,
    row_index: int,
) -> dict[str, object]:
    display_ordinal = row["displayGroupOrdinal"]
    accession = row["accessionNumber"]
    assert isinstance(display_ordinal, int) and not isinstance(display_ordinal, bool)
    assert isinstance(accession, str)
    return {
        key: row[key]
        for key in (
            "acceptedAt",
            "accessionNumber",
            "acquiredDisposedCode",
            "deemedExecutionDate",
            "directIndirectOwnership",
            "filingDate",
            "formType",
            "isAmended",
            "isSuperseded",
            "normalizedCategory",
            "ownerGroup",
            "planStatus",
            "postTransactionShares",
            "priceIsWeightedAverage",
            "pricePerShare",
            "secDocumentUrl",
            "shares",
            "transactionCode",
            "transactionDate",
            "transactionLabel",
            "transactionTimeliness",
            "value",
            "valueMethod",
        )
    } | {
        "privateDisplayGroupKeyOverride": _private_public_display_group_key(
            accession,
            display_ordinal,
        ),
        "privateFootnoteIds": [f"display-group-{display_ordinal}"],
        "privateOwnerGroupKey": _private_public_reconciliation_owner_key(accession),
        "privateRowKey": f"{accession}:public-detail:{row_index}",
        "privateSourceRowIndex": row_index,
        "privateSourceTable": row["transactionTable"],
        "securityId": row["normalizedSecurityId"],
    }


def _metric_holding_from_public_detail(row: Mapping[str, object]) -> dict[str, object]:
    accession = row["accessionNumber"]
    return {
        "asOfDate": row["asOfDate"],
        "directIndirectOwnership": row["directIndirectOwnership"],
        "ownerGroup": row["ownerGroup"],
        "privateOwnerGroupKey": _private_public_reconciliation_owner_key(accession),
        "securityId": row["normalizedSecurityId"],
        "shares": row["sharesOwned"],
    }


def _identity_independent_metric_projection(
    value: Mapping[str, object],
) -> dict[str, object]:
    """Remove values that cannot be rederived without a private owner identity."""

    raw_summary = value.get("summary")
    raw_sidebar = value.get("sidebar")
    if not isinstance(raw_summary, Mapping) or not isinstance(raw_sidebar, Mapping):
        raise _fail("public identity-independent metric projection")

    normalized: dict[str, object] = dict(value)
    summary: dict[str, object] = dict(raw_summary)
    for key in ("purchases", "sales"):
        raw_code_summary = summary.get(key)
        if not isinstance(raw_code_summary, Mapping):
            raise _fail("public identity-independent metric projection")
        code_summary: dict[str, object] = dict(raw_code_summary)
        code_summary.pop("ownerGroupCount", None)
        summary[key] = code_summary
    normalized["summary"] = summary

    sidebar: dict[str, object] = dict(raw_sidebar)
    sidebar.pop("topBuyers", None)
    sidebar.pop("topSellers", None)
    sidebar.pop("latestReportedHoldings", None)
    raw_rule = sidebar.get("rule10b51")
    if not isinstance(raw_rule, Mapping):
        raise _fail("public identity-independent metric projection")
    rule: dict[str, object] = dict(raw_rule)
    rule.pop("distinctOwnerGroupCount", None)
    sidebar["rule10b51"] = rule
    normalized["sidebar"] = sidebar
    return normalized


def _validate_page_against_filing_details(
    page: Mapping[str, object],
    *,
    relative: str,
    stock_id: str,
    allowed_accessions: set[str],
    details: Mapping[str, Mapping[str, object]],
    expected_unmapped: int,
    expected_unresolved: int,
    errors: list[str],
) -> None:
    metric_rows: list[dict[str, object]] = []
    metric_holdings: list[dict[str, object]] = []
    try:
        for accession in sorted(allowed_accessions):
            detail = details[accession]
            filing = detail["filing"]
            if (
                not isinstance(filing, dict)
                or filing.get("isCurrentEffectiveVersion") is not True
            ):
                continue
            transactions = detail["transactions"]
            holdings = detail["holdings"]
            if not isinstance(transactions, list) or not isinstance(holdings, list):
                raise _fail("public filing detail collections")
            for row_index, row in enumerate(transactions):
                if (
                    not isinstance(row, dict)
                    or row.get("normalizedSecurityId") != stock_id
                ):
                    continue
                metric_rows.append(
                    _metric_row_from_public_detail(row, row_index=row_index)
                )
            for row in holdings:
                if (
                    not isinstance(row, dict)
                    or row.get("normalizedSecurityId") != stock_id
                ):
                    continue
                metric_holdings.append(_metric_holding_from_public_detail(row))

        freshness = page["dataFreshness"]
        quality = page["dataQuality"]
        if not isinstance(freshness, dict) or not isinstance(quality, dict):
            raise _fail("public freshness or quality")
        expected = build_static_insider_metric_projection(
            metric_rows,
            security_id=stock_id,
            as_of=page["asOf"],
            holdings=metric_holdings,
            quality={
                "freshnessMaxAgeSeconds": freshness["secFreshnessThresholdSeconds"],
                "latestSuccessfulSyncAt": quality["latestSuccessfulSyncAt"],
                "unmappedSecurityRowCount": expected_unmapped,
                "unresolvedAmendmentCount": expected_unresolved,
            },
        )
    except (
        InsiderMetricsError,
        InsiderPublicationError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        errors.append(
            f"{relative} cannot derive metrics from filing detail rows: {error}"
        )
        return

    try:
        page_projection = _identity_independent_metric_projection(page)
        expected_projection = _identity_independent_metric_projection(expected)
    except InsiderPublicationError as error:
        errors.append(f"{relative} cannot compare public metric projection: {error}")
        return
    if any(
        page_projection.get(key) != value for key, value in expected_projection.items()
    ):
        errors.append(f"{relative} does not reconcile to filing detail rows")


def _validate_public_ranking(
    value: object,
    *,
    code: str,
    label: str,
    canonical_items: list[dict[str, object]],
    errors: list[str],
) -> None:
    if not isinstance(value, list) or len(value) > 5:
        errors.append(f"{label} is invalid")
        return

    code_items = [
        item for item in canonical_items if item.get("transactionCode") == code
    ]
    if bool(value) != bool(code_items):
        errors.append(f"{label} presence does not match public transactions")
    public_owner_shapes = {
        (
            owner.get("displayName"),
            owner.get("primaryTitle"),
        )
        for item in code_items
        for owner in (item.get("ownerGroup"),)
        if isinstance(owner, dict)
    }
    total_transaction_count = 0
    total_incomplete_count = 0
    ordering: list[tuple[Decimal | None, str]] = []
    for index, item in enumerate(value):
        item_label = f"{label}[{index}]"
        if not isinstance(item, dict) or set(item) != _PUBLIC_RANKING_FIELDS:
            errors.append(f"{item_label} fields are invalid")
            continue
        display_name = item.get("displayName")
        role_label = item.get("roleLabel")
        try:
            canonical_name = _safe_public_name(
                display_name,
                f"{item_label} display name",
                maximum=512,
            )
            _validate_public_company_title_value(
                role_label,
                label=f"{item_label} role label",
            )
        except InsiderPublicationError as error:
            errors.append(str(error))
            canonical_name = ""
        if (display_name, role_label) not in public_owner_shapes:
            errors.append(f"{item_label} does not match a public transaction owner")

        rank = item.get("rank")
        transaction_count = item.get("transactionCount")
        incomplete_count = item.get("incompleteCount")
        if rank != index + 1 or type(rank) is not int or type(rank) is bool:
            errors.append(f"{item_label} rank is invalid")
        if (
            type(transaction_count) is not int
            or type(transaction_count) is bool
            or not 1 <= transaction_count <= len(code_items)
        ):
            errors.append(f"{item_label} transaction count is invalid")
        else:
            total_transaction_count += transaction_count
        if (
            type(incomplete_count) is not int
            or type(incomplete_count) is bool
            or type(transaction_count) is not int
            or type(transaction_count) is bool
            or not 0 <= incomplete_count <= transaction_count
        ):
            errors.append(f"{item_label} incomplete count is invalid")
        else:
            total_incomplete_count += incomplete_count

        raw_value = item.get("value")
        value_decimal: Decimal | None = None
        if raw_value is not None:
            if not _is_canonical_decimal_text(raw_value):
                errors.append(f"{item_label} value is invalid")
            else:
                assert isinstance(raw_value, str)
                value_decimal = Decimal(raw_value)
                if value_decimal < 0:
                    errors.append(f"{item_label} value is negative")
        expected_display = (
            "—" if value_decimal is None else _public_compact_money(value_decimal)
        )
        if item.get("displayValue") != expected_display:
            errors.append(f"{item_label} display value is invalid")

        percentage = item.get("planMarkedKnownValuePercentage")
        if code == "P" and percentage is not None:
            errors.append(f"{item_label} plan percentage is invalid")
        elif percentage is not None:
            if not _is_canonical_decimal_text(percentage):
                errors.append(f"{item_label} plan percentage is invalid")
            else:
                assert isinstance(percentage, str)
                percentage_decimal = Decimal(percentage)
                if not Decimal(0) <= percentage_decimal <= Decimal(1):
                    errors.append(f"{item_label} plan percentage is invalid")
        ordering.append((value_decimal, canonical_name.casefold()))

    if total_transaction_count > len(code_items):
        errors.append(f"{label} transaction counts exceed public transactions")
    available_incomplete = sum(
        item.get("valueCoverage") != "complete" for item in code_items
    )
    if total_incomplete_count > available_incomplete:
        errors.append(f"{label} incomplete counts exceed public transactions")
    for previous, current in zip(ordering, ordering[1:]):
        previous_value, previous_name = previous
        current_value, current_name = current
        if previous_value is None and current_value is not None:
            errors.append(f"{label} ordering is invalid")
            break
        if previous_value is not None and current_value is not None:
            if current_value > previous_value or (
                current_value == previous_value and current_name < previous_name
            ):
                errors.append(f"{label} ordering is invalid")
                break


def _validate_public_holding_sidebar(
    value: object,
    *,
    label: str,
    errors: list[str],
) -> None:
    if not isinstance(value, dict) or set(value) != _PUBLIC_HOLDING_GROUP_FIELDS:
        errors.append(f"{label} fields are invalid")
        return
    category_roles = {
        "officersAndDirectors": {"Officer", "Director"},
        "tenPercentOwnersAndEntities": {"TenPercentOwner"},
    }
    for category, required_roles in category_roles.items():
        rows = value.get(category)
        category_label = f"{label}.{category}"
        if not isinstance(rows, list) or len(rows) > 5:
            errors.append(f"{category_label} is invalid")
            continue
        ordering: list[tuple[Decimal | None, str]] = []
        for index, item in enumerate(rows):
            item_label = f"{category_label}[{index}]"
            if not isinstance(item, dict) or set(item) != _PUBLIC_HOLDING_ITEM_FIELDS:
                errors.append(f"{item_label} fields are invalid")
                continue
            try:
                canonical_name = _safe_public_name(
                    item.get("displayName"),
                    f"{item_label} display name",
                    maximum=512,
                )
                _validate_public_company_title_value(
                    item.get("roleLabel"),
                    label=f"{item_label} role label",
                )
                _safe_iso_date(item.get("asOfDate"), f"{item_label} as-of date")
            except InsiderPublicationError as error:
                errors.append(str(error))
                canonical_name = ""
            roles = item.get("roles")
            if (
                not isinstance(roles, list)
                or not roles
                or any(
                    type(role) is not str or role not in _PUBLIC_OWNER_ROLE_ORDER
                    for role in roles
                )
                or roles != sorted(set(roles), key=_PUBLIC_OWNER_ROLE_ORDER.__getitem__)
                or not required_roles.intersection(roles)
            ):
                errors.append(f"{item_label} roles are invalid")
            if item.get("ownershipPercentage") is not None:
                errors.append(f"{item_label} ownership percentage is unavailable")
            raw_shares = item.get("shares")
            shares: Decimal | None = None
            if raw_shares is not None:
                if not _is_canonical_decimal_text(raw_shares):
                    errors.append(f"{item_label} shares are invalid")
                else:
                    assert isinstance(raw_shares, str)
                    shares = Decimal(raw_shares)
                    if shares < 0:
                        errors.append(f"{item_label} shares are negative")
            ordering.append((shares, canonical_name.casefold()))
        for previous, current in zip(ordering, ordering[1:]):
            previous_shares, previous_name = previous
            current_shares, current_name = current
            if previous_shares is None and current_shares is not None:
                errors.append(f"{category_label} ordering is invalid")
                break
            if previous_shares is not None and current_shares is not None:
                if current_shares > previous_shares or (
                    current_shares == previous_shares and current_name < previous_name
                ):
                    errors.append(f"{category_label} ordering is invalid")
                    break


def _validate_public_rule_10b51(
    value: object,
    *,
    label: str,
    canonical_items: list[dict[str, object]],
    errors: list[str],
) -> None:
    if not isinstance(value, dict) or set(value) != _PUBLIC_RULE_10B51_FIELDS:
        errors.append(f"{label} fields are invalid")
        return
    marked_sales = [
        item
        for item in canonical_items
        if item.get("transactionCode") == "S"
        and item.get("planStatus") == "filing_marked"
    ]
    known_values = [
        Decimal(str(item["value"]))
        for item in marked_sales
        if isinstance(item.get("value"), str)
        and _is_canonical_decimal_text(item.get("value"))
    ]
    marked_value = sum(known_values, Decimal(0))
    expected_display = _public_compact_money(marked_value) if known_values else "—"
    expected_missing = sum(
        item.get("valueCoverage") != "complete" for item in marked_sales
    )
    if (
        value.get("planMarkedSalesValue") != _public_decimal_text(marked_value)
        or value.get("planMarkedSalesDisplayValue") != expected_display
        or value.get("missingValueCount") != expected_missing
        or value.get("latestPlanAdoptionDate") is not None
    ):
        errors.append(f"{label} does not reconcile to public transactions")
    distinct_count = value.get("distinctOwnerGroupCount")
    minimum = 1 if marked_sales else 0
    if (
        type(distinct_count) is not int
        or type(distinct_count) is bool
        or not minimum <= distinct_count <= len(marked_sales)
    ):
        errors.append(f"{label} distinct owner count is invalid")


def _validate_public_identity_derived_sidebar(
    page: Mapping[str, object],
    *,
    relative: str,
    canonical_items: list[dict[str, object]],
    errors: list[str],
) -> None:
    sidebar = page.get("sidebar")
    if not isinstance(sidebar, dict) or set(sidebar) != _PUBLIC_SIDEBAR_FIELDS:
        errors.append(f"{relative} sidebar fields are invalid")
        return
    summary = page.get("summary")
    summary_window = summary.get("window") if isinstance(summary, dict) else None
    if (
        sidebar.get("window") not in {"12m", "filtered"}
        or sidebar.get("window") != summary_window
    ):
        errors.append(f"{relative} sidebar window is invalid")
    _validate_public_ranking(
        sidebar.get("topBuyers"),
        code="P",
        label=f"{relative} sidebar.topBuyers",
        canonical_items=canonical_items,
        errors=errors,
    )
    _validate_public_ranking(
        sidebar.get("topSellers"),
        code="S",
        label=f"{relative} sidebar.topSellers",
        canonical_items=canonical_items,
        errors=errors,
    )
    _validate_public_holding_sidebar(
        sidebar.get("latestReportedHoldings"),
        label=f"{relative} sidebar.latestReportedHoldings",
        errors=errors,
    )
    _validate_public_rule_10b51(
        sidebar.get("rule10b51"),
        label=f"{relative} sidebar.rule10b51",
        canonical_items=canonical_items,
        errors=errors,
    )


def _validate_public_metric_self_consistency(
    page: Mapping[str, object],
    *,
    relative: str,
    expected_unmapped: int,
    expected_unresolved: int,
    errors: list[str],
) -> None:
    """Validate public-only invariants without recreating private identities."""

    security = page.get("security")
    stock_id = security.get("id") if isinstance(security, dict) else None
    transactions = page.get("transactions")
    static_pagination = page.get("staticPagination")
    if not isinstance(transactions, dict) or set(transactions) != {
        "items",
        "nextCursor",
        "total",
        "totalApproximate",
    }:
        errors.append(f"{relative} transactions fields are invalid")
        return
    items = transactions.get("items")
    if not isinstance(items, list) or len(items) > MAX_PUBLIC_TRANSACTION_ROWS:
        errors.append(f"{relative} transactions items are invalid")
        return
    if (
        transactions.get("nextCursor") is not None
        or transactions.get("total") != len(items)
        or transactions.get("totalApproximate") != len(items)
        or static_pagination
        != {"itemCount": len(items), "mode": "client", "pageSize": 100}
    ):
        errors.append(f"{relative} static pagination does not reconcile")

    canonical_items: list[dict[str, object]] = []
    decimal_fields = {
        "percentChange",
        "postTransactionShares",
        "pricePerShare",
        "shares",
        "value",
    }
    for index, item in enumerate(items):
        label = f"{relative} transactions.items[{index}]"
        if not isinstance(item, dict) or set(item) != _PUBLIC_TRANSACTION_GROUP_FIELDS:
            errors.append(f"{label} fields are invalid")
            continue
        if item.get("securityId") != stock_id:
            errors.append(f"{label} security identity is invalid")
        accession = item.get("accessionNumber")
        if type(accession) is not str or _ACCESSION_RE.fullmatch(accession) is None:
            errors.append(f"{label} accession is invalid")
        leg_count = item.get("transactionLegCount")
        if type(leg_count) is not int or type(leg_count) is bool or leg_count < 1:
            errors.append(f"{label} transaction leg count is invalid")
        if item.get("valueCoverage") not in {"complete", "partial", "unavailable"}:
            errors.append(f"{label} value coverage is invalid")
        if type(item.get("priceIsWeightedAverage")) is not bool:
            errors.append(f"{label} weighted-average flag is invalid")
        if item.get("isSuperseded") is not False:
            errors.append(f"{label} superseded state is invalid")
        _validate_public_owner_group_shape(relative, item.get("ownerGroup"), errors)
        try:
            _safe_sec_url(item.get("secDocumentUrl"), f"{label} SEC URL")
        except InsiderPublicationError as error:
            errors.append(str(error))
        for field in decimal_fields:
            value = item.get(field)
            if value is not None and not _is_canonical_decimal_text(value):
                errors.append(f"{label}.{field} is not a canonical decimal")
        canonical_items.append(item)

    summary = page.get("summary")
    if not isinstance(summary, dict) or set(summary) != {
        "latestMeaningfulTransaction",
        "netPS",
        "purchases",
        "sales",
        "window",
    }:
        errors.append(f"{relative} summary fields are invalid")
    else:
        expected_values: dict[str, Decimal] = {}
        for code, key in (("P", "purchases"), ("S", "sales")):
            selected = [
                item for item in canonical_items if item.get("transactionCode") == code
            ]
            known_values = [
                Decimal(str(item["value"]))
                for item in selected
                if isinstance(item.get("value"), str)
            ]
            missing = sum(item.get("valueCoverage") != "complete" for item in selected)
            expected_values[key] = sum(known_values, Decimal(0))
            code_summary = summary.get(key)
            if not isinstance(code_summary, dict):
                errors.append(f"{relative} does not reconcile summary {key}")
                continue
            expected_common = {
                "value": _public_decimal_text(expected_values[key]),
                "transactionCount": len(selected),
                "knownValueCount": len(selected) - missing,
                "missingValueCount": missing,
            }
            if any(
                code_summary.get(field) != value
                for field, value in expected_common.items()
            ):
                errors.append(f"{relative} does not reconcile summary {key}")
            owner_count = code_summary.get("ownerGroupCount")
            minimum_owner_count = 1 if selected else 0
            if (
                type(owner_count) is not int
                or type(owner_count) is bool
                or not minimum_owner_count <= owner_count <= len(selected)
            ):
                errors.append(f"{relative} summary {key} owner count is invalid")

        net = expected_values.get("purchases", Decimal(0)) - expected_values.get(
            "sales",
            Decimal(0),
        )
        net_summary = summary.get("netPS")
        if not isinstance(net_summary, dict) or net_summary.get(
            "value"
        ) != _public_decimal_text(net):
            errors.append(f"{relative} does not reconcile summary netPS")
        latest = summary.get("latestMeaningfulTransaction")
        if isinstance(latest, dict):
            _validate_public_owner_group_shape(
                f"{relative} summary latest transaction",
                latest.get("ownerGroup"),
                errors,
            )
        meaningful = [
            item
            for item in canonical_items
            if item.get("transactionCode") in {"P", "S"}
        ]
        if not meaningful:
            if latest is not None:
                errors.append(f"{relative} does not reconcile latest transaction")
        elif latest not in meaningful or not isinstance(latest, dict):
            errors.append(f"{relative} does not reconcile latest transaction")
        else:
            latest_key = (
                latest.get("transactionDate"),
                latest.get("acceptedAt") or "",
                latest.get("accessionNumber"),
            )
            if latest_key != max(
                (
                    item.get("transactionDate"),
                    item.get("acceptedAt") or "",
                    item.get("accessionNumber"),
                )
                for item in meaningful
            ):
                errors.append(f"{relative} does not reconcile latest transaction")

    chart_events = page.get("chartEvents")
    if not isinstance(chart_events, list) or len(chart_events) != len(canonical_items):
        errors.append(f"{relative} chart events do not reconcile transactions")
    else:
        for index, (event, item) in enumerate(
            zip(chart_events, canonical_items, strict=True)
        ):
            if not isinstance(event, dict) or set(event) != _PUBLIC_CHART_EVENT_FIELDS:
                errors.append(f"{relative} chartEvents[{index}] fields are invalid")
                continue
            try:
                _safe_public_name(
                    event.get("ownerGroupDisplayName"),
                    f"{relative} chartEvents[{index}] owner name",
                    maximum=512,
                )
            except InsiderPublicationError as error:
                errors.append(str(error))
            expected_event_values = {
                "accessionNumber": item["accessionNumber"],
                "category": item["normalizedCategory"],
                "code": item["transactionCode"],
                "filingDate": item["filingDate"],
                "formType": item["formType"],
                "ownerGroupDisplayName": item["ownerGroup"]["displayName"],
                "planStatus": item["planStatus"],
                "postTransactionShares": item["postTransactionShares"],
                "pricePerShare": item["pricePerShare"],
                "roleLabel": item["ownerGroup"]["primaryTitle"],
                "shares": item["shares"],
                "transactionDate": item["transactionDate"],
                "value": item["value"],
            }
            if any(
                event.get(key) != value for key, value in expected_event_values.items()
            ):
                errors.append(f"{relative} chartEvents[{index}] does not reconcile")

    _validate_public_identity_derived_sidebar(
        page,
        relative=relative,
        canonical_items=canonical_items,
        errors=errors,
    )

    quality = page.get("dataQuality")
    if not isinstance(quality, dict):
        errors.append(f"{relative} data quality is invalid")
    elif (
        quality.get("unmappedSecurityRowCount") != expected_unmapped
        or quality.get("unresolvedAmendmentCount") != expected_unresolved
    ):
        errors.append(f"{relative} does not reconcile dataQuality")


def _validate_insider_public_snapshot_dir(public_root: Path) -> list[str]:
    """Audit one immutable insider snapshot and recompute every page metric."""

    errors: list[str] = []
    root = Path(public_root)
    if root.is_symlink() or not root.is_dir():
        return [f"insider public root is missing or a symlink: {root}"]
    manifest_path = root / "manifest.json"
    manifest = _load_public_payload(
        manifest_path,
        maximum=MAX_PUBLIC_SECURITY_PAYLOAD_BYTES,
        errors=errors,
    )
    if manifest is None:
        return errors
    expected_manifest_fields = {
        "asOf",
        "data_contract_version",
        "insider_public_contract_version",
        "issuerCiks",
        "payloadType",
        "securityPayloads",
    }
    if set(manifest) != expected_manifest_fields:
        errors.append("insider manifest fields are invalid")
    if (
        manifest.get("payloadType") != "insider_publication_manifest"
        or manifest.get("data_contract_version") != DATA_CONTRACT_VERSION
        or manifest.get("insider_public_contract_version")
        != INSIDER_PUBLIC_CONTRACT_VERSION
    ):
        errors.append("insider manifest contract version is invalid")
    manifest_issuer_ciks: set[str] = set()
    issuer_cik_values = manifest.get("issuerCiks")
    issuer_cik_values_are_safe = (
        isinstance(issuer_cik_values, list)
        and bool(issuer_cik_values)
        and len(issuer_cik_values) <= MAX_PUBLIC_ISSUERS
        and all(
            type(value) is str and re.fullmatch(r"[0-9]{10}", value) is not None
            for value in issuer_cik_values
        )
    )
    canonical_issuer_cik_values = (
        sorted(set(issuer_cik_values))
        if issuer_cik_values_are_safe and isinstance(issuer_cik_values, list)
        else []
    )
    if (
        not issuer_cik_values_are_safe
        or issuer_cik_values != canonical_issuer_cik_values
    ):
        errors.append("manifest issuer CIKs are invalid")
    else:
        manifest_issuer_ciks = set(canonical_issuer_cik_values)
    manifest_as_of: str | None = None
    try:
        manifest_as_of = _safe_utc_timestamp(manifest.get("asOf"), "manifest asOf")
    except InsiderPublicationError as error:
        errors.append(str(error))

    security_entries = _manifest_entries(
        manifest,
        "securityPayloads",
        path_pattern=re.compile(r"securities/[A-Z0-9._-]+\.json"),
        errors=errors,
    )
    pages: dict[str, dict[str, object]] = {}
    filing_entries_by_path: dict[str, dict[str, object]] = {}
    page_filing_accessions: dict[str, set[str]] = {}
    for entry in security_entries:
        relative = entry["path"]
        assert isinstance(relative, str)
        page = _load_public_payload(
            root / relative,
            maximum=MAX_PUBLIC_SECURITY_PAYLOAD_BYTES,
            errors=errors,
        )
        if page is None:
            continue
        pages[relative] = page
        refs = _filing_references(page, relative, errors)
        page_filing_accessions[relative] = {str(ref["accessionNumber"]) for ref in refs}
        for ref in refs:
            path = ref["path"]
            assert isinstance(path, str)
            prior = filing_entries_by_path.setdefault(path, ref)
            if prior != ref:
                errors.append(
                    f"{relative} filing reference conflicts with another security"
                )

    filing_entries = [
        filing_entries_by_path[path] for path in sorted(filing_entries_by_path)
    ]
    expected_files = {
        "manifest.json",
        *(str(entry["path"]) for entry in security_entries),
        *(str(entry["path"]) for entry in filing_entries),
    }
    actual_files: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            errors.append(
                f"insider public tree contains a symlink: "
                f"{path.relative_to(root).as_posix()}"
            )
            actual_files.add(path.relative_to(root).as_posix())
        elif path.is_file():
            actual_files.add(path.relative_to(root).as_posix())
        elif not path.is_dir():
            errors.append(
                f"insider public tree contains an unsupported entry: "
                f"{path.relative_to(root).as_posix()}"
            )
    for unexpected in sorted(actual_files - expected_files):
        errors.append(f"insider public tree has unexpected file: {unexpected}")
    for missing in sorted(expected_files - actual_files):
        errors.append(f"insider public tree is missing file: {missing}")
    for entry in (*security_entries, *filing_entries):
        _validate_manifest_file(root, entry, errors)

    details: dict[str, dict[str, object]] = {}
    for entry in filing_entries:
        relative = entry["path"]
        assert isinstance(relative, str)
        accession = Path(relative).stem
        detail = _load_public_payload(
            root / relative,
            maximum=MAX_PUBLIC_FILING_DETAIL_BYTES,
            errors=errors,
        )
        if detail is None:
            continue
        _validate_public_detail_shape(accession, detail, errors)
        detail_issuer = detail.get("issuer")
        if (
            not isinstance(detail_issuer, dict)
            or detail_issuer.get("cik") not in manifest_issuer_ciks
        ):
            errors.append(f"filing {accession} issuer is outside manifest scope")
        details[accession] = detail

    known_stock_ids: set[str] = set()
    for relative, page in pages.items():
        expected_page_fields = {
            "asOf",
            "chartEvents",
            "dataFreshness",
            "dataQuality",
            "data_contract_version",
            "filters",
            "filingRefs",
            "insider_public_contract_version",
            "methodologyBanner",
            "payloadType",
            "priceSeries",
            "security",
            "sidebar",
            "staticPagination",
            "summary",
            "transactions",
        }
        if set(page) != expected_page_fields:
            errors.append(f"{relative} public page fields are invalid")
        if (
            page.get("payloadType") != "security_insider_activity"
            or page.get("data_contract_version") != DATA_CONTRACT_VERSION
            or page.get("insider_public_contract_version")
            != INSIDER_PUBLIC_CONTRACT_VERSION
        ):
            errors.append(f"{relative} contract version is invalid")
        try:
            page_as_of = _safe_utc_timestamp(page.get("asOf"), f"{relative} asOf")
            if page_as_of != manifest_as_of:
                errors.append(f"{relative} asOf does not match the manifest")
        except InsiderPublicationError as error:
            errors.append(str(error))
        methodology = page.get("methodologyBanner")
        expected_methodology = {
            "actionLabel": "Learn more",
            "text": _PUBLIC_METHODOLOGY_TEXT,
            "tone": "informational",
        }
        if methodology != expected_methodology:
            errors.append(f"{relative} methodology banner is invalid")
        security = page.get("security")
        if not isinstance(security, dict) or set(security) != _PUBLIC_SECURITY_FIELDS:
            errors.append(f"{relative} security identity fields are invalid")
            continue
        try:
            canonical_security = _validate_security_metadata(
                {
                    "companyName": security.get("companyName"),
                    "cusip": security.get("cusip"),
                    "fileStem": security.get("fileStem"),
                    "primary": security.get("primary"),
                    "securityType": security.get("securityType"),
                    "securityTypeLabel": security.get("securityTypeLabel"),
                    "stockId": security.get("id"),
                    "ticker": security.get("ticker"),
                }
            )
        except InsiderPublicationError as error:
            errors.append(f"{relative} security identity is invalid: {error}")
            continue
        if any(security.get(key) != value for key, value in canonical_security.items()):
            errors.append(f"{relative} security identity is not canonical")
            continue
        issuer_cik = security.get("issuerCik")
        if (
            type(issuer_cik) is not str
            or re.fullmatch(r"[0-9]{10}", issuer_cik) is None
            or issuer_cik not in manifest_issuer_ciks
        ):
            errors.append(f"{relative} issuer is outside manifest scope")
        stock_id = canonical_security["id"]
        assert isinstance(stock_id, str)
        if stock_id in known_stock_ids:
            errors.append(f"duplicate insider public security ID: {stock_id}")
        known_stock_ids.add(stock_id)
        if stock_file_stem(stock_id) != Path(relative).stem:
            errors.append(f"{relative} security filename does not match its identity")

    unmapped_count_by_issuer: dict[str, int] = {}
    unresolved_accessions_by_issuer: dict[str, set[str]] = {}
    for accession, detail in details.items():
        detail_issuer = detail.get("issuer")
        detail_issuer_cik = (
            detail_issuer.get("cik") if isinstance(detail_issuer, dict) else None
        )
        if type(detail_issuer_cik) is not str:
            continue
        filing = detail.get("filing")
        for collection_name in ("transactions", "holdings"):
            collection = detail.get(collection_name)
            if not isinstance(collection, list):
                continue
            for row in collection:
                if not isinstance(row, dict):
                    continue
                for field in (
                    "normalizedSecurityId",
                    "underlyingNormalizedSecurityId",
                ):
                    referenced_stock_id = row.get(field)
                    if (
                        referenced_stock_id is not None
                        and referenced_stock_id not in known_stock_ids
                    ):
                        errors.append(
                            f"filing {accession} {collection_name} references "
                            f"unpublished security {referenced_stock_id}"
                        )
        current = (
            isinstance(filing, dict) and filing.get("isCurrentEffectiveVersion") is True
        )
        if (
            isinstance(filing, dict)
            and filing.get("isAmendment") is True
            and filing.get("isCurrentEffectiveVersion") is None
        ):
            unresolved_accessions_by_issuer.setdefault(
                detail_issuer_cik,
                set(),
            ).add(accession)
        if not current:
            continue
        for collection_name in ("transactions", "holdings"):
            collection = detail.get(collection_name)
            if not isinstance(collection, list):
                continue
            for row in collection:
                if not isinstance(row, dict):
                    continue
                stock_id = row.get("normalizedSecurityId")
                if stock_id is None:
                    unmapped_count_by_issuer[detail_issuer_cik] = (
                        unmapped_count_by_issuer.get(detail_issuer_cik, 0) + 1
                    )

    for relative, page in pages.items():
        security = page.get("security")
        if not isinstance(security, dict) or type(security.get("id")) is not str:
            continue
        stock_id = security["id"]
        issuer_cik = security.get("issuerCik")
        if type(issuer_cik) is not str:
            continue
        allowed_accessions = page_filing_accessions.get(relative, set())
        for accession in allowed_accessions:
            detail = details.get(accession)
            detail_issuer = detail.get("issuer") if isinstance(detail, dict) else None
            if (
                not isinstance(detail_issuer, dict)
                or detail_issuer.get("cik") != issuer_cik
            ):
                errors.append(f"{relative} filing reference belongs to another issuer")
            if isinstance(detail, dict):
                scoped_rows: list[dict[str, object]] = []
                for collection_name in ("transactions", "holdings"):
                    collection = detail.get(collection_name)
                    if isinstance(collection, list):
                        scoped_rows.extend(
                            row for row in collection if isinstance(row, dict)
                        )
                if not any(
                    row.get("normalizedSecurityId") == stock_id for row in scoped_rows
                ):
                    errors.append(
                        f"{relative} filing reference is outside its security scope"
                    )
        filters = page.get("filters")
        freshness = page.get("dataFreshness")
        if not isinstance(filters, dict) or not isinstance(freshness, dict):
            errors.append(f"{relative} filters or freshness are invalid")
            continue
        expected_unmapped = unmapped_count_by_issuer.get(issuer_cik, 0)
        expected_unresolved = len(
            unresolved_accessions_by_issuer.get(issuer_cik, set())
        )
        _validate_public_metric_self_consistency(
            page,
            relative=relative,
            expected_unmapped=expected_unmapped,
            expected_unresolved=expected_unresolved,
            errors=errors,
        )
        _validate_page_against_filing_details(
            page,
            relative=relative,
            stock_id=stock_id,
            allowed_accessions=allowed_accessions,
            details=details,
            expected_unmapped=expected_unmapped,
            expected_unresolved=expected_unresolved,
            errors=errors,
        )
        transactions = page.get("transactions")
        allowed_accessions = page_filing_accessions.get(relative, set())
        if isinstance(transactions, dict):
            for item in transactions.get("items", []):
                if isinstance(item, dict):
                    accession = item.get("accessionNumber")
                    if accession not in details:
                        errors.append(
                            f"{relative} transaction references missing filing detail"
                        )
                    if accession not in allowed_accessions:
                        errors.append(
                            f"{relative} transaction is outside its filing references"
                        )

    return errors


def _publication_tree_metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _publication_tree_entry_seal(
    metadata: os.stat_result,
    *,
    kind: str,
    sha256: str | None,
) -> _PublicationTreeEntrySeal:
    return _PublicationTreeEntrySeal(
        kind=kind,
        st_dev=metadata.st_dev,
        st_ino=metadata.st_ino,
        st_mode=metadata.st_mode,
        st_size=metadata.st_size,
        st_mtime_ns=metadata.st_mtime_ns,
        st_ctime_ns=metadata.st_ctime_ns,
        sha256=sha256,
    )


def _read_regular_at(
    directory_fd: int,
    name: str,
    *,
    relative: str,
    label: str,
    maximum: int,
    errors: list[str],
    seal: dict[str, _PublicationTreeEntrySeal] | None = None,
    allow_empty: bool = False,
) -> bytes | None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        file_fd = os.open(name, flags, dir_fd=directory_fd)
    except OSError as error:
        errors.append(f"{label} cannot be opened safely: {error}")
        return None
    try:
        before = os.fstat(file_fd)
        if not stat.S_ISREG(before.st_mode):
            errors.append(f"{label} must be a regular file")
            return None
        if before.st_size < (0 if allow_empty else 1) or before.st_size > maximum:
            errors.append(f"{label} exceeds its bounded public size")
            return None
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining > 0:
            chunk = os.read(file_fd, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        encoded = b"".join(chunks)
        after = os.fstat(file_fd)
        before_identity = _publication_tree_metadata_identity(before)
        after_identity = _publication_tree_metadata_identity(after)
        if (
            len(encoded) > maximum
            or len(encoded) != before.st_size
            or before_identity != after_identity
        ):
            errors.append(f"{label} changed while it was being snapshotted")
            return None
        if seal is not None:
            if relative in seal:
                errors.append(f"{label} has a duplicate publication seal entry")
                return None
            seal[relative] = _publication_tree_entry_seal(
                after,
                kind="file",
                sha256=hashlib.sha256(encoded).hexdigest(),
            )
        return encoded
    except OSError as error:
        errors.append(f"{label} cannot be read safely: {error}")
        return None
    finally:
        os.close(file_fd)


def _snapshot_payload_directory(
    root_fd: int,
    directory_name: str,
    *,
    filename_pattern: re.Pattern[str],
    file_limit: int,
    per_file_limit: int,
    byte_budget: list[int],
    errors: list[str],
    seal: dict[str, _PublicationTreeEntrySeal] | None = None,
) -> dict[str, bytes]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        directory_fd = os.open(directory_name, flags, dir_fd=root_fd)
    except OSError as error:
        errors.append(f"{directory_name} cannot be opened safely: {error}")
        return {}
    payloads: dict[str, bytes] = {}
    entry_count = 0
    before: os.stat_result | None = None
    try:
        before = os.fstat(directory_fd)
        if not stat.S_ISDIR(before.st_mode):
            errors.append(f"{directory_name} must be a regular directory")
            return {}
        with os.scandir(directory_fd) as entries:
            for entry in entries:
                entry_count += 1
                if entry_count > file_limit:
                    errors.append(f"{directory_name} exceeds its file-count limit")
                    break
                name = entry.name
                relative = f"{directory_name}/{name}"
                if (
                    filename_pattern.fullmatch(name) is None
                    or entry.is_symlink()
                    or not entry.is_file(follow_symlinks=False)
                ):
                    errors.append(f"{relative} is not an allowed regular payload")
                    continue
                maximum = min(per_file_limit, byte_budget[0])
                if maximum < 1:
                    errors.append("insider public tree exceeds its total byte limit")
                    break
                encoded = _read_regular_at(
                    directory_fd,
                    name,
                    relative=relative,
                    label=relative,
                    maximum=maximum,
                    errors=errors,
                    seal=seal,
                )
                if encoded is None:
                    continue
                byte_budget[0] -= len(encoded)
                payloads[relative] = encoded
    except OSError as error:
        errors.append(f"{directory_name} cannot be scanned safely: {error}")
    finally:
        try:
            after = os.fstat(directory_fd)
            if before is None or _publication_tree_metadata_identity(
                before
            ) != _publication_tree_metadata_identity(after):
                errors.append(
                    f"{directory_name} changed while it was being snapshotted"
                )
            elif seal is not None:
                if directory_name in seal:
                    errors.append(
                        f"{directory_name} has a duplicate publication seal entry"
                    )
                else:
                    seal[directory_name] = _publication_tree_entry_seal(
                        after,
                        kind="directory",
                        sha256=None,
                    )
        except OSError as error:
            errors.append(f"{directory_name} cannot be sealed safely: {error}")
        os.close(directory_fd)
    if not payloads:
        errors.append(f"{directory_name} contains no public payloads")
    return payloads


def _snapshot_insider_public_tree_fd(
    root_fd: int,
    *,
    seal: dict[str, _PublicationTreeEntrySeal] | None = None,
) -> tuple[dict[str, bytes], list[str]]:
    errors: list[str] = []
    snapshot: dict[str, bytes] = {}
    allowed_root_entries = {"filings", "manifest.json", "securities"}
    observed_root_entries: set[str] = set()
    root_before: os.stat_result | None = None
    try:
        root_before = os.fstat(root_fd)
        if not stat.S_ISDIR(root_before.st_mode):
            return {}, ["insider public root must be a regular directory"]
        with os.scandir(root_fd) as entries:
            for entry in entries:
                observed_root_entries.add(entry.name)
                if len(observed_root_entries) > len(allowed_root_entries):
                    errors.append("insider public root contains unexpected entries")
                    break
                if entry.name not in allowed_root_entries or entry.is_symlink():
                    errors.append(
                        f"insider public root contains unsupported entry: {entry.name}"
                    )
                elif entry.name == "manifest.json":
                    if not entry.is_file(follow_symlinks=False):
                        errors.append("manifest.json must be a regular file")
                elif not entry.is_dir(follow_symlinks=False):
                    errors.append(f"{entry.name} must be a regular directory")
    except OSError as error:
        errors.append(f"insider public root cannot be scanned safely: {error}")
    for missing in sorted(allowed_root_entries - observed_root_entries):
        errors.append(f"insider public root is missing: {missing}")

    byte_budget = [MAX_PUBLIC_TOTAL_BYTES]
    manifest = _read_regular_at(
        root_fd,
        "manifest.json",
        relative="manifest.json",
        label="manifest.json",
        maximum=min(MAX_PUBLIC_SECURITY_PAYLOAD_BYTES, byte_budget[0]),
        errors=errors,
        seal=seal,
    )
    if manifest is not None:
        byte_budget[0] -= len(manifest)
        snapshot["manifest.json"] = manifest
    snapshot.update(
        _snapshot_payload_directory(
            root_fd,
            "securities",
            filename_pattern=re.compile(rf"{_SECURITY_STEM_RE.pattern}\.json"),
            file_limit=MAX_PUBLIC_SECURITY_FILES,
            per_file_limit=MAX_PUBLIC_SECURITY_PAYLOAD_BYTES,
            byte_budget=byte_budget,
            errors=errors,
            seal=seal,
        )
    )
    snapshot.update(
        _snapshot_payload_directory(
            root_fd,
            "filings",
            filename_pattern=re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}\.json"),
            file_limit=MAX_PUBLIC_FILING_FILES,
            per_file_limit=MAX_PUBLIC_FILING_DETAIL_BYTES,
            byte_budget=byte_budget,
            errors=errors,
            seal=seal,
        )
    )
    if len(snapshot) > MAX_PUBLIC_TOTAL_FILES:
        errors.append("insider public tree exceeds its total file-count limit")
    try:
        root_after = os.fstat(root_fd)
        if root_before is None or _publication_tree_metadata_identity(
            root_before
        ) != _publication_tree_metadata_identity(root_after):
            errors.append("insider public root changed while it was being snapshotted")
        elif seal is not None:
            if "." in seal:
                errors.append(
                    "insider public root has a duplicate publication seal entry"
                )
            else:
                seal["."] = _publication_tree_entry_seal(
                    root_after,
                    kind="directory",
                    sha256=None,
                )
    except OSError as error:
        errors.append(f"insider public root cannot be sealed safely: {error}")
    return snapshot, errors


def _read_validated_insider_public_snapshot_path(
    public_root: Path,
    *,
    allow_missing: bool = False,
) -> tuple[dict[str, bytes], list[str]]:
    root = Path(os.path.abspath(os.fspath(public_root)))
    name = root.name
    if name in {"", ".", ".."}:
        return {}, [f"insider public root has an invalid name: {root}"]
    parent_fd: int | None = None
    root_fd: int | None = None
    parent_locked = False
    try:
        parent_fd = os.open(os.fspath(root.parent), _PUBLICATION_DIRECTORY_FLAGS)
        fcntl.flock(parent_fd, fcntl.LOCK_SH)
        parent_locked = True
        root_fd = os.open(name, _PUBLICATION_DIRECTORY_FLAGS, dir_fd=parent_fd)
        snapshot, snapshot_errors = _snapshot_insider_public_tree_fd(root_fd)
        validation_errors = _validate_insider_public_snapshot_bytes(snapshot)
        errors = [*snapshot_errors, *validation_errors]
        if errors:
            return {}, errors
        return snapshot, []
    except FileNotFoundError as error:
        if allow_missing and root_fd is None:
            return {}, []
        return {}, [f"insider public root cannot be opened safely: {root}: {error}"]
    except OSError as error:
        return {}, [f"insider public root cannot be opened safely: {root}: {error}"]
    finally:
        if root_fd is not None:
            os.close(root_fd)
        if parent_locked and parent_fd is not None:
            try:
                fcntl.flock(parent_fd, fcntl.LOCK_UN)
            except OSError:
                pass
        if parent_fd is not None:
            os.close(parent_fd)


def _validate_insider_public_snapshot_bytes(
    snapshot: Mapping[str, bytes],
) -> list[str]:
    if not snapshot:
        return []
    with tempfile.TemporaryDirectory(prefix="insider-public-audit-") as tmpdir:
        audit_root = Path(tmpdir)
        for relative, encoded in sorted(snapshot.items()):
            destination = audit_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(encoded)
        return _validate_insider_public_snapshot_dir(audit_root)


def _read_validated_insider_public_snapshot_sealed_fd(
    root_fd: int,
) -> tuple[
    dict[str, bytes],
    tuple[tuple[str, _PublicationTreeEntrySeal], ...],
    list[str],
]:
    seal: dict[str, _PublicationTreeEntrySeal] = {}
    snapshot, snapshot_errors = _snapshot_insider_public_tree_fd(root_fd, seal=seal)
    validation_errors = _validate_insider_public_snapshot_bytes(snapshot)
    errors = [*snapshot_errors, *validation_errors]
    if errors:
        return {}, (), errors
    return snapshot, tuple(sorted(seal.items())), []


def _read_validated_insider_public_snapshot_fd(
    root_fd: int,
) -> tuple[dict[str, bytes], list[str]]:
    snapshot, _, errors = _read_validated_insider_public_snapshot_sealed_fd(root_fd)
    return snapshot, errors


def read_validated_insider_public_snapshot_fd(
    public_root_fd: int,
) -> tuple[dict[str, bytes], list[str]]:
    """Validate an already-open public root without returning to path lookup.

    A caller that opened this descriptor through the live ``data/insiders``
    namespace must hold a shared lock on that retained parent directory
    descriptor for the complete read-and-use interval. Publication writers hold
    the corresponding exclusive lock.
    """

    if type(public_root_fd) is not int or public_root_fd < 0:
        return {}, ["public insider projection descriptor is invalid"]
    try:
        duplicate = os.dup(public_root_fd)
    except OSError as error:
        return {}, [
            f"public insider projection descriptor cannot be duplicated: {error}"
        ]
    try:
        metadata = os.fstat(duplicate)
        if not stat.S_ISDIR(metadata.st_mode):
            return {}, ["public insider projection descriptor is not a directory"]
        return _read_validated_insider_public_snapshot_fd(duplicate)
    except OSError as error:
        return {}, [f"public insider projection descriptor cannot be read: {error}"]
    finally:
        os.close(duplicate)


def read_validated_insider_public_snapshot(
    public_root: Path,
) -> tuple[dict[str, bytes], list[str]]:
    """Read once without following links, then validate the immutable bytes."""

    return _read_validated_insider_public_snapshot_path(public_root)


def validate_insider_public_tree(public_root: Path) -> list[str]:
    """Audit a descriptor-snapshotted tree and recompute every page metric."""

    _, errors = read_validated_insider_public_snapshot(public_root)
    return errors


def validate_optional_insider_public_tree(public_root: Path) -> list[str]:
    """Validate an optional tree while coordinating absence with the writer."""

    _, errors = _read_validated_insider_public_snapshot_path(
        public_root,
        allow_missing=True,
    )
    return errors


__all__ = [
    "INSIDER_PUBLIC_CONTRACT_VERSION",
    "MAX_PUBLIC_FILING_DETAIL_BYTES",
    "MAX_PUBLIC_SECURITY_PAYLOAD_BYTES",
    "InsiderPublication",
    "InsiderPublicationError",
    "build_insider_publication",
    "build_static_insider_metric_projection",
    "canonical_public_json_bytes",
    "combine_insider_publications",
    "read_validated_insider_public_snapshot",
    "read_validated_insider_public_snapshot_fd",
    "validate_insider_public_tree",
    "validate_optional_insider_public_tree",
    "write_insider_publication",
]
