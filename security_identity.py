"""Shared, deterministic security-identity helpers.

Public position identity is the normalized security identifier plus its
instrument type.  Descriptive metadata such as issuer, ticker, and SEC
``titleOfClass`` text deliberately stays outside the identity.

This module is stdlib-only so ingestion, validation, repair scripts, and tests
can all import the same primitives without creating dependency or network
requirements.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from datetime import date


INSTRUMENT_TYPES = (
    "EQUITY",
    "PREF",
    "NOTE",
    "WARRANT",
    "CALL",
    "PUT",
    "OPT",
)
VALID_INSTRUMENT_TYPES = frozenset(INSTRUMENT_TYPES)
DEFAULT_INSTRUMENT_TYPE = "EQUITY"
SECURITY_KINDS = (
    "COMMON",
    "PREFERRED",
    "ETF",
    "ETN",
    "MUTUAL FUND",
    "CLOSED-END FUND",
    "BOND",
    "WARRANT",
    "RIGHT",
    "UNIT",
)
VALID_SECURITY_KINDS = frozenset(SECURITY_KINDS)

_UNSAFE_FILENAME_CHARS_RE = re.compile(r"[^A-Z0-9._-]")
_MAX_SECURITY_LABEL_LENGTH = 160
_SECURITY_CLASS_TOKEN_RE = re.compile(r"[A-Z0-9]+")
_DATE_TOKEN_RE = re.compile(
    r"(?<!\d)(?P<month>\d{1,2})/(?P<day>\d{1,2})/"
    r"(?P<year>\d{2,4})(?!\d)"
)
_TRUNCATED_DATE_RE = re.compile(r"(?<!\d)\d{1,2}/\d{1,2}/\d{3}(?!\d)")
_PLACEHOLDER_CLASS_RE = re.compile(
    r"(?:^|\s)(?:#?N/?A|NONE|NULL|INVALID|UNKNOWN)(?:$|\s)",
    re.IGNORECASE,
)
_PLACEHOLDER_SECURITY_LABELS = frozenset({
    "#N/A",
    "N/A",
    "NA",
    "NONE",
    "NULL",
    "INVALID",
    "LOOK IT UP",
    "UNKNOWN",
})
_INVALID_SECURITY_LABEL_RE = re.compile(
    r"(?:^|\s)#N/A(?:$|\s)|\bINVALID SECURITY\b",
    re.IGNORECASE,
)
_GENERIC_SECURITY_CLASS_TOKENS = frozenset({
    "ADR",
    "ADS",
    "BD",
    "BDS",
    "BOND",
    "BONDS",
    "CALL",
    "CLASS",
    "CL",
    "CNV",
    "COM",
    "COMMON",
    "CONV",
    "CV",
    "CVP",
    "CVPFD",
    "DEBT",
    "EQUITY",
    "NOTE",
    "NOTES",
    "OPT",
    "OPTION",
    "OPTIONS",
    "ORD",
    "ORDINARY",
    "PFD",
    "PREF",
    "PREFERRED",
    "PRF",
    "PUBLIC",
    "SDBCV",
    "SEC",
    "SECURITY",
    "SER",
    "SERIES",
    "SH",
    "SHARE",
    "SHARES",
    "SHS",
    "STK",
    "STOCK",
    "UNIT",
    "UNITS",
    "WARRANT",
    "WARRANTS",
    "WT",
    "WTS",
})
_NOTE_SECURITY_LABEL_RE = re.compile(
    r"^(?P<symbol>[A-Z][A-Z0-9./-]{0,15}"
    r"(?:\s+[A-Z][A-Z0-9./-]{0,15}){0,2})\s+"
    r"(?P<coupon>V?\d+(?:\.\d+)?%?(?:\s+\d+/\d+)?|FLT|VAR)\s+"
    r"(?P<maturity>\d{1,2}/\d{1,2}/\d{2,4}|PERP)"
    r"(?P<qualifiers>(?:\s+[A-Z0-9.*!@-]+){0,2})$"
)
_NOTE_MATURITY_ONLY_LABEL_RE = re.compile(
    r"^(?P<symbol>[A-Z]{2}\s+[A-Z][A-Z0-9./-]{0,15})\s+"
    r"(?P<maturity>\d{1,2}/\d{1,2}/\d{2,4})"
    r"(?P<qualifiers>(?:\s+[A-Z0-9.*!@-]+){0,2})$"
)
_SEC_ISSUER_ABBREVIATIONS = {
    "BIOSCIENC": "BIOSCIENCES",
    "COS": "COMPANIES",
    "ENTMT": "ENTERTAINMENT",
    "HLDGS": "HOLDINGS",
    "INDS": "INDUSTRIES",
    "INDUS": "INDUSTRIAL",
    "INFRA": "INFRASTRUCTURE",
    "INTL": "INTERNATIONAL",
    "LABS": "LABORATORIES",
    "MED": "MEDICAL",
    "PPTY": "PROPERTY",
    "PPTYS": "PROPERTIES",
    "RES": "RESOURCES",
    "SYS": "SYSTEMS",
    "TR": "TRUST",
}
_SEC_ISSUER_IGNORED_TOKENS = {
    "A",
    "AB",
    "AG",
    "AN",
    "AND",
    "CA",
    "CAL",
    "CALIFORNIA",
    "CL",
    "CLASS",
    "CO",
    "COM",
    "COMMON",
    "COMPANY",
    "COMPANIES",
    "CONSOL",
    "CONSOLIDATED",
    "CORP",
    "CORPORATION",
    "DE",
    "DEL",
    "DELAWARE",
    "FL",
    "FOR",
    "FUND",
    "GP",
    "GRP",
    "GROUP",
    "HLDG",
    "HLDGS",
    "HOLDING",
    "HOLDINGS",
    "IL",
    "IN",
    "INC",
    "INCORPORATED",
    "LLC",
    "LLP",
    "LP",
    "LTD",
    "LIMITED",
    "MD",
    "MARYLAND",
    "N",
    "NEW",
    "NJ",
    "NV",
    "NY",
    "OF",
    "ON",
    "PA",
    "PLC",
    "SA",
    "SER",
    "SERIES",
    "STOCK",
    "TEXAS",
    "THE",
    "TO",
    "TRUST",
    "TX",
    "US",
    "USA",
    "WITH",
}


def normalize_instrument_type(instrument_type: object | None) -> str:
    """Return a supported uppercase type, defaulting invalid values to equity."""

    normalized = str(instrument_type or DEFAULT_INSTRUMENT_TYPE).strip().upper()
    if normalized in VALID_INSTRUMENT_TYPES:
        return normalized
    return DEFAULT_INSTRUMENT_TYPE


def sec_issuer_proof_key(issuer: object) -> str:
    """Return a conservative issuer key for independent SEC alias proof."""

    value = str(issuer or "").upper()
    value = re.sub(r"/[A-Z]{1,10}/?", " ", value)
    value = re.sub(
        r"\b(CL|CLASS|SER|SERIES)\s+[A-Z0-9]+\b",
        " ",
        value,
    )
    value = re.sub(r"\bINC-[A-Z]\b", " INC ", value)
    tokens = re.sub(r"[^A-Z0-9]+", " ", value).split()
    normalized = (
        _SEC_ISSUER_ABBREVIATIONS.get(token, token)
        for token in tokens
    )
    return "".join(
        token
        for token in normalized
        if token not in _SEC_ISSUER_IGNORED_TOKENS
    )


def sec_ticker_titles(company_tickers: object) -> dict[str, str]:
    """Build an unambiguous ticker-to-title index from SEC company data."""

    if isinstance(company_tickers, Mapping):
        entries = company_tickers.values()
    elif isinstance(company_tickers, list):
        entries = company_tickers
    else:
        return {}

    candidates: dict[str, set[str]] = {}
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        ticker = str(entry.get("ticker") or "").strip().upper()
        title = str(entry.get("title") or "").strip()
        if ticker and title:
            candidates.setdefault(ticker, set()).add(title)

    # A duplicate ticker with issuer-disagreeing SEC titles is not proof.
    titles: dict[str, str] = {}
    for ticker, ticker_titles in candidates.items():
        issuer_keys = {
            sec_issuer_proof_key(title) for title in ticker_titles
        }
        if len(issuer_keys) == 1 and "" not in issuer_keys:
            titles[ticker] = sorted(ticker_titles)[0]
    return titles


def normalize_security_identifier(identifier: object | None) -> str:
    """Normalize a CUSIP or fallback ticker for use in a public identity."""

    return str(identifier or "").strip().upper()


def normalize_security_kind(kind: object | None) -> str | None:
    """Return one finite, reader-facing security kind when recognized."""

    normalized = " ".join(str(kind or "").strip().upper().split())
    return normalized if normalized in VALID_SECURITY_KINDS else None


def normalize_security_label(
    label: object | None,
    identifier: object | None = None,
) -> str | None:
    """Return safe, deterministic single-line display metadata.

    Labels deliberately permit punctuation used by OpenFIGI and SEC security
    descriptions. Control characters, overlong values, and a bare copy of the
    canonical identifier are rejected so callers never mistake a raw CUSIP
    fallback for descriptive metadata.
    """

    raw = str(label) if label is not None else ""
    if any(
        unicodedata.category(character) in {"Cc", "Cf", "Cs"}
        for character in raw
    ):
        return None

    normalized = " ".join(raw.strip().split())
    if not normalized or len(normalized) > _MAX_SECURITY_LABEL_LENGTH:
        return None
    if normalized.isdigit():
        return None
    if (
        normalized.upper() in _PLACEHOLDER_SECURITY_LABELS
        or _INVALID_SECURITY_LABEL_RE.search(normalized)
        or _TRUNCATED_DATE_RE.search(normalized)
    ):
        return None

    normalized_identifier = normalize_security_identifier(identifier)
    if (
        normalized_identifier
        and normalized.casefold() == normalized_identifier.casefold()
    ):
        return None
    return normalized


def _informative_security_class_label(
    class_name: str | None,
) -> str | None:
    if not class_name:
        return None
    if (
        _TRUNCATED_DATE_RE.search(class_name)
        or _PLACEHOLDER_CLASS_RE.search(class_name)
    ):
        return None

    def normalize_or_remove_date(match: re.Match[str]) -> str:
        first = int(match.group("month"))
        second = int(match.group("day"))
        year_text = match.group("year")
        year = int(year_text)
        if (
            first > 12
            and len(year_text) == 4
            and second in range(1, 13)
            and year not in {0, 1, 99, 1900, 1970, 9999}
        ):
            try:
                date(year, second, first)
            except ValueError:
                return " "
            return f"{second:02d}/{first:02d}/{year % 100:02d}"
        if (
            first not in range(1, 13)
            or second not in range(1, 32)
            or year in {0, 1, 99, 1900, 1970, 9999}
        ):
            return " "
        return match.group(0)

    without_placeholders = " ".join(
        _DATE_TOKEN_RE.sub(
            normalize_or_remove_date,
            class_name,
        ).split()
    )
    if (
        not re.search(r"[A-Z]", without_placeholders, re.IGNORECASE)
        and not _DATE_TOKEN_RE.search(without_placeholders)
    ):
        return None
    tokens = _SECURITY_CLASS_TOKEN_RE.findall(without_placeholders.upper())
    if not tokens or not any(
        token not in _GENERIC_SECURITY_CLASS_TOKENS
        for token in tokens
    ):
        return None
    return without_placeholders


def compose_security_label(
    name: object | None,
    class_name: object | None,
    instrument_type: object | None,
    identifier: object | None = None,
) -> str:
    """Compose a useful non-identifier fallback from retained SEC metadata."""

    issuer = normalize_security_label(name, identifier)
    if issuer and not re.search(r"[A-Z]", issuer, re.IGNORECASE):
        issuer = None
    security_class = normalize_security_label(class_name, identifier)
    informative_class = _informative_security_class_label(security_class)

    if issuer and informative_class:
        combined = normalize_security_label(
            f"{issuer} — {informative_class}",
            identifier,
        )
        if combined:
            return combined
    if issuer:
        return issuer
    if informative_class:
        return informative_class
    return f"{normalize_instrument_type(instrument_type)} SECURITY"


def normalize_note_security_label(label: object | None) -> str | None:
    """Return one structured issuer/terms note label for display.

    OpenFIGI note tickers commonly look like ``RIVN 3.625 10/15/30``. Some
    venues append the maturity year a second time (for example
    ``UBER 0.875 12/01/28 2028``); that redundant suffix is removed. A narrow
    state/issuer/maturity form is also retained for municipal securities whose
    OpenFIGI ticker omits the coupon.
    """

    normalized = " ".join(str(label or "").strip().upper().split())
    match = _NOTE_SECURITY_LABEL_RE.fullmatch(normalized)
    if match is None:
        maturity_only_match = _NOTE_MATURITY_ONLY_LABEL_RE.fullmatch(
            normalized
        )
        return (
            maturity_only_match.group(0)
            if maturity_only_match is not None
            else None
        )

    maturity = match.group("maturity")
    qualifiers = match.group("qualifiers").split()
    if maturity != "PERP" and qualifiers and re.fullmatch(
        r"(?:19|20)\d{2}",
        qualifiers[-1],
    ):
        possible_redundant_year = qualifiers[-1]
        maturity_year = maturity.rsplit("/", 1)[-1]
        year_matches = (
            len(maturity_year) == 2
            and possible_redundant_year[-2:] == maturity_year
        ) or (
            len(maturity_year) == 4
            and possible_redundant_year == maturity_year
        )
        if year_matches:
            qualifiers.pop()

    terms = " ".join(
        [match.group("symbol"), match.group("coupon"), maturity, *qualifiers]
    )
    return terms


def is_canonical_security_identifier(identifier: object | None) -> bool:
    """Whether an identifier is nonempty, normalized, and ID-delimiter safe."""

    normalized = normalize_security_identifier(identifier)
    return (
        type(identifier) is str
        and bool(normalized)
        and identifier == normalized
        and "|" not in normalized
    )


def holding_instrument_type(holding: Mapping[str, object] | None) -> str:
    """Return the public row type, giving explicit SEC option side precedence."""

    if not isinstance(holding, Mapping):
        return DEFAULT_INSTRUMENT_TYPE
    put_call = str(holding.get("put_call") or "").strip().upper()
    if put_call in {"CALL", "PUT"}:
        return put_call
    return normalize_instrument_type(
        holding.get("holding_type") or holding.get("option_type")
    )


def stock_lookup_id(
    identifier: object | None,
    instrument_type: object | None = DEFAULT_INSTRUMENT_TYPE,
) -> str:
    """Build the canonical public stock ID from an identifier and row type."""

    base = normalize_security_identifier(identifier)
    if not base:
        return ""
    normalized_type = normalize_instrument_type(instrument_type)
    if normalized_type == DEFAULT_INSTRUMENT_TYPE:
        return base
    return f"{base}|{normalized_type}"


def parse_stock_lookup_id(stock_id: object | None) -> tuple[str, str]:
    """Split a stock ID into its normalized identifier and instrument type."""

    raw = normalize_security_identifier(stock_id)
    if "|" not in raw:
        return raw, DEFAULT_INSTRUMENT_TYPE
    base, instrument_type = raw.rsplit("|", 1)
    return (
        normalize_security_identifier(base),
        normalize_instrument_type(instrument_type),
    )


def safe_ticker(identifier: object | None) -> str:
    """Return the filesystem-safe form historically used for stock filenames."""

    normalized = normalize_security_identifier(identifier)
    return _UNSAFE_FILENAME_CHARS_RE.sub("_", normalized)


def stock_file_stem(stock_id: object | None) -> str:
    """Return the generated stock filename stem for a canonical stock ID."""

    base, instrument_type = parse_stock_lookup_id(stock_id)
    safe_base = safe_ticker(base)
    if not safe_base:
        return ""
    if instrument_type == DEFAULT_INSTRUMENT_TYPE:
        return safe_base
    return f"{safe_base}__{instrument_type}"


def stock_filename(
    identifier: object | None,
    instrument_type: object | None = DEFAULT_INSTRUMENT_TYPE,
) -> str:
    """Return the generated JSON filename for an identifier and row type."""

    stem = stock_file_stem(stock_lookup_id(identifier, instrument_type))
    return f"{stem}.json" if stem else ""


__all__ = [
    "DEFAULT_INSTRUMENT_TYPE",
    "INSTRUMENT_TYPES",
    "SECURITY_KINDS",
    "VALID_INSTRUMENT_TYPES",
    "VALID_SECURITY_KINDS",
    "compose_security_label",
    "holding_instrument_type",
    "is_canonical_security_identifier",
    "normalize_instrument_type",
    "normalize_note_security_label",
    "normalize_security_kind",
    "normalize_security_label",
    "normalize_security_identifier",
    "parse_stock_lookup_id",
    "safe_ticker",
    "stock_file_stem",
    "stock_filename",
    "stock_lookup_id",
]
