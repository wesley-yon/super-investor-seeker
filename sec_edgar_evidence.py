"""Exact, SEC-only CUSIP-to-symbol evidence from EDGAR filings.

Schedule 13D/G XML proves that one exact CUSIP belongs to an issuer CIK and
reported security class.  A periodic filing's inline XBRL proves a trading
symbol only when the security title, symbol, and exchange share one XBRL
context.  Neither source is sufficient by itself: :func:`bridge_sec_evidence`
joins them on issuer CIK and an exact normalized class string and otherwise
fails closed.

The refresh API intentionally accepts explicit filing documents.  A separate,
bounded discovery API can locate those documents through SEC-hosted full-text
search and the documented Submissions API, but search metadata never becomes
mapping evidence by itself.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import threading
import time
import unicodedata
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

import requests
from lxml import etree

from sec_http import RedirectPolicy, get_sec_response, make_rate_pacer
from atomic_files import atomic_text_output
from security_identity import SEC_TICKER_RE


ROOT = Path(__file__).resolve().parent
DEFAULT_CACHE_PATH = ROOT / ".cache" / "sec_edgar_evidence.json"
CACHE_SCHEMA_VERSION = 1
DEFAULT_MAX_CURRENT_EVIDENCE_AGE_DAYS = 395

SCHEDULE_13DG = "schedule_13dg"
PERIODIC_IXBRL = "periodic_ixbrl"
SEC_CUSIP_SEARCH = "sec_cusip_search"
SEC_SUBMISSIONS = "sec_submissions"

_SOURCE_KINDS = frozenset({SCHEDULE_13DG, PERIODIC_IXBRL})
_SEC_FILING_HOSTS = frozenset({"sec.gov", "www.sec.gov"})
_SEC_DISCOVERY_HOSTS = frozenset(
    {
        "data.sec.gov",
        "efts.sec.gov",
        "sec.gov",
        "www.sec.gov",
    }
)
_DEFAULT_MAX_REDIRECTS = 5
_ACCESSION_RE = re.compile(r"^(?P<cik>\d{10})-(?P<year>\d{2})-(?P<sequence>\d{6})$")
_ARCHIVE_PATH_RE = re.compile(
    r"^/Archives/edgar/data/\d{1,10}/(?P<accession>\d{18})/[^?#]+$",
    re.IGNORECASE,
)
_SCHEDULE_NAMESPACE_RE = re.compile(
    r"^https?://www\.sec\.gov/edgar/schedule13(?P<form>[dg])/?$",
    re.IGNORECASE,
)
_STRUCTURED_SCHEDULE_XSL_RE = re.compile(
    r"^xslSCHEDULE_13[DG]_X\d+$",
    re.IGNORECASE,
)
_SAFE_PRIMARY_DOCUMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_MAX_ARCHIVE_CIK_ATTEMPTS = 8
_PERIODIC_FORMS = frozenset(
    {
        "10-K",
        "10-K/A",
        "10-Q",
        "10-Q/A",
        "20-F",
        "20-F/A",
        "40-F",
        "40-F/A",
    }
)
_SYMBOL_RE = SEC_TICKER_RE
_XBRLI_NAMESPACE = "http://www.xbrl.org/2003/instance"
_XBRLDI_NAMESPACE = "http://xbrl.org/2006/xbrldi"
_IX_NAMESPACE = "http://www.xbrl.org/2013/inlineXBRL"
_SEC_CIK_SCHEMES = frozenset(
    {
        "http://www.sec.gov/cik",
        "https://www.sec.gov/cik",
    }
)

_EXCHANGE_CODES = {
    value.casefold(): value
    for value in (
        "NYSE",
        "NASDAQ",
        "CHX",
        "BOX",
        "BX",
        "C2",
        "CBOE",
        "CboeBYX",
        "CboeBZX",
        "CboeEDGA",
        "CboeEDGX",
        "GEMX",
        "IEX",
        "ISE",
        "MIAX",
        "MRX",
        "NYSEAMER",
        "NYSEArca",
        "NYSENAT",
        "PEARL",
        "Phlx",
        "NONE",
    )
}
_EXCHANGE_NAMES = {
    "the nasdaq stock market llc": "NASDAQ",
    "the nasdaq stock market": "NASDAQ",
    "nasdaq stock market": "NASDAQ",
    "new york stock exchange": "NYSE",
    "new york stock exchange, inc.": "NYSE",
    "new york stock exchange llc": "NYSE",
    "nyse american llc": "NYSEAMER",
    "nyse arca, inc.": "NYSEArca",
    "nyse arca inc.": "NYSEArca",
    "investors exchange llc": "IEX",
}

Fetcher = Callable[[str], bytes]


class SecEdgarEvidenceError(ValueError):
    """Base error for unsafe sources, malformed evidence, and refreshes."""


class NonSECFilingURL(SecEdgarEvidenceError):
    """Raised before a non-SEC or non-archive URL can be fetched."""


class EvidenceParseError(SecEdgarEvidenceError):
    """Raised when a document cannot safely become identity evidence."""


class EvidenceCorruptResponseError(EvidenceParseError):
    """Raised when fetched bytes cannot be decoded into a complete document."""


class EvidenceSchemaError(EvidenceParseError):
    """Raised when a decoded SEC document breaks its required contract."""


class EvidenceRefreshError(SecEdgarEvidenceError):
    """Raised when an explicit-source cache refresh cannot complete."""


@dataclass(frozen=True)
class FilingSource:
    """One explicit EDGAR filing document supplied by the caller."""

    kind: str
    url: str
    accession: str


@dataclass(frozen=True)
class ScheduleSearchCandidate:
    """Untrusted structured-Schedule locator returned by SEC search."""

    accession: str
    primary_document: str
    filing_date: str
    archive_ciks: tuple[str, ...]


@dataclass(frozen=True)
class ScheduleSearchResults:
    """Purely parsed SEC full-text-search candidates and completeness state."""

    candidates: tuple[ScheduleSearchCandidate, ...]
    total_hits: int
    truncated: bool


@dataclass(frozen=True)
class DiscoveryDiagnostic:
    """Terminal or retryable result for one requested exact CUSIP."""

    cusip: str
    status: str
    terminal: bool
    reason: str
    issuer_cik: str | None = None
    security_class: str | None = None
    schedule_candidate_count: int = 0
    exact_schedule_count: int = 0
    periodic_candidate_count: int = 0
    source_accessions: tuple[str, ...] = ()


@dataclass(frozen=True)
class DiscoveryFetch:
    """Hashable audit state for one attempted SEC discovery fetch."""

    kind: str
    url: str
    outcome: str
    sha256: str | None


@dataclass(frozen=True)
class DiscoveryResult:
    """Immutable, persistable automatic-discovery result."""

    sources: tuple[FilingSource, ...]
    diagnostics: tuple[DiscoveryDiagnostic, ...]
    fetched_sources: tuple[DiscoveryFetch, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "sources": [
                {
                    "kind": source.kind,
                    "url": source.url,
                    "accession": source.accession,
                }
                for source in self.sources
            ],
            "diagnostics": [
                {
                    "cusip": item.cusip,
                    "status": item.status,
                    "terminal": item.terminal,
                    "reason": item.reason,
                    "issuer_cik": item.issuer_cik,
                    "security_class": item.security_class,
                    "schedule_candidate_count": item.schedule_candidate_count,
                    "exact_schedule_count": item.exact_schedule_count,
                    "periodic_candidate_count": item.periodic_candidate_count,
                    "source_accessions": list(item.source_accessions),
                }
                for item in self.diagnostics
            ],
            "fetched_sources": [
                {
                    "kind": item.kind,
                    "url": item.url,
                    "outcome": item.outcome,
                    "sha256": item.sha256,
                }
                for item in self.fetched_sources
            ],
        }


def _local_name(tag: object) -> str:
    if not isinstance(tag, str):
        return ""
    if tag.startswith("{"):
        return etree.QName(tag).localname
    return tag.rsplit(":", 1)[-1]


def _namespace(tag: object) -> str:
    if not isinstance(tag, str) or not tag.startswith("{"):
        return ""
    return etree.QName(tag).namespace or ""


def _normalized_text(value: object | None) -> str:
    text = unicodedata.normalize("NFKC", unescape(str(value or "")))
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def normalize_security_class(value: object | None) -> str:
    """Normalize only presentation differences; do not fuzzy-match classes."""

    return _normalized_text(value).casefold()


def _normalize_cik(value: object | None) -> str:
    text = _normalized_text(value)
    if not text.isdigit() or not 1 <= len(text) <= 10:
        raise EvidenceParseError(f"invalid issuer CIK: {value!r}")
    return text.zfill(10)


def _cusip_character_value(character: str) -> int | None:
    if character.isdigit():
        return int(character)
    if "A" <= character <= "Z":
        return ord(character) - ord("A") + 10
    return {"*": 36, "@": 37, "#": 38}.get(character)


def _normalize_cusip(value: object | None) -> str:
    cusip = _normalized_text(value).upper()
    if len(cusip) != 9 or not cusip[-1].isdigit():
        raise EvidenceParseError(f"invalid CUSIP: {value!r}")
    total = 0
    for index, character in enumerate(cusip[:8]):
        numeric = _cusip_character_value(character)
        if numeric is None:
            raise EvidenceParseError(f"invalid CUSIP: {value!r}")
        if index % 2 == 1:
            numeric *= 2
        total += numeric // 10 + numeric % 10
    expected = (10 - total % 10) % 10
    if int(cusip[-1]) != expected:
        raise EvidenceParseError(f"invalid CUSIP check digit: {cusip}")
    return cusip


def _normalize_date(value: object | None) -> str:
    text = _normalized_text(value)
    for pattern in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, pattern).date().isoformat()
        except ValueError:
            continue
    raise EvidenceParseError(f"invalid evidence date: {value!r}")


def _normalize_accession(value: object | None) -> str:
    accession = _normalized_text(value)
    if not _ACCESSION_RE.fullmatch(accession):
        raise SecEdgarEvidenceError(
            f"accession must use ##########-##-######: {value!r}"
        )
    return accession


def normalize_sec_filing_url(
    value: object | None,
    *,
    accession: object | None = None,
) -> str:
    """Return a canonical HTTPS SEC archive URL or fail before fetching."""

    raw_url = _normalized_text(value)
    parsed = urlsplit(raw_url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise NonSECFilingURL(f"invalid SEC filing URL: {raw_url!r}") from exc
    host = (parsed.hostname or "").casefold()
    if (
        parsed.scheme.casefold() != "https"
        or host not in _SEC_FILING_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.query
        or parsed.fragment
        or "\\" in parsed.path
        or any(part in {".", ".."} for part in parsed.path.split("/"))
    ):
        raise NonSECFilingURL(
            f"only direct HTTPS SEC archive filing URLs are allowed: {raw_url!r}"
        )
    archive_match = _ARCHIVE_PATH_RE.fullmatch(parsed.path)
    if archive_match is None:
        raise NonSECFilingURL(f"not an SEC EDGAR filing document URL: {raw_url!r}")
    if accession is not None:
        normalized_accession = _normalize_accession(accession)
        if archive_match.group("accession") != normalized_accession.replace("-", ""):
            raise NonSECFilingURL(
                "SEC URL accession directory does not match supplied accession"
            )
    netloc = host if port is None else f"{host}:443"
    return urlunsplit(("https", netloc, parsed.path, "", ""))


def normalize_sec_discovery_url(value: object | None) -> str:
    """Validate one SEC search, submissions, or archive discovery URL."""

    raw_url = _normalized_text(value)
    parsed = urlsplit(raw_url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise NonSECFilingURL(f"invalid SEC discovery URL: {raw_url!r}") from exc
    host = (parsed.hostname or "").casefold()
    if (
        parsed.scheme.casefold() != "https"
        or host not in _SEC_DISCOVERY_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.fragment
    ):
        raise NonSECFilingURL(f"not an allowed SEC discovery URL: {raw_url!r}")
    if host == "efts.sec.gov":
        if parsed.path != "/LATEST/search-index" or not parsed.query:
            raise NonSECFilingURL("invalid SEC full-text-search URL")
    elif host == "data.sec.gov":
        if parsed.query or not re.fullmatch(
            r"/submissions/CIK\d{10}\.json", parsed.path
        ):
            raise NonSECFilingURL("invalid SEC submissions URL")
    else:
        return normalize_sec_filing_url(raw_url)
    netloc = host if port is None else f"{host}:443"
    return urlunsplit(("https", netloc, parsed.path, parsed.query, ""))


def build_sec_cusip_search_url(cusip: object | None) -> str:
    """Build the official SEC full-text exact-phrase Schedule search URL."""

    normalized_cusip = _normalize_cusip(cusip)
    query = urlencode(
        {
            "q": f'"{normalized_cusip}"',
            "forms": "SCHEDULE 13D,SCHEDULE 13G",
            "startdt": "2024-12-18",
            "from": "0",
            "size": "100",
        },
        quote_via=quote,
    )
    return normalize_sec_discovery_url(
        f"https://efts.sec.gov/LATEST/search-index?{query}"
    )


def build_sec_submissions_url(issuer_cik: object | None) -> str:
    """Build the documented SEC Submissions API URL for one issuer CIK."""

    normalized_cik = _normalize_cik(issuer_cik)
    return normalize_sec_discovery_url(
        f"https://data.sec.gov/submissions/CIK{normalized_cik}.json"
    )


def _json_object(payload: str | bytes, *, document_name: str) -> dict[str, Any]:
    try:
        raw = (
            payload.decode("utf-8-sig") if isinstance(payload, bytes) else str(payload)
        )
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceCorruptResponseError(
            f"malformed {document_name} JSON"
        ) from exc
    if not isinstance(parsed, dict):
        raise EvidenceSchemaError(
            f"{document_name} JSON root must be an object"
        )
    return parsed


def _safe_primary_document(value: object | None, *, xml_only: bool = False) -> str:
    filename = _normalized_text(value)
    suffixes = {".xml"} if xml_only else {".htm", ".html", ".xhtml"}
    if (
        not _SAFE_PRIMARY_DOCUMENT_RE.fullmatch(filename)
        or Path(filename).suffix.casefold() not in suffixes
    ):
        raise EvidenceParseError(f"unsafe SEC primary document name: {value!r}")
    return filename


def parse_sec_schedule_search_results(
    payload: str | bytes,
    *,
    max_hits: int = 100,
) -> ScheduleSearchResults:
    """Purely parse structured Schedule candidates from SEC full-text search."""

    if max_hits < 1 or max_hits > 100:
        raise SecEdgarEvidenceError("max_hits must be between 1 and 100")
    document = _json_object(payload, document_name="SEC full-text search")
    hits_object = document.get("hits")
    if not isinstance(hits_object, dict) or not isinstance(
        hits_object.get("hits"), list
    ):
        raise EvidenceSchemaError(
            "SEC full-text search response has no hits array"
        )
    raw_total = hits_object.get("total", 0)
    total_is_lower_bound = False
    if isinstance(raw_total, dict):
        total = raw_total.get("value")
        relation = _normalized_text(raw_total.get("relation")).casefold()
        if relation not in {"eq", "gte"}:
            raise EvidenceSchemaError(
                "SEC full-text search response has invalid hit-total relation"
            )
        total_is_lower_bound = relation == "gte"
    else:
        total = raw_total
    if type(total) is not int or total < 0:
        raise EvidenceSchemaError(
            "SEC full-text search response has invalid hit total"
        )
    if total < len(hits_object["hits"]):
        raise EvidenceSchemaError(
            "SEC full-text search hit total is inconsistent"
        )

    candidates = []
    for hit in hits_object["hits"]:
        if not isinstance(hit, dict) or not isinstance(hit.get("_source"), dict):
            continue
        source = hit["_source"]
        form = _normalized_text(source.get("form")).upper()
        roots = source.get("root_forms")
        root_forms = (
            {_normalized_text(value).upper() for value in roots}
            if isinstance(roots, list)
            else set()
        )
        if (
            form
            not in {"SCHEDULE 13D", "SCHEDULE 13D/A", "SCHEDULE 13G", "SCHEDULE 13G/A"}
            or not root_forms & {"SCHEDULE 13D", "SCHEDULE 13G"}
            or not _STRUCTURED_SCHEDULE_XSL_RE.fullmatch(
                _normalized_text(source.get("xsl"))
            )
            or not _normalized_text(source.get("schema_version"))
            .upper()
            .startswith("X")
        ):
            continue
        try:
            accession = _normalize_accession(source.get("adsh"))
            raw_id = _normalized_text(hit.get("_id"))
            id_accession, separator, raw_filename = raw_id.partition(":")
            if separator != ":" or id_accession != accession:
                raise EvidenceSchemaError(
                    "SEC search hit accession does not match its id"
                )
            filename = _safe_primary_document(raw_filename, xml_only=True)
            filing_date = _normalize_date(source.get("file_date"))
        except SecEdgarEvidenceError as exc:
            if isinstance(exc, EvidenceSchemaError):
                raise
            raise EvidenceSchemaError(
                "SEC search hit has invalid document metadata"
            ) from exc
        raw_ciks = source.get("ciks")
        if not isinstance(raw_ciks, list):
            raise EvidenceSchemaError("SEC search hit has no CIK candidates")
        archive_ciks = []
        for raw_cik in raw_ciks:
            try:
                cik = _normalize_cik(raw_cik)
            except EvidenceParseError as exc:
                raise EvidenceSchemaError(
                    "SEC search hit has an invalid archive CIK"
                ) from exc
            if int(cik) and cik not in archive_ciks:
                archive_ciks.append(cik)
        if not archive_ciks:
            raise EvidenceSchemaError(
                "SEC search hit has no valid archive CIK"
            )
        candidates.append(
            ScheduleSearchCandidate(
                accession=accession,
                primary_document=filename,
                filing_date=filing_date,
                archive_ciks=tuple(archive_ciks),
            )
        )
    candidates.sort(
        key=lambda item: (
            item.filing_date,
            item.accession,
            item.primary_document,
            item.archive_ciks,
        ),
        reverse=True,
    )
    deduplicated = []
    seen = set()
    for candidate in candidates:
        key = (candidate.accession, candidate.primary_document)
        if key not in seen:
            seen.add(key)
            deduplicated.append(candidate)
    truncated = (
        total_is_lower_bound
        or total > len(hits_object["hits"])
        or len(deduplicated) > max_hits
    )
    return ScheduleSearchResults(
        candidates=tuple(deduplicated[:max_hits]),
        total_hits=total,
        truncated=truncated,
    )


@dataclass(frozen=True)
class _PeriodicCandidate:
    source: FilingSource
    report_date: str
    filing_date: str


def _parse_periodic_candidates(
    payload: str | bytes,
    *,
    issuer_cik: str,
) -> tuple[_PeriodicCandidate, ...]:
    document = _json_object(payload, document_name="SEC submissions")
    normalized_cik = _normalize_cik(issuer_cik)
    try:
        document_cik = _normalize_cik(document.get("cik"))
    except EvidenceParseError as exc:
        raise EvidenceSchemaError(
            "SEC submissions response has invalid issuer CIK"
        ) from exc
    if document_cik != normalized_cik:
        raise EvidenceSchemaError("SEC submissions issuer CIK mismatch")
    filings = document.get("filings")
    recent = filings.get("recent") if isinstance(filings, dict) else None
    if not isinstance(recent, dict):
        raise EvidenceSchemaError(
            "SEC submissions response has no recent filings"
        )
    fields = (
        "accessionNumber",
        "filingDate",
        "reportDate",
        "form",
        "isInlineXBRL",
        "primaryDocument",
    )
    columns = {field: recent.get(field) for field in fields}
    if any(not isinstance(value, list) for value in columns.values()):
        raise EvidenceSchemaError(
            "SEC submissions recent columns are malformed"
        )
    lengths = {len(value) for value in columns.values()}
    if len(lengths) != 1:
        raise EvidenceSchemaError(
            "SEC submissions recent columns are misaligned"
        )

    candidates = []
    row_count = next(iter(lengths), 0)
    for index in range(row_count):
        form = _normalized_text(columns["form"][index]).upper()
        inline_flag = _normalized_text(columns["isInlineXBRL"][index]).casefold()
        if form not in _PERIODIC_FORMS or inline_flag not in {"1", "true", "yes"}:
            continue
        try:
            accession = _normalize_accession(columns["accessionNumber"][index])
            report_date = _normalize_date(columns["reportDate"][index])
            filing_date = _normalize_date(columns["filingDate"][index])
            primary_document = _safe_primary_document(
                columns["primaryDocument"][index]
            )
        except SecEdgarEvidenceError as exc:
            raise EvidenceSchemaError(
                "SEC submissions recent row has invalid document metadata"
            ) from exc
        archive_cik = str(int(normalized_cik))
        url = normalize_sec_filing_url(
            "https://www.sec.gov/Archives/edgar/data/"
            f"{archive_cik}/{accession.replace('-', '')}/{primary_document}",
            accession=accession,
        )
        candidates.append(
            _PeriodicCandidate(
                source=FilingSource(PERIODIC_IXBRL, url, accession),
                report_date=report_date,
                filing_date=filing_date,
            )
        )
    candidates.sort(
        key=lambda item: (
            item.report_date,
            item.filing_date,
            item.source.accession,
            item.source.url,
        ),
        reverse=True,
    )
    # An amended filing for one report date supersedes the original locator.
    latest_by_report_date = {}
    for candidate in candidates:
        latest_by_report_date.setdefault(candidate.report_date, candidate)
    return tuple(latest_by_report_date.values())


def parse_sec_submissions_periodic_sources(
    payload: str | bytes,
    *,
    issuer_cik: str,
) -> tuple[FilingSource, ...]:
    """Purely extract latest-first periodic iXBRL documents from submissions."""

    return tuple(
        candidate.source
        for candidate in _parse_periodic_candidates(
            payload,
            issuer_cik=issuer_cik,
        )
    )


def _xml_root(payload: str | bytes, *, document_name: str) -> etree._Element:
    raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    if not raw.strip():
        raise EvidenceCorruptResponseError(f"empty {document_name}")
    parser = etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        recover=False,
        huge_tree=False,
    )
    try:
        return etree.fromstring(raw, parser=parser)
    except (etree.XMLSyntaxError, ValueError, TypeError) as exc:
        raise EvidenceCorruptResponseError(
            f"malformed {document_name}"
        ) from exc


def _descendant_texts(parent: etree._Element, local_name: str) -> list[str]:
    expected = local_name.casefold()
    values = []
    for element in parent.iter():
        if _local_name(element.tag).casefold() != expected:
            continue
        value = _normalized_text("".join(element.itertext()))
        if value:
            values.append(value)
    return values


def _unique_text(
    values: Iterable[object],
    *,
    field: str,
    normalizer: Callable[[object], str] = _normalized_text,
) -> str:
    normalized: dict[str, str] = {}
    for raw_value in values:
        display = _normalized_text(raw_value)
        if not display:
            continue
        key = normalizer(raw_value)
        normalized.setdefault(key, display)
    if len(normalized) != 1:
        raise EvidenceParseError(
            f"expected one unambiguous {field}; found {len(normalized)}"
        )
    return next(iter(normalized.values()))


def parse_schedule_13dg_xml(
    payload: str | bytes,
    *,
    accession: str,
    source_url: str,
) -> list[dict[str, Any]]:
    """Parse structured Schedule 13D/G cover-page CUSIP/class evidence."""

    normalized_accession = _normalize_accession(accession)
    normalized_url = normalize_sec_filing_url(
        source_url,
        accession=normalized_accession,
    )
    root = _xml_root(payload, document_name="Schedule 13D/G XML")
    namespace_match = _SCHEDULE_NAMESPACE_RE.fullmatch(_namespace(root.tag))
    if _local_name(root.tag).casefold() != "edgarsubmission" or not namespace_match:
        raise EvidenceSchemaError(
            "document is not structured SEC Schedule 13D/G XML"
        )

    try:
        submission_type = _unique_text(
            _descendant_texts(root, "submissionType"),
            field="Schedule submission type",
            normalizer=lambda value: re.sub(r"\s+", " ", str(value)).upper(),
        ).upper()
    except EvidenceParseError as exc:
        raise EvidenceSchemaError(
            "Schedule document lacks one required submission type"
        ) from exc
    if not re.fullmatch(r"SCHEDULE 13[DG](?:/A)?", submission_type):
        raise EvidenceSchemaError(f"unsupported Schedule form: {submission_type!r}")
    if submission_type[11].casefold() != namespace_match.group("form").casefold():
        raise EvidenceSchemaError(
            "Schedule namespace and submission type disagree"
        )

    cover_pages = [
        element
        for element in root.iter()
        if _local_name(element.tag).casefold() == "coverpageheader"
    ]
    if len(cover_pages) != 1:
        raise EvidenceSchemaError("expected one Schedule coverPageHeader")
    cover = cover_pages[0]
    issuer_infos = [
        element
        for element in cover.iter()
        if _local_name(element.tag).casefold() == "issuerinfo"
    ]
    if len(issuer_infos) != 1:
        raise EvidenceSchemaError("expected one Schedule issuerInfo")
    issuer = issuer_infos[0]

    security_class = _unique_text(
        _descendant_texts(cover, "securitiesClassTitle"),
        field="Schedule security class",
        normalizer=normalize_security_class,
    )
    issuer_cik = _normalize_cik(
        _unique_text(
            _descendant_texts(issuer, "issuerCik"),
            field="Schedule issuer CIK",
        )
    )
    issuer_name = _unique_text(
        _descendant_texts(issuer, "issuerName"),
        field="Schedule issuer name",
        normalizer=lambda value: _normalized_text(value).casefold(),
    )
    event_dates = []
    for name in (
        "dateOfEvent",
        "eventDateRequiresFilingThisStatement",
        "dateOfEventWhichRequiresFilingThisStatement",
    ):
        event_dates.extend(_descendant_texts(cover, name))
    as_of = _normalize_date(
        _unique_text(
            event_dates,
            field="Schedule event date",
            normalizer=lambda value: _normalize_date(value),
        )
    )

    raw_cusips = _descendant_texts(issuer, "issuerCusipNumber")
    if not raw_cusips:
        raise EvidenceSchemaError("Schedule issuerInfo contains no CUSIP")
    cusips = sorted({_normalize_cusip(value) for value in raw_cusips})
    class_key = normalize_security_class(security_class)
    return [
        {
            "kind": SCHEDULE_13DG,
            "cusip": cusip,
            "issuer_cik": issuer_cik,
            "issuer_name": issuer_name,
            "security_class": security_class,
            "security_class_key": class_key,
            "filing_type": submission_type,
            "accession": normalized_accession,
            "url": normalized_url,
            "as_of": as_of,
        }
        for cusip in cusips
    ]


def _attribute(element: etree._Element, local_name: str) -> str:
    expected = local_name.casefold()
    for name, value in element.attrib.items():
        if _local_name(name).casefold() == expected:
            return _normalized_text(value)
    return ""


def _parse_context(context: etree._Element) -> dict[str, Any]:
    identifiers = []
    periods = []
    dimensions = []
    for element in context.iter():
        local = _local_name(element.tag).casefold()
        namespace = _namespace(element.tag)
        if namespace == _XBRLI_NAMESPACE and local == "identifier":
            scheme = _attribute(element, "scheme").casefold()
            if scheme not in _SEC_CIK_SCHEMES:
                raise EvidenceParseError("XBRL context uses a non-SEC CIK scheme")
            identifiers.append(_normalize_cik("".join(element.itertext())))
        elif namespace == _XBRLI_NAMESPACE and local in {"enddate", "instant"}:
            periods.append(_normalize_date("".join(element.itertext())))
        elif namespace == _XBRLDI_NAMESPACE and local == "explicitmember":
            dimension = _attribute(element, "dimension")
            member = _normalized_text("".join(element.itertext()))
            if not dimension or not member:
                raise EvidenceParseError("malformed XBRL explicit dimension")
            dimensions.append({"dimension": dimension, "member": member})
    if len(set(identifiers)) != 1 or len(set(periods)) != 1:
        raise EvidenceParseError("XBRL context has ambiguous entity or period")
    dimensions.sort(key=lambda item: (item["dimension"], item["member"]))
    return {
        "issuer_cik": identifiers[0],
        "as_of": periods[0],
        "dimensions": dimensions,
    }


def _dei_concept(element: etree._Element) -> str | None:
    raw_name = _attribute(element, "name")
    if ":" not in raw_name:
        return None
    prefix, local = raw_name.split(":", 1)
    namespace = element.nsmap.get(prefix)
    if not namespace:
        return None
    parsed = urlsplit(namespace)
    if (parsed.hostname or "").casefold() != "xbrl.sec.gov":
        return None
    if not parsed.path.casefold().startswith("/dei/"):
        return None
    return local


def _inline_fragment_text(element: etree._Element) -> str:
    pieces = []

    def visit(node: etree._Element) -> None:
        if node.text:
            pieces.append(node.text)
        for child in node:
            if not (
                _namespace(child.tag) == _IX_NAMESPACE
                and _local_name(child.tag).casefold() == "exclude"
            ):
                visit(child)
            if child.tail:
                pieces.append(child.tail)

    visit(element)
    return _normalized_text("".join(pieces))


def _inline_fact_text(
    fact: etree._Element,
    continuations: Mapping[str, etree._Element],
) -> str:
    pieces = [_inline_fragment_text(fact)]
    continued_at = _attribute(fact, "continuedAt")
    visited = set()
    while continued_at:
        if continued_at in visited or continued_at not in continuations:
            raise EvidenceParseError("invalid inline-XBRL continuation chain")
        visited.add(continued_at)
        continuation = continuations[continued_at]
        pieces.append(_inline_fragment_text(continuation))
        continued_at = _attribute(continuation, "continuedAt")
    return _normalized_text(" ".join(filter(None, pieces)))


def _unique_normalized(
    values: Iterable[str],
    normalizer: Callable[[str], str | None],
) -> tuple[str | None, bool]:
    raw_values = list(values)
    normalized_values = [normalizer(item) for item in raw_values]
    if any(value is None for value in normalized_values):
        return None, bool(raw_values)
    normalized = {value for value in normalized_values if value}
    if len(normalized) != 1:
        return None, bool(normalized)
    return next(iter(normalized)), False


def _normalize_symbol(value: str) -> str | None:
    symbol = _normalized_text(value).upper()
    return symbol if _SYMBOL_RE.fullmatch(symbol) else None


def _normalize_exchange(value: str) -> str | None:
    text = _normalized_text(value)
    if not text:
        return None
    direct = _EXCHANGE_CODES.get(text.casefold())
    if direct:
        return direct
    return _EXCHANGE_NAMES.get(text.casefold())


def _truthy_inline_flag(value: str) -> bool:
    return _normalized_text(value).casefold() in {"1", "true", "yes", "y"}


def parse_periodic_ixbrl(
    payload: str | bytes,
    *,
    accession: str,
    source_url: str,
) -> list[dict[str, Any]]:
    """Parse same-context class/symbol/exchange evidence from periodic iXBRL."""

    normalized_accession = _normalize_accession(accession)
    normalized_url = normalize_sec_filing_url(
        source_url,
        accession=normalized_accession,
    )
    root = _xml_root(payload, document_name="periodic inline XBRL")

    contexts: dict[str, dict[str, Any]] = {}
    continuations: dict[str, etree._Element] = {}
    inline_facts = []
    for element in root.iter():
        namespace = _namespace(element.tag)
        local = _local_name(element.tag).casefold()
        if namespace == _XBRLI_NAMESPACE and local == "context":
            context_id = _attribute(element, "id")
            if not context_id:
                raise EvidenceParseError("XBRL context is missing id")
            parsed_context = _parse_context(element)
            if context_id in contexts and contexts[context_id] != parsed_context:
                raise EvidenceParseError(f"conflicting XBRL context id: {context_id}")
            contexts[context_id] = parsed_context
        elif namespace == _IX_NAMESPACE and local == "continuation":
            continuation_id = _attribute(element, "id")
            if not continuation_id or continuation_id in continuations:
                raise EvidenceParseError("invalid inline-XBRL continuation id")
            continuations[continuation_id] = element
        elif namespace == _IX_NAMESPACE and local == "nonnumeric":
            inline_facts.append(element)
    if not contexts or not inline_facts:
        raise EvidenceSchemaError(
            "document contains no inline-XBRL facts or contexts"
        )

    facts_by_context: dict[str, dict[str, list[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for fact in inline_facts:
        concept = _dei_concept(fact)
        if concept is None:
            continue
        context_id = _attribute(fact, "contextRef")
        if context_id not in contexts:
            raise EvidenceParseError(
                f"DEI fact references missing XBRL context: {context_id!r}"
            )
        value = _inline_fact_text(fact, continuations)
        if value:
            facts_by_context[context_id][concept.casefold()].append(value)

    cik_values = set()
    form_values = set()
    document_periods = set()
    for context_id, concepts in facts_by_context.items():
        for value in concepts.get("entitycentralindexkey", []):
            cik_values.add(_normalize_cik(value))
        for value in concepts.get("documenttype", []):
            form_values.add(_normalized_text(value).upper())
        if concepts.get("documentperiodenddate"):
            document_periods.add(contexts[context_id]["as_of"])
    if len(cik_values) != 1:
        raise EvidenceSchemaError(
            "periodic filing has ambiguous or missing issuer CIK"
        )
    if len(form_values) != 1 or next(iter(form_values)) not in _PERIODIC_FORMS:
        raise EvidenceSchemaError("document is not a supported periodic filing")
    if len(document_periods) != 1:
        raise EvidenceSchemaError(
            "periodic filing has ambiguous or missing period end"
        )
    issuer_cik = next(iter(cik_values))
    filing_type = next(iter(form_values))
    document_as_of = next(iter(document_periods))

    records = []
    for context_id in sorted(facts_by_context):
        concepts = facts_by_context[context_id]
        title_12b = concepts.get("security12btitle", [])
        title_12g = concepts.get("security12gtitle", [])
        if bool(title_12b) == bool(title_12g):
            continue
        title_values = title_12b or title_12g
        class_keys = {normalize_security_class(value) for value in title_values}
        class_keys.discard("")
        if len(class_keys) != 1:
            continue
        class_key = next(iter(class_keys))
        class_displays = sorted(
            {_normalized_text(value) for value in title_values},
            key=lambda value: (value.casefold(), value),
        )
        symbol, symbol_conflict = _unique_normalized(
            concepts.get("tradingsymbol", []),
            _normalize_symbol,
        )
        exchange, exchange_conflict = _unique_normalized(
            concepts.get("securityexchangename", []),
            _normalize_exchange,
        )
        no_symbol = any(
            _truthy_inline_flag(value)
            for value in concepts.get("notradingsymbolflag", [])
        )
        context = contexts[context_id]
        if (
            not symbol
            or symbol_conflict
            or not exchange
            or exchange == "NONE"
            or exchange_conflict
            or no_symbol
            or context["issuer_cik"] != issuer_cik
            or context["as_of"] != document_as_of
        ):
            continue
        records.append(
            {
                "kind": PERIODIC_IXBRL,
                "issuer_cik": issuer_cik,
                "security_class": class_displays[0],
                "security_class_key": class_key,
                "ticker": symbol,
                "exchange": exchange,
                "registration": "12b" if title_12b else "12g",
                "context_id": context_id,
                "dimensions": context["dimensions"],
                "filing_type": filing_type,
                "accession": normalized_accession,
                "url": normalized_url,
                "as_of": document_as_of,
            }
        )
    return records


def _source_sort_key(record: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(record.get("as_of") or ""),
        str(record.get("accession") or ""),
        str(record.get("url") or ""),
        str(record.get("context_id") or ""),
    )


def _resolve_one_schedule_identity(
    schedule: Mapping[str, Any],
    ixbrl_records: Iterable[Mapping[str, Any]],
) -> dict[str, Any] | None:
    key = (
        str(schedule.get("issuer_cik") or ""),
        str(schedule.get("security_class_key") or ""),
    )
    compatible = [
        record
        for record in ixbrl_records
        if (
            str(record.get("issuer_cik") or ""),
            str(record.get("security_class_key") or ""),
        )
        == key
    ]
    if not compatible:
        return None
    latest_as_of = max(str(record.get("as_of") or "") for record in compatible)
    latest = [
        record
        for record in compatible
        if str(record.get("as_of") or "") == latest_as_of
    ]
    tickers = {str(record.get("ticker") or "") for record in latest}
    if "" in tickers or len(tickers) != 1:
        return None
    ticker = next(iter(tickers))
    exchanges = sorted({str(record.get("exchange") or "") for record in latest})
    if "" in exchanges:
        return None
    selected_ixbrl = max(latest, key=_source_sort_key)
    return {
        "cusip": schedule["cusip"],
        "issuer_cik": schedule["issuer_cik"],
        "issuer_name": schedule["issuer_name"],
        "security_class": schedule["security_class"],
        "ticker": ticker,
        "exchange": exchanges[0] if len(exchanges) == 1 else None,
        "exchanges": exchanges,
        "mapping_status": "resolved",
        "cusip_source": "sec_schedule_13dg",
        "ticker_source": "sec_ixbrl",
        "ticker_as_of": latest_as_of,
        "schedule_13dg_accession": schedule["accession"],
        "schedule_13dg_url": schedule["url"],
        "schedule_13dg_as_of": schedule["as_of"],
        "ixbrl_accession": selected_ixbrl["accession"],
        "ixbrl_url": selected_ixbrl["url"],
        "ixbrl_as_of": selected_ixbrl["as_of"],
        "ixbrl_context_ids": sorted(
            {str(record["context_id"]) for record in latest if record.get("context_id")}
        ),
    }


def bridge_sec_evidence(
    schedule_records: Iterable[Mapping[str, Any]],
    ixbrl_records: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return only CUSIPs with one exact, unambiguous SEC class bridge."""

    schedules_by_cusip: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for record in schedule_records:
        cusip = str(record.get("cusip") or "")
        if cusip:
            schedules_by_cusip[cusip].append(record)
    ixbrl = list(ixbrl_records)
    resolved = []
    for cusip in sorted(schedules_by_cusip):
        schedules = schedules_by_cusip[cusip]
        identities = {
            (
                str(record.get("issuer_cik") or ""),
                str(record.get("security_class_key") or ""),
            )
            for record in schedules
        }
        if len(identities) != 1:
            continue
        selected_schedule = max(schedules, key=_source_sort_key)
        record = _resolve_one_schedule_identity(selected_schedule, ixbrl)
        if record is not None:
            resolved.append(record)
    return resolved


def _fsync_cache_directory(path: Path) -> None:
    try:
        directory_fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    with atomic_text_output(path, sync_parent=_fsync_cache_directory) as output:
        json.dump(payload, output, sort_keys=True, indent=2, ensure_ascii=False)
        output.write("\n")


def _utc_timestamp(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc).replace(microsecond=0)
    return current.isoformat().replace("+00:00", "Z")


def _normalized_source(
    source: FilingSource | Mapping[str, object],
) -> FilingSource:
    if isinstance(source, FilingSource):
        kind = source.kind
        url = source.url
        accession = source.accession
    elif isinstance(source, Mapping):
        kind = str(source.get("kind") or "")
        url = str(source.get("url") or "")
        accession = str(source.get("accession") or "")
    else:
        raise SecEdgarEvidenceError("filing source must be a mapping or FilingSource")
    normalized_kind = kind.strip().casefold()
    if normalized_kind not in _SOURCE_KINDS:
        raise SecEdgarEvidenceError(f"unsupported SEC evidence kind: {kind!r}")
    normalized_accession = _normalize_accession(accession)
    normalized_url = normalize_sec_filing_url(
        url,
        accession=normalized_accession,
    )
    return FilingSource(normalized_kind, normalized_url, normalized_accession)


def make_sec_filing_fetcher(
    user_agent: str | None = None,
    *,
    timeout: float = 30.0,
    session: requests.Session | None = None,
    max_redirects: int = _DEFAULT_MAX_REDIRECTS,
) -> Fetcher:
    """Create a filing fetcher that validates every URL before requesting it."""

    agent = str(user_agent or os.environ.get("SEC_USER_AGENT") or "").strip()
    if "@" not in agent:
        raise SecEdgarEvidenceError(
            "SEC_USER_AGENT with a contact email is required for SEC downloads"
        )
    if type(max_redirects) is not int or not 0 <= max_redirects <= 10:
        raise SecEdgarEvidenceError(
            "SEC filing max_redirects must be between zero and ten"
        )
    http = session or requests.Session()

    def fetch(url: str) -> bytes:
        requested = normalize_sec_filing_url(url)
        requested_accession = _ARCHIVE_PATH_RE.fullmatch(
            urlsplit(requested).path
        ).group("accession")
        requested_path = urlsplit(requested).path

        def check_redirect(candidate: str) -> None:
            redirect_match = _ARCHIVE_PATH_RE.fullmatch(urlsplit(candidate).path)
            if (
                redirect_match is None
                or redirect_match.group("accession") != requested_accession
                or urlsplit(candidate).path != requested_path
            ):
                raise NonSECFilingURL(
                    "SEC response redirected to another filing document"
                )

        # Admitted redirects retain this document's path. Requiring every
        # response URL to equal its request also binds it to that same scope.
        response = get_sec_response(
            http, requested,
            headers={"User-Agent": agent, "Accept-Encoding": "gzip, deflate"},
            timeout=timeout, max_redirects=max_redirects,
            policy=RedirectPolicy(
                normalize_url=normalize_sec_filing_url,
                error_type=NonSECFilingURL,
                limit_message="SEC filing redirect limit exceeded",
                missing_location_message="SEC filing redirect response has no Location header",
                changed_response_message="SEC filing response URL changed without an approved redirect",
                unsupported_status_message="unsupported SEC filing redirect response",
                check_redirect=check_redirect,
            ),
        )
        response.raise_for_status()
        return bytes(response.content)

    return fetch


def make_sec_discovery_fetcher(
    user_agent: str | None = None,
    *,
    timeout: float = 30.0,
    session: requests.Session | None = None,
    max_attempts: int = 3,
    backoff_seconds: float = 0.5,
    max_requests_per_second: float = 8.0,
    pace: Callable[[], None] | None = None,
    max_redirects: int = _DEFAULT_MAX_REDIRECTS,
) -> Fetcher:
    """Create a rate-bounded fetcher for only the SEC surfaces we use.

    One fetcher instance serializes request start slots to at most eight per
    second, including retries.  A caller coordinating multiple instances may
    instead provide one shared ``pace`` hook, which is called before every
    physical request, including redirect hops.
    """

    agent = str(user_agent or os.environ.get("SEC_USER_AGENT") or "").strip()
    if "@" not in agent:
        raise SecEdgarEvidenceError(
            "SEC_USER_AGENT with a contact email is required for SEC downloads"
        )
    if type(timeout) not in {int, float} or timeout <= 0:
        raise SecEdgarEvidenceError("SEC discovery timeout must be positive")
    if type(max_attempts) is not int or not 1 <= max_attempts <= 5:
        raise SecEdgarEvidenceError("SEC discovery max_attempts must be 1 through 5")
    if type(backoff_seconds) not in {int, float} or not 0 <= backoff_seconds <= 10:
        raise SecEdgarEvidenceError(
            "SEC discovery backoff_seconds must be between 0 and 10"
        )
    if (
        type(max_requests_per_second) not in {int, float}
        or not 0 < max_requests_per_second <= 8
    ):
        raise SecEdgarEvidenceError(
            "SEC discovery rate must be greater than zero and at most 8 req/s"
        )
    if pace is not None and not callable(pace):
        raise SecEdgarEvidenceError("shared SEC discovery pace hook must be callable")
    if type(max_redirects) is not int or not 0 <= max_redirects <= 10:
        raise SecEdgarEvidenceError(
            "SEC discovery max_redirects must be between zero and ten"
        )
    http = session or requests.Session()
    before_request = pace if pace is not None else make_rate_pacer(
        max_requests_per_second, clock=time, lock=threading.Lock(),
    )

    def retry_delay(attempt_number: int) -> None:
        delay = min(8.0, float(backoff_seconds) * (2 ** (attempt_number - 1)))
        if delay > 0:
            time.sleep(delay)

    def fetch(url: str) -> bytes:
        requested = normalize_sec_discovery_url(url)
        requested_host = (urlsplit(requested).hostname or "").casefold()
        requested_match = _ARCHIVE_PATH_RE.fullmatch(urlsplit(requested).path)
        requested_path = urlsplit(requested).path

        def validate_scope(candidate: str) -> str:
            normalized = normalize_sec_discovery_url(candidate)
            if requested_host in {"efts.sec.gov", "data.sec.gov"}:
                if normalized != requested:
                    raise NonSECFilingURL(
                        "SEC discovery response redirected unexpectedly"
                    )
                return normalized
            candidate_match = _ARCHIVE_PATH_RE.fullmatch(
                urlsplit(normalized).path
            )
            if (
                requested_match is None
                or candidate_match is None
                or candidate_match.group("accession")
                != requested_match.group("accession")
                or urlsplit(normalized).path != requested_path
            ):
                raise NonSECFilingURL(
                    "SEC filing response redirected to another document"
                )
            return normalized

        redirect_policy = RedirectPolicy(
            normalize_url=validate_scope,
            error_type=NonSECFilingURL,
            limit_message="SEC discovery redirect limit exceeded",
            missing_location_message="SEC redirect response has no Location header",
            changed_response_message="SEC response URL changed without an approved redirect",
            unsupported_status_message="unsupported SEC discovery redirect response",
        )
        for attempt_number in range(1, max_attempts + 1):
            try:
                response = get_sec_response(
                    http, requested,
                    headers={
                        "User-Agent": agent,
                        "Accept-Encoding": "gzip, deflate",
                        "Accept": "application/json, application/xml, text/html",
                    },
                    timeout=timeout, max_redirects=max_redirects,
                    policy=redirect_policy, pace=before_request,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                response_for_error = getattr(exc, "response", None)
                status_code = getattr(response_for_error, "status_code", None)
                retryable = (
                    status_code in {429, 500, 502, 503, 504} or status_code is None
                )
                if not retryable or attempt_number == max_attempts:
                    raise
                retry_delay(attempt_number)
                continue

            return bytes(response.content)
        raise AssertionError("bounded SEC discovery retry loop exhausted")

    return fetch


class _DiscoveryFetchAudit:
    def __init__(
        self,
        fetcher: Fetcher,
        pace: Callable[[], None] | None,
    ) -> None:
        self._fetcher = fetcher
        self._pace = pace
        self._payloads: dict[str, bytes | None] = {}
        self._records: dict[str, DiscoveryFetch] = {}
        self._kinds: dict[str, str] = {}

    def fetch(self, kind: str, url: str) -> bytes | None:
        canonical_url = normalize_sec_discovery_url(url)
        existing_kind = self._kinds.get(canonical_url)
        if existing_kind is not None and existing_kind != kind:
            raise SecEdgarEvidenceError(
                "one SEC discovery URL was assigned conflicting source kinds"
            )
        if canonical_url in self._payloads:
            return self._payloads[canonical_url]
        self._kinds[canonical_url] = kind
        try:
            if self._pace is not None:
                self._pace()
            payload = self._fetcher(canonical_url)
            if not isinstance(payload, (bytes, bytearray)):
                raise TypeError("SEC discovery fetcher must return bytes")
            raw = bytes(payload)
        except Exception:
            self._payloads[canonical_url] = None
            self._records[canonical_url] = DiscoveryFetch(
                kind=kind,
                url=canonical_url,
                outcome="transient_error",
                sha256=None,
            )
            return None
        self._payloads[canonical_url] = raw
        self._records[canonical_url] = DiscoveryFetch(
            kind=kind,
            url=canonical_url,
            outcome="fetched",
            sha256=hashlib.sha256(raw).hexdigest(),
        )
        return raw

    def records(self) -> tuple[DiscoveryFetch, ...]:
        return tuple(
            sorted(
                self._records.values(),
                key=lambda item: (item.url, item.kind, item.outcome, item.sha256 or ""),
            )
        )


def _schedule_candidate_sources(
    candidate: ScheduleSearchCandidate,
) -> tuple[FilingSource, ...]:
    sources = []
    accession_directory = candidate.accession.replace("-", "")
    for issuer_cik in candidate.archive_ciks[:_MAX_ARCHIVE_CIK_ATTEMPTS]:
        archive_cik = str(int(issuer_cik))
        url = normalize_sec_filing_url(
            "https://www.sec.gov/Archives/edgar/data/"
            f"{archive_cik}/{accession_directory}/{candidate.primary_document}",
            accession=candidate.accession,
        )
        sources.append(FilingSource(SCHEDULE_13DG, url, candidate.accession))
    return tuple(sources)


def discover_sec_edgar_sources(
    cusips: Iterable[object],
    *,
    fetcher: Fetcher,
    max_search_hits: int = 100,
    max_schedule_documents: int = 12,
    max_periodic_documents_per_issuer: int = 3,
    pace: Callable[[], None] | None = None,
) -> DiscoveryResult:
    """Discover complete SEC filing pairs for exact unresolved CUSIPs.

    Search and submissions responses are locators only.  A source pair is
    returned only after the Schedule document re-proves the requested CUSIP
    and a periodic filing proves one compatible same-class trading symbol.
    Retryable fetch or completeness failures never produce filing sources.
    """

    if not callable(fetcher):
        raise SecEdgarEvidenceError("SEC discovery fetcher must be callable")
    if max_search_hits < 1 or max_search_hits > 100:
        raise SecEdgarEvidenceError("max_search_hits must be between 1 and 100")
    if max_schedule_documents < 1 or max_schedule_documents > 100:
        raise SecEdgarEvidenceError("max_schedule_documents must be between 1 and 100")
    if max_periodic_documents_per_issuer < 1 or max_periodic_documents_per_issuer > 20:
        raise SecEdgarEvidenceError(
            "max_periodic_documents_per_issuer must be between 1 and 20"
        )
    if pace is not None and not callable(pace):
        raise SecEdgarEvidenceError("SEC discovery pace hook must be callable")

    raw_cusips = [cusips] if isinstance(cusips, str) else list(cusips)
    normalized_cusips = sorted({_normalize_cusip(value) for value in raw_cusips})
    audit = _DiscoveryFetchAudit(fetcher, pace)
    diagnostics = []
    accepted_sources: set[FilingSource] = set()

    for cusip in normalized_cusips:
        schedule_candidate_count = 0
        exact_schedule_count = 0
        periodic_candidate_count = 0

        def diagnostic(
            status: str,
            terminal: bool,
            reason: str,
            *,
            issuer_cik: str | None = None,
            security_class: str | None = None,
            source_accessions: tuple[str, ...] = (),
        ) -> DiscoveryDiagnostic:
            return DiscoveryDiagnostic(
                cusip=cusip,
                status=status,
                terminal=terminal,
                reason=reason,
                issuer_cik=issuer_cik,
                security_class=security_class,
                schedule_candidate_count=schedule_candidate_count,
                exact_schedule_count=exact_schedule_count,
                periodic_candidate_count=periodic_candidate_count,
                source_accessions=source_accessions,
            )

        search_url = build_sec_cusip_search_url(cusip)
        search_payload = audit.fetch(SEC_CUSIP_SEARCH, search_url)
        if search_payload is None:
            diagnostics.append(
                diagnostic("transient_error", False, "search_fetch_failed")
            )
            continue
        try:
            search_results = parse_sec_schedule_search_results(
                search_payload,
                max_hits=max_search_hits,
            )
        except EvidenceSchemaError:
            raise
        except SecEdgarEvidenceError:
            diagnostics.append(
                diagnostic("transient_error", False, "malformed_search_response")
            )
            continue
        schedule_candidate_count = len(search_results.candidates)
        if search_results.truncated:
            diagnostics.append(
                diagnostic("transient_error", False, "search_results_incomplete")
            )
            continue
        if schedule_candidate_count > max_schedule_documents:
            diagnostics.append(
                diagnostic(
                    "transient_error",
                    False,
                    "schedule_candidate_limit_exceeded",
                )
            )
            continue
        if not search_results.candidates:
            diagnostics.append(
                diagnostic("no_evidence", True, "no_structured_schedule_hits")
            )
            continue

        exact_schedules: list[tuple[dict[str, Any], FilingSource]] = []
        schedule_candidate_failure = False
        for candidate in search_results.candidates:
            parsed_candidate = None
            parsed_source = None
            for source in _schedule_candidate_sources(candidate):
                schedule_payload = audit.fetch(source.kind, source.url)
                if schedule_payload is None:
                    continue
                try:
                    parsed_candidate = parse_schedule_13dg_xml(
                        schedule_payload,
                        accession=source.accession,
                        source_url=source.url,
                    )
                except SecEdgarEvidenceError:
                    continue
                parsed_source = source
                break
            if parsed_candidate is None or parsed_source is None:
                schedule_candidate_failure = True
                continue
            for record in parsed_candidate:
                if record["cusip"] == cusip:
                    exact_schedules.append((record, parsed_source))

        exact_schedule_count = len(exact_schedules)
        if schedule_candidate_failure:
            diagnostics.append(
                diagnostic(
                    "transient_error",
                    False,
                    "schedule_candidate_fetch_or_parse_failed",
                )
            )
            continue
        if not exact_schedules:
            diagnostics.append(
                diagnostic("no_evidence", True, "no_exact_schedule_cusip")
            )
            continue

        identities = {
            (record["issuer_cik"], record["security_class_key"])
            for record, _source in exact_schedules
        }
        if len(identities) != 1:
            diagnostics.append(
                diagnostic("conflict", True, "conflicting_schedule_identities")
            )
            continue
        issuer_cik, security_class_key = next(iter(identities))
        selected_schedule, selected_schedule_source = max(
            exact_schedules,
            key=lambda item: _source_sort_key(item[0]),
        )
        security_class = str(selected_schedule["security_class"])

        submissions_url = build_sec_submissions_url(issuer_cik)
        submissions_payload = audit.fetch(SEC_SUBMISSIONS, submissions_url)
        if submissions_payload is None:
            diagnostics.append(
                diagnostic(
                    "transient_error",
                    False,
                    "submissions_fetch_failed",
                    issuer_cik=issuer_cik,
                    security_class=security_class,
                )
            )
            continue
        try:
            periodic_candidates = _parse_periodic_candidates(
                submissions_payload,
                issuer_cik=issuer_cik,
            )
        except EvidenceSchemaError:
            raise
        except SecEdgarEvidenceError:
            diagnostics.append(
                diagnostic(
                    "transient_error",
                    False,
                    "malformed_submissions_response",
                    issuer_cik=issuer_cik,
                    security_class=security_class,
                )
            )
            continue
        periodic_candidate_count = len(periodic_candidates)
        if not periodic_candidates:
            diagnostics.append(
                diagnostic(
                    "no_evidence",
                    True,
                    "no_periodic_ixbrl_candidates",
                    issuer_cik=issuer_cik,
                    security_class=security_class,
                )
            )
            continue

        periodic_records = []
        periodic_sources_by_url = {}
        periodic_fetch_failure = False
        for candidate in periodic_candidates[:max_periodic_documents_per_issuer]:
            source = candidate.source
            periodic_payload = audit.fetch(source.kind, source.url)
            if periodic_payload is None:
                periodic_fetch_failure = True
                continue
            try:
                parsed_periodic = parse_periodic_ixbrl(
                    periodic_payload,
                    accession=source.accession,
                    source_url=source.url,
                )
            except SecEdgarEvidenceError:
                periodic_fetch_failure = True
                continue
            periodic_records.extend(parsed_periodic)
            periodic_sources_by_url[source.url] = source
        if periodic_fetch_failure:
            diagnostics.append(
                diagnostic(
                    "transient_error",
                    False,
                    "periodic_candidate_fetch_or_parse_failed",
                    issuer_cik=issuer_cik,
                    security_class=security_class,
                )
            )
            continue
        if any(record["issuer_cik"] != issuer_cik for record in periodic_records):
            diagnostics.append(
                diagnostic(
                    "conflict",
                    True,
                    "periodic_issuer_conflict",
                    issuer_cik=issuer_cik,
                    security_class=security_class,
                )
            )
            continue

        compatible = [
            record
            for record in periodic_records
            if record["issuer_cik"] == issuer_cik
            and record["security_class_key"] == security_class_key
        ]
        if not compatible:
            diagnostics.append(
                diagnostic(
                    "no_evidence",
                    True,
                    "no_compatible_class_in_latest_periodic_filings",
                    issuer_cik=issuer_cik,
                    security_class=security_class,
                )
            )
            continue
        latest_as_of = max(record["as_of"] for record in compatible)
        latest_compatible = [
            record for record in compatible if record["as_of"] == latest_as_of
        ]
        latest_tickers = {record["ticker"] for record in latest_compatible}
        if len(latest_tickers) != 1:
            diagnostics.append(
                diagnostic(
                    "conflict",
                    True,
                    "conflicting_latest_periodic_tickers",
                    issuer_cik=issuer_cik,
                    security_class=security_class,
                )
            )
            continue

        bridged = bridge_sec_evidence([selected_schedule], periodic_records)
        if len(bridged) != 1:
            diagnostics.append(
                diagnostic(
                    "conflict",
                    True,
                    "inconsistent_exact_sec_evidence_bridge",
                    issuer_cik=issuer_cik,
                    security_class=security_class,
                )
            )
            continue
        resolved = bridged[0]
        periodic_source = periodic_sources_by_url.get(resolved["ixbrl_url"])
        if periodic_source is None:
            diagnostics.append(
                diagnostic(
                    "transient_error",
                    False,
                    "missing_periodic_source_provenance",
                    issuer_cik=issuer_cik,
                    security_class=security_class,
                )
            )
            continue
        accepted_sources.update({selected_schedule_source, periodic_source})
        diagnostics.append(
            diagnostic(
                "sources_found",
                True,
                "exact_schedule_cusip_and_ixbrl_class_bridge",
                issuer_cik=issuer_cik,
                security_class=security_class,
                source_accessions=tuple(
                    sorted(
                        {
                            selected_schedule_source.accession,
                            periodic_source.accession,
                        }
                    )
                ),
            )
        )

    return DiscoveryResult(
        sources=tuple(
            sorted(
                accepted_sources,
                key=lambda source: (source.kind, source.url, source.accession),
            )
        ),
        diagnostics=tuple(sorted(diagnostics, key=lambda item: item.cusip)),
        fetched_sources=audit.records(),
    )


def refresh_sec_edgar_evidence(
    sources: Iterable[FilingSource | Mapping[str, object]],
    *,
    cache_path: Path | None = DEFAULT_CACHE_PATH,
    fetcher: Fetcher | None = None,
    user_agent: str | None = None,
    refreshed_at: datetime | None = None,
) -> dict[str, Any]:
    """Rebuild the private cache from explicit SEC URLs and accessions.

    The refresh is all-or-nothing.  Any fetch or parse failure raises and
    leaves an existing cache untouched.  Pass ``cache_path=None`` to return
    the validated cache without creating a third durable state file.
    """

    normalized_sources = sorted(
        {_normalized_source(source) for source in sources},
        key=lambda source: (source.kind, source.url, source.accession),
    )
    if not normalized_sources:
        raise EvidenceRefreshError("at least one explicit SEC filing is required")
    by_url: dict[str, FilingSource] = {}
    for source in normalized_sources:
        existing = by_url.get(source.url)
        if existing is not None and existing != source:
            raise EvidenceRefreshError("one SEC URL was assigned conflicting metadata")
        by_url[source.url] = source

    active_fetcher = fetcher or make_sec_filing_fetcher(user_agent)
    schedule_records = []
    ixbrl_records = []
    accepted_sources = []
    for source in normalized_sources:
        try:
            payload = active_fetcher(source.url)
            if not isinstance(payload, (bytes, bytearray)):
                raise EvidenceRefreshError("SEC fetcher must return bytes")
            raw = bytes(payload)
            if source.kind == SCHEDULE_13DG:
                parsed = parse_schedule_13dg_xml(
                    raw,
                    accession=source.accession,
                    source_url=source.url,
                )
                schedule_records.extend(parsed)
            else:
                parsed = parse_periodic_ixbrl(
                    raw,
                    accession=source.accession,
                    source_url=source.url,
                )
                if not parsed:
                    raise EvidenceParseError(
                        "periodic filing contains no complete class/symbol/exchange proof"
                    )
                ixbrl_records.extend(parsed)
            accepted_sources.append(
                {
                    "kind": source.kind,
                    "url": source.url,
                    "accession": source.accession,
                    "as_of": parsed[0]["as_of"],
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "record_count": len(parsed),
                }
            )
        except Exception as exc:
            if isinstance(exc, EvidenceRefreshError):
                raise
            raise EvidenceRefreshError(
                f"failed SEC evidence source {source.url}: {exc}"
            ) from exc

    cache = _build_sec_edgar_evidence_cache(
        schedule_records,
        ixbrl_records,
        accepted_sources,
        refreshed_at=refreshed_at,
    )
    if cache_path is not None:
        try:
            _atomic_write_json(Path(cache_path), cache)
        except Exception as exc:
            raise EvidenceRefreshError(
                f"could not atomically persist SEC evidence cache {cache_path}"
            ) from exc
    return cache


def _deduplicated_evidence_records(
    records: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_payload = {
        json.dumps(record, sort_keys=True, separators=(",", ":")): copy.deepcopy(
            dict(record)
        )
        for record in records
    }
    return list(by_payload.values())


def _build_sec_edgar_evidence_cache(
    schedule_records: Iterable[Mapping[str, Any]],
    ixbrl_records: Iterable[Mapping[str, Any]],
    accepted_sources: Iterable[Mapping[str, Any]],
    *,
    refreshed_at: datetime | None = None,
) -> dict[str, Any]:
    """Build one deterministic cache from already-validated SEC documents."""

    normalized_schedule = _deduplicated_evidence_records(schedule_records)
    normalized_ixbrl = _deduplicated_evidence_records(ixbrl_records)
    sources_by_url: dict[str, dict[str, Any]] = {}
    for raw_source in accepted_sources:
        source = copy.deepcopy(dict(raw_source))
        url = str(source.get("url") or "")
        prior = sources_by_url.get(url)
        if prior is not None and prior != source:
            raise EvidenceRefreshError(
                "one SEC evidence URL has conflicting accepted provenance"
            )
        sources_by_url[url] = source
    normalized_sources = sorted(
        sources_by_url.values(),
        key=lambda source: (
            str(source.get("kind") or ""),
            str(source.get("url") or ""),
            str(source.get("accession") or ""),
        ),
    )

    resolved = bridge_sec_evidence(normalized_schedule, normalized_ixbrl)
    resolved_by_cusip = {record["cusip"]: record for record in resolved}
    schedule_cusips = sorted({record["cusip"] for record in normalized_schedule})
    unresolved = {
        cusip: {
            "cusip": cusip,
            "mapping_status": "unresolved",
            "ticker": None,
            "resolution_reason": "no_exact_unambiguous_sec_class_bridge",
        }
        for cusip in schedule_cusips
        if cusip not in resolved_by_cusip
    }
    cache = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "generated_at": _utc_timestamp(refreshed_at),
        "sources": normalized_sources,
        "schedule_evidence": sorted(
            normalized_schedule,
            key=lambda record: (record["cusip"],) + _source_sort_key(record),
        ),
        "ixbrl_evidence": sorted(
            normalized_ixbrl,
            key=lambda record: (
                (
                    record["issuer_cik"],
                    record["security_class_key"],
                )
                + _source_sort_key(record)
            ),
        ),
        "records": {
            cusip: resolved_by_cusip[cusip] for cusip in sorted(resolved_by_cusip)
        },
        "unresolved": unresolved,
        "summary": {
            "source_count": len(normalized_sources),
            "schedule_record_count": len(normalized_schedule),
            "ixbrl_record_count": len(normalized_ixbrl),
            "resolved_count": len(resolved_by_cusip),
            "unresolved_count": len(unresolved),
        },
    }
    validate_sec_edgar_evidence_cache(cache)
    return cache


def merge_sec_edgar_evidence_caches(
    existing_cache: Mapping[str, Any] | None,
    refreshed_cache: Mapping[str, Any] | None,
    *,
    retired_urls: Iterable[str] = (),
    refreshed_at: datetime | None = None,
) -> dict[str, Any]:
    """Merge newly fetched filing evidence without refetching prior sources.

    ``retired_urls`` withdraws superseded documents before the bridge is
    rebuilt. A URL present in ``refreshed_cache`` also replaces, rather than
    duplicates, its prior document. Empty output uses the source-state's
    intentional ``{}`` sentinel.
    """

    existing = dict(existing_cache or {})
    refreshed = dict(refreshed_cache or {})
    if existing:
        validate_sec_edgar_evidence_cache(existing)
    if refreshed:
        validate_sec_edgar_evidence_cache(refreshed)

    replacement_urls = {
        str(source.get("url") or "")
        for source in refreshed.get("sources", [])
        if isinstance(source, Mapping)
    }
    withdrawn = {str(url) for url in retired_urls if str(url)} | replacement_urls

    old_sources = [
        source
        for source in existing.get("sources", [])
        if isinstance(source, Mapping)
        and str(source.get("url") or "") not in withdrawn
    ]
    old_schedule = [
        record
        for record in existing.get("schedule_evidence", [])
        if isinstance(record, Mapping)
        and str(record.get("url") or "") not in withdrawn
    ]
    old_ixbrl = [
        record
        for record in existing.get("ixbrl_evidence", [])
        if isinstance(record, Mapping)
        and str(record.get("url") or "") not in withdrawn
    ]
    combined_sources = [*old_sources, *refreshed.get("sources", [])]
    if not combined_sources:
        return {}
    return _build_sec_edgar_evidence_cache(
        [*old_schedule, *refreshed.get("schedule_evidence", [])],
        [*old_ixbrl, *refreshed.get("ixbrl_evidence", [])],
        combined_sources,
        refreshed_at=refreshed_at,
    )


_MASTER_ELIGIBLE_TYPES = frozenset({"EQUITY", "PREF", "WARRANT"})
_MASTER_APPLICABLE_STATUSES = frozenset({"unresolved", "ambiguous"})
_MASTER_STATUS_VALUES = frozenset(
    {
        "resolved",
        "unresolved",
        "ambiguous",
        "no_listed_symbol",
        "malformed_as_filed",
    }
)


def _issuer_proof_key(value: object | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", _normalized_text(value).casefold())


def _issuer_names_compatible(left: object | None, right: object | None) -> bool:
    left_key = _issuer_proof_key(left)
    right_key = _issuer_proof_key(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    shorter, longer = sorted((left_key, right_key), key=len)
    return len(shorter) >= 10 and longer.startswith(shorter)


def _class_profile(value: object | None) -> tuple[str | None, frozenset[str]]:
    normalized = re.sub(
        r"[^A-Z0-9]+",
        " ",
        _normalized_text(value).upper(),
    ).strip()
    tokens = set(normalized.split())
    family = None
    if tokens & {"PREF", "PFD", "PREFERRED"}:
        family = "PREF"
    elif tokens & {"WARRANT", "WARRANTS", "WT", "WTS"}:
        family = "WARRANT"
    elif tokens & {"NOTE", "NOTES", "BOND", "BONDS", "DEBENTURE", "DEBENTURES"}:
        family = "DEBT"
    elif tokens & {"CALL", "PUT", "OPTION", "OPTIONS"}:
        family = "OPTION"
    elif tokens & {"COM", "COMMON", "ORD", "ORDINARY"} or (
        "CLASS" in tokens and tokens & {"SHARE", "SHARES", "STOCK"}
    ) or tokens & {"ADR", "ADRS", "ADS", "GDR", "GDRS"} or re.search(
        r"\b(?:AMERICAN|GLOBAL)\s+DEPOSITARY\s+(?:SHARE|SHARES|RECEIPT|RECEIPTS)\b",
        normalized,
    ):
        family = "EQUITY"
    designators = frozenset(
        match.group(1)
        for match in re.finditer(r"\b(?:CLASS|CL)\s+([A-Z0-9]+)\b", normalized)
    )
    return family, designators


def _master_identity_conflict(
    master_record: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> str | None:
    instrument_type = str(master_record.get("instrument_type") or "").upper()
    candidate_family, candidate_designators = _class_profile(
        candidate.get("security_class")
    )
    if candidate_family != instrument_type:
        return "sec_edgar_security_class_instrument_type_conflict"

    master_classes = []
    for field in ("reported_class", "security_class", "class"):
        if _normalized_text(master_record.get(field)):
            master_classes.append(master_record[field])
    raw_classes = master_record.get("reported_classes")
    if isinstance(raw_classes, list):
        master_classes.extend(raw_classes)
    for master_class in master_classes:
        master_family, master_designators = _class_profile(master_class)
        if master_family and master_family != candidate_family:
            return "sec_edgar_security_class_conflict"
        if (
            master_designators
            and candidate_designators
            and master_designators != candidate_designators
        ):
            return "sec_edgar_security_class_conflict"

    candidate_cik = str(candidate.get("issuer_cik") or "")
    master_cik = str(master_record.get("issuer_cik") or "")
    if master_cik and master_cik.zfill(10) != candidate_cik:
        return "sec_edgar_issuer_cik_conflict"
    candidate_name = candidate.get("issuer_name")
    master_issuers = []
    for field in ("reported_issuer", "issuer"):
        if _normalized_text(master_record.get(field)):
            master_issuers.append(master_record[field])
    raw_issuers = master_record.get("reported_issuers")
    if isinstance(raw_issuers, list):
        master_issuers.extend(raw_issuers)
    if master_issuers and any(
        not _issuer_names_compatible(master_issuer, candidate_name)
        for master_issuer in master_issuers
    ):
        return "sec_edgar_issuer_name_conflict"
    return None


def _cache_conflicts(
    schedule_records: list[Mapping[str, Any]],
    ixbrl_records: list[Mapping[str, Any]],
) -> dict[str, str]:
    schedules_by_cusip: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for record in schedule_records:
        cusip = str(record.get("cusip") or "")
        if cusip:
            schedules_by_cusip[cusip].append(record)
    conflicts = {}
    for cusip, schedules in schedules_by_cusip.items():
        identities = {
            (
                str(record.get("issuer_cik") or ""),
                str(record.get("security_class_key") or ""),
            )
            for record in schedules
        }
        if len(identities) != 1:
            conflicts[cusip] = "conflicting_sec_schedule_13dg_identity"
            continue
        identity = next(iter(identities))
        compatible = [
            record
            for record in ixbrl_records
            if (
                str(record.get("issuer_cik") or ""),
                str(record.get("security_class_key") or ""),
            )
            == identity
        ]
        if not compatible:
            continue
        latest_as_of = max(str(record.get("as_of") or "") for record in compatible)
        latest_tickers = {
            str(record.get("ticker") or "")
            for record in compatible
            if str(record.get("as_of") or "") == latest_as_of
        }
        if "" in latest_tickers or len(latest_tickers) != 1:
            conflicts[cusip] = "conflicting_sec_ixbrl_tickers"
    return conflicts


def _validated_cache_bridge(
    cache: Mapping[str, Any],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, str],
    list[dict[str, str]],
]:
    if cache.get("schema_version") != CACHE_SCHEMA_VERSION:
        raise SecEdgarEvidenceError("unsupported SEC EDGAR evidence cache schema")
    schedule_records = cache.get("schedule_evidence")
    ixbrl_records = cache.get("ixbrl_evidence")
    cached_records = cache.get("records")
    raw_sources = cache.get("sources")
    if (
        not isinstance(schedule_records, list)
        or not all(isinstance(record, Mapping) for record in schedule_records)
        or not isinstance(ixbrl_records, list)
        or not all(isinstance(record, Mapping) for record in ixbrl_records)
        or not isinstance(cached_records, Mapping)
        or not isinstance(raw_sources, list)
    ):
        raise SecEdgarEvidenceError("malformed SEC EDGAR evidence cache")

    conflicts = _cache_conflicts(schedule_records, ixbrl_records)
    rebuilt_records = {
        record["cusip"]: record
        for record in bridge_sec_evidence(schedule_records, ixbrl_records)
    }
    core_fields = (
        "cusip",
        "issuer_cik",
        "issuer_name",
        "security_class",
        "ticker",
        "ticker_source",
        "ticker_as_of",
        "schedule_13dg_accession",
        "schedule_13dg_url",
        "schedule_13dg_as_of",
        "ixbrl_accession",
        "ixbrl_url",
        "ixbrl_as_of",
    )
    for raw_cusip, raw_record in cached_records.items():
        cusip = str(raw_cusip)
        if not isinstance(raw_record, Mapping) or raw_record.get("cusip") != cusip:
            conflicts.setdefault(cusip, "malformed_sec_edgar_cache_record")
            continue
        rebuilt = rebuilt_records.get(cusip)
        if rebuilt is None or any(
            raw_record.get(field) != rebuilt.get(field) for field in core_fields
        ):
            conflicts.setdefault(
                cusip,
                "sec_edgar_cache_record_does_not_match_raw_evidence",
            )
    for cusip in set(rebuilt_records) - set(cached_records):
        conflicts.setdefault(cusip, "sec_edgar_cache_missing_rebuilt_record")
    for cusip in conflicts:
        rebuilt_records.pop(cusip, None)

    sources = []
    source_documents = set()
    sha256_by_url: dict[str, str] = {}
    for raw_source in raw_sources:
        if not isinstance(raw_source, Mapping):
            raise SecEdgarEvidenceError("malformed SEC EDGAR source provenance")
        kind = str(raw_source.get("kind") or "")
        accession = _normalize_accession(raw_source.get("accession"))
        url = normalize_sec_filing_url(
            raw_source.get("url"),
            accession=accession,
        )
        sha256 = str(raw_source.get("sha256") or "")
        if kind not in _SOURCE_KINDS or not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise SecEdgarEvidenceError("invalid SEC EDGAR source provenance")
        prior_sha256 = sha256_by_url.setdefault(url, sha256)
        if prior_sha256 != sha256:
            raise SecEdgarEvidenceError(
                "one SEC EDGAR source URL has conflicting content hashes"
            )
        as_of = _normalize_date(raw_source.get("as_of"))
        source_documents.add((kind, url, accession, as_of))
        sources.append({"url": url, "sha256": sha256, "kind": kind})
    sources.sort(key=lambda item: (item["url"], item["kind"], item["sha256"]))

    for cusip, candidate in list(rebuilt_records.items()):
        try:
            schedule_accession = _normalize_accession(
                candidate.get("schedule_13dg_accession")
            )
            schedule_url = normalize_sec_filing_url(
                candidate.get("schedule_13dg_url"),
                accession=schedule_accession,
            )
            schedule_as_of = _normalize_date(candidate.get("schedule_13dg_as_of"))
            ixbrl_accession = _normalize_accession(candidate.get("ixbrl_accession"))
            ixbrl_url = normalize_sec_filing_url(
                candidate.get("ixbrl_url"),
                accession=ixbrl_accession,
            )
            ixbrl_as_of = _normalize_date(candidate.get("ixbrl_as_of"))
        except SecEdgarEvidenceError:
            conflicts[cusip] = "invalid_sec_edgar_evidence_reference"
            rebuilt_records.pop(cusip, None)
            continue
        expected_sources = {
            (SCHEDULE_13DG, schedule_url, schedule_accession, schedule_as_of),
            (PERIODIC_IXBRL, ixbrl_url, ixbrl_accession, ixbrl_as_of),
        }
        if not expected_sources.issubset(source_documents):
            conflicts[cusip] = "missing_sec_edgar_source_provenance"
            rebuilt_records.pop(cusip, None)
    return rebuilt_records, conflicts, sources


def validate_sec_edgar_evidence_cache(cache: Mapping[str, Any]) -> None:
    """Validate one complete schema-v1 SEC evidence cache without mutation."""

    if not isinstance(cache, Mapping):
        raise SecEdgarEvidenceError("SEC EDGAR evidence cache must be a mapping")
    generated_at = cache.get("generated_at")
    if not isinstance(generated_at, str):
        raise SecEdgarEvidenceError("SEC EDGAR evidence cache has no generated_at")
    try:
        parsed_generated_at = datetime.fromisoformat(
            generated_at.removesuffix("Z") + "+00:00"
        )
    except ValueError as exc:
        raise SecEdgarEvidenceError(
            "SEC EDGAR evidence cache has invalid generated_at"
        ) from exc
    if _utc_timestamp(parsed_generated_at) != generated_at:
        raise SecEdgarEvidenceError(
            "SEC EDGAR evidence cache generated_at must be canonical UTC"
        )

    try:
        _validated_cache_bridge(cache)
    except SecEdgarEvidenceError:
        raise
    except Exception as exc:
        raise SecEdgarEvidenceError("malformed SEC EDGAR evidence cache") from exc

    schedule_records = cache.get("schedule_evidence")
    ixbrl_records = cache.get("ixbrl_evidence")
    cached_records = cache.get("records")
    cached_unresolved = cache.get("unresolved")
    raw_sources = cache.get("sources")
    summary = cache.get("summary")
    if (
        not isinstance(schedule_records, list)
        or not isinstance(ixbrl_records, list)
        or not isinstance(cached_records, Mapping)
        or not isinstance(cached_unresolved, Mapping)
        or not isinstance(raw_sources, list)
        or not isinstance(summary, Mapping)
    ):
        raise SecEdgarEvidenceError("malformed SEC EDGAR evidence cache")

    evidence_document_counts: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for expected_kind, records in (
        (SCHEDULE_13DG, schedule_records),
        (PERIODIC_IXBRL, ixbrl_records),
    ):
        for record in records:
            if not isinstance(record, Mapping) or record.get("kind") != expected_kind:
                raise SecEdgarEvidenceError("malformed SEC EDGAR evidence record")
            accession = _normalize_accession(record.get("accession"))
            url = normalize_sec_filing_url(record.get("url"), accession=accession)
            as_of = _normalize_date(record.get("as_of"))
            evidence_document_counts[(expected_kind, url, accession, as_of)] += 1

    source_document_counts = {}
    for source in raw_sources:
        if not isinstance(source, Mapping):
            raise SecEdgarEvidenceError("malformed SEC EDGAR source provenance")
        kind = str(source.get("kind") or "")
        accession = _normalize_accession(source.get("accession"))
        url = normalize_sec_filing_url(source.get("url"), accession=accession)
        as_of = _normalize_date(source.get("as_of"))
        record_count = source.get("record_count")
        if type(record_count) is not int or record_count < 1:
            raise SecEdgarEvidenceError("invalid SEC EDGAR source record count")
        key = (kind, url, accession, as_of)
        if key in source_document_counts:
            raise SecEdgarEvidenceError("duplicate SEC EDGAR source provenance")
        source_document_counts[key] = record_count
    if source_document_counts != dict(evidence_document_counts):
        raise SecEdgarEvidenceError(
            "SEC EDGAR evidence records do not match source provenance"
        )

    expected_records = {
        record["cusip"]: record
        for record in bridge_sec_evidence(schedule_records, ixbrl_records)
    }
    if dict(cached_records) != expected_records:
        raise SecEdgarEvidenceError(
            "SEC EDGAR cached mappings do not match raw evidence"
        )
    schedule_cusips = sorted(
        {
            str(record.get("cusip") or "")
            for record in schedule_records
            if record.get("cusip")
        }
    )
    expected_unresolved = {
        cusip: {
            "cusip": cusip,
            "mapping_status": "unresolved",
            "ticker": None,
            "resolution_reason": "no_exact_unambiguous_sec_class_bridge",
        }
        for cusip in schedule_cusips
        if cusip not in expected_records
    }
    if dict(cached_unresolved) != expected_unresolved:
        raise SecEdgarEvidenceError(
            "SEC EDGAR unresolved mappings do not match raw evidence"
        )
    expected_summary = {
        "source_count": len(raw_sources),
        "schedule_record_count": len(schedule_records),
        "ixbrl_record_count": len(ixbrl_records),
        "resolved_count": len(expected_records),
        "unresolved_count": len(expected_unresolved),
    }
    if dict(summary) != expected_summary:
        raise SecEdgarEvidenceError("SEC EDGAR evidence cache summary mismatch")


def _sec_evidence_reference(
    candidate: Mapping[str, Any],
    source_sha256: Mapping[str, str],
) -> dict[str, Any]:
    schedule_url = str(candidate["schedule_13dg_url"])
    ixbrl_url = str(candidate["ixbrl_url"])
    return {
        "status": "accepted",
        "issuer_cik": candidate["issuer_cik"],
        "issuer_name": candidate["issuer_name"],
        "security_class": candidate["security_class"],
        "exchange": candidate["exchange"],
        "exchanges": list(candidate["exchanges"]),
        "schedule_13dg": {
            "accession": candidate["schedule_13dg_accession"],
            "url": schedule_url,
            "as_of": candidate["schedule_13dg_as_of"],
            "sha256": source_sha256.get(schedule_url),
        },
        "ixbrl": {
            "accession": candidate["ixbrl_accession"],
            "url": ixbrl_url,
            "as_of": candidate["ixbrl_as_of"],
            "sha256": source_sha256.get(ixbrl_url),
            "context_ids": list(candidate["ixbrl_context_ids"]),
        },
    }


def apply_sec_edgar_evidence(
    master: Mapping[str, Any],
    cache: Mapping[str, Any],
    *,
    successful_checkpoints: Mapping[str, str | None] | None = None,
) -> dict[str, Any]:
    """Purely apply exact EDGAR bridges to eligible non-resolved identities.

    Existing resolved records are never rewritten.  Detectable cache,
    issuer, class, or instrument conflicts become null-ticker ambiguous
    records.  The input mappings are not mutated.
    """

    from sec_security_master import (
        DEFAULT_RECENT_WINDOW_DAYS,
        _reconcile_current_symbol_cusips,
        _resolved_mapping_counts,
        validate_security_master,
    )

    validate_security_master(master)
    candidates, cache_conflicts, new_sources = _validated_cache_bridge(cache)
    updated = copy.deepcopy(dict(master))
    records = updated.get("records")
    if not isinstance(records, dict):
        raise SecEdgarEvidenceError("security master has no records object")
    source_sha256 = {source["url"]: source["sha256"] for source in new_sources}
    audit = updated.get("audit")
    master_as_of_value = (
        audit.get("as_of") if isinstance(audit, Mapping) else None
    ) or str(updated.get("generated_at") or "")[:10]
    master_as_of = (
        _normalize_date(master_as_of_value) if master_as_of_value else None
    )

    for key in sorted(records):
        record = records[key]
        status = str(record.get("mapping_status") or "")
        instrument_type = str(record.get("instrument_type") or "").upper()
        if status == "resolved":
            continue
        if (
            status not in _MASTER_APPLICABLE_STATUSES
            or instrument_type not in _MASTER_ELIGIBLE_TYPES
        ):
            continue
        cusip = str(record.get("cusip") or "")
        conflict = cache_conflicts.get(cusip)
        candidate = candidates.get(cusip)
        if candidate is not None and conflict is None:
            conflict = _master_identity_conflict(record, candidate)
        if conflict:
            record.update(
                {
                    "mapping_status": "ambiguous",
                    "ticker": None,
                    "ticker_source": None,
                    "ticker_as_of": None,
                    "mapping_method": None,
                    "effective_from": None,
                    "effective_to": None,
                    "last_verification_date": None,
                    "resolution_reason": conflict,
                }
            )
            if candidate is not None:
                rejected = _sec_evidence_reference(candidate, source_sha256)
                rejected["status"] = "rejected"
                rejected["reason"] = conflict
                record["sec_edgar_evidence"] = rejected
            continue
        if candidate is None:
            continue
        ticker = _normalize_symbol(str(candidate.get("ticker") or ""))
        ticker_as_of = _normalize_date(candidate.get("ticker_as_of"))
        if ticker is None or candidate.get("ticker_source") != "sec_ixbrl":
            record.update(
                {
                    "mapping_status": "ambiguous",
                    "ticker": None,
                    "ticker_source": None,
                    "ticker_as_of": None,
                    "mapping_method": None,
                    "effective_from": None,
                    "effective_to": None,
                    "last_verification_date": None,
                    "resolution_reason": "invalid_sec_edgar_ticker_provenance",
                }
            )
            continue
        if (
            master_as_of is not None
            and ticker_as_of is not None
            and (
                datetime.fromisoformat(master_as_of).date()
                - datetime.fromisoformat(ticker_as_of).date()
            ).days
            > DEFAULT_MAX_CURRENT_EVIDENCE_AGE_DAYS
        ):
            rejected = _sec_evidence_reference(candidate, source_sha256)
            rejected["status"] = "rejected"
            rejected["reason"] = "sec_edgar_evidence_is_stale"
            record.update(
                {
                    "mapping_status": (
                        "ambiguous" if status == "ambiguous" else "unresolved"
                    ),
                    "ticker": None,
                    "ticker_source": None,
                    "ticker_as_of": None,
                    "mapping_method": None,
                    "effective_from": None,
                    "effective_to": None,
                    "last_verification_date": None,
                    "resolution_reason": "sec_edgar_evidence_is_stale",
                    "sec_edgar_evidence": rejected,
                }
            )
            continue
        record.update(
            {
                "mapping_status": "resolved",
                "ticker": ticker,
                "ticker_source": "sec_ixbrl",
                "ticker_as_of": ticker_as_of,
                "mapping_method": "exact_schedule_13dg_ixbrl_class_bridge",
                "effective_from": ticker_as_of,
                "effective_to": None,
                "last_verification_date": ticker_as_of,
                "resolution_reason": "exact_sec_schedule_13dg_ixbrl_class_bridge",
                "issuer": candidate.get("issuer_name"),
                "security_class": candidate.get("security_class"),
                "exchange": candidate.get("exchange"),
                "exchanges": list(candidate.get("exchanges") or []),
                "sec_edgar_evidence": _sec_evidence_reference(
                    candidate,
                    source_sha256,
                ),
            }
        )

    existing_sources = updated.get("sources")
    if not isinstance(existing_sources, list):
        raise SecEdgarEvidenceError("security master has no sources list")
    source_index = {
        json.dumps(source, sort_keys=True, separators=(",", ":")): source
        for source in [*existing_sources, *new_sources]
    }
    updated["sources"] = sorted(
        source_index.values(),
        key=lambda source: (
            str(source.get("url") or ""),
            str(source.get("kind") or ""),
            str(source.get("sha256") or ""),
        ),
    )
    policy = updated.get("policy")
    recent_window_days = (
        policy.get("recent_window_days")
        if isinstance(policy, Mapping)
        else DEFAULT_RECENT_WINDOW_DAYS
    )
    if type(recent_window_days) is not int or recent_window_days < 0:
        recent_window_days = DEFAULT_RECENT_WINDOW_DAYS
    _reconcile_current_symbol_cusips(
        records,
        concurrent_window_days=recent_window_days,
    )
    summary = {status: 0 for status in sorted(_MASTER_STATUS_VALUES)}
    for record in records.values():
        status = record.get("mapping_status")
        if status in summary:
            summary[status] += 1
    updated["summary"] = summary
    if isinstance(audit, dict) and "sec_ixbrl_source_checkpoints" in audit:
        checkpoints = successful_checkpoints or {}
        audit["sec_ixbrl_source_checkpoints"] = {
            key: checkpoints.get(str(record.get("cusip") or ""))
            for key, record in sorted(records.items())
            if isinstance(record, Mapping)
            and record.get("mapping_status") == "resolved"
            and record.get("ticker_source") == "sec_ixbrl"
        }
    if isinstance(audit, dict) and "resolved_mapping_count_by_ticker_source" in audit:
        audit["resolved_mapping_count_by_ticker_source"] = (
            _resolved_mapping_counts(records)
        )
    validate_security_master(updated)
    return updated


__all__ = [
    "CACHE_SCHEMA_VERSION",
    "DEFAULT_MAX_CURRENT_EVIDENCE_AGE_DAYS",
    "DEFAULT_CACHE_PATH",
    "DiscoveryDiagnostic",
    "DiscoveryFetch",
    "DiscoveryResult",
    "EvidenceCorruptResponseError",
    "EvidenceParseError",
    "EvidenceRefreshError",
    "EvidenceSchemaError",
    "FilingSource",
    "NonSECFilingURL",
    "PERIODIC_IXBRL",
    "SCHEDULE_13DG",
    "SEC_CUSIP_SEARCH",
    "SEC_SUBMISSIONS",
    "ScheduleSearchCandidate",
    "ScheduleSearchResults",
    "SecEdgarEvidenceError",
    "apply_sec_edgar_evidence",
    "bridge_sec_evidence",
    "build_sec_cusip_search_url",
    "build_sec_submissions_url",
    "discover_sec_edgar_sources",
    "make_sec_discovery_fetcher",
    "make_sec_filing_fetcher",
    "merge_sec_edgar_evidence_caches",
    "normalize_sec_discovery_url",
    "normalize_sec_filing_url",
    "normalize_security_class",
    "parse_periodic_ixbrl",
    "parse_schedule_13dg_xml",
    "parse_sec_schedule_search_results",
    "parse_sec_submissions_periodic_sources",
    "refresh_sec_edgar_evidence",
    "validate_sec_edgar_evidence_cache",
]
