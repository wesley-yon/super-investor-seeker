"""Discover exact Form 13F accessions from the SEC Submissions API.

The SEC's per-CIK submissions document contains a ``filings.recent``
parallel-array table and, for older history, metadata pointing to immutable
archive shards.  This module validates those SEC-only locators and tables,
then returns every 13F-HR or 13F-HR/A accession for explicitly requested
report dates.

No cache or persistence API is provided here.  The SEC user agent is used
only as an in-memory HTTP header by :func:`make_sec_submissions_fetcher`; it is
never included in returned provenance.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests


SUBMISSIONS_ROOT = "https://data.sec.gov/submissions"
FORM_13F_TYPES = frozenset({"13F-HR", "13F-HR/A"})
MAX_REQUESTS_PER_SECOND = 8.0

_ACCESSION_RE = re.compile(r"^\d{10}-(?P<year>\d{2})-\d{6}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAIN_PATH_RE = re.compile(r"^/submissions/CIK(?P<cik>\d{10})\.json$")
_SHARD_NAME_RE = re.compile(
    r"^CIK(?P<cik>\d{10})-submissions-(?P<sequence>\d{3})\.json$"
)
_SHARD_PATH_RE = re.compile(
    r"^/submissions/CIK(?P<cik>\d{10})-submissions-"
    r"(?P<sequence>\d{3})\.json$"
)
_CONTACT_EMAIL_RE = re.compile(r"[^\s@]+@[^\s@]+\.[^\s@]+")
_RETRYABLE_STATUS_CODES = frozenset({403, 429, 500, 502, 503, 504})

Fetcher = Callable[[str], bytes]


class Sec13FAccessionDiscoveryError(ValueError):
    """Base error for unsafe sources, malformed SEC data, and discovery."""


class NonSECSubmissionsURL(Sec13FAccessionDiscoveryError):
    """Raised before an unsafe or unrelated URL can be fetched."""


class SubmissionsSchemaError(Sec13FAccessionDiscoveryError):
    """Raised when decoded SEC JSON violates the required schema."""


class SubmissionsChecksumError(Sec13FAccessionDiscoveryError):
    """Raised when fetched bytes do not match a caller-supplied checksum."""


class SubmissionsFetchError(Sec13FAccessionDiscoveryError):
    """Raised when a supplied fetcher cannot return one complete document."""


@dataclass(frozen=True, order=True)
class SubmissionEvidence:
    """Checksum-bound reference to one fetched SEC submissions document."""

    kind: str
    url: str
    sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "url": self.url,
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class SubmissionSource:
    """Validated source summary for one recent table or archive shard."""

    evidence: SubmissionEvidence
    row_count: int
    matched_row_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.evidence.to_dict(),
            "row_count": self.row_count,
            "matched_row_count": self.matched_row_count,
        }


@dataclass(frozen=True)
class Form13FAccession:
    """One exact SEC Form 13F filing locator for a requested report date."""

    cik: str
    report_date: str
    accession: str
    form_type: str
    filing_date: str
    evidence: tuple[SubmissionEvidence, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "cik": self.cik,
            "report_date": self.report_date,
            "accession": self.accession,
            "form_type": self.form_type,
            "filing_date": self.filing_date,
            "evidence": [item.to_dict() for item in self.evidence],
        }


@dataclass(frozen=True)
class Form13FAccessionDiscovery:
    """Deterministic result for one CIK and an explicit set of report dates."""

    cik: str
    report_dates: tuple[str, ...]
    filings: tuple[Form13FAccession, ...]
    sources: tuple[SubmissionSource, ...]

    @property
    def accessions(self) -> tuple[str, ...]:
        return tuple(filing.accession for filing in self.filings)

    @property
    def missing_report_dates(self) -> tuple[str, ...]:
        found = {filing.report_date for filing in self.filings}
        return tuple(value for value in self.report_dates if value not in found)

    def accessions_for(self, report_date: object) -> tuple[str, ...]:
        target = normalize_report_date(report_date)
        if target not in self.report_dates:
            raise Sec13FAccessionDiscoveryError(
                f"report date was not requested: {target}"
            )
        return tuple(
            filing.accession
            for filing in self.filings
            if filing.report_date == target
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "cik": self.cik,
            "report_dates": list(self.report_dates),
            "missing_report_dates": list(self.missing_report_dates),
            "filings": [filing.to_dict() for filing in self.filings],
            "sources": [source.to_dict() for source in self.sources],
        }


@dataclass(frozen=True)
class _ArchiveDescriptor:
    name: str
    url: str
    filing_count: int
    filing_from: str
    filing_to: str


@dataclass(frozen=True)
class _ParsedFiling:
    cik: str
    report_date: str
    accession: str
    form_type: str
    filing_date: str
    evidence: SubmissionEvidence


def normalize_cik(value: object) -> str:
    """Return a nonzero SEC CIK padded to ten decimal digits."""

    if isinstance(value, bool):
        raise Sec13FAccessionDiscoveryError(f"invalid SEC CIK: {value!r}")
    text = str(value).strip()
    if not text.isdigit() or not 1 <= len(text) <= 10 or int(text) == 0:
        raise Sec13FAccessionDiscoveryError(f"invalid SEC CIK: {value!r}")
    return text.zfill(10)


def normalize_report_date(value: object) -> str:
    """Return a canonical ISO calendar date, rejecting timestamps and blanks."""

    if isinstance(value, datetime):
        raise Sec13FAccessionDiscoveryError(
            f"report date must not include a time: {value!r}"
        )
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str) or value != value.strip():
        raise Sec13FAccessionDiscoveryError(f"invalid report date: {value!r}")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise Sec13FAccessionDiscoveryError(
            f"invalid report date: {value!r}"
        ) from exc
    if parsed.isoformat() != value:
        raise Sec13FAccessionDiscoveryError(f"invalid report date: {value!r}")
    return value


def _schema_date(value: object, *, field: str, source_url: str) -> str:
    try:
        return normalize_report_date(value)
    except Sec13FAccessionDiscoveryError as exc:
        raise SubmissionsSchemaError(
            f"{source_url} has invalid {field}: {value!r}"
        ) from exc


def _normalize_accession(
    value: object,
    *,
    filing_date: str,
    source_url: str,
) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise SubmissionsSchemaError(
            f"{source_url} has invalid accession: {value!r}"
        )
    match = _ACCESSION_RE.fullmatch(value)
    if match is None or match.group("year") != filing_date[2:4]:
        raise SubmissionsSchemaError(
            f"{source_url} has accession inconsistent with filing date: {value!r}"
        )
    return value


def normalize_sec_submissions_url(value: object) -> str:
    """Return one canonical SEC Submissions API URL or fail closed."""

    raw = str(value or "").strip()
    parsed = urlsplit(raw)
    try:
        port = parsed.port
    except ValueError as exc:
        raise NonSECSubmissionsURL(f"invalid SEC submissions URL: {raw!r}") from exc
    host = (parsed.hostname or "").casefold()
    if (
        parsed.scheme.casefold() != "https"
        or host != "data.sec.gov"
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.query
        or parsed.fragment
        or "\\" in parsed.path
        or any(part in {".", ".."} for part in parsed.path.split("/"))
        or not (
            _MAIN_PATH_RE.fullmatch(parsed.path)
            or _SHARD_PATH_RE.fullmatch(parsed.path)
        )
    ):
        raise NonSECSubmissionsURL(
            f"not an SEC submissions document URL: {raw!r}"
        )
    return urlunsplit(("https", "data.sec.gov", parsed.path, "", ""))


def build_sec_submissions_url(cik: object) -> str:
    """Build the documented recent-submissions URL for one filer CIK."""

    normalized_cik = normalize_cik(cik)
    return normalize_sec_submissions_url(
        f"{SUBMISSIONS_ROOT}/CIK{normalized_cik}.json"
    )


def _build_archive_url(name: str, *, cik: str) -> str:
    match = _SHARD_NAME_RE.fullmatch(name)
    if match is None or match.group("cik") != cik:
        raise SubmissionsSchemaError(
            f"SEC submissions archive name is not bound to CIK {cik}: {name!r}"
        )
    return normalize_sec_submissions_url(f"{SUBMISSIONS_ROOT}/{name}")


def _json_object(payload: bytes, *, source_url: str) -> dict[str, Any]:
    if not isinstance(payload, bytes):
        raise SubmissionsFetchError("SEC submissions fetcher must return bytes")
    if not payload.strip():
        raise SubmissionsSchemaError(f"empty SEC submissions JSON: {source_url}")
    try:
        decoded = payload.decode("utf-8-sig")
        document = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SubmissionsSchemaError(
            f"malformed SEC submissions JSON: {source_url}"
        ) from exc
    if not isinstance(document, dict):
        raise SubmissionsSchemaError(
            f"SEC submissions JSON root must be an object: {source_url}"
        )
    return document


def _parallel_columns(
    table: object,
    *,
    source_url: str,
) -> tuple[dict[str, list[Any]], int]:
    if not isinstance(table, dict):
        raise SubmissionsSchemaError(
            f"SEC submissions filing table must be an object: {source_url}"
        )
    fields = ("form", "accessionNumber", "filingDate", "reportDate")
    columns = {field: table.get(field) for field in fields}
    if any(not isinstance(column, list) for column in columns.values()):
        raise SubmissionsSchemaError(
            f"SEC submissions required columns are malformed: {source_url}"
        )
    lengths = {len(column) for column in columns.values()}
    if len(lengths) != 1:
        raise SubmissionsSchemaError(
            f"SEC submissions columns are misaligned: {source_url}"
        )
    return columns, next(iter(lengths), 0)


def _parse_table(
    table: object,
    *,
    cik: str,
    targets: frozenset[str],
    evidence: SubmissionEvidence,
) -> tuple[list[_ParsedFiling], tuple[str, ...]]:
    columns, row_count = _parallel_columns(table, source_url=evidence.url)
    rows: list[_ParsedFiling] = []
    filing_dates: list[str] = []
    for index in range(row_count):
        raw_form = columns["form"][index]
        if (
            not isinstance(raw_form, str)
            or not raw_form
            or raw_form != raw_form.strip()
        ):
            raise SubmissionsSchemaError(
                f"{evidence.url} has invalid form at row {index}"
            )
        filing_date = _schema_date(
            columns["filingDate"][index],
            field=f"filingDate at row {index}",
            source_url=evidence.url,
        )
        filing_dates.append(filing_date)
        canonical_form = raw_form.strip().upper()
        if canonical_form in FORM_13F_TYPES and raw_form != canonical_form:
            raise SubmissionsSchemaError(
                f"{evidence.url} has noncanonical Form 13F type at row {index}"
            )
        raw_report_date = columns["reportDate"][index]
        if canonical_form not in FORM_13F_TYPES:
            if raw_report_date is not None and raw_report_date != "":
                _schema_date(
                    raw_report_date,
                    field=f"reportDate at row {index}",
                    source_url=evidence.url,
                )
            continue
        report_date = _schema_date(
            raw_report_date,
            field=f"Form 13F reportDate at row {index}",
            source_url=evidence.url,
        )
        # SEC submissions documents contain historical metadata anomalies in
        # unrelated rows (including old Form 13F accessions whose year does
        # not match their filingDate). Their aligned filing dates remain part
        # of archive count/range validation, but an accession outside the
        # requested report-date scope is not a locator for this discovery and
        # must not block exact in-scope evidence.
        if report_date not in targets:
            continue
        accession = _normalize_accession(
            columns["accessionNumber"][index],
            filing_date=filing_date,
            source_url=evidence.url,
        )
        rows.append(
            _ParsedFiling(
                cik=cik,
                report_date=report_date,
                accession=accession,
                form_type=canonical_form,
                filing_date=filing_date,
                evidence=evidence,
            )
        )
    return rows, tuple(filing_dates)


def _parse_archive_descriptors(
    raw_files: object,
    *,
    cik: str,
) -> tuple[_ArchiveDescriptor, ...]:
    if not isinstance(raw_files, list):
        raise SubmissionsSchemaError(
            f"SEC submissions archive metadata is malformed for CIK {cik}"
        )
    descriptors: list[_ArchiveDescriptor] = []
    seen_names: set[str] = set()
    seen_sequences: set[str] = set()
    for index, raw in enumerate(raw_files):
        if not isinstance(raw, dict):
            raise SubmissionsSchemaError(
                f"SEC submissions archive metadata row {index} is not an object"
            )
        name = raw.get("name")
        if not isinstance(name, str):
            raise SubmissionsSchemaError(
                f"SEC submissions archive metadata row {index} has no valid name"
            )
        url = _build_archive_url(name, cik=cik)
        match = _SHARD_NAME_RE.fullmatch(name)
        assert match is not None
        sequence = match.group("sequence")
        if name in seen_names or sequence in seen_sequences:
            raise SubmissionsSchemaError(
                f"duplicate SEC submissions archive metadata: {name}"
            )
        seen_names.add(name)
        seen_sequences.add(sequence)
        filing_count = raw.get("filingCount")
        if type(filing_count) is not int or filing_count < 0:
            raise SubmissionsSchemaError(
                f"SEC submissions archive {name} has invalid filingCount"
            )
        filing_from = _schema_date(
            raw.get("filingFrom"),
            field=f"{name} filingFrom",
            source_url=build_sec_submissions_url(cik),
        )
        filing_to = _schema_date(
            raw.get("filingTo"),
            field=f"{name} filingTo",
            source_url=build_sec_submissions_url(cik),
        )
        if filing_from > filing_to:
            raise SubmissionsSchemaError(
                f"SEC submissions archive {name} has a reversed filing range"
            )
        descriptors.append(
            _ArchiveDescriptor(
                name=name,
                url=url,
                filing_count=filing_count,
                filing_from=filing_from,
                filing_to=filing_to,
            )
        )
    return tuple(sorted(descriptors, key=lambda item: item.name))


def _normalize_targets(report_dates: str | date | Iterable[object]) -> tuple[str, ...]:
    raw_values: Iterable[object]
    if isinstance(report_dates, (str, date)):
        raw_values = (report_dates,)
    else:
        try:
            raw_values = tuple(report_dates)
        except TypeError as exc:
            raise Sec13FAccessionDiscoveryError(
                "report_dates must be a date or iterable of dates"
            ) from exc
    normalized = tuple(sorted({normalize_report_date(value) for value in raw_values}))
    if not normalized:
        raise Sec13FAccessionDiscoveryError("at least one report date is required")
    return normalized


def _normalize_expected_checksums(
    values: Mapping[str, str] | None,
) -> dict[str, str]:
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise Sec13FAccessionDiscoveryError(
            "expected_sha256_by_url must be a mapping"
        )
    normalized: dict[str, str] = {}
    for raw_url, raw_digest in values.items():
        url = normalize_sec_submissions_url(raw_url)
        digest = str(raw_digest or "").strip().casefold()
        if not _SHA256_RE.fullmatch(digest):
            raise Sec13FAccessionDiscoveryError(
                f"invalid expected SEC source checksum for {url}"
            )
        prior = normalized.setdefault(url, digest)
        if prior != digest:
            raise Sec13FAccessionDiscoveryError(
                f"conflicting expected SEC source checksums for {url}"
            )
    return normalized


def _fetch_source(
    fetcher: Fetcher,
    url: str,
    *,
    expected_checksums: Mapping[str, str],
) -> tuple[bytes, str]:
    canonical_url = normalize_sec_submissions_url(url)
    try:
        payload = fetcher(canonical_url)
    except Exception as exc:
        raise SubmissionsFetchError(
            f"failed to fetch SEC submissions source: {canonical_url}"
        ) from exc
    if not isinstance(payload, (bytes, bytearray)):
        raise SubmissionsFetchError("SEC submissions fetcher must return bytes")
    raw = bytes(payload)
    digest = hashlib.sha256(raw).hexdigest()
    expected = expected_checksums.get(canonical_url)
    if expected is not None and not hmac.compare_digest(digest, expected):
        raise SubmissionsChecksumError(
            f"SEC submissions source checksum mismatch: {canonical_url}"
        )
    return raw, digest


def make_sec_submissions_fetcher(
    user_agent: str | None = None,
    *,
    timeout: float = 30.0,
    session: requests.Session | None = None,
    max_attempts: int = 3,
    requests_per_second: float = MAX_REQUESTS_PER_SECOND,
) -> Fetcher:
    """Create a bounded, SEC-submissions-only HTTP fetcher.

    The contact-bearing user agent remains only in this closure and request
    headers.  Redirects are refused because every allowed source has one
    canonical ``data.sec.gov`` URL.
    """

    agent = str(user_agent or os.environ.get("SEC_USER_AGENT") or "").strip()
    if not _CONTACT_EMAIL_RE.search(agent):
        raise Sec13FAccessionDiscoveryError(
            "SEC_USER_AGENT with a contact email is required for SEC downloads"
        )
    if type(timeout) not in {int, float} or timeout <= 0:
        raise Sec13FAccessionDiscoveryError("SEC timeout must be positive")
    if type(max_attempts) is not int or not 1 <= max_attempts <= 5:
        raise Sec13FAccessionDiscoveryError(
            "SEC max_attempts must be between one and five"
        )
    if (
        type(requests_per_second) not in {int, float}
        or not 0 < requests_per_second <= MAX_REQUESTS_PER_SECOND
    ):
        raise Sec13FAccessionDiscoveryError(
            "SEC request rate must be greater than zero and at most eight per second"
        )
    http = session or requests.Session()
    rate_lock = threading.Lock()
    next_request_at = 0.0
    interval = 1.0 / float(requests_per_second)

    def pace() -> None:
        nonlocal next_request_at
        with rate_lock:
            now = time.monotonic()
            scheduled = max(now, next_request_at)
            delay = scheduled - now
            next_request_at = scheduled + interval
        if delay > 0:
            time.sleep(delay)

    def fetch(url: str) -> bytes:
        canonical_url = normalize_sec_submissions_url(url)
        for attempt in range(max_attempts):
            pace()
            try:
                response = http.get(
                    canonical_url,
                    headers={
                        "User-Agent": agent,
                        "Accept": "application/json",
                        "Accept-Encoding": "gzip, deflate",
                    },
                    timeout=float(timeout),
                    allow_redirects=False,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt + 1 == max_attempts:
                    raise
                time.sleep(min(float(2**attempt), 8.0))
                continue
            response_url = normalize_sec_submissions_url(
                str(response.url or canonical_url)
            )
            if response_url != canonical_url:
                raise NonSECSubmissionsURL(
                    "SEC submissions response URL changed unexpectedly"
                )
            if response.status_code in {301, 302, 303, 307, 308}:
                raise NonSECSubmissionsURL(
                    "SEC submissions redirects are not allowed"
                )
            if response.status_code in _RETRYABLE_STATUS_CODES:
                if attempt + 1 < max_attempts:
                    time.sleep(min(float(2**attempt), 8.0))
                    continue
            response.raise_for_status()
            return bytes(response.content)
        raise AssertionError("bounded SEC submissions retry loop exhausted")

    return fetch


def discover_form13f_accessions(
    cik: object,
    report_dates: str | date | Iterable[object],
    *,
    include_archive_shards: bool = True,
    fetcher: Fetcher | None = None,
    user_agent: str | None = None,
    expected_sha256_by_url: Mapping[str, str] | None = None,
) -> Form13FAccessionDiscovery:
    """Return all exact Form 13F accessions for one CIK/report-date scope.

    Archive shards are fetched in full when enabled.  Their metadata ranges
    describe filing dates, not report dates, so skipping a shard by range
    could silently omit a late amendment for a requested historical period.
    """

    if type(include_archive_shards) is not bool:
        raise Sec13FAccessionDiscoveryError(
            "include_archive_shards must be a boolean"
        )
    normalized_cik = normalize_cik(cik)
    normalized_targets = _normalize_targets(report_dates)
    targets = frozenset(normalized_targets)
    expected_checksums = _normalize_expected_checksums(expected_sha256_by_url)
    active_fetcher = (
        fetcher if fetcher is not None else make_sec_submissions_fetcher(user_agent)
    )
    if not callable(active_fetcher):
        raise Sec13FAccessionDiscoveryError("SEC submissions fetcher must be callable")

    main_url = build_sec_submissions_url(normalized_cik)
    main_payload, main_sha256 = _fetch_source(
        active_fetcher,
        main_url,
        expected_checksums=expected_checksums,
    )
    main_document = _json_object(main_payload, source_url=main_url)
    try:
        document_cik = normalize_cik(main_document.get("cik"))
    except Sec13FAccessionDiscoveryError as exc:
        raise SubmissionsSchemaError(
            f"SEC submissions document has invalid CIK: {main_url}"
        ) from exc
    if document_cik != normalized_cik:
        raise SubmissionsSchemaError(
            f"SEC submissions document CIK mismatch: {main_url}"
        )
    filings_object = main_document.get("filings")
    if not isinstance(filings_object, dict):
        raise SubmissionsSchemaError(
            f"SEC submissions document has no filings object: {main_url}"
        )
    main_evidence = SubmissionEvidence("recent", main_url, main_sha256)
    recent_rows, recent_filing_dates = _parse_table(
        filings_object.get("recent"),
        cik=normalized_cik,
        targets=targets,
        evidence=main_evidence,
    )
    recent_row_count = len(recent_filing_dates)
    descriptors = _parse_archive_descriptors(
        filings_object.get("files"),
        cik=normalized_cik,
    )

    parsed_rows = list(recent_rows)
    sources = [
        SubmissionSource(
            main_evidence,
            row_count=recent_row_count,
            matched_row_count=len(recent_rows),
        )
    ]
    if include_archive_shards:
        for descriptor in descriptors:
            payload, digest = _fetch_source(
                active_fetcher,
                descriptor.url,
                expected_checksums=expected_checksums,
            )
            document = _json_object(payload, source_url=descriptor.url)
            evidence = SubmissionEvidence(
                "archive_shard",
                descriptor.url,
                digest,
            )
            rows, filing_dates = _parse_table(
                document,
                cik=normalized_cik,
                targets=targets,
                evidence=evidence,
            )
            row_count = len(filing_dates)
            if row_count != descriptor.filing_count:
                raise SubmissionsSchemaError(
                    f"SEC submissions archive count mismatch: {descriptor.name}"
                )
            if row_count:
                if (
                    min(filing_dates) != descriptor.filing_from
                    or max(filing_dates) != descriptor.filing_to
                ):
                    raise SubmissionsSchemaError(
                        f"SEC submissions archive date-range mismatch: {descriptor.name}"
                    )
            sources.append(
                SubmissionSource(
                    evidence,
                    row_count=row_count,
                    matched_row_count=len(rows),
                )
            )
            parsed_rows.extend(rows)

    by_accession: dict[str, _ParsedFiling] = {}
    evidence_by_accession: dict[str, set[SubmissionEvidence]] = {}
    for row in parsed_rows:
        prior = by_accession.get(row.accession)
        if prior is not None and (
            prior.cik,
            prior.report_date,
            prior.form_type,
            prior.filing_date,
        ) != (
            row.cik,
            row.report_date,
            row.form_type,
            row.filing_date,
        ):
            raise SubmissionsSchemaError(
                f"conflicting SEC metadata for accession {row.accession}"
            )
        by_accession.setdefault(row.accession, row)
        evidence_by_accession.setdefault(row.accession, set()).add(row.evidence)

    filings = tuple(
        Form13FAccession(
            cik=row.cik,
            report_date=row.report_date,
            accession=row.accession,
            form_type=row.form_type,
            filing_date=row.filing_date,
            evidence=tuple(sorted(evidence_by_accession[row.accession])),
        )
        for row in sorted(
            by_accession.values(),
            key=lambda item: (
                item.report_date,
                item.filing_date,
                item.accession,
                item.form_type,
            ),
        )
    )
    return Form13FAccessionDiscovery(
        cik=normalized_cik,
        report_dates=normalized_targets,
        filings=filings,
        sources=tuple(sources),
    )


__all__ = [
    "FORM_13F_TYPES",
    "Form13FAccession",
    "Form13FAccessionDiscovery",
    "NonSECSubmissionsURL",
    "Sec13FAccessionDiscoveryError",
    "SubmissionEvidence",
    "SubmissionSource",
    "SubmissionsChecksumError",
    "SubmissionsFetchError",
    "SubmissionsSchemaError",
    "build_sec_submissions_url",
    "discover_form13f_accessions",
    "make_sec_submissions_fetcher",
    "normalize_cik",
    "normalize_report_date",
    "normalize_sec_submissions_url",
]
