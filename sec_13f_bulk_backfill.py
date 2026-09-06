"""SEC-only historical Form 13F reported-identity backfill.

The SEC publishes quarterly Form 13F data sets containing the flattened,
as-filed ``SUBMISSION`` and ``INFOTABLE`` tables.  This module builds a private
SQLite evidence index from those official ZIP files, then fills immutable
``reported_*`` fields on existing fund holdings only when an exact row match is
unique.  Canonical display fields are never rewritten here.

The small JSON state manifest is switched atomically and points to an immutable,
generation-named SQLite file.  A corrupt ZIP, missing required column, failed
download, or interrupted refresh therefore leaves the previous manifest and
index usable.  No issuer-name or other fuzzy match is performed.
"""

from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import unicodedata
import zipfile
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from itertools import product
from html import unescape
from html.parser import HTMLParser
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urldefrag, urljoin, urlsplit, urlunsplit
from xml.etree import ElementTree

from atomic_files import atomic_text_output
from atomic_files import fsync_directory as _fsync_directory
from composition_integrity import calculate_quarter_composition_hash
from sec_13f_accession_discovery import (
    Sec13FAccessionDiscoveryError,
    discover_form13f_accessions,
    make_sec_submissions_fetcher,
    normalize_sec_submissions_url,
)
from sec_security_master import make_sec_fetcher
from security_master_migration import economic_positions_for_fund
from value_units import AmbiguousValueUnits, classify_value_units


ROOT = Path(__file__).resolve().parent
FORM_13F_DATASETS_PAGE_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
)
FORM_13F_DATASET_PATH_PREFIX = (
    "/files/structureddata/data/form-13f-data-sets/"
)
DEFAULT_STATE_PATH = ROOT / ".cache" / "sec_13f_bulk_source_state.json"
DEFAULT_INDEX_DIR = ROOT / ".cache" / "sec_13f_bulk_indices"
DEFAULT_REBUILD_CHECKPOINT_PATH = (
    ROOT / ".cache" / "sec_13f_bulk_rebuild_checkpoint.json"
)
DEFAULT_COMPLETED_RECEIPT_PATH = (
    ROOT / ".cache" / "sec_13f_bulk_completed_receipt.json"
)
ACCESSION_DISCOVERY_CHECKPOINT_SCHEMA_VERSION = 1
ACCESSION_DISCOVERY_CHECKPOINT_INTERVAL = 10

# A clean hosted rebuild temporarily owns one filtered SQLite candidate while
# the restored corpus and the security-master staging tree are also present.
# Archive fallbacks are folded into that same candidate, and the bulk working
# set is removed before snapshot packing, so no second SQLite generation or
# snapshot archive has to overlap it.  Keep a deliberately large fixed reserve
# for filesystem metadata, SEC-master staging, and runner setup variance.
DEFAULT_CLEAN_REBUILD_MIN_FREE_BYTES = 8 * 1024**3

STATE_SCHEMA_VERSION = 1
INDEX_SCHEMA_VERSION = 1
CLEAN_REBUILD_PARSER_CONTRACT_VERSION = 1
COMPLETED_CLEAN_REBUILD_RECEIPT_SCHEMA_VERSION = 2
COMPLETED_CLEAN_REBUILD_RECEIPT_SCOPE = "sec_13f_index_only"
LEGACY_INDEX_ADOPTION_RECEIPT_SCOPE = (
    "sec_13f_index_only_unpublished_pre_plan_adoption"
)
MAX_ARCHIVE_MEMBERS = 16
MAX_ARCHIVE_UNCOMPRESSED_BYTES = 4_000_000_000
MAX_SINGLE_MEMBER_BYTES = 3_000_000_000

_SEC_HOSTS = frozenset({"sec.gov", "www.sec.gov"})
_DATASET_NAME_RE = re.compile(
    r"^(?:\d{4}q[1-4]|\d{2}[a-z]{3}\d{4}-\d{2}[a-z]{3}\d{4})_form13f\.zip$",
    re.IGNORECASE,
)
_ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MANAGED_INDEX_NAME_RE = re.compile(r"^index-[0-9a-f]{64}\.sqlite3$")
_MANAGED_PARTIAL_INDEX_NAME_RE = re.compile(
    r"^\.rebuild-[0-9a-f]{64}\.sqlite3\.partial$"
)
_ARCHIVE_INDEX_PATH_RE = re.compile(
    r"^/Archives/edgar/data/(?P<cik>\d{1,10})/"
    r"(?P<accession_compact>\d{18})/index\.json$",
    re.IGNORECASE,
)
_ARCHIVE_SUBMISSION_PATH_RE = re.compile(
    r"^/Archives/edgar/data/(?P<cik>\d{1,10})/"
    r"(?P<accession_compact>\d{18})/"
    r"(?P<accession>\d{10}-\d{2}-\d{6})\.txt$",
    re.IGNORECASE,
)
_ARCHIVE_DOCUMENT_PATH_RE = re.compile(
    r"^/Archives/edgar/data/(?P<cik>\d{1,10})/"
    r"(?P<accession_compact>\d{18})/"
    r"(?P<filename>[A-Za-z0-9][A-Za-z0-9._-]{0,255})$",
    re.IGNORECASE,
)

_SUBMISSION_REQUIRED_COLUMNS = frozenset({
    "ACCESSION_NUMBER",
    "FILING_DATE",
    "SUBMISSIONTYPE",
    "CIK",
    "PERIODOFREPORT",
})
_INFOTABLE_REQUIRED_COLUMNS = frozenset({
    "ACCESSION_NUMBER",
    "INFOTABLE_SK",
    "NAMEOFISSUER",
    "TITLEOFCLASS",
    "CUSIP",
    "VALUE",
    "SSHPRNAMT",
    "SSHPRNAMTTYPE",
    "PUTCALL",
})
Fetcher = Callable[[str], bytes]


class Sec13FBulkError(ValueError):
    """Base error for unsafe sources, malformed data, and invalid state."""


class NonSECDatasetURL(Sec13FBulkError):
    """Raised before a non-SEC or non-dataset URL can be fetched."""


class DatasetParseError(Sec13FBulkError):
    """Raised when a ZIP cannot safely become durable evidence."""


class BulkIndexRefreshError(Sec13FBulkError):
    """Raised by the high-level rebuild when no complete refresh is possible."""


@dataclass(frozen=True)
class BulkIndexRefreshResult:
    """Outcome of refreshing the private SEC Form 13F evidence index."""

    state: dict[str, Any]
    changed: bool
    refreshed_urls: tuple[str, ...]
    reused_urls: tuple[str, ...]
    errors: tuple[str, ...]


@dataclass(frozen=True)
class FundBackfillResult:
    """Aggregate result of applying exact evidence to fund JSON files."""

    files_scanned: int
    files_changed: int
    holdings_scanned: int
    holdings_changed: int
    exact_matches: int
    unmatched: int
    ambiguous: int
    conflicts: int


@dataclass(frozen=True)
class ReportedIdentityVerificationResult:
    """Exact SEC-evidence verification for the retained holdings corpus."""

    files_scanned: int
    holdings_scanned: int
    placeholder_holdings: int
    exact_matches: int
    unmatched: int
    ambiguous: int
    conflicts: int
    unaddressable: int
    issues: tuple[dict[str, Any], ...]

    @property
    def ok(self) -> bool:
        return not (
            self.unmatched
            or self.ambiguous
            or self.conflicts
            or self.unaddressable
        )


@dataclass(frozen=True)
class RebuildReportedIdentityResult:
    """Combined source-index refresh and fund-file backfill result."""

    refresh: BulkIndexRefreshResult
    backfill: FundBackfillResult
    archive_fallback: ArchiveFallbackRefreshResult | None = None
    completed_rebuild_receipt: dict[str, Any] | None = None


@dataclass(frozen=True)
class ArchiveFallbackRefreshResult:
    """Outcome of exact-accession SEC Archives fallback ingestion."""

    state: dict[str, Any]
    changed: bool
    resolved_accessions: tuple[str, ...]
    reused_accessions: tuple[str, ...]
    unresolved: tuple[dict[str, str], ...]


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


def _normalize_text(value: object | None) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(text.replace("\xa0", " ").split())


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_timestamp(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return (
        current.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def normalize_sec_13f_dataset_url(value: object | None) -> str:
    """Return a canonical official SEC Form 13F ZIP URL or fail closed."""

    raw = _normalize_text(value)
    parsed = urlsplit(raw)
    try:
        port = parsed.port
    except ValueError as exc:
        raise NonSECDatasetURL(f"invalid SEC data-set URL: {raw!r}") from exc
    host = (parsed.hostname or "").casefold()
    if (
        parsed.scheme.casefold() != "https"
        or host not in _SEC_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
    ):
        raise NonSECDatasetURL(f"not an SEC HTTPS data-set URL: {raw!r}")
    path = parsed.path
    if not path.casefold().startswith(FORM_13F_DATASET_PATH_PREFIX.casefold()):
        raise NonSECDatasetURL(f"not an SEC Form 13F data-set path: {raw!r}")
    name = PurePosixPath(path).name
    if not _DATASET_NAME_RE.fullmatch(name):
        raise NonSECDatasetURL(f"not a recognized Form 13F ZIP: {raw!r}")
    canonical_path = FORM_13F_DATASET_PATH_PREFIX + name
    return urlunsplit(("https", "www.sec.gov", canonical_path, "", ""))


def _archive_base(cik: object | None, accession: object | None) -> tuple[str, str]:
    normalized_cik = _normalize_cik(cik)
    normalized_accession = _normalize_accession(accession)
    compact = normalized_accession.replace("-", "")
    return str(int(normalized_cik)), compact


def sec_archive_index_url(cik: object | None, accession: object | None) -> str:
    cik_path, compact = _archive_base(cik, accession)
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{cik_path}/{compact}/index.json"
    )


def sec_archive_submission_url(
    cik: object | None,
    accession: object | None,
) -> str:
    cik_path, compact = _archive_base(cik, accession)
    normalized_accession = _normalize_accession(accession)
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{cik_path}/{compact}/{normalized_accession}.txt"
    )


def normalize_sec_archive_url(value: object | None) -> str:
    """Validate an exact SEC Archives index/full-submission URL."""

    raw = _normalize_text(value)
    parsed = urlsplit(raw)
    try:
        port = parsed.port
    except ValueError as exc:
        raise NonSECDatasetURL(f"invalid SEC Archives URL: {raw!r}") from exc
    host = (parsed.hostname or "").casefold()
    if (
        parsed.scheme.casefold() != "https"
        or host not in _SEC_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
    ):
        raise NonSECDatasetURL(f"not an SEC HTTPS Archives URL: {raw!r}")
    match = _ARCHIVE_INDEX_PATH_RE.fullmatch(parsed.path)
    submission_match = _ARCHIVE_SUBMISSION_PATH_RE.fullmatch(parsed.path)
    if match is None and submission_match is None:
        raise NonSECDatasetURL(f"not an exact SEC filing archive URL: {raw!r}")
    selected = match or submission_match
    assert selected is not None
    compact = selected.group("accession_compact")
    accession = (
        selected.groupdict().get("accession")
        if submission_match is not None
        else None
    )
    if accession is not None and accession.replace("-", "") != compact:
        raise NonSECDatasetURL("SEC full-submission path/accession mismatch")
    canonical_path = parsed.path
    return urlunsplit(("https", "www.sec.gov", canonical_path, "", ""))


def normalize_sec_identity_source_url(
    value: object | None,
    *,
    accession: object | None,
) -> str:
    """Validate a bulk ZIP or one exact document in an SEC filing directory."""

    raw = _normalize_text(value)
    try:
        return normalize_sec_13f_dataset_url(raw)
    except NonSECDatasetURL:
        pass
    normalized_accession = _normalize_accession(accession)
    parsed = urlsplit(raw)
    try:
        port = parsed.port
    except ValueError as exc:
        raise NonSECDatasetURL(f"invalid SEC identity source URL: {raw!r}") from exc
    host = (parsed.hostname or "").casefold()
    match = _ARCHIVE_DOCUMENT_PATH_RE.fullmatch(parsed.path)
    if (
        parsed.scheme.casefold() != "https"
        or host not in _SEC_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or match is None
        or match.group("accession_compact")
        != normalized_accession.replace("-", "")
    ):
        raise NonSECDatasetURL(
            f"not an exact SEC Form 13F identity source URL: {raw!r}"
        )
    return urlunsplit(("https", "www.sec.gov", parsed.path, "", ""))


def discover_13f_dataset_urls(
    html: str | bytes,
    *,
    page_url: str = FORM_13F_DATASETS_PAGE_URL,
) -> list[str]:
    """Discover SEC-hosted quarterly ZIP links from the official page."""

    text = (
        bytes(html).decode("utf-8-sig", errors="replace")
        if isinstance(html, (bytes, bytearray))
        else str(html)
    )
    parser = _LinkParser()
    parser.feed(text)
    discovered: set[str] = set()
    for href in parser.hrefs:
        candidate, _fragment = urldefrag(urljoin(page_url, href))
        try:
            discovered.add(normalize_sec_13f_dataset_url(candidate))
        except NonSECDatasetURL:
            continue
    return sorted(discovered, key=_dataset_url_sort_key)


def _dataset_url_sort_key(url: str) -> tuple[int, int, str]:
    name = PurePosixPath(urlsplit(url).path).name.casefold()
    quarter = re.fullmatch(r"(\d{4})q([1-4])_form13f\.zip", name)
    if quarter:
        return int(quarter.group(1)), int(quarter.group(2)), name
    interval = re.fullmatch(
        r"(\d{2})([a-z]{3})(\d{4})-(\d{2})([a-z]{3})(\d{4})_form13f\.zip",
        name,
    )
    if interval:
        month_numbers = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }
        end_month = month_numbers.get(interval.group(5), 0)
        return int(interval.group(6)), (end_month - 1) // 3 + 1, name
    return 9999, 9, name


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    with atomic_text_output(path, sync_parent=_fsync_directory) as output:
        json.dump(payload, output, sort_keys=True, indent=2, ensure_ascii=False)
        output.write("\n")


def _atomic_write_fund_json(path: Path, payload: Mapping[str, Any]) -> None:
    with atomic_text_output(
        path, sync_parent=_fsync_directory, newline=None,
    ) as output:
        json.dump(payload, output, separators=(",", ":"), ensure_ascii=False)
        output.write("\n")


def _existing_filesystem_ancestor(path: Path) -> Path:
    candidate = Path(path).resolve()
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            raise BulkIndexRefreshError(
                f"cannot locate a filesystem for SEC 13F bulk path {path}"
            )
        candidate = parent
    return candidate


def ensure_clean_rebuild_disk_space(
    *,
    index_dir: Path = DEFAULT_INDEX_DIR,
    checkpoint_path: Path = DEFAULT_REBUILD_CHECKPOINT_PATH,
    minimum_free_bytes: int = DEFAULT_CLEAN_REBUILD_MIN_FREE_BYTES,
    minimum_remaining_free_bytes: int = 1024**3,
    disk_usage: Callable[[Path], Any] = shutil.disk_usage,
) -> dict[str, int | str]:
    """Fail before a hosted clean rebuild when disk headroom is unsafe.

    ``disk_usage`` and ``minimum_free_bytes`` are injectable so the publication
    gate can be tested without filling a filesystem. A fresh build reserves
    eight GiB because historical targets can expand substantially; a validated
    plan-addressed partial index receives byte-for-byte credit while a one-GiB
    floor preserves space for SQLite growth and filesystem metadata.
    """

    if (
        type(minimum_free_bytes) is not int
        or minimum_free_bytes < 0
        or type(minimum_remaining_free_bytes) is not int
        or minimum_remaining_free_bytes < 0
    ):
        raise Sec13FBulkError("minimum SEC 13F free-space reserve is invalid")
    probe = _existing_filesystem_ancestor(Path(index_dir))
    try:
        usage = disk_usage(probe)
        available = int(usage.free)
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        raise BulkIndexRefreshError(
            f"cannot determine free disk space for SEC 13F rebuild at {probe}: {exc}"
        ) from exc
    resumable_bytes = _validated_resumable_partial_bytes(
        checkpoint_path=Path(checkpoint_path),
        index_dir=Path(index_dir),
    )
    required = max(
        minimum_remaining_free_bytes,
        minimum_free_bytes - resumable_bytes,
    )
    if available < required:
        gib = 1024**3
        raise BulkIndexRefreshError(
            "insufficient free disk space for clean SEC Form 13F backfill: "
            f"{available / gib:.1f} GiB available at {probe}, "
            f"{required / gib:.1f} GiB required"
            + (
                f" after crediting {resumable_bytes / gib:.1f} GiB of "
                "validated partial index"
                if resumable_bytes
                else ""
            )
        )
    return {
        "path": str(probe),
        "available_bytes": available,
        "minimum_free_bytes": required,
        "resumable_bytes": resumable_bytes,
    }


def _validated_resumable_partial_bytes(
    *,
    checkpoint_path: Path,
    index_dir: Path,
) -> int:
    try:
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return 0
    if (
        not isinstance(checkpoint, dict)
        or checkpoint.get("schema_version") != 1
        or not _clean_checkpoint_checksum_valid(checkpoint)
    ):
        return 0
    raw_path = checkpoint.get("partial_index_path")
    if not isinstance(raw_path, str):
        return 0
    partial = Path(raw_path)
    if (
        partial.resolve().parent != Path(index_dir).resolve()
        or _MANAGED_PARTIAL_INDEX_NAME_RE.fullmatch(partial.name) is None
        or not partial.is_file()
    ):
        return 0
    connection: sqlite3.Connection | None = None
    try:
        connection = _open_index(partial, read_only=True)
        integrity = connection.execute("PRAGMA quick_check").fetchone()
        if integrity is None or integrity[0] != "ok":
            return 0
        return partial.stat().st_size
    except (OSError, sqlite3.Error, Sec13FBulkError):
        return 0
    finally:
        if connection is not None:
            connection.close()


def _is_managed_index_path(path: Path, index_dir: Path) -> bool:
    candidate = Path(path).resolve()
    directory = Path(index_dir).resolve()
    return (
        candidate.parent == directory
        and _MANAGED_INDEX_NAME_RE.fullmatch(candidate.name) is not None
    )


def _cleanup_superseded_index_generations(
    *,
    index_dir: Path,
    active_index_path: Path,
) -> tuple[Path, ...]:
    """Remove only owned generations after the active manifest is durable."""

    directory = Path(index_dir)
    if not directory.is_dir():
        return ()
    active = Path(active_index_path).resolve()
    removed: list[Path] = []
    for candidate in sorted(directory.iterdir()):
        if (
            not candidate.is_file()
            or not _is_managed_index_path(candidate, directory)
            or candidate.resolve() == active
        ):
            continue
        candidate.unlink()
        removed.append(candidate)
    if removed:
        _fsync_directory(directory)
    return tuple(removed)


def _discard_unpublished_index(
    path: Path | None,
    *,
    prior_index_path: Path | None,
    index_dir: Path,
) -> None:
    if path is None:
        return
    candidate = Path(path)
    if prior_index_path is not None and candidate.resolve() == Path(
        prior_index_path
    ).resolve():
        return
    if _is_managed_index_path(candidate, index_dir):
        candidate.unlink(missing_ok=True)


def cleanup_13f_bulk_working_set(
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    index_dir: Path = DEFAULT_INDEX_DIR,
    checkpoint_path: Path = DEFAULT_REBUILD_CHECKPOINT_PATH,
    completed_receipt_path: Path | None = None,
) -> tuple[Path, ...]:
    """Delete only the disposable bulk manifest and its managed generations.

    This is called after immutable holding identity has been applied and
    re-verified.  The manifest is unlinked and fsynced first so a crash can
    leave only harmless orphaned working files, never a state file pointing at
    an absent index.  Unknown files in the dedicated directory are untouched.
    """

    state_path = Path(state_path)
    index_dir = Path(index_dir)
    checkpoint_path = Path(checkpoint_path)
    receipt_path = (
        Path(completed_receipt_path)
        if completed_receipt_path is not None
        else state_path.parent / DEFAULT_COMPLETED_RECEIPT_PATH.name
    )
    accession_checkpoint_path = _accession_discovery_checkpoint_path(
        checkpoint_path
    )
    state = load_13f_bulk_index(state_path, verify_index_checksum=True)
    active_index = _index_path_from_state(state, state_path)
    if active_index is not None and not _is_managed_index_path(
        active_index,
        index_dir,
    ):
        raise Sec13FBulkError(
            "refusing to delete SEC 13F evidence index outside its managed directory"
        )

    removed: list[Path] = []
    if state_path.exists():
        state_path.unlink()
        removed.append(state_path)
        _fsync_directory(state_path.parent)
    if checkpoint_path.exists():
        checkpoint_path.unlink()
        removed.append(checkpoint_path)
        _fsync_directory(checkpoint_path.parent)
    if accession_checkpoint_path.exists():
        accession_checkpoint_path.unlink()
        removed.append(accession_checkpoint_path)
        _fsync_directory(accession_checkpoint_path.parent)
    if receipt_path.exists():
        receipt_path.unlink()
        removed.append(receipt_path)
        _fsync_directory(receipt_path.parent)
    if index_dir.is_dir():
        for candidate in sorted(index_dir.iterdir()):
            if not candidate.is_file() or not (
                _is_managed_index_path(candidate, index_dir)
                or (
                    candidate.resolve().parent == index_dir.resolve()
                    and _MANAGED_PARTIAL_INDEX_NAME_RE.fullmatch(candidate.name)
                    is not None
                )
            ):
                continue
            candidate.unlink()
            removed.append(candidate)
        if any(path.parent == index_dir for path in removed):
            _fsync_directory(index_dir)
        try:
            index_dir.rmdir()
        except OSError:
            # Unknown files are deliberately preserved.
            pass
    return tuple(removed)


def _empty_state() -> dict[str, Any]:
    target_scope = {"accessions": [], "periods": []}
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "generated_at": None,
        "source_page": {
            "url": FORM_13F_DATASETS_PAGE_URL,
            "sha256": None,
        },
        "sources": {},
        "archive_sources": {},
        "target_scope": {
            **target_scope,
            "sha256": _sha256_bytes(_canonical_json_bytes(target_scope)),
        },
        "index": None,
        "summary": {
            "datasets": 0,
            "archive_filings": 0,
            "submissions": 0,
            "information_table_rows": 0,
        },
    }


def _index_path_from_state(state: Mapping[str, Any], state_path: Path) -> Path | None:
    index = state.get("index")
    if not isinstance(index, dict):
        return None
    raw_path = index.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        return None
    path = Path(raw_path)
    if not path.is_absolute():
        path = Path(state_path).parent / path
    return path


def _relative_index_path(index_path: Path, state_path: Path) -> str:
    try:
        return str(Path(index_path).relative_to(Path(state_path).parent))
    except ValueError:
        return str(Path(index_path).resolve())


def _validate_state(
    state: Mapping[str, Any],
    *,
    state_path: Path,
    verify_index_checksum: bool,
) -> None:
    if state.get("schema_version") != STATE_SCHEMA_VERSION:
        raise Sec13FBulkError("unsupported SEC 13F bulk state schema")
    clean_plan = state.get("clean_rebuild_plan_sha256")
    if clean_plan is not None and not _SHA256_RE.fullmatch(str(clean_plan)):
        raise Sec13FBulkError("SEC 13F state clean-rebuild plan is invalid")
    source_page = state.get("source_page")
    if not isinstance(source_page, dict):
        raise Sec13FBulkError("SEC 13F state source_page must be an object")
    if source_page.get("url") != FORM_13F_DATASETS_PAGE_URL:
        raise Sec13FBulkError("SEC 13F state has an unexpected source page")
    page_sha = source_page.get("sha256")
    if page_sha is not None and not _SHA256_RE.fullmatch(str(page_sha)):
        raise Sec13FBulkError("SEC 13F state has an invalid page checksum")
    sources = state.get("sources")
    if not isinstance(sources, dict):
        raise Sec13FBulkError("SEC 13F state sources must be an object")
    for raw_url, source in sources.items():
        url = normalize_sec_13f_dataset_url(raw_url)
        if url != raw_url or not isinstance(source, dict):
            raise Sec13FBulkError("SEC 13F state has a malformed source entry")
        if source.get("url") != url:
            raise Sec13FBulkError("SEC 13F state source URL mismatch")
        if not _SHA256_RE.fullmatch(str(source.get("sha256") or "")):
            raise Sec13FBulkError("SEC 13F state source checksum is invalid")
        for count_key in (
            "submission_count",
            "information_table_count",
        ):
            if type(source.get(count_key)) is not int or source[count_key] < 0:
                raise Sec13FBulkError(
                    f"SEC 13F state source has invalid {count_key}"
                )
        schema = source.get("schema")
        if (
            not isinstance(schema, dict)
            or not isinstance(schema.get("submission_columns"), list)
            or not isinstance(schema.get("infotable_columns"), list)
        ):
            raise Sec13FBulkError("SEC 13F state source schema is invalid")
    archive_sources = state.get("archive_sources", {})
    if not isinstance(archive_sources, dict):
        raise Sec13FBulkError("SEC 13F archive_sources must be an object")
    for raw_url, source in archive_sources.items():
        url = normalize_sec_archive_url(raw_url)
        if url != raw_url or not isinstance(source, dict):
            raise Sec13FBulkError("SEC 13F state has a malformed archive source")
        if source.get("url") != url:
            raise Sec13FBulkError("SEC 13F archive source URL mismatch")
        index_url = normalize_sec_archive_url(source.get("index_url"))
        if not _ARCHIVE_INDEX_PATH_RE.fullmatch(urlsplit(index_url).path):
            raise Sec13FBulkError("SEC 13F archive source index URL is invalid")
        for checksum_field in ("sha256", "index_sha256"):
            if not _SHA256_RE.fullmatch(str(source.get(checksum_field) or "")):
                raise Sec13FBulkError(
                    f"SEC 13F archive source {checksum_field} is invalid"
                )
        _normalize_accession(source.get("accession"))
        _normalize_cik(source.get("cik"))
        if not _valid_iso_date(source.get("report_date")):
            raise Sec13FBulkError("SEC 13F archive report date is invalid")
        if type(source.get("information_table_count")) is not int or source[
            "information_table_count"
        ] < 0:
            raise Sec13FBulkError("SEC 13F archive row count is invalid")
    target_scope = state.get("target_scope")
    if not isinstance(target_scope, dict):
        raise Sec13FBulkError("SEC 13F target_scope must be an object")
    accessions = target_scope.get("accessions")
    periods = target_scope.get("periods")
    if (
        not isinstance(accessions, list)
        or accessions != sorted(set(accessions))
        or any(not _ACCESSION_RE.fullmatch(str(value)) for value in accessions)
        or not isinstance(periods, list)
        or any(
            not isinstance(period, dict)
            or set(period) != {"cik", "report_date"}
            for period in periods
        )
    ):
        raise Sec13FBulkError("SEC 13F target_scope values are invalid")
    normalized_periods = [
        {"cik": _normalize_cik(period["cik"]), "report_date": _normalize_date(period["report_date"])}
        for period in periods
    ]
    expected_periods = [
        {"cik": cik, "report_date": report_date}
        for cik, report_date in sorted({
            (period["cik"], period["report_date"])
            for period in normalized_periods
        })
    ]
    if periods != expected_periods:
        raise Sec13FBulkError("SEC 13F target periods are not canonical")
    scope_payload = {"accessions": accessions, "periods": periods}
    if target_scope.get("sha256") != _sha256_bytes(
        _canonical_json_bytes(scope_payload)
    ):
        raise Sec13FBulkError("SEC 13F target_scope checksum mismatch")
    index = state.get("index")
    if index is None:
        if sources or archive_sources:
            raise Sec13FBulkError("SEC 13F state has sources but no index")
        return
    if not isinstance(index, dict):
        raise Sec13FBulkError("SEC 13F state index must be an object")
    if index.get("schema_version") != INDEX_SCHEMA_VERSION:
        raise Sec13FBulkError("unsupported SEC 13F evidence-index schema")
    if not _SHA256_RE.fullmatch(str(index.get("sha256") or "")):
        raise Sec13FBulkError("SEC 13F state index checksum is invalid")
    if type(index.get("size_bytes")) is not int or index["size_bytes"] < 0:
        raise Sec13FBulkError("SEC 13F state index size is invalid")
    index_path = _index_path_from_state(state, state_path)
    if index_path is None or not index_path.is_file():
        raise Sec13FBulkError("SEC 13F evidence index is missing")
    if index_path.stat().st_size != index["size_bytes"]:
        raise Sec13FBulkError("SEC 13F evidence index size does not match state")
    if verify_index_checksum and _sha256_file(index_path) != index["sha256"]:
        raise Sec13FBulkError("SEC 13F evidence index checksum mismatch")


def load_13f_bulk_index(
    state_path: Path = DEFAULT_STATE_PATH,
    *,
    verify_index_checksum: bool = False,
) -> dict[str, Any]:
    """Load and validate the source-state manifest.

    The default performs constant-time file-size validation.  Weekly/full
    audits can request a full SQLite SHA-256 verification.
    """

    path = Path(state_path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return _empty_state()
    except (OSError, json.JSONDecodeError) as exc:
        raise Sec13FBulkError(f"cannot read SEC 13F state {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise Sec13FBulkError("SEC 13F state root must be an object")
    _validate_state(
        payload,
        state_path=path,
        verify_index_checksum=verify_index_checksum,
    )
    return payload


def _completed_clean_rebuild_receipt_for_state(
    state: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind one completed clean index generation to its exact state."""

    clean_plan = state.get("clean_rebuild_plan_sha256")
    index = state.get("index")
    generated_at = state.get("generated_at")
    if (
        not isinstance(clean_plan, str)
        or _SHA256_RE.fullmatch(clean_plan) is None
        or not isinstance(index, dict)
        or not isinstance(index.get("sha256"), str)
        or _SHA256_RE.fullmatch(index["sha256"]) is None
        or not isinstance(generated_at, str)
        or not generated_at
    ):
        raise Sec13FBulkError(
            "SEC 13F state is not a completed clean-rebuild generation"
        )
    receipt = {
        "schema_version": COMPLETED_CLEAN_REBUILD_RECEIPT_SCHEMA_VERSION,
        "receipt_scope": COMPLETED_CLEAN_REBUILD_RECEIPT_SCOPE,
        "clean_rebuild_plan_sha256": clean_plan,
        "state_sha256": _sha256_bytes(_canonical_json_bytes(state)),
        "index_sha256": index["sha256"],
        "generated_at": generated_at,
    }
    receipt["receipt_sha256"] = _sha256_bytes(
        _canonical_json_bytes(receipt)
    )
    return receipt


def build_completed_clean_rebuild_receipt(
    state_path: Path = DEFAULT_STATE_PATH,
    *,
    verify_index_checksum: bool = True,
) -> dict[str, Any]:
    """Return a private content receipt for one accepted clean index.

    The receipt is deliberately not a general cache key.  A higher-level
    orchestration attempt must persist and later supply the exact receipt
    before a completed generation can be reused.  It never attests that the
    separate, idempotent fund-file backfill has run or completed.
    """

    state = load_13f_bulk_index(
        state_path,
        verify_index_checksum=verify_index_checksum,
    )
    return _completed_clean_rebuild_receipt_for_state(state)


def completed_clean_rebuild_receipt_matches(
    receipt: Mapping[str, Any] | None,
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    expected_plan_sha256: str | None = None,
    verify_index_checksum: bool = True,
) -> bool:
    """Validate a receipt against the exact durable state and SQLite bytes."""

    if not isinstance(receipt, Mapping):
        return False
    try:
        state = load_13f_bulk_index(
            state_path,
            verify_index_checksum=verify_index_checksum,
        )
        expected = _completed_clean_rebuild_receipt_for_state(state)
    except (OSError, Sec13FBulkError):
        return False
    if dict(receipt) != expected:
        return False
    return (
        expected_plan_sha256 is None
        or expected["clean_rebuild_plan_sha256"] == expected_plan_sha256
    )


def load_completed_clean_rebuild_receipt(
    receipt_path: Path = DEFAULT_COMPLETED_RECEIPT_PATH,
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    verify_index_checksum: bool = False,
) -> dict[str, Any] | None:
    """Load a small receipt only when it still binds the exact index state."""

    path = Path(receipt_path)
    try:
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or not completed_clean_rebuild_receipt_matches(
        payload,
        state_path=state_path,
        verify_index_checksum=verify_index_checksum,
    ):
        return None
    return payload


def _legacy_index_adoption_receipt_for_state(
    state: Mapping[str, Any],
    *,
    clean_rebuild_plan_sha256: str,
    dataset_urls: Sequence[str],
    archive_targets: Sequence[Mapping[str, str]],
) -> dict[str, Any]:
    """Bind a one-time unpublished adoption to an exact pre-plan state."""

    index = state.get("index")
    generated_at = state.get("generated_at")
    target_scope = state.get("target_scope")
    normalized_urls = sorted(
        {normalize_sec_13f_dataset_url(url) for url in dataset_urls},
        key=_dataset_url_sort_key,
    )
    normalized_archive_targets = _normalize_archive_targets(archive_targets)
    if (
        state.get("clean_rebuild_plan_sha256") is not None
        or not isinstance(clean_rebuild_plan_sha256, str)
        or _SHA256_RE.fullmatch(clean_rebuild_plan_sha256) is None
        or not isinstance(index, dict)
        or index.get("schema_version") != INDEX_SCHEMA_VERSION
        or not isinstance(index.get("sha256"), str)
        or _SHA256_RE.fullmatch(index["sha256"]) is None
        or not isinstance(generated_at, str)
        or not generated_at
        or not isinstance(target_scope, dict)
        or not isinstance(target_scope.get("sha256"), str)
        or _SHA256_RE.fullmatch(target_scope["sha256"]) is None
        or set(state.get("sources", {})) != set(normalized_urls)
    ):
        raise Sec13FBulkError(
            "SEC 13F state is not eligible for unpublished pre-plan adoption"
        )
    receipt = {
        "schema_version": COMPLETED_CLEAN_REBUILD_RECEIPT_SCHEMA_VERSION,
        "receipt_scope": LEGACY_INDEX_ADOPTION_RECEIPT_SCOPE,
        "clean_rebuild_plan_sha256": clean_rebuild_plan_sha256,
        "state_sha256": _sha256_bytes(_canonical_json_bytes(state)),
        "index_sha256": index["sha256"],
        "target_scope_sha256": target_scope["sha256"],
        "dataset_urls_sha256": _sha256_bytes(_canonical_json_bytes({
            "dataset_urls": normalized_urls,
        })),
        "archive_targets_sha256": _sha256_bytes(_canonical_json_bytes({
            "archive_targets": normalized_archive_targets,
        })),
        "generated_at": generated_at,
    }
    receipt["receipt_sha256"] = _sha256_bytes(
        _canonical_json_bytes(receipt)
    )
    return receipt


def legacy_index_adoption_receipt_matches(
    receipt: Mapping[str, Any] | None,
    *,
    state_path: Path,
    expected_plan_sha256: str,
    dataset_urls: Sequence[str],
    archive_targets: Sequence[Mapping[str, str]],
    verify_index_checksum: bool = True,
) -> bool:
    """Validate an explicit unpublished-adoption receipt and index bytes."""

    if (
        not isinstance(receipt, Mapping)
        or receipt.get("receipt_scope") != LEGACY_INDEX_ADOPTION_RECEIPT_SCOPE
    ):
        return False
    try:
        state = load_13f_bulk_index(
            state_path,
            verify_index_checksum=verify_index_checksum,
        )
        expected = _legacy_index_adoption_receipt_for_state(
            state,
            clean_rebuild_plan_sha256=expected_plan_sha256,
            dataset_urls=dataset_urls,
            archive_targets=archive_targets,
        )
    except (OSError, Sec13FBulkError):
        return False
    return dict(receipt) == expected


def _create_schema(connection: sqlite3.Connection) -> None:
    # Evidence lookups use the accession-leading primary key or the filer/date
    # submissions index. Extra CUSIP indexes duplicate a large part of every
    # row without supporting a production query. Leave existing immutable
    # generations alone; newly built generations do not need those indexes.
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS submissions (
            accession TEXT PRIMARY KEY,
            cik TEXT NOT NULL,
            report_date TEXT NOT NULL,
            filing_date TEXT NOT NULL,
            submission_type TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_sha256 TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS submissions_cik_report
            ON submissions(cik, report_date);
        CREATE TABLE IF NOT EXISTS information_table (
            accession TEXT NOT NULL,
            infotable_sk TEXT NOT NULL,
            reported_issuer TEXT NOT NULL,
            reported_class TEXT NOT NULL,
            reported_cusip TEXT NOT NULL,
            cusip_key TEXT NOT NULL,
            reported_figi TEXT,
            reported_value TEXT NOT NULL,
            reported_shares TEXT NOT NULL,
            share_amount_type TEXT NOT NULL,
            put_call TEXT NOT NULL,
            investment_discretion TEXT,
            other_manager TEXT,
            source_url TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,
            PRIMARY KEY(accession, infotable_sk),
            FOREIGN KEY(accession) REFERENCES submissions(accession)
                ON DELETE CASCADE
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS filing_chain (
            accession TEXT PRIMARY KEY,
            acceptance_datetime TEXT,
            cover_is_amendment INTEGER,
            cover_amendment_type TEXT,
            cover_table_entry_total INTEGER,
            cover_table_value_total TEXT,
            cover_metadata_consistent INTEGER NOT NULL,
            source_url TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,
            FOREIGN KEY(accession) REFERENCES submissions(accession)
                ON DELETE CASCADE
        ) WITHOUT ROWID;
        """
    )
    connection.execute(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
        ("schema_version", str(INDEX_SCHEMA_VERSION)),
    )


def _open_index(path: Path, *, read_only: bool) -> sqlite3.Connection:
    if read_only:
        uri = f"file:{Path(path).resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
    else:
        connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    if read_only:
        schema = connection.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()
        if schema is None or schema[0] != str(INDEX_SCHEMA_VERSION):
            connection.close()
            raise Sec13FBulkError("unsupported SEC 13F SQLite index schema")
    return connection


def _normalized_header(value: object | None) -> str:
    return re.sub(r"[^A-Z0-9_]", "", str(value or "").strip().upper())


def _normalize_accession(value: object | None) -> str:
    accession = _normalize_text(value)
    if not _ACCESSION_RE.fullmatch(accession):
        raise DatasetParseError(f"invalid Form 13F accession: {value!r}")
    return accession


def _normalize_cik(value: object | None) -> str:
    cik = _normalize_text(value)
    if not cik.isdigit() or not 1 <= len(cik) <= 10:
        raise DatasetParseError(f"invalid Form 13F filer CIK: {value!r}")
    return cik.zfill(10)


def _normalize_date(value: object | None) -> str:
    raw = _normalize_text(value)
    for pattern in ("%d-%b-%Y", "%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw, pattern).date().isoformat()
        except ValueError:
            continue
    raise DatasetParseError(f"invalid Form 13F date: {value!r}")


def _decimal_text(value: object | None, *, field: str) -> str:
    raw = _normalize_text("" if value is None else str(value)).replace(",", "")
    try:
        numeric = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise DatasetParseError(f"invalid {field}: {value!r}") from exc
    if not numeric.is_finite() or numeric < 0:
        raise DatasetParseError(f"invalid {field}: {value!r}")
    if numeric == 0:
        return "0"
    normalized = format(numeric, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _safe_archive(payload: bytes) -> tuple[zipfile.ZipFile, dict[str, zipfile.ZipInfo]]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except (zipfile.BadZipFile, OSError) as exc:
        raise DatasetParseError(f"invalid SEC Form 13F ZIP: {exc}") from exc
    members: dict[str, zipfile.ZipInfo] = {}
    try:
        archive_infos = archive.infolist()
        infos = [info for info in archive_infos if not info.is_dir()]
        if not infos or len(archive_infos) > MAX_ARCHIVE_MEMBERS:
            raise DatasetParseError("SEC Form 13F ZIP has an unsafe member count")
        if sum(info.file_size for info in infos) > MAX_ARCHIVE_UNCOMPRESSED_BYTES:
            raise DatasetParseError(
                "SEC Form 13F ZIP exceeds the uncompressed size limit"
            )
        seen_paths: set[str] = set()
        directory_names: set[str] = set()
        file_parents: set[str | None] = set()
        for info in archive_infos:
            raw_name = info.filename
            member_name = raw_name[:-1] if info.is_dir() else raw_name
            path = PurePosixPath(member_name)
            parts = member_name.split("/")
            unix_mode = (
                (info.external_attr >> 16) & 0xFFFF
                if info.create_system == 3
                else 0
            )
            file_type = stat.S_IFMT(unix_mode)
            if (
                not member_name
                or "\\" in raw_name
                or path.is_absolute()
                or any(part in {"", ".", ".."} for part in parts)
                or re.fullmatch(r"[A-Za-z]:", parts[0]) is not None
                or len(parts) > (1 if info.is_dir() else 2)
                or file_type not in {0, stat.S_IFREG, stat.S_IFDIR}
                or (info.is_dir() and file_type not in {0, stat.S_IFDIR})
                or (not info.is_dir() and file_type == stat.S_IFDIR)
                or (info.is_dir() and info.file_size != 0)
                or info.flag_bits & 0x1
                or (not info.is_dir() and info.file_size > MAX_SINGLE_MEMBER_BYTES)
            ):
                raise DatasetParseError(
                    f"unsafe SEC Form 13F ZIP member: {info.filename!r}"
                )
            path_key = "/".join(parts).casefold()
            if path_key in seen_paths:
                raise DatasetParseError(
                    f"duplicate SEC Form 13F ZIP member: {info.filename!r}"
                )
            seen_paths.add(path_key)
            if info.is_dir():
                directory_names.add(parts[0])
                continue
            parent = parts[0] if len(parts) == 2 else None
            file_parents.add(parent)
            key = path.name.upper()
            if key in members:
                raise DatasetParseError(
                    f"duplicate SEC Form 13F ZIP member: {path.name!r}"
                )
            members[key] = info
        if len(file_parents) != 1:
            raise DatasetParseError(
                "SEC Form 13F ZIP members do not share one common directory layout"
            )
        common_parent = next(iter(file_parents))
        expected_directories = {common_parent} if common_parent is not None else set()
        if directory_names != expected_directories and directory_names:
            raise DatasetParseError(
                "SEC Form 13F ZIP has an unexpected directory layout"
            )
        missing = {"SUBMISSION.TSV", "INFOTABLE.TSV"} - set(members)
        if missing:
            raise DatasetParseError(
                "SEC Form 13F ZIP is missing required members: "
                + ", ".join(sorted(missing))
            )
        return archive, members
    except Exception:
        archive.close()
        raise


def _open_tsv(
    archive: zipfile.ZipFile,
    member: zipfile.ZipInfo,
    *,
    required_columns: frozenset[str],
) -> tuple[io.TextIOWrapper, csv.reader, list[str]]:
    try:
        raw = archive.open(member, "r")
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="strict", newline="")
        reader = csv.reader(text, delimiter="\t")
        header = next(reader)
    except (KeyError, RuntimeError, StopIteration, UnicodeError, zipfile.BadZipFile) as exc:
        raise DatasetParseError(
            f"cannot read SEC Form 13F TSV {member.filename!r}: {exc}"
        ) from exc
    normalized = [_normalized_header(column) for column in header]
    if any(not column for column in normalized) or len(set(normalized)) != len(normalized):
        text.close()
        raise DatasetParseError(
            f"SEC Form 13F TSV has duplicate or blank columns: {member.filename!r}"
        )
    missing = required_columns - set(normalized)
    if missing:
        text.close()
        raise DatasetParseError(
            f"SEC Form 13F TSV {member.filename!r} is missing required columns: "
            + ", ".join(sorted(missing))
        )
    return text, reader, normalized


def _row_mapping(
    row: Sequence[str],
    columns: Sequence[str],
    *,
    member_name: str,
    line_number: int,
) -> dict[str, str] | None:
    if not row or all(not str(cell).strip() for cell in row):
        return None
    if len(row) != len(columns):
        raise DatasetParseError(
            f"SEC Form 13F TSV {member_name!r} has {len(row)} values for "
            f"{len(columns)} columns on line {line_number}"
        )
    if any("\x00" in cell for cell in row):
        raise DatasetParseError(
            f"SEC Form 13F TSV {member_name!r} contains a NUL byte"
        )
    return dict(zip(columns, row, strict=True))


def _submission_record(
    row: Mapping[str, str],
    *,
    source_url: str,
    source_sha256: str,
) -> dict[str, str]:
    submission_type = _normalize_text(row.get("SUBMISSIONTYPE")).upper()
    if not submission_type.startswith("13F-"):
        raise DatasetParseError(
            f"unexpected submission type in Form 13F data set: {submission_type!r}"
        )
    return {
        "accession": _normalize_accession(row.get("ACCESSION_NUMBER")),
        "cik": _normalize_cik(row.get("CIK")),
        "report_date": _normalize_date(row.get("PERIODOFREPORT")),
        "filing_date": _normalize_date(row.get("FILING_DATE")),
        "submission_type": submission_type,
        "source_url": source_url,
        "source_sha256": source_sha256,
    }


def _information_record(
    row: Mapping[str, str],
    *,
    source_url: str,
    source_sha256: str,
) -> dict[str, str | None]:
    reported_cusip = str(row.get("CUSIP") or "").strip()
    if not reported_cusip:
        raise DatasetParseError("Form 13F information-table row has a blank CUSIP")
    infotable_sk = _normalize_text(row.get("INFOTABLE_SK"))
    if not infotable_sk:
        raise DatasetParseError("Form 13F information-table row has a blank key")
    if any(
        field not in row or not isinstance(row[field], str)
        for field in ("NAMEOFISSUER", "TITLEOFCLASS")
    ):
        raise DatasetParseError(
            "Form 13F information-table row has a missing or non-string "
            "issuer or class"
        )
    issuer = row["NAMEOFISSUER"].strip()
    security_class = row["TITLEOFCLASS"].strip()
    figi = str(row.get("FIGI") or "").strip() or None
    return {
        "accession": _normalize_accession(row.get("ACCESSION_NUMBER")),
        "infotable_sk": infotable_sk,
        "reported_issuer": issuer,
        "reported_class": security_class,
        "reported_cusip": reported_cusip,
        "cusip_key": reported_cusip.upper(),
        "reported_figi": figi,
        "reported_value": _decimal_text(row.get("VALUE"), field="reported value"),
        "reported_shares": _decimal_text(
            row.get("SSHPRNAMT"), field="reported shares"
        ),
        "share_amount_type": _normalize_text(row.get("SSHPRNAMTTYPE")).upper(),
        "put_call": _normalize_text(row.get("PUTCALL")).upper(),
        "investment_discretion": (
            str(row.get("INVESTMENTDISCRETION") or "").strip() or None
        ),
        "other_manager": str(row.get("OTHERMANAGER") or "").strip() or None,
        "source_url": source_url,
        "source_sha256": source_sha256,
    }


def _insert_submission(
    connection: sqlite3.Connection,
    record: Mapping[str, str],
) -> None:
    values = tuple(record[key] for key in (
        "accession",
        "cik",
        "report_date",
        "filing_date",
        "submission_type",
        "source_url",
        "source_sha256",
    ))
    try:
        connection.execute(
            """
            INSERT INTO submissions(
                accession, cik, report_date, filing_date, submission_type,
                source_url, source_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
    except sqlite3.IntegrityError as exc:
        raise DatasetParseError(
            f"duplicate or conflicting Form 13F submission {record['accession']}"
        ) from exc


def _insert_information_record(
    connection: sqlite3.Connection,
    record: Mapping[str, str | None],
) -> None:
    keys = (
        "accession",
        "infotable_sk",
        "reported_issuer",
        "reported_class",
        "reported_cusip",
        "cusip_key",
        "reported_figi",
        "reported_value",
        "reported_shares",
        "share_amount_type",
        "put_call",
        "investment_discretion",
        "other_manager",
        "source_url",
        "source_sha256",
    )
    values = tuple(record[key] for key in keys)
    try:
        connection.execute(
            """
            INSERT INTO information_table(
                accession, infotable_sk, reported_issuer, reported_class,
                reported_cusip, cusip_key, reported_figi, reported_value,
                reported_shares, share_amount_type, put_call,
                investment_discretion, other_manager, source_url, source_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
    except sqlite3.IntegrityError as exc:
        raise DatasetParseError(
            "duplicate or conflicting Form 13F information-table key "
            f"{record['accession']}/{record['infotable_sk']}"
        ) from exc


def ingest_13f_dataset_zip(
    connection: sqlite3.Connection,
    payload: bytes,
    *,
    source_url: str,
    source_sha256: str | None = None,
    target_accessions: Iterable[str] | None = None,
    target_periods: Iterable[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Validate and stream one quarterly SEC ZIP into an open SQLite index."""

    canonical_url = normalize_sec_13f_dataset_url(source_url)
    raw_payload = bytes(payload)
    payload_sha256 = source_sha256 or _sha256_bytes(raw_payload)
    if not _SHA256_RE.fullmatch(payload_sha256):
        raise DatasetParseError("invalid Form 13F source checksum")
    if _sha256_bytes(raw_payload) != payload_sha256:
        raise DatasetParseError("Form 13F source checksum does not match payload")
    normalized_target_accessions = (
        {_normalize_accession(value) for value in target_accessions}
        if target_accessions is not None
        else None
    )
    normalized_target_periods = (
        {
            (_normalize_cik(cik), _normalize_date(report_date))
            for cik, report_date in target_periods
        }
        if target_periods is not None
        else None
    )

    def eligible(record: Mapping[str, str]) -> bool:
        if normalized_target_accessions is None and normalized_target_periods is None:
            return True
        return bool(
            normalized_target_accessions is not None
            and record["accession"] in normalized_target_accessions
        ) or bool(
            normalized_target_periods is not None
            and (record["cik"], record["report_date"])
            in normalized_target_periods
        )

    archive, members = _safe_archive(raw_payload)
    try:
        submission_text, submission_reader, submission_columns = _open_tsv(
            archive,
            members["SUBMISSION.TSV"],
            required_columns=_SUBMISSION_REQUIRED_COLUMNS,
        )
        all_submissions: set[str] = set()
        retained_submissions: set[str] = set()
        source_submission_count = 0
        submission_count = 0
        try:
            for line_number, row in enumerate(submission_reader, start=2):
                mapping = _row_mapping(
                    row,
                    submission_columns,
                    member_name=members["SUBMISSION.TSV"].filename,
                    line_number=line_number,
                )
                if mapping is None:
                    continue
                record = _submission_record(
                    mapping,
                    source_url=canonical_url,
                    source_sha256=payload_sha256,
                )
                if record["accession"] in all_submissions:
                    raise DatasetParseError(
                        "duplicate Form 13F submission in source: "
                        f"{record['accession']}"
                    )
                all_submissions.add(record["accession"])
                source_submission_count += 1
                if eligible(record):
                    _insert_submission(connection, record)
                    retained_submissions.add(record["accession"])
                    submission_count += 1
        except (UnicodeError, csv.Error, zipfile.BadZipFile) as exc:
            raise DatasetParseError(
                f"cannot parse SEC Form 13F submissions TSV: {exc}"
            ) from exc
        finally:
            submission_text.close()

        info_text, info_reader, info_columns = _open_tsv(
            archive,
            members["INFOTABLE.TSV"],
            required_columns=_INFOTABLE_REQUIRED_COLUMNS,
        )
        source_information_count = 0
        information_count = 0
        try:
            for line_number, row in enumerate(info_reader, start=2):
                mapping = _row_mapping(
                    row,
                    info_columns,
                    member_name=members["INFOTABLE.TSV"].filename,
                    line_number=line_number,
                )
                if mapping is None:
                    continue
                accession = _normalize_accession(mapping.get("ACCESSION_NUMBER"))
                source_information_count += 1
                if accession not in all_submissions:
                    raise DatasetParseError(
                        "Form 13F information-table row references an accession "
                        f"missing from its SUBMISSION table: {accession}"
                    )
                if accession not in retained_submissions:
                    continue
                record = _information_record(
                    mapping,
                    source_url=canonical_url,
                    source_sha256=payload_sha256,
                )
                _insert_information_record(connection, record)
                information_count += 1
        except (UnicodeError, csv.Error, zipfile.BadZipFile) as exc:
            raise DatasetParseError(
                f"cannot parse SEC Form 13F information TSV: {exc}"
            ) from exc
        finally:
            info_text.close()
    finally:
        archive.close()
    return {
        "url": canonical_url,
        "sha256": payload_sha256,
        "submission_count": submission_count,
        "source_submission_count": source_submission_count,
        "information_table_count": information_count,
        "source_information_table_count": source_information_count,
        "schema": {
            "submission_columns": submission_columns,
            "infotable_columns": info_columns,
            "schema_sha256": _sha256_bytes(_canonical_json_bytes({
                "submission_columns": submission_columns,
                "infotable_columns": info_columns,
            })),
        },
    }


def parse_13f_dataset_zip(
    payload: bytes,
    *,
    source_url: str,
) -> dict[str, Any]:
    """Parse a small/in-memory fixture and return normalized evidence rows.

    Production refreshes call :func:`ingest_13f_dataset_zip` directly so real
    quarterly archives are streamed into SQLite instead of materialized as a
    Python list.
    """

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    _create_schema(connection)
    try:
        metadata = ingest_13f_dataset_zip(
            connection,
            payload,
            source_url=source_url,
        )
        submissions = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM submissions ORDER BY accession"
            )
        ]
        records = [
            dict(row)
            for row in connection.execute(
                """
                SELECT i.*, s.cik, s.report_date, s.filing_date,
                       s.submission_type
                  FROM information_table AS i
                  JOIN submissions AS s USING(accession)
                 ORDER BY i.accession, i.infotable_sk
                """
            )
        ]
        return {**metadata, "submissions": submissions, "records": records}
    finally:
        connection.close()


def _sgml_header_value(text: str, name: str) -> str:
    tag_name = name.replace(" ", "-")
    tag_match = re.search(
        rf"<{re.escape(tag_name)}>\s*([^\r\n<]+)",
        text,
        re.IGNORECASE,
    )
    if tag_match:
        return _normalize_text(tag_match.group(1))
    label_match = re.search(
        rf"^\s*{re.escape(name)}\s*:\s*([^\r\n]+)",
        text,
        re.IGNORECASE | re.MULTILINE,
    )
    return _normalize_text(label_match.group(1)) if label_match else ""


def _document_value(document: str, tag: str) -> str:
    match = re.search(
        rf"^\s*<{re.escape(tag)}>\s*([^\r\n<]+)",
        document,
        re.IGNORECASE | re.MULTILINE,
    )
    return _normalize_text(match.group(1)) if match else ""


def _document_text(document: str) -> str:
    match = re.search(
        r"<TEXT>(.*?)</TEXT>",
        document,
        re.IGNORECASE | re.DOTALL,
    )
    return match.group(1) if match else document


def _local_xml_name(tag: object) -> str:
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def _xml_child_text(element: ElementTree.Element, name: str) -> str:
    target = name.casefold()
    for child in element.iter():
        if _local_xml_name(child.tag).casefold() == target:
            value = "".join(child.itertext()).strip()
            if value:
                return value
    return ""


def _normalize_acceptance_datetime(value: object | None) -> str | None:
    """Normalize an SEC acceptance timestamp without inventing a timezone.

    Complete-submission headers encode local SEC acceptance time as fourteen
    digits.  Its lexical order is chronological within a filing chain, which
    is all the duplicate/restatement selector needs.
    """

    raw = re.sub(r"[^0-9]", "", str(value or ""))
    if not raw:
        return None
    if len(raw) != 14:
        raise DatasetParseError("invalid SEC acceptance datetime")
    try:
        datetime.strptime(raw, "%Y%m%d%H%M%S")
    except ValueError as exc:
        raise DatasetParseError("invalid SEC acceptance datetime") from exc
    return raw


def _parse_archive_cover_metadata(content: str) -> dict[str, Any] | None:
    """Return structured Form 13F cover metadata from one XML document."""

    candidate = content.strip()
    candidate = re.sub(r"^\s*<XML>\s*", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s*</XML>\s*$", "", candidate, flags=re.IGNORECASE)
    if "<" not in candidate:
        return None
    try:
        root = ElementTree.fromstring(candidate)
    except ElementTree.ParseError:
        return None

    def unique_text(name: str) -> str:
        values = {
            "".join(element.itertext()).strip()
            for element in root.iter()
            if _local_xml_name(element.tag).casefold() == name.casefold()
            and "".join(element.itertext()).strip()
        }
        if len(values) > 1:
            raise DatasetParseError(
                f"conflicting Form 13F cover values for {name}"
            )
        return next(iter(values), "")

    is_amendment_raw = unique_text("isAmendment").casefold()
    amendment_type = unique_text("amendmentType").upper()
    entry_total_raw = unique_text("tableEntryTotal")
    value_total_raw = unique_text("tableValueTotal")
    if not any((is_amendment_raw, amendment_type, entry_total_raw, value_total_raw)):
        return None
    if is_amendment_raw not in {"", "true", "false"}:
        raise DatasetParseError("invalid Form 13F cover isAmendment value")
    if amendment_type not in {"", "RESTATEMENT", "NEW HOLDINGS"}:
        raise DatasetParseError("invalid Form 13F cover amendmentType value")
    try:
        entry_total = int(entry_total_raw)
    except ValueError as exc:
        raise DatasetParseError("invalid Form 13F cover tableEntryTotal") from exc
    if entry_total < 0 or str(entry_total) != entry_total_raw.strip():
        raise DatasetParseError("invalid Form 13F cover tableEntryTotal")
    value_total = _decimal_text(
        value_total_raw,
        field="Form 13F cover tableValueTotal",
    )
    return {
        # Current SEC original filings commonly omit the optional false flag.
        # Header/cover consistency below still requires an original 13F-HR
        # and no amendmentType before that omission is treated as coherent.
        "is_amendment": is_amendment_raw == "true",
        "amendment_type": amendment_type or None,
        "table_entry_total": entry_total,
        "table_value_total": value_total,
    }


def _parse_archive_xml_rows(
    content: str,
    *,
    accession: str,
    source_url: str,
    source_sha256: str,
    document_number: int,
) -> list[dict[str, str | None]] | None:
    candidate = content.strip()
    candidate = re.sub(r"^\s*<XML>\s*", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s*</XML>\s*$", "", candidate, flags=re.IGNORECASE)
    if "<" not in candidate:
        return None
    try:
        root = ElementTree.fromstring(candidate)
    except ElementTree.ParseError:
        return None
    info_tables = [
        element
        for element in root.iter()
        if _local_xml_name(element.tag).casefold() == "infotable"
    ]
    if not info_tables:
        return None
    records: list[dict[str, str | None]] = []
    for row_number, element in enumerate(info_tables, start=1):
        mapping = {
            "ACCESSION_NUMBER": accession,
            "INFOTABLE_SK": f"archive:{document_number}:{row_number}",
            "NAMEOFISSUER": _xml_child_text(element, "nameOfIssuer"),
            "TITLEOFCLASS": _xml_child_text(element, "titleOfClass"),
            "CUSIP": _xml_child_text(element, "cusip"),
            "FIGI": _xml_child_text(element, "figi"),
            "VALUE": _xml_child_text(element, "value"),
            "SSHPRNAMT": _xml_child_text(element, "sshPrnamt"),
            "SSHPRNAMTTYPE": _xml_child_text(element, "sshPrnamtType"),
            "PUTCALL": _xml_child_text(element, "putCall"),
            "INVESTMENTDISCRETION": _xml_child_text(
                element, "investmentDiscretion"
            ),
            "OTHERMANAGER": _xml_child_text(element, "otherManager"),
        }
        records.append(
            _information_record(
                mapping,
                source_url=source_url,
                source_sha256=source_sha256,
            )
        )
    return records


class _HTMLTables(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._table_depth = 0
        self._rows: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        del attrs
        lowered = tag.casefold()
        if lowered == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._rows = []
        elif self._table_depth == 1 and lowered == "tr":
            self._row = []
        elif (
            self._table_depth == 1
            and lowered in {"td", "th"}
            and self._row is not None
        ):
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.casefold()
        if (
            self._table_depth == 1
            and lowered in {"td", "th"}
            and self._cell_parts is not None
            and self._row is not None
        ):
            self._row.append(_normalize_text("".join(self._cell_parts)))
            self._cell_parts = None
        elif self._table_depth == 1 and lowered == "tr" and self._row is not None:
            if any(self._row):
                assert self._rows is not None
                self._rows.append(self._row)
            self._row = None
        elif lowered == "table" and self._table_depth:
            if self._table_depth == 1:
                if self._rows:
                    self.tables.append(self._rows)
                self._rows = None
            self._table_depth -= 1


_LEGACY_HEADER_ALIASES = {
    "issuer": frozenset({"NAMEOFISSUER", "ISSUER"}),
    "security_class": frozenset({"TITLEOFCLASS", "CLASS"}),
    "cusip": frozenset({"CUSIP", "CUSIPNUMBER"}),
    "value": frozenset({"VALUE", "MARKETVALUE", "VALUEX1000"}),
    "shares": frozenset({
        "SSHPRNAMT",
        "SHARES",
        "SHARESPRNAMT",
        "SHARESORPRINCIPALAMOUNT",
        "SHSORPRNAMT",
    }),
    "amount_type": frozenset({"SSHPRNAMTTYPE", "SHPRN", "SHORPRN"}),
    "put_call": frozenset({"PUTCALL"}),
    "figi": frozenset({"FIGI"}),
}


def _legacy_header_key(value: object | None) -> str:
    return re.sub(r"[^A-Z0-9]", "", unescape(str(value or "")).upper())


def _legacy_column_map(row: Sequence[str]) -> dict[str, int] | None:
    normalized = [_legacy_header_key(value) for value in row]
    result: dict[str, int] = {}
    for logical_name, aliases in _LEGACY_HEADER_ALIASES.items():
        indices = [index for index, value in enumerate(normalized) if value in aliases]
        if len(indices) > 1:
            return None
        if indices:
            result[logical_name] = indices[0]
    required = {"issuer", "security_class", "cusip", "value", "shares", "amount_type"}
    return result if required.issubset(result) else None


def _legacy_table_records(
    rows: Sequence[Sequence[str]],
    *,
    accession: str,
    source_url: str,
    source_sha256: str,
    document_number: int,
) -> list[dict[str, str | None]] | None:
    header_index = -1
    columns: dict[str, int] | None = None
    for index, row in enumerate(rows):
        candidate = _legacy_column_map(row)
        if candidate is not None:
            header_index = index
            columns = candidate
            break
    if columns is None:
        return None
    records: list[dict[str, str | None]] = []
    for row_number, row in enumerate(rows[header_index + 1 :], start=1):
        if len(row) <= max(columns.values()):
            continue
        cusip = str(row[columns["cusip"]]).strip()
        if not cusip:
            continue
        mapping = {
            "ACCESSION_NUMBER": accession,
            "INFOTABLE_SK": f"archive:{document_number}:{row_number}",
            "NAMEOFISSUER": row[columns["issuer"]],
            "TITLEOFCLASS": row[columns["security_class"]],
            "CUSIP": cusip,
            "FIGI": row[columns["figi"]] if "figi" in columns else "",
            "VALUE": row[columns["value"]],
            "SSHPRNAMT": row[columns["shares"]],
            "SSHPRNAMTTYPE": row[columns["amount_type"]],
            "PUTCALL": row[columns["put_call"]] if "put_call" in columns else "",
            "INVESTMENTDISCRETION": "",
            "OTHERMANAGER": "",
        }
        try:
            records.append(
                _information_record(
                    mapping,
                    source_url=source_url,
                    source_sha256=source_sha256,
                )
            )
        except DatasetParseError:
            # A structurally explicit table with an invalid data row is not
            # safe to partially accept.
            raise
    return records or None


def _parse_archive_legacy_rows(
    content: str,
    *,
    accession: str,
    source_url: str,
    source_sha256: str,
    document_number: int,
) -> list[dict[str, str | None]] | None:
    html_tables = _HTMLTables()
    try:
        html_tables.feed(content)
    except Exception:
        html_tables.tables = []
    parsed_tables = [
        records
        for table in html_tables.tables
        if (
            records := _legacy_table_records(
                table,
                accession=accession,
                source_url=source_url,
                source_sha256=source_sha256,
                document_number=document_number,
            )
        )
    ]
    if len(parsed_tables) > 1:
        raise DatasetParseError("multiple structurally valid legacy HTML tables")
    if parsed_tables:
        return parsed_tables[0]

    blocks = re.findall(r"<TABLE[^>]*>(.*?)</TABLE>", content, re.I | re.S)
    fixed_width_tables: list[list[dict[str, str | None]]] = []
    for block in blocks:
        lines = block.splitlines()
        marker_indices = [
            index
            for index, line in enumerate(lines)
            if re.search(r"<S(?:>|\s)", line, re.I)
            and len(re.findall(r"<C(?:>|\s)", line, re.I)) >= 5
        ]
        if len(marker_indices) != 1:
            continue
        marker_index = marker_indices[0]
        positions = [
            match.start()
            for match in re.finditer(r"<[SC](?:>|\s[^>]*>)", lines[marker_index], re.I)
        ]
        if len(positions) < 6:
            continue
        name_indices = [
            index
            for index in range(marker_index)
            if "NAME OF ISSUER" in lines[index].upper()
        ]
        if not name_indices:
            continue
        header_start = max(0, name_indices[-1] - 1)
        header_rows: list[str] = []
        for start, end in zip(positions, positions[1:] + [None]):
            parts = [
                lines[index][start:end].strip()
                for index in range(header_start, marker_index)
                if not lines[index].lstrip().startswith("-")
            ]
            header_rows.append(" ".join(part for part in parts if part))
        columns = _legacy_column_map(header_rows)
        if columns is None:
            continue
        rows: list[list[str]] = []
        prior_identity = ["", "", ""]
        for line in lines[marker_index + 1 :]:
            if not line.strip() or "<PAGE>" in line.upper():
                continue
            cells = [
                line[start:end].strip()
                for start, end in zip(positions, positions[1:] + [None])
            ]
            value_start = positions[columns["value"]]
            numeric_tail = re.match(
                r"\s*(?P<value>[0-9,]+)\s+"
                r"(?P<shares>[0-9,.]+)\s*"
                r"(?P<amount_type>SH|PRN)\s*"
                r"(?P<put_call>PUT|CALL)?(?:\s|$)",
                line[value_start:],
                re.IGNORECASE,
            )
            if numeric_tail is None:
                continue
            cells[columns["value"]] = numeric_tail.group("value")
            cells[columns["shares"]] = numeric_tail.group("shares")
            cells[columns["amount_type"]] = numeric_tail.group("amount_type")
            if "put_call" in columns:
                cells[columns["put_call"]] = numeric_tail.group("put_call") or ""
            identity_indices = [
                columns["issuer"], columns["security_class"], columns["cusip"]
            ]
            current_identity = [cells[index] for index in identity_indices]
            if current_identity[0] and current_identity[1] and current_identity[2]:
                prior_identity = current_identity
            elif not any(current_identity) and all(prior_identity):
                for index, value in zip(identity_indices, prior_identity):
                    cells[index] = value
            else:
                continue
            rows.append(cells)
        records = _legacy_table_records(
            [header_rows, *rows],
            accession=accession,
            source_url=source_url,
            source_sha256=source_sha256,
            document_number=document_number,
        )
        if records:
            fixed_width_tables.append(records)
    if len(fixed_width_tables) > 1:
        raise DatasetParseError("multiple structurally valid fixed-width tables")
    if fixed_width_tables:
        return fixed_width_tables[0]

    parsed_text_tables: list[list[dict[str, str | None]]] = []
    for block in blocks:
        rows: list[list[str]] = []
        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if re.search(r"<C(?:>|\s)", line, re.IGNORECASE):
                cells = re.split(r"<C(?:>|\s[^>]*>)", line, flags=re.IGNORECASE)[1:]
            elif "\t" in line:
                cells = line.split("\t")
            elif "|" in line:
                cells = line.split("|")
            else:
                continue
            cleaned = [
                _normalize_text(unescape(re.sub(r"<[^>]+>", " ", cell)))
                for cell in cells
            ]
            rows.append(cleaned)
        records = _legacy_table_records(
            rows,
            accession=accession,
            source_url=source_url,
            source_sha256=source_sha256,
            document_number=document_number,
        )
        if records:
            parsed_text_tables.append(records)
    if len(parsed_text_tables) > 1:
        raise DatasetParseError("multiple structurally valid legacy text tables")
    return parsed_text_tables[0] if parsed_text_tables else None


def parse_sec_archive_submission(
    payload: bytes,
    *,
    cik: object,
    accession: object,
    report_date: object,
    source_url: str | None = None,
) -> dict[str, Any]:
    """Parse one exact SEC complete submission, preferring XML evidence."""

    normalized_cik = _normalize_cik(cik)
    normalized_accession = _normalize_accession(accession)
    normalized_report_date = _normalize_date(report_date)
    url = normalize_sec_archive_url(
        source_url or sec_archive_submission_url(normalized_cik, normalized_accession)
    )
    raw_payload = bytes(payload)
    source_sha256 = _sha256_bytes(raw_payload)
    try:
        text = raw_payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw_payload.decode("cp1252")
    header_accession = _sgml_header_value(text, "ACCESSION NUMBER")
    header_cik = _sgml_header_value(text, "CENTRAL INDEX KEY")
    header_report = _sgml_header_value(text, "CONFORMED PERIOD OF REPORT")
    form_type = _sgml_header_value(text, "CONFORMED SUBMISSION TYPE").upper()
    filing_date = _sgml_header_value(text, "FILED AS OF DATE")
    acceptance_datetime = _normalize_acceptance_datetime(
        _sgml_header_value(text, "ACCEPTANCE DATETIME")
    )
    if _normalize_accession(header_accession) != normalized_accession:
        raise DatasetParseError("SEC archive accession conflicts with target")
    if _normalize_cik(header_cik) != normalized_cik:
        raise DatasetParseError("SEC archive filer CIK conflicts with target")
    if _normalize_date(header_report) != normalized_report_date:
        raise DatasetParseError("SEC archive report date conflicts with target")
    if form_type not in {"13F-HR", "13F-HR/A"}:
        raise DatasetParseError(f"SEC archive is not a holdings report: {form_type!r}")
    normalized_filing_date = _normalize_date(filing_date)
    documents = re.findall(r"<DOCUMENT>(.*?)</DOCUMENT>", text, re.I | re.S)
    if not documents:
        raise DatasetParseError("SEC archive contains no SGML document blocks")
    xml_candidates: list[list[dict[str, str | None]]] = []
    legacy_candidates: list[list[dict[str, str | None]]] = []
    cover_candidates: list[dict[str, Any]] = []
    for document_number, document in enumerate(documents, start=1):
        document_type = _document_value(document, "TYPE").upper()
        filename = _document_value(document, "FILENAME").casefold()
        description = _document_value(document, "DESCRIPTION").casefold()
        content = _document_text(document)
        cover_metadata = _parse_archive_cover_metadata(content)
        if cover_metadata is not None:
            cover_candidates.append(cover_metadata)
        xml_rows = _parse_archive_xml_rows(
            content,
            accession=normalized_accession,
            source_url=url,
            source_sha256=source_sha256,
            document_number=document_number,
        )
        if xml_rows:
            xml_candidates.append(xml_rows)
            continue
        is_information_candidate = (
            document_type in {"INFORMATION TABLE", "13F-HR", "13F-HR/A"}
            or "info" in filename
            or "table" in filename
            or "information table" in description
        )
        if not is_information_candidate:
            continue
        legacy_rows = _parse_archive_legacy_rows(
            content,
            accession=normalized_accession,
            source_url=url,
            source_sha256=source_sha256,
            document_number=document_number,
        )
        if legacy_rows:
            legacy_candidates.append(legacy_rows)
    if len(xml_candidates) > 1:
        raise DatasetParseError("multiple XML information tables in SEC archive")
    if xml_candidates:
        records = xml_candidates[0]
        method = "sec_archive_xml"
    else:
        if len(legacy_candidates) != 1:
            detail = "none" if not legacy_candidates else "multiple"
            raise DatasetParseError(
                f"SEC archive has {detail} structurally provable information tables"
            )
        records = legacy_candidates[0]
        method = "sec_archive_legacy_table"
    distinct_covers = {
        _canonical_json_bytes(metadata) for metadata in cover_candidates
    }
    if len(distinct_covers) > 1:
        raise DatasetParseError("multiple conflicting Form 13F cover documents")
    cover_metadata = (
        json.loads(next(iter(distinct_covers)).decode("utf-8"))
        if distinct_covers
        else None
    )
    cover_metadata_consistent = False
    if cover_metadata is not None:
        cover_is_amendment = bool(cover_metadata["is_amendment"])
        header_consistent = (
            (cover_is_amendment and form_type == "13F-HR/A")
            or (not cover_is_amendment and form_type == "13F-HR")
        )
        amendment_consistent = (
            (cover_is_amendment and bool(cover_metadata["amendment_type"]))
            or (
                not cover_is_amendment
                and cover_metadata["amendment_type"] is None
            )
        )
        raw_value_total = sum(
            (Decimal(str(record["reported_value"])) for record in records),
            Decimal(0),
        )
        cover_metadata_consistent = bool(
            header_consistent
            and amendment_consistent
            and cover_metadata["table_entry_total"] == len(records)
            and _decimal_equal(
                cover_metadata["table_value_total"],
                raw_value_total,
            )
        )
    return {
        "url": url,
        "sha256": source_sha256,
        "accession": normalized_accession,
        "cik": normalized_cik,
        "report_date": normalized_report_date,
        "filing_date": normalized_filing_date,
        "submission_type": form_type,
        "acceptance_datetime": acceptance_datetime,
        "cover_metadata": cover_metadata,
        "cover_metadata_consistent": cover_metadata_consistent,
        "method": method,
        "records": records,
        "information_table_count": len(records),
    }


def parse_sec_archive_index(
    payload: bytes,
    *,
    cik: object,
    accession: object,
) -> dict[str, Any]:
    """Validate an SEC directory index for one exact full-submission file."""

    normalized_cik = _normalize_cik(cik)
    normalized_accession = _normalize_accession(accession)
    try:
        parsed = json.loads(bytes(payload).decode("utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise DatasetParseError(f"invalid SEC archive index JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not isinstance(parsed.get("directory"), dict):
        raise DatasetParseError("SEC archive index directory is malformed")
    items = parsed["directory"].get("item")
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise DatasetParseError("SEC archive index item list is malformed")
    expected_name = f"{normalized_accession}.txt"
    names: list[str] = []
    for item in items:
        name = item.get("name")
        if not isinstance(name, str) or PurePosixPath(name).name != name:
            raise DatasetParseError("SEC archive index contains an unsafe filename")
        names.append(name)
    if names.count(expected_name) != 1:
        raise DatasetParseError(
            "SEC archive index does not identify exactly one full submission"
        )
    return {
        "url": sec_archive_index_url(normalized_cik, normalized_accession),
        "sha256": _sha256_bytes(bytes(payload)),
        "submission_url": sec_archive_submission_url(
            normalized_cik,
            normalized_accession,
        ),
    }


def _fetch_bytes(fetcher: Fetcher, url: str) -> bytes:
    payload = fetcher(url)
    if not isinstance(payload, (bytes, bytearray)):
        raise Sec13FBulkError("SEC fetcher must return bytes")
    return bytes(payload)


def _normalize_archive_targets(
    targets: Iterable[Mapping[str, Any]],
) -> list[dict[str, str]]:
    by_accession: dict[str, tuple[str, str]] = {}
    for target in targets:
        try:
            cik = _normalize_cik(target.get("cik"))
            accession = _normalize_accession(target.get("accession"))
            report_date = _normalize_date(target.get("report_date"))
        except DatasetParseError as exc:
            raise Sec13FBulkError(
                f"invalid SEC archive fallback target: {exc}"
            ) from exc
        identity = (cik, report_date)
        prior = by_accession.setdefault(accession, identity)
        if prior != identity:
            raise Sec13FBulkError(
                "SEC archive fallback accession has conflicting filer/report targets: "
                f"{accession}"
            )
    return [
        {"cik": cik, "accession": accession, "report_date": report_date}
        for accession, (cik, report_date) in sorted(by_accession.items())
    ]


def _index_covers_archive_targets_exactly(
    state: Mapping[str, Any],
    *,
    state_path: Path,
    archive_targets: Sequence[Mapping[str, str]],
) -> bool:
    """Require every normalized target to have its exact filer/date identity."""

    index_path = _index_path_from_state(state, Path(state_path))
    if index_path is None:
        return False
    try:
        normalized_targets = _normalize_archive_targets(archive_targets)
        expected = {
            target["accession"]: (target["cik"], target["report_date"])
            for target in normalized_targets
        }
        connection = _open_index(index_path, read_only=True)
        try:
            actual = {
                row["accession"]: (row["cik"], row["report_date"])
                for row in connection.execute(
                    "SELECT accession, cik, report_date FROM submissions"
                )
                if row["accession"] in expected
            }
        finally:
            connection.close()
    except (OSError, sqlite3.Error, Sec13FBulkError):
        return False
    return actual == expected


def _copy_index(source_path: Path, destination_path: Path) -> None:
    source = _open_index(source_path, read_only=True)
    destination = sqlite3.connect(destination_path)
    try:
        source.backup(destination)
        destination.commit()
    finally:
        destination.close()
        source.close()


def _clean_rebuild_plan_sha256(
    *,
    target_scope: Mapping[str, Any],
    dataset_urls: Sequence[str],
    archive_targets: Sequence[Mapping[str, str]],
) -> str:
    return _sha256_bytes(_canonical_json_bytes({
        "target_scope_sha256": target_scope["sha256"],
        "dataset_urls": list(dataset_urls),
        "archive_targets": list(archive_targets),
        "index_schema_version": INDEX_SCHEMA_VERSION,
        "parser_contract_version": CLEAN_REBUILD_PARSER_CONTRACT_VERSION,
    }))


def _clean_checkpoint_checksum_valid(checkpoint: Mapping[str, Any]) -> bool:
    expected = checkpoint.get("checkpoint_sha256")
    if not isinstance(expected, str) or not _SHA256_RE.fullmatch(expected):
        return False
    body = {
        key: value
        for key, value in checkpoint.items()
        if key != "checkpoint_sha256"
    }
    return _sha256_bytes(_canonical_json_bytes(body)) == expected


def _load_clean_rebuild_checkpoint(
    checkpoint_path: Path,
    *,
    plan_sha256: str,
    partial_index_path: Path,
) -> dict[str, Any] | None:
    try:
        checkpoint = json.loads(Path(checkpoint_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return None
    if (
        not isinstance(checkpoint, dict)
        or checkpoint.get("schema_version") != 1
        or not _clean_checkpoint_checksum_valid(checkpoint)
        or checkpoint.get("plan_sha256") != plan_sha256
        or checkpoint.get("partial_index_path")
        != str(Path(partial_index_path).resolve())
        or not isinstance(checkpoint.get("sources"), dict)
        or not isinstance(checkpoint.get("archive_sources"), dict)
        or not isinstance(checkpoint.get("accepted_at"), str)
        or not partial_index_path.is_file()
    ):
        return None
    connection: sqlite3.Connection | None = None
    try:
        connection = _open_index(partial_index_path, read_only=True)
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
    except (OSError, sqlite3.Error, Sec13FBulkError):
        return None
    finally:
        if connection is not None:
            connection.close()
    if integrity is None or integrity[0] != "ok":
        return None
    return checkpoint


def _write_clean_rebuild_checkpoint(
    checkpoint_path: Path,
    *,
    plan_sha256: str,
    partial_index_path: Path,
    accepted_at: str,
    sources: Mapping[str, Any],
    archive_sources: Mapping[str, Any],
) -> None:
    with partial_index_path.open("rb") as handle:
        os.fsync(handle.fileno())
    checkpoint = {
        "schema_version": 1,
        "plan_sha256": plan_sha256,
        "partial_index_path": str(partial_index_path.resolve()),
        "accepted_at": accepted_at,
        "sources": dict(sources),
        "archive_sources": dict(archive_sources),
    }
    checkpoint["checkpoint_sha256"] = _sha256_bytes(
        _canonical_json_bytes(checkpoint)
    )
    _atomic_write_json(checkpoint_path, checkpoint)


def _prepare_temporary_index(
    *,
    index_dir: Path,
    prior_index_path: Path | None,
    full_rebuild: bool,
) -> Path:
    index_dir.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".sec_13f_bulk_index.",
        suffix=".sqlite3.tmp",
        dir=index_dir,
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        if prior_index_path is not None and not full_rebuild:
            temporary_path.unlink()
            _copy_index(prior_index_path, temporary_path)
        else:
            connection = sqlite3.connect(temporary_path)
            try:
                _create_schema(connection)
                connection.commit()
            finally:
                connection.close()
        return temporary_path
    except BaseException:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        raise


def _finalize_index(
    temporary_path: Path,
    *,
    index_dir: Path,
) -> tuple[Path, str, int]:
    connection = sqlite3.connect(temporary_path)
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity is None or integrity[0] != "ok":
            raise Sec13FBulkError("SEC 13F SQLite integrity check failed")
        connection.execute("PRAGMA optimize")
        connection.commit()
    finally:
        connection.close()
    with temporary_path.open("rb") as handle:
        os.fsync(handle.fileno())
    checksum = _sha256_file(temporary_path)
    size = temporary_path.stat().st_size
    final_path = index_dir / f"index-{checksum}.sqlite3"
    if final_path.exists():
        if (
            final_path.stat().st_size != size
            or _sha256_file(final_path) != checksum
        ):
            raise Sec13FBulkError(
                "SEC 13F content-addressed index path has conflicting content"
            )
        temporary_path.unlink()
    else:
        os.replace(temporary_path, final_path)
        _fsync_directory(index_dir)
    return final_path, checksum, size


def refresh_13f_bulk_index(
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    index_dir: Path = DEFAULT_INDEX_DIR,
    dataset_urls: Iterable[str] | None = None,
    discovery_html: str | bytes | None = None,
    fetcher: Fetcher | None = None,
    user_agent: str | None = None,
    target_accessions: Iterable[str] | None = None,
    target_periods: Iterable[tuple[str, str] | Mapping[str, Any]] | None = None,
    archive_fallback_targets: Iterable[Mapping[str, Any]] | None = None,
    clean_rebuild_checkpoint_path: Path | None = None,
    completed_rebuild_receipt: Mapping[str, Any] | None = None,
    allow_unpublished_legacy_index_adoption: bool = False,
    full_rebuild: bool = False,
    recheck_recent_archives: int = 1,
    refreshed_at: datetime | None = None,
) -> BulkIndexRefreshResult:
    """Refresh the official quarterly data-set index with last-good semantics.

    Existing archives are not redownloaded except for the requested number of
    most recent URLs.  Any source failure aborts the candidate generation and
    returns the prior state unchanged.  A full rebuild starts from an empty
    SQLite database and reparses every discovered archive. Exact accession
    fallbacks, when supplied, are inserted into that same unfinalized candidate
    so a clean rebuild never copies a multi-gigabyte SQLite generation. A
    completed generation is reusable only when the caller supplies its exact
    content receipt as part of resuming an interrupted higher-level rebuild.
    A pre-plan generation can be adopted only through the separately verified,
    explicitly unpublished migration path.
    """

    if recheck_recent_archives < 0:
        raise Sec13FBulkError("recheck_recent_archives cannot be negative")
    state_path = Path(state_path)
    index_dir = Path(index_dir)
    prior_state = load_13f_bulk_index(state_path)
    normalized_archive_targets = _normalize_archive_targets(
        archive_fallback_targets or ()
    )
    if target_accessions is None and target_periods is None:
        prior_scope = prior_state.get("target_scope", {})
        scoped_accessions = list(prior_scope.get("accessions", []))
        scoped_periods = [
            (period["cik"], period["report_date"])
            for period in prior_scope.get("periods", [])
        ]
    else:
        scoped_accessions = sorted({
            _normalize_accession(value) for value in (target_accessions or [])
        })
        normalized_periods: set[tuple[str, str]] = set()
        for period in target_periods or []:
            if isinstance(period, Mapping):
                cik = period.get("cik")
                report_date = period.get("report_date")
            else:
                cik, report_date = period
            normalized_periods.add(
                (_normalize_cik(cik), _normalize_date(report_date))
            )
        scoped_periods = sorted(normalized_periods)
    if not scoped_accessions and not scoped_periods:
        raise Sec13FBulkError(
            "SEC Form 13F bulk refresh requires an exact accession or CIK/report scope"
        )
    target_scope_payload = {
        "accessions": scoped_accessions,
        "periods": [
            {"cik": cik, "report_date": report_date}
            for cik, report_date in scoped_periods
        ],
    }
    target_scope = {
        **target_scope_payload,
        "sha256": _sha256_bytes(_canonical_json_bytes(target_scope_payload)),
    }
    scope_changed = target_scope != prior_state.get("target_scope")
    effective_full_rebuild = full_rebuild or (
        prior_state.get("index") is not None and scope_changed
    )
    prior_index_path = _index_path_from_state(prior_state, state_path)
    active_fetcher = fetcher or make_sec_fetcher(user_agent)
    errors: list[str] = []

    page_payload: bytes | None
    if discovery_html is not None:
        page_payload = (
            bytes(discovery_html)
            if isinstance(discovery_html, (bytes, bytearray))
            else str(discovery_html).encode("utf-8")
        )
    elif dataset_urls is None:
        try:
            page_payload = _fetch_bytes(
                active_fetcher,
                FORM_13F_DATASETS_PAGE_URL,
            )
        except Exception as exc:
            errors.append(f"{FORM_13F_DATASETS_PAGE_URL}: {exc}")
            return BulkIndexRefreshResult(
                prior_state,
                False,
                (),
                tuple(sorted(prior_state.get("sources", {}))),
                tuple(errors),
            )
    else:
        page_payload = None

    if dataset_urls is None:
        assert page_payload is not None
        discovered = discover_13f_dataset_urls(page_payload)
    else:
        discovered = sorted(
            {normalize_sec_13f_dataset_url(url) for url in dataset_urls},
            key=_dataset_url_sort_key,
        )
    # A temporary omission from the SEC landing page must not delete evidence
    # already accepted from an immutable official URL.
    all_urls = sorted(
        set(discovered) | set(prior_state.get("sources", {})),
        key=_dataset_url_sort_key,
    )
    if not all_urls:
        errors.append("official SEC Form 13F page exposed no data-set ZIPs")
        return BulkIndexRefreshResult(prior_state, False, (), (), tuple(errors))

    page_sha256 = (
        _sha256_bytes(page_payload)
        if page_payload is not None
        else prior_state.get("source_page", {}).get("sha256")
    )

    prior_sources = prior_state.get("sources", {})
    checkpoint_path = (
        Path(clean_rebuild_checkpoint_path)
        if clean_rebuild_checkpoint_path is not None and effective_full_rebuild
        else None
    )
    checkpoint: dict[str, Any] | None = None
    checkpoint_partial_path: Path | None = None
    checkpoint_plan_sha256: str | None = None
    if checkpoint_path is not None:
        checkpoint_plan_sha256 = _clean_rebuild_plan_sha256(
            target_scope=target_scope,
            dataset_urls=all_urls,
            archive_targets=normalized_archive_targets,
        )
        checkpoint_partial_path = index_dir / (
            f".rebuild-{checkpoint_plan_sha256}.sqlite3.partial"
        )
        receipt_scope = (
            completed_rebuild_receipt.get("receipt_scope")
            if isinstance(completed_rebuild_receipt, Mapping)
            else None
        )
        completed_receipt_matches = False
        legacy_adoption_matches = False
        if receipt_scope == COMPLETED_CLEAN_REBUILD_RECEIPT_SCOPE:
            completed_receipt_matches = completed_clean_rebuild_receipt_matches(
                completed_rebuild_receipt,
                state_path=state_path,
                expected_plan_sha256=checkpoint_plan_sha256,
                verify_index_checksum=True,
            )
        elif (
            allow_unpublished_legacy_index_adoption
            and receipt_scope == LEGACY_INDEX_ADOPTION_RECEIPT_SCOPE
        ):
            legacy_adoption_matches = (
                legacy_index_adoption_receipt_matches(
                    completed_rebuild_receipt,
                    state_path=state_path,
                    expected_plan_sha256=checkpoint_plan_sha256,
                    dataset_urls=discovered,
                    archive_targets=normalized_archive_targets,
                    verify_index_checksum=True,
                )
                and target_scope == prior_state.get("target_scope")
                and set(prior_state.get("sources", {})) == set(discovered)
                and prior_index_path is not None
                and _index_covers_archive_targets_exactly(
                    prior_state,
                    state_path=state_path,
                    archive_targets=normalized_archive_targets,
                )
            )
        if (
            completed_receipt_matches
            and prior_state.get("clean_rebuild_plan_sha256")
            == checkpoint_plan_sha256
            and set(prior_state.get("sources", {})) == set(all_urls)
            and prior_index_path is not None
        ):
            if load_13f_bulk_index(state_path) != prior_state:
                raise Sec13FBulkError(
                    "SEC 13F state changed during completed-index reuse"
                )
            # The receipt matcher above already performed the mandatory full
            # SQLite checksum. Avoid reading the multi-gigabyte index twice.
            _validate_state(
                prior_state,
                state_path=state_path,
                verify_index_checksum=False,
            )
            _cleanup_superseded_index_generations(
                index_dir=index_dir,
                active_index_path=prior_index_path,
            )
            return BulkIndexRefreshResult(
                state=prior_state,
                changed=False,
                refreshed_urls=(),
                reused_urls=tuple(all_urls),
                errors=(),
            )
        if legacy_adoption_matches:
            if load_13f_bulk_index(state_path) != prior_state:
                raise Sec13FBulkError(
                    "SEC 13F state changed during legacy index adoption"
                )
            adopted_state = copy.deepcopy(prior_state)
            adopted_state["clean_rebuild_plan_sha256"] = (
                checkpoint_plan_sha256
            )
            _validate_state(
                adopted_state,
                state_path=state_path,
                verify_index_checksum=False,
            )
            _atomic_write_json(state_path, adopted_state)
            _cleanup_superseded_index_generations(
                index_dir=index_dir,
                active_index_path=prior_index_path,
            )
            return BulkIndexRefreshResult(
                state=adopted_state,
                changed=True,
                refreshed_urls=(),
                reused_urls=tuple(all_urls),
                errors=(),
            )
        checkpoint = _load_clean_rebuild_checkpoint(
            checkpoint_path,
            plan_sha256=checkpoint_plan_sha256,
            partial_index_path=checkpoint_partial_path,
        )
        if checkpoint is None:
            checkpoint_path.unlink(missing_ok=True)
            checkpoint_partial_path.unlink(missing_ok=True)
    if effective_full_rebuild:
        fetch_urls = set(all_urls) - set(
            (checkpoint or {}).get("sources", {})
        )
    else:
        fetch_urls = set(all_urls) - set(prior_sources)
        if recheck_recent_archives:
            fetch_urls.update(all_urls[-recheck_recent_archives:])
    reused_urls = set(all_urls) - fetch_urls
    temporary_path: Path | None = None
    finalized_candidate_path: Path | None = None
    promoted_state: dict[str, Any] | None = None
    refreshed_urls: list[str] = []
    changed_index = effective_full_rebuild or bool(set(all_urls) - set(prior_sources))
    candidate_sources = (
        copy.deepcopy((checkpoint or {}).get("sources", {}))
        if effective_full_rebuild
        else copy.deepcopy(prior_sources)
    )
    candidate_archive_sources = (
        copy.deepcopy((checkpoint or {}).get("archive_sources", {}))
        if effective_full_rebuild
        else copy.deepcopy(prior_state.get("archive_sources", {}))
    )
    accepted_at = str(
        (checkpoint or {}).get("accepted_at") or _utc_timestamp(refreshed_at)
    )

    try:
        if checkpoint is not None:
            assert checkpoint_partial_path is not None
            temporary_path = checkpoint_partial_path
        else:
            temporary_path = _prepare_temporary_index(
                index_dir=index_dir,
                prior_index_path=prior_index_path,
                full_rebuild=effective_full_rebuild,
            )
            if checkpoint_partial_path is not None:
                os.replace(temporary_path, checkpoint_partial_path)
                _fsync_directory(index_dir)
                temporary_path = checkpoint_partial_path
        connection = sqlite3.connect(temporary_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        _create_schema(connection)
        try:
            for url in all_urls:
                if url not in fetch_urls:
                    continue
                try:
                    payload = _fetch_bytes(active_fetcher, url)
                    payload_sha256 = _sha256_bytes(payload)
                    prior = prior_sources.get(url)
                    if (
                        not effective_full_rebuild
                        and isinstance(prior, dict)
                        and prior.get("sha256") == payload_sha256
                    ):
                        reused_urls.add(url)
                        continue
                    connection.execute("SAVEPOINT ingest_archive")
                    try:
                        connection.execute(
                            "DELETE FROM submissions WHERE source_url = ?",
                            (url,),
                        )
                        metadata = ingest_13f_dataset_zip(
                            connection,
                            payload,
                            source_url=url,
                            source_sha256=payload_sha256,
                            target_accessions=scoped_accessions,
                            target_periods=scoped_periods,
                        )
                    except Exception:
                        connection.execute("ROLLBACK TO ingest_archive")
                        connection.execute("RELEASE ingest_archive")
                        raise
                    connection.execute("RELEASE ingest_archive")
                    candidate_sources[url] = {
                        **metadata,
                        "accepted_at": accepted_at,
                    }
                    refreshed_urls.append(url)
                    changed_index = True
                    if checkpoint_path is not None:
                        connection.commit()
                        assert checkpoint_plan_sha256 is not None
                        _write_clean_rebuild_checkpoint(
                            checkpoint_path,
                            plan_sha256=checkpoint_plan_sha256,
                            partial_index_path=temporary_path,
                            accepted_at=accepted_at,
                            sources=candidate_sources,
                            archive_sources=candidate_archive_sources,
                        )
                except Exception as exc:
                    errors.append(f"{url}: {exc}")
                    break
            if not errors:
                for target in normalized_archive_targets:
                    accession = target["accession"]
                    existing = connection.execute(
                        "SELECT cik, report_date FROM submissions "
                        "WHERE accession = ?",
                        (accession,),
                    ).fetchone()
                    if existing is not None:
                        if (
                            existing["cik"] != target["cik"]
                            or existing["report_date"] != target["report_date"]
                        ):
                            # SEC quarterly bulk files occasionally bind an
                            # accession to the accession-prefix CIK instead of
                            # the actual filing manager. The exact Archives
                            # path and complete-submission header below are the
                            # stronger identity proof; fetch and atomically
                            # replace the conflicting bulk row. A bad target
                            # still fails because the parser checks accession,
                            # filer CIK, and report date against the SGML.
                            pass
                        else:
                            continue
                    index_url = sec_archive_index_url(target["cik"], accession)
                    submission_url = sec_archive_submission_url(
                        target["cik"],
                        accession,
                    )
                    connection.execute("SAVEPOINT integrated_archive_fallback")
                    try:
                        index_payload = _fetch_bytes(active_fetcher, index_url)
                        index_metadata = parse_sec_archive_index(
                            index_payload,
                            cik=target["cik"],
                            accession=accession,
                        )
                        if index_metadata["submission_url"] != submission_url:
                            raise DatasetParseError(
                                "SEC archive index resolved an unexpected submission URL"
                            )
                        parsed = parse_sec_archive_submission(
                            _fetch_bytes(active_fetcher, submission_url),
                            cik=target["cik"],
                            accession=accession,
                            report_date=target["report_date"],
                            source_url=submission_url,
                        )
                        _insert_archive_evidence(connection, parsed)
                        candidate_archive_sources[submission_url] = {
                            "url": submission_url,
                            "sha256": parsed["sha256"],
                            "index_url": index_url,
                            "index_sha256": index_metadata["sha256"],
                            "accession": accession,
                            "cik": target["cik"],
                            "report_date": target["report_date"],
                            "accepted_at": accepted_at,
                            "method": parsed["method"],
                            "information_table_count": parsed[
                                "information_table_count"
                            ],
                        }
                    except Exception as exc:
                        connection.execute("ROLLBACK TO integrated_archive_fallback")
                        connection.execute("RELEASE integrated_archive_fallback")
                        errors.append(f"{submission_url}: {exc}")
                        break
                    connection.execute("RELEASE integrated_archive_fallback")
                    changed_index = True
                    if checkpoint_path is not None:
                        connection.commit()
                        assert checkpoint_plan_sha256 is not None
                        _write_clean_rebuild_checkpoint(
                            checkpoint_path,
                            plan_sha256=checkpoint_plan_sha256,
                            partial_index_path=temporary_path,
                            accepted_at=accepted_at,
                            sources=candidate_sources,
                            archive_sources=candidate_archive_sources,
                        )
            if errors:
                connection.rollback()
            else:
                connection.commit()
        finally:
            connection.close()

        if errors:
            if checkpoint_path is None:
                temporary_path.unlink(missing_ok=True)
            return BulkIndexRefreshResult(
                prior_state,
                False,
                (),
                tuple(sorted(prior_sources, key=_dataset_url_sort_key)),
                tuple(errors),
            )

        page_changed = (
            page_sha256 != prior_state.get("source_page", {}).get("sha256")
        )
        if not changed_index and prior_index_path is not None:
            temporary_path.unlink(missing_ok=True)
            final_index_path = prior_index_path
            index_checksum = prior_state["index"]["sha256"]
            index_size = prior_state["index"]["size_bytes"]
        else:
            final_index_path, index_checksum, index_size = _finalize_index(
                temporary_path,
                index_dir=index_dir,
            )
            finalized_candidate_path = final_index_path
            temporary_path = None

        summary_connection = _open_index(final_index_path, read_only=True)
        try:
            submission_count = summary_connection.execute(
                "SELECT COUNT(*) FROM submissions"
            ).fetchone()[0]
            information_count = summary_connection.execute(
                "SELECT COUNT(*) FROM information_table"
            ).fetchone()[0]
        finally:
            summary_connection.close()
        candidate_state = {
            "schema_version": STATE_SCHEMA_VERSION,
            **(
                {"clean_rebuild_plan_sha256": checkpoint_plan_sha256}
                if checkpoint_plan_sha256 is not None
                else {}
            ),
            "generated_at": (
                accepted_at
                if changed_index or page_changed or prior_state.get("index") is None
                else prior_state.get("generated_at")
            ),
            "source_page": {
                "url": FORM_13F_DATASETS_PAGE_URL,
                "sha256": page_sha256,
            },
            "sources": {
                url: candidate_sources[url]
                for url in sorted(candidate_sources, key=_dataset_url_sort_key)
            },
            "archive_sources": {
                url: candidate_archive_sources[url]
                for url in sorted(candidate_archive_sources)
            },
            "target_scope": target_scope,
            "index": {
                "schema_version": INDEX_SCHEMA_VERSION,
                "path": _relative_index_path(final_index_path, state_path),
                "sha256": index_checksum,
                "size_bytes": index_size,
            },
            "summary": {
                "datasets": len(candidate_sources),
                "archive_filings": len(candidate_archive_sources),
                "submissions": submission_count,
                "information_table_rows": information_count,
            },
        }
        _validate_state(
            candidate_state,
            state_path=state_path,
            verify_index_checksum=False,
        )
        changed = _canonical_json_bytes(candidate_state) != _canonical_json_bytes(
            prior_state
        )
        if changed:
            _atomic_write_json(state_path, candidate_state)
            promoted_state = candidate_state
        if checkpoint_path is not None:
            checkpoint_path.unlink(missing_ok=True)
            _fsync_directory(checkpoint_path.parent)
        # The state write above fsyncs both file and parent directory. Only now
        # may an older generation be removed. Running this on unchanged states
        # also clears a generation orphaned by a prior post-promotion crash.
        _cleanup_superseded_index_generations(
            index_dir=index_dir,
            active_index_path=final_index_path,
        )
        return BulkIndexRefreshResult(
            candidate_state,
            changed,
            tuple(sorted(refreshed_urls, key=_dataset_url_sort_key)),
            tuple(sorted(reused_urls, key=_dataset_url_sort_key)),
            (),
        )
    except Exception as exc:
        if temporary_path is not None:
            if checkpoint_path is None:
                temporary_path.unlink(missing_ok=True)
        if promoted_state is None:
            _discard_unpublished_index(
                finalized_candidate_path,
                prior_index_path=prior_index_path,
                index_dir=index_dir,
            )
        return BulkIndexRefreshResult(
            promoted_state or prior_state,
            promoted_state is not None,
            (),
            tuple(sorted(prior_sources, key=_dataset_url_sort_key)),
            (f"SEC Form 13F index refresh failed: {exc}",),
        )


def collect_archive_fallback_targets(
    fund_documents: Iterable[Mapping[str, Any]],
    *,
    incomplete_only: bool = True,
) -> dict[str, Any]:
    """Collect exact CIK/accession/report-date fallback targets.

    Ordinary callers retain the historical incomplete-only behavior.  A clean
    rebuild passes ``incomplete_only=False`` so every exact accession absent
    from a bulk ZIP can be inserted into the same candidate SQLite generation
    before its one and only finalization.
    """

    targets: set[tuple[str, str, str]] = set()
    unaddressable: set[tuple[str, str]] = set()
    for fund in fund_documents:
        try:
            cik = _normalize_cik(fund.get("cik"))
        except DatasetParseError:
            continue
        quarters = fund.get("quarters")
        if not isinstance(quarters, list):
            continue
        for quarter in quarters:
            if not isinstance(quarter, dict) or not isinstance(
                quarter.get("holdings"), list
            ):
                continue
            retained = [
                holding
                for holding in quarter["holdings"]
                if isinstance(holding, dict)
                and not _is_zero_value_confidential_placeholder(holding)
            ]
            relevant = (
                [
                    holding
                    for holding in retained
                    if _needs_reported_identity_backfill(holding)
                ]
                if incomplete_only
                else retained
            )
            if not relevant:
                continue
            try:
                report_date = _normalize_date(quarter.get("report_date"))
            except DatasetParseError:
                unaddressable.add((cik, str(quarter.get("report_date") or "")))
                continue
            accessions = set(_quarter_accessions(quarter))
            accessions.update(
                _normalize_text(holding.get("accession"))
                for holding in relevant
                if _ACCESSION_RE.fullmatch(
                    _normalize_text(holding.get("accession"))
                )
            )
            if not accessions:
                unaddressable.add((cik, report_date))
                continue
            targets.update(
                (cik, accession, report_date) for accession in accessions
            )
    return {
        "targets": [
            {"cik": cik, "accession": accession, "report_date": report_date}
            for cik, accession, report_date in sorted(targets)
        ],
        "unaddressable": [
            {"cik": cik, "report_date": report_date}
            for cik, report_date in sorted(unaddressable)
        ],
    }


def _iter_fund_documents(funds_dir: Path) -> Iterable[dict[str, Any]]:
    for path in sorted(Path(funds_dir).glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            yield payload


def collect_archive_fallback_targets_from_funds(
    funds_dir: Path,
    *,
    incomplete_only: bool = True,
) -> dict[str, Any]:
    return collect_archive_fallback_targets(
        _iter_fund_documents(funds_dir),
        incomplete_only=incomplete_only,
    )


def collect_archive_enrichment_targets_from_funds(
    funds_dir: Path,
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    workers: int | None = None,
) -> list[dict[str, str]]:
    """Find only unresolved accessionless periods needing filing-chain proof.

    Most legacy quarters are uniquely recoverable from the bulk data and must
    not trigger thousands of complete-submission downloads. This dry pass uses
    the same matcher/reconstructor as publication and requests exact SEC
    archive documents only for a residual quarter that still has an unmatched
    or ambiguous holding.
    """

    state_path = Path(state_path)
    state = load_13f_bulk_index(state_path)
    index_path = _index_path_from_state(state, state_path)
    if index_path is None:
        raise Sec13FBulkError("SEC Form 13F evidence index has not been built")
    targets: set[tuple[str, str, str]] = set()
    paths = sorted(Path(funds_dir).glob("*.json"))
    for found in _map_fund_evidence_jobs(
        _enrichment_targets_for_file, paths, index_path, workers=workers,
    ):
        targets.update(found)
    return [
        {"cik": cik, "accession": accession, "report_date": report_date}
        for cik, accession, report_date in sorted(targets)
    ]


def _enrichment_targets_for_file(
    job: tuple[Path, Path, bool],
) -> set[tuple[str, str, str]]:
    path, index_path, _required = job
    # Preserve the discovery pass's tolerant handling of malformed documents;
    # the mandatory pre-apply verifier below still rejects them before writes.
    try:
        fund = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(fund, dict):
        return set()
    targets: set[tuple[str, str, str]] = set()
    connection = _open_index(index_path, read_only=True)
    try:
        if isinstance(fund, dict):
            try:
                cik = _normalize_cik(fund.get("cik"))
            except DatasetParseError:
                return targets
            quarters = fund.get("quarters")
            if not isinstance(quarters, list):
                return targets
            for quarter in quarters:
                if (
                    not isinstance(quarter, dict)
                    or not isinstance(quarter.get("holdings"), list)
                    or _quarter_accessions(quarter)
                ):
                    continue
                try:
                    report_date = _normalize_date(quarter.get("report_date"))
                except DatasetParseError:
                    continue
                _updated, stats = backfill_fund_document(
                    {"cik": cik, "quarters": [quarter]},
                    connection=connection,
                )
                if not (stats["unmatched"] or stats["ambiguous"]):
                    continue
                accessions = connection.execute(
                    """
                    SELECT accession
                      FROM submissions
                     WHERE cik = ? AND report_date = ?
                     ORDER BY accession
                    """,
                    (cik, report_date),
                ).fetchall()
                targets.update(
                    (cik, str(row["accession"]), report_date)
                    for row in accessions
                )
    finally:
        connection.close()
    return targets


def _periods_without_index_evidence(
    periods: Iterable[Mapping[str, Any]],
    *,
    state: Mapping[str, Any],
    state_path: Path,
) -> list[dict[str, str]]:
    """Return exact filer/report periods absent from the current SEC index."""

    normalized = sorted({
        (_normalize_cik(period.get("cik")), _normalize_date(period.get("report_date")))
        for period in periods
    })
    index_path = _index_path_from_state(state, state_path)
    if index_path is None:
        return [
            {"cik": cik, "report_date": report_date}
            for cik, report_date in normalized
        ]
    required = set(normalized)
    covered: set[tuple[str, str]] = set()
    connection = _open_index(index_path, read_only=True)
    try:
        for row in connection.execute(
            "SELECT accession, cik, report_date, source_url FROM submissions"
        ):
            period = (str(row["cik"]), str(row["report_date"]))
            if period in required and _submission_source_is_rebuildable(
                row,
                state=state,
            ):
                covered.add(period)
    finally:
        connection.close()
    missing = sorted(required - covered)
    return [
        {"cik": cik, "report_date": report_date}
        for cik, report_date in missing
    ]


def _submission_source_is_rebuildable(
    row: Mapping[str, Any],
    *,
    state: Mapping[str, Any],
) -> bool:
    """Whether one indexed submission can be recreated from durable state."""

    source_url = str(row["source_url"])
    sources = state.get("sources")
    if isinstance(sources, Mapping) and isinstance(
        sources.get(source_url), Mapping
    ):
        return True
    archive_sources = state.get("archive_sources")
    source = (
        archive_sources.get(source_url)
        if isinstance(archive_sources, Mapping)
        else None
    )
    return isinstance(source, Mapping) and (
        str(source.get("accession")) == str(row["accession"])
        and str(source.get("cik")) == str(row["cik"])
        and str(source.get("report_date")) == str(row["report_date"])
        and str(source.get("url")) == source_url
    )


def _archive_targets_from_existing_index(
    periods: Iterable[Mapping[str, Any]],
    *,
    state: Mapping[str, Any],
    state_path: Path,
) -> list[dict[str, str]]:
    """Carry exact archive accessions forward for accessionless periods.

    A clean rebuild starts from an empty SQLite database.  If a legacy fund
    quarter omits its accession, the prior index can still prove that the
    period is covered and therefore suppress fresh submissions discovery.
    The exact archive accessions that supplied that coverage must also remain
    in the next clean-rebuild plan; otherwise the empty candidate silently
    drops those rows.  Carry only checksum-bound archive sources whose
    accession, filer CIK, report date, and source URL agree with the prior
    SQLite generation.  Bulk-data rows need no target because every accepted
    bulk archive is reparsed independently.
    """

    normalized_periods = {
        (_normalize_cik(period.get("cik")), _normalize_date(period.get("report_date")))
        for period in periods
    }
    if not normalized_periods:
        return []
    index_path = _index_path_from_state(state, Path(state_path))
    archive_sources = state.get("archive_sources")
    if index_path is None or not isinstance(archive_sources, Mapping):
        return []

    targets: list[dict[str, str]] = []
    connection = _open_index(index_path, read_only=True)
    try:
        for row in connection.execute(
            "SELECT accession, cik, report_date, source_url FROM submissions "
            "ORDER BY accession"
        ):
            cik = str(row["cik"])
            report_date = str(row["report_date"])
            if (cik, report_date) not in normalized_periods:
                continue
            source_url = str(row["source_url"])
            source = archive_sources.get(source_url)
            if (
                not isinstance(source, Mapping)
                or not _submission_source_is_rebuildable(row, state=state)
            ):
                continue
            targets.append({
                "cik": cik,
                "accession": str(row["accession"]),
                "report_date": report_date,
            })
    finally:
        connection.close()
    return _normalize_archive_targets(targets)


def _accession_discovery_checkpoint_path(rebuild_checkpoint_path: Path) -> Path:
    """Return the sibling path cached with the bulk rebuild checkpoint."""

    path = Path(rebuild_checkpoint_path)
    suffix = path.suffix or ".json"
    stem = path.name[:-len(path.suffix)] if path.suffix else path.name
    return path.parent / f"{stem}.accession-discovery{suffix}"


def _accession_discovery_scope(
    grouped: Mapping[str, set[str]],
) -> list[dict[str, Any]]:
    return [
        {"cik": cik, "report_dates": sorted(grouped[cik])}
        for cik in sorted(grouped)
    ]


def _accession_discovery_plan_sha256(
    scope: Sequence[Mapping[str, Any]],
) -> str:
    return _sha256_bytes(_canonical_json_bytes({
        "scope": list(scope),
        "schema_version": ACCESSION_DISCOVERY_CHECKPOINT_SCHEMA_VERSION,
    }))


def _load_accession_discovery_checkpoint(
    checkpoint_path: Path,
    *,
    scope: Sequence[Mapping[str, Any]],
    plan_sha256: str,
) -> dict[str, dict[str, Any]] | None:
    """Load only a canonical, scope-bound private discovery checkpoint."""

    try:
        checkpoint = json.loads(Path(checkpoint_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        return None
    if (
        not isinstance(checkpoint, dict)
        or checkpoint.get("schema_version")
        != ACCESSION_DISCOVERY_CHECKPOINT_SCHEMA_VERSION
        or not _clean_checkpoint_checksum_valid(checkpoint)
        or checkpoint.get("plan_sha256") != plan_sha256
        or checkpoint.get("scope") != list(scope)
        or not isinstance(checkpoint.get("completed"), dict)
    ):
        return None
    expected_by_cik = {
        str(item["cik"]): list(item["report_dates"])
        for item in scope
    }
    normalized: dict[str, dict[str, Any]] = {}
    try:
        for raw_cik, raw_entry in checkpoint["completed"].items():
            cik = _normalize_cik(raw_cik)
            if (
                cik != raw_cik
                or cik not in expected_by_cik
                or not isinstance(raw_entry, dict)
                or set(raw_entry) != {"report_dates", "sources", "targets"}
                or raw_entry.get("report_dates") != expected_by_cik[cik]
            ):
                return None
            targets = _normalize_archive_targets(raw_entry.get("targets") or ())
            if raw_entry.get("targets") != targets or any(
                target["cik"] != cik
                or target["report_date"] not in expected_by_cik[cik]
                for target in targets
            ):
                return None
            if {
                target["report_date"] for target in targets
            } != set(expected_by_cik[cik]):
                return None
            raw_sources = raw_entry.get("sources")
            if not isinstance(raw_sources, list) or not raw_sources:
                return None
            sources: list[dict[str, str]] = []
            seen_urls: set[str] = set()
            for source in raw_sources:
                if not isinstance(source, dict) or set(source) != {"url", "sha256"}:
                    return None
                url = normalize_sec_submissions_url(source.get("url"))
                sha256 = str(source.get("sha256") or "")
                if (
                    source.get("url") != url
                    or not _SHA256_RE.fullmatch(sha256)
                    or url in seen_urls
                ):
                    return None
                seen_urls.add(url)
                sources.append({"url": url, "sha256": sha256})
            sources.sort(key=lambda item: item["url"])
            if raw_sources != sources:
                return None
            normalized[cik] = {
                "report_dates": expected_by_cik[cik],
                "targets": targets,
                "sources": sources,
            }
    except (Sec13FBulkError, Sec13FAccessionDiscoveryError, TypeError, ValueError):
        return None
    return normalized


def _write_accession_discovery_checkpoint(
    checkpoint_path: Path,
    *,
    scope: Sequence[Mapping[str, Any]],
    plan_sha256: str,
    completed: Mapping[str, Mapping[str, Any]],
) -> None:
    checkpoint = {
        "schema_version": ACCESSION_DISCOVERY_CHECKPOINT_SCHEMA_VERSION,
        "plan_sha256": plan_sha256,
        "scope": list(scope),
        "completed": {
            cik: dict(completed[cik]) for cik in sorted(completed)
        },
    }
    checkpoint["checkpoint_sha256"] = _sha256_bytes(
        _canonical_json_bytes(checkpoint)
    )
    _atomic_write_json(checkpoint_path, checkpoint)


def discover_archive_fallback_targets_for_periods(
    periods: Iterable[Mapping[str, Any]],
    *,
    fetcher: Fetcher | None = None,
    user_agent: str | None = None,
    checkpoint_path: Path | None = None,
) -> dict[str, Any]:
    """Resolve accessionless periods through checksum-bound SEC submissions.

    Recent submissions are sufficient for normal maintenance. Only a CIK
    whose requested period is absent from that document triggers the SEC's
    historical submission shards. No contact header or response payload is
    persisted; exact archive bytes become the durable row evidence later.
    """

    grouped: dict[str, set[str]] = {}
    for period in periods:
        cik = _normalize_cik(period.get("cik"))
        report_date = _normalize_date(period.get("report_date"))
        grouped.setdefault(cik, set()).add(report_date)
    active_fetcher = (
        fetcher
        if fetcher is not None
        else make_sec_submissions_fetcher(user_agent)
    )
    scope = _accession_discovery_scope(grouped)
    plan_sha256 = _accession_discovery_plan_sha256(scope)
    checkpoint = Path(checkpoint_path) if checkpoint_path is not None else None
    completed: dict[str, dict[str, Any]] = {}
    if checkpoint is not None:
        loaded = _load_accession_discovery_checkpoint(
            checkpoint,
            scope=scope,
            plan_sha256=plan_sha256,
        )
        if loaded is None:
            checkpoint_existed = checkpoint.exists()
            checkpoint.unlink(missing_ok=True)
            if checkpoint_existed:
                _fsync_directory(checkpoint.parent)
        else:
            completed = loaded
    targets: list[dict[str, str]] = []
    missing: list[dict[str, str]] = []
    source_checksums: dict[str, str] = {}
    dirty_completed = 0

    def persist_completed() -> None:
        nonlocal dirty_completed
        if checkpoint is None or not dirty_completed:
            return
        _write_accession_discovery_checkpoint(
            checkpoint,
            scope=scope,
            plan_sha256=plan_sha256,
            completed=completed,
        )
        dirty_completed = 0

    for cik in sorted(grouped):
        report_dates = sorted(grouped[cik])
        cached = completed.get(cik)
        if cached is not None:
            targets.extend(cached["targets"])
            for source in cached["sources"]:
                source_checksums[source["url"]] = source["sha256"]
            continue
        per_cik_payloads: dict[str, bytes] = {}

        def fetch_once(url: str) -> bytes:
            canonical_url = normalize_sec_submissions_url(url)
            if canonical_url not in per_cik_payloads:
                per_cik_payloads[canonical_url] = active_fetcher(canonical_url)
            return per_cik_payloads[canonical_url]

        try:
            discovery = discover_form13f_accessions(
                cik,
                report_dates,
                include_archive_shards=False,
                fetcher=fetch_once,
            )
            if discovery.missing_report_dates:
                discovery = discover_form13f_accessions(
                    cik,
                    report_dates,
                    include_archive_shards=True,
                    fetcher=fetch_once,
                )
        except Sec13FAccessionDiscoveryError as exc:
            try:
                persist_completed()
            except Exception:
                pass
            raise BulkIndexRefreshError(
                f"SEC submissions accession discovery failed for CIK {cik}"
            ) from exc
        current_sources: list[dict[str, str]] = []
        for source in discovery.sources:
            prior = source_checksums.setdefault(
                source.evidence.url,
                source.evidence.sha256,
            )
            if prior != source.evidence.sha256:
                raise BulkIndexRefreshError(
                    "SEC submissions source changed during one discovery run: "
                    f"{source.evidence.url}"
                )
            current_sources.append({
                "url": source.evidence.url,
                "sha256": source.evidence.sha256,
            })
        filings_by_period: dict[str, list[Any]] = {}
        for filing in discovery.filings:
            filings_by_period.setdefault(filing.report_date, []).append(filing)
        current_targets: list[dict[str, str]] = []
        current_missing: list[dict[str, str]] = []
        for report_date in report_dates:
            filings = filings_by_period.get(report_date, [])
            if not filings:
                current_missing.append({"cik": cik, "report_date": report_date})
                continue
            current_targets.extend(
                {
                    "cik": cik,
                    "accession": filing.accession,
                    "report_date": report_date,
                }
                for filing in filings
            )
        targets.extend(current_targets)
        missing.extend(current_missing)
        if not current_missing and checkpoint is not None:
            completed[cik] = {
                "report_dates": report_dates,
                "targets": _normalize_archive_targets(current_targets),
                "sources": sorted(current_sources, key=lambda item: item["url"]),
            }
            dirty_completed += 1
            if dirty_completed >= ACCESSION_DISCOVERY_CHECKPOINT_INTERVAL:
                persist_completed()
    persist_completed()
    return {
        "targets": _normalize_archive_targets(targets),
        "missing": missing,
        "sources": [
            {"url": url, "sha256": source_checksums[url]}
            for url in sorted(source_checksums)
        ],
    }


def _insert_archive_evidence(
    connection: sqlite3.Connection,
    parsed: Mapping[str, Any],
) -> None:
    # An exact complete-submission document is stronger evidence than the
    # flattened quarterly row for the same accession. Replacing the accession
    # atomically also prevents a mixed-source information table.
    connection.execute(
        "DELETE FROM submissions WHERE accession = ?",
        (parsed["accession"],),
    )
    submission = {
        "accession": parsed["accession"],
        "cik": parsed["cik"],
        "report_date": parsed["report_date"],
        "filing_date": parsed["filing_date"],
        "submission_type": parsed["submission_type"],
        "source_url": parsed["url"],
        "source_sha256": parsed["sha256"],
    }
    _insert_submission(connection, submission)
    for record in parsed["records"]:
        _insert_information_record(connection, record)
    cover = parsed.get("cover_metadata")
    connection.execute(
        """
        INSERT INTO filing_chain(
            accession, acceptance_datetime, cover_is_amendment,
            cover_amendment_type, cover_table_entry_total,
            cover_table_value_total, cover_metadata_consistent,
            source_url, source_sha256
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            parsed["accession"],
            parsed.get("acceptance_datetime"),
            (
                int(bool(cover.get("is_amendment")))
                if isinstance(cover, Mapping)
                else None
            ),
            (
                cover.get("amendment_type")
                if isinstance(cover, Mapping)
                else None
            ),
            (
                cover.get("table_entry_total")
                if isinstance(cover, Mapping)
                else None
            ),
            (
                cover.get("table_value_total")
                if isinstance(cover, Mapping)
                else None
            ),
            int(bool(parsed.get("cover_metadata_consistent"))),
            parsed["url"],
            parsed["sha256"],
        ),
    )


def refresh_sec_archive_fallbacks(
    targets: Iterable[Mapping[str, Any]],
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    index_dir: Path = DEFAULT_INDEX_DIR,
    fetcher: Fetcher | None = None,
    user_agent: str | None = None,
    refreshed_at: datetime | None = None,
) -> ArchiveFallbackRefreshResult:
    """Ingest exact SEC Archives filings missing from the quarterly index.

    Each target is independent: a transient or unparseable filing is reported
    unresolved while other proven filings may be accepted.  Fund JSON is not
    touched by this function.
    """

    normalized_targets = _normalize_archive_targets(targets)
    state_path = Path(state_path)
    index_dir = Path(index_dir)
    prior_state = load_13f_bulk_index(state_path)
    prior_index_path = _index_path_from_state(prior_state, state_path)
    active_fetcher = fetcher or make_sec_fetcher(user_agent)
    if not normalized_targets:
        return ArchiveFallbackRefreshResult(prior_state, False, (), (), ())
    temporary_path: Path | None = None
    finalized_candidate_path: Path | None = None
    promoted_state: dict[str, Any] | None = None
    resolved: list[str] = []
    reused: list[str] = []
    unresolved: list[dict[str, str]] = []
    accepted_at = _utc_timestamp(refreshed_at)
    try:
        temporary_path = _prepare_temporary_index(
            index_dir=index_dir,
            prior_index_path=prior_index_path,
            full_rebuild=prior_index_path is None,
        )
        connection = sqlite3.connect(temporary_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        _create_schema(connection)
        archive_sources = copy.deepcopy(prior_state.get("archive_sources", {}))
        try:
            for target in normalized_targets:
                accession = target["accession"]
                existing = connection.execute(
                    """
                    SELECT s.cik, s.report_date, f.accession AS chain_accession
                      FROM submissions AS s
                      LEFT JOIN filing_chain AS f USING(accession)
                     WHERE s.accession = ?
                    """,
                    (accession,),
                ).fetchone()
                existing_identity_matches = existing is not None and (
                    existing["cik"] == target["cik"]
                    and existing["report_date"] == target["report_date"]
                )
                if existing_identity_matches and existing["chain_accession"] is not None:
                    reused.append(accession)
                    continue
                index_url = sec_archive_index_url(target["cik"], accession)
                submission_url = sec_archive_submission_url(
                    target["cik"], accession
                )
                connection.execute("SAVEPOINT archive_fallback")
                try:
                    index_payload = _fetch_bytes(active_fetcher, index_url)
                    index_metadata = parse_sec_archive_index(
                        index_payload,
                        cik=target["cik"],
                        accession=accession,
                    )
                    if index_metadata["submission_url"] != submission_url:
                        raise DatasetParseError(
                            "SEC archive index resolved an unexpected submission URL"
                        )
                    submission_payload = _fetch_bytes(
                        active_fetcher,
                        submission_url,
                    )
                    parsed = parse_sec_archive_submission(
                        submission_payload,
                        cik=target["cik"],
                        accession=accession,
                        report_date=target["report_date"],
                        source_url=submission_url,
                    )
                    _insert_archive_evidence(connection, parsed)
                    archive_sources[submission_url] = {
                        "url": submission_url,
                        "sha256": parsed["sha256"],
                        "index_url": index_url,
                        "index_sha256": index_metadata["sha256"],
                        "accession": accession,
                        "cik": target["cik"],
                        "report_date": target["report_date"],
                        "accepted_at": accepted_at,
                        "method": parsed["method"],
                        "information_table_count": parsed[
                            "information_table_count"
                        ],
                    }
                except Exception as exc:
                    connection.execute("ROLLBACK TO archive_fallback")
                    connection.execute("RELEASE archive_fallback")
                    unresolved.append({
                        **target,
                        "reason": str(exc),
                    })
                    continue
                connection.execute("RELEASE archive_fallback")
                connection.commit()
                resolved.append(accession)
        finally:
            connection.close()
        if not resolved:
            temporary_path.unlink(missing_ok=True)
            return ArchiveFallbackRefreshResult(
                prior_state,
                False,
                (),
                tuple(sorted(set(reused))),
                tuple(unresolved),
            )
        final_path, checksum, size = _finalize_index(
            temporary_path,
            index_dir=index_dir,
        )
        finalized_candidate_path = final_path
        temporary_path = None
        summary_connection = _open_index(final_path, read_only=True)
        try:
            submission_count = summary_connection.execute(
                "SELECT COUNT(*) FROM submissions"
            ).fetchone()[0]
            information_count = summary_connection.execute(
                "SELECT COUNT(*) FROM information_table"
            ).fetchone()[0]
        finally:
            summary_connection.close()
        sources = copy.deepcopy(prior_state.get("sources", {}))
        candidate_state = {
            "schema_version": STATE_SCHEMA_VERSION,
            **(
                {
                    "clean_rebuild_plan_sha256": prior_state[
                        "clean_rebuild_plan_sha256"
                    ]
                }
                if prior_state.get("clean_rebuild_plan_sha256") is not None
                else {}
            ),
            "generated_at": accepted_at,
            "source_page": copy.deepcopy(prior_state.get("source_page"))
            or _empty_state()["source_page"],
            "sources": sources,
            "archive_sources": {
                url: archive_sources[url] for url in sorted(archive_sources)
            },
            "target_scope": copy.deepcopy(prior_state.get("target_scope"))
            or _empty_state()["target_scope"],
            "index": {
                "schema_version": INDEX_SCHEMA_VERSION,
                "path": _relative_index_path(final_path, state_path),
                "sha256": checksum,
                "size_bytes": size,
            },
            "summary": {
                "datasets": len(sources),
                "archive_filings": len(archive_sources),
                "submissions": submission_count,
                "information_table_rows": information_count,
            },
        }
        _validate_state(
            candidate_state,
            state_path=state_path,
            verify_index_checksum=False,
        )
        _atomic_write_json(state_path, candidate_state)
        promoted_state = candidate_state
        _cleanup_superseded_index_generations(
            index_dir=index_dir,
            active_index_path=final_path,
        )
        return ArchiveFallbackRefreshResult(
            candidate_state,
            True,
            tuple(sorted(set(resolved))),
            tuple(sorted(set(reused))),
            tuple(unresolved),
        )
    except Exception as exc:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        if promoted_state is None:
            _discard_unpublished_index(
                finalized_candidate_path,
                prior_index_path=prior_index_path,
                index_dir=index_dir,
            )
        failed = [
            {**target, "reason": f"SEC archive fallback refresh failed: {exc}"}
            for target in normalized_targets
        ]
        return ArchiveFallbackRefreshResult(
            promoted_state or prior_state,
            promoted_state is not None,
            (),
            (),
            tuple(failed),
        )


_REPORTED_IDENTITY_FIELDS = (
    "reported_issuer",
    "reported_class",
    "reported_cusip",
)
_REPORTED_DESCRIPTOR_FIELDS = (
    "reported_issuer",
    "reported_class",
)
_BACKFILL_MUTABLE_FIELDS = frozenset({
    *_REPORTED_IDENTITY_FIELDS,
    "reported_figi",
    "accession",
    "report_date",
})
_CONFIDENTIAL_PLACEHOLDER_CUSIPS = frozenset({
    "000000000",
    "000000NAN",
    "N/A",
})
_CONFIDENTIAL_PLACEHOLDER_LABELS = frozenset({"", "N/A", "NONE"})


def _is_zero_value_confidential_placeholder(
    holding: Mapping[str, Any],
) -> bool:
    """Whether a row is the SEC's exact zero-value confidential dummy row.

    Malformed and synthetic identifiers are otherwise evidence-bearing as-filed
    values.  In particular, a nonzero ``N/A`` or all-zero CUSIP is *not*
    exempted from exact verification.
    """

    cusip = _holding_cusip_key(holding)
    if cusip not in _CONFIDENTIAL_PLACEHOLDER_CUSIPS:
        return False
    retained_values = [
        holding[field]
        for field in ("reported_value", "value")
        if holding.get(field) is not None
    ]
    if not retained_values or any(
        not _decimal_equal(raw_value, 0) for raw_value in retained_values
    ):
        return False
    issuer = _normalize_text(
        holding.get("reported_issuer", holding.get("issuer"))
    ).upper()
    security_class = _normalize_text(
        holding.get("reported_class", holding.get("class"))
    ).upper()
    return (
        issuer in _CONFIDENTIAL_PLACEHOLDER_LABELS
        and security_class in _CONFIDENTIAL_PLACEHOLDER_LABELS
    )


def _needs_reported_identity_backfill(holding: Mapping[str, Any]) -> bool:
    if any(
        not _reported_descriptor_is_valid(holding, field)
        for field in _REPORTED_DESCRIPTOR_FIELDS
    ):
        return True
    return any(
        not str(holding.get(field) or "").strip()
        for field in ("reported_cusip", "accession", "report_date")
    )


def _reported_descriptor_is_missing(
    holding: Mapping[str, Any],
    field: str,
) -> bool:
    return field not in holding or holding[field] is None


def _reported_descriptor_is_valid(
    holding: Mapping[str, Any],
    field: str,
) -> bool:
    if _reported_descriptor_is_missing(holding, field):
        return False
    value = holding[field]
    return isinstance(value, str) and value == value.strip()


def _valid_iso_date(value: object | None) -> bool:
    raw = str(value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return False
    try:
        return date.fromisoformat(raw).isoformat() == raw
    except ValueError:
        return False


def _normalize_reported_identity_source(
    source: Mapping[str, Any],
) -> dict[str, str]:
    """Return one canonical, checksummed SEC identity-evidence reference."""

    expected_fields = {"accession", "report_date", "url", "sha256"}
    if set(source) != expected_fields:
        raise Sec13FBulkError(
            "reported_identity_sources entries must contain exactly "
            "accession, report_date, url, and sha256"
        )
    try:
        accession = _normalize_accession(source.get("accession"))
        report_date = _normalize_date(source.get("report_date"))
    except DatasetParseError as exc:
        raise Sec13FBulkError(
            "reported_identity_sources has invalid filing identity"
        ) from exc
    raw_url = str(source.get("url") or "").strip()
    try:
        url = normalize_sec_identity_source_url(raw_url, accession=accession)
    except (DatasetParseError, NonSECDatasetURL) as exc:
        raise Sec13FBulkError(
            "reported_identity_sources has an invalid SEC source URL"
        ) from exc
    checksum = str(source.get("sha256") or "").strip().lower()
    if _SHA256_RE.fullmatch(checksum) is None:
        raise Sec13FBulkError(
            "reported_identity_sources has an invalid source checksum"
        )
    return {
        "accession": accession,
        "report_date": report_date,
        "url": url,
        "sha256": checksum,
    }


def _canonical_reported_identity_sources(
    value: object | None,
) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise Sec13FBulkError("reported_identity_sources must be a list")
    normalized = [
        _normalize_reported_identity_source(source)
        for source in value
        if isinstance(source, Mapping)
    ]
    if len(normalized) != len(value):
        raise Sec13FBulkError(
            "reported_identity_sources entries must be objects"
        )
    canonical = sorted(
        normalized,
        key=lambda source: (
            source["accession"],
            source["report_date"],
            source["url"],
            source["sha256"],
        ),
    )
    if normalized != value or normalized != canonical or len({
        (source["accession"], source["report_date"], source["url"], source["sha256"])
        for source in canonical
    }) != len(canonical):
        raise Sec13FBulkError(
            "reported_identity_sources must be unique and canonically ordered"
        )
    return canonical


def _reported_identity_source_from_match(
    match: Mapping[str, Any],
) -> dict[str, str]:
    return _normalize_reported_identity_source({
        "accession": match.get("accession"),
        "report_date": match.get("report_date"),
        "url": match.get("source_url"),
        "sha256": match.get("source_sha256"),
    })


def reported_identity_backfill_audit(funds_dir: Path) -> dict[str, Any]:
    """Cheap, deterministic completeness audit with no network/index access.

    Only exact zero-value confidential-treatment dummy rows are ignored.
    Malformed/synthetic nonzero identifiers remain evidence-bearing, while
    malformed files and invalid provenance fail closed by setting ``needed``
    to true.
    """

    root = Path(funds_dir)
    audit: dict[str, Any] = {
        "needed": False,
        "files_scanned": 0,
        "holdings_scanned": 0,
        "placeholder_holdings": 0,
        "incomplete_holdings": 0,
        "missing_or_invalid_fields": {
            field: 0
            for field in (
                "reported_issuer",
                "reported_class",
                "reported_cusip",
                "accession",
                "report_date",
                "reported_identity_sources",
            )
        },
        "malformed_files": [],
    }
    if not root.is_dir():
        audit["needed"] = True
        audit["malformed_files"] = ["<funds-directory-missing>"]
        return audit
    for path in sorted(root.glob("*.json")):
        audit["files_scanned"] += 1
        try:
            fund = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            audit["malformed_files"].append(path.name)
            continue
        if not isinstance(fund, dict) or not isinstance(fund.get("quarters"), list):
            audit["malformed_files"].append(path.name)
            continue
        malformed_structure = False
        for quarter in fund["quarters"]:
            if not isinstance(quarter, dict) or not isinstance(
                quarter.get("holdings"), list
            ):
                malformed_structure = True
                break
            raw_identity_sources = quarter.get("reported_identity_sources")
            invalid_identity_sources = False
            try:
                identity_sources = _canonical_reported_identity_sources(
                    raw_identity_sources
                )
            except Sec13FBulkError:
                identity_sources = []
                invalid_identity_sources = raw_identity_sources is not None
            identity_source_pairs = {
                (source["accession"], source["report_date"])
                for source in identity_sources
            }
            for holding in quarter["holdings"]:
                if not isinstance(holding, dict):
                    malformed_structure = True
                    break
                if _is_zero_value_confidential_placeholder(holding):
                    audit["placeholder_holdings"] += 1
                    continue
                audit["holdings_scanned"] += 1
                missing: list[str] = []
                for field in _REPORTED_DESCRIPTOR_FIELDS:
                    if not _reported_descriptor_is_valid(holding, field):
                        missing.append(field)
                reported_cusip = str(holding.get("reported_cusip") or "").strip()
                if not reported_cusip:
                    missing.append("reported_cusip")
                accession = str(holding.get("accession") or "").strip()
                if not _ACCESSION_RE.fullmatch(accession):
                    missing.append("accession")
                if not _valid_iso_date(holding.get("report_date")):
                    missing.append("report_date")
                holding_report_date = str(holding.get("report_date") or "").strip()
                has_exact_source = (
                    accession,
                    holding_report_date,
                ) in identity_source_pairs
                if invalid_identity_sources or not has_exact_source:
                    missing.append("reported_identity_sources")
                if missing:
                    audit["incomplete_holdings"] += 1
                    for field in missing:
                        audit["missing_or_invalid_fields"][field] += 1
            if malformed_structure:
                break
        if malformed_structure:
            audit["malformed_files"].append(path.name)
    audit["malformed_files"] = sorted(set(audit["malformed_files"]))
    audit["needed"] = bool(
        audit["incomplete_holdings"] or audit["malformed_files"]
    )
    return audit


def reported_identity_backfill_needed(funds_dir: Path) -> bool:
    """Whether any retained non-placeholder holding lacks immutable evidence."""

    return bool(reported_identity_backfill_audit(funds_dir)["needed"])


def _quarter_accessions(quarter: Mapping[str, Any]) -> list[str]:
    candidates: list[object] = []
    applied = quarter.get("applied_accessions")
    if isinstance(applied, list) and applied:
        candidates.extend(applied)
    else:
        candidates.extend((quarter.get("accession"), quarter.get("base_accession")))
        source_filings = quarter.get("source_filings")
        if isinstance(source_filings, list):
            candidates.extend(
                source.get("accession")
                for source in source_filings
                if isinstance(source, dict) and source.get("applied") is not False
            )
    normalized: set[str] = set()
    for value in candidates:
        accession = _normalize_text(value)
        if _ACCESSION_RE.fullmatch(accession):
            normalized.add(accession)
    return sorted(normalized)


def collect_backfill_targets(
    fund_documents: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Summarize every retained, non-placeholder exact SEC evidence target.

    A clean rebuild must index complete-looking holdings too: completeness is
    not proof that their immutable fields agree with the SEC filing.
    """

    accessions: set[str] = set()
    periods: set[tuple[str, str]] = set()
    holdings = 0
    missing_holdings = 0
    for fund in fund_documents:
        try:
            cik = _normalize_cik(fund.get("cik"))
        except DatasetParseError:
            cik = None
        quarters = fund.get("quarters")
        if not isinstance(quarters, list):
            continue
        for quarter in quarters:
            if not isinstance(quarter, dict):
                continue
            rows = quarter.get("holdings")
            if not isinstance(rows, list):
                continue
            target_rows = [
                row
                for row in rows
                if isinstance(row, dict)
                and not _is_zero_value_confidential_placeholder(row)
            ]
            if not target_rows:
                continue
            holdings += len(target_rows)
            missing_holdings += sum(
                _needs_reported_identity_backfill(row) for row in target_rows
            )
            try:
                report_date = _normalize_date(quarter.get("report_date"))
            except DatasetParseError:
                continue
            if cik is not None:
                periods.add((cik, report_date))
            accessions.update(_quarter_accessions(quarter))
            for row in target_rows:
                accession = _normalize_text(row.get("accession"))
                if _ACCESSION_RE.fullmatch(accession):
                    accessions.add(accession)
    return {
        "accessions": sorted(accessions),
        "periods": [
            {"cik": cik, "report_date": report_date}
            for cik, report_date in sorted(periods)
        ],
        "holdings_targeted": holdings,
        "holdings_missing_reported_identity": missing_holdings,
    }


def collect_backfill_targets_from_funds(
    funds_dir: Path,
) -> dict[str, Any]:
    # ``collect_backfill_targets`` consumes its iterable once, so yielding one
    # decoded fund at a time bounds this all-corpus pass to the largest single
    # fund file instead of retaining every holding document in memory.
    return collect_backfill_targets(_iter_fund_documents(funds_dir))


def _query_quarter_evidence(
    connection: sqlite3.Connection,
    *,
    cik: str,
    report_date: str,
    accessions: Sequence[str],
) -> list[dict[str, Any]]:
    has_filing_chain = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'filing_chain'"
    ).fetchone() is not None
    chain_columns = (
        """
        , f.acceptance_datetime, f.cover_is_amendment,
          f.cover_amendment_type, f.cover_table_entry_total,
          f.cover_table_value_total, f.cover_metadata_consistent
        """
        if has_filing_chain
        else """
        , NULL AS acceptance_datetime, NULL AS cover_is_amendment,
          NULL AS cover_amendment_type, NULL AS cover_table_entry_total,
          NULL AS cover_table_value_total, NULL AS cover_metadata_consistent
        """
    )
    chain_join = " LEFT JOIN filing_chain AS f USING(accession)" if has_filing_chain else ""
    select = f"""
        SELECT i.*, s.cik, s.report_date, s.filing_date, s.submission_type
               {chain_columns}
          FROM information_table AS i
          JOIN submissions AS s USING(accession)
          {chain_join}
    """
    if accessions:
        placeholders = ",".join("?" for _ in accessions)
        query = select + f" WHERE i.accession IN ({placeholders})"
        parameters: tuple[object, ...] = tuple(accessions)
    else:
        query = select + " WHERE s.cik = ? AND s.report_date = ?"
        parameters = (cik, report_date)
    query += " ORDER BY i.accession, i.infotable_sk"
    rows = [dict(row) for row in connection.execute(query, parameters)]
    return rows if accessions else _select_canonical_filing_chain(rows)


def _filing_row_multiset(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[tuple[str, ...], ...]:
    """Canonical content of a complete information table, excluding storage."""

    fields = (
        "reported_issuer",
        "reported_class",
        "reported_cusip",
        "reported_figi",
        "reported_value",
        "reported_shares",
        "share_amount_type",
        "put_call",
        "investment_discretion",
        "other_manager",
    )
    return tuple(sorted(
        tuple("" if row.get(field) is None else str(row.get(field)) for field in fields)
        for row in rows
    ))


def _select_canonical_filing_chain(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Select an exact complete restatement or duplicate evidence source.

    This is deliberately limited to accessionless legacy quarters. Every
    candidate accession must have checksum-bound cover metadata whose entry and
    value totals reproduce its complete information table. A coherent latest
    RESTATEMENT replaces earlier originals. Multiple coherent originals are
    collapsed only when their full normalized row multisets are identical; in
    that case the latest SEC acceptance is merely the deterministic evidence
    source, not an inference that different economic content was superseded.
    """

    by_accession: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        by_accession.setdefault(str(row.get("accession") or ""), []).append(row)
    if len(by_accession) <= 1:
        return [dict(row) for row in rows]

    eligible: dict[str, list[dict[str, Any]]] = {}
    for accession, accession_rows in by_accession.items():
        first = accession_rows[0]
        acceptance = str(first.get("acceptance_datetime") or "")
        if (
            len(acceptance) != 14
            or first.get("cover_is_amendment") not in {0, 1}
            or first.get("cover_table_entry_total") is None
            or first.get("cover_table_value_total") is None
            or any(
                row.get("acceptance_datetime") != first.get("acceptance_datetime")
                or row.get("cover_is_amendment") != first.get("cover_is_amendment")
                or row.get("cover_amendment_type")
                != first.get("cover_amendment_type")
                or row.get("cover_metadata_consistent")
                != first.get("cover_metadata_consistent")
                for row in accession_rows
            )
        ):
            # Partial chain enrichment must never cause the enriched subset to
            # win. A checksum-bound but internally inconsistent cover is
            # different: it may be ignored when another complete filing is
            # coherent.
            return [dict(row) for row in rows]
        if first.get("cover_metadata_consistent") == 1:
            eligible[accession] = accession_rows
    if not eligible:
        return [dict(row) for row in rows]

    restatements = {
        accession: accession_rows
        for accession, accession_rows in eligible.items()
        if accession_rows[0].get("cover_is_amendment") == 1
        and accession_rows[0].get("cover_amendment_type") == "RESTATEMENT"
        and accession_rows[0].get("submission_type") == "13F-HR/A"
    }
    if restatements:
        latest = max(
            restatements,
            key=lambda accession: (
                str(restatements[accession][0]["acceptance_datetime"]),
                accession,
            ),
        )
        latest_time = str(restatements[latest][0]["acceptance_datetime"])
        if sum(
            str(candidate[0]["acceptance_datetime"]) == latest_time
            for candidate in restatements.values()
        ) != 1:
            return [dict(row) for row in rows]
        return restatements[latest]

    originals = {
        accession: accession_rows
        for accession, accession_rows in eligible.items()
        if accession_rows[0].get("cover_is_amendment") == 0
        and accession_rows[0].get("cover_amendment_type") is None
        and accession_rows[0].get("submission_type") == "13F-HR"
    }
    if len(originals) != len(eligible):
        return [dict(row) for row in rows]
    multisets = {
        _filing_row_multiset(accession_rows)
        for accession_rows in originals.values()
    }
    if len(multisets) != 1:
        return [dict(row) for row in rows]
    latest = max(
        originals,
        key=lambda accession: (
            str(originals[accession][0]["acceptance_datetime"]),
            accession,
        ),
    )
    latest_time = str(originals[latest][0]["acceptance_datetime"])
    if sum(
        str(candidate[0]["acceptance_datetime"]) == latest_time
        for candidate in originals.values()
    ) != 1:
        return [dict(row) for row in rows]
    return originals[latest]


def _holding_cusip_key(holding: Mapping[str, Any]) -> str:
    return str(
        holding.get("reported_cusip") or holding.get("cusip") or ""
    ).strip().upper()


def _holding_option_side(holding: Mapping[str, Any]) -> str:
    for field in ("put_call", "option_type"):
        value = _normalize_text(holding.get(field)).upper()
        if value in {"CALL", "PUT"}:
            return value
    holding_type = _normalize_text(holding.get("holding_type")).upper()
    return holding_type if holding_type in {"CALL", "PUT"} else ""


def _holding_share_amount_type(holding: Mapping[str, Any]) -> str | None:
    explicit = _normalize_text(holding.get("share_amount_type")).upper()
    if explicit:
        return explicit
    holding_type = _normalize_text(holding.get("holding_type")).upper()
    if holding_type == "NOTE":
        return "PRN"
    if holding_type in {"CALL", "PUT"}:
        return "SH"
    return None


def _decimal_equal(left: object, right: object) -> bool:
    try:
        left_decimal = Decimal(str(left).replace(",", ""))
        right_decimal = Decimal(str(right).replace(",", ""))
    except (InvalidOperation, ValueError):
        return False
    return (
        left_decimal.is_finite()
        and right_decimal.is_finite()
        and left_decimal == right_decimal
    )


_FORM_13F_VALUE_MULTIPLIERS = (Decimal(1), Decimal(1000))


def _valid_value_multiplier(value: object) -> Decimal | None:
    try:
        multiplier = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return (
        multiplier
        if multiplier in _FORM_13F_VALUE_MULTIPLIERS
        else None
    )


def _source_value_multiplier(
    quarter: Mapping[str, Any],
    record: Mapping[str, Any],
) -> tuple[Decimal | None, bool]:
    """Return an accession-bound multiplier and whether it was declared.

    The retained corpus can contain amendments whose applied components use
    different value units.  A report-date default therefore cannot describe
    the exact SEC row.  Trust a component multiplier only when its raw and
    normalized totals prove the same scaling relationship.  A malformed or
    conflicting declaration is terminal rather than silently ignored.
    """

    source_filings = quarter.get("source_filings")
    if not isinstance(source_filings, list):
        return None, False
    accession = _normalize_text(record.get("accession"))
    all_matching = [
        source
        for source in source_filings
        if isinstance(source, Mapping)
        and _normalize_text(source.get("accession")) == accession
    ]
    matching = [
        source for source in all_matching
        if source.get("applied") is not False
    ]
    if all_matching and not matching:
        return None, True
    declared = [
        source for source in matching
        if source.get("value_multiplier") is not None
    ]
    if not declared:
        return None, False

    multipliers: set[Decimal] = set()
    for source in declared:
        multiplier = _valid_value_multiplier(source.get("value_multiplier"))
        try:
            reported_total = Decimal(str(source.get("reported_value_total")))
            normalized_total = Decimal(str(source.get("normalized_value_total")))
        except (InvalidOperation, ValueError):
            return None, True
        if (
            multiplier is None
            or not reported_total.is_finite()
            or not normalized_total.is_finite()
            or reported_total * multiplier != normalized_total
        ):
            return None, True
        multipliers.add(multiplier)
    if len(multipliers) != 1:
        return None, True
    return multipliers.pop(), True


def _repair_value_multiplier(
    holding: Mapping[str, Any],
    quarter: Mapping[str, Any],
    record: Mapping[str, Any],
) -> tuple[Decimal | None, bool]:
    """Return a checksum-audited historical row-repair multiplier."""

    repair = quarter.get("value_unit_repair")
    if not isinstance(repair, Mapping):
        return None, False
    evidence = repair.get("evidence")
    if not isinstance(evidence, Mapping):
        return None, True
    repair_accession = _normalize_text(evidence.get("sec_accession"))
    if (
        repair.get("confidence") != "high"
        or not _ACCESSION_RE.fullmatch(repair_accession)
    ):
        return None, True
    if repair_accession != _normalize_text(record.get("accession")):
        return None, True
    row_multipliers = evidence.get("row_value_multipliers")
    if not isinstance(row_multipliers, Mapping):
        return None, True
    cusip = _holding_cusip_key(holding)
    raw = row_multipliers.get(cusip, row_multipliers.get("default"))
    multiplier = _valid_value_multiplier(raw)
    return multiplier, True


def _candidate_value_multipliers(
    holding: Mapping[str, Any],
    quarter: Mapping[str, Any],
    record: Mapping[str, Any],
) -> tuple[Decimal, ...]:
    """Return the only SEC Form 13F unit conventions allowed for this row."""

    repair_multiplier, repair_declared = _repair_value_multiplier(
        holding,
        quarter,
        record,
    )
    if repair_declared:
        return (repair_multiplier,) if repair_multiplier is not None else ()

    source_multiplier, source_declared = _source_value_multiplier(
        quarter,
        record,
    )
    if source_declared:
        return (source_multiplier,) if source_multiplier is not None else ()

    quarter_multiplier = _valid_value_multiplier(
        quarter.get("value_multiplier")
    )
    if quarter_multiplier is not None:
        return (quarter_multiplier,)

    # Some retained historical quarters predate unit-provenance fields, and
    # some modern filers still submit values in thousands.  In that absence,
    # equality under exactly the two documented conventions is evidence; a
    # date guess is not.  All other identity and numeric fields remain exact.
    return _FORM_13F_VALUE_MULTIPLIERS


def _candidate_matches_holding(
    holding: Mapping[str, Any],
    quarter: Mapping[str, Any],
    record: Mapping[str, Any],
) -> bool:
    if not _holding_cusip_key(holding):
        return False
    if _holding_cusip_key(holding) != str(record.get("cusip_key") or ""):
        return False
    holding_accession = _normalize_text(holding.get("accession"))
    if holding_accession and holding_accession != record.get("accession"):
        return False
    applied_accessions = quarter.get("applied_accessions")
    if isinstance(applied_accessions, list) and applied_accessions:
        normalized_applied = {
            _normalize_text(accession) for accession in applied_accessions
            if _ACCESSION_RE.fullmatch(_normalize_text(accession))
        }
        if _normalize_text(record.get("accession")) not in normalized_applied:
            return False
    option_side = _holding_option_side(holding)
    record_option = _normalize_text(record.get("put_call")).upper()
    if record_option != option_side:
        return False
    share_amount_type = _holding_share_amount_type(holding)
    if (
        share_amount_type is not None
        and share_amount_type
        != _normalize_text(record.get("share_amount_type")).upper()
    ):
        return False
    if holding.get("shares_imputed") is not True:
        holding_shares = holding.get("reported_shares", holding.get("shares"))
        if holding_shares is not None and not _decimal_equal(
            holding_shares,
            record.get("reported_shares"),
        ):
            return False
    if holding.get("reported_value") is not None:
        if not _decimal_equal(
            holding["reported_value"],
            record.get("reported_value"),
        ):
            return False
    elif holding.get("value") is not None:
        try:
            reported_value = Decimal(str(record.get("reported_value")))
        except (InvalidOperation, ValueError):
            return False
        if (
            not reported_value.is_finite()
            or not any(
                _decimal_equal(
                    holding["value"],
                    reported_value * multiplier,
                )
                for multiplier in _candidate_value_multipliers(
                    holding,
                    quarter,
                    record,
                )
            )
        ):
            return False
    return True


def _candidate_reported_identity_consistent(
    holding: Mapping[str, Any],
    record: Mapping[str, Any],
) -> bool:
    for field in _REPORTED_DESCRIPTOR_FIELDS:
        if _reported_descriptor_is_missing(holding, field):
            continue
        if (
            not _reported_descriptor_is_valid(holding, field)
            or holding[field] != record.get(field)
        ):
            return False
    for field in ("reported_cusip",):
        current = holding.get(field)
        if current is not None and str(current).strip() and str(current) != str(
            record.get(field) or ""
        ):
            return False
    # A missing key is a legacy wildcard that may be backfilled.  An explicit
    # JSON null is different: it records that the exact SEC information-table
    # row omitted FIGI.  Preserving that distinction is what lets a complete
    # reconstructed bucket containing otherwise identical FIGI/null-FIGI rows
    # verify one-to-one instead of becoming ambiguous again on the next pass.
    if "reported_figi" in holding:
        current_figi = holding["reported_figi"]
        record_figi = record.get("reported_figi")
        if current_figi is None:
            if record_figi is not None:
                return False
        elif (
            not isinstance(current_figi, str)
            or not current_figi
            or current_figi != current_figi.strip()
            or current_figi != record_figi
        ):
            return False
    current_report_date = _normalize_text(holding.get("report_date"))
    if current_report_date and current_report_date != record.get("report_date"):
        return False
    return True


def _match_holding(
    holding: Mapping[str, Any],
    quarter: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any] | None, str]:
    grouped: dict[tuple[str, ...], list[Mapping[str, Any]]] = {}
    for row in candidates:
        key = tuple(str(row.get(field) or "") for field in (
            "accession",
            "cik",
            "report_date",
            "reported_issuer",
            "reported_class",
            "reported_cusip",
            "reported_figi",
            "share_amount_type",
            "put_call",
        ))
        grouped.setdefault(key, []).append(row)
    expanded_candidates: list[Mapping[str, Any]] = list(candidates)
    for rows in grouped.values():
        if len(rows) < 2:
            continue
        aggregate = dict(rows[0])
        aggregate["reported_value"] = _decimal_text(
            sum(Decimal(str(row["reported_value"])) for row in rows),
            field="aggregated reported value",
        )
        aggregate["reported_shares"] = _decimal_text(
            sum(Decimal(str(row["reported_shares"])) for row in rows),
            field="aggregated reported shares",
        )
        aggregate["infotable_sk"] = "aggregate:" + _sha256_bytes(
            _canonical_json_bytes({
                "rows": sorted(str(row.get("infotable_sk") or "") for row in rows)
            })
        )[:24]
        expanded_candidates.append(aggregate)
    exact_rows = [
        row
        for row in expanded_candidates
        if _candidate_matches_holding(holding, quarter, row)
    ]
    if not exact_rows:
        return None, "unmatched"
    consistent = [
        row
        for row in exact_rows
        if _candidate_reported_identity_consistent(holding, row)
    ]
    if not consistent:
        return None, "conflict"
    # Multiple SEC rows (or an original row plus a same-identity aggregate)
    # can be indistinguishable for every immutable field we publish.  Their
    # INFOTABLE_SK values are storage keys, not security identity. Collapse
    # only those evidence-equivalent paths; descriptor or numeric differences
    # remain genuinely ambiguous and fail closed.
    equivalent: dict[tuple[str, ...], Mapping[str, Any]] = {}
    for row in consistent:
        key = tuple(str(row.get(field) or "") for field in (
            "accession",
            "cik",
            "report_date",
            "reported_issuer",
            "reported_class",
            "reported_cusip",
            "reported_figi",
            "reported_value",
            "reported_shares",
            "share_amount_type",
            "put_call",
            "source_url",
            "source_sha256",
        ))
        prior = equivalent.get(key)
        if prior is None or str(row.get("infotable_sk") or "") < str(
            prior.get("infotable_sk") or ""
        ):
            equivalent[key] = row
    if len(equivalent) != 1:
        return None, "ambiguous"
    return next(iter(equivalent.values())), "exact"


def _json_number(value: Decimal) -> int | float:
    """Return a finite JSON number without changing an exact integer."""

    if not value.is_finite():
        raise Sec13FBulkError("SEC Form 13F reconstruction produced a non-finite number")
    integral = value.to_integral_value()
    return int(integral) if value == integral else float(value)


def _legacy_bucket_has_no_reported_identity(
    holdings: Sequence[Mapping[str, Any]],
) -> bool:
    """Whether a lossy legacy bucket may be replaced from exact SEC rows."""

    protected = {
        *_REPORTED_IDENTITY_FIELDS,
        "reported_figi",
        "accession",
        "report_date",
    }
    return all(
        not holding.get("shares_imputed")
        and not any(field in holding for field in protected)
        for holding in holdings
    )


_WHOLE_QUARTER_PROTECTED_FIELDS = frozenset({
    "accession",
    "applied_accessions",
    "base_accession",
    "composition_hash",
    "composition_hash_version",
    "composition_version",
    "is_complete",
    "reported_identity_sources",
    "security_identity_version",
    "source_filings",
    "value_multiplier",
    "value_unit_confidence",
    "value_unit_evidence",
    "value_unit_method",
    "value_unit_policy_version",
    "value_unit_repair",
})
_WHOLE_QUARTER_PROTECTED_HOLDING_FIELDS = frozenset({
    *_REPORTED_IDENTITY_FIELDS,
    "accession",
    "investment_discretion",
    "other_manager",
    "put_call",
    "report_date",
    "reported_figi",
    "reported_shares",
    "reported_value",
    "share_amount_type",
    "shares_imputed",
    "source_hash",
    "source_sha256",
    "source_url",
})


def _legacy_quarter_has_no_identity_or_composition_metadata(
    quarter: Mapping[str, Any],
) -> bool:
    """Whether replacing the entire lossy legacy quarter is permissible."""

    holdings = quarter.get("holdings")
    return (
        isinstance(holdings, list)
        and bool(holdings)
        and not any(field in quarter for field in _WHOLE_QUARTER_PROTECTED_FIELDS)
        and not any(
            str(field).startswith(("composition_", "reported_", "source_"))
            for field in quarter
        )
        and all(
            isinstance(holding, Mapping)
            and not any(
                field in holding
                for field in _WHOLE_QUARTER_PROTECTED_HOLDING_FIELDS
            )
            and not any(
                str(field).startswith(("reported_", "source_"))
                for field in holding
            )
            for holding in holdings
        )
    )


def _complete_whole_quarter_accession(
    records: Sequence[Mapping[str, Any]],
    *,
    cik: str,
    report_date: str,
) -> tuple[list[dict[str, Any]], str] | None:
    """Return one cover-reconciled, exact SEC accession or fail closed.

    The information-table index alone cannot prove that a bulk-data slice is
    complete. Whole-quarter replacement therefore requires SEC Archives cover
    metadata and exact agreement on row count. A filer-authored cover summary
    may differ from its complete lines by no more than one raw value unit per
    entry; that narrow inconsistency is tolerated while the reconstructed
    total always comes from the exact indexed rows. This remains intentionally
    stricter than per-row backfill and bucket reconstruction.
    """

    normalized = [dict(record) for record in records]
    accessions = {
        _normalize_text(record.get("accession")) for record in normalized
    }
    if (
        not normalized
        or len(accessions) != 1
        or not all(_ACCESSION_RE.fullmatch(value) for value in accessions)
        or any(
            record.get("cik") != cik
            or record.get("report_date") != report_date
            for record in normalized
        )
    ):
        return None
    accession = next(iter(accessions))
    storage_keys = [_normalize_text(record.get("infotable_sk")) for record in normalized]
    if not all(storage_keys) or len(set(storage_keys)) != len(storage_keys):
        return None

    first = normalized[0]
    acceptance_datetime = _normalize_text(first.get("acceptance_datetime"))
    submission_type = _normalize_text(first.get("submission_type")).upper()
    is_amendment = first.get("cover_is_amendment")
    amendment_type = first.get("cover_amendment_type")
    canonical_filing_kind = (
        submission_type == "13F-HR"
        and is_amendment == 0
        and amendment_type is None
    ) or (
        submission_type == "13F-HR/A"
        and is_amendment == 1
        and amendment_type == "RESTATEMENT"
    )
    if not re.fullmatch(r"\d{14}", acceptance_datetime) or not canonical_filing_kind:
        return None
    cover_entries = first.get("cover_table_entry_total")
    try:
        cover_value = Decimal(str(first.get("cover_table_value_total")))
        raw_value_total = sum(
            (Decimal(str(record.get("reported_value"))) for record in normalized),
            Decimal(0),
        )
    except (InvalidOperation, ValueError):
        return None
    if (
        first.get("cover_metadata_consistent") not in {0, 1}
        or isinstance(cover_entries, bool)
        or not isinstance(cover_entries, int)
        or cover_entries != len(normalized)
        or not cover_value.is_finite()
        or cover_value < 0
        or not raw_value_total.is_finite()
        or abs(cover_value - raw_value_total) > Decimal(cover_entries)
        or any(
            record.get("acceptance_datetime") != acceptance_datetime
            or record.get("submission_type") != submission_type
            or record.get("cover_is_amendment") != is_amendment
            or record.get("cover_amendment_type") != amendment_type
            or record.get("cover_metadata_consistent")
            != first.get("cover_metadata_consistent")
            or record.get("cover_table_entry_total") != cover_entries
            or str(record.get("cover_table_value_total"))
            != str(first.get("cover_table_value_total"))
            for record in normalized
        )
    ):
        return None

    for record in normalized:
        issuer = record.get("reported_issuer")
        security_class = record.get("reported_class")
        reported_cusip = record.get("reported_cusip")
        reported_figi = record.get("reported_figi")
        try:
            reported_value = Decimal(str(record.get("reported_value")))
            reported_shares = Decimal(str(record.get("reported_shares")))
        except (InvalidOperation, ValueError):
            return None
        if (
            not isinstance(issuer, str)
            or issuer != issuer.strip()
            or not isinstance(security_class, str)
            or security_class != security_class.strip()
            or not isinstance(reported_cusip, str)
            or not reported_cusip
            or reported_cusip != reported_cusip.strip()
            or record.get("cusip_key") != reported_cusip.upper()
            or (
                reported_figi is not None
                and (
                    not isinstance(reported_figi, str)
                    or not reported_figi
                    or reported_figi != reported_figi.strip()
                )
            )
            or not reported_value.is_finite()
            or reported_value < 0
            or not reported_shares.is_finite()
            or reported_shares < 0
        ):
            return None

    source_fields = (
        _normalize_text(first.get("source_url")),
        _normalize_text(first.get("source_sha256")).lower(),
    )
    if any(
        (
            _normalize_text(record.get("source_url")),
            _normalize_text(record.get("source_sha256")).lower(),
        )
        != source_fields
        for record in normalized
    ):
        return None
    try:
        _reported_identity_source_from_match(first)
    except Sec13FBulkError:
        return None
    return normalized, accession


def _complete_whole_quarter_filing(
    records: Sequence[Mapping[str, Any]],
    *,
    cik: str,
    report_date: str,
) -> tuple[list[dict[str, Any]], str] | None:
    """Return one cover-complete canonical filing or fail closed.

    Whole-quarter replacement has no trustworthy retained fingerprint. It is
    therefore permitted only when the SEC candidate set contains one accession
    total; an original plus any amendment remains ambiguous here.
    """

    normalized = [dict(record) for record in records]
    if not normalized or any(
        record.get("cik") != cik
        or record.get("report_date") != report_date
        or not _ACCESSION_RE.fullmatch(
            _normalize_text(record.get("accession"))
        )
        for record in normalized
    ):
        return None

    accessions = {
        _normalize_text(record.get("accession")) for record in normalized
    }
    if len(accessions) != 1:
        return None
    return _complete_whole_quarter_accession(
        normalized,
        cik=cik,
        report_date=report_date,
    )


def _whole_quarter_value_unit_decision(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Classify filing-wide units only from broad, intrinsic SEC row evidence."""

    evidence_rows: list[dict[str, Any]] = []
    for record in records:
        try:
            value = Decimal(str(record.get("reported_value")))
            shares = Decimal(str(record.get("reported_shares")))
        except (InvalidOperation, ValueError):
            return None
        if not value.is_finite() or not shares.is_finite():
            return None
        put_call = _normalize_text(record.get("put_call")).upper()
        amount_type = _normalize_text(record.get("share_amount_type")).upper()
        if put_call not in {"", "CALL", "PUT"} or amount_type not in {
            "SH",
            "PRN",
        }:
            return None
        evidence_rows.append({
            "cusip": _normalize_text(record.get("reported_cusip")).upper(),
            "class": record.get("reported_class"),
            "value": _json_number(value),
            "shares": _json_number(shares),
            "share_amount_type": amount_type,
            "put_call": put_call,
            "holding_type": (
                put_call
                if put_call
                else ("NOTE" if amount_type == "PRN" else "EQUITY")
            ),
        })
    try:
        decision = classify_value_units(evidence_rows)
    except (AmbiguousValueUnits, ValueError, TypeError):
        return None
    if (
        decision.get("value_unit_confidence") != "high"
        or decision.get("value_multiplier") not in {1, 1000}
    ):
        return None
    return decision


def _record_reconstruction_multiplier_options(
    quarter: Mapping[str, Any],
    record: Mapping[str, Any],
) -> tuple[Decimal, ...]:
    synthetic_holding = {"cusip": record.get("reported_cusip")}
    return _candidate_value_multipliers(
        synthetic_holding,
        quarter,
        record,
    )


def _reconstructed_identity_key(record: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(record.get(field) or "") for field in (
        "accession",
        "reported_issuer",
        "reported_class",
        "reported_cusip",
        "reported_figi",
        "share_amount_type",
        "put_call",
        "source_url",
        "source_sha256",
    ))


def _build_reconstructed_bucket(
    records: Sequence[Mapping[str, Any]],
    multipliers: Mapping[str, Decimal],
) -> tuple[tuple[tuple[str, ...], Decimal, Decimal], ...]:
    grouped: dict[tuple[str, ...], list[Decimal]] = {}
    for record in records:
        try:
            raw_value = Decimal(str(record.get("reported_value")))
            raw_shares = Decimal(str(record.get("reported_shares")))
        except (InvalidOperation, ValueError):
            return ()
        multiplier = multipliers.get(_normalize_text(record.get("accession")))
        if (
            multiplier is None
            or not raw_value.is_finite()
            or not raw_shares.is_finite()
        ):
            return ()
        totals = grouped.setdefault(
            _reconstructed_identity_key(record),
            [Decimal(0), Decimal(0)],
        )
        totals[0] += raw_value * multiplier
        totals[1] += raw_shares
    return tuple(sorted(
        (key, totals[0], totals[1])
        for key, totals in grouped.items()
    ))


def _reconstruct_legacy_cusip_bucket(
    holdings: Sequence[Mapping[str, Any]],
    quarter: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]] | None:
    """Split one lossy legacy CUSIP bucket into exact SEC identities.

    Existing issuer/class/ticker text is deliberately ignored. Reconstruction
    is allowed only when the exact SEC rows reproduce both retained numeric
    totals under a unique set of documented per-accession unit conventions.
    """

    if (
        not holdings
        or not records
        or not _legacy_bucket_has_no_reported_identity(holdings)
        or quarter.get("composition_hash_version") == 3
    ):
        return None
    try:
        expected_value = sum(
            (Decimal(str(holding.get("value"))) for holding in holdings),
            Decimal(0),
        )
        expected_shares = sum(
            (Decimal(str(holding.get("shares"))) for holding in holdings),
            Decimal(0),
        )
    except (InvalidOperation, ValueError):
        return None
    if not expected_value.is_finite() or not expected_shares.is_finite():
        return None

    options_by_accession: dict[str, set[Decimal]] = {}
    for record in records:
        accession = _normalize_text(record.get("accession"))
        options = _record_reconstruction_multiplier_options(quarter, record)
        if not accession or not options:
            return None
        if len(options) == 1:
            prior = options_by_accession.setdefault(accession, set(options))
            prior.intersection_update(options)
        else:
            options_by_accession.setdefault(accession, set(options))
    if any(not options for options in options_by_accession.values()):
        return None

    accessions = sorted(options_by_accession)
    candidate_outputs: set[
        tuple[tuple[tuple[str, ...], Decimal, Decimal], ...]
    ] = set()
    option_sets = [tuple(sorted(options_by_accession[a])) for a in accessions]
    for choices in product(*option_sets):
        assignment = dict(zip(accessions, choices, strict=True))
        output = _build_reconstructed_bucket(records, assignment)
        if not output:
            continue
        total_value = sum((row[1] for row in output), Decimal(0))
        total_shares = sum((row[2] for row in output), Decimal(0))
        if total_value == expected_value and total_shares == expected_shares:
            candidate_outputs.add(output)
    if len(candidate_outputs) != 1:
        return None

    reconstructed: list[dict[str, Any]] = []
    for key, normalized_value, reported_shares in next(iter(candidate_outputs)):
        (
            accession,
            reported_issuer,
            reported_class,
            reported_cusip,
            reported_figi,
            share_amount_type,
            put_call,
            _source_url,
            _source_sha256,
        ) = key
        holding_type = (
            put_call
            if put_call in {"CALL", "PUT"}
            else ("NOTE" if share_amount_type == "PRN" else "EQUITY")
        )
        holding: dict[str, Any] = {
            "ticker": None,
            "issuer": reported_issuer,
            "cusip": reported_cusip.upper(),
            "class": reported_class,
            "value": _json_number(normalized_value),
            "shares": _json_number(reported_shares),
            "holding_type": holding_type,
            "reported_issuer": reported_issuer,
            "reported_class": reported_class,
            "reported_cusip": reported_cusip,
            # Keep SEC null explicit.  Omitting this key would make the null
            # member of a proven FIGI/null-FIGI multiset a legacy wildcard and
            # lose the one-to-one assignment on strict re-verification.
            "reported_figi": reported_figi or None,
            "accession": accession,
            "report_date": _normalize_text(quarter.get("report_date")),
        }
        if share_amount_type:
            holding["share_amount_type"] = share_amount_type
        if put_call:
            holding["put_call"] = put_call
        reconstructed.append(holding)
    return reconstructed


def _reconstruct_legacy_whole_quarter(
    quarter: dict[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    cik: str,
    report_date: str,
    exact_first_pass_matches: int,
) -> tuple[bool, int]:
    """Replace a wholly corrupt accessionless quarter from one exact filing.

    This escape hatch is deliberately narrower than CUSIP-bucket repair. It
    applies only when the retained quarter has no immutable or composition
    metadata, none of its rows matches SEC evidence, and filing-chain plus
    cover metadata proves one complete canonical filing. Legacy display text,
    identifiers, types, values, and shares are never used to select or shape
    the replacement.
    """

    if (
        exact_first_pass_matches != 0
        or not _legacy_quarter_has_no_identity_or_composition_metadata(quarter)
    ):
        return False, 0
    complete = _complete_whole_quarter_filing(
        records,
        cik=cik,
        report_date=report_date,
    )
    if complete is None:
        return False, 0
    complete_records, accession = complete
    decision = _whole_quarter_value_unit_decision(complete_records)
    if decision is None:
        return False, 0
    multiplier = Decimal(decision["value_multiplier"])
    reconstructed_output = _build_reconstructed_bucket(
        complete_records,
        {accession: multiplier},
    )
    if not reconstructed_output:
        return False, 0

    normalized_total = sum(
        (normalized_value for _key, normalized_value, _shares in reconstructed_output),
        Decimal(0),
    )
    rebuilt: list[dict[str, Any]] = []
    for key, normalized_value, reported_shares in reconstructed_output:
        (
            record_accession,
            reported_issuer,
            reported_class,
            reported_cusip,
            reported_figi,
            share_amount_type,
            put_call,
            _source_url,
            _source_sha256,
        ) = key
        if (
            record_accession != accession
            or not reported_cusip
            or put_call not in {"", "CALL", "PUT"}
            or share_amount_type not in {"SH", "PRN"}
        ):
            return False, 0
        holding_type = (
            put_call
            if put_call
            else ("NOTE" if share_amount_type == "PRN" else "EQUITY")
        )
        holding: dict[str, Any] = {
            "ticker": None,
            "issuer": reported_issuer,
            "cusip": reported_cusip.upper(),
            "class": reported_class,
            "value": _json_number(normalized_value),
            "shares": _json_number(reported_shares),
            "holding_type": holding_type,
            "reported_issuer": reported_issuer,
            "reported_class": reported_class,
            "reported_cusip": reported_cusip,
            "reported_figi": reported_figi or None,
            "accession": accession,
            "report_date": report_date,
            "share_amount_type": share_amount_type,
        }
        if put_call:
            holding["put_call"] = put_call
        rebuilt.append(holding)

    matching_quarter = {**quarter, **decision, "holdings": rebuilt}
    by_cusip: dict[str, list[dict[str, Any]]] = {}
    for record in complete_records:
        by_cusip.setdefault(str(record.get("cusip_key") or ""), []).append(
            record
        )
    if any(
        _match_holding(
            holding,
            matching_quarter,
            by_cusip.get(_holding_cusip_key(holding), []),
        )[1]
        != "exact"
        for holding in rebuilt
    ):
        return False, 0

    filing_dates = {
        _normalize_text(record.get("filing_date")) for record in complete_records
    }
    if len(filing_dates) != 1:
        return False, 0
    filing_date = next(iter(filing_dates))
    if not _valid_iso_date(filing_date):
        return False, 0
    source = _reported_identity_source_from_match(complete_records[0])
    rebuilt.sort(key=lambda holding: (
        -float(holding.get("value", 0) or 0),
        str(holding.get("cusip") or ""),
        str(holding.get("class") or ""),
        str(holding.get("holding_type") or ""),
        str(holding.get("accession") or ""),
    ))
    quarter["holdings"] = rebuilt
    quarter["filing_date"] = filing_date
    quarter["num_holdings"] = len(rebuilt)
    quarter["total_value"] = _json_number(normalized_total)
    quarter.update(decision)
    quarter["reported_identity_sources"] = [source]
    return True, len(rebuilt)


def _legacy_quarter_matches_complete_filing(
    quarter: Mapping[str, Any],
    holdings: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
) -> bool:
    """Prove a one-to-one numeric/type projection against one SEC filing."""

    declared_count = quarter.get("num_holdings")
    if (
        isinstance(declared_count, bool)
        or not isinstance(declared_count, int)
        or declared_count != len(holdings)
    ):
        return False
    decision = _whole_quarter_value_unit_decision(records)
    if decision is None:
        return False
    multiplier = Decimal(str(decision["value_multiplier"]))

    def holding_signature(
        holding: Mapping[str, Any],
    ) -> tuple[str, str, str, Decimal, Decimal] | None:
        cusip = _holding_cusip_key(holding)
        put_call = _normalize_text(holding.get("put_call")).upper()
        holding_type = _normalize_text(holding.get("holding_type")).upper()
        if not put_call and holding_type in {"CALL", "PUT"}:
            put_call = holding_type
        amount_type = _normalize_text(
            holding.get("share_amount_type")
        ).upper()
        if not amount_type:
            amount_type = "PRN" if holding_type == "NOTE" else "SH"
        try:
            value = Decimal(str(holding.get("value")))
            shares = Decimal(str(holding.get("shares")))
        except (InvalidOperation, ValueError):
            return None
        if (
            not cusip
            or put_call not in {"", "CALL", "PUT"}
            or amount_type not in {"SH", "PRN"}
            or not value.is_finite()
            or value < 0
            or not shares.is_finite()
            or shares < 0
        ):
            return None
        return cusip, amount_type, put_call, value, shares

    holding_signatures = [holding_signature(holding) for holding in holdings]
    accessions = {
        _normalize_text(record.get("accession")) for record in records
    }
    if len(accessions) != 1:
        return False
    reconstructed = _build_reconstructed_bucket(
        records,
        {next(iter(accessions)): multiplier},
    )
    record_signatures = [
        (
            key[3].upper(),
            key[5].upper(),
            key[6].upper(),
            normalized_value,
            reported_shares,
        )
        for key, normalized_value, reported_shares in reconstructed
    ]
    if (
        not reconstructed
        or len(record_signatures) != len(holdings)
        or any(signature is None for signature in holding_signatures)
    ):
        return False
    if Counter(holding_signatures) != Counter(record_signatures):
        return False

    try:
        declared_total = Decimal(str(quarter.get("total_value")))
    except (InvalidOperation, ValueError):
        return False
    calculated_total = sum(
        (signature[3] for signature in holding_signatures if signature is not None),
        Decimal(0),
    )
    return declared_total.is_finite() and declared_total == calculated_total


def _matching_complete_legacy_filing(
    quarter: Mapping[str, Any],
    holdings: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], str] | None:
    """Identify exactly one SEC accession reproducing a legacy quarter.

    This binds immutable holding provenance only; it does not certify or alter
    amendment composition. The retained quarter must match the accession's
    entire indexed information-table projection and filing date, so a partial
    or indistinguishable filing still fails closed.
    """

    normalized = [dict(record) for record in records]
    ciks = {_normalize_text(record.get("cik")) for record in normalized}
    report_date = _normalize_text(quarter.get("report_date"))
    if (
        not normalized
        or len(ciks) != 1
        or not report_date
        or any(
            record.get("report_date") != report_date
            or not _ACCESSION_RE.fullmatch(
                _normalize_text(record.get("accession"))
            )
            for record in normalized
        )
    ):
        return None

    by_accession: dict[str, list[dict[str, Any]]] = {}
    for record in normalized:
        by_accession.setdefault(
            _normalize_text(record.get("accession")),
            [],
        ).append(record)

    legacy_filing_date = _normalize_text(quarter.get("filing_date"))
    matches: list[tuple[list[dict[str, Any]], str]] = []
    for accession in sorted(by_accession):
        accession_records = by_accession[accession]
        submission_types = {
            _normalize_text(record.get("submission_type")).upper()
            for record in accession_records
        }
        storage_keys = [
            _normalize_text(record.get("infotable_sk"))
            for record in accession_records
        ]
        if (
            len(submission_types) != 1
            or next(iter(submission_types)) not in {"13F-HR", "13F-HR/A"}
            or not all(storage_keys)
            or len(set(storage_keys)) != len(storage_keys)
        ):
            continue
        filing_dates = {
            _normalize_text(record.get("filing_date"))
            for record in accession_records
        }
        if len(filing_dates) != 1:
            continue
        exact_filing_date = next(iter(filing_dates))
        legacy_date_matches = legacy_filing_date == exact_filing_date or (
            bool(re.fullmatch(r"\d{4}-\d{2}", legacy_filing_date))
            and exact_filing_date.startswith(f"{legacy_filing_date}-")
        )
        if not legacy_date_matches or not _legacy_quarter_matches_complete_filing(
            quarter,
            holdings,
            accession_records,
        ):
            continue
        try:
            for record in accession_records:
                _reported_identity_source_from_match(record)
        except Sec13FBulkError:
            continue
        matches.append((accession_records, accession))
    if len(matches) != 1:
        return None
    return matches[0]


def _reconstruct_legacy_quarter_buckets(
    quarter: dict[str, Any],
    by_cusip: Mapping[str, Sequence[Mapping[str, Any]]],
    failed_cusips: set[str],
) -> tuple[bool, int]:
    holdings = quarter.get("holdings")
    if not isinstance(holdings, list):
        return False, 0
    positions: dict[str, list[dict[str, Any]]] = {}
    for holding in holdings:
        if isinstance(holding, dict) and not _is_zero_value_confidential_placeholder(
            holding
        ):
            positions.setdefault(_holding_cusip_key(holding), []).append(holding)

    known_accessions = set(_quarter_accessions(quarter))
    candidate_accessions = {
        _normalize_text(record.get("accession"))
        for records in by_cusip.values()
        for record in records
        if _ACCESSION_RE.fullmatch(_normalize_text(record.get("accession")))
    }
    reconstruction_records = by_cusip
    # With no retained composition provenance, multiple filings usually make
    # the applied accession unknowable. One narrow legacy case remains
    # provable: a later NEW HOLDINGS amendment can repeat every row from the
    # original filing, making each retained row ambiguous even though filing-
    # chain metadata identifies exactly one complete canonical filing. Use
    # that filing only when the entire legacy quarter is unresolved, its SEC
    # filing date agrees (including the old YYYY-MM representation), and its
    # CUSIP universe exactly matches the retained quarter. Otherwise fail
    # closed and leave the period for filing replay.
    if not known_accessions and len(candidate_accessions) != 1:
        if failed_cusips != set(positions):
            return False, 0
        all_records = [
            record
            for records in by_cusip.values()
            for record in records
        ]
        retained_holdings = [
            holding
            for bucket in positions.values()
            for holding in bucket
        ]
        complete = _matching_complete_legacy_filing(
            quarter,
            retained_holdings,
            all_records,
        )
        if complete is None:
            return False, 0
        complete_records, complete_accession = complete
        canonical_by_cusip: dict[str, list[dict[str, Any]]] = {}
        for record in complete_records:
            canonical_by_cusip.setdefault(
                str(record.get("cusip_key") or ""),
                [],
            ).append(record)
        if set(canonical_by_cusip) != set(positions):
            return False, 0
        reconstruction_records = canonical_by_cusip
        candidate_accessions = {complete_accession}

    replacements: dict[str, list[dict[str, Any]]] = {}
    reconstructed_rows = 0
    for cusip, bucket in positions.items():
        if cusip not in failed_cusips:
            continue
        records = list(reconstruction_records.get(cusip, ()))
        if not records:
            continue
        rebuilt = _reconstruct_legacy_cusip_bucket(bucket, quarter, records)
        if rebuilt is None:
            continue
        replacements[cusip] = rebuilt
        reconstructed_rows += len(rebuilt)
    if not replacements:
        return False, 0

    retained = [
        holding
        for holding in holdings
        if not (
            isinstance(holding, dict)
            and _holding_cusip_key(holding) in replacements
        )
    ]
    rebuilt_rows = [
        holding
        for cusip in sorted(replacements)
        for holding in replacements[cusip]
    ]
    quarter["holdings"] = retained + rebuilt_rows
    quarter["holdings"].sort(key=lambda holding: (
        -float(holding.get("value", 0) or 0),
        str(holding.get("cusip") or ""),
        str(holding.get("class") or ""),
        str(holding.get("holding_type") or ""),
        str(holding.get("accession") or ""),
    ))
    if isinstance(quarter.get("num_holdings"), int):
        quarter["num_holdings"] = len(quarter["holdings"])
    if "composition_version" in quarter:
        quarter["composition_hash"] = calculate_quarter_composition_hash(
            quarter,
            current_hash_version=3,
        )
    return True, reconstructed_rows


def backfill_fund_document(
    fund: Mapping[str, Any],
    *,
    connection: sqlite3.Connection,
) -> tuple[dict[str, Any], dict[str, int]]:
    """Return a copied fund document with uniquely matched raw fields filled."""

    updated = copy.deepcopy(dict(fund))
    stats = {
        "holdings_scanned": 0,
        "holdings_changed": 0,
        "exact_matches": 0,
        "unmatched": 0,
        "ambiguous": 0,
        "conflicts": 0,
    }
    try:
        cik = _normalize_cik(updated.get("cik"))
    except DatasetParseError:
        return updated, stats
    quarters = updated.get("quarters")
    if not isinstance(quarters, list):
        return updated, stats
    for quarter in quarters:
        if not isinstance(quarter, dict):
            continue
        holdings = quarter.get("holdings")
        if not isinstance(holdings, list):
            continue
        addressable_holdings = [
            row
            for row in holdings
            if isinstance(row, dict)
            and not _is_zero_value_confidential_placeholder(row)
        ]
        if not addressable_holdings:
            continue
        try:
            report_date = _normalize_date(quarter.get("report_date"))
        except DatasetParseError:
            continue
        accessions = set(_quarter_accessions(quarter))
        accessions.update(
            _normalize_text(row.get("accession"))
            for row in holdings
            if isinstance(row, dict)
            and _ACCESSION_RE.fullmatch(_normalize_text(row.get("accession")))
        )
        candidates = _query_quarter_evidence(
            connection,
            cik=cik,
            report_date=report_date,
            accessions=sorted(accessions),
        )
        has_new_holdings_amendment = any(
            _normalize_text(record.get("submission_type")).upper()
            == "13F-HR/A"
            and record.get("cover_is_amendment") == 1
            and record.get("cover_amendment_type") == "NEW HOLDINGS"
            for record in candidates
        )
        # An accessionless legacy quarter cannot compose a NEW HOLDINGS chain
        # or choose among multiple filings row by row. Require its complete
        # numeric/type fingerprint and filing date to select exactly one
        # cover-reconciled original/restatement. Explicit applied-accession
        # metadata remains authoritative and bypasses this legacy repair.
        if not accessions and has_new_holdings_amendment:
            complete = _matching_complete_legacy_filing(
                quarter,
                addressable_holdings,
                candidates,
            )
            candidates = complete[0] if complete is not None else []
        by_cusip: dict[str, list[dict[str, Any]]] = {}
        for record in candidates:
            if (
                record.get("cik") != cik
                or record.get("report_date") != report_date
            ):
                continue
            by_cusip.setdefault(str(record.get("cusip_key") or ""), []).append(
                record
            )
        # Retain the original object beside each cached result. Reconstruction
        # can remove a legacy dict and allocate a replacement at the same
        # CPython ``id``; an integer-only cache would then attach the removed
        # row's stale ambiguous result to a new exact SEC row.
        initial_matches = {
            id(holding): (
                holding,
                _match_holding(
                    holding,
                    quarter,
                    by_cusip.get(_holding_cusip_key(holding), []),
                ),
            )
            for holding in addressable_holdings
        }
        exact_first_pass_matches = sum(
            initial_matches[id(holding)][1][1] == "exact"
            for holding in addressable_holdings
        )
        failed_cusips = {
            _holding_cusip_key(holding)
            for holding in addressable_holdings
            if initial_matches[id(holding)][1][1] != "exact"
        }
        reconstructed, reconstructed_rows = _reconstruct_legacy_whole_quarter(
            quarter,
            candidates,
            cik=cik,
            report_date=report_date,
            exact_first_pass_matches=exact_first_pass_matches,
        )
        if not reconstructed:
            reconstructed, reconstructed_rows = (
                _reconstruct_legacy_quarter_buckets(
                    quarter,
                    by_cusip,
                    failed_cusips,
                )
            )
        if reconstructed:
            holdings = quarter["holdings"]
            addressable_holdings = [
                row
                for row in holdings
                if isinstance(row, dict)
                and not _is_zero_value_confidential_placeholder(row)
            ]
            stats["holdings_changed"] += reconstructed_rows

        # A legacy quarter can omit its applied accession even when most of
        # its positions identify the exact filing component.  Freeze the set
        # of accessions proved by those unique first-pass matches, then retry
        # only positions that were ambiguous across filings.  The set may
        # contain several accessions for a multi-component amendment.  It is
        # deliberately not expanded by the retry itself, and a retry replaces
        # the original result only when the narrowed evidence is unique.
        final_matches: list[
            tuple[
                dict[str, Any],
                tuple[Mapping[str, Any] | None, str],
            ]
        ] = []
        for holding in addressable_holdings:
            cached_entry = initial_matches.get(id(holding))
            cached_match = (
                cached_entry[1]
                if cached_entry is not None and cached_entry[0] is holding
                else None
            )
            if cached_match is None:
                cached_match = _match_holding(
                    holding,
                    quarter,
                    by_cusip.get(_holding_cusip_key(holding), []),
                )
            final_matches.append((holding, cached_match))

        established_accessions = {
            accession
            for _holding, (match, status) in final_matches
            if status == "exact"
            and match is not None
            and _ACCESSION_RE.fullmatch(
                accession := _normalize_text(match.get("accession"))
            )
        }
        if established_accessions:
            for index, (holding, (match, status)) in enumerate(final_matches):
                if status != "ambiguous":
                    continue
                narrowed_candidates = [
                    candidate
                    for candidate in by_cusip.get(
                        _holding_cusip_key(holding),
                        [],
                    )
                    if _normalize_text(candidate.get("accession"))
                    in established_accessions
                ]
                narrowed_match = _match_holding(
                    holding,
                    quarter,
                    narrowed_candidates,
                )
                if narrowed_match[1] == "exact":
                    final_matches[index] = (
                        holding,
                        narrowed_match,
                    )

        matched_sources: dict[tuple[str, str, str, str], dict[str, str]] = {}
        all_exact = True
        for holding, (match, status) in final_matches:
            stats["holdings_scanned"] += 1
            if match is None:
                stats[status if status != "conflict" else "conflicts"] += 1
                all_exact = False
                continue
            before_canonical = {
                key: copy.deepcopy(value)
                for key, value in holding.items()
                if key not in _BACKFILL_MUTABLE_FIELDS
            }
            changed = False
            for field in _REPORTED_DESCRIPTOR_FIELDS:
                if _reported_descriptor_is_missing(holding, field):
                    holding[field] = match[field]
                    changed = True
            if not str(holding.get("reported_cusip") or "").strip():
                holding["reported_cusip"] = match["reported_cusip"]
                changed = True
            if (
                not str(holding.get("reported_figi") or "").strip()
                and match.get("reported_figi")
            ):
                holding["reported_figi"] = match["reported_figi"]
                changed = True
            if not str(holding.get("accession") or "").strip():
                holding["accession"] = match["accession"]
                changed = True
            if not str(holding.get("report_date") or "").strip():
                holding["report_date"] = match["report_date"]
                changed = True
            after_canonical = {
                key: copy.deepcopy(value)
                for key, value in holding.items()
                if key not in _BACKFILL_MUTABLE_FIELDS
            }
            if before_canonical != after_canonical:
                raise Sec13FBulkError(
                    "reported-identity backfill attempted to rewrite canonical fields"
                )
            stats["exact_matches"] += 1
            source = _reported_identity_source_from_match(match)
            source_key = (
                source["accession"],
                source["report_date"],
                source["url"],
                source["sha256"],
            )
            matched_sources[source_key] = source
            if changed:
                stats["holdings_changed"] += 1
        if all_exact:
            quarter["reported_identity_sources"] = [
                matched_sources[key] for key in sorted(matched_sources)
            ]
    return updated, stats


def _map_fund_evidence_jobs(
    worker: Callable,
    paths: list[Path],
    index_path: Path,
    *,
    workers: int | None,
    require_verified: bool = False,
) -> Iterable[Any]:
    """Read independent funds against one immutable index in stable file order."""
    if workers is None:
        configured = os.environ.get("SEC_PIPELINE_WORKERS")
        try:
            workers = int(configured) if configured is not None else (
                min(6, os.cpu_count() or 1) if len(paths) >= 32 else 1
            )
        except ValueError as exc:
            raise Sec13FBulkError(
                "SEC pipeline workers must be a positive integer"
            ) from exc
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise Sec13FBulkError("SEC pipeline workers must be a positive integer")
    workers = min(workers, 12, max(1, len(paths)))
    jobs = ((path, index_path, require_verified) for path in paths)
    if workers == 1:
        for job in jobs:
            yield worker(job)
    else:
        # Workers never write funds or share a SQLite connection. Ordered map
        # preserves counts, diagnostics, and the pre-apply manifest exactly.
        with ProcessPoolExecutor(max_workers=workers) as executor:
            yield from executor.map(worker, jobs, chunksize=4)


def _read_fund_for_evidence(path: Path) -> dict[str, Any]:
    try:
        fund = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Sec13FBulkError(f"cannot read fund JSON {path}: {exc}") from exc
    if not isinstance(fund, dict):
        raise Sec13FBulkError(f"fund JSON root must be an object: {path}")
    return fund


def _preflight_fund_file(
    job: tuple[Path, Path, bool],
) -> tuple[dict[str, int], Counter[str], list[dict[str, Any]], dict | None]:
    path, index_path, require_all_verified = job
    original = _read_fund_for_evidence(path)
    connection = _open_index(index_path, read_only=True)
    try:
        updated, stats = backfill_fund_document(original, connection=connection)
        issues = []
        if require_all_verified:
            if economic_positions_for_fund(
                original, fallback_cik=path.stem,
            ) != economic_positions_for_fund(updated, fallback_cik=path.stem):
                raise BulkIndexRefreshError(
                    "SEC Form 13F economic-position verification failed "
                    f"before apply: {path.name}"
                )
            _verification_stats, issues = _verify_fund_document_against_index(
                updated,
                connection=connection,
                path=path,
                require_source_provenance=True,
            )
        counts = Counter(str(issue.get("status") or "unaddressable") for issue in issues)
        entry = (
            {"name": path.name, "original_sha256": _sha256_file(path)}
            if updated != original else None
        )
        return stats, counts, issues[:5], entry
    finally:
        connection.close()


def backfill_fund_files(
    funds_dir: Path,
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    verify_index_checksum: bool = False,
    require_all_verified: bool = False,
    workers: int | None = None,
) -> FundBackfillResult:
    """Verify the corpus, then atomically apply one fund at a time.

    With ``require_all_verified``, every logical post-backfill document is
    checked against the same immutable SEC index before the first file is
    replaced. A killed apply may leave a verified prefix updated, but reruns are
    idempotent and the hosted workflow cannot publish until the whole command
    and the post-apply corpus verification succeed.
    """

    state_path = Path(state_path)
    state = load_13f_bulk_index(
        state_path,
        verify_index_checksum=verify_index_checksum,
    )
    index_path = _index_path_from_state(state, state_path)
    if index_path is None:
        raise Sec13FBulkError("SEC Form 13F evidence index has not been built")
    totals = {
        "files_scanned": 0,
        "files_changed": 0,
        "holdings_scanned": 0,
        "holdings_changed": 0,
        "exact_matches": 0,
        "unmatched": 0,
        "ambiguous": 0,
        "conflicts": 0,
    }
    verification_counts: Counter[str] = Counter()
    verification_sample: list[dict[str, Any]] = []
    funds_dir = Path(funds_dir)
    funds_dir.parent.mkdir(parents=True, exist_ok=True)
    # Pass one verifies the complete logical post-backfill corpus without
    # retaining decoded documents or writing any fund. The manifest contains
    # only one small entry per changed file, not a second copy of the corpus.
    manifest_entries: list[dict[str, Any]] = []
    for stats, counts, sample, entry in _map_fund_evidence_jobs(
        _preflight_fund_file,
        sorted(funds_dir.glob("*.json")),
        index_path,
        workers=workers,
        require_verified=require_all_verified,
    ):
        totals["files_scanned"] += 1
        for key, value in stats.items():
            totals[key] += value
        verification_counts.update(counts)
        verification_sample.extend(sample[: max(0, 5 - len(verification_sample))])
        if entry is not None:
            manifest_entries.append(entry)
            totals["files_changed"] += 1
    if verification_counts:
        sample = [
            f"{issue['file']}:{issue['reported_cusip']}:{issue['status']}"
            for issue in verification_sample
        ]
        raise BulkIndexRefreshError(
            "SEC Form 13F retained-identity verification failed before apply: "
            f"unmatched={verification_counts['unmatched']}, "
            f"ambiguous={verification_counts['ambiguous']}, "
            f"conflicts={verification_counts['conflict']}, "
            f"unaddressable={verification_counts['unaddressable']}, "
            f"sample={sample}"
        )

    with tempfile.TemporaryDirectory(
        prefix=".sec-13f-fund-manifest-",
        dir=funds_dir.parent,
    ) as temporary_name:
        staging_root = Path(temporary_name)
        manifest = {
            "schema_version": 1,
            "index_sha256": state["index"]["sha256"],
            "funds_dir": str(funds_dir.resolve()),
            "updates": manifest_entries,
        }
        manifest_path = staging_root / "manifest.json"
        _atomic_write_json(manifest_path, manifest)
        durable_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if durable_manifest != manifest:
            raise Sec13FBulkError("SEC 13F fund staging manifest did not round-trip")
        updates = durable_manifest["updates"]
        connection = _open_index(index_path, read_only=True)
        try:
            for entry in updates:
                name = entry.get("name") if isinstance(entry, dict) else None
                if (
                    not isinstance(name, str)
                    or not name
                    or PurePosixPath(name).name != name
                ):
                    raise Sec13FBulkError(
                        "SEC 13F fund staging manifest filename is unsafe"
                    )
                path = funds_dir / name
                if (
                    not path.is_file()
                    or _sha256_file(path) != entry.get("original_sha256")
                ):
                    raise Sec13FBulkError(
                        f"SEC 13F fund changed before promotion: {name}"
                    )
                try:
                    original = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    raise Sec13FBulkError(
                        f"cannot reread fund JSON {path}: {exc}"
                    ) from exc
                if not isinstance(original, dict):
                    raise Sec13FBulkError(
                        f"fund JSON root must be an object: {path}"
                    )
                updated, _stats = backfill_fund_document(
                    original,
                    connection=connection,
                )
                if require_all_verified and economic_positions_for_fund(
                    original, fallback_cik=path.stem,
                ) != economic_positions_for_fund(updated, fallback_cik=path.stem):
                    raise BulkIndexRefreshError(
                        "SEC Form 13F economic-position verification failed "
                        f"before apply: {path.name}"
                    )
                if updated == original:
                    raise Sec13FBulkError(
                        f"SEC 13F staged fund no longer needs promotion: {name}"
                    )
                _atomic_write_fund_json(path, updated)
        finally:
            connection.close()
    return FundBackfillResult(**totals)


def _verification_issue(
    *,
    status: str,
    path: Path | None,
    cik: str,
    report_date: str,
    quarter_accessions: Sequence[str],
    holding: Mapping[str, Any],
) -> dict[str, Any]:
    holding_accession = _normalize_text(holding.get("accession"))
    accessions = set(quarter_accessions)
    if _ACCESSION_RE.fullmatch(holding_accession):
        accessions.add(holding_accession)
    return {
        "status": status,
        "file": path.name if path is not None else "",
        "cik": cik,
        "report_date": report_date,
        "accessions": tuple(sorted(accessions)),
        "holding_accession": holding_accession,
        "reported_cusip": _holding_cusip_key(holding),
    }


def _verify_fund_document_against_index(
    fund: Mapping[str, Any],
    *,
    connection: sqlite3.Connection,
    path: Path | None = None,
    require_source_provenance: bool = False,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    stats = {
        "holdings_scanned": 0,
        "placeholder_holdings": 0,
        "exact_matches": 0,
        "unmatched": 0,
        "ambiguous": 0,
        "conflicts": 0,
        "unaddressable": 0,
    }
    issues: list[dict[str, Any]] = []
    quarters = fund.get("quarters")
    if not isinstance(quarters, list):
        raise Sec13FBulkError("fund JSON quarters must be a list")
    try:
        cik = _normalize_cik(fund.get("cik"))
    except DatasetParseError:
        cik = ""
    for quarter in quarters:
        if not isinstance(quarter, dict) or not isinstance(
            quarter.get("holdings"), list
        ):
            raise Sec13FBulkError("fund JSON quarter/holdings structure is invalid")
        holdings = quarter["holdings"]
        addressable_holdings: list[Mapping[str, Any]] = []
        for holding in holdings:
            if not isinstance(holding, dict):
                raise Sec13FBulkError("fund JSON holding must be an object")
            if _is_zero_value_confidential_placeholder(holding):
                stats["placeholder_holdings"] += 1
                continue
            addressable_holdings.append(holding)
            stats["holdings_scanned"] += 1
        if not addressable_holdings:
            continue
        quarter_accessions = _quarter_accessions(quarter)
        try:
            report_date = _normalize_date(quarter.get("report_date"))
        except DatasetParseError:
            report_date = ""
        if not cik or not report_date:
            for holding in addressable_holdings:
                stats["unaddressable"] += 1
                issues.append(_verification_issue(
                    status="unaddressable",
                    path=path,
                    cik=cik,
                    report_date=report_date,
                    quarter_accessions=quarter_accessions,
                    holding=holding,
                ))
            continue
        accessions = set(quarter_accessions)
        accessions.update(
            _normalize_text(holding.get("accession"))
            for holding in addressable_holdings
            if _ACCESSION_RE.fullmatch(
                _normalize_text(holding.get("accession"))
            )
        )
        candidates = _query_quarter_evidence(
            connection,
            cik=cik,
            report_date=report_date,
            accessions=sorted(accessions),
        )
        by_cusip: dict[str, list[dict[str, Any]]] = {}
        for record in candidates:
            if (
                record.get("cik") != cik
                or record.get("report_date") != report_date
            ):
                continue
            by_cusip.setdefault(str(record.get("cusip_key") or ""), []).append(
                record
            )
        exact_matches: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
        for holding in addressable_holdings:
            if not _holding_cusip_key(holding):
                status = "unaddressable"
            else:
                match, status = _match_holding(
                    holding,
                    quarter,
                    by_cusip.get(_holding_cusip_key(holding), []),
                )
            if status == "exact":
                stats["exact_matches"] += 1
                assert match is not None
                exact_matches.append((holding, match))
                continue
            stats[status if status != "conflict" else "conflicts"] += 1
            issues.append(_verification_issue(
                status=status,
                path=path,
                cik=cik,
                report_date=report_date,
                quarter_accessions=quarter_accessions,
                holding=holding,
            ))
        if require_source_provenance and len(exact_matches) == len(
            addressable_holdings
        ):
            provenance_ok = False
            try:
                actual_sources = _canonical_reported_identity_sources(
                    quarter.get("reported_identity_sources")
                )
            except Sec13FBulkError:
                actual_sources = []
            expected_by_key: dict[
                tuple[str, str, str, str], dict[str, str]
            ] = {}
            for _holding, match in exact_matches:
                source = _reported_identity_source_from_match(match)
                key = (
                    source["accession"],
                    source["report_date"],
                    source["url"],
                    source["sha256"],
                )
                expected_by_key[key] = source
            expected_sources = [
                expected_by_key[key] for key in sorted(expected_by_key)
            ]
            provenance_ok = actual_sources == expected_sources
            if not provenance_ok:
                stats["conflicts"] += 1
                issues.append(_verification_issue(
                    status="conflict",
                    path=path,
                    cik=cik,
                    report_date=report_date,
                    quarter_accessions=quarter_accessions,
                    holding=addressable_holdings[0],
                ))
    return stats, issues


def _verify_fund_file(
    job: tuple[Path, Path, bool],
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    path, index_path, require_source_provenance = job
    fund = _read_fund_for_evidence(path)
    connection = _open_index(index_path, read_only=True)
    try:
        return _verify_fund_document_against_index(
            fund,
            connection=connection,
            path=path,
            require_source_provenance=require_source_provenance,
        )
    finally:
        connection.close()


def verify_reported_identity_against_sec(
    funds_dir: Path,
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    verify_index_checksum: bool = False,
    require_source_provenance: bool = False,
    workers: int | None = None,
) -> ReportedIdentityVerificationResult:
    """Verify every retained non-placeholder holding against exact SEC rows."""

    state_path = Path(state_path)
    state = load_13f_bulk_index(
        state_path,
        verify_index_checksum=verify_index_checksum,
    )
    index_path = _index_path_from_state(state, state_path)
    if index_path is None:
        raise Sec13FBulkError("SEC Form 13F evidence index has not been built")
    totals = {
        "files_scanned": 0,
        "holdings_scanned": 0,
        "placeholder_holdings": 0,
        "exact_matches": 0,
        "unmatched": 0,
        "ambiguous": 0,
        "conflicts": 0,
        "unaddressable": 0,
    }
    issues: list[dict[str, Any]] = []
    for stats, fund_issues in _map_fund_evidence_jobs(
        _verify_fund_file,
        sorted(Path(funds_dir).glob("*.json")),
        index_path,
        workers=workers,
        require_verified=require_source_provenance,
    ):
        totals["files_scanned"] += 1
        for key, value in stats.items():
            totals[key] += value
        issues.extend(fund_issues)
    issues.sort(key=lambda issue: (
        issue["file"],
        issue["report_date"],
        issue["reported_cusip"],
        issue["holding_accession"],
        issue["status"],
    ))
    return ReportedIdentityVerificationResult(
        **totals,
        issues=tuple(issues),
    )


def prepare_unpublished_legacy_index_adoption(
    funds_dir: Path,
    *,
    published_sec_security_state: bool,
    state_path: Path = DEFAULT_STATE_PATH,
    receipt_path: Path | None = None,
) -> dict[str, Any]:
    """Prepare one verified pre-plan index for an unpublished cutover only.

    This migration bridge is intentionally stronger than normal receipt
    creation. It checks the complete immutable SQLite bytes, proves every
    retained holding and its source provenance against that index, upgrades a
    legacy period-scoped manifest to the stable exact accession scope, and
    binds the current source and archive-target sets. The refresh path still
    rechecks the full checksum and exact live plan before it can stamp the
    generation as reusable.
    """

    if published_sec_security_state:
        raise Sec13FBulkError(
            "legacy SEC 13F index adoption is forbidden for published state"
        )
    state_path = Path(state_path)
    funds_dir = Path(funds_dir)
    state = load_13f_bulk_index(state_path)
    if state.get("index") is None or not state.get("sources"):
        raise Sec13FBulkError(
            "legacy SEC 13F index adoption requires an existing complete index"
        )
    if state.get("clean_rebuild_plan_sha256") is not None:
        raise Sec13FBulkError(
            "legacy SEC 13F index adoption requires a pre-plan generation"
        )

    completeness = reported_identity_backfill_audit(funds_dir)
    if completeness["needed"]:
        raise Sec13FBulkError(
            "legacy SEC 13F index adoption requires a complete retained corpus"
        )
    verification = verify_reported_identity_against_sec(
        funds_dir,
        state_path=state_path,
        verify_index_checksum=False,
        require_source_provenance=True,
    )
    if (
        not verification.ok
        or verification.holdings_scanned != completeness["holdings_scanned"]
        or verification.exact_matches != completeness["holdings_scanned"]
    ):
        raise Sec13FBulkError(
            "legacy SEC 13F index adoption failed full retained-corpus verification"
        )

    targets = collect_backfill_targets_from_funds(funds_dir)
    archive_scope = collect_archive_fallback_targets_from_funds(
        funds_dir,
        incomplete_only=False,
    )
    if archive_scope["unaddressable"]:
        raise Sec13FBulkError(
            "legacy SEC 13F index adoption has unaddressable retained periods"
        )
    normalized_archive_targets = _normalize_archive_targets(
        archive_scope["targets"]
    )
    if not normalized_archive_targets or not _index_covers_archive_targets_exactly(
        state,
        state_path=state_path,
        archive_targets=normalized_archive_targets,
    ):
        raise Sec13FBulkError(
            "legacy SEC 13F index does not cover the exact archive-target identity"
        )

    stable_accessions = sorted({
        target["accession"] for target in normalized_archive_targets
    })
    normalized_periods = sorted({
        (_normalize_cik(period["cik"]), _normalize_date(period["report_date"]))
        for period in targets["periods"]
    })
    archive_periods = sorted({
        (target["cik"], target["report_date"])
        for target in normalized_archive_targets
    })
    if (
        stable_accessions != targets["accessions"]
        or archive_periods != normalized_periods
    ):
        raise Sec13FBulkError(
            "legacy SEC 13F corpus target scopes are not exact"
        )
    target_scope_payload = {
        "accessions": stable_accessions,
        "periods": [
            {"cik": cik, "report_date": report_date}
            for cik, report_date in normalized_periods
        ],
    }
    target_scope = {
        **target_scope_payload,
        "sha256": _sha256_bytes(_canonical_json_bytes(target_scope_payload)),
    }
    dataset_urls = sorted(
        state["sources"],
        key=_dataset_url_sort_key,
    )
    plan_sha256 = _clean_rebuild_plan_sha256(
        target_scope=target_scope,
        dataset_urls=dataset_urls,
        archive_targets=normalized_archive_targets,
    )

    output_path = (
        Path(receipt_path)
        if receipt_path is not None
        else state_path.parent / DEFAULT_COMPLETED_RECEIPT_PATH.name
    )
    candidate_state = copy.deepcopy(state)
    candidate_state["target_scope"] = target_scope
    if load_13f_bulk_index(state_path) != state:
        raise Sec13FBulkError(
            "SEC 13F state changed while preparing legacy index adoption"
        )
    _validate_state(
        candidate_state,
        state_path=state_path,
        verify_index_checksum=True,
    )
    receipt = _legacy_index_adoption_receipt_for_state(
        candidate_state,
        clean_rebuild_plan_sha256=plan_sha256,
        dataset_urls=dataset_urls,
        archive_targets=normalized_archive_targets,
    )
    if candidate_state != state:
        _atomic_write_json(state_path, candidate_state)
    _atomic_write_json(output_path, receipt)
    return receipt


def rebuild_reported_identity_from_sec(
    funds_dir: Path,
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    index_dir: Path = DEFAULT_INDEX_DIR,
    checkpoint_path: Path | None = None,
    dataset_urls: Iterable[str] | None = None,
    discovery_html: str | bytes | None = None,
    fetcher: Fetcher | None = None,
    user_agent: str | None = None,
    refreshed_at: datetime | None = None,
    completed_rebuild_receipt: Mapping[str, Any] | None = None,
    completed_receipt_path: Path | None = None,
    allow_unpublished_legacy_index_adoption: bool = False,
) -> RebuildReportedIdentityResult:
    """One-call integration point for ``--rebuild-security-master``.

    All official quarterly ZIPs are rebuilt into a new generation first.  Fund
    files are touched only after that complete candidate index is accepted.
    """

    preflight = reported_identity_backfill_audit(funds_dir)
    if preflight["malformed_files"]:
        raise BulkIndexRefreshError(
            "SEC Form 13F retained-identity preflight failed: "
            f"malformed_files={preflight['malformed_files']}"
        )
    targets = collect_backfill_targets_from_funds(funds_dir)
    if targets["holdings_targeted"] == 0:
        empty_backfill = FundBackfillResult(
            files_scanned=preflight["files_scanned"],
            files_changed=0,
            holdings_scanned=0,
            holdings_changed=0,
            exact_matches=0,
            unmatched=0,
            ambiguous=0,
            conflicts=0,
        )
        prior_state = load_13f_bulk_index(state_path)
        return RebuildReportedIdentityResult(
            refresh=BulkIndexRefreshResult(
                state=prior_state,
                changed=False,
                refreshed_urls=(),
                reused_urls=tuple(sorted(prior_state.get("sources", {}))),
                errors=(),
            ),
            backfill=empty_backfill,
            archive_fallback=None,
        )
    archive_targets = collect_archive_fallback_targets_from_funds(
        funds_dir,
        incomplete_only=False,
    )
    active_fetcher = fetcher or make_sec_fetcher(user_agent)
    rebuild_checkpoint_path = (
        Path(checkpoint_path)
        if checkpoint_path is not None
        else Path(state_path).parent / DEFAULT_REBUILD_CHECKPOINT_PATH.name
    )
    accession_discovery_checkpoint_path = (
        _accession_discovery_checkpoint_path(rebuild_checkpoint_path)
    )
    prior_state = load_13f_bulk_index(state_path)
    retained_archive_targets = _archive_targets_from_existing_index(
        archive_targets["unaddressable"],
        state=prior_state,
        state_path=Path(state_path),
    )
    missing_periods = _periods_without_index_evidence(
        archive_targets["unaddressable"],
        state=prior_state,
        state_path=Path(state_path),
    )
    discovered_targets = discover_archive_fallback_targets_for_periods(
        missing_periods,
        fetcher=fetcher,
        user_agent=user_agent,
        checkpoint_path=accession_discovery_checkpoint_path,
    )
    if discovered_targets["missing"]:
        sample = [
            f"{item['cik']}:{item['report_date']}"
            for item in discovered_targets["missing"][:5]
        ]
        raise BulkIndexRefreshError(
            "SEC submissions did not expose Form 13F accessions for "
            f"{len(discovered_targets['missing'])} retained period(s): {sample}"
        )
    combined_archive_targets = _normalize_archive_targets([
        *archive_targets["targets"],
        *retained_archive_targets,
        *discovered_targets["targets"],
    ])
    refresh = refresh_13f_bulk_index(
        state_path=state_path,
        index_dir=index_dir,
        dataset_urls=dataset_urls,
        discovery_html=discovery_html,
        fetcher=active_fetcher,
        # Archive discovery makes this scope stable across the cutover: an
        # accessionless legacy holding and that same holding after exact SEC
        # identity backfill now produce the identical clean-rebuild plan.
        target_accessions=sorted({
            target["accession"] for target in combined_archive_targets
        }),
        target_periods=[
            (period["cik"], period["report_date"])
            for period in targets["periods"]
        ],
        archive_fallback_targets=combined_archive_targets,
        clean_rebuild_checkpoint_path=rebuild_checkpoint_path,
        completed_rebuild_receipt=completed_rebuild_receipt,
        allow_unpublished_legacy_index_adoption=(
            allow_unpublished_legacy_index_adoption
        ),
        full_rebuild=True,
        recheck_recent_archives=0,
        refreshed_at=refreshed_at,
    )
    if refresh.errors or refresh.state.get("index") is None:
        raise BulkIndexRefreshError(
            "SEC Form 13F retained-identity verification failed because the "
            "bulk rebuild did not produce a complete index: "
            + "; ".join(refresh.errors or ("missing index",))
        )
    uncovered_periods = _periods_without_index_evidence(
        targets["periods"],
        state=refresh.state,
        state_path=Path(state_path),
    )
    if uncovered_periods:
        sample = [
            f"{item['cik']}:{item['report_date']}"
            for item in uncovered_periods[:5]
        ]
        raise BulkIndexRefreshError(
            "SEC Form 13F clean index omitted "
            f"{len(uncovered_periods)} nonempty retained period(s): {sample}"
        )
    fallback_result: ArchiveFallbackRefreshResult | None = None
    enrichment_targets = collect_archive_enrichment_targets_from_funds(
        funds_dir,
        state_path=state_path,
    )
    if enrichment_targets:
        fallback_result = refresh_sec_archive_fallbacks(
            enrichment_targets,
            state_path=state_path,
            index_dir=index_dir,
            fetcher=active_fetcher,
            refreshed_at=refreshed_at,
        )
        if fallback_result.unresolved:
            sample = [
                f"{item['cik']}:{item['accession']}:{item['report_date']}"
                for item in fallback_result.unresolved[:5]
            ]
            raise BulkIndexRefreshError(
                "SEC archive filing-chain enrichment failed for "
                f"{len(fallback_result.unresolved)} target(s): {sample}"
            )
        refresh = BulkIndexRefreshResult(
            state=fallback_result.state,
            changed=refresh.changed or fallback_result.changed,
            refreshed_urls=refresh.refreshed_urls,
            reused_urls=refresh.reused_urls,
            errors=refresh.errors,
        )
    else:
        resolved_fallback_accessions = tuple(sorted({
            str(source.get("accession"))
            for source in refresh.state.get("archive_sources", {}).values()
            if isinstance(source, dict) and source.get("accession")
        }))
        if resolved_fallback_accessions:
            fallback_result = ArchiveFallbackRefreshResult(
                state=refresh.state,
                changed=True,
                resolved_accessions=resolved_fallback_accessions,
                reused_accessions=(),
                unresolved=(),
            )
    # Persist the index-only receipt before touching fund files. A later
    # interruption can reuse the exact plan/state/SQLite generation, while the
    # fund backfill and post-apply verification below must still run in full.
    accepted_receipt = build_completed_clean_rebuild_receipt(
        state_path,
        verify_index_checksum=False,
    )
    receipt_path = (
        Path(completed_receipt_path)
        if completed_receipt_path is not None
        else Path(state_path).parent / DEFAULT_COMPLETED_RECEIPT_PATH.name
    )
    _atomic_write_json(receipt_path, accepted_receipt)
    if accession_discovery_checkpoint_path.exists():
        accession_discovery_checkpoint_path.unlink()
        _fsync_directory(accession_discovery_checkpoint_path.parent)

    # The retained corpus may contain legacy buckets that combined multiple
    # exact as-filed identities.  Those buckets cannot verify until
    # ``backfill_fund_document`` deterministically reconstructs them from the
    # SEC rows.  The strict backfill performs a complete logical post-rebuild
    # corpus verification before replacing the first fund file, so a separate
    # pre-reconstruction verifier here would incorrectly make the repair path
    # unreachable without adding any safety.
    backfill = backfill_fund_files(
        funds_dir,
        state_path=state_path,
        verify_index_checksum=True,
        require_all_verified=True,
    )
    completeness = reported_identity_backfill_audit(funds_dir)
    verification = verify_reported_identity_against_sec(
        funds_dir,
        state_path=state_path,
        verify_index_checksum=True,
        require_source_provenance=True,
    )
    if completeness["needed"] or not verification.ok:
        raise BulkIndexRefreshError(
            "SEC Form 13F post-apply verification failed: "
            f"incomplete={completeness['incomplete_holdings']}, "
            f"unmatched={verification.unmatched}, "
            f"ambiguous={verification.ambiguous}, "
            f"conflicts={verification.conflicts}, "
            f"unaddressable={verification.unaddressable}"
        )
    return RebuildReportedIdentityResult(
        refresh=refresh,
        backfill=backfill,
        archive_fallback=fallback_result,
        # The receipt authorizes index reuse only. Returning it after the
        # strict verifier does not widen its scope to fund-file completion.
        completed_rebuild_receipt=accepted_receipt,
    )


# Stable provider-neutral aliases for callers that prefer shorter names.
refresh_bulk_index = refresh_13f_bulk_index
rebuild_reported_identity = rebuild_reported_identity_from_sec
apply_reported_identity_backfill = backfill_fund_files
