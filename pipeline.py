#!/usr/bin/env python3
"""
Super Investor Seeker - Phase 1 data pipeline.

Downloads SEC 13F-HR institutional holdings filings, parses them, and writes
JSON files into ./data for the static website to consume.

Usage:
    python3 pipeline.py --all                       # search 4 filing quarters
    python3 pipeline.py --all --quarters 1          # search 1 filing quarter
    python3 pipeline.py --cik 1067983 --quarters 2  # search 2 for one filer
    python3 pipeline.py --regenerate-only           # rebuild from existing fund
                                                    # files; no SEC filing fetches
    python3 pipeline.py --regenerate-only --rebuild-security-master
                                                    # rebuild from existing fund
                                                    # files, fully refresh the
                                                    # private SEC security
                                                    # master, and rebuild the
                                                    # snapshot CUSIP
                                                    # registry + derived outputs

Reads SEC_USER_AGENT from env. SEC requires a real contact email in the UA.
"""

from __future__ import annotations

import argparse
import fcntl
import functools
import hashlib
import json
import logging
import math
import os
import queue
import re
import shutil
import signal
import statistics
import sys
import tempfile
import threading
import time
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse, urlsplit, urlunsplit

import requests
from lxml import etree
from lxml import html as lxml_html

from composition_integrity import (
    calculate_composition_hash as _calculate_composition_hash,
    canonical_json_hash as _canonical_json_hash,
)
from data_contract import DATA_CONTRACT_VERSION
from quarter_health import (
    add_quarter_peer_observations,
    compile_peer_price_index,
    peer_price_quarter_health_issue,
    same_date_peer_price_references,
    structural_quarter_health_issues,
)
from security_identity import (
    FUND_IDENTITY_TICKER_SOURCES as _FUND_IDENTITY_TICKER_SOURCES,
    SEC_TICKER_RE,
    VALID_INSTRUMENT_TYPES,
    compose_security_label,
    holding_instrument_type,
    is_mutual_fund_ticker,
    is_synthetic_identifier,
    normalize_instrument_type,
    normalize_note_security_label,
    normalize_security_identifier,
    normalize_security_kind,
    normalize_security_label,
    sec_ticker_titles,
    stock_filename,
    stock_lookup_id,
    published_holding_instrument_type,
    registry_entry_has_equity_fund_identity as _registry_entry_has_equity_fund_identity,
    synthetic_identifier_ticker_hint,
)
from sec_13f_bulk_backfill import (
    LEGACY_INDEX_ADOPTION_RECEIPT_SCOPE,
    build_completed_clean_rebuild_receipt,
    ensure_clean_rebuild_disk_space,
    load_completed_clean_rebuild_receipt,
    normalize_sec_identity_source_url,
    prepare_unpublished_legacy_index_adoption,
    rebuild_reported_identity_from_sec,
    reported_identity_backfill_audit,
)
from sec_edgar_evidence import (
    CACHE_SCHEMA_VERSION as SEC_EDGAR_CACHE_SCHEMA_VERSION,
    EvidenceSchemaError,
    discover_sec_edgar_sources,
    make_sec_discovery_fetcher,
    merge_sec_edgar_evidence_caches,
    refresh_sec_edgar_evidence,
)
from sec_security_master import (
    DEFAULT_MASTER_PATH as SEC_SECURITY_MASTER_PATH,
    DEFAULT_SOURCE_STATE_PATH as SEC_SOURCE_STATE_PATH,
    MASTER_SCHEMA_VERSION as SEC_SECURITY_MASTER_SCHEMA_VERSION,
    PRODUCTION_MIN_ACTIVE_OFFICIAL_CUSIP_COUNT,
    PRODUCTION_MIN_CURRENT_SYMBOL_POPULATION_BY_KIND,
    PRODUCTION_MIN_CURRENT_SYMBOL_TITLE_RATIO,
    RefreshResult as SecSecurityMasterRefreshResult,
    SecurityMasterAcceptanceError,
    SourceParseError,
    SourceSchemaChangeError,
    SourceSchemaError,
    SOURCE_STATE_SCHEMA_VERSION as SEC_SOURCE_STATE_SCHEMA_VERSION,
    audit_security_master as _audit_security_master,
    cusip_quarantine_reason,
    load_security_master,
    load_security_master_pair,
    load_source_state,  # noqa: F401 - retained for pipeline API compatibility
    make_sec_fetcher,
    rebuild_security_master as rebuild_sec_security_master,
    recover_security_master_pair,
    refresh_security_master,
    resolve_security,
    save_security_master,  # noqa: F401 - retained for test/caller compatibility
    save_security_master_pair,
    save_source_state,  # noqa: F401 - retained for test/caller compatibility
    sec_fund_series_url,
    security_key,
    source_state_sha256,
)
from security_master_migration import (
    build_cutover_difference_report,
    capture_cutover_projection,
    write_cutover_difference_report,
)
from value_units import (
    VALUE_UNIT_POLICY_VERSION,
    AmbiguousValueUnits,
    is_unit_evidence_holding,
    normalize_value_units,
)


# GitHub Actions sends SIGTERM to the job's processes when a step's
# timeout-minutes is reached. Python doesn't raise on SIGTERM by default — it
# just dies — so we'd lose the chance to save pipeline_state.json and
# regenerate stock files. Converting SIGTERM into KeyboardInterrupt lets the
# existing `except KeyboardInterrupt` blocks in run_all() / run_for_cik() run
# their cleanup path.
def _sigterm_to_keyboardinterrupt(signum, frame):
    raise KeyboardInterrupt()

signal.signal(signal.SIGTERM, _sigterm_to_keyboardinterrupt)


# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
CACHE_DIR = ROOT / ".cache"
FUNDS_DIR = DATA_DIR / "funds"
STOCKS_DIR = DATA_DIR / "stocks"
INDEX_PATH = DATA_DIR / "index.json"
FUNDS_INDEX_PATH = DATA_DIR / "funds-index.json"
STATE_PATH = DATA_DIR / "pipeline_state.json"
LEGACY_STATE_PATH = CACHE_DIR / "pipeline_state.json"
SEC_SECURITY_MASTER_MIGRATION_REPORT_PATH = (
    CACHE_DIR / "sec_security_master_migration_report.json"
)
TICKER_HEALTH_PATH = DATA_DIR / "ticker_health.json"
SECURITY_LABELS_PATH = DATA_DIR / "security_labels.json"
# The private cache carries operational evidence, while the snapshot's data
# copy is a durability floor so an older restored cache cannot erase reviewed
# labels, kinds, or fund names. The compact browser-facing label map feeds
# display without duplicating metadata in every holding.
CUSIP_REGISTRY_PATH = CACHE_DIR / "cusip_registry.json"
LEGACY_CUSIP_REGISTRY_PATH = DATA_DIR / "cusip_registry.json"

DEFAULT_USER_AGENT = "SuperInvestorSeeker contact@example.com"
USER_AGENT = os.environ.get("SEC_USER_AGENT", DEFAULT_USER_AGENT)

# Rate limiting. SEC's declared limit is 10 req/sec for a given contact;
# 8 req/sec leaves a 20% safety margin. No inter-filer pause — the per-request
# throttle already provides even spacing and SEC cares about sustained rate.
MIN_REQUEST_INTERVAL = 1.0 / 8.0
MAX_RETRIES = 6
RETRY_BASE = 2.0
RETRY_MAX = 60.0
HTTP_TIMEOUT = 30
MAX_SEC_REDIRECTS = 5
SEC_REDIRECT_STATUS_CODES = frozenset({301, 302, 303, 307, 308})
SEC_HTTP_HOSTS = frozenset({"sec.gov", "www.sec.gov", "data.sec.gov"})

# Concurrent workers. The SEC rate limiter is shared + thread-safe, so all
# workers collectively stay under MIN_REQUEST_INTERVAL. Parallelism exists
# purely to absorb network round-trip latency (~200-300ms per SEC request)
# and to keep forward progress when some workers are blocked in downstream
# SEC evidence requests.
WORKER_COUNT = int(os.environ.get("PIPELINE_WORKERS", "8"))
AMENDMENT_REDUCER_VERSION = 2
COMPOSITION_HASH_VERSION = 3
AMENDMENT_MIGRATION_FILING_QUARTERS = 8
AMENDMENT_MIGRATION_RETRY_INTERVAL_DAYS = 7
NEW_HOLDINGS_IDENTITY_VERSION = 1
NEW_HOLDINGS_REPLACEMENT_MIN_MATCHED_ROWS = 5
NEW_HOLDINGS_REPLACEMENT_COVERAGE_NUMERATOR = 9
NEW_HOLDINGS_REPLACEMENT_COVERAGE_DENOMINATOR = 10
SECURITY_IDENTITY_VERSION = 1
SECURITY_IDENTITY_MIGRATION_RETRY_INTERVAL_DAYS = 7
SECURITY_IDENTITY_MIGRATION_MAX_FAILURES = 25
QUARANTINE_RETRY_INTERVAL_DAYS = 7
FETCH_FAILURE_RETRY_INTERVAL_DAYS = 1
QUARTER_HEALTH_RETRY_INTERVAL_DAYS = 7
VALUE_UNIT_MIGRATION_VERSION = VALUE_UNIT_POLICY_VERSION

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")


_PIPELINE_MAINTENANCE_THREAD_LOCK = threading.RLock()
_PIPELINE_MAINTENANCE_LOCAL = threading.local()
_PIPELINE_MAINTENANCE_RELEASER_THREAD = threading.Thread


class _PipelineMaintenanceToken:
    """Lease the process lock until every inherited worker has exited."""

    def __init__(self, lock_file) -> None:
        self.lock_file = lock_file
        self.condition = threading.Condition()
        self.worker_count = 0
        self.closing = False

    def try_enter_worker(self) -> bool:
        with self.condition:
            if self.closing:
                return False
            self.worker_count += 1
            return True

    def leave_worker(self) -> None:
        with self.condition:
            self.worker_count -= 1
            self.condition.notify_all()

    def begin_close(self) -> bool:
        with self.condition:
            self.closing = True
            return self.worker_count > 0

    def wait_for_workers(self) -> None:
        with self.condition:
            while self.worker_count:
                self.condition.wait()


_PIPELINE_MAINTENANCE_PROCESS_TOKEN: _PipelineMaintenanceToken | None = None


def _release_pipeline_maintenance_token(
    token: _PipelineMaintenanceToken,
) -> None:
    global _PIPELINE_MAINTENANCE_PROCESS_TOKEN
    try:
        fcntl.flock(token.lock_file.fileno(), fcntl.LOCK_UN)
    except OSError as exc:
        log.warning("could not explicitly unlock pipeline maintenance lock: %s", exc)
    finally:
        try:
            token.lock_file.close()
        finally:
            if _PIPELINE_MAINTENANCE_PROCESS_TOKEN is token:
                _PIPELINE_MAINTENANCE_PROCESS_TOKEN = None


def _release_pipeline_maintenance_token_when_idle(
    token: _PipelineMaintenanceToken,
) -> None:
    token.wait_for_workers()
    _release_pipeline_maintenance_token(token)


def _inherit_pipeline_maintenance(func):
    """Let a child worker reuse the process lock held by its parent workflow."""

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        token = _PIPELINE_MAINTENANCE_PROCESS_TOKEN
        if token is None or not token.try_enter_worker():
            return func(*args, **kwargs)
        inherited = getattr(
            _PIPELINE_MAINTENANCE_LOCAL,
            "inherited_token",
            None,
        )
        _PIPELINE_MAINTENANCE_LOCAL.inherited_token = token
        try:
            return func(*args, **kwargs)
        finally:
            if inherited is not None:
                _PIPELINE_MAINTENANCE_LOCAL.inherited_token = inherited
            else:
                del _PIPELINE_MAINTENANCE_LOCAL.inherited_token
            token.leave_worker()

    return wrapped


def _serialize_pipeline_maintenance(func):
    """Serialize nested maintenance calls across threads and processes."""

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        global _PIPELINE_MAINTENANCE_PROCESS_TOKEN
        inherited = getattr(
            _PIPELINE_MAINTENANCE_LOCAL,
            "inherited_token",
            None,
        )
        if (
            inherited is not None
            and inherited is _PIPELINE_MAINTENANCE_PROCESS_TOKEN
        ):
            return func(*args, **kwargs)
        with _PIPELINE_MAINTENANCE_THREAD_LOCK:
            depth = getattr(_PIPELINE_MAINTENANCE_LOCAL, "depth", 0)
            if depth:
                _PIPELINE_MAINTENANCE_LOCAL.depth = depth + 1
                try:
                    return func(*args, **kwargs)
                finally:
                    _PIPELINE_MAINTENANCE_LOCAL.depth = depth

            lock_path = DATA_DIR.parent / ".pipeline-maintenance.lock"
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            lock_file = open(  # noqa: SIM115 - worker lease can outlive caller
                lock_path,
                "a+b",
            )
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            except BaseException:
                lock_file.close()
                raise
            token = _PipelineMaintenanceToken(lock_file)
            _PIPELINE_MAINTENANCE_LOCAL.depth = 1
            _PIPELINE_MAINTENANCE_PROCESS_TOKEN = token
            try:
                return func(*args, **kwargs)
            finally:
                workers_active = token.begin_close()
                del _PIPELINE_MAINTENANCE_LOCAL.depth
                if workers_active:
                    releaser = _PIPELINE_MAINTENANCE_RELEASER_THREAD(
                        target=_release_pipeline_maintenance_token_when_idle,
                        args=(token,),
                        name="pipeline-maintenance-lock-releaser",
                        daemon=True,
                    )
                    try:
                        releaser.start()
                    except BaseException as exc:  # noqa: BLE001 - preserve lease
                        log.error(
                            "could not start maintenance-lock releaser; "
                            "waiting synchronously: %s",
                            exc,
                        )
                        token.wait_for_workers()
                        _release_pipeline_maintenance_token(token)
                else:
                    _release_pipeline_maintenance_token(token)

    return wrapped


# ----------------------------------------------------------------------------
# Rate-limited HTTP session with retry/backoff
# ----------------------------------------------------------------------------

class FilingDiscoveryError(RuntimeError):
    """The SEC filing chain could not be discovered authoritatively."""

class FilingParseError(RuntimeError):
    """One immutable SEC filing component could not be parsed or reconciled."""

class FilingIdentityError(FilingParseError):
    """Primary-document filer identity is absent or conflicts with its CIK."""

class FilingFetchError(FilingParseError):
    """One SEC filing resource remained unavailable after HTTP retries."""

class FilingChainError(RuntimeError):
    """A quarter's filing chain is ambiguous or incomplete and must not publish."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason

class FundDataError(RuntimeError):
    """Existing materialized fund data is unsafe to read or replace."""


class NonSECRequestURL(ValueError):
    """A URL could send the contact-bearing SEC user agent off policy."""


def _normalize_sec_request_url(value: object) -> str:
    """Return a canonical HTTPS SEC URL or fail before network access."""

    raw_url = str(value or "").strip()
    parsed = urlsplit(raw_url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise NonSECRequestURL("invalid SEC request URL port") from exc
    host = (parsed.hostname or "").casefold()
    if (
        parsed.scheme.casefold() != "https"
        or host not in SEC_HTTP_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.fragment
        or "\\" in parsed.path
        or any(part in {".", ".."} for part in parsed.path.split("/"))
    ):
        raise NonSECRequestURL("only canonical HTTPS SEC request URLs are allowed")
    return urlunsplit(("https", host, parsed.path or "/", parsed.query, ""))


class RateLimitedSession:
    """Thread-safe global HTTP session. Throttles the rate at which requests
    are *issued* (not in-flight) — multiple workers can have simultaneous
    requests on the wire, but collectively they respect MIN_REQUEST_INTERVAL
    between issues. Retries 403/429/503 with exponential backoff.

    The lock is held only around the "claim next slot" computation + any rate
    sleep, not around the actual HTTP call, so workers overlap cleanly on
    round-trip latency."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "*/*",
        })
        self._last_request = 0.0
        self._rate_lock = threading.Lock()

    def _claim_slot(self) -> None:
        """Block until this caller may issue its next request. Thread-safe —
        serializes across workers so we don't burst past MIN_REQUEST_INTERVAL."""
        with self._rate_lock:
            now = time.monotonic()
            wait = MIN_REQUEST_INTERVAL - (now - self._last_request)
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

    def get(self, url: str, **kwargs) -> requests.Response:
        canonical_url = _normalize_sec_request_url(url)
        request_kwargs = dict(kwargs)
        # This invariant cannot be overridden by a caller: redirect targets
        # must be validated before the contact-bearing user agent is sent.
        request_kwargs["allow_redirects"] = False
        delay = RETRY_BASE
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            request_url = canonical_url
            try:
                for redirect_count in range(MAX_SEC_REDIRECTS + 1):
                    self._claim_slot()
                    resp = self.session.get(
                        request_url,
                        timeout=HTTP_TIMEOUT,
                        **request_kwargs,
                    )
                    response_url = _normalize_sec_request_url(
                        str(resp.url or request_url)
                    )
                    if response_url != request_url:
                        raise NonSECRequestURL(
                            "SEC response URL changed without an approved redirect"
                        )
                    if resp.status_code not in SEC_REDIRECT_STATUS_CODES:
                        if 300 <= resp.status_code < 400:
                            raise NonSECRequestURL(
                                "unsupported SEC redirect response"
                            )
                        break
                    if redirect_count >= MAX_SEC_REDIRECTS:
                        raise NonSECRequestURL(
                            "SEC response exceeded the redirect limit"
                        )
                    location = str(
                        resp.headers.get("Location") or ""
                    ).strip()
                    if not location:
                        raise NonSECRequestURL(
                            "SEC redirect response has no Location header"
                        )
                    # Resolve and validate before Requests receives the target.
                    # This prevents its session-level private user agent from
                    # ever being sent to a non-SEC or non-HTTPS destination.
                    request_url = _normalize_sec_request_url(
                        urljoin(response_url, location)
                    )
                if resp.status_code in (403, 429, 503):
                    log.warning(
                        f"  HTTP {resp.status_code} on {request_url} "
                        f"(attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(min(delay, RETRY_MAX))
                        delay *= 2
                        continue
                resp.raise_for_status()
                resp.url = response_url
                return resp
            except requests.RequestException as e:
                last_exc = e
                log.warning(
                    f"  request error on {canonical_url}: {e} "
                    f"(attempt {attempt + 1}/{MAX_RETRIES})"
                )
                if attempt < MAX_RETRIES - 1:
                    time.sleep(min(delay, RETRY_MAX))
                    delay *= 2
                    continue
        raise RuntimeError(
            f"GET failed after {MAX_RETRIES} retries: {canonical_url}"
        ) from last_exc


HTTP = RateLimitedSession()


# ----------------------------------------------------------------------------
# Quarter math
# ----------------------------------------------------------------------------

def get_recent_filing_quarters(n: int) -> list[tuple[int, int]]:
    """Return the N most-recent (year, quarter) pairs in which 13F filings
    could have been submitted, current quarter first."""
    today = date.today()
    cur_q = (today.month - 1) // 3 + 1
    out: list[tuple[int, int]] = []
    y, q = today.year, cur_q
    for _ in range(n):
        out.append((y, q))
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return out


# ----------------------------------------------------------------------------
# Discovery: parse SEC quarterly company.idx
# ----------------------------------------------------------------------------

ACCESSION_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")


def normalize_report_date(text: str | None) -> str | None:
    """SEC periodOfReport values come back in various formats; return YYYY-MM-DD."""
    if not text:
        return None
    text = text.strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
    if m:
        return text
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", text)
    if m:
        mm, dd, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", text)
    if m:
        mm, dd, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"
    return text


_REPORT_QUARTER_BY_MONTH_DAY = {
    (3, 31): 1,
    (6, 30): 2,
    (9, 30): 3,
    (12, 31): 4,
}


def report_quarter_code(report_date: object) -> int | None:
    """Return compact YYYYQ for a canonical calendar-quarter report date.

    Persisted fund quarters are expected to use normalized ``YYYY-MM-DD``
    dates.  Keep this conversion strict so malformed or non-quarter dates do
    not silently make a stale holding look current in the generated UI.
    """
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


def fund_report_quarter_codes(quarters: object) -> list[int]:
    """Return the four newest distinct valid report quarters, newest first."""
    if not isinstance(quarters, list):
        return []
    codes = {
        code
        for quarter in quarters
        if isinstance(quarter, dict)
        and (code := report_quarter_code(quarter.get("report_date"))) is not None
    }
    return sorted(codes, reverse=True)[:4]


def _modal_latest_reporting_quarter(funds: list[dict]) -> int | None:
    """Match the frontend's current-reporting-quarter baseline.

    A withheld filer cannot define the sitewide baseline.  Ties during filing
    season choose the newer quarter so managers already reporting the next
    quarter remain current.
    """
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


def _current_fund_quarters(
    funds: list[dict],
    baseline: int | None,
) -> dict[int, int]:
    """Return eligible filer CIK -> latest quarter for current-holder counts."""
    if baseline is None:
        return {}
    current: dict[int, int] = {}
    for fund in funds:
        if not isinstance(fund, dict) or fund.get("status") == "WITHHELD":
            continue
        calendar = fund.get("q")
        if not isinstance(calendar, list) or not calendar:
            continue
        latest = calendar[0]
        if type(latest) is not int or latest < baseline:
            continue
        try:
            cik = int(fund.get("cik"))
        except (TypeError, ValueError):
            continue
        if cik > 0:
            current[cik] = latest
    return current


def download_company_idx(
    year: int,
    quarter: int,
    *,
    strict: bool = False,
) -> list[dict]:
    """Download SEC's full-index company.idx for one filing quarter and
    return a list of 13F-HR filings."""
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.idx"
    log.info(f"Downloading company.idx for {year} QTR{quarter}")
    try:
        resp = HTTP.get(url)
    except Exception as e:
        if strict:
            raise FilingDiscoveryError(
                f"company.idx discovery failed for {year} QTR{quarter}"
            ) from e
        log.warning(f"  failed: {e}")
        return []

    text = resp.text
    lines = text.splitlines()

    # Find header line + dashed separator. company.idx is fixed-width.
    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Company Name") and "Form Type" in line and "CIK" in line:
            header_idx = i
            break
    if header_idx is None or header_idx + 2 >= len(lines):
        if strict:
            raise FilingDiscoveryError(
                f"unrecognized company.idx format for {year} QTR{quarter}"
            )
        log.warning("  unrecognized company.idx format")
        return []

    header = lines[header_idx]
    # Pull column start positions from the header text. Header label for the
    # last column is "File Name" in newer idx files and "Filename" in older.
    try:
        col_company = header.index("Company Name")
        col_form = header.index("Form Type")
        col_cik = header.index("CIK")
        col_date = header.index("Date Filed")
        if "File Name" in header:
            col_filename = header.index("File Name")
        else:
            col_filename = header.index("Filename")
    except ValueError as exc:
        if strict:
            raise FilingDiscoveryError(
                f"missing company.idx columns for {year} QTR{quarter}"
            ) from exc
        log.warning("  could not locate columns")
        return []

    filings: list[dict] = []
    for line in lines[header_idx + 2:]:
        if not line.strip():
            continue
        try:
            company = line[col_company:col_form].strip()
            form = line[col_form:col_cik].strip()
            cik_s = line[col_cik:col_date].strip()
            date_filed = line[col_date:col_filename].strip()
            filename = line[col_filename:].strip()
        except IndexError:
            continue

        # Accept both complete reports and amendments. A discovered accession
        # is only a replay trigger; quarter composition happens later from the
        # authoritative filing chain. Skip 13F-NT delegation notices.
        if form not in ("13F-HR", "13F-HR/A"):
            continue

        try:
            cik = int(cik_s)
        except ValueError:
            continue

        m = ACCESSION_RE.search(filename)
        if not m:
            continue

        filings.append({
            "cik": cik,
            "name": company,
            "form_type": form,
            "date_filed": date_filed,
            "filename": filename,
            "accession": m.group(1),
        })

    log.info(f"  found {len(filings)} 13F-HR filings")
    if strict and not filings:
        raise FilingDiscoveryError(
            f"company.idx contained no 13F-HR rows for {year} QTR{quarter}"
        )
    return filings


# ----------------------------------------------------------------------------
# Single-CIK discovery via the submissions endpoint
# ----------------------------------------------------------------------------

def _normalize_accepted_at(value: str | None) -> str | None:
    """Normalize SEC acceptance timestamps so lexical order is chronological."""
    text = str(value or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{14}", text):
        parsed = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
    normalized = text.replace(" ", "T")
    if normalized.endswith("+00:00"):
        normalized = normalized[:-6] + "Z"
    return normalized

def _submission_rows(payload: dict, cik: int, name: str, source: str) -> list[dict]:
    """Convert one SEC submissions parallel-array payload into filing rows."""
    if not isinstance(payload, dict):
        raise FilingDiscoveryError(f"{source} submissions payload is not an object")

    required = {
        "form": payload.get("form", []) or [],
        "accessionNumber": payload.get("accessionNumber", []) or [],
        "filingDate": payload.get("filingDate", []) or [],
        "reportDate": payload.get("reportDate", []) or [],
    }
    lengths = {key: len(value) for key, value in required.items() if isinstance(value, list)}
    if len(lengths) != len(required) or len(set(lengths.values())) > 1:
        raise FilingDiscoveryError(
            f"{source} submissions arrays disagree in length: {lengths}"
        )
    count = next(iter(lengths.values()), 0)
    accepted = payload.get("acceptanceDateTime", []) or []
    if not isinstance(accepted, list) or len(accepted) not in (0, count):
        raise FilingDiscoveryError(
            f"{source} acceptanceDateTime array has unexpected length"
        )

    rows: list[dict] = []
    for index in range(count):
        form = required["form"][index]
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        accession = str(required["accessionNumber"][index] or "").strip()
        report_date = normalize_report_date(required["reportDate"][index])
        if not accession or not report_date:
            raise FilingDiscoveryError(
                f"{source} has a 13F row without accession/report date at index {index}"
            )
        rows.append({
            "cik": cik,
            "name": name,
            "form_type": form,
            "accession": accession,
            "date_filed": str(required["filingDate"][index] or "").strip(),
            "accepted_at": _normalize_accepted_at(accepted[index]) if accepted else None,
            "report_date": report_date,
            "filename": "",
        })
    return rows

def _discover_submission_filings(
    cik: int,
    *,
    include_archives: bool = False,
) -> tuple[list[dict], str]:
    """Fetch a CIK's authoritative SEC submissions rows.

    The recent payload is normally sufficient. Archive shards are opt-in for
    late amendments and historical repair; callers retry with them when a
    complete base is not present in the recent payload.
    """
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    try:
        sub = HTTP.get(url).json()
    except Exception as exc:
        raise FilingDiscoveryError(f"submissions discovery failed for CIK {cik}") from exc
    if not isinstance(sub, dict):
        raise FilingDiscoveryError(f"submissions payload for CIK {cik} is not an object")

    name = str(sub.get("name") or "")
    filings_meta = sub.get("filings") or {}
    if not isinstance(filings_meta, dict):
        raise FilingDiscoveryError(f"submissions filing metadata missing for CIK {cik}")
    rows = _submission_rows(
        filings_meta.get("recent") or {}, cik, name, f"CIK {cik} recent"
    )

    if include_archives:
        archive_files = filings_meta.get("files") or []
        if not isinstance(archive_files, list):
            raise FilingDiscoveryError(f"archive metadata malformed for CIK {cik}")
        for archive in archive_files:
            archive_name = archive.get("name") if isinstance(archive, dict) else None
            if not archive_name:
                raise FilingDiscoveryError(f"archive metadata missing name for CIK {cik}")
            archive_url = f"https://data.sec.gov/submissions/{archive_name}"
            try:
                archive_payload = HTTP.get(archive_url).json()
            except Exception as exc:
                raise FilingDiscoveryError(
                    f"archive discovery failed for CIK {cik}: {archive_name}"
                ) from exc
            rows.extend(_submission_rows(
                archive_payload, cik, name, f"CIK {cik} {archive_name}"
            ))

    by_accession: dict[str, dict] = {}
    for row in rows:
        accession = row["accession"]
        prior = by_accession.get(accession)
        if prior is not None and prior != row:
            raise FilingDiscoveryError(
                f"conflicting submissions metadata for accession {accession}"
            )
        by_accession[accession] = row
    ordered = sorted(
        by_accession.values(),
        key=lambda row: (
            row.get("accepted_at") or row.get("date_filed") or "",
            row["accession"],
        ),
    )
    return ordered, name


def get_13f_filings_for_cik(
    cik: int,
    max_quarters: int,
    *,
    target_report_date: str | None = None,
    include_archives: bool = False,
) -> tuple[list[dict], str]:
    """Return complete filing chains for selected report dates.

    Selection happens *after* all rows are collected, so reaching the quarter
    limit can never truncate the original or another amendment belonging to
    the oldest selected quarter.
    """
    rows, name = _discover_submission_filings(cik, include_archives=include_archives)
    if target_report_date:
        target = normalize_report_date(target_report_date)
        return [row for row in rows if row.get("report_date") == target], name
    if max_quarters < 1:
        raise ValueError("max_quarters must be positive")
    selected_dates = sorted(
        {row["report_date"] for row in rows if row.get("report_date")},
        reverse=True,
    )[:max_quarters]
    selected = set(selected_dates)
    return [row for row in rows if row.get("report_date") in selected], name


# ----------------------------------------------------------------------------
# Holdings parsing
# ----------------------------------------------------------------------------

def _first_local_text(tree: etree._Element, *names: str) -> str | None:
    wanted = set(names)
    for element in tree.iter():
        if not isinstance(element.tag, str):
            continue
        if etree.QName(element.tag).localname in wanted and element.text:
            text = element.text.strip()
            if text:
                return text
    return None


def _first_local_element(
    tree: etree._Element,
    name: str,
) -> etree._Element | None:
    for element in tree.iter():
        if (
            isinstance(element.tag, str)
            and etree.QName(element.tag).localname == name
        ):
            return element
    return None


def _parse_bool_text(value: str | None) -> bool | None:
    text = str(value or "").strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None

def normalize_amendment_kind(value: str | None) -> str:
    normalized = re.sub(r"[\s_-]+", " ", str(value or "").strip().upper())
    if normalized == "RESTATEMENT":
        return "RESTATEMENT"
    if normalized in {"NEW HOLDINGS", "NEW HOLDING"}:
        return "NEW_HOLDINGS"
    return "UNKNOWN"


def normalize_filer_identity_name(value: str | None) -> str:
    """Normalize punctuation/case only for authoritative name comparisons."""
    return "".join(re.findall(r"[A-Z0-9]+", str(value or "").upper()))

def parse_primary_document(xml_bytes: bytes, form_type: str | None = None) -> dict:
    """Parse composition metadata from one filing's primary SEC document."""
    try:
        tree = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as exc:
        raise FilingParseError("primary document is not valid XML") from exc

    report_date = normalize_report_date(_first_local_text(tree, "periodOfReport"))
    is_amendment = _parse_bool_text(_first_local_text(tree, "isAmendment"))
    amendment_number_text = _first_local_text(tree, "amendmentNo", "amendmentNumber")
    amendment_number = parse_numeric_text(amendment_number_text or "")
    raw_kind = _first_local_text(tree, "amendmentType")
    reported_entry_total = parse_numeric_text(
        _first_local_text(tree, "tableEntryTotal") or ""
    )
    reported_value_total = parse_numeric_text(
        _first_local_text(tree, "tableValueTotal") or ""
    )
    filer = _first_local_element(tree, "filer")
    raw_filer_cik = _first_local_text(filer, "cik") if filer is not None else None
    filer_cik = (
        int(raw_filer_cik)
        if raw_filer_cik and re.fullmatch(r"\d+", raw_filer_cik)
        else None
    )
    filing_manager = _first_local_element(tree, "filingManager")
    filing_manager_name = (
        _first_local_text(filing_manager, "name")
        if filing_manager is not None
        else None
    )
    manager_address = (
        _first_local_element(filing_manager, "address")
        if filing_manager is not None
        else None
    )
    address_fields = {
        "street1": "street1",
        "street2": "street2",
        "city": "city",
        "state_or_country": "stateOrCountry",
        "zip_code": "zipCode",
    }
    filing_manager_address = {
        output_name: value
        for output_name, xml_name in address_fields.items()
        if manager_address is not None
        and (value := _first_local_text(manager_address, xml_name))
    }

    declared_amendment = str(form_type or "").upper().endswith("/A")
    has_amendment_metadata = bool(raw_kind or amendment_number_text or is_amendment is True)
    metadata_errors: list[str] = []
    if declared_amendment:
        amendment_kind = normalize_amendment_kind(raw_kind)
        if is_amendment is False:
            metadata_errors.append("amended form declares isAmendment=false")
        if amendment_kind == "UNKNOWN":
            metadata_errors.append("amendment type is missing or unrecognized")
        if not isinstance(amendment_number, int) or amendment_number < 1:
            metadata_errors.append("amendment number is missing or invalid")
    else:
        amendment_kind = "ORIGINAL"
        if has_amendment_metadata:
            metadata_errors.append("original form contains amendment metadata")

    if metadata_errors:
        amendment_kind = "UNKNOWN"
    return {
        "report_date": report_date,
        "is_amendment": is_amendment,
        "amendment_number": amendment_number,
        "amendment_kind": amendment_kind,
        "reported_entry_total": reported_entry_total,
        "reported_value_total": reported_value_total,
        **({"filer_cik": filer_cik} if filer_cik is not None else {}),
        **(
            {"filing_manager_name": filing_manager_name}
            if filing_manager_name is not None
            else {}
        ),
        **(
            {"filing_manager_address": filing_manager_address}
            if filing_manager_address
            else {}
        ),
        **(
            {"form_13f_file_number": form_13f_file_number}
            if (
                form_13f_file_number := _first_local_text(
                    tree,
                    "form13FFileNumber",
                )
            )
            else {}
        ),
        "metadata_errors": metadata_errors,
    }

def _information_table_totals(xml_bytes: bytes) -> tuple[int, int]:
    try:
        tree = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as exc:
        raise FilingParseError("information table is not valid XML") from exc
    rows = 0
    total_value = 0
    for element in tree.iter():
        if not isinstance(element.tag, str):
            continue
        if etree.QName(element.tag).localname != "infoTable":
            continue
        rows += 1
        value_text = _first_local_text(element, "value")
        value = parse_numeric_text(value_text or "")
        if value is None:
            raise FilingParseError("information table row has an invalid value")
        total_value += value
    return rows, total_value


_PEER_PRICE_HISTORY_CACHE: dict[
    tuple[str, str], dict[str, list[tuple[int | None, float]]]
] = {}
_PEER_PRICE_MAX_DISTANCE_DAYS = 100


def load_peer_value_unit_prices(
    cik: int,
    report_date: str,
    holdings: list[dict],
) -> dict[str, tuple[float, int, int]]:
    """Load same-security SEC price references from generated stock history.

    Exact-quarter peers are preferred. When a new quarter has not accumulated
    three other filers yet, the nearest retained quarter is used; a 1,000x
    unit difference is much larger than normal quarter-to-quarter price moves.
    """
    try:
        target_date = date.fromisoformat(report_date)
    except (TypeError, ValueError):
        return {}

    references: dict[str, tuple[float, int, int]] = {}
    cusips = {
        str(holding.get("cusip") or "").strip().upper()
        for holding in holdings
        if is_unit_evidence_holding(holding) and holding.get("cusip")
    }
    for cusip in sorted(cusips):
        cache_key = (str(STOCKS_DIR.resolve()), cusip)
        histories = _PEER_PRICE_HISTORY_CACHE.get(cache_key)
        if histories is None:
            histories = defaultdict(list)
            stock_path = STOCKS_DIR / stock_filename(cusip, "EQUITY")
            try:
                with open(stock_path) as f:
                    stock = json.load(f)
            except (OSError, json.JSONDecodeError):
                stock = {}
            if str(stock.get("instrument_type") or "EQUITY").upper() == "EQUITY":
                for holder in stock.get("holders", []) or []:
                    holder_cik = holder.get("cik")
                    for entry in holder.get("history", []) or []:
                        entry_date = str(entry.get("date") or "")
                        value = entry.get("value")
                        shares = entry.get("shares")
                        if (
                            entry_date
                            and isinstance(value, (int, float))
                            and not isinstance(value, bool)
                            and value > 0
                            and isinstance(shares, (int, float))
                            and not isinstance(shares, bool)
                            and shares > 0
                            and not entry.get("shares_imputed")
                            and not entry.get("quantity_unknown")
                        ):
                            histories[entry_date].append(
                                (holder_cik, float(value) / float(shares))
                            )
            histories = dict(histories)
            _PEER_PRICE_HISTORY_CACHE[cache_key] = histories

        candidates: list[tuple[int, str, list[float]]] = []
        for candidate_date, observations in histories.items():
            prices = [
                price
                for holder_cik, price in observations
                if str(holder_cik) != str(cik)
            ]
            if len(prices) < 3:
                continue
            try:
                distance = abs(
                    (date.fromisoformat(candidate_date) - target_date).days
                )
            except ValueError:
                continue
            if distance <= _PEER_PRICE_MAX_DISTANCE_DAYS:
                candidates.append((distance, candidate_date, prices))
        if not candidates:
            continue
        distance, _reference_date, prices = min(
            candidates, key=lambda item: (item[0], item[1])
        )
        references[cusip] = (
            statistics.median(prices),
            len(prices),
            distance,
        )
    return references


def _load_prior_value_unit_quarter(
    cik: int,
    report_date: str,
) -> dict | None:
    """Load the exact preceding calendar quarter, without skipping gaps."""
    try:
        target_date = date.fromisoformat(report_date)
    except (TypeError, ValueError):
        return None
    try:
        with open(FUNDS_DIR / f"{cik}.json") as f:
            fund = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None

    prior_date_parts = {
        (3, 31): (target_date.year - 1, 12, 31),
        (6, 30): (target_date.year, 3, 31),
        (9, 30): (target_date.year, 6, 30),
        (12, 31): (target_date.year, 9, 30),
    }.get((target_date.month, target_date.day))
    if prior_date_parts is None:
        return None
    expected_prior_date = date(*prior_date_parts).isoformat()
    matches = [
        quarter
        for quarter in fund.get("quarters", []) or []
        if isinstance(quarter, dict)
        and quarter.get("report_date") == expected_prior_date
    ]
    return matches[0] if len(matches) == 1 else None


def _trusted_value_unit_multiplier(quarter: dict) -> int | None:
    """Return a uniform multiplier only when every applied source proves it."""
    source_filings = quarter.get("source_filings")
    if "source_filings" in quarter and not isinstance(source_filings, list):
        return None
    if isinstance(source_filings, list) and source_filings:
        if any(not isinstance(source, dict) for source in source_filings):
            return None
        applied_sources = [
            source
            for source in source_filings
            if source.get("applied") is True
        ]
        trusted_sources = [
            source
            for source in applied_sources
            if (
                source.get("value_unit_policy_version")
                == VALUE_UNIT_POLICY_VERSION
                and source.get("value_unit_confidence") == "high"
                and source.get("value_multiplier") in {1, 1000}
            )
        ]
        multipliers = {
            source["value_multiplier"] for source in trusted_sources
        }
        if (
            not applied_sources
            or len(trusted_sources) != len(applied_sources)
            or len(multipliers) != 1
        ):
            return None
        return multipliers.pop()

    if (
        quarter.get("value_unit_policy_version")
        == VALUE_UNIT_POLICY_VERSION
        and quarter.get("value_unit_confidence") == "high"
        and quarter.get("value_multiplier") in {1, 1000}
    ):
        return quarter["value_multiplier"]
    return None


def load_prior_value_unit_context(
    cik: int,
    report_date: str,
) -> tuple[int | None, list[dict] | None]:
    """Return a trusted prior convention and its normalized holdings.

    The adjacent holdings are exposed only when the exact preceding calendar
    quarter has complete high-confidence provenance. This prevents a legacy
    bad quarter from vetoing a correct current classification.
    """
    quarter = _load_prior_value_unit_quarter(cik, report_date)
    if quarter is None:
        return None, None
    multiplier = _trusted_value_unit_multiplier(quarter)
    if multiplier is None:
        return None, None
    holdings = quarter.get("holdings")
    total_value = quarter.get("total_value")
    if (
        not isinstance(holdings, list)
        or any(not isinstance(holding, dict) for holding in holdings)
        or isinstance(total_value, bool)
        or not isinstance(total_value, (int, float))
    ):
        return None, None
    values = [holding.get("value") for holding in holdings]
    if (
        any(
            isinstance(value, bool) or not isinstance(value, (int, float))
            for value in values
        )
        or sum(values) != total_value
    ):
        return None, None
    return multiplier, holdings


def load_prior_value_unit_multiplier(
    cik: int,
    report_date: str,
) -> int | None:
    """Return the exact prior quarter's trusted, uniform unit convention."""
    multiplier, _holdings = load_prior_value_unit_context(cik, report_date)
    return multiplier


def fetch_filing_holdings(
    cik: int,
    accession: str,
    *,
    filing: dict | None = None,
) -> dict:
    """Fetch and validate one immutable filing component.

    Holdings stay unconsolidated here. The reducer composes the active filing
    chain first and consolidates exactly once afterward.
    """
    acc_no_dashes = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_dashes}/"

    # 1) Directory listing tells us which files are in the filing.
    try:
        idx_resp = HTTP.get(base + "index.json")
    except Exception as exc:
        raise FilingFetchError(
            f"index fetch failed for {cik}/{accession}"
        ) from exc
    try:
        idx = idx_resp.json()
    except Exception as exc:
        # SEC and intermediate proxies sometimes return an HTML error page
        # with HTTP 200.  The filing itself has not changed, but this response
        # is not authoritative and should get the short fetch retry interval.
        raise FilingFetchError(
            f"index response is not valid JSON for {cik}/{accession}"
        ) from exc
    if not isinstance(idx, dict):
        raise FilingFetchError(
            f"index response is not an object for {cik}/{accession}"
        )

    directory = idx.get("directory")
    if not isinstance(directory, dict):
        raise FilingParseError(
            f"index directory is missing or malformed for {cik}/{accession}"
        )
    items = directory.get("item")
    if (
        not isinstance(items, list)
        or any(not isinstance(item, dict) for item in items)
    ):
        raise FilingParseError(
            f"index item list is malformed for {cik}/{accession}"
        )

    primary_doc_name = None
    candidate_xmls: list[str] = []
    for item in items:
        name = item.get("name", "")
        if not isinstance(name, str):
            raise FilingParseError(
                f"index item name is malformed for {cik}/{accession}"
            )
        if name == "primary_doc.xml":
            primary_doc_name = name
        elif name.lower().endswith(".xml"):
            if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}", name) is None:
                raise FilingParseError(
                    f"unsafe filing document name for {cik}/{accession}"
                )
            candidate_xmls.append(name)

    if not primary_doc_name:
        raise FilingParseError(f"primary_doc.xml missing for {cik}/{accession}")
    try:
        primary_bytes = HTTP.get(base + primary_doc_name).content
    except Exception as exc:
        raise FilingFetchError(
            f"primary document fetch failed for {cik}/{accession}"
        ) from exc
    supplied_form = (filing or {}).get("form_type")
    metadata = parse_primary_document(primary_bytes, supplied_form)
    source_filer_cik = metadata.get("filer_cik")
    if source_filer_cik is None:
        raise FilingIdentityError(
            f"primary filing identity is missing filer CIK for "
            f"{cik}/{accession}"
        )
    if source_filer_cik != int(cik):
        raise FilingIdentityError(
            f"filer CIK conflict for {accession}: requested {cik}, "
            f"primary document declares {source_filer_cik}"
        )
    source_manager_name = metadata.get("filing_manager_name")
    if not normalize_filer_identity_name(source_manager_name):
        raise FilingIdentityError(
            f"primary filing identity is missing filing-manager name for "
            f"{cik}/{accession}"
        )
    expected_manager_name = (filing or {}).get("name")
    filer_name_discrepancy = None
    if (
        normalize_filer_identity_name(expected_manager_name)
        and normalize_filer_identity_name(expected_manager_name)
        != normalize_filer_identity_name(source_manager_name)
    ):
        # A matching CIK remains the authoritative identity key. Historical
        # covers can legitimately use a prior legal name, so preserve the
        # discrepancy for audit instead of quarantining a valid filing.
        filer_name_discrepancy = {
            "discovery_name": str(expected_manager_name),
            "primary_name": str(source_manager_name),
        }
        log.warning(
            "  filing-manager name differs for %s/%s: discovery=%r, primary=%r",
            cik,
            accession,
            expected_manager_name,
            source_manager_name,
        )
    report_date = metadata.get("report_date")
    expected_report_date = normalize_report_date((filing or {}).get("report_date"))
    if not report_date:
        raise FilingParseError(f"periodOfReport missing for {cik}/{accession}")
    if expected_report_date and report_date != expected_report_date:
        raise FilingParseError(
            f"report-date conflict for {accession}: {expected_report_date} != {report_date}"
        )
    cover_entries = metadata.get("reported_entry_total")
    cover_value = metadata.get("reported_value_total")
    if cover_entries is None or cover_value is None:
        raise FilingParseError(f"cover-page totals missing for {cik}/{accession}")

    # 3) Find the information table xml. Prefer files with "info" or "table"
    #    in the name; fall back to any other .xml.
    preferred = [n for n in candidate_xmls if re.search(r"info|table", n, re.I)]
    candidates = preferred + [n for n in candidate_xmls if n not in preferred]

    holdings: list[dict] | None = None
    information_bytes: bytes | None = None
    information_url: str | None = None
    raw_entry_total = 0
    raw_value_total = 0
    effective_entry_total = 0
    effective_value_total = 0
    cover_reconciliation_status = "EXACT"
    internally_complete_candidates: list[
        tuple[list[dict], bytes, int, int, str]
    ] = []
    candidate_fetch_failures: list[str] = []
    reconciliation_errors: list[str] = []
    for fname in candidates:
        try:
            xml_resp = HTTP.get(base + fname)
        except Exception as e:
            log.warning(f"  fetch {fname} failed: {e}")
            candidate_fetch_failures.append(fname)
            continue
        try:
            entry_total, value_total = _information_table_totals(xml_resp.content)
        except FilingParseError:
            continue
        parsed = parse_information_table(
            xml_resp.content,
            accession=accession,
            report_date=report_date,
        )
        recognized_empty_placeholder = (
            parsed is None
            and value_total == 0
            and cover_value == 0
            # SEC confidential-treatment filings appear in three verified
            # zero-value shapes: an empty named table with a 0-row cover,
            # or one dummy zero row with a 0- or 1-row cover.  A cover that
            # reports one row while the table contains none is not one of
            # those shapes and must fail reconciliation.
            and (entry_total, cover_entries) in {(0, 0), (1, 0), (1, 1)}
            and (
                entry_total == 1
                or bool(re.search(r"info|table", fname, re.I))
            )
        )
        if recognized_empty_placeholder:
            holdings = []
            information_bytes = xml_resp.content
            information_url = base + fname
            raw_entry_total = entry_total
            raw_value_total = value_total
            effective_entry_total = cover_entries
            effective_value_total = cover_value
            break
        if parsed is None:
            if entry_total != cover_entries:
                reconciliation_errors.append(
                    f"entry-total mismatch: table={entry_total}, "
                    f"cover={cover_entries}"
                )
            elif value_total != cover_value:
                reconciliation_errors.append(
                    f"value-total mismatch: table={value_total}, "
                    f"cover={cover_value}"
                )
            else:
                reconciliation_errors.append(
                    "nonzero information-table rows were dropped during parsing"
                )
            continue
        parsed_value = sum(row.get("value", 0) or 0 for row in parsed)
        if len(parsed) != entry_total or parsed_value != value_total:
            reconciliation_errors.append(
                "parsed rows do not reconcile to the SEC information table"
            )
            continue
        if entry_total == cover_entries and value_total == cover_value:
            holdings = parsed
            information_bytes = xml_resp.content
            information_url = base + fname
            raw_entry_total = entry_total
            raw_value_total = value_total
            effective_entry_total = entry_total
            effective_value_total = value_total
            break

        internally_complete_candidates.append(
            (parsed, xml_resp.content, entry_total, value_total, fname)
        )
        if entry_total != cover_entries:
            reconciliation_errors.append(
                f"entry-total mismatch: table={entry_total}, cover={cover_entries}"
            )
        if value_total != cover_value:
            reconciliation_errors.append(
                f"value-total mismatch: table={value_total}, cover={cover_value}"
            )

    if holdings is None:
        if (
            len(internally_complete_candidates) == 1
            and not candidate_fetch_failures
        ):
            (
                holdings,
                information_bytes,
                raw_entry_total,
                raw_value_total,
                selected_name,
            ) = internally_complete_candidates[0]
            effective_entry_total = raw_entry_total
            effective_value_total = raw_value_total
            cover_reconciliation_status = "MISMATCH_UNIQUE_TABLE"
            information_url = base + selected_name
            log.warning(
                "  accepting unique internally complete information table %s "
                "for %s despite cover mismatch: entries table=%s cover=%s; "
                "value table=%s cover=%s",
                selected_name,
                accession,
                raw_entry_total,
                cover_entries,
                raw_value_total,
                cover_value,
            )
        else:
            detail = (
                "multiple internally complete table candidates disagree with cover"
                if len(internally_complete_candidates) > 1
                else (
                    "candidate XML fetch failed while evaluating cover "
                    f"mismatch: {', '.join(candidate_fetch_failures)}"
                    if candidate_fetch_failures
                    else (
                        reconciliation_errors[-1]
                        if reconciliation_errors
                        else "no table XML"
                    )
                )
            )
            error_type = (
                FilingFetchError
                if candidate_fetch_failures
                else FilingParseError
            )
            raise error_type(
                f"information table reconciliation failed for "
                f"{cik}/{accession}: {detail}"
            )

    peer_prices = load_peer_value_unit_prices(cik, report_date, holdings)
    prior_multiplier, adjacent_holdings = load_prior_value_unit_context(
        cik,
        report_date,
    )
    try:
        unit_metadata = normalize_value_units(
            holdings,
            peer_prices,
            prior_multiplier=prior_multiplier,
            adjacent_holdings=adjacent_holdings,
        )
    except AmbiguousValueUnits as exc:
        raise FilingParseError(
            f"ambiguous value units for {cik}/{accession}: {exc}"
        ) from exc
    normalized_value_total = sum(
        holding.get("value", 0) or 0 for holding in holdings
    )
    expected_normalized_total = (
        effective_value_total * unit_metadata["value_multiplier"]
    )
    if normalized_value_total != expected_normalized_total:
        raise FilingParseError(
            f"normalized value-total mismatch for {cik}/{accession}: "
            f"holdings={normalized_value_total}, "
            f"expected={expected_normalized_total}"
        )
    source_hash = hashlib.sha256(
        primary_bytes + b"\0" + (information_bytes or b"")
    ).hexdigest()
    if information_bytes is None or information_url is None:
        raise FilingParseError(
            f"selected information table lacks source evidence for "
            f"{cik}/{accession}"
        )
    try:
        reported_identity_url = normalize_sec_identity_source_url(
            information_url,
            accession=accession,
        )
    except ValueError as exc:
        raise FilingParseError(
            f"selected information-table URL is invalid for {cik}/{accession}"
        ) from exc
    reported_identity_source = {
        "accession": accession,
        "report_date": report_date,
        "url": reported_identity_url,
        "sha256": hashlib.sha256(information_bytes).hexdigest(),
    }
    return {
        "cik": cik,
        "report_date": report_date,
        "filing_date": (filing or {}).get("date_filed"),
        "accepted_at": _normalize_accepted_at((filing or {}).get("accepted_at")),
        "accession": accession,
        "form_type": supplied_form,
        "amendment_number": metadata.get("amendment_number"),
        "amendment_kind": metadata.get("amendment_kind"),
        "metadata_errors": metadata.get("metadata_errors") or [],
        **(
            {"filer_name_discrepancy": filer_name_discrepancy}
            if filer_name_discrepancy is not None
            else {}
        ),
        # These totals describe the selected information table and therefore
        # remain the reconciliation basis for composition. Cover totals are
        # retained separately so small filer-authored discrepancies are
        # visible without discarding an otherwise unique, internally complete
        # SEC table.
        "reported_entry_total": effective_entry_total,
        "reported_value_total": effective_value_total,
        "cover_reported_entry_total": cover_entries,
        "cover_reported_value_total": cover_value,
        "cover_reconciliation_status": cover_reconciliation_status,
        "normalized_value_total": normalized_value_total,
        **unit_metadata,
        "security_identity_version": SECURITY_IDENTITY_VERSION,
        "source_hash": source_hash,
        "reported_identity_source": reported_identity_source,
        "holdings": holdings,
    }


def parse_numeric_text(text: str, *, allow_float: bool = False) -> int | float | None:
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        if allow_float:
            num = float(cleaned)
            return int(num) if num.is_integer() else num
        return int(cleaned)
    except ValueError:
        if not allow_float:
            try:
                num = float(cleaned)
                return int(num) if num.is_integer() else None
            except ValueError:
                return None
        return None


def consolidate_holdings(holdings: list[dict]) -> list[dict]:
    """13F filings emit one row per (security, manager); the same CUSIP can
    appear many times. Sum shares/value across rows with the same CUSIP +
    class + instrument type so common stock and options on the same issuer
    stay separate."""
    merged: dict[tuple[str, str, str], dict] = {}
    for h in holdings:
        holding_type = classify_saved_holding(h)
        key = (h["cusip"], h.get("class", ""), holding_type)
        if key in merged:
            merged[key]["value"] += h.get("value", 0)
            merged[key]["shares"] += h.get("shares", 0)
            if not merged[key].get("put_call") and h.get("put_call"):
                merged[key]["put_call"] = h["put_call"]
        else:
            merged[key] = {**h, "holding_type": holding_type}
    return list(merged.values())


_OPTION_CLASS_EXACT = {"CALL", "PUT", "EQUITY OPTION", "ETF OPTION",
                       "LST EQUITY OPTION", "ETD EQUITY OPTION",
                       "FLEX OPTION", "OPTION", "OPTIONS", "OPT"}
_NOT_OPTION_KEYWORDS = {"COVERED CALL", "COVERED PUT", "OPTIMAL", "OPTIMIZE",
                        "OPTIMIZED", "OPTIMUM", "OPTION CARE"}
_PREFERRED_KEYWORDS = {"PFD", "PRF", "PREF", "PREFERRED"}
_NOTE_KEYWORDS = {
    "NOTE",
    "NOTES",
    "DEBT",
    "BOND",
    "BONDS",
    "CONV BD",
    "CONVERTIBLE BOND",
    "CONV SR",
    "SR NOTE",
}
_IS_ETF_HINT = {"ETF", "SECS INC", "INCOME", "INCM SEC", "SPECTRUM",
                "FINL PFD", "INSTL PFD", "INVT GRD"}
_NOTE_CLASS_EXACT = {
    "CCB",
    "CNV",
    "CONV BD",
    "CONV BND",
    "CONVERTIBLE",
    "CV",
    "CV BND",
    "FD CV",
    "SDBCV",
    "SOVEREIGN/CORPORATE",
}
_FUND_EQUITY_CLASS_TOKEN_RE = re.compile(
    r"\b(?:EQTY|EQUITIES|EQUITY|ETF|ETP|FUND)\b"
)
_COMMON_EQUITY_CLASS_TOKEN_RE = re.compile(
    r"\b(?:COM|COMMON|ORD|ORDINARY|SHS|SHARE|SHARES|STOCK)\b"
)
_NOTE_COUPON_DATE_RE = re.compile(r"\b\d+(?:\.\d+)?%?\s+\d{2}/\d{2}/\d{2,4}\b")
# Match issuers that start with a bare PUT/CALL token — "PUT 8 APPLE INC",
# "CALL 280 TSLA". Word-boundary guard avoids false positives on legit
# company names like CALLAWAY, PUTNAM, etc.
_OPTION_ISSUER_PREFIX_RE = re.compile(r"^(PUT|CALL)\s+")


def _has_note_coupon_date_evidence(holding: dict) -> bool:
    """Return coupon/maturity evidence contained within one SEC field.

    Issuer and class are separate source fields and the issuer is later
    replaced with mutable display metadata.  Concatenating them can invent a
    coupon at the boundary, for example issuer ``... CORP 1`` plus class
    ``06/10/2030``.  Search each field independently so classification never
    depends on that synthetic boundary.
    """
    return any(
        _NOTE_COUPON_DATE_RE.search(
            str(holding.get(field) or "").upper().strip()
        )
        for field in ("issuer", "class")
    )


def display_ticker_for_holding_type(
    ticker: object | None,
    instrument_type: object | None,
) -> str | None:
    """Return safe display metadata for one persisted holding type.

    A structured note label is useful only on a NOTE row. Conversely, a NOTE
    row should not inherit a plain common-stock symbol when SEC evidence or the
    SEC class evidence refers to a different security type.
    """
    raw_ticker = str(ticker or "").strip().upper()
    if not raw_ticker:
        return None
    note_label = normalize_note_security_label(raw_ticker)
    if normalize_instrument_type(instrument_type) == "NOTE":
        return note_label
    if note_label:
        return None
    return raw_ticker


def _component_source_hash(component: dict) -> str:
    source_hash = str(component.get("source_hash") or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", source_hash):
        return source_hash
    return _canonical_json_hash({
        "accession": component.get("accession"),
        "form_type": component.get("form_type"),
        "report_date": component.get("report_date"),
        "filing_date": component.get("filing_date"),
        "accepted_at": component.get("accepted_at"),
        "amendment_number": component.get("amendment_number"),
        "amendment_kind": component.get("amendment_kind"),
        "reported_entry_total": component.get("reported_entry_total"),
        "reported_value_total": component.get("reported_value_total"),
        "cover_reported_entry_total": component.get(
            "cover_reported_entry_total"
        ),
        "cover_reported_value_total": component.get(
            "cover_reported_value_total"
        ),
        "cover_reconciliation_status": component.get(
            "cover_reconciliation_status"
        ),
        "normalized_value_total": component.get("normalized_value_total"),
        "value_unit_policy_version": component.get("value_unit_policy_version"),
        "value_multiplier": component.get("value_multiplier"),
        "value_unit_method": component.get("value_unit_method"),
        "value_unit_confidence": component.get("value_unit_confidence"),
        "value_unit_evidence": component.get("value_unit_evidence"),
        **(
            {
                "security_identity_version":
                    component["security_identity_version"]
            }
            if "security_identity_version" in component
            else {}
        ),
        "holdings": component.get("holdings"),
    })


def _component_reported_identity_source(
    component: dict,
) -> dict[str, str] | None:
    """Validate the exact SEC document that supplied reported identities."""

    raw = component.get("reported_identity_source")
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) != {
        "accession",
        "report_date",
        "url",
        "sha256",
    }:
        raise FilingChainError(
            "invalid_component",
            "reported identity source must contain accession, report_date, "
            "url, and sha256",
        )
    accession = str(raw.get("accession") or "").strip()
    report_date = str(raw.get("report_date") or "").strip()
    checksum = str(raw.get("sha256") or "").strip().lower()
    try:
        url = normalize_sec_identity_source_url(
            str(raw.get("url") or "").strip(),
            accession=accession,
        )
    except ValueError as exc:
        raise FilingChainError(
            "invalid_component",
            "reported identity source URL is not an exact SEC filing source",
        ) from exc
    if (
        accession != component.get("accession")
        or report_date != component.get("report_date")
        or raw.get("url") != url
        or raw.get("sha256") != checksum
        or re.fullmatch(r"[0-9a-f]{64}", checksum) is None
    ):
        raise FilingChainError(
            "invalid_component",
            "reported identity source does not match its filing component",
        )
    return {
        "accession": accession,
        "report_date": report_date,
        "url": url,
        "sha256": checksum,
    }


def _quarter_has_complete_reported_identity_sources(quarter: dict) -> bool:
    """Check compact SEC source refs before upgrading immutable hash proof."""

    sources = quarter.get("reported_identity_sources")
    holdings = quarter.get("holdings")
    applied = quarter.get("applied_accessions")
    if (
        not isinstance(sources, list)
        or not isinstance(holdings, list)
        or not isinstance(applied, list)
    ):
        return False
    normalized: list[dict[str, str]] = []
    for source in sources:
        if not isinstance(source, dict) or set(source) != {
            "accession",
            "report_date",
            "url",
            "sha256",
        }:
            return False
        accession = str(source.get("accession") or "").strip()
        report_date = str(source.get("report_date") or "").strip()
        checksum = str(source.get("sha256") or "").strip().lower()
        try:
            url = normalize_sec_identity_source_url(
                source.get("url"),
                accession=accession,
            )
        except ValueError:
            return False
        canonical = {
            "accession": accession,
            "report_date": report_date,
            "url": url,
            "sha256": checksum,
        }
        if (
            source != canonical
            or accession not in applied
            or report_date != quarter.get("report_date")
            or re.fullmatch(r"[0-9a-f]{64}", checksum) is None
        ):
            return False
        normalized.append(canonical)
    canonical_sources = sorted(
        normalized,
        key=lambda source: (
            source["accession"],
            source["report_date"],
            source["url"],
            source["sha256"],
        ),
    )
    if normalized != canonical_sources or len({
        (
            source["accession"],
            source["report_date"],
            source["url"],
            source["sha256"],
        )
        for source in normalized
    }) != len(normalized):
        return False
    covered = {
        (source["accession"], source["report_date"])
        for source in normalized
    }
    return all(
        isinstance(holding, dict)
        and (
            str(holding.get("accession") or ""),
            str(holding.get("report_date") or ""),
        )
        in covered
        for holding in holdings
    )


def _holding_has_hashable_reported_identity(holding: dict) -> bool:
    """Require explicit SEC fields while allowing canonical as-filed blanks."""

    for field in ("reported_issuer", "reported_class"):
        value = holding.get(field)
        if (
            field not in holding
            or not isinstance(value, str)
            or value != value.strip()
        ):
            return False
    return all(
        isinstance(holding.get(field), str)
        and bool(holding[field].strip())
        for field in ("reported_cusip", "accession", "report_date")
    )


def calculate_composition_hash(
    report_date: str,
    base_accession: str,
    applied_accessions: list[str],
    applied_source_hashes: list[str],
    holdings: list[dict],
    *,
    composition_version: int = AMENDMENT_REDUCER_VERSION,
    source_filings: list[dict] | None = None,
    security_identity_version: int | None = None,
    composition_hash_version: int = 1,
) -> str:
    return _calculate_composition_hash(
        report_date,
        base_accession,
        applied_accessions,
        applied_source_hashes,
        holdings,
        composition_version=composition_version,
        source_filings=source_filings,
        security_identity_version=security_identity_version,
        composition_hash_version=composition_hash_version,
    )


def _normalize_amendment_identity_text(value: object) -> str:
    return " ".join(str(value or "").upper().split())


def _normalize_other_managers(value: object) -> str:
    managers = []
    for manager in re.split(
        r"[\s,;]+", _normalize_amendment_identity_text(value)
    ):
        if manager:
            managers.append(str(int(manager)) if manager.isdigit() else manager)
    return ",".join(sorted(managers))


def _holding_entry_identity(holding: dict) -> tuple[str, ...]:
    """Return the SEC row identity used before downstream consolidation."""
    return (
        _normalize_amendment_identity_text(holding.get("cusip")),
        _normalize_amendment_identity_text(holding.get("class")),
        normalize_instrument_type(
            holding.get("holding_type") or holding.get("option_type")
        ),
        _normalize_amendment_identity_text(holding.get("put_call")),
        _normalize_amendment_identity_text(holding.get("share_amount_type")),
        _normalize_amendment_identity_text(
            holding.get("investment_discretion")
        ),
        _normalize_other_managers(holding.get("other_manager")),
    )


def _new_holdings_overlap(
    prior_rows: list[dict],
    amendment_rows: list[dict],
) -> dict:
    prior_identities = Counter(
        _holding_entry_identity(holding) for holding in prior_rows
    )
    amendment_identities = Counter(
        _holding_entry_identity(holding) for holding in amendment_rows
    )
    matched_rows = sum((prior_identities & amendment_identities).values())
    # Values and shares are mutable attributes an amendment may correct.
    # "Exact positions" means the SEC row-identity multiset is unchanged.
    exact_positions = (
        bool(prior_rows)
        and bool(amendment_rows)
        and prior_identities == amendment_identities
    )
    return {
        "identity_version": NEW_HOLDINGS_IDENTITY_VERSION,
        "matched_rows": matched_rows,
        "prior_rows": len(prior_rows),
        "amendment_rows": len(amendment_rows),
        "exact_positions": exact_positions,
    }


def _classify_new_holdings_overlap(overlap: dict) -> str:
    matched = overlap["matched_rows"]
    prior_rows = overlap["prior_rows"]
    amendment_rows = overlap["amendment_rows"]
    if amendment_rows == 0:
        raise FilingChainError(
            "empty_new_holdings",
            "NEW_HOLDINGS amendment has no information-table rows",
        )
    if matched == 0:
        return "APPEND"
    if overlap["exact_positions"]:
        return "REPLACE"
    if (
        matched >= NEW_HOLDINGS_REPLACEMENT_MIN_MATCHED_ROWS
        and matched * NEW_HOLDINGS_REPLACEMENT_COVERAGE_DENOMINATOR
        >= prior_rows * NEW_HOLDINGS_REPLACEMENT_COVERAGE_NUMERATOR
        and matched * NEW_HOLDINGS_REPLACEMENT_COVERAGE_DENOMINATOR
        >= amendment_rows * NEW_HOLDINGS_REPLACEMENT_COVERAGE_NUMERATOR
    ):
        return "REPLACE"
    raise FilingChainError(
        "ambiguous_new_holdings_overlap",
        "NEW_HOLDINGS amendment overlaps the active portfolio without being "
        f"a clear supplement or replacement: matched={matched}, "
        f"prior={prior_rows}, amendment={amendment_rows}",
    )


def _component_holdings(component: dict) -> list[dict]:
    holdings = component.get("holdings")
    if (
        not isinstance(holdings, list)
        or any(not isinstance(holding, dict) for holding in holdings)
    ):
        raise FilingChainError(
            "invalid_component",
            f"accession {component['accession']} has invalid holdings",
        )
    return [dict(holding) for holding in holdings]

def compose_quarter_filings(components: list[dict]) -> dict:
    """Reduce immutable SEC filing components into one publishable quarter.

    The latest declared complete component (ORIGINAL or RESTATEMENT) starts the
    active chain. A disjoint NEW_HOLDINGS amendment appends; a near-complete
    copy replaces the active portfolio despite its declaration; partial
    overlap is quarantined. Ambiguous chains therefore retain the last-known-
    good materialized quarter.
    """
    if not components:
        raise FilingChainError("missing_base", "filing chain is empty")

    deduplicated: dict[str, dict] = {}
    signatures: dict[str, str] = {}
    for raw_component in components:
        component = dict(raw_component)
        accession = str(component.get("accession") or "").strip()
        if not accession:
            raise FilingChainError("invalid_component", "component is missing accession")
        component["accession"] = accession
        component["report_date"] = normalize_report_date(component.get("report_date"))
        component["accepted_at"] = _normalize_accepted_at(component.get("accepted_at"))
        component["amendment_kind"] = str(
            component.get("amendment_kind") or "UNKNOWN"
        ).upper()
        component["source_hash"] = _component_source_hash(component)
        reported_identity_source = _component_reported_identity_source(
            component
        )
        if reported_identity_source is not None:
            component["reported_identity_source"] = reported_identity_source
        signature = _canonical_json_hash({
            key: component.get(key)
            for key in (
                "cik", "report_date", "filing_date", "accepted_at", "accession",
                "form_type", "amendment_number", "amendment_kind", "source_hash",
                "reported_entry_total", "reported_value_total",
                "normalized_value_total", "value_unit_policy_version",
                "value_multiplier", "value_unit_method",
                "value_unit_confidence", "value_unit_evidence", "holdings",
                "reported_identity_source",
            )
        })
        if accession in deduplicated:
            if signatures[accession] != signature:
                raise FilingChainError(
                    "duplicate_accession_conflict",
                    f"accession {accession} has conflicting component data",
                )
            continue
        deduplicated[accession] = component
        signatures[accession] = signature

    chain = list(deduplicated.values())
    report_dates = {component.get("report_date") for component in chain}
    if None in report_dates or len(report_dates) != 1:
        raise FilingChainError(
            "mixed_report_dates", f"filing chain has report dates {sorted(map(str, report_dates))}"
        )
    ciks = {component.get("cik") for component in chain if component.get("cik") is not None}
    if len(ciks) > 1:
        raise FilingChainError("invalid_component", "filing chain mixes multiple CIKs")

    if len(chain) > 1 and any(not component.get("accepted_at") for component in chain):
        raise FilingChainError(
            "ambiguous_order", "multi-component chain is missing SEC acceptance time"
        )
    by_accepted_at: dict[str | None, list[dict]] = defaultdict(list)
    for component in chain:
        by_accepted_at[component.get("accepted_at")].append(component)
    for accepted_at, tied in by_accepted_at.items():
        if len(tied) < 2:
            continue
        sequence_keys = [
            0 if component["amendment_kind"] == "ORIGINAL"
            else component.get("amendment_number")
            for component in tied
        ]
        if (
            any(not isinstance(key, int) for key in sequence_keys)
            or len(sequence_keys) != len(set(sequence_keys))
        ):
            raise FilingChainError(
                "ambiguous_order",
                f"components accepted at {accepted_at} lack a unique declared sequence",
            )
    chain.sort(key=lambda component: (
        component.get("accepted_at") or component.get("filing_date") or "",
        0 if component["amendment_kind"] == "ORIGINAL"
        else component.get("amendment_number") or sys.maxsize,
        component["accession"],
    ))

    originals = [component for component in chain if component["amendment_kind"] == "ORIGINAL"]
    if len(originals) > 1:
        raise FilingChainError("multiple_originals", "filing chain has multiple originals")
    if originals and chain[0]["accession"] != originals[0]["accession"]:
        raise FilingChainError(
            "amendment_number_conflict", "original was accepted after an amendment"
        )

    complete_indexes = [
        index for index, component in enumerate(chain)
        if component["amendment_kind"] in {"ORIGINAL", "RESTATEMENT"}
    ]
    if not complete_indexes:
        raise FilingChainError(
            "missing_base", "new-holdings amendments have no complete base filing"
        )
    base_index = complete_indexes[-1]
    base = chain[base_index]
    active = chain[base_index:]
    for component in active[1:]:
        if component["amendment_kind"] == "UNKNOWN":
            raise FilingChainError(
                "unknown_amendment_type",
                f"active amendment {component['accession']} has unknown semantics",
            )
        if component["amendment_kind"] != "NEW_HOLDINGS":
            raise FilingChainError(
                "invalid_component",
                f"unexpected active component kind {component['amendment_kind']}",
            )

    # Only the active suffix affects the materialized result. A complete later
    # restatement can safely recover from an unparseable or oddly numbered
    # earlier amendment because it supersedes that earlier chain in full.
    expected_number = 1
    numbered_active = active[1:]
    if base["amendment_kind"] == "RESTATEMENT":
        base_number = base.get("amendment_number")
        if not isinstance(base_number, int) or base_number < 1:
            raise FilingChainError(
                "amendment_number_conflict",
                f"restatement {base['accession']} has no valid amendment number",
            )
        expected_number = base_number + 1
    for component in numbered_active:
        number = component.get("amendment_number")
        if not isinstance(number, int) or number != expected_number:
            raise FilingChainError(
                "amendment_number_conflict",
                f"active amendment {component['accession']} has number {number!r}; "
                f"expected {expected_number}",
            )
        expected_number += 1

    actions = {
        component["accession"]: "SUPERSEDED"
        for component in chain
    }
    overlaps: dict[str, dict] = {}
    applied = [base]
    actions[base["accession"]] = "BASE"
    rows = _component_holdings(base)
    for component in active[1:]:
        amendment_rows = _component_holdings(component)
        overlap = _new_holdings_overlap(rows, amendment_rows)
        overlaps[component["accession"]] = overlap
        action = _classify_new_holdings_overlap(overlap)
        if action == "APPEND":
            actions[component["accession"]] = action
            applied.append(component)
            rows.extend(amendment_rows)
            continue

        for superseded in applied:
            actions[superseded["accession"]] = "SUPERSEDED"
        actions[component["accession"]] = "REPLACE"
        applied = [component]
        rows = amendment_rows

    holdings = consolidate_holdings(rows)
    for holding in holdings:
        holding.pop("investment_discretion", None)
        holding.pop("other_manager", None)
    holdings.sort(key=lambda holding: (
        -float(holding.get("value", 0) or 0),
        str(holding.get("cusip") or ""),
        str(holding.get("class") or ""),
        str(holding.get("holding_type") or ""),
    ))

    applied_accessions = [component["accession"] for component in applied]
    applied_set = set(applied_accessions)
    source_filings = [{
        "accession": component["accession"],
        "form_type": component.get("form_type"),
        "filing_date": component.get("filing_date"),
        "accepted_at": component.get("accepted_at"),
        "amendment_number": component.get("amendment_number"),
        "amendment_kind": component["amendment_kind"],
        **(
            {"filer_name_discrepancy": component["filer_name_discrepancy"]}
            if isinstance(component.get("filer_name_discrepancy"), dict)
            else {}
        ),
        "source_hash": component["source_hash"],
        **(
            {
                "reported_identity_source": component[
                    "reported_identity_source"
                ]
            }
            if (
                component["accession"] in applied_set
                and isinstance(component.get("reported_identity_source"), dict)
            )
            else {}
        ),
        "reported_entry_total": component.get("reported_entry_total"),
        "reported_value_total": component.get("reported_value_total"),
        "cover_reported_entry_total": component.get(
            "cover_reported_entry_total"
        ),
        "cover_reported_value_total": component.get(
            "cover_reported_value_total"
        ),
        "cover_reconciliation_status": component.get(
            "cover_reconciliation_status"
        ),
        "normalized_value_total": component.get("normalized_value_total"),
        "value_unit_policy_version": component.get("value_unit_policy_version"),
        "value_multiplier": component.get("value_multiplier"),
        "value_unit_method": component.get("value_unit_method"),
        "value_unit_confidence": component.get("value_unit_confidence"),
        "value_unit_evidence": component.get("value_unit_evidence"),
        **(
            {
                "security_identity_version":
                    component["security_identity_version"]
            }
            if component.get("security_identity_version")
            == SECURITY_IDENTITY_VERSION
            else {}
        ),
        "composition_action": actions[component["accession"]],
        **(
            {"new_holdings_overlap": overlaps[component["accession"]]}
            if component["accession"] in overlaps
            else {}
        ),
        "applied": component["accession"] in applied_set,
    } for component in chain]
    reported_identity_sources = sorted(
        [
            component["reported_identity_source"]
            for component in applied
            if isinstance(component.get("reported_identity_source"), dict)
        ],
        key=lambda source: (
            source["accession"],
            source["report_date"],
            source["url"],
            source["sha256"],
        ),
    )
    total_value = sum(holding.get("value", 0) or 0 for holding in holdings)
    latest = applied[-1]
    effective_base = applied[0]
    composed_security_identity_version = (
        SECURITY_IDENTITY_VERSION
        if all(
            component.get("security_identity_version")
            == SECURITY_IDENTITY_VERSION
            for component in applied
        )
        else None
    )
    composition_hash = calculate_composition_hash(
        effective_base["report_date"],
        effective_base["accession"],
        applied_accessions,
        [component["source_hash"] for component in applied],
        holdings,
        source_filings=source_filings,
        security_identity_version=composed_security_identity_version,
        composition_hash_version=COMPOSITION_HASH_VERSION,
    )
    return {
        "report_date": effective_base["report_date"],
        "filing_date": latest.get("filing_date"),
        "accession": latest["accession"],
        "total_value": total_value,
        "num_holdings": len(holdings),
        "holdings": holdings,
        "composition_version": AMENDMENT_REDUCER_VERSION,
        "composition_hash_version": COMPOSITION_HASH_VERSION,
        **(
            {"security_identity_version": SECURITY_IDENTITY_VERSION}
            if composed_security_identity_version
            == SECURITY_IDENTITY_VERSION
            else {}
        ),
        "is_complete": True,
        "base_accession": effective_base["accession"],
        "applied_accessions": applied_accessions,
        "source_filings": source_filings,
        **(
            {"reported_identity_sources": reported_identity_sources}
            if reported_identity_sources
            else {}
        ),
        "composition_hash": composition_hash,
    }


def _classify_holding(h: dict) -> str:
    """Classify a holding into EQUITY, CALL, PUT, OPT, PREF, NOTE, or WARRANT
    based on the 13F titleOfClass field and putCall XML field."""
    cls = (h.get("class") or "").upper().strip()
    issuer = (h.get("issuer") or "").upper()
    combined = issuer + " " + cls

    # --- Options (most specific first) ---
    pc = h.get("put_call", "")
    if pc in ("PUT", "CALL"):
        return pc
    if cls in ("CALL", "PUT"):
        return cls
    if cls in _OPTION_CLASS_EXACT:
        return "OPT"

    # --- Warrants ---
    if (
        cls in (
            "WARR",
            "WARRANT",
            "WARRANTS",
            "WT",
            "WTS",
        )
        or "WARRANT" in cls
        or cls.startswith("WT ")
        or cls.endswith(" WT")
        or cls.startswith("*W EXP ")
        or cls.startswith("*WEXP ")
    ):
        return "WARRANT"

    # --- Preferred (but not ETFs that hold preferreds) ---
    if any(kw in cls for kw in _PREFERRED_KEYWORDS):
        if not any(hint in cls for hint in _IS_ETF_HINT):
            return "PREF"

    # Explicit stock/fund tokens take precedence over descriptive debt words.
    # Keep this after warrant/preferred recognition because their SEC class
    # names commonly include STOCK or SHARES.
    if _has_explicit_equity_class(h):
        return "EQUITY"

    # --- Notes / Convertible bonds ---
    cusip = normalize_security_identifier(h.get("cusip"))
    if cls == "US TREASURY" and cusip.startswith("912"):
        return "NOTE"
    if cls in _NOTE_CLASS_EXACT:
        return "NOTE"
    if any(kw in cls for kw in _NOTE_KEYWORDS):
        if not any(hint in cls for hint in _IS_ETF_HINT):
            return "NOTE"
    if re.search(r"\d+\.\d+%", cls):
        return "NOTE"
    if _has_note_coupon_date_evidence(h):
        return "NOTE"

    # --- Options (generic text heuristics after debt/pref/warrant guards) ---
    if not any(excl in combined for excl in _NOT_OPTION_KEYWORDS):
        prefix = _OPTION_ISSUER_PREFIX_RE.match(issuer)
        if prefix:
            return prefix.group(1)
        if " PUT OPT" in issuer or " PUT " in issuer.split("INC")[0]:
            return "PUT"
        if " CALL OPT" in issuer or " CALL " in issuer.split("INC")[0]:
            return "CALL"
    return "EQUITY"


_HOLDING_TYPE_PRIORITY = {
    "EQUITY": 0,
    "PREF": 1,
    "NOTE": 2,
    "WARRANT": 3,
    "CALL": 4,
    "PUT": 4,
    "OPT": 4,
}


def classify_saved_holding(
    h: dict,
    *,
    allow_missing_option_side_reclassification: bool = False,
) -> str:
    """Reclassify a persisted row while preserving truly legacy options.

    Before April 16, 2026 the parser saved CALL/PUT in ``holding_type`` but
    did not retain the raw SEC ``putCall`` field. Callers may reclassify a
    missing-side option label only when all contributing filings are known to
    postdate that parser change. Ambiguous historical NOTE/PREF/WARRANT labels
    are also retained unless the raw class contains positive equity evidence.
    """
    old_type = normalize_instrument_type(
        h.get("holding_type") or h.get("option_type")
    )
    new_type = _classify_holding(h)
    if (
        old_type in {"CALL", "PUT", "OPT"}
        and not h.get("put_call")
    ):
        security_class = str(h.get("class") or "").strip().upper()
        if security_class in {"CALL", "PUT"}:
            return security_class
        if not allow_missing_option_side_reclassification:
            return old_type
        # Parser-era proof establishes that this is not a sided option, but
        # broad debt wording is still not enough to distinguish an underlying
        # equity (for example QQQ classed "CONV BONDS") from a true note.
        if new_type == "NOTE":
            return "EQUITY"
    if (
        new_type == "EQUITY"
        and old_type in {"NOTE", "PREF", "WARRANT"}
        and not _has_explicit_equity_class(h)
    ):
        return old_type
    return new_type


def has_unsafe_legacy_option_identity(holding: dict) -> bool:
    """Whether a persisted option label conflicts with current raw-row rules.

    A missing ``put_call`` is not enough by itself: an exact CALL/PUT class or
    a stable generic option class is already direct SEC evidence. Migration is
    limited to rows where the saved option type would actually change once
    current-parser provenance allows deterministic reclassification.
    """
    saved_type = normalize_instrument_type(
        holding.get("holding_type") or holding.get("option_type")
    )
    put_call = str(holding.get("put_call") or "").strip().upper()
    if saved_type not in {"CALL", "PUT", "OPT"}:
        return False
    if put_call in {"CALL", "PUT"}:
        return False
    return (
        classify_saved_holding(
            holding,
            allow_missing_option_side_reclassification=True,
        )
        != saved_type
    )


def _has_explicit_equity_class(holding: dict) -> bool:
    security_class = " ".join(
        str(holding.get("class") or "").upper().split()
    )
    if _has_note_coupon_date_evidence(holding):
        return False
    if (
        _FUND_EQUITY_CLASS_TOKEN_RE.search(security_class)
        or "EXCHANGE TRADED" in security_class
        or "MF CLOSED" in security_class
    ):
        return True
    if (
        "DEP SHS" in security_class
        or security_class in _NOTE_CLASS_EXACT
        or any(keyword in security_class for keyword in _NOTE_KEYWORDS)
        or re.search(r"\d+\.\d+%", security_class)
    ):
        return False
    return bool(_COMMON_EQUITY_CLASS_TOKEN_RE.search(security_class))


_PUT_CALL_PERSISTED_ON = date(2026, 4, 16)


def _filing_retains_raw_put_call(filing_date: object) -> bool:
    """Whether a filing was first parsed after raw option-side persistence."""
    raw = str(filing_date or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        try:
            return date.fromisoformat(raw) > _PUT_CALL_PERSISTED_ON
        except ValueError:
            return False
    month_match = re.fullmatch(r"(\d{4})-(\d{2})", raw)
    if month_match:
        return (int(month_match.group(1)), int(month_match.group(2))) > (2026, 4)
    return False


def _quarter_retains_raw_put_call(quarter: dict) -> bool:
    """Whether every known filing contributing rows retained raw option side."""
    source_filings = quarter.get("source_filings")
    if "source_filings" in quarter:
        if not isinstance(source_filings, list) or not source_filings:
            return False
        applied_sources = [
            source
            for source in source_filings
            if isinstance(source, dict) and source.get("applied") is True
        ]
        if not applied_sources:
            return False
        applied_accessions = quarter.get("applied_accessions")
        if not isinstance(applied_accessions, list):
            return False
        source_accessions = [
            source.get("accession")
            for source in applied_sources
            if source.get("accession")
        ]
        if (
            len(source_accessions) != len(applied_sources)
            or len(applied_accessions) != len(set(applied_accessions))
            or source_accessions != applied_accessions
        ):
            return False
        has_identity_marker = (
            "security_identity_version" in quarter
            or any(
                "security_identity_version" in source
                for source in applied_sources
            )
        )
        if has_identity_marker:
            return (
                quarter.get("security_identity_version")
                == SECURITY_IDENTITY_VERSION
                and all(
                    source.get("security_identity_version")
                    == SECURITY_IDENTITY_VERSION
                    for source in applied_sources
                )
            )
        return all(
            _filing_retains_raw_put_call(source.get("filing_date"))
            for source in applied_sources
        )

    applied_accessions = quarter.get("applied_accessions")
    if isinstance(applied_accessions, list) and len(applied_accessions) > 1:
        return False
    if "security_identity_version" in quarter:
        return False
    # Without per-source provenance, only a post-cutover report period proves
    # that every possible original/amendment filing was parsed after cutover.
    return _filing_retains_raw_put_call(quarter.get("report_date"))


def _canonical_holding_type_for_quarter(
    quarter: dict,
    holding: dict,
) -> str:
    """Return the type a derived-output rebuild may safely publish.

    Composition hash v2 binds the parser-backed holding type.  Registry and
    ticker rebuilds may refresh display metadata, but they must not rewrite
    that immutable source identity or silently invalidate its hash.  Legacy
    quarters remain eligible for the existing evidence-gated normalization.
    """
    if quarter.get("composition_hash_version") == COMPOSITION_HASH_VERSION:
        return holding_instrument_type(holding)

    allow_missing_option_side_reclassification = (
        _quarter_retains_raw_put_call(quarter)
    )
    saved_type = normalize_instrument_type(
        holding.get("holding_type") or holding.get("option_type")
    )
    marker_backed_identity = (
        quarter.get("security_identity_version")
        == SECURITY_IDENTITY_VERSION
        and allow_missing_option_side_reclassification
    )
    if (
        marker_backed_identity
        and saved_type in {"CALL", "PUT", "OPT"}
        and not holding.get("put_call")
    ):
        # Current-parser provenance is the durable option-side proof after
        # registry canonicalization replaces the filer issuer text that
        # originally supplied it.
        return saved_type
    return classify_saved_holding(
        holding,
        allow_missing_option_side_reclassification=(
            allow_missing_option_side_reclassification
        ),
    )


def quarter_has_unsafe_legacy_option_identity(quarter: dict) -> bool:
    """Whether a quarter needs SEC replay to establish position identity.

    Registry canonicalization intentionally replaces the filer-supplied issuer
    text used by the parser. Row-level reclassification after that display
    rewrite can therefore disagree with a legitimate CALL/PUT classification.
    A complete current-parser marker is the durable proof for those quarters;
    only unproved quarters are migration candidates.
    """
    if not isinstance(quarter, dict) or _quarter_retains_raw_put_call(quarter):
        return False
    holdings = quarter.get("holdings")
    return isinstance(holdings, list) and any(
        isinstance(holding, dict)
        and has_unsafe_legacy_option_identity(holding)
        for holding in holdings
    )


def find_ambiguous_ticker_cusips(holdings: list[dict]) -> set[str]:
    """CUSIPs that collide under the same ticker + instrument type."""
    by_key: dict[tuple[str, str], set[str]] = defaultdict(set)
    for h in holdings:
        ticker = str(h.get("ticker") or "").strip().upper()
        cusip = str(h.get("cusip") or "").strip().upper()
        if not (ticker and cusip):
            continue
        holding_type = classify_saved_holding(h)
        by_key[(ticker, holding_type)].add(cusip)
    ambiguous: set[str] = set()
    for cusips in by_key.values():
        if len(cusips) > 1:
            ambiguous.update(cusips)
    return ambiguous


def _collect_zero_share_price_references(
    quarter: dict,
    by_report_position: dict[tuple[str, str, str], list[float]],
    by_position: dict[tuple[str, str], list[float]],
) -> None:
    """Record non-derived per-share prices from one materialized quarter."""
    report_date = quarter.get("report_date") or ""
    for holding in quarter.get("holdings", []):
        cusip = str(holding.get("cusip") or "").strip().upper()
        holding_type = classify_saved_holding(holding)
        value = holding.get("value") or 0
        shares = holding.get("shares") or 0
        if (
            not cusip
            or value <= 0
            or shares <= 0
            or "shares_imputed" in holding
        ):
            continue
        price = value / shares
        if price <= 0:
            continue
        if report_date:
            by_report_position[(report_date, cusip, holding_type)].append(price)
        by_position[(cusip, holding_type)].append(price)


def _median_zero_share_price_references(
    by_report_position: dict[tuple[str, str, str], list[float]],
    by_position: dict[tuple[str, str], list[float]],
) -> tuple[
    dict[tuple[str, str, str], float],
    dict[tuple[str, str], float],
]:
    return (
        {
            key: statistics.median(values)
            for key, values in by_report_position.items()
            if values
        },
        {
            key: statistics.median(values)
            for key, values in by_position.items()
            if values
        },
    )


def build_zero_share_price_reference_maps() -> tuple[
    dict[tuple[str, str, str], float],
    dict[tuple[str, str], float],
]:
    """Build price medians by quarter and canonical public position."""
    by_report_position: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    by_position: dict[tuple[str, str], list[float]] = defaultdict(list)

    if not FUNDS_DIR.exists():
        return {}, {}

    for fp in sorted(FUNDS_DIR.glob("*.json")):
        try:
            with open(fp) as f:
                fund = json.load(f)
        except json.JSONDecodeError:
            continue
        for quarter in fund.get("quarters", []):
            _collect_zero_share_price_references(
                quarter,
                by_report_position,
                by_position,
            )

    return _median_zero_share_price_references(
        by_report_position,
        by_position,
    )


class CusipRegistry(dict[str, dict]):
    """Registry result carrying the CUSIP set observed during its build."""

    def __init__(
        self,
        registry: dict[str, dict] | None = None,
        *,
        observed_cusips: set[str] | frozenset[str] = frozenset(),
    ) -> None:
        super().__init__(registry or {})
        self.observed_cusips = frozenset(observed_cusips)


_SEC_REGISTRY_MAPPING_STATUSES = frozenset({
    "resolved",
    "unresolved",
    "ambiguous",
    "no_listed_symbol",
    "malformed_as_filed",
})
_SEC_REGISTRY_TICKER_SOURCES = frozenset({
    "sec_ftd",
    "sec_ixbrl",
})
_PRIVATE_MASTER_FIELDS = frozenset({
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
    "resolution_reason",
    "sec_edgar_evidence",
    "symbol_evidence",
    "symbol_intervals",
    "symbol_validation_exchanges",
    "symbol_validation_sources",
    "symbol_validation_titles",
})


def _dominant_registry_value(values: dict | None) -> str:
    if not values:
        return ""
    return max(
        values,
        key=lambda value: (values.get(value, 0), str(value)),
        default="",
    )


def _raw_registry_instrument_type(rec: dict) -> str:
    """Choose an exact row type without consulting tickers or old metadata."""

    values = rec.get("instrument_type_value") or {}
    counts = rec.get("instrument_type_count") or {}
    non_options = {
        normalize_instrument_type(kind)
        for kind, count in counts.items()
        if count and normalize_instrument_type(kind)
        not in {"CALL", "PUT", "OPT"}
    }
    if non_options:
        return max(
            non_options,
            key=lambda kind: (
                values.get(kind, 0),
                counts.get(kind, 0),
                -_HOLDING_TYPE_PRIORITY.get(kind, 99),
                kind,
            ),
        )

    options = {
        normalize_instrument_type(kind)
        for kind, count in counts.items()
        if count and normalize_instrument_type(kind)
        in {"CALL", "PUT", "OPT"}
    }
    if options:
        return max(
            options,
            key=lambda kind: (
                values.get(kind, 0),
                counts.get(kind, 0),
                {"OPT": 0, "PUT": 1, "CALL": 2}[kind],
            ),
        )

    # Older fixture objects can predate per-row type counters.
    call_value = (rec.get("put_call_value") or {}).get("CALL", 0)
    put_value = (rec.get("put_call_value") or {}).get("PUT", 0)
    if call_value or put_value:
        return "CALL" if call_value >= put_value else "PUT"
    return normalize_instrument_type(_classify_holding({
        "issuer": _dominant_registry_value(rec.get("issuer_value")),
        "class": _dominant_registry_value(rec.get("class_value")),
    }))


def _master_record_issuer(record: dict, fallback: str) -> tuple[str, str]:
    identifier = normalize_security_identifier(record.get("cusip"))
    official = record.get("official_13f")
    if isinstance(official, dict):
        for row in official.get("records") or []:
            issuer = normalize_security_label(
                row.get("issuer"), identifier=identifier
            )
            if issuer:
                return issuer, "sec_13f_list"
    for value in (
        record.get("reported_issuer"),
        *((record.get("reported_issuers") or [])),
    ):
        issuer = normalize_security_label(value, identifier=identifier)
        if issuer:
            return issuer, "sec_13f_filer_consensus"
    issuer = normalize_security_label(fallback, identifier=identifier)
    if issuer:
        return issuer, "sec_13f_filer_consensus"
    return "", ""


def _master_record_class(record: dict, fallback: str) -> str:
    official = record.get("official_13f")
    if isinstance(official, dict):
        for row in official.get("records") or []:
            description = normalize_security_label(row.get("description"))
            if description:
                return description
    for value in (
        record.get("reported_class"),
        *((record.get("reported_classes") or [])),
    ):
        security_class = normalize_security_label(value)
        if security_class:
            return security_class
    return normalize_security_label(fallback) or ""


def _official_master_class(record: dict) -> str | None:
    official = record.get("official_13f")
    if not isinstance(official, dict) or official.get("status") != "active":
        return None
    descriptions = {
        normalize_security_label(row.get("description"))
        for row in official.get("records") or []
        if isinstance(row, dict)
        and row.get("status") != "*D*"
        and normalize_security_label(row.get("description"))
    }
    return next(iter(descriptions)) if len(descriptions) == 1 else None


def _registry_instrument_type_from_master(
    raw_type: str,
    resolution: dict,
) -> str:
    """Let one exact official-list class correct a broad non-option parse."""

    normalized_raw = normalize_instrument_type(raw_type)
    if normalized_raw in {"CALL", "PUT", "OPT"}:
        return normalized_raw
    official_class = _official_master_class(resolution)
    if not official_class:
        return normalized_raw
    return _classify_holding({
        "class": official_class,
        "issuer": resolution.get("issuer") or "",
        "put_call": "",
    })


def _fallback_registry_label(
    *,
    cusip: str,
    issuer: str,
    security_class: str,
    instrument_type: str,
) -> str:
    base = compose_security_label(
        issuer,
        security_class,
        instrument_type,
        identifier=cusip,
    )
    if is_synthetic_identifier(cusip) and base == f"{instrument_type} SECURITY":
        base = f"UNIDENTIFIED {instrument_type} SECURITY"
    # The final fail-closed label includes the as-filed identifier so even an
    # unresolved security remains unambiguous to the reader.
    return normalize_security_label(f"{base} — {cusip}", identifier=cusip) or base


def _registry_position_ticker(entry: dict | None, instrument_type: str) -> str | None:
    """Return only an exact ticker or an explicitly proven option underlying."""

    if not isinstance(entry, dict):
        return None
    normalized_type = normalize_instrument_type(instrument_type)
    if normalized_type in {"CALL", "PUT", "OPT"}:
        ticker = entry.get("underlying_ticker")
    else:
        raw_entry_type = entry.get("type")
        if (
            not isinstance(raw_entry_type, str)
            or raw_entry_type.strip().upper() not in VALID_INSTRUMENT_TYPES
        ):
            return None
        entry_type = normalize_instrument_type(raw_entry_type)
        if normalized_type != entry_type:
            return None
        ticker = entry.get("ticker")
    return display_ticker_for_holding_type(ticker, normalized_type)


def _resolve_loaded_security(
    master: dict,
    cusip: object,
    instrument_type: object = "EQUITY",
) -> dict:
    """Resolve from an already-validated master without validating it again.

    ``load_security_master`` and every refresh validate the complete document.
    Re-running that whole-master validation for every holding turns a rewrite of
    millions of positions into quadratic work.  The compatibility fallback is
    retained for lightweight test doubles that do not expose ``records``.
    """

    records = master.get("records") if isinstance(master, dict) else None
    if not isinstance(records, dict):
        return resolve_security(master, cusip, instrument_type)
    normalized_cusip = normalize_security_identifier(cusip)
    normalized_type = normalize_instrument_type(instrument_type)
    entry = records.get(security_key(normalized_cusip, normalized_type))
    if isinstance(entry, dict):
        return dict(entry)
    quarantine_reason = cusip_quarantine_reason(normalized_cusip)
    return {
        "cusip": normalized_cusip,
        "instrument_type": normalized_type,
        "mapping_status": (
            "malformed_as_filed" if quarantine_reason else "unresolved"
        ),
        "ticker": None,
        "ticker_source": None,
        "ticker_as_of": None,
        "resolution_reason": quarantine_reason or "security_not_in_master",
        "symbol_evidence": [],
    }


def build_cusip_registry(
    *,
    full_refresh: bool = False,
    company_ticker_data: dict | list | None = None,
    refresh_official_fund_names: bool | None = None,
) -> CusipRegistry:
    """Build the public registry exclusively from exact SEC-master evidence."""

    # Retained for call-site compatibility. SEC fund product names now come
    # only from checksummed fund-series evidence embedded in the master.
    _ = (full_refresh, refresh_official_fund_names)
    log.info("Building SEC-only CUSIP registry...")
    if not FUNDS_DIR.exists():
        log.info("  no funds directory; skipping registry build")
        return CusipRegistry()

    evidence = _aggregate_cusip_evidence()
    master = load_security_master(SEC_SECURITY_MASTER_PATH)
    # Current company/fund symbols and titles are already checksum-bound in the
    # private SEC source state and copied onto each resolved master record.
    # The optional payload remains only for callers/tests that already have it;
    # rebuilding derived data never performs a second implicit network fetch.
    sec_titles = sec_ticker_titles(company_ticker_data or {})
    registry: dict[str, dict] = {}

    for cusip, rec in sorted(evidence.items()):
        dominant_issuer = _dominant_registry_value(rec.get("issuer_value"))
        dominant_class = _dominant_registry_value(rec.get("class_value"))
        raw_instrument_type = _raw_registry_instrument_type(rec)
        resolution = _resolve_loaded_security(
            master,
            cusip,
            raw_instrument_type,
        )
        instrument_type = _registry_instrument_type_from_master(
            raw_instrument_type,
            resolution,
        )
        if instrument_type != raw_instrument_type:
            resolution = _resolve_loaded_security(
                master,
                cusip,
                instrument_type,
            )

        mapping_status = resolution.get("mapping_status")
        if mapping_status not in _SEC_REGISTRY_MAPPING_STATUSES:
            mapping_status = "unresolved"
        ticker = (
            str(resolution.get("ticker") or "").strip().upper() or None
            if mapping_status == "resolved"
            else None
        )
        ticker_source = (
            resolution.get("ticker_source") if ticker else None
        )
        ticker_as_of = resolution.get("ticker_as_of") if ticker else None

        exact_issuer, issuer_source = _master_record_issuer(
            resolution,
            dominant_issuer,
        )
        exact_class = _master_record_class(resolution, dominant_class)
        sources: list[str] = []
        if issuer_source:
            sources.append(issuer_source)
        name = exact_issuer
        validated_titles = [
            normalize_security_label(title)
            for title in resolution.get("symbol_validation_titles", [])
            if normalize_security_label(title)
        ]
        validated_title = (
            sorted(set(validated_titles), key=lambda value: (len(value), value))[0]
            if validated_titles
            else normalize_security_label(sec_titles.get(ticker))
            if ticker
            else None
        )
        if not name and validated_title:
            name = validated_title
            sources.append("sec_company_tickers")
        if not name:
            name = f"UNIDENTIFIED {instrument_type} SECURITY"
            sources.append("sec_13f_filer_consensus")
        if ticker_source and ticker_source not in sources:
            sources.append(ticker_source)
        for validation_source in resolution.get(
            "symbol_validation_sources",
            [],
        ):
            public_source = {
                "sec_company_exchange_tickers": "sec_company_tickers",
                "sec_fund_tickers": "sec_fund_series",
            }.get(validation_source, validation_source)
            if public_source in {
                "sec_company_tickers",
                "sec_fund_series",
            }:
                sources.append(public_source)

        master_label = normalize_security_label(
            resolution.get("security_label"),
            identifier=cusip,
        )
        if master_label and master_label.partition(" — ")[0].upper() == cusip:
            # A filer can put its CUSIP in the issuer field. Prefer the safe
            # issuer selected above over carrying that identifier into the
            # public display label as though it were a company name.
            master_label = None
        if master_label:
            security_label = master_label
            label_source = str(
                resolution.get("security_label_source") or "sec_13f_list"
            )
        else:
            security_label = _fallback_registry_label(
                cusip=cusip,
                issuer=name,
                security_class=exact_class,
                instrument_type=instrument_type,
            )
            label_source = (
                issuer_source or "sec_13f_filer_consensus"
                if dominant_issuer or dominant_class
                else "synthetic_identifier"
            )

        entry = {
            "ticker": ticker,
            "ticker_source": ticker_source,
            "ticker_as_of": ticker_as_of,
            "mapping_status": mapping_status,
            "name": name,
            "type": instrument_type,
            "dominant_issuer": dominant_issuer,
            "dominant_class": dominant_class,
            "holder_count": len(rec.get("holder_ciks") or ()),
            "total_value": rec.get("total_value", 0),
            "first_seen": rec.get("first_seen") or "",
            "last_seen": rec.get("last_seen") or "",
            "security_label": security_label,
            "label_source": label_source,
            "sources": sorted(set(sources)),
        }

        official_class = _official_master_class(resolution)
        kind = {
            "NOTE": "BOND",
            "PREF": "PREFERRED",
            "WARRANT": "WARRANT",
        }.get(instrument_type)
        kind_source = (
            "sec_13f_list"
            if kind and official_class
            else "sec_13f_filer_consensus"
            if kind
            else None
        )
        if kind is None:
            classification_entry = (
                {**entry, "dominant_class": official_class}
                if official_class
                else entry
            )
            kind = _filer_security_kind(classification_entry)
            if kind in {"ETF", "MUTUAL FUND", "CLOSED-END FUND"} and (
                "sec_fund_series" in sources
            ):
                kind_source = "sec_fund_series"
            elif kind and official_class:
                kind_source = "sec_13f_list"
            elif kind == "COMMON" and "sec_company_tickers" in sources:
                kind_source = "sec_company_tickers"
            else:
                kind_source = "filer_metadata" if kind else None
        if kind:
            entry["security_kind"] = kind
            entry["security_kind_source"] = kind_source

        observed_option_types = {
            normalize_instrument_type(kind)
            for kind, count in (
                rec.get("instrument_type_count") or {}
            ).items()
            if count
            and normalize_instrument_type(kind) in {"CALL", "PUT", "OPT"}
        }
        if observed_option_types:
            underlying = _resolve_loaded_security(master, cusip, "EQUITY")
            if underlying.get("mapping_status") == "resolved":
                entry.update({
                    "underlying_ticker": underlying.get("ticker"),
                    "underlying_ticker_source": underlying.get("ticker_source"),
                    "underlying_ticker_as_of": underlying.get("ticker_as_of"),
                })
        fund_series_name = normalize_security_label(
            resolution.get("fund_series_name"),
            identifier=cusip,
        )
        if (
            fund_series_name
            and normalize_security_kind(entry.get("security_kind"))
            in {"ETF", "MUTUAL FUND", "CLOSED-END FUND"}
        ):
            entry["product_name"] = fund_series_name
            entry["product_name_source"] = "sec_fund_series"
            entry["sources"] = sorted({
                *(entry.get("sources") or []),
                "sec_fund_series",
            })
        registry[cusip] = entry

    save_cusip_registry(registry)
    resolved = sum(entry["mapping_status"] == "resolved" for entry in registry.values())
    log.info(
        "  wrote %s SEC-only entries (%s resolved, %s tickerless)",
        len(registry),
        resolved,
        len(registry) - resolved,
    )
    return CusipRegistry(registry, observed_cusips=set(evidence))


def validate_cusip_registry(
    *,
    current_cusips: set[str] | frozenset[str] | None = None,
) -> list[str]:
    """Validate exact SEC provenance and fail-closed public metadata."""

    issues: list[str] = []
    registry = load_cusip_registry()
    if not registry:
        issues.append("registry is empty")
    master = load_security_master(SEC_SECURITY_MASTER_PATH)
    master_records = master.get("records") or {}

    if not LEGACY_CUSIP_REGISTRY_PATH.exists():
        issues.append(
            f"snapshot data copy missing at {LEGACY_CUSIP_REGISTRY_PATH.name}"
        )
    else:
        snapshot_registry = _read_json_object(LEGACY_CUSIP_REGISTRY_PATH)
        if snapshot_registry != registry:
            issues.append(
                f"snapshot data copy {LEGACY_CUSIP_REGISTRY_PATH.name} differs "
                "from cache registry"
            )

    missing_master: list[str] = []
    mismatched_master: list[str] = []
    unsafe_entries: list[str] = []
    private_leaks: list[str] = []
    for cusip, entry in registry.items():
        if not isinstance(entry, dict):
            unsafe_entries.append(cusip)
            continue
        key = f"{normalize_security_identifier(cusip)}|{normalize_instrument_type(entry.get('type'))}"
        master_entry = master_records.get(key)
        if not isinstance(master_entry, dict):
            missing_master.append(key)
        elif any(
            entry.get(field) != master_entry.get(field)
            for field in (
                "mapping_status",
                "ticker",
                "ticker_source",
                "ticker_as_of",
            )
        ):
            mismatched_master.append(key)
        elif entry.get("product_name_source") == "sec_fund_series" and (
            entry.get("product_name") != master_entry.get("fund_series_name")
        ):
            mismatched_master.append(key)

        status = entry.get("mapping_status")
        ticker = entry.get("ticker")
        source = entry.get("ticker_source")
        as_of = entry.get("ticker_as_of")
        resolved_ok = (
            status == "resolved"
            and isinstance(ticker, str)
            and bool(_SEC_PLAIN_TICKER_RE.fullmatch(ticker))
            and source in _SEC_REGISTRY_TICKER_SOURCES
            and isinstance(as_of, str)
            and bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", as_of))
        )
        unresolved_ok = (
            status in _SEC_REGISTRY_MAPPING_STATUSES - {"resolved"}
            and ticker is None
            and source is None
            and as_of is None
        )
        safe_label = normalize_security_label(
            entry.get("security_label"), identifier=cusip
        )
        if not (
            resolved_ok or unresolved_ok
        ) or not entry.get("name") or not safe_label or not entry.get("label_source"):
            unsafe_entries.append(cusip)
        if _PRIVATE_MASTER_FIELDS & set(entry):
            private_leaks.append(cusip)

        if normalize_instrument_type(entry.get("type")) == "NOTE" and ticker:
            unsafe_entries.append(cusip)
        underlying = entry.get("underlying_ticker")
        if underlying:
            proof = master_records.get(f"{cusip}|EQUITY") or {}
            if any(
                entry.get(public_field) != proof.get(master_field)
                for public_field, master_field in (
                    ("underlying_ticker", "ticker"),
                    ("underlying_ticker_source", "ticker_source"),
                    ("underlying_ticker_as_of", "ticker_as_of"),
                )
            ) or proof.get("mapping_status") != "resolved":
                unsafe_entries.append(cusip)

    for label, values in (
        ("entries absent from the exact SEC master", missing_master),
        ("entries that differ from exact SEC master provenance", mismatched_master),
        ("unsafe or incomplete registry entries", unsafe_entries),
        ("entries leaking private SEC evidence", private_leaks),
    ):
        if values:
            issues.append(f"{len(set(values))} {label}; samples: {sorted(set(values))[:5]}")

    if current_cusips is None:
        current_cusips = set(_aggregate_cusip_evidence())
    missing_current = sorted(set(current_cusips) - set(registry))
    if missing_current:
        issues.append(
            f"{len(missing_current)} fund-file CUSIPs missing from registry; "
            f"samples: {missing_current[:5]}"
        )

    for cusip, ticker in (
        ("037833100", "AAPL"),
        ("30231G102", "XOM"),
        ("76954A103", "RIVN"),
    ):
        entry = registry.get(cusip)
        if entry is None:
            issues.append(f"required safety-case equity {cusip} is missing")
        elif entry.get("ticker") != ticker:
            issues.append(f"{cusip} should resolve to ticker {ticker}")
    for cusip in ("76954AAD5", "090043AF7", "26210CAC8", "26210CAD6"):
        entry = registry.get(cusip)
        if entry is None:
            issues.append(f"required safety-case debt {cusip} is missing")
        elif (
            normalize_instrument_type(entry.get("type")) != "NOTE"
            or normalize_security_kind(entry.get("security_kind")) != "BOND"
            or entry.get("mapping_status") == "resolved"
            or any(
                entry.get(field) is not None
                for field in ("ticker", "ticker_source", "ticker_as_of")
            )
        ):
            issues.append(
                f"{cusip} is debt and must remain a tickerless NOTE/BOND"
            )
    return issues


@_serialize_pipeline_maintenance
def repair_zero_share_holdings_in_place() -> int:
    """Apply the evidence-bound quantity policy without changing reported data."""
    from quantity_estimation import apply_plan, build_plan, cache_dir_for_funds

    if not FUNDS_DIR.exists():
        return 0
    cache = cache_dir_for_funds(FUNDS_DIR)
    evidence_path = cache / "quantity_estimation_evidence.json"
    plan = build_plan(
        FUNDS_DIR,
        evidence_path=evidence_path,
        market_path=cache / "quarter_close_prices.json",
    )
    result = apply_plan(
        plan,
        FUNDS_DIR,
        evidence_path=evidence_path,
        request_path=cache / "quarter_close_price_requests.json",
    )
    log.info("Quantity policy: %s", result)
    return result["estimated_rows"]


def parse_information_table(
    xml_bytes: bytes,
    *,
    accession: str | None = None,
    report_date: str | None = None,
) -> list[dict] | None:
    """Parse the 13F informationTable XML into a list of holding dicts.

    Retain SEC-reported identity fields alongside mutable display metadata so
    later canonicalization cannot erase the source row. Filing provenance is
    attached when the caller supplies it.

    Returns None if the XML doesn't appear to be an information table."""
    try:
        tree = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return None

    holdings: list[dict] = []
    for el in tree.iter():
        # Skip XML comments / processing instructions — their .tag is not a string.
        if not isinstance(el.tag, str):
            continue
        if etree.QName(el.tag).localname != "infoTable":
            continue

        h: dict = {}
        for child in el.iter():
            if not isinstance(child.tag, str):
                continue
            ctag = etree.QName(child.tag).localname
            text = (child.text or "").strip()
            if not text:
                continue
            if ctag == "nameOfIssuer":
                h["issuer"] = text
            elif ctag == "titleOfClass":
                h["class"] = text
            elif ctag == "cusip":
                h["reported_cusip"] = text
                h["cusip"] = text.upper()
            elif ctag.casefold() == "figi":
                h["reported_figi"] = text
            elif ctag == "value":
                parsed_value = parse_numeric_text(text)
                if parsed_value is not None:
                    h["value"] = parsed_value
            elif ctag == "sshPrnamt":
                parsed_shares = parse_numeric_text(text, allow_float=True)
                if parsed_shares is not None:
                    h["shares"] = parsed_shares
            elif ctag == "sshPrnamtType":
                h["share_amount_type"] = text.upper()
            elif ctag == "putCall":
                h["put_call"] = text.upper()
            elif ctag == "investmentDiscretion":
                h["investment_discretion"] = text.upper()
            elif ctag == "otherManager":
                h["other_manager"] = text

        cusip = h.get("cusip", "")
        if not cusip:
            continue
        # SEC confidential-treatment filings sometimes contain one all-zero
        # dummy row to represent an empty portfolio.  Suppress only that exact
        # zero-value placeholder.  Every nonzero malformed or synthetic value
        # remains visible as filed and is quarantined by the security master.
        empty_placeholder = (
            cusip in {"000000000", "000000NAN", "N/A"}
            and int(h.get("value", 0) or 0) == 0
            and str(h.get("issuer") or "").strip().upper() in {"", "N/A", "NONE"}
            and str(h.get("class") or "").strip().upper() in {"", "N/A", "NONE"}
        )
        if empty_placeholder:
            continue
        if "cusip" in h and "value" in h:
            holding_type = _classify_holding(h)
            entry = {
                "ticker": None,
                "issuer": h.get("issuer", ""),
                "cusip": h["cusip"],
                "class": h.get("class", ""),
                "reported_issuer": h.get("issuer", ""),
                "reported_class": h.get("class", ""),
                "reported_cusip": h.get("reported_cusip", h["cusip"]),
                "value": h["value"],
                "shares": h.get("shares", 0),
                "holding_type": holding_type,
            }
            if h.get("reported_figi"):
                entry["reported_figi"] = h["reported_figi"]
            if accession:
                entry["accession"] = str(accession).strip()
            if report_date:
                entry["report_date"] = str(report_date).strip()
            if h.get("share_amount_type"):
                entry["share_amount_type"] = h["share_amount_type"]
            if h.get("put_call"):
                entry["put_call"] = h["put_call"]
            if h.get("investment_discretion"):
                entry["investment_discretion"] = h["investment_discretion"]
            if h.get("other_manager"):
                entry["other_manager"] = h["other_manager"]
            holdings.append(entry)

    return holdings if holdings else None


# ----------------------------------------------------------------------------
# CUSIP -> ticker mapping
# ----------------------------------------------------------------------------

# Common 13F abbreviations that need expansion to match company_tickers titles
NAME_ABBREV: dict[str, str] = {
    "FINL": "FINANCIAL",
    "INTL": "INTERNATIONAL",
    "NATL": "NATIONAL",
    "MFG": "MANUFACTURING",
    "INDS": "INDUSTRIES",
    "SVCS": "SERVICES",
    "SVC": "SERVICE",
    "SYS": "SYSTEMS",
    "HLTH": "HEALTH",
    "ELEC": "ELECTRIC",
    "COMMS": "COMMUNICATIONS",
    "COMMUN": "COMMUNICATIONS",
    "PROPS": "PROPERTIES",
    "RLTY": "REALTY",
    "PETE": "PETROLEUM",
    "PETRO": "PETROLEUM",
    "PAC": "PACIFIC",
    "AMER": "AMERICAN",
    "MED": "MEDICAL",
    "CHEM": "CHEMICAL",
    "PHARM": "PHARMACEUTICAL",
    "BRDCSTG": "BROADCASTING",
    "COS": "COMPANIES",
    "ENTMT": "ENTERTAINMENT",
    "ASSN": "ASSOCIATION",
    "TECHS": "TECHNOLOGIES",
    "RESTS": "RESTAURANTS",
    "ENTPRS": "ENTERPRISES",
    "ENTPRISES": "ENTERPRISES",
    "MGMT": "MANAGEMENT",
    # 13F filings truncate names at ~30 chars — these are common truncations
    "WHSL": "WHOLESALE",
    "MACHS": "MACHINES",
    "MATLS": "MATERIALS",
    "SOFTWAR": "SOFTWARE",
    "TECHNOL": "TECHNOLOGIES",
    "MO": "MISSOURI",
    "SEMICONDUC": "SEMICONDUCTOR",
    "SEMICOND": "SEMICONDUCTOR",
    "BANCSH": "BANCSHARES",
    "INVT": "INVESTMENT",
    "INVMT": "INVESTMENT",
    "PRODS": "PRODUCTS",
    "RES": "RESOURCES",
    "RESH": "RESEARCH",
    "INDUS": "INDUSTRIAL",
    "FDS": "FUNDS",
    "TR": "TRUST",
    "EXCH": "EXCHANGE",
    "EXCHNG": "EXCHANGE",
    "EXCHNGTRADEDFD": "EXCHANGETRADEDFUND",
    "MTG": "MORTGAGE",
    "INS": "INSURANCE",
    "BANCORP": "BANCORP",
    "AEROSPAC": "AEROSPACE",
    "INSTRUMEN": "INSTRUMENTS",
    "CENTY": "CENTURY",
    "BD": "BOND",
    "ADVSR": "ADVISORS",
    "STR": "STRATEGIC",
    "MIDSTRM": "MIDSTREAM",
    "FIN": "FINANCIAL",
    "SCIS": "SCIENCES",
    "THERAPEUT": "THERAPEUTICS",
    "BIOSCIENC": "BIOSCIENCES",
    "DIAGNOSTI": "DIAGNOSTICS",
    "GENOMIC": "GENOMICS",
    "MKTS": "MARKETS",
    "MKT": "MARKET",
    "DEF": "DEFENSE",
    "CAP": "CAPITAL",
    "STD": "STANDARD",
    "SOLU": "SOLUTIONS",
    "SOLUT": "SOLUTIONS",
    "SEC": "SECURITY",
    "SCI": "SCIENCE",
    "BANKSHARES": "BANCSHARES",
    "BNCSHS": "BANCSHARES",
    "BANCSHS": "BANCSHARES",
    "LABS": "LABORATORIES",
    "NETWRKS": "NETWORKS",
    "NETWKS": "NETWORKS",
    "PPTY": "PROPERTY",
    "PPTYS": "PROPERTIES",
    "INFRA": "INFRASTRUCTURE",
    "INFRASTR": "INFRASTRUCTURE",
    "COMMNS": "COMMONS",
    "INVSTS": "INVESTMENTS",
    "ENGR": "ENERGY",
    "ENGY": "ENERGY",
    "PWR": "POWER",
    "AUTO": "AUTOMOTIVE",
    "AUTOMTV": "AUTOMOTIVE",
    "CONTL": "CONTINENTAL",
    "EDU": "EDUCATION",
    "GOVT": "GOVERNMENT",
    "BEVG": "BEVERAGE",
    "BEVGS": "BEVERAGES",
    "RETL": "RETAIL",
    "LOGIS": "LOGISTICS",
}

# Tokens dropped entirely (whole-word match)
ENTITY_SUFFIXES: set[str] = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "COMPANIES",
    "LTD", "LIMITED", "LLC", "LLP", "LP", "PLC",
    "HOLDINGS", "HLDGS", "HOLDING", "HLDG", "GROUP", "GRP", "GP",
    "TRUST", "FUND", "TR", "NV", "SA", "AG", "AB",
    "CONSOLIDATED", "CONSOL",
}

# State / reincorporation markers (e.g., "LENNAR CORP /NEW/", "CHEVRON CORP NEW")
LOCATION_MARKERS: set[str] = {
    "NEW", "DEL", "DELAWARE", "MD", "MARYLAND", "DE", "NY",
    "CALIFORNIA", "CAL", "CA", "TEXAS", "TX", "MO", "FL", "IL", "PA", "NJ",
    "USA", "US",
    "N",  # seen in "CHARTER COMMUNICATIONS INC N"
}

# Connectors / stopwords
STOPWORDS: set[str] = {
    "OF", "AND", "THE", "A", "AN", "FOR", "TO", "WITH", "IN", "ON",
}

# Share class + common-stock markers stripped before tokenization
CLASS_MARKERS_RE = re.compile(
    r"\b(CL|CLASS|SER|SERIES)\s+[A-Z0-9]+\b|"
    r"\b(COMMON\s+STOCK|COMMON|COM|ORDINARY\s+SHARES|ORD|PREF|PFD|PREFERRED)\b",
    re.IGNORECASE,
)

# Slash-wrapped state codes like "/DE/", "/NEW/", or trailing "/CA"
SLASH_MARKER_RE = re.compile(r"/[A-Z]{1,10}/?")

# Ticker-health diagnostics compare a registry issuer with the current SEC
# company title after removing harmless filing abbreviations. This normalized
# text is never an identity key and can never publish or resolve a ticker.
def normalize_name(name: str) -> str:
    """Normalize issuer text for a non-authoritative health-report comparison.

    Collapses entity suffixes, share classes, state markers, and common 13F
    abbreviations. Exact CUSIP-backed SEC evidence remains the sole ticker
    authority; this helper only suppresses a false-positive diagnostic for a
    symbol whose SEC title is compatible with the published registry label.
    """
    if not name:
        return ""
    s = name.upper()
    s = CLASS_MARKERS_RE.sub(" ", s)
    s = SLASH_MARKER_RE.sub(" ", s)
    # Possessives: "MOODY'S" -> "MOODYS"
    s = re.sub(r"'S\b", "S", s)
    s = s.replace("'", "")
    # Remaining punctuation -> space
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    tokens = s.split()
    tokens = [NAME_ABBREV.get(t, t) for t in tokens]
    tokens = [
        t for t in tokens
        if t not in ENTITY_SUFFIXES
        and t not in STOPWORDS
        and t not in LOCATION_MARKERS
    ]
    return "".join(tokens)


def _parse_sec_fund_series_page(
    page_text: str,
) -> tuple[dict[str, str], dict[str, str]]:
    """Parse official series and class names from one registrant page."""

    if not isinstance(page_text, str) or not page_text.strip():
        raise SourceParseError("SEC fund-series response was empty")
    try:
        document = lxml_html.fromstring(page_text)
    except (etree.ParserError, ValueError) as exc:
        raise SourceParseError(
            "SEC fund-series response was not complete HTML"
        ) from exc

    series_names: dict[str, str] = {}
    class_names: dict[str, str] = {}
    conflicts: set[str] = set()
    recognized_identifiers: set[str] = set()
    parsed_identifiers: set[str] = set()

    def expanded_cells(row: etree._Element) -> list[etree._Element]:
        cells: list[etree._Element] = []
        for cell in row.xpath("./th | ./td"):
            try:
                colspan = max(1, int(cell.get("colspan") or 1))
            except (TypeError, ValueError):
                colspan = 1
            cells.extend([cell] * colspan)
        return cells

    def anchor_identifier(anchor: etree._Element) -> str | None:
        href = str(anchor.get("href") or "")
        query = parse_qs(urlparse(href).query)
        identifiers = query.get("CIK") or query.get("cik") or []
        if len(identifiers) != 1:
            return None
        identifier = str(identifiers[0]).strip().upper()
        return identifier if re.fullmatch(r"[SC]\d+", identifier) else None

    for table in document.xpath("//table"):
        table_identifiers = {
            identifier
            for anchor in table.xpath(".//a[@href]")
            if (identifier := anchor_identifier(anchor)) is not None
        }
        recognized_identifiers.update(table_identifiers)
        if not table_identifiers:
            continue
        name_columns: set[int] = set()
        for row in table.xpath(".//tr"):
            if any(
                anchor_identifier(anchor)
                for anchor in row.xpath(".//a[@href]")
            ):
                break
            name_columns.update(
                index
                for index, cell in enumerate(expanded_cells(row))
                if " ".join(cell.text_content().split()).casefold()
                == "name"
            )
        if len(name_columns) != 1:
            continue
        name_column = next(iter(name_columns))

        for anchor in table.xpath(".//a[@href]"):
            identifier = anchor_identifier(anchor)
            if identifier is None:
                continue
            rows = anchor.xpath("ancestor::tr[1]")
            if not rows:
                continue
            cells = expanded_cells(rows[0])
            if name_column >= len(cells):
                continue
            name = normalize_security_label(
                " ".join(cells[name_column].text_content().split())
            )
            if not name:
                continue
            parsed_identifiers.add(identifier)
            target = (
                series_names
                if identifier.startswith("S")
                else class_names
            )
            previous = target.get(identifier)
            if previous and previous.casefold() != name.casefold():
                conflicts.add(identifier)
                target.pop(identifier, None)
            elif identifier not in conflicts:
                target[identifier] = name
    if not recognized_identifiers:
        visible_text = " ".join(document.text_content().casefold().split())
        transient_markers = (
            "busy",
            "temporarily unavailable",
            "service unavailable",
            "request rate threshold",
            "access denied",
        )
        error_type = (
            SourceParseError
            if any(marker in visible_text for marker in transient_markers)
            else SourceSchemaError
        )
        raise error_type(
            "SEC fund-series response contained no series/class identifiers"
        )
    if conflicts:
        raise SourceSchemaError(
            "SEC fund-series page assigned conflicting names to: "
            + ", ".join(sorted(conflicts))
        )
    missing_identifiers = sorted(recognized_identifiers - parsed_identifiers)
    if not series_names or missing_identifiers:
        detail = (
            "; unparsed identifiers: " + ", ".join(missing_identifiers[:10])
            if missing_identifiers
            else ""
        )
        raise SourceSchemaError(
            "SEC fund-series page no longer matches the expected Name-column "
            f"layout{detail}"
        )
    return series_names, class_names


def _fsync_directory(path: Path) -> None:
    """Persist directory-entry changes used by atomic file transactions."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write_json(
    path: Path,
    payload,
    *,
    indent: int | None = None,
    sort_keys: bool = False,
    fsync_parent: bool = True,
) -> None:
    """Write JSON atomically: render to a sibling temp file, fsync, then
    os.replace() into place. A SIGTERM or power loss mid-write leaves either
    the old file or the new file — never a half-flushed one that
    json.load() would reject."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # mkstemp provides an unpredictable, O_EXCL-created sibling on the same
    # filesystem. The private mode prevents another local account from reading
    # a partially rendered cache or substituting a symlink target.
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    tmp = Path(temporary_name)
    descriptor_open = True
    try:
        os.fchmod(descriptor, _SEC_PRIVATE_FILE_MODE)
        output = os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="\n",
        )
        descriptor_open = False
        with output as f:
            json.dump(
                payload,
                f,
                indent=indent,
                sort_keys=sort_keys,
                separators=(",", ":") if indent is None else None,
            )
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        if fsync_parent:
            _fsync_directory(path.parent)
    except BaseException:
        if descriptor_open:
            try:
                os.close(descriptor)
            except BaseException:
                pass
        try:
            tmp.unlink()
        except BaseException:
            # Cleanup is best-effort.  In particular, do not replace the
            # original write interruption with a secondary unlink failure.
            pass
        else:
            try:
                _fsync_directory(path.parent)
            except BaseException:
                # Preserve the primary failure even if persisting the temp
                # file removal is itself interrupted or unavailable.
                pass
        raise


def _remove_derived_output(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def _recover_interrupted_derived_publishes() -> None:
    """Restore the prior generation after an interrupted publish commit."""
    transaction_parent = DATA_DIR.parent
    if not transaction_parent.exists():
        return

    stale_stages = list(transaction_parent.glob(".derived-stage-*"))
    for stale_stage in stale_stages:
        _remove_derived_output(stale_stage)
    if stale_stages:
        _fsync_directory(transaction_parent)

    backup_roots = sorted(
        transaction_parent.glob(".derived-backup-*")
    )
    if len(backup_roots) > 1:
        raise FundDataError(
            "multiple interrupted derived-output publishes require review: "
            + ", ".join(str(path) for path in backup_roots)
        )
    if not backup_roots:
        return

    backup_root = backup_roots[0]
    marker_path = backup_root / "transaction.json"
    try:
        marker = json.loads(marker_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise FundDataError(
            f"invalid derived-output transaction marker: {marker_path}"
        ) from exc
    if not isinstance(marker, dict):
        raise FundDataError(
            f"derived-output transaction marker must be an object: {marker_path}"
        )

    status = marker.get("status")
    present = marker.get("present")
    targets = (
        (STOCKS_DIR.name, STOCKS_DIR),
        (FUNDS_INDEX_PATH.name, FUNDS_INDEX_PATH),
        (INDEX_PATH.name, INDEX_PATH),
    )
    valid_names = {name for name, _target in targets}
    if (
        status not in {"prepared", "published"}
        or not isinstance(present, list)
        or any(
            not isinstance(name, str) or name not in valid_names
            for name in present
        )
    ):
        raise FundDataError(
            f"invalid derived-output transaction state: {marker_path}"
        )
    live_outputs_complete = all(
        target.exists() or target.is_symlink()
        for _name, target in targets
    )
    if status == "published" and live_outputs_complete:
        shutil.rmtree(backup_root)
        _fsync_directory(transaction_parent)
        return
    if status == "published":
        log.warning(
            "published derived-output transaction is incomplete; "
            "restoring the previous generation"
        )

    present_names = set(present)
    for name, target in targets:
        backup = backup_root / name
        if backup.exists() or backup.is_symlink():
            _remove_derived_output(target)
            os.replace(backup, target)
            _fsync_directory(backup_root)
            _fsync_directory(DATA_DIR)
        elif name not in present_names:
            _remove_derived_output(target)
    _fsync_directory(DATA_DIR)
    _fsync_directory(backup_root)
    shutil.rmtree(backup_root)
    _fsync_directory(transaction_parent)


def _publish_staged_derived_outputs(staging_root: Path) -> None:
    """Publish staged stocks and indexes, restoring prior outputs on errors.

    POSIX cannot expose these three paths in one rename. Deployment consumers
    run only after this producer returns and validation succeeds; the marker
    provides crash recovery for the private working tree in the meantime.
    """
    replacements = (
        (staging_root / "stocks", STOCKS_DIR),
        (staging_root / "funds-index.json", FUNDS_INDEX_PATH),
        (staging_root / "index.json", INDEX_PATH),
    )
    missing = [
        str(path)
        for path, _target in replacements
        if not path.exists()
    ]
    if missing:
        raise FundDataError(
            "derived output staging is incomplete: " + ", ".join(missing)
        )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    _fsync_directory(DATA_DIR.parent)
    backup_root = Path(
        tempfile.mkdtemp(prefix=".derived-backup-", dir=DATA_DIR.parent)
    )
    _fsync_directory(DATA_DIR.parent)
    present = [
        target.name
        for _staged, target in replacements
        if target.exists() or target.is_symlink()
    ]
    marker_path = backup_root / "transaction.json"
    try:
        _atomic_write_json(
            marker_path,
            {"status": "prepared", "present": present},
            indent=None,
            sort_keys=True,
        )
    except BaseException:
        shutil.rmtree(backup_root, ignore_errors=True)
        _fsync_directory(DATA_DIR.parent)
        raise
    backups: list[tuple[Path, Path]] = []
    published: list[Path] = []
    try:
        for _staged, target in replacements:
            if target.exists() or target.is_symlink():
                backup = backup_root / target.name
                os.replace(target, backup)
                backups.append((backup, target))
                _fsync_directory(DATA_DIR)
                _fsync_directory(backup_root)
        for staged, target in replacements:
            os.replace(staged, target)
            published.append(target)
            _fsync_directory(staging_root)
            _fsync_directory(DATA_DIR)
        _atomic_write_json(
            marker_path,
            {"status": "published", "present": present},
            indent=None,
            sort_keys=True,
        )
    except BaseException as exc:
        rollback_errors: list[str] = []
        for target in reversed(published):
            try:
                _remove_derived_output(target)
            except OSError as rollback_exc:
                rollback_errors.append(f"remove {target}: {rollback_exc}")
        for backup, target in reversed(backups):
            try:
                _remove_derived_output(target)
                os.replace(backup, target)
                _fsync_directory(backup_root)
                _fsync_directory(DATA_DIR)
            except OSError as rollback_exc:
                rollback_errors.append(f"restore {target}: {rollback_exc}")
        try:
            _fsync_directory(DATA_DIR)
            _fsync_directory(backup_root)
        except OSError as rollback_exc:
            rollback_errors.append(f"fsync rollback: {rollback_exc}")
        if rollback_errors:
            raise FundDataError(
                "derived output publish failed and rollback was incomplete: "
                + "; ".join(rollback_errors)
            ) from exc
        try:
            shutil.rmtree(backup_root)
        except OSError as cleanup_exc:
            log.warning(
                "could not remove derived-output backup %s after rollback: %s",
                backup_root,
                cleanup_exc,
            )
        else:
            try:
                _fsync_directory(DATA_DIR.parent)
            except OSError as cleanup_exc:
                log.warning(
                    "could not fsync backup cleanup after rollback: %s",
                    cleanup_exc,
                )
        raise
    try:
        shutil.rmtree(backup_root)
    except OSError as exc:
        log.warning(
            "could not remove derived-output backup %s: %s",
            backup_root,
            exc,
        )
    else:
        try:
            _fsync_directory(DATA_DIR.parent)
        except OSError as exc:
            log.warning("could not fsync derived-output backup cleanup: %s", exc)


def _read_json_object(path: Path) -> dict | None:
    """Best-effort read used only to preserve semantic timestamps.

    Authoritative loaders still report malformed required data.  Timestamp
    preservation should never block a repair: a missing or malformed prior
    artifact simply behaves like a changed payload and gets replaced.
    """
    try:
        with open(path) as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _is_strict_utc_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ") == value


def _load_json_dict_with_fallback(
    primary_path: Path,
    legacy_path: Path,
    *,
    sort_keys: bool = False,
) -> dict:
    for path in (primary_path, legacy_path):
        if not path.exists():
            continue
        try:
            with open(path) as f:
                payload = json.load(f)
        except json.JSONDecodeError as e:
            # A corrupted map would otherwise silently return {}, forcing a
            # multi-hour SEC evidence re-resolve on the next run. Rename the bad
            # file out of the way so it's visible and the pipeline can rebuild
            # a fresh copy, rather than overwriting the evidence.
            size = path.stat().st_size if path.exists() else -1
            corrupt_path = path.with_suffix(path.suffix + ".corrupt")
            try:
                path.rename(corrupt_path)
            except OSError as rename_err:
                log.error(
                    f"  {path}: JSON parse failed ({e}); size={size}B; "
                    f"could not rename to .corrupt: {rename_err}"
                )
            else:
                log.error(
                    f"  {path}: JSON parse failed ({e}); size={size}B; "
                    f"moved to {corrupt_path}"
                )
            continue
        if not isinstance(payload, dict):
            log.error(
                f"  {path}: expected JSON object at top level, got "
                f"{type(payload).__name__}; ignoring"
            )
            continue
        if path != primary_path:
            _atomic_write_json(primary_path, payload, sort_keys=sort_keys)
        return payload
    return {}


def load_cusip_map() -> dict[str, str]:
    """Return the compatibility CUSIP->ticker view of the SEC master.

    The persisted authority is the provenance-bearing, type-keyed security
    master.  This compact view exists only for older ingestion call sites that
    still pass a mutable mapping through replay functions; it is never written
    to a separate unprovenanced cache.
    """

    candidates: dict[str, set[str]] = defaultdict(set)
    for record in load_security_master(SEC_SECURITY_MASTER_PATH).get(
        "records", {}
    ).values():
        if not isinstance(record, dict) or record.get("mapping_status") != "resolved":
            continue
        cusip = normalize_security_identifier(record.get("cusip"))
        ticker = str(record.get("ticker") or "").strip().upper()
        if cusip and ticker:
            candidates[cusip].add(ticker)
    return {
        cusip: next(iter(tickers))
        for cusip, tickers in candidates.items()
        if len(tickers) == 1
    }


def save_cusip_map(cusip_map: dict[str, str]) -> None:
    """Compatibility no-op; mappings persist only with SEC provenance."""

    _ = cusip_map


def load_sec_security_details() -> dict[str, dict]:
    """Return one deterministic descriptive SEC-master record per CUSIP."""

    type_priority = {
        "EQUITY": 0,
        "PREF": 1,
        "WARRANT": 2,
        "NOTE": 3,
        "CALL": 4,
        "PUT": 5,
        "OPT": 6,
    }
    selected: dict[str, tuple[int, dict]] = {}
    records = load_security_master(SEC_SECURITY_MASTER_PATH).get("records", {})
    for record in records.values():
        if not isinstance(record, dict):
            continue
        cusip = normalize_security_identifier(record.get("cusip"))
        if not cusip:
            continue
        instrument_type = normalize_instrument_type(
            record.get("instrument_type")
        )
        priority = type_priority.get(instrument_type, 99)
        current = selected.get(cusip)
        if current is None or priority < current[0]:
            selected[cusip] = (priority, dict(record))
    return {cusip: record for cusip, (_priority, record) in selected.items()}


def load_cusip_registry() -> dict:
    """Load one registry copy without merging stale provider-era metadata."""

    return _load_json_dict_with_fallback(
        LEGACY_CUSIP_REGISTRY_PATH,
        CUSIP_REGISTRY_PATH,
        sort_keys=True,
    )


def save_cusip_registry(registry: dict) -> None:
    for path in (CUSIP_REGISTRY_PATH, LEGACY_CUSIP_REGISTRY_PATH):
        _atomic_write_json(path, registry, sort_keys=True)


_SEC_PLAIN_TICKER_RE = SEC_TICKER_RE
_FILER_CLOSED_END_KIND_RE = re.compile(
    r"\bCLOSED[- ]END(?:\s+FUNDS?)?\b",
    re.IGNORECASE,
)
_FILER_ETN_KIND_RE = re.compile(
    r"\bETNS?\b|\bEXCHANGE[- ]TRADED\s+NOTES?\b",
    re.IGNORECASE,
)
_FILER_ETF_KIND_RE = re.compile(
    r"\bETFS?\b|\bETP\b|"
    r"\bEXCHANGE[- ]TRADED\s+(?:F|FD|FDS|FUND|FUNDS|PRODUCT|PRODUCTS)\b|"
    r"\bEXCH(?:ANGE|NG)?[- ]+TR(?:D|A?DED)\b",
    re.IGNORECASE,
)
_FILER_SPONSOR_TRUST_ETF_RE = re.compile(
    r"\b(?:ISHARES|SPDR)\b.*\bTRUST\b|\bINVESCO\s+QQQ\s+TRUST\b",
    re.IGNORECASE,
)
_FILER_ABBREVIATED_SPONSOR_TR_RE = re.compile(
    r"\b(?:ISHARES|SPDR)\b.*\bTR\b",
    re.IGNORECASE,
)
_FILER_EXCLUSIVE_ETF_ISSUER_RE = re.compile(
    r"(?:"
    r"ISHARES\s+TR|"
    r"ETFIS\s+SER(?:IES)?\s+TR(?:UST)?(?:\s+I)?|"
    r"JANUS\s+DETROIT\s+STR\s+TR|"
    r"(?:SELECT\s+SECTOR\s+)?SPDR\s+"
    r"(?:S&P\s+500\s+ETF\s+)?TR(?:UST)?"
    r")",
    re.IGNORECASE,
)
_FILER_SCHWAB_STRATEGIC_TR_RE = re.compile(
    r"SCHWAB\s+STRATEGIC\s+TR",
    re.IGNORECASE,
)
_FILER_RBB_FD_INC_RE = re.compile(r"RBB\s+FD\s+INC", re.IGNORECASE)
_FILER_MUTUAL_FUND_KIND_RE = re.compile(
    r"\bMUTUAL\s+FUNDS?\b|\bOPEN[- ]END\b|"
    r"\bMUTL\s+FUNDS?\b|\bMONEY\s+MARKET\s+FUNDS?\b|"
    r"\bNO[- ]?LOAD\s+FUNDS?\b",
    re.IGNORECASE,
)
_FILER_PREFERRED_KIND_RE = re.compile(
    r"\b(?:PFD|PREF|PREFERRED|PREFERENCE)\b",
    re.IGNORECASE,
)
_FILER_WARRANT_KIND_RE = re.compile(
    r"\b(?:WARRANTS?|WTS?)\b",
    re.IGNORECASE,
)
_FILER_RIGHT_KIND_RE = re.compile(r"\bRIGHTS?\b", re.IGNORECASE)
_FILER_UNIT_KIND_RE = re.compile(r"\bUNITS?\b", re.IGNORECASE)
_FILER_COMMON_KIND_RE = re.compile(
    r"\b(?:COM|COMMON|ORD|ORDINARY)\b|"
    r"\bCAP(?:ITAL)?\s+ST(?:OCK|K)\b",
    re.IGNORECASE,
)
_FILER_COMMON_CLASS_ONLY_RE = re.compile(
    r"^(?:CL|CLASS)\s+[A-Z0-9]+"
    r"(?:\s+(?:NEW|SH|SHS|SHARE|SHARES))?$",
    re.IGNORECASE,
)
_FILER_COMMON_CLASS_EXCLUSION_RE = re.compile(
    r"\b(?:"
    r"ADRS?|ADS|DEPOSITARY|DEP(?:OSITARY)?(?:\s+SHS?)?|"
    r"PFD|PREF|PREFERRED|PREFERENCE|"
    r"WARRANTS?|WTS?|RIGHTS?|CVR|CONTINGENT\s+VALUE|"
    r"UNITS?|UT|UNT|LP|PTN|PARTNERSHIPS?|"
    r"ETF|ETP|ETN|FUNDS?|FDS|INDEX|PORTFOLIOS?|"
    r"NOTES?|BONDS?|DEBT|CERTIFICATES?|DEPOSITOR|INDEXPLUS|"
    r"CALL|PUT|OPTIONS?|ISHARES|SPDR"
    r")\b|EXCHANGE[- ]TRADED",
    re.IGNORECASE,
)
_FILER_COMMON_ISSUER_EXCLUSION_RE = re.compile(
    r"\b(?:"
    r"ADRS?|ADS|DEPOSITARY|DEPOSITOR|DEP(?:OSITARY)?(?:\s+SHS?)?|"
    r"WARRANTS?|RIGHTS?|CONTINGENT\s+VALUE|"
    r"LP|PTN|PARTNERSHIPS?|"
    r"ETF|ETP|ETN|FUNDS?|FDS|INDEXPLUS|CERTIFICATES?|"
    r"ISHARES|SPDR"
    r")\b|EXCHANGE[- ]TRADED",
    re.IGNORECASE,
)
_FUND_PRODUCT_NAME_KINDS = frozenset({
    "ETF",
    "ETN",
    "MUTUAL FUND",
    "CLOSED-END FUND",
})
SEC_SECURITY_REFRESH_LOCK = threading.Lock()


class SecurityMasterRefreshError(RuntimeError):
    """Raised when no verified SEC security master can be produced."""


def _reported_descriptor_text(
    holding: dict,
    reported_field: str,
) -> str:
    """Return the SEC descriptor with only whitespace normalized."""

    raw_value = holding.get(reported_field)
    return " ".join(str(raw_value or "").split())


def _security_universe_from_holdings(
    holdings,
    reported_identity_sources=None,
) -> list[dict[str, object]]:
    """Return exact identity records plus immutable SEC filing descriptors."""

    sources_by_filing: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(
        list
    )
    for source in reported_identity_sources or []:
        if not isinstance(source, dict):
            continue
        sources_by_filing[
            (
                str(source.get("accession") or "").strip(),
                str(source.get("report_date") or "").strip(),
            )
        ].append(source)

    universe: dict[tuple[str, str, str, str], dict[str, object]] = {}

    def merge(record: dict[str, object]) -> None:
        identity = (
            str(record["cusip"]),
            str(record["instrument_type"]),
            str(record["issuer"]),
            str(record["security_class"]),
        )
        existing = universe.setdefault(identity, record)
        combined = {
            json.dumps(item, sort_keys=True, separators=(",", ":")): item
            for item in [
                *(existing.get("reported_identity_evidence") or []),
                *(record.get("reported_identity_evidence") or []),
            ]
            if isinstance(item, dict)
        }
        if combined:
            existing["reported_identity_evidence"] = [
                combined[min(combined)]
            ]

    for holding in holdings:
        cusip = normalize_security_identifier(
            holding.get("reported_cusip") or holding.get("cusip")
        )
        if not cusip:
            continue
        # Master keys must use the same preserved position identity as ticker
        # refresh, registry aggregation, and the economic-invariant gate.
        # Descriptive class text is evidence for resolution, not permission
        # to replace a persisted instrument type during this refresh.
        instrument_type = holding_instrument_type(holding)
        record = {
            "cusip": cusip,
            "instrument_type": instrument_type,
            "issuer": _reported_descriptor_text(
                holding,
                "reported_issuer",
            ),
            "security_class": _reported_descriptor_text(
                holding,
                "reported_class",
            ),
        }
        evidence = (
            sources_by_filing.get(
                (
                    str(holding.get("accession") or "").strip(),
                    str(holding.get("report_date") or "").strip(),
                ),
                [],
            )
            if _holding_has_hashable_reported_identity(holding)
            else []
        )
        if evidence:
            record["reported_identity_evidence"] = [
                {
                    **source,
                    "reported_cusip": cusip,
                    "reported_issuer": record["issuer"],
                    "reported_class": record["security_class"],
                }
                for source in evidence
                if isinstance(source, dict)
            ]
        merge(record)
        # Form 13F options report the underlying security's identifier. Keep
        # the option position identity while allowing the exact underlying
        # Equity record to provide display metadata.
        if instrument_type in {"CALL", "PUT", "OPT"}:
            equity_record = {**record, "instrument_type": "EQUITY"}
            merge(equity_record)
    return [universe[key] for key in sorted(universe)]


def collect_security_master_universe(
    extra_holdings: list[dict] | None = None,
) -> list[dict[str, object]]:
    """Collect all persisted and newly parsed security identities once."""

    universe: dict[tuple[str, str, str, str], dict[str, object]] = {}

    def merge_holdings(holdings, reported_identity_sources) -> None:
        for record in _security_universe_from_holdings(
            holdings,
            reported_identity_sources,
        ):
            key = (
                record["cusip"],
                record["instrument_type"],
                record["issuer"],
                record["security_class"],
            )
            existing = universe.setdefault(key, record)
            evidence = {
                json.dumps(item, sort_keys=True, separators=(",", ":")): item
                for item in [
                    *(existing.get("reported_identity_evidence") or []),
                    *(record.get("reported_identity_evidence") or []),
                ]
                if isinstance(item, dict)
            }
            if evidence:
                existing["reported_identity_evidence"] = [
                    evidence[min(evidence)]
                ]

    if FUNDS_DIR.exists():
        for fund_path in sorted(FUNDS_DIR.glob("*.json")):
            try:
                fund = json.loads(fund_path.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            for quarter in fund.get("quarters", []):
                merge_holdings(
                    quarter.get("holdings", []),
                    quarter.get("reported_identity_sources", []),
                )
    if extra_holdings:
        merge_holdings(extra_holdings, [])
    return [universe[key] for key in sorted(universe)]


_SEC_EDGAR_DISCOVERY_TYPES = frozenset({"EQUITY", "PREF", "WARRANT"})
_SEC_EDGAR_DISCOVERY_STATUSES = frozenset({"unresolved", "ambiguous"})
_SEC_EDGAR_RESOLVED_RECHECK_DAYS = 30
# A newly reported holding can precede the next official-list publication.  A
# six-month window covers one complete 13F reporting cycle plus the filing lag,
# without turning the repository's full historical corpus into an EDGAR queue.
_SEC_EDGAR_NEW_REPORTED_IDENTITY_DAYS = 183
# One unresolved CUSIP can fan out to many Schedule documents and periodic
# filings. Bound each unattended run; terminal results fall out of the queue,
# so an exceptional backlog drains deterministically over subsequent runs.
_SEC_EDGAR_INCREMENTAL_CANDIDATE_LIMIT = 50
_SEC_EDGAR_CLEAN_CANDIDATE_LIMIT = 250
_SEC_EDGAR_CLEAN_CHUNK_SIZE = 100
_SEC_EDGAR_JOURNAL_SCHEMA_VERSION = 1
_SEC_EDGAR_JOURNAL_FILE_RE = re.compile(
    r"^edgar-exception-batch-(?P<sequence>\d{6})\.json$"
)
_SEC_EDGAR_JOURNAL_TEMP_FILE_RE = re.compile(
    r"^\.edgar-exception-batch-\d{6}\.json\.[A-Za-z0-9_-]+\.tmp$"
)
_SEC_EDGAR_JOURNAL_FILE_PREFIX = "edgar-exception-batch-"
_SEC_PRIVATE_DIRECTORY_MODE = 0o700
_SEC_PRIVATE_FILE_MODE = 0o600
_SEC_SECURITY_MASTER_PAIR_LOCK_NAME = ".sec-security-master-pair.lock"
SEC_SECURITY_MASTER_REBUILD_WORK_ROOT = (
    CACHE_DIR / "sec-security-master-rebuild-work"
)
_SEC_SECURITY_MASTER_REBUILD_MANIFEST_SCHEMA_VERSION = 2
_SEC_SECURITY_MASTER_REBUILD_PLAN_VERSION = 2


def _is_sec_edgar_journal_managed_file(path: Path) -> bool:
    """Whether *path* is one of the rebuild workspace's private journals."""

    name = Path(path).name
    return bool(
        _SEC_EDGAR_JOURNAL_FILE_RE.fullmatch(name)
        or _SEC_EDGAR_JOURNAL_TEMP_FILE_RE.fullmatch(name)
        or (
            name.startswith(_SEC_EDGAR_JOURNAL_FILE_PREFIX)
            and ".json.tmp." in name
        )
    )


def _is_sec_edgar_journal_temp_file(path: Path) -> bool:
    """Whether *path* is an interrupted old or current journal temp file."""

    name = Path(path).name
    return bool(
        _SEC_EDGAR_JOURNAL_TEMP_FILE_RE.fullmatch(name)
        or (
            name.startswith(_SEC_EDGAR_JOURNAL_FILE_PREFIX)
            and ".json.tmp." in name
        )
    )


def _set_and_verify_private_mode(
    path: Path,
    mode: int,
    *,
    description: str,
) -> None:
    """Tighten one already type-checked private path and verify the result."""

    try:
        Path(path).chmod(mode)
        actual_mode = Path(path).stat().st_mode & 0o7777
    except OSError as exc:
        raise SecurityMasterRefreshError(
            f"could not secure {description}"
        ) from exc
    if actual_mode != mode:
        raise SecurityMasterRefreshError(
            f"{description} must have mode {mode:04o}"
        )


def audit_security_master(*args, **kwargs):
    """Apply production completeness gates at every publication boundary."""

    kwargs.setdefault(
        "minimum_current_symbol_population_by_kind",
        PRODUCTION_MIN_CURRENT_SYMBOL_POPULATION_BY_KIND,
    )
    kwargs.setdefault(
        "minimum_current_symbol_title_ratio",
        PRODUCTION_MIN_CURRENT_SYMBOL_TITLE_RATIO,
    )
    kwargs.setdefault(
        "minimum_active_official_cusip_count",
        PRODUCTION_MIN_ACTIVE_OFFICIAL_CUSIP_COUNT,
    )
    kwargs.setdefault("enforce_latest_completed_official_period", True)
    kwargs.setdefault("enforce_reported_identity_evidence", True)
    return _audit_security_master(*args, **kwargs)


def _security_master_pair_fingerprint(
    master: dict,
    state: dict,
) -> dict[str, str]:
    """Bind one production or staged master/state pair by canonical content."""

    components = {
        "master_sha256": _canonical_json_hash(master),
        "source_state_sha256": _canonical_json_hash(state),
    }
    return {
        **components,
        "pair_sha256": _canonical_json_hash(components),
    }


def _valid_security_master_pair_fingerprint(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "master_sha256",
        "source_state_sha256",
        "pair_sha256",
    }:
        return False
    components = {
        "master_sha256": value.get("master_sha256"),
        "source_state_sha256": value.get("source_state_sha256"),
    }
    return (
        all(
            isinstance(digest, str)
            and re.fullmatch(r"[0-9a-f]{64}", digest) is not None
            for digest in value.values()
        )
        and value.get("pair_sha256") == _canonical_json_hash(components)
    )


def _has_published_sec_security_state(master: dict, state: dict) -> bool:
    """Whether an explicit rebuild starts from an established SEC master.

    A legacy snapshot has neither cache file, so its default empty pair may
    safely reuse a completed, fingerprinted cutover stage after a downstream
    workflow failure. Once any SEC master has been published, an explicit
    rebuild is an independent reproducibility run and must start fresh.
    """

    return bool(
        master.get("records")
        or isinstance(master.get("audit"), dict)
        or master.get("generated_at") is not None
        or state.get("sources")
        or state.get("updated_at") is not None
    )


def _security_master_universe_sha256(
    universe: list[dict[str, str]],
) -> str:
    universe_bytes = json.dumps(
        {"securities": universe},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(universe_bytes).hexdigest()


def _security_master_rebuild_manifest(
    universe: list[dict[str, str]],
    *,
    production_master: dict,
    production_state: dict,
) -> dict[str, object]:
    parser_hashes = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in (
            "pipeline.py",
            "sec_security_master.py",
            "sec_edgar_evidence.py",
            "security_identity.py",
        )
    }
    plan = {
        "plan_version": _SEC_SECURITY_MASTER_REBUILD_PLAN_VERSION,
        "master_schema_version": SEC_SECURITY_MASTER_SCHEMA_VERSION,
        "source_state_schema_version": SEC_SOURCE_STATE_SCHEMA_VERSION,
        "edgar_cache_schema_version": SEC_EDGAR_CACHE_SCHEMA_VERSION,
        "ftd_history_start": "2004-03-22",
        "ftd_lookback_months": None,
        "edgar_chunk_size": _SEC_EDGAR_CLEAN_CHUNK_SIZE,
        "edgar_clean_candidate_limit": _SEC_EDGAR_CLEAN_CANDIDATE_LIMIT,
        "edgar_journal_schema_version": _SEC_EDGAR_JOURNAL_SCHEMA_VERSION,
        "parser_sha256": parser_hashes,
    }
    plan_bytes = json.dumps(
        plan,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "schema_version": _SEC_SECURITY_MASTER_REBUILD_MANIFEST_SCHEMA_VERSION,
        "plan": plan,
        "plan_sha256": hashlib.sha256(plan_bytes).hexdigest(),
        "universe_sha256": _security_master_universe_sha256(universe),
        "base_production": _security_master_pair_fingerprint(
            production_master,
            production_state,
        ),
    }


def _reported_identity_resume_receipt_for_in_progress_rebuild(
    universe: list[dict[str, str]],
) -> dict[str, object] | None:
    """Return the exact 13F receipt owned by one interrupted master build.

    An in-progress workspace and a completed-but-not-yet-promoted stage both
    belong to the same interrupted attempt. A completed stage is eligible only
    while production still equals its recorded base; after promotion, invoking
    ``--rebuild-security-master`` again must perform a new independent 13F
    reconstruction. The stored parser hash need only be internally valid so a
    downstream parser fix can reuse the immutable 13F generation; the universe
    and production rollback boundary still have to match the live inputs.
    """

    root = SEC_SECURITY_MASTER_REBUILD_WORK_ROOT
    manifest_path = root / "manifest.json"
    if (
        root.is_symlink()
        or not root.is_dir()
        or manifest_path.is_symlink()
        or not manifest_path.is_file()
    ):
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    status = manifest.get("status") if isinstance(manifest, dict) else None
    in_progress = (
        status == "in_progress"
        and manifest.get("completed_at") is None
        and manifest.get("completed_stage") is None
        and manifest.get("completed_master_universe_sha256") is None
    )
    completed_unpromoted = (
        status == "complete"
        and _is_strict_utc_timestamp(manifest.get("completed_at"))
        and _valid_security_master_pair_fingerprint(
            manifest.get("completed_stage")
        )
        and isinstance(
            manifest.get("completed_master_universe_sha256"),
            str,
        )
    )
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version")
        != _SEC_SECURITY_MASTER_REBUILD_MANIFEST_SCHEMA_VERSION
        or not (in_progress or completed_unpromoted)
        or manifest.get("universe_sha256")
        != _security_master_universe_sha256(universe)
        or not _valid_security_master_pair_fingerprint(
            manifest.get("base_production")
        )
    ):
        return None
    plan = manifest.get("plan")
    plan_sha256 = manifest.get("plan_sha256")
    if (
        not isinstance(plan, dict)
        or not isinstance(plan_sha256, str)
        or hashlib.sha256(
            json.dumps(
                plan,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        != plan_sha256
    ):
        return None
    try:
        production_master, production_state = load_security_master_pair(
            master_path=SEC_SECURITY_MASTER_PATH,
            source_state_path=SEC_SOURCE_STATE_PATH,
        )
        current_production = _security_master_pair_fingerprint(
            production_master,
            production_state,
        )
    except (OSError, ValueError):
        return None
    if manifest["base_production"] != current_production:
        return None
    if completed_unpromoted:
        try:
            staged_master, staged_state = load_security_master_pair(
                master_path=root / "sec_security_master.json",
                source_state_path=root / "sec_source_state.json",
            )
        except (OSError, ValueError):
            return None
        if (
            _security_master_pair_fingerprint(staged_master, staged_state)
            != manifest.get("completed_stage")
            or staged_master.get("universe_sha256")
            != manifest.get("completed_master_universe_sha256")
        ):
            return None

    stored_receipt = manifest.get("reported_identity_rebuild_receipt")
    if stored_receipt is None:
        # Migration path for an attempt created immediately before receipts
        # were introduced. The exact state and 6+ GiB SQLite generation are
        # fully checksummed before they can be adopted.
        try:
            receipt = build_completed_clean_rebuild_receipt(
                verify_index_checksum=True,
            )
        except (OSError, ValueError):
            return None
    elif isinstance(stored_receipt, dict):
        receipt = dict(stored_receipt)
    else:
        return None

    created_at = manifest.get("created_at")
    generated_at = receipt.get("generated_at")
    if (
        not _is_strict_utc_timestamp(created_at)
        or not _is_strict_utc_timestamp(generated_at)
        or str(generated_at) > str(created_at)
    ):
        return None
    return receipt


def _legacy_cutover_completed_identity_receipt(
    *,
    published_sec_security_state: bool,
) -> dict[str, object] | None:
    """Return only a receipt safe to reuse for an unpublished v1 cutover."""

    if published_sec_security_state:
        return None
    receipt = load_completed_clean_rebuild_receipt()
    if isinstance(receipt, dict):
        return dict(receipt)
    try:
        receipt = prepare_unpublished_legacy_index_adoption(
            FUNDS_DIR,
            published_sec_security_state=False,
        )
    except (OSError, ValueError):
        return None
    return dict(receipt)


def _record_reported_identity_rebuild_receipt(
    root: Path,
    receipt: dict[str, object] | None,
) -> None:
    """Attach a verified 13F generation to the active private workspace."""

    if receipt is None:
        return
    manifest_path = Path(root) / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise SecurityMasterRefreshError(
            "SEC rebuild workspace manifest is not a regular file"
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SecurityMasterRefreshError(
            "invalid SEC rebuild workspace manifest"
        ) from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("status") != "in_progress"
        or manifest.get("completed_stage") is not None
    ):
        raise SecurityMasterRefreshError(
            "SEC reported-identity receipt requires an in-progress rebuild"
        )
    manifest["reported_identity_rebuild_receipt"] = dict(receipt)
    _atomic_write_json(manifest_path, manifest, sort_keys=True)


def _prepare_security_master_rebuild_work(
    universe: list[dict[str, str]],
    *,
    production_master: dict,
    production_state: dict,
    current: datetime | None = None,
    force_fresh_completed: bool = False,
) -> tuple[Path, bool]:
    """Return a manifest-bound persistent, nonpublishable rebuild workspace."""

    root = SEC_SECURITY_MASTER_REBUILD_WORK_ROOT
    if root.parent.is_symlink():
        raise SecurityMasterRefreshError(
            "SEC security-master rebuild workspace parent cannot be a symlink"
        )
    if root.is_symlink():
        raise SecurityMasterRefreshError(
            "SEC security-master rebuild workspace cannot be a symlink"
        )
    if root.exists() and not root.is_dir():
        raise SecurityMasterRefreshError(
            "SEC security-master rebuild workspace must be a directory"
        )
    try:
        root.mkdir(
            parents=True,
            exist_ok=True,
            mode=_SEC_PRIVATE_DIRECTORY_MODE,
        )
    except OSError as exc:
        raise SecurityMasterRefreshError(
            "could not create SEC security-master rebuild workspace"
        ) from exc
    if root.is_symlink() or not root.is_dir():
        raise SecurityMasterRefreshError(
            "SEC security-master rebuild workspace must be a regular directory"
        )
    _set_and_verify_private_mode(
        root,
        _SEC_PRIVATE_DIRECTORY_MODE,
        description="SEC security-master rebuild workspace",
    )
    staged_master = root / "sec_security_master.json"
    staged_state = root / "sec_source_state.json"
    managed_names = {
        _SEC_SECURITY_MASTER_PAIR_LOCK_NAME,
        "manifest.json",
        "sec_security_master.json",
        "sec_source_state.json",
    }
    for name in sorted(managed_names):
        child = root / name
        if child.is_symlink() or (child.exists() and not child.is_file()):
            raise SecurityMasterRefreshError(
                "SEC rebuild managed path must be a regular file: " + name
            )
        if child.is_file():
            _set_and_verify_private_mode(
                child,
                _SEC_PRIVATE_FILE_MODE,
                description="SEC rebuild managed file " + name,
            )
    if root.is_dir():
        removed_temporary_journal = False
        for child in root.iterdir():
            if _is_sec_edgar_journal_managed_file(child):
                if child.is_symlink() or not child.is_file():
                    raise SecurityMasterRefreshError(
                        "SEC rebuild EDGAR journal path must be a regular file: "
                        + child.name
                    )
                _set_and_verify_private_mode(
                    child,
                    _SEC_PRIVATE_FILE_MODE,
                    description="SEC rebuild EDGAR journal " + child.name,
                )
                if _is_sec_edgar_journal_temp_file(child):
                    child.unlink()
                    removed_temporary_journal = True
        if removed_temporary_journal:
            _fsync_directory(root)
    try:
        recover_security_master_pair(
            master_path=staged_master,
            source_state_path=staged_state,
        )
    except (OSError, ValueError) as exc:
        raise SecurityMasterRefreshError(
            "could not recover the SEC rebuild staged pair"
        ) from exc
    desired = _security_master_rebuild_manifest(
        universe,
        production_master=production_master,
        production_state=production_state,
    )
    manifest_path = root / "manifest.json"
    existing: dict[str, object] = {}
    try:
        existing_raw = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(existing_raw, dict):
            existing = existing_raw
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        existing = {}
    identity_fields = ("schema_version", "plan_sha256", "universe_sha256")
    compatible = bool(existing) and all(
        existing.get(field) == desired.get(field) for field in identity_fields
    )
    status = existing.get("status")
    existing_base = existing.get("base_production")
    completed_stage = existing.get("completed_stage")
    current_production = desired["base_production"]
    if (
        force_fresh_completed
        and status == "complete"
        and current_production == completed_stage
    ):
        # Production already equals this completed stage, so a new explicit
        # --rebuild-security-master call must be an independent reproducibility
        # run.  When production still equals the manifest's base, the prior
        # attempt stopped between completing the stage and promoting it; that
        # exact stage remains the deterministic resume candidate.
        compatible = False
    if compatible:
        compatible = (
            status in {"in_progress", "complete"}
            and _valid_security_master_pair_fingerprint(existing_base)
            and (
                (
                    status == "in_progress"
                    and completed_stage is None
                    and existing_base == current_production
                )
                or (
                    status == "complete"
                    and _valid_security_master_pair_fingerprint(
                        completed_stage
                    )
                    and current_production
                    in (existing_base, completed_stage)
                )
            )
        )
    if compatible:
        try:
            loaded_state = None
            loaded_master = None
            if existing.get("status") == "complete":
                loaded_master, loaded_state = load_security_master_pair(
                    master_path=staged_master,
                    source_state_path=staged_state,
                )
            if existing.get("status") == "complete" and (
                loaded_master is None
                or loaded_state is None
                or loaded_master.get("source_state_sha256")
                != source_state_sha256(loaded_state)
                or _security_master_pair_fingerprint(
                    loaded_master,
                    loaded_state,
                )
                != existing.get("completed_stage")
                or existing.get("completed_master_universe_sha256")
                != loaded_master.get("universe_sha256")
            ):
                compatible = False
        except Exception:
            compatible = False
    if not compatible:
        if root.exists():
            unknown = [
                child
                for child in root.iterdir()
                if child.name not in managed_names
                and not _is_sec_edgar_journal_managed_file(child)
                and not (
                    child.is_file()
                    and (
                        child.name.startswith(".manifest.json.")
                        or child.name.startswith(".sec_security_master.json.")
                        or child.name.startswith(".sec_source_state.json.")
                        or child.name.startswith("manifest.json.tmp.")
                    )
                )
            ]
            if unknown:
                raise SecurityMasterRefreshError(
                    "SEC rebuild workspace contains unmanaged entries: "
                    + ", ".join(sorted(child.name for child in unknown))
                )
            for child in list(root.iterdir()):
                if child.name == _SEC_SECURITY_MASTER_PAIR_LOCK_NAME:
                    continue
                if child.is_dir() and not child.is_symlink():
                    raise SecurityMasterRefreshError(
                        "SEC rebuild managed path unexpectedly became a directory: "
                        + child.name
                    )
                child.unlink()
        existing = {
            **desired,
            "status": "in_progress",
            "created_at": (current or datetime.now(timezone.utc))
            .astimezone(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "completed_at": None,
            "completed_master_universe_sha256": None,
            "completed_stage": None,
        }
        _atomic_write_json(manifest_path, existing, sort_keys=True)

    complete = existing.get("status") == "complete"
    if complete:
        try:
            completed_value = str(existing.get("completed_at") or "")
            completed = datetime.fromisoformat(
                completed_value.removesuffix("Z") + "+00:00"
            )
        except ValueError:
            completed = None
        now = current or datetime.now(timezone.utc)
        stage_audit_ok = False
        if completed is not None:
            try:
                stage_audit = audit_security_master(
                    loaded_master,
                    prior_master=(
                        production_master
                        if production_master.get("audit")
                        else None
                    ),
                    as_of=now,
                )
                stage_audit_ok = bool(stage_audit.get("ok"))
            except Exception:
                stage_audit_ok = False
        if (
            completed is None
            or now.astimezone(timezone.utc) - completed
            > timedelta(days=_SEC_EDGAR_RESOLVED_RECHECK_DAYS)
            or not stage_audit_ok
        ):
            existing["status"] = "in_progress"
            # Once a promoted stage is reopened, the current production pair
            # becomes the rollback boundary for all subsequent checkpoints.
            # This preserves resumability without allowing that older stage to
            # overwrite a later incremental production refresh.
            existing["base_production"] = current_production
            existing["completed_at"] = None
            existing["completed_master_universe_sha256"] = None
            existing["completed_stage"] = None
            _atomic_write_json(manifest_path, existing, sort_keys=True)
            complete = False
    return root, complete


def _mark_security_master_rebuild_complete(
    root: Path,
    *,
    current: datetime | None = None,
) -> None:
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise SecurityMasterRefreshError("invalid SEC rebuild workspace manifest")
    completed = (current or datetime.now(timezone.utc)).astimezone(timezone.utc)
    staged_master, staged_state = load_security_master_pair(
        master_path=root / "sec_security_master.json",
        source_state_path=root / "sec_source_state.json",
    )
    if staged_master.get("source_state_sha256") != source_state_sha256(
        staged_state
    ):
        raise SecurityMasterRefreshError(
            "SEC rebuild staged master is not bound to its source state"
        )
    manifest["status"] = "complete"
    manifest["completed_at"] = (
        completed.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    manifest["completed_master_universe_sha256"] = staged_master.get(
        "universe_sha256"
    )
    manifest["completed_stage"] = _security_master_pair_fingerprint(
        staged_master,
        staged_state,
    )
    _atomic_write_json(manifest_path, manifest, sort_keys=True)
_SEC_FUND_SERIES_RECHECK_DAYS = 7
_SEC_EDGAR_FINGERPRINT_FIELDS = (
    "cusip",
    "instrument_type",
    "mapping_status",
    "resolution_reason",
    "reported_issuer",
    "reported_issuers",
    "reported_class",
    "reported_classes",
    "reported_identities",
    "reported_identity_evidence",
    "official_13f_status",
    "official_13f_as_of",
    "official_13f",
    "candidate_ticker",
    "candidate_symbols",
    "candidate_as_of",
    "confirmation_dates",
    "symbol_validation_titles",
    "ticker",
    "ticker_source",
    "ticker_as_of",
    "last_verification_date",
    "sec_edgar_evidence",
)


def _sec_edgar_fingerprint_record(record: dict) -> dict[str, object]:
    """Project only exact identity/evidence changes into discovery state.

    A new quarterly official-list file changes its URL, checksum, and period
    even when the exact CUSIP row is byte-for-byte equivalent.  Those container
    changes must not reopen a terminal no-evidence result forever.  Exact list
    membership and descriptor changes remain in the fingerprint.
    """

    projected: dict[str, object] = {
        field: record.get(field)
        for field in _SEC_EDGAR_FINGERPRINT_FIELDS
        if field in record and field not in {"official_13f", "official_13f_as_of"}
    }
    official = record.get("official_13f")
    if isinstance(official, dict):
        rows = official.get("records")
        if isinstance(rows, list):
            exact_rows = [
                {
                    field: row.get(field)
                    for field in (
                        "cusip",
                        "option_indicator",
                        "issuer",
                        "description",
                        "status",
                    )
                    if field in row
                }
                for row in rows
                if isinstance(row, dict)
            ]
            projected["official_13f"] = {
                "status": official.get("status"),
                "records": sorted(
                    exact_rows,
                    key=lambda row: json.dumps(
                        row,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            }
    return projected


def _sec_edgar_date_within(
    raw_date: object,
    *,
    reference: date | None,
    days: int,
) -> bool:
    """Return whether one canonical date is recent without accepting futures."""

    if reference is None or not isinstance(raw_date, str):
        return False
    try:
        parsed = date.fromisoformat(raw_date)
    except ValueError:
        return False
    age = (reference - parsed).days
    return 0 <= age <= days


def _sec_edgar_candidate_priority(
    records: list[dict],
    *,
    current_date: date,
    latest_ftd_date: date | None,
    recent_window_days: int,
) -> int | None:
    """Rank current/actionable exceptions; omit permanent historical gaps."""

    priorities: list[int] = []
    for record in records:
        status = record.get("mapping_status")
        if status == "resolved" and record.get("ticker_source") == "sec_ixbrl":
            # These mappings have a 45-day freshness gate, so their 30-day
            # revalidation cadence takes precedence over enrichment attempts.
            priorities.append(0)
            continue
        if status not in _SEC_EDGAR_DISCOVERY_STATUSES:
            continue

        official_active = record.get("official_13f_status") == "active"
        recent_ftd = any(
            _sec_edgar_date_within(
                record.get(field),
                reference=latest_ftd_date,
                days=recent_window_days,
            )
            for field in ("candidate_as_of", "last_verification_date")
        )
        identity_evidence = record.get("reported_identity_evidence", [])
        recent_report = isinstance(identity_evidence, list) and any(
            isinstance(evidence, dict)
            and _sec_edgar_date_within(
                evidence.get("report_date"),
                reference=current_date,
                days=_SEC_EDGAR_NEW_REPORTED_IDENTITY_DAYS,
            )
            for evidence in identity_evidence
        )
        if not (official_active or recent_ftd or recent_report):
            continue
        if official_active:
            priorities.append(2 if status == "ambiguous" else 3)
        elif recent_ftd:
            priorities.append(4 if status == "ambiguous" else 5)
        else:
            priorities.append(6 if status == "ambiguous" else 7)
    return min(priorities) if priorities else None


def _sec_fund_series_target_ciks(master: dict, source_state: dict) -> set[str]:
    """Return registrants needed by exact resolved fund-symbol records."""

    resolved_symbols = {
        str(record.get("ticker") or "").strip().upper()
        for record in master.get("records", {}).values()
        if isinstance(record, dict)
        and record.get("mapping_status") == "resolved"
        and str(record.get("ticker") or "").strip()
    }
    symbol_records: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for source in source_state.get("sources", {}).values():
        if not isinstance(source, dict) or source.get("kind") != "sec_fund_tickers":
            continue
        for record in source.get("fund_records", []):
            if not isinstance(record, dict):
                continue
            symbol = str(record.get("symbol") or "").strip().upper()
            cik = str(record.get("cik") or "")
            series_id = str(record.get("series_id") or "")
            class_id = str(record.get("class_id") or "")
            if symbol in resolved_symbols:
                symbol_records[symbol].add((cik, series_id, class_id))
    return {
        next(iter(records))[0]
        for records in symbol_records.values()
        if len(records) == 1
    }


def _sec_fund_series_refresh_due(
    source: dict | None,
    *,
    current: datetime,
) -> bool:
    if not isinstance(source, dict):
        return True
    checked_at = source.get(
        "last_successful_check_at",
        source.get("accepted_at"),
    )
    if not _is_strict_utc_timestamp(checked_at):
        return True
    checked = datetime.strptime(checked_at, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )
    return current.astimezone(timezone.utc) >= checked + timedelta(
        days=_SEC_FUND_SERIES_RECHECK_DAYS
    )


def _refresh_sec_fund_series_evidence(
    result,
    universe: list[dict[str, str]],
    *,
    refreshed_at: datetime | None = None,
    fetcher=None,
    master_path: Path = SEC_SECURITY_MASTER_PATH,
    source_state_path: Path = SEC_SOURCE_STATE_PATH,
):
    """Refresh selected SEC series pages and atomically bind them to master."""

    current = refreshed_at or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc).replace(microsecond=0)
    checked_at = current.strftime("%Y-%m-%dT%H:%M:%SZ")
    candidate_state = json.loads(json.dumps(result.state))
    sources = candidate_state.get("sources")
    if not isinstance(sources, dict):
        return result
    target_ciks = _sec_fund_series_target_ciks(result.master, candidate_state)
    due_urls = [
        sec_fund_series_url(cik)
        for cik in sorted(target_ciks)
        if _sec_fund_series_refresh_due(
            sources.get(sec_fund_series_url(cik)),
            current=current,
        )
    ]
    if not due_urls:
        return result

    fetch = fetcher or make_sec_fetcher(USER_AGENT)
    refreshed_urls: set[str] = set(result.refreshed_urls)
    retained_urls: set[str] = set(result.retained_urls)
    errors = list(result.errors)
    state_changed = False
    for url in due_urls:
        prior = sources.get(url)
        try:
            payload = fetch(url)
            if not isinstance(payload, (bytes, bytearray)):
                raise SourceParseError(
                    "SEC fund-series fetcher returned non-bytes"
                )
            raw = bytes(payload)
            digest = hashlib.sha256(raw).hexdigest()
            if isinstance(prior, dict) and prior.get("sha256") == digest:
                replacement = dict(prior)
                replacement["last_successful_check_at"] = checked_at
                sources[url] = replacement
                retained_urls.add(url)
                state_changed = replacement != prior or state_changed
                continue
            series_names, class_names = _parse_sec_fund_series_page(
                raw.decode("utf-8-sig")
            )
            cik = dict(parse_qs(urlparse(url).query)).get("CIK", [""])[0]
            sources[url] = {
                "url": url,
                "kind": "sec_fund_series",
                "sha256": digest,
                "accepted_at": checked_at,
                "last_successful_check_at": checked_at,
                "cik": str(cik).zfill(10),
                "series_names": {
                    key: series_names[key] for key in sorted(series_names)
                },
                "class_names": {
                    key: class_names[key] for key in sorted(class_names)
                },
            }
            refreshed_urls.add(url)
            state_changed = True
        except SourceSchemaError as exc:
            raise SourceSchemaChangeError([f"{url}: {exc}"]) from exc
        except UnicodeDecodeError:
            errors.append(f"{url}: SourceParseError: invalid UTF-8 response")
            if isinstance(prior, dict):
                retained_urls.add(url)
        except Exception as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise
            errors.append(f"{url}: {type(exc).__name__}: {exc}")
            if isinstance(prior, dict):
                retained_urls.add(url)

    if len(errors) > len(result.errors):
        # A batch of selected series pages is one enrichment checkpoint. Do
        # not publish successful pages from a partial batch alongside retained
        # stale pages; keep the exact prior master/state and retry the complete
        # due set on the next maintenance run.
        return replace(
            result,
            retained_urls=tuple(sorted(retained_urls)),
            errors=tuple(errors),
        )
    if not state_changed:
        return replace(result, errors=tuple(errors))
    candidate_state["updated_at"] = checked_at
    policy = result.master.get("policy", {})
    rebuilt = rebuild_sec_security_master(
        candidate_state,
        universe,
        recent_window_days=int(policy.get("recent_window_days", 31)),
        max_evidence_age_days=int(policy.get("max_evidence_age_days", 395)),
        min_confirmation_dates=int(policy.get("min_confirmation_dates", 2)),
    )
    acceptance = audit_security_master(
        rebuilt,
        prior_master=result.master,
        as_of=current,
        enforce_sec_ixbrl_freshness=False,
    )
    if not acceptance["ok"]:
        raise SecurityMasterRefreshError(
            "SEC fund-series evidence failed the security-master gate: "
            + ", ".join(acceptance["issues"])
        )
    save_security_master_pair(
        rebuilt,
        candidate_state,
        master_path=master_path,
        source_state_path=source_state_path,
    )
    return replace(
        result,
        master=rebuilt,
        state=candidate_state,
        changed=True,
        refreshed_urls=tuple(sorted(refreshed_urls)),
        retained_urls=tuple(sorted(retained_urls)),
        errors=tuple(errors),
        acceptance=acceptance,
    )


def _sec_edgar_discovery_candidates(
    master: dict,
    source_state: dict,
    *,
    as_of: datetime | None = None,
    max_candidates: int | None = _SEC_EDGAR_INCREMENTAL_CANDIDATE_LIMIT,
) -> tuple[list[str], dict[str, str]]:
    """Return a bounded, prioritized queue of actionable exact CUSIPs.

    A terminal no-evidence or conflict result stays terminal while its exact
    SEC identity/list-row/FTD/class/conflict fingerprint is unchanged. Current
    official-list securities, recent exact FTD evidence, and newly reported
    13F identities are actionable; old corpus-only gaps remain tickerless and
    do not become a one-time 27,000-item search queue. Resolved iXBRL bridges
    use a 30-day cadence so each mapping can complete a successful check before
    the 45-day publication limit. Permanent ``no_listed_symbol`` and malformed
    master states never enter this queue. Transient actionable results are
    retried by the next run.
    """

    if max_candidates is not None and (
        type(max_candidates) is not int or max_candidates < 0
    ):
        raise ValueError("SEC EDGAR candidate limit must be a non-negative integer")

    eligible: dict[str, list[dict]] = defaultdict(list)
    for record in master.get("records", {}).values():
        if not isinstance(record, dict):
            continue
        cusip = normalize_security_identifier(record.get("cusip"))
        instrument_type = normalize_instrument_type(
            record.get("instrument_type")
        )
        status = record.get("mapping_status")
        is_unresolved_candidate = status in _SEC_EDGAR_DISCOVERY_STATUSES
        is_resolved_ixbrl_candidate = (
            status == "resolved" and record.get("ticker_source") == "sec_ixbrl"
        )
        if (
            not cusip
            or instrument_type not in _SEC_EDGAR_DISCOVERY_TYPES
            or not (is_unresolved_candidate or is_resolved_ixbrl_candidate)
        ):
            continue
        eligible[cusip].append(_sec_edgar_fingerprint_record(record))

    prior_discovery = source_state.get("edgar_discovery", {})
    prior_records = (
        prior_discovery.get("records", {})
        if isinstance(prior_discovery, dict)
        else {}
    )
    if not isinstance(prior_records, dict):
        prior_records = {}

    ranked_candidates: list[tuple[int, str, int, str]] = []
    fingerprints: dict[str, str] = {}
    current = as_of or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current_date = current.astimezone(timezone.utc).date()
    audit = master.get("audit", {})
    latest_ftd_text = (
        audit.get("latest_ftd_settlement_date")
        if isinstance(audit, dict)
        else None
    )
    try:
        latest_ftd_date = (
            date.fromisoformat(latest_ftd_text)
            if isinstance(latest_ftd_text, str)
            else None
        )
    except ValueError:
        latest_ftd_date = None
    policy = master.get("policy", {})
    recent_window_days = (
        policy.get("recent_window_days", 31)
        if isinstance(policy, dict)
        else 31
    )
    if type(recent_window_days) is not int or recent_window_days < 0:
        recent_window_days = 31
    for cusip, records in sorted(eligible.items()):
        fingerprint = _canonical_json_hash({
            "records": sorted(
                records,
                key=lambda record: (
                    str(record.get("instrument_type") or ""),
                    json.dumps(
                        record,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
        })
        fingerprints[cusip] = fingerprint
        priority = _sec_edgar_candidate_priority(
            records,
            current_date=current_date,
            latest_ftd_date=latest_ftd_date,
            recent_window_days=recent_window_days,
        )
        if priority is None:
            continue
        prior = prior_records.get(cusip)
        fingerprint_changed = (
            isinstance(prior, dict)
            and prior.get("record_sha256") != fingerprint
        )
        if fingerprint_changed and priority > 0:
            # A changed exact fingerprint must not wait behind the untouched
            # historical/current backlog. Due iXBRL checks remain lane zero.
            priority = 1
        checked_at = prior.get("checked_at") if isinstance(prior, dict) else None
        checked_date = None
        if _is_strict_utc_timestamp(checked_at):
            checked_date = datetime.strptime(
                checked_at,
                "%Y-%m-%dT%H:%M:%SZ",
            ).date()
        same_terminal_result = (
            isinstance(prior, dict)
            and prior.get("record_sha256") == fingerprint
            and prior.get("terminal") is True
        )
        is_resolved_ixbrl = any(
            record.get("mapping_status") == "resolved"
            and record.get("ticker_source") == "sec_ixbrl"
            for record in records
        )
        terminal_is_current = same_terminal_result and (
            not is_resolved_ixbrl
            or (
                checked_date is not None
                and max(0, (current_date - checked_date).days)
                < _SEC_EDGAR_RESOLVED_RECHECK_DAYS
            )
        )
        if terminal_is_current:
            continue
        # Publication-critical iXBRL rechecks and materially changed evidence
        # lead the queue. After that, every never-checked identity precedes
        # retries, and retries rotate globally by oldest attempt before their
        # evidence-priority lane. Persistent transient failures therefore
        # cannot starve lower-lane actionable identities behind a fixed cap.
        queue_clock = (
            ""
            if fingerprint_changed or checked_date is None
            else str(checked_at)
        )
        if priority == 0:
            queue_phase = 0
        elif fingerprint_changed:
            queue_phase = 1
        elif checked_date is None:
            queue_phase = 2
        else:
            queue_phase = 3
        ranked_candidates.append((queue_phase, queue_clock, priority, cusip))
    ranked_candidates.sort()
    if max_candidates is not None and len(ranked_candidates) > max_candidates:
        log.warning(
            "  SEC EDGAR candidate backlog bounded to %s of %s actionable "
            "CUSIPs; deferred records remain tickerless",
            max_candidates,
            len(ranked_candidates),
        )
    selected = (
        ranked_candidates
        if max_candidates is None
        else ranked_candidates[:max_candidates]
    )
    return (
        [cusip for _phase, _queue_clock, _priority, cusip in selected],
        fingerprints,
    )


def _sec_edgar_batch_journal_payload(
    candidates: list[str],
    fingerprints: dict[str, str],
    *,
    sequence: int,
    prior_entry_sha256: str | None,
    current: datetime,
    discovery_fetcher,
) -> dict[str, object]:
    """Fetch and validate one bounded EDGAR batch without copying the master."""

    try:
        discovery = discover_sec_edgar_sources(
            candidates,
            fetcher=discovery_fetcher,
        )
    except EvidenceSchemaError as exc:
        raise SourceSchemaChangeError([
            f"SEC EDGAR discovery contract changed: {exc}"
        ]) from exc
    serialized = discovery.to_dict()
    refreshed_cache: dict[str, object] = {}
    if discovery.sources:
        try:
            refreshed_cache = refresh_sec_edgar_evidence(
                discovery.sources,
                cache_path=None,
                fetcher=discovery_fetcher,
                refreshed_at=current,
            )
        except EvidenceSchemaError as exc:
            raise SourceSchemaChangeError([
                f"SEC EDGAR evidence contract changed: {exc}"
            ]) from exc
    payload: dict[str, object] = {
        "schema_version": _SEC_EDGAR_JOURNAL_SCHEMA_VERSION,
        "sequence": sequence,
        "prior_entry_sha256": prior_entry_sha256,
        "candidates": list(candidates),
        "fingerprints": {
            cusip: fingerprints[cusip] for cusip in candidates
        },
        "checked_at": current.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "discovery": serialized,
        "refreshed_evidence": refreshed_cache,
        "source_pair_count": len(discovery.sources) // 2,
    }
    payload["entry_sha256"] = _canonical_json_hash(payload)
    return _validate_sec_edgar_batch_journal_entry(
        payload,
        sequence=sequence,
        prior_entry_sha256=prior_entry_sha256,
        candidates=candidates,
        fingerprints=fingerprints,
    )


def _validate_sec_edgar_batch_journal_entry(
    entry: object,
    *,
    sequence: int,
    prior_entry_sha256: str | None,
    candidates: list[str],
    fingerprints: dict[str, str],
) -> dict[str, object]:
    """Reject a stale, incomplete, reordered, or tampered EDGAR checkpoint."""

    required = {
        "schema_version",
        "sequence",
        "prior_entry_sha256",
        "candidates",
        "fingerprints",
        "checked_at",
        "discovery",
        "refreshed_evidence",
        "source_pair_count",
        "entry_sha256",
    }
    if not isinstance(entry, dict) or set(entry) != required:
        raise SecurityMasterRefreshError("malformed SEC EDGAR batch journal")
    candidate_entry = dict(entry)
    entry_sha256 = candidate_entry.pop("entry_sha256")
    if (
        not isinstance(entry_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", entry_sha256) is None
        or entry_sha256 != _canonical_json_hash(candidate_entry)
    ):
        raise SecurityMasterRefreshError(
            "SEC EDGAR batch journal checksum mismatch"
        )
    if (
        entry.get("schema_version") != _SEC_EDGAR_JOURNAL_SCHEMA_VERSION
        or entry.get("sequence") != sequence
        or entry.get("prior_entry_sha256") != prior_entry_sha256
        or entry.get("candidates") != candidates
        or entry.get("fingerprints")
        != {cusip: fingerprints[cusip] for cusip in candidates}
        or not _is_strict_utc_timestamp(entry.get("checked_at"))
        or type(entry.get("source_pair_count")) is not int
        or int(entry["source_pair_count"]) < 0
    ):
        raise SecurityMasterRefreshError(
            "SEC EDGAR batch journal does not match the active rebuild"
        )
    discovery = entry.get("discovery")
    if not isinstance(discovery, dict) or set(discovery) != {
        "sources",
        "diagnostics",
        "fetched_sources",
    }:
        raise SecurityMasterRefreshError(
            "SEC EDGAR batch journal has malformed discovery evidence"
        )
    diagnostics = discovery.get("diagnostics")
    fetched_sources = discovery.get("fetched_sources")
    sources = discovery.get("sources")
    diagnostic_cusips = (
        [item.get("cusip") for item in diagnostics]
        if isinstance(diagnostics, list)
        and all(isinstance(item, dict) for item in diagnostics)
        else []
    )
    if (
        not isinstance(diagnostics, list)
        or not all(isinstance(item, dict) for item in diagnostics)
        or any(not isinstance(cusip, str) for cusip in diagnostic_cusips)
        or sorted(diagnostic_cusips) != sorted(candidates)
        or len(set(diagnostic_cusips)) != len(candidates)
        or not isinstance(fetched_sources, list)
        or not all(isinstance(item, dict) for item in fetched_sources)
        or not isinstance(sources, list)
        or not all(isinstance(item, dict) for item in sources)
        or not isinstance(entry.get("refreshed_evidence"), dict)
    ):
        raise SecurityMasterRefreshError(
            "SEC EDGAR batch journal has incomplete discovery evidence"
        )
    refreshed_evidence = entry["refreshed_evidence"]
    if refreshed_evidence:
        try:
            merge_sec_edgar_evidence_caches({}, refreshed_evidence)
        except Exception as exc:
            raise SecurityMasterRefreshError(
                "SEC EDGAR batch journal has invalid filing evidence"
            ) from exc
    return dict(entry)


def _sec_edgar_journal_path(root: Path, sequence: int) -> Path:
    return Path(root) / f"{_SEC_EDGAR_JOURNAL_FILE_PREFIX}{sequence:06d}.json"


def _clear_sec_edgar_batch_journal(root: Path) -> None:
    """Remove only recognized nonpublishable EDGAR journal artifacts."""

    changed = False
    for child in Path(root).iterdir():
        if not _is_sec_edgar_journal_managed_file(child):
            continue
        if child.is_symlink() or not child.is_file():
            raise SecurityMasterRefreshError(
                "SEC rebuild EDGAR journal path must be a regular file: "
                + child.name
            )
        child.unlink()
        changed = True
    if changed:
        _fsync_directory(Path(root))


def _load_sec_edgar_batch_journal(
    root: Path,
    batches: list[list[str]],
    fingerprints: dict[str, str],
) -> list[dict[str, object]]:
    """Load the durable contiguous prefix of an interrupted clean rebuild."""

    root = Path(root)
    paths = sorted(
        (
            child
            for child in root.iterdir()
            if _SEC_EDGAR_JOURNAL_FILE_RE.fullmatch(child.name)
        ),
        key=lambda child: child.name,
    )
    entries: list[dict[str, object]] = []
    prior_entry_sha256: str | None = None
    try:
        if len(paths) > len(batches):
            raise SecurityMasterRefreshError(
                "SEC EDGAR batch journal exceeds the active candidate set"
            )
        for sequence, path in enumerate(paths):
            match = _SEC_EDGAR_JOURNAL_FILE_RE.fullmatch(path.name)
            if (
                match is None
                or int(match.group("sequence")) != sequence
                or path.is_symlink()
                or not path.is_file()
                or path.stat().st_mode & 0o7777 != _SEC_PRIVATE_FILE_MODE
            ):
                raise SecurityMasterRefreshError(
                    "SEC EDGAR batch journal is not a contiguous regular-file prefix"
                )
            try:
                raw_entry = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise SecurityMasterRefreshError(
                    "SEC EDGAR batch journal is unreadable"
                ) from exc
            entry = _validate_sec_edgar_batch_journal_entry(
                raw_entry,
                sequence=sequence,
                prior_entry_sha256=prior_entry_sha256,
                candidates=batches[sequence],
                fingerprints=fingerprints,
            )
            entries.append(entry)
            prior_entry_sha256 = str(entry["entry_sha256"])
    except SecurityMasterRefreshError as exc:
        # These files are nonpublishable work products. A stale or torn
        # journal is safe to discard because no state/master pair has yet
        # incorporated it; the exact SEC batches will simply be fetched again.
        log.warning("  discarded unusable SEC EDGAR batch journal: %s", exc)
        _clear_sec_edgar_batch_journal(root)
        return []
    return entries


def _append_sec_edgar_batch_journal(
    root: Path,
    entry: dict[str, object],
) -> None:
    """Atomically commit one hash-chained EDGAR batch before advancing."""

    sequence = int(entry["sequence"])
    path = _sec_edgar_journal_path(root, sequence)
    if path.exists() or path.is_symlink():
        raise SecurityMasterRefreshError(
            "SEC EDGAR batch journal would overwrite an existing checkpoint"
        )
    _atomic_write_json(path, entry, indent=None, sort_keys=True)
    if (
        path.is_symlink()
        or not path.is_file()
        or path.stat().st_mode & 0o7777 != _SEC_PRIVATE_FILE_MODE
    ):
        raise SecurityMasterRefreshError(
            "SEC EDGAR batch journal was not committed as a private regular file"
        )


def _apply_sec_edgar_batch_journal(
    result,
    universe: list[dict[str, str]],
    entries: list[dict[str, object]],
):
    """Apply all compact EDGAR batches with one full state copy/pair publish."""

    if not entries:
        return result
    candidate_state = json.loads(json.dumps(result.state))
    prior_discovery = candidate_state.get("edgar_discovery", {})
    prior_records = (
        prior_discovery.get("records", {})
        if isinstance(prior_discovery, dict)
        else {}
    )
    records = {
        str(cusip): dict(record)
        for cusip, record in (
            prior_records.items() if isinstance(prior_records, dict) else []
        )
        if isinstance(record, dict)
    }
    for record in records.values():
        if "last_successful_check_at" not in record:
            record["last_successful_check_at"] = (
                record.get("checked_at")
                if record.get("terminal") is True
                and record.get("status") != "transient_error"
                and _is_strict_utc_timestamp(record.get("checked_at"))
                else None
            )
    prior_fetches = (
        prior_discovery.get("fetched_sources", {})
        if isinstance(prior_discovery, dict)
        else {}
    )
    fetched_sources = {
        str(url): dict(record)
        for url, record in (
            prior_fetches.items() if isinstance(prior_fetches, dict) else []
        )
        if isinstance(record, dict)
    }
    terminal_cusips: set[str] = set()
    all_candidates: list[str] = []
    fetched_urls: set[str] = set(result.refreshed_urls)
    source_pair_count = 0
    checked_values: list[str] = []
    for entry in entries:
        checked_at = str(entry["checked_at"])
        checked_values.append(checked_at)
        all_candidates.extend(str(cusip) for cusip in entry["candidates"])
        source_pair_count += int(entry["source_pair_count"])
        serialized = entry["discovery"]
        for diagnostic in serialized["diagnostics"]:
            cusip = str(diagnostic["cusip"])
            prior_record = records.get(cusip, {})
            last_successful_check_at = (
                checked_at
                if diagnostic.get("terminal") is True
                and diagnostic.get("status") != "transient_error"
                else (
                    prior_record.get("last_successful_check_at")
                    if isinstance(prior_record, dict)
                    else None
                )
            )
            if (
                last_successful_check_at is None
                and isinstance(prior_record, dict)
                and prior_record.get("terminal") is True
                and prior_record.get("status") != "transient_error"
                and _is_strict_utc_timestamp(prior_record.get("checked_at"))
            ):
                # Nested discovery schema v1 used checked_at as its only
                # success clock. Preserve it across a transient v2 attempt.
                last_successful_check_at = prior_record["checked_at"]
            records[cusip] = {
                **diagnostic,
                "record_sha256": entry["fingerprints"][cusip],
                "checked_at": checked_at,
                "last_successful_check_at": last_successful_check_at,
            }
            if diagnostic.get("terminal") is True:
                terminal_cusips.add(cusip)
        for fetched in serialized["fetched_sources"]:
            fetched_sources[fetched["url"]] = fetched
            if fetched.get("outcome") == "fetched":
                fetched_urls.add(fetched["url"])

    candidate_state["edgar_discovery"] = {
        "schema_version": 2,
        "records": {cusip: records[cusip] for cusip in sorted(records)},
        "fetched_sources": {
            url: fetched_sources[url] for url in sorted(fetched_sources)
        },
    }
    existing_cache = candidate_state.get("edgar_evidence", {})
    retired_urls: set[str] = set()
    if isinstance(existing_cache, dict):
        existing_records = existing_cache.get("records", {})
        if isinstance(existing_records, dict):
            for cusip in terminal_cusips:
                prior_record = existing_records.get(cusip)
                if not isinstance(prior_record, dict):
                    continue
                for field in ("schedule_13dg_url", "ixbrl_url"):
                    url = str(prior_record.get(field) or "")
                    if url:
                        retired_urls.add(url)
            # Shared periodic filings remain while any record outside this
            # complete revalidation set still depends on them.
            for cusip, prior_record in existing_records.items():
                if cusip in terminal_cusips or not isinstance(prior_record, dict):
                    continue
                retired_urls.discard(
                    str(prior_record.get("schedule_13dg_url") or "")
                )
                retired_urls.discard(str(prior_record.get("ixbrl_url") or ""))

    merged_cache = existing_cache if isinstance(existing_cache, dict) else {}
    latest_checked_at = max(checked_values)
    latest_checked = datetime.strptime(
        latest_checked_at,
        "%Y-%m-%dT%H:%M:%SZ",
    ).replace(tzinfo=timezone.utc)
    if retired_urls:
        merged_cache = merge_sec_edgar_evidence_caches(
            merged_cache,
            {},
            retired_urls=retired_urls,
            refreshed_at=latest_checked,
        )
    for entry in entries:
        refreshed_cache = entry["refreshed_evidence"]
        if refreshed_cache:
            entry_checked = datetime.strptime(
                str(entry["checked_at"]),
                "%Y-%m-%dT%H:%M:%SZ",
            ).replace(tzinfo=timezone.utc)
            merged_cache = merge_sec_edgar_evidence_caches(
                merged_cache,
                refreshed_cache,
                refreshed_at=entry_checked,
            )
    if merged_cache != existing_cache:
        candidate_state["edgar_evidence"] = merged_cache
    candidate_state["updated_at"] = max(
        latest_checked_at,
        str(candidate_state.get("updated_at") or ""),
    )

    policy = result.master.get("policy", {})
    rebuilt = rebuild_sec_security_master(
        candidate_state,
        universe,
        recent_window_days=int(policy.get("recent_window_days", 31)),
        max_evidence_age_days=int(policy.get("max_evidence_age_days", 395)),
        min_confirmation_dates=int(policy.get("min_confirmation_dates", 2)),
    )
    # Store the fingerprint of the post-application record. Otherwise a new
    # exact bridge would cause one redundant discovery run on the next day.
    _, rebuilt_fingerprints = _sec_edgar_discovery_candidates(
        rebuilt,
        {},
        as_of=latest_checked,
        max_candidates=None,
    )
    discovery_records = candidate_state["edgar_discovery"]["records"]
    for cusip in all_candidates:
        if cusip in rebuilt_fingerprints and cusip in discovery_records:
            discovery_records[cusip]["record_sha256"] = rebuilt_fingerprints[cusip]
    # The post-application fingerprints above are authoritative state, not a
    # transient queue projection. Rebuild from that final state so the
    # master's source_state_sha256 binds the exact state that the pair
    # transaction will publish. Merely replacing the digest here would hide a
    # future dependency on discovery state and could publish a master derived
    # from a different state than the one beside it.
    del rebuilt
    rebuilt = rebuild_sec_security_master(
        candidate_state,
        universe,
        recent_window_days=int(policy.get("recent_window_days", 31)),
        max_evidence_age_days=int(policy.get("max_evidence_age_days", 395)),
        min_confirmation_dates=int(policy.get("min_confirmation_dates", 2)),
    )
    _, final_fingerprints = _sec_edgar_discovery_candidates(
        rebuilt,
        {},
        as_of=latest_checked,
        max_candidates=None,
    )
    unstable_fingerprints = sorted(
        cusip
        for cusip in all_candidates
        if cusip in final_fingerprints
        and cusip in discovery_records
        and discovery_records[cusip].get("record_sha256")
        != final_fingerprints[cusip]
    )
    if unstable_fingerprints:
        raise SecurityMasterRefreshError(
            "SEC EDGAR post-application fingerprints did not reach a stable "
            "state: "
            + ", ".join(unstable_fingerprints)
        )
    acceptance = audit_security_master(
        rebuilt,
        prior_master=result.master,
        as_of=latest_checked,
    )
    if not acceptance["ok"]:
        raise SecurityMasterRefreshError(
            "SEC EDGAR exception evidence failed the security-master gate: "
            + ", ".join(acceptance["issues"])
        )
    log.info(
        "  SEC EDGAR exceptions: %s checked; %s exact filing pair(s); "
        "%s resolved master record(s) total (net change %+d)",
        len(all_candidates),
        source_pair_count,
        rebuilt.get("summary", {}).get("resolved", 0),
        (
            rebuilt.get("summary", {}).get("resolved", 0)
            - result.master.get("summary", {}).get("resolved", 0)
        ),
    )
    return replace(
        result,
        master=rebuilt,
        state=candidate_state,
        changed=True,
        refreshed_urls=tuple(sorted(fetched_urls)),
        acceptance=acceptance,
    )


def _persist_sec_edgar_result_pair(
    prior_result,
    refreshed_result,
    *,
    master_path: Path,
    source_state_path: Path,
) -> None:
    """Crash-safely publish one validated state/master pair."""

    del prior_result
    save_security_master_pair(
        refreshed_result.master,
        refreshed_result.state,
        master_path=master_path,
        source_state_path=source_state_path,
    )


def _refresh_sec_edgar_exceptions(
    result,
    universe: list[dict[str, str]],
    *,
    refreshed_at: datetime | None = None,
    fetcher=None,
    master_path: Path = SEC_SECURITY_MASTER_PATH,
    source_state_path: Path = SEC_SOURCE_STATE_PATH,
    checkpoint_batches: bool = False,
    checkpoint_root: Path | None = None,
    _candidate_cusips: tuple[str, ...] | None = None,
):
    """Discover and atomically apply exact Schedule 13D/G -> iXBRL bridges.

    A clean rebuild journals only compact, checksummed batch results. The
    hundreds-of-megabytes source state and larger derived master are copied
    and persisted once after all batches finish. The final state is rebuilt
    again after its post-application discovery fingerprints are known, so the
    published master remains digest-bound without performing
    O(batch_count * full_pair_size) local work. Thus a cooperative timeout can
    resume without refetching accepted batches.
    """

    current = refreshed_at or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc).replace(microsecond=0)
    candidates, fingerprints = _sec_edgar_discovery_candidates(
        result.master,
        result.state,
        as_of=current,
        max_candidates=(
            _SEC_EDGAR_CLEAN_CANDIDATE_LIMIT
            if checkpoint_batches
            else _SEC_EDGAR_INCREMENTAL_CANDIDATE_LIMIT
        ),
    )
    if _candidate_cusips is not None:
        eligible = set(candidates)
        candidates = [cusip for cusip in _candidate_cusips if cusip in eligible]
    if not candidates:
        return result
    discovery_fetcher = fetcher or make_sec_discovery_fetcher(USER_AGENT)
    use_batches = checkpoint_batches and _candidate_cusips is None
    chunk_size = _SEC_EDGAR_CLEAN_CHUNK_SIZE if use_batches else len(candidates)
    batches = [
        candidates[offset : offset + chunk_size]
        for offset in range(0, len(candidates), chunk_size)
    ]
    entries: list[dict[str, object]] = []
    if use_batches and checkpoint_root is not None:
        entries = _load_sec_edgar_batch_journal(
            checkpoint_root,
            batches,
            fingerprints,
        )
        if entries:
            log.info(
                "  resumed %s/%s SEC EDGAR exception batch checkpoint(s)",
                len(entries),
                len(batches),
            )
    prior_entry_sha256 = (
        str(entries[-1]["entry_sha256"]) if entries else None
    )
    for sequence in range(len(entries), len(batches)):
        entry = _sec_edgar_batch_journal_payload(
            batches[sequence],
            fingerprints,
            sequence=sequence,
            prior_entry_sha256=prior_entry_sha256,
            current=current,
            discovery_fetcher=discovery_fetcher,
        )
        if use_batches and checkpoint_root is not None:
            _append_sec_edgar_batch_journal(checkpoint_root, entry)
        entries.append(entry)
        prior_entry_sha256 = str(entry["entry_sha256"])
        if use_batches:
            log.info(
                "  SEC EDGAR exception checkpoint: %s/%s batches",
                sequence + 1,
                len(batches),
            )

    refreshed = _apply_sec_edgar_batch_journal(result, universe, entries)
    _persist_sec_edgar_result_pair(
        result,
        refreshed,
        master_path=master_path,
        source_state_path=source_state_path,
    )
    return refreshed


def refresh_sec_security_master_from_funds(
    *,
    full_rebuild: bool = False,
    extra_holdings: list[dict] | None = None,
):
    """Refresh official SEC evidence and rebuild the complete local master."""

    reported_identity_resume_receipt = None
    resume_universe = None
    completed_reported_identity_receipt = None
    if full_rebuild:
        published_master, published_state = load_security_master_pair(
            master_path=SEC_SECURITY_MASTER_PATH,
            source_state_path=SEC_SOURCE_STATE_PATH,
        )
        published_sec_security_state = _has_published_sec_security_state(
            published_master,
            published_state,
        )
        del published_master, published_state
        before_backfill = reported_identity_backfill_audit(FUNDS_DIR)
        log.info(
            "  verifying immutable reported identity for %s retained "
            "holding(s) against SEC Form 13F evidence (%s incomplete)",
            before_backfill.get("holdings_scanned", 0),
            before_backfill.get("incomplete_holdings", 0),
        )
        if before_backfill.get("needed"):
            try:
                capacity = ensure_clean_rebuild_disk_space()
            except Exception as exc:
                raise SecurityMasterRefreshError(str(exc)) from exc
            log.info(
                "  SEC Form 13F clean-rebuild disk preflight: %.1f GiB free "
                "(%.1f GiB additional minimum; %.1f GiB resumable)",
                int(capacity["available_bytes"]) / 1024**3,
                int(capacity["minimum_free_bytes"]) / 1024**3,
                int(capacity["resumable_bytes"]) / 1024**3,
            )
        # Only an unfinished security-master attempt can authorize reuse of a
        # completed clean 13F generation. This includes a validated stage that
        # has not yet crossed the production promotion boundary.
        manifest_path = (
            SEC_SECURITY_MASTER_REBUILD_WORK_ROOT / "manifest.json"
        )
        try:
            possible_resume = json.loads(
                manifest_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            possible_resume = None
        if (
            not before_backfill.get("needed")
            and isinstance(possible_resume, dict)
            and possible_resume.get("status") in {"in_progress", "complete"}
        ):
            resume_universe = collect_security_master_universe(extra_holdings)
            reported_identity_resume_receipt = (
                _reported_identity_resume_receipt_for_in_progress_rebuild(
                    resume_universe
                )
            )
            if reported_identity_resume_receipt is not None:
                log.info(
                    "  resuming the completed verified SEC Form 13F index "
                    "owned by the interrupted security-master rebuild"
                )
        if (
            reported_identity_resume_receipt is None
        ):
            reported_identity_resume_receipt = (
                _legacy_cutover_completed_identity_receipt(
                    published_sec_security_state=published_sec_security_state,
                )
            )
            if reported_identity_resume_receipt is not None:
                log.info(
                    "  reusing the checksum-bound completed SEC Form 13F "
                    "index from an earlier legacy-cutover attempt"
                )
        if (
            published_sec_security_state
            and isinstance(reported_identity_resume_receipt, dict)
            and reported_identity_resume_receipt.get("receipt_scope")
            == LEGACY_INDEX_ADOPTION_RECEIPT_SCOPE
        ):
            # A staged-workspace manifest is private but not an authority to
            # cross the one-time cutover boundary. Fall back to a fresh rebuild
            # if it ever contains the migration-only receipt shape.
            reported_identity_resume_receipt = None
        try:
            backfill_kwargs = {"user_agent": USER_AGENT}
            if reported_identity_resume_receipt is not None:
                backfill_kwargs["completed_rebuild_receipt"] = (
                    reported_identity_resume_receipt
                )
                if (
                    reported_identity_resume_receipt.get("receipt_scope")
                    == LEGACY_INDEX_ADOPTION_RECEIPT_SCOPE
                ):
                    backfill_kwargs[
                        "allow_unpublished_legacy_index_adoption"
                    ] = True
            backfill = rebuild_reported_identity_from_sec(
                FUNDS_DIR, **backfill_kwargs
            )
        except Exception as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise
            raise SecurityMasterRefreshError(
                "SEC Form 13F reported-identity backfill failed"
            ) from exc
        after_backfill = reported_identity_backfill_audit(FUNDS_DIR)
        if after_backfill.get("needed"):
            missing = after_backfill.get("missing_or_invalid_fields", {})
            unresolved = (
                len(backfill.archive_fallback.unresolved)
                if backfill.archive_fallback is not None
                else 0
            )
            raise SecurityMasterRefreshError(
                "SEC Form 13F reported-identity backfill remained incomplete: "
                f"{after_backfill.get('incomplete_holdings', 0)} holding(s), "
                f"{unresolved} accession fallback target(s), fields={missing}"
            )
        candidate_receipt = getattr(
            backfill,
            "completed_rebuild_receipt",
            None,
        )
        if isinstance(candidate_receipt, dict):
            completed_reported_identity_receipt = candidate_receipt
        log.info(
            "  SEC Form 13F identity backfill changed %s holding(s) in %s "
            "fund file(s)",
            backfill.backfill.holdings_changed,
            backfill.backfill.files_changed,
        )

    if (
        resume_universe is not None
        and getattr(backfill.backfill, "files_changed", None) == 0
    ):
        universe = resume_universe
    else:
        universe = collect_security_master_universe(extra_holdings)
    if not universe:
        log.info("  no security identities found; SEC master refresh is a no-op")
        return None
    with SEC_SECURITY_REFRESH_LOCK:
        prior_master, prior_state = load_security_master_pair(
            master_path=SEC_SECURITY_MASTER_PATH,
            source_state_path=SEC_SOURCE_STATE_PATH,
        )
        staging_complete = False
        working_root = None
        if full_rebuild:
            # An explicit rebuild over an established SEC master must not
            # inherit a stage that production already matches. A failed
            # promotion (or a legacy-cutover retry) may reuse its complete,
            # fingerprint-bound stage while production still matches the
            # recorded base. In-progress work remains resumable in either
            # case, and promotion occurs only after all publication gates pass
            # against the prior master.
            working_root, staging_complete = (
                _prepare_security_master_rebuild_work(
                    universe,
                    production_master=prior_master,
                    production_state=prior_state,
                    force_fresh_completed=_has_published_sec_security_state(
                        prior_master,
                        prior_state,
                    ),
                )
            )
            working_master_path = working_root / "sec_security_master.json"
            working_source_state_path = working_root / "sec_source_state.json"
            if not staging_complete:
                _record_reported_identity_rebuild_receipt(
                    working_root,
                    completed_reported_identity_receipt,
                )
        else:
            working_master_path = SEC_SECURITY_MASTER_PATH
            working_source_state_path = SEC_SOURCE_STATE_PATH

        try:
            try:
                if staging_complete:
                    staged_master, staged_state = load_security_master_pair(
                        master_path=working_master_path,
                        source_state_path=working_source_state_path,
                    )
                    result = SecSecurityMasterRefreshResult(
                        master=staged_master,
                        state=staged_state,
                        changed=False,
                        refreshed_urls=(),
                        retained_urls=tuple(
                            sorted(staged_state.get("sources", {}))
                        ),
                        errors=(),
                        acceptance=audit_security_master(
                            staged_master,
                            prior_master=(
                                prior_master if prior_master.get("audit") else None
                            ),
                            as_of=datetime.now(timezone.utc),
                        ),
                    )
                else:
                    result = refresh_security_master(
                        universe,
                        master_path=working_master_path,
                        source_state_path=working_source_state_path,
                        # A clean rebuild discovers and parses the full
                        # 2004-present archive set from empty state. Incremental
                        # runs retain a compact rolling validation window.
                        lookback_months=None if full_rebuild else 24,
                        recheck_recent_archives=2,
                        minimum_current_symbol_population_by_kind=(
                            PRODUCTION_MIN_CURRENT_SYMBOL_POPULATION_BY_KIND
                        ),
                        minimum_current_symbol_title_ratio=(
                            PRODUCTION_MIN_CURRENT_SYMBOL_TITLE_RATIO
                        ),
                        minimum_active_official_cusip_count=(
                            PRODUCTION_MIN_ACTIVE_OFFICIAL_CUSIP_COUNT
                        ),
                        enforce_latest_completed_official_period=True,
                        enforce_reported_identity_evidence=True,
                    )
            except SecurityMasterAcceptanceError as exc:
                issues = set(exc.audit.get("issues", []))
                can_defer_new_filter_evidence = (
                    not full_rebuild
                    and issues == {"ftd_filter_universe_incomplete"}
                    and bool(prior_master.get("audit"))
                )
                if not can_defer_new_filter_evidence:
                    raise

                # A newly observed repo-only CUSIP can require old filtered FTD
                # archives to be fetched again. A transient archive outage must
                # not block an otherwise valid filing update: roll back the
                # partially checkpointed candidate state, retain every verified
                # mapping, and add the new exact identities as unresolved. The
                # unchanged source profile makes them retry automatically on the
                # next normal run.
                policy = prior_master.get("policy", {})
                fallback_master = rebuild_sec_security_master(
                    prior_state,
                    universe,
                    recent_window_days=int(
                        policy.get("recent_window_days", 31)
                    ),
                    max_evidence_age_days=int(
                        policy.get("max_evidence_age_days", 395)
                    ),
                    min_confirmation_dates=int(
                        policy.get("min_confirmation_dates", 2)
                    ),
                )
                fallback_acceptance = audit_security_master(
                    fallback_master,
                    prior_master=prior_master,
                    as_of=datetime.now(timezone.utc),
                )
                if not fallback_acceptance["ok"]:
                    raise SecurityMasterRefreshError(
                        "last-good SEC security master could not accept new "
                        "unresolved identities: "
                        + ", ".join(fallback_acceptance["issues"])
                    ) from exc
                save_security_master_pair(
                    fallback_master,
                    prior_state,
                    master_path=SEC_SECURITY_MASTER_PATH,
                    source_state_path=SEC_SOURCE_STATE_PATH,
                )
                result = SecSecurityMasterRefreshResult(
                    master=fallback_master,
                    state=prior_state,
                    changed=(fallback_master != prior_master),
                    refreshed_urls=(),
                    retained_urls=(),
                    errors=(
                        "deferred incomplete FTD filter refresh; new "
                        "identifiers remain unresolved",
                    ),
                    acceptance=fallback_acceptance,
                )
            if not result.errors and not staging_complete:
                supplemental_paths = (
                    {
                        "master_path": working_master_path,
                        "source_state_path": working_source_state_path,
                    }
                    if full_rebuild
                    else {}
                )
                try:
                    result = _refresh_sec_fund_series_evidence(
                        result,
                        universe,
                        **supplemental_paths,
                    )
                except Exception as exc:
                    if isinstance(exc, KeyboardInterrupt):
                        raise
                    if isinstance(exc, SourceSchemaChangeError):
                        if not full_rebuild:
                            save_security_master_pair(
                                prior_master,
                                prior_state,
                                master_path=SEC_SECURITY_MASTER_PATH,
                                source_state_path=SEC_SOURCE_STATE_PATH,
                            )
                        raise
                    if full_rebuild:
                        raise
                    # Series/class names are display enrichment. A delayed
                    # registrant page must not evict exact ticker mappings or
                    # other last-good SEC evidence; the weekly run will retry it.
                    log.warning(
                        "  SEC fund-series refresh retained last-good "
                        "evidence: %s",
                        exc,
                    )
                if not result.errors:
                    try:
                        result = _refresh_sec_edgar_exceptions(
                            result,
                            universe,
                            checkpoint_batches=full_rebuild,
                            checkpoint_root=(
                                working_root if full_rebuild else None
                            ),
                            **supplemental_paths,
                        )
                    except Exception as exc:
                        if isinstance(exc, KeyboardInterrupt):
                            raise
                        if isinstance(exc, SourceSchemaChangeError):
                            if not full_rebuild:
                                save_security_master_pair(
                                    prior_master,
                                    prior_state,
                                    master_path=SEC_SECURITY_MASTER_PATH,
                                    source_state_path=SEC_SOURCE_STATE_PATH,
                                )
                            raise
                        if full_rebuild:
                            raise
                        # EDGAR is an exception resolver. A delayed search,
                        # submissions response, or filing document must not
                        # evict the accepted FTD master.
                        log.warning(
                            "  SEC EDGAR exception refresh retained last-good "
                            "evidence: %s",
                            exc,
                        )
            else:
                # The core refresh deliberately rolled back to one coherent
                # prior SEC source set. Do not mix supplemental evidence into
                # that state or resolve a new identity during the same failed-
                # source run.
                log.info(
                    "  supplemental SEC evidence refresh deferred until all "
                    "core SEC sources refresh successfully"
                )

            if full_rebuild:
                if result.errors:
                    raise SecurityMasterRefreshError(
                        "clean SEC security-master rebuild retained no "
                        "supplemental last-good state: "
                        + "; ".join(result.errors)
                    )
                discovery = result.state.get("edgar_discovery", {})
                discovery_records = (
                    discovery.get("records", {})
                    if isinstance(discovery, dict)
                    else None
                )
                if not isinstance(discovery_records, dict):
                    raise SecurityMasterRefreshError(
                        "clean SEC security-master rebuild produced malformed "
                        "EDGAR discovery state"
                    )
                transient_edgar = sorted(
                    (
                        f"{cusip}:"
                        f"{record.get('reason') or 'transient_error'}"
                        if isinstance(record, dict)
                        else f"{cusip}:malformed_record"
                    )
                    for cusip, record in discovery_records.items()
                    if not isinstance(record, dict)
                    or record.get("terminal") is not True
                    or record.get("status") == "transient_error"
                )
                if transient_edgar:
                    # EDGAR is optional exception enrichment, not part of the
                    # core FTD/list publication boundary. A transient search
                    # or filing fetch therefore leaves the affected identity
                    # unresolved and tickerless; schema drift and stale
                    # already-published iXBRL proof remain fatal through the
                    # dedicated exception handling and audit gates above.
                    log.warning(
                        "  clean SEC security-master rebuild deferred %s "
                        "transient EDGAR exception(s); affected identities "
                        "remain unresolved and tickerless",
                        len(transient_edgar),
                    )
                staged_records = result.master.get("records", {})
                if not staged_records:
                    raise SecurityMasterRefreshError(
                        "full SEC security-master rebuild produced no records"
                    )
                staged_audit = audit_security_master(
                    result.master,
                    prior_master=(
                        prior_master if prior_master.get("audit") else None
                    ),
                    as_of=datetime.now(timezone.utc),
                )
                if not staged_audit["ok"]:
                    raise SecurityMasterRefreshError(
                        "SEC security-master acceptance gate failed: "
                        + ", ".join(staged_audit["issues"])
                    )
                result = replace(result, acceptance=staged_audit)
                if working_root is None:
                    raise SecurityMasterRefreshError(
                        "clean rebuild has no persistent workspace"
                    )
                # Commit the validated staged identity before promotion.  A
                # crash after this point can resume the exact completed stage
                # instead of treating its terminal EDGAR fingerprints as a
                # new queue and producing a crash-dependent result.
                _mark_security_master_rebuild_complete(working_root)
                save_security_master_pair(
                    result.master,
                    result.state,
                    master_path=SEC_SECURITY_MASTER_PATH,
                    source_state_path=SEC_SOURCE_STATE_PATH,
                )
        finally:
            pass
    for error in result.errors:
        log.warning("  SEC security source retained last-good state: %s", error)
    records = result.master.get("records", {})
    if full_rebuild and not records:
        raise SecurityMasterRefreshError(
            "full SEC security-master rebuild produced no records"
        )
    audit = audit_security_master(
        result.master,
        prior_master=(prior_master if prior_master.get("audit") else None),
        as_of=datetime.now(timezone.utc),
    )
    if not audit["ok"]:
        ixbrl_freshness_failed = any(
            issue in {
                "sec_ixbrl_source_date_unavailable",
                "sec_ixbrl_source_is_stale",
            }
            for issue in audit["issues"]
        )
        if ixbrl_freshness_failed and not full_rebuild:
            save_security_master_pair(
                prior_master,
                prior_state,
                master_path=SEC_SECURITY_MASTER_PATH,
                source_state_path=SEC_SOURCE_STATE_PATH,
            )
        raise SecurityMasterRefreshError(
            "SEC security-master acceptance gate failed: "
            + ", ".join(audit["issues"])
        )
    summary = result.master.get("summary", {})
    log.info(
        "  SEC security master: %s records; %s resolved; %s ambiguous; "
        "%s malformed",
        len(records),
        summary.get("resolved", 0),
        summary.get("ambiguous", 0),
        summary.get("malformed_as_filed", 0),
    )
    log.info(
        "  current official-list FTD coverage: %.2f%% (%s/%s)",
        100 * audit["ftd_coverage_ratio"],
        audit["ftd_evidenced_official_cusip_count"],
        audit["active_non_option_official_cusip_count"],
    )
    return result


def resolve_cusips_via_sec_security_master(
    cusips: list[str],
    *,
    holdings: list[dict] | None = None,
    master: dict | None = None,
) -> dict[str, str]:
    """Resolve exact security identities from persisted SEC evidence only."""

    requested = {
        normalize_security_identifier(cusip)
        for cusip in cusips
        if normalize_security_identifier(cusip)
    }
    if not requested:
        return {}
    loaded = master or load_security_master(SEC_SECURITY_MASTER_PATH)
    types_by_cusip: dict[str, set[str]] = defaultdict(set)
    for holding in holdings or []:
        cusip = normalize_security_identifier(
            holding.get("reported_cusip") or holding.get("cusip")
        )
        if cusip in requested:
            instrument_type = holding_instrument_type(holding)
            types_by_cusip[cusip].add(instrument_type)
            if instrument_type in {"CALL", "PUT", "OPT"}:
                types_by_cusip[cusip].add("EQUITY")
    records = loaded.get("records", {})
    # The legacy compatibility map has only one slot per CUSIP. Consider every
    # exact identity already known to the master even when this caller happens
    # to be updating one quarter/type; otherwise an EQUITY-only call could
    # resurrect a broad ticker while a retained PREF or WARRANT sibling is
    # unresolved.
    for record in records.values():
        if not isinstance(record, dict):
            continue
        cusip = normalize_security_identifier(record.get("cusip"))
        if cusip in requested:
            raw_type = str(record.get("instrument_type") or "").upper()
            if raw_type in VALID_INSTRUMENT_TYPES:
                types_by_cusip[cusip].add(raw_type)

    resolved: dict[str, str] = {}
    for cusip in sorted(requested):
        tickers: set[str] = set()
        complete = True
        for instrument_type in types_by_cusip.get(cusip, {"EQUITY"}):
            evidence_type = (
                "EQUITY"
                if instrument_type in {"CALL", "PUT", "OPT"}
                else instrument_type
            )
            record = _resolve_loaded_security(loaded, cusip, evidence_type)
            ticker = str(record.get("ticker") or "").strip().upper()
            if record.get("mapping_status") != "resolved" or not ticker:
                complete = False
                break
            tickers.add(ticker)
        if complete and len(tickers) == 1:
            resolved[cusip] = next(iter(tickers))
    return resolved


def update_cusip_map(
    cusip_map: dict[str, str],
    holdings: list[dict],
) -> None:
    """Apply exact SEC-master mappings; unsupported identities fail closed."""

    cusips = sorted({
        normalize_security_identifier(
            holding.get("reported_cusip") or holding.get("cusip")
        )
        for holding in holdings
        if normalize_security_identifier(
            holding.get("reported_cusip") or holding.get("cusip")
        )
    })
    master = load_security_master(SEC_SECURITY_MASTER_PATH)
    resolved = resolve_cusips_via_sec_security_master(
        cusips,
        holdings=holdings,
        master=master,
    )
    # Do not allow a caller-provided legacy mapping to survive without an exact
    # record in the SEC master.
    for cusip in cusips:
        if cusip in resolved:
            cusip_map[cusip] = resolved[cusip]
        else:
            cusip_map.pop(cusip, None)

    for holding in holdings:
        cusip = normalize_security_identifier(
            holding.get("reported_cusip") or holding.get("cusip")
        )
        instrument_type = holding_instrument_type(holding)
        evidence_type = (
            "EQUITY"
            if instrument_type in {"CALL", "PUT", "OPT"}
            else instrument_type
        )
        record = _resolve_loaded_security(master, cusip, evidence_type)
        ticker = (
            record.get("ticker")
            if record.get("mapping_status") == "resolved"
            else None
        )
        holding["ticker"] = display_ticker_for_holding_type(
            ticker,
            instrument_type,
        )

# ----------------------------------------------------------------------------
# Pipeline state (which accession numbers we've processed)
# ----------------------------------------------------------------------------

def load_state() -> dict:
    state = _load_json_dict_with_fallback(STATE_PATH, LEGACY_STATE_PATH)
    if state:
        if "processed" not in state:
            state["processed"] = []
        # Internally use a set for fast membership tests
        state["_processed_set"] = set(state["processed"])
        quarantined = state.get("quarantined", {})
        state["_quarantined"] = quarantined if isinstance(quarantined, dict) else {}
        migration_pending = state.get("amendment_migration_pending", {})
        state["amendment_migration_pending"] = (
            migration_pending if isinstance(migration_pending, dict) else {}
        )
        state["amendment_reducer_version"] = state.get(
            "amendment_reducer_version", 0
        )
        state["amendment_migration_last_retry"] = state.get(
            "amendment_migration_last_retry"
        )
        identity_pending = state.get(
            "security_identity_migration_pending", {}
        )
        state["security_identity_migration_pending"] = (
            identity_pending if isinstance(identity_pending, dict) else {}
        )
        state["security_identity_migration_version"] = state.get(
            "security_identity_migration_version", 0
        )
        state["security_identity_migration_last_retry"] = state.get(
            "security_identity_migration_last_retry"
        )
        quarter_health_pending = state.get("quarter_health_pending", {})
        state["quarter_health_pending"] = (
            quarter_health_pending
            if isinstance(quarter_health_pending, dict)
            else {}
        )
        state["quarter_health_last_retry"] = state.get(
            "quarter_health_last_retry"
        )
        state["value_unit_migration_version"] = state.get(
            "value_unit_migration_version", 0
        )
        return state
    return {
        "processed": [],
        "_processed_set": set(),
        "quarantined": {},
        "_quarantined": {},
        "amendment_migration_pending": {},
        "amendment_migration_last_retry": None,
        "security_identity_migration_pending": {},
        "security_identity_migration_last_retry": None,
        "security_identity_migration_version": 0,
        "quarter_health_pending": {},
        "quarter_health_last_retry": None,
        "value_unit_migration_version": 0,
        "last_run": None,
        "amendment_reducer_version": 0,
    }


def save_state(state: dict) -> None:
    quarantined = state.get("_quarantined", {})
    out = {
        "processed": sorted(state.get("_processed_set", set())),
        "quarantined": {
            accession: quarantined[accession]
            for accession in sorted(quarantined)
        },
        "amendment_migration_pending": {
            accession: state.get("amendment_migration_pending", {})[accession]
            for accession in sorted(state.get("amendment_migration_pending", {}))
        },
        "amendment_migration_last_retry": state.get(
            "amendment_migration_last_retry"
        ),
        "amendment_reducer_version": state.get("amendment_reducer_version", 0),
        "security_identity_migration_pending": {
            key: state.get("security_identity_migration_pending", {})[key]
            for key in sorted(
                state.get("security_identity_migration_pending", {})
            )
        },
        "security_identity_migration_last_retry": state.get(
            "security_identity_migration_last_retry"
        ),
        "security_identity_migration_version": state.get(
            "security_identity_migration_version", 0
        ),
        "quarter_health_pending": {
            key: state.get("quarter_health_pending", {})[key]
            for key in sorted(state.get("quarter_health_pending", {}))
        },
        "quarter_health_last_retry": state.get(
            "quarter_health_last_retry"
        ),
        "value_unit_migration_version": state.get(
            "value_unit_migration_version", 0
        ),
    }
    previous = _read_json_object(STATE_PATH)
    previous_semantic = dict(previous or {})
    previous_last_run = previous_semantic.pop("last_run", None)
    if previous_semantic == out and _is_strict_utc_timestamp(
        previous_last_run
    ):
        last_run = previous_last_run
    else:
        last_run = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out["last_run"] = last_run
    state["last_run"] = last_run
    _atomic_write_json(STATE_PATH, out)


# ----------------------------------------------------------------------------
# Per-fund file management
# ----------------------------------------------------------------------------

def quarantine_replay_failure(
    state: dict,
    cik: int,
    triggers: list[dict],
    error: FilingChainError | FilingParseError,
    state_lock: threading.Lock | None = None,
    *,
    reason_override: str | None = None,
) -> None:
    """Record a scoped failure while leaving its last-known-good fund intact."""
    lock = state_lock or threading.Lock()
    reason = reason_override or getattr(error, "reason", type(error).__name__)
    attempted_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with lock:
        quarantined = state.setdefault("_quarantined", {})
        processed = state.setdefault("_processed_set", set())
        for trigger in triggers:
            processed.discard(trigger["accession"])
            quarantined[trigger["accession"]] = {
                "cik": cik,
                "report_date": normalize_report_date(trigger.get("report_date")),
                "reason": reason,
                "message": str(error)[:500],
                "last_attempt_at": attempted_at,
            }


def accession_retry_due(
    state: dict,
    accession: str,
    *,
    now: datetime | None = None,
) -> bool:
    """Whether an unprocessed accession may be attempted automatically.

    Deterministic parse and filing-chain failures are durable, so they retain
    the weekly cooldown. An exhausted SEC filing-resource fetch is transient
    and may retry on the next daily run. Explicit repair and migration calls
    use ``force=True`` and bypass this cooldown.
    """
    quarantined = state.get("_quarantined", {})
    if not isinstance(quarantined, dict):
        return True
    diagnostic = quarantined.get(accession)
    if not isinstance(diagnostic, dict):
        return True
    raw_attempted_at = diagnostic.get("last_attempt_at")
    if not isinstance(raw_attempted_at, str) or not raw_attempted_at.strip():
        return True
    try:
        attempted_at = datetime.fromisoformat(
            raw_attempted_at.replace("Z", "+00:00")
        )
    except ValueError:
        return True
    if attempted_at.tzinfo is None:
        return True
    current = now or datetime.now(timezone.utc)
    interval_days = (
        FETCH_FAILURE_RETRY_INTERVAL_DAYS
        if diagnostic.get("reason") == FilingFetchError.__name__
        else QUARANTINE_RETRY_INTERVAL_DAYS
    )
    return current >= attempted_at + timedelta(
        days=interval_days
    )


def load_fund(cik: int) -> dict | None:
    p = FUNDS_DIR / f"{cik}.json"
    if p.exists():
        try:
            with open(p) as f:
                return json.load(f)
        except json.JSONDecodeError as exc:
            raise FundDataError(
                f"existing fund file is corrupt and will not be overwritten: {p}"
            ) from exc
    return None


def save_fund(cik: int, fund: dict) -> None:
    _atomic_write_json(FUNDS_DIR / f"{cik}.json", fund)


def merge_composed_quarters_into_fund(
    cik: int,
    name: str,
    quarters: list[dict],
    max_quarters: int,
    *,
    preserve_history: bool = True,
) -> dict:
    """Build a candidate fund snapshot without writing it.

    Existing report dates are replaced in place, while unrelated report dates
    are always retained. ``max_quarters`` and ``preserve_history`` remain in
    the signature for call compatibility; quarter limits govern SEC discovery,
    never stored-history retention.
    """
    fund = load_fund(cik) or {"cik": cik, "name": name, "quarters": []}
    fund = {
        **fund,
        "cik": cik,
        "name": name or fund.get("name") or "",
        "quarters": [dict(quarter) for quarter in fund.get("quarters", [])],
    }
    by_report_date = {
        quarter.get("report_date"): index
        for index, quarter in enumerate(fund["quarters"])
        if isinstance(quarter, dict)
    }
    for quarter in quarters:
        if (
            quarter.get("composition_version") != AMENDMENT_REDUCER_VERSION
            or quarter.get("is_complete") is not True
            or not quarter.get("base_accession")
        ):
            raise FilingChainError(
                "invalid_component", "refusing to persist an incomplete composed quarter"
            )
        report_date = quarter.get("report_date")
        existing_index = by_report_date.get(report_date)
        if existing_index is None:
            by_report_date[report_date] = len(fund["quarters"])
            fund["quarters"].append(quarter)
        else:
            fund["quarters"][existing_index] = quarter

    fund["quarters"].sort(
        key=lambda quarter: quarter.get("report_date") or "", reverse=True
    )
    return fund


# ----------------------------------------------------------------------------
# Stock files & search index (regenerated at end of every run)
# ----------------------------------------------------------------------------

# A CUSIP is considered the dominant claimant of a ticker when its cumulative
# holding value is at least this many times larger than the next CUSIP claiming
# the same ticker. Anything short of that is flagged as ambiguous and left
# alone — we'd rather keep a real multi-CUSIP case (e.g. a share-class split
# misclassified as common) than silently drop it.
# Stripped before tokenizing issuer names — drops corporate-entity noise so
# "WALT DISNEY CO/THE" and "DISNEY WALT CO" don't hinge on whether "CO" or
# "THE" happen to match.
@_serialize_pipeline_maintenance
def rebuild_tickers_in_place(
    *,
    full_refresh: bool = False,
    refresh_master: bool = False,
    company_ticker_data: dict | list | None = None,
) -> int:
    """Rewrite only ticker metadata from the exact SEC security master.

    A security-master refresh is not an identity migration.  In particular,
    it must not opportunistically reclassify a retained row while replacing a
    vendor ticker: the cutover invariant is keyed by ``CUSIP | instrument
    type``.  Dedicated filing replay/canonicalization paths own any proven
    identity repair.
    """

    _ = company_ticker_data
    if not FUNDS_DIR.exists():
        log.info("  no funds directory; nothing to rebuild")
        return 0
    log.info(
        "%s the SEC security master and stored holding metadata...",
        "Rebuilding" if full_refresh else "Refreshing",
    )
    result = (
        refresh_sec_security_master_from_funds(full_rebuild=full_refresh)
        if refresh_master or full_refresh
        else None
    )
    master = (
        result.master
        if result is not None
        else load_security_master(SEC_SECURITY_MASTER_PATH)
    )

    fund_paths = sorted(FUNDS_DIR.glob("*.json"))
    updated = 0
    reassigned = 0
    for index, fund_path in enumerate(fund_paths):
        if index and index % 2000 == 0:
            log.info(
                "    SEC rewrite progress: %s/%s files (%s changed)",
                index,
                len(fund_paths),
                updated,
            )
        try:
            fund = json.loads(fund_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        changed = False
        for quarter in fund.get("quarters", []):
            for holding in quarter.get("holdings", []):
                cusip = normalize_security_identifier(
                    holding.get("reported_cusip") or holding.get("cusip")
                )
                if not cusip:
                    continue
                instrument_type = holding_instrument_type(holding)
                resolution = _resolve_loaded_security(
                    master,
                    cusip,
                    instrument_type,
                )
                if (
                    resolution.get("mapping_status") != "resolved"
                    and instrument_type in {"CALL", "PUT", "OPT"}
                ):
                    resolution = _resolve_loaded_security(
                        master,
                        cusip,
                        "EQUITY",
                    )
                new_ticker = display_ticker_for_holding_type(
                    resolution.get("ticker")
                    if resolution.get("mapping_status") == "resolved"
                    else None,
                    instrument_type,
                )
                if holding.get("ticker") != new_ticker:
                    holding["ticker"] = new_ticker
                    changed = True
                    reassigned += 1
        if changed:
            _atomic_write_json(fund_path, fund)
            updated += 1

    log.info(
        "  updated %s/%s fund files (%s holding ticker changes) from "
        "SEC-only evidence",
        updated,
        len(fund_paths),
        reassigned,
    )
    return updated

def quarter_health_key(cik: int, report_date: str) -> str:
    normalized = normalize_report_date(report_date)
    if (
        not isinstance(cik, int)
        or cik <= 0
        or report_quarter_code(normalized) is None
    ):
        raise ValueError("quarter health target requires CIK and report date")
    return f"{cik}:{normalized}"


def _quarter_source_accessions(quarter: dict) -> list[str]:
    accessions = {
        str(accession).strip()
        for accession in quarter.get("applied_accessions", []) or []
        if str(accession).strip()
    }
    if not accessions:
        accessions.update(
            str(source.get("accession") or "").strip()
            for source in quarter.get("source_filings", []) or []
            if isinstance(source, dict) and source.get("applied") is not False
        )
    if not accessions:
        accession = str(quarter.get("accession") or "").strip()
        if accession:
            accessions.add(accession)
    return sorted(accession for accession in accessions if accession)


def _quarter_health_source_is_referenced(
    pending: dict,
    accession: str,
) -> bool:
    return any(
        accession in (target.get("source_accessions", []) or [])
        for target in pending.values()
        if isinstance(target, dict)
    )


def _verify_durable_quarter_health_queue(
    state: dict,
    affected: dict[int, set[str]],
) -> None:
    """Prove the retry queue reached disk before withholding fund data."""
    if not affected:
        return
    if FUNDS_DIR.resolve().parent != STATE_PATH.resolve().parent:
        raise FundDataError(
            "quarter-health state and fund data must share one data directory"
        )
    try:
        with open(STATE_PATH) as handle:
            persisted = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise FundDataError(
            "quarter-health queue was not durably readable before withholding"
        ) from exc
    if not isinstance(persisted, dict):
        raise FundDataError(
            "quarter-health queue was not durably persisted before withholding"
        )

    persisted_pending = persisted.get("quarter_health_pending", {})
    persisted_quarantined = persisted.get("quarantined", {})
    persisted_processed = set(persisted.get("processed", []) or [])
    in_memory_pending = state.get("quarter_health_pending", {})
    for cik, report_dates in affected.items():
        for report_date in report_dates:
            key = quarter_health_key(cik, report_date)
            expected = in_memory_pending.get(key)
            actual = (
                persisted_pending.get(key)
                if isinstance(persisted_pending, dict)
                else None
            )
            if (
                not isinstance(expected, dict)
                or not isinstance(actual, dict)
                or actual.get("cik") != cik
                or actual.get("report_date") != report_date
                or actual.get("source_accessions")
                != expected.get("source_accessions")
            ):
                raise FundDataError(
                    f"quarter-health target {key} was not durably queued"
                )
            for accession in expected.get("source_accessions", []) or []:
                diagnostic = (
                    persisted_quarantined.get(accession)
                    if isinstance(persisted_quarantined, dict)
                    else None
                )
                if (
                    accession in persisted_processed
                    or not isinstance(diagnostic, dict)
                    or diagnostic.get("reason") != "QuarterHealthError"
                ):
                    raise FundDataError(
                        f"quarter-health source {accession} was not durably "
                        "quarantined"
                    )


def inventory_published_quarter_health_issues(
) -> tuple[dict[tuple[int, str], list], set[tuple[int, str]]]:
    """Return every unhealthy published quarter and all valid published keys."""
    raw_peer_index = defaultdict(lambda: defaultdict(list))
    issues_by_key: dict[tuple[int, str], list] = defaultdict(list)
    published_keys: set[tuple[int, str]] = set()

    fund_paths = sorted(FUNDS_DIR.glob("*.json"))
    for path in fund_paths:
        try:
            with open(path) as handle:
                fund = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        cik = fund.get("cik") if isinstance(fund, dict) else None
        if not isinstance(cik, int) or cik <= 0:
            continue
        for quarter in fund.get("quarters", []) or []:
            if not isinstance(quarter, dict):
                continue
            report_date = normalize_report_date(quarter.get("report_date"))
            if report_quarter_code(report_date) is None:
                continue
            key = (cik, report_date)
            published_keys.add(key)
            structural_issues = structural_quarter_health_issues(quarter)
            if structural_issues:
                issues_by_key[key].extend(structural_issues)
                continue
            add_quarter_peer_observations(
                raw_peer_index,
                filer_id=cik,
                quarter=quarter,
            )

    peer_index = compile_peer_price_index(raw_peer_index, consume=True)
    for path in fund_paths:
        try:
            with open(path) as handle:
                fund = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        cik = fund.get("cik") if isinstance(fund, dict) else None
        if not isinstance(cik, int) or cik <= 0:
            continue
        for quarter in fund.get("quarters", []) or []:
            if not isinstance(quarter, dict):
                continue
            report_date = normalize_report_date(quarter.get("report_date"))
            key = (cik, report_date)
            if key in issues_by_key or key not in published_keys:
                continue
            peer_prices = same_date_peer_price_references(
                peer_index,
                filer_id=cik,
                quarter=quarter,
            )
            issue = peer_price_quarter_health_issue(quarter, peer_prices)
            if issue is not None:
                issues_by_key[key].append(issue)
    return dict(issues_by_key), published_keys


@_serialize_pipeline_maintenance
def enforce_published_quarter_health(state: dict) -> int:
    """Withhold every unhealthy quarter and durably queue an SEC retry."""
    log.info("Checking published quarter health before output generation...")
    issues_by_key, published_keys = (
        inventory_published_quarter_health_issues()
    )
    pending = state.setdefault("quarter_health_pending", {})
    quarantined = state.setdefault("_quarantined", {})
    processed = state.setdefault("_processed_set", set())
    attempted_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Clear only targets that are visibly healthy again. An absent target
    # remains queued until authoritative SEC replay restores it.
    healthy_keys = published_keys - set(issues_by_key)
    for key, target in list(pending.items()):
        if not isinstance(target, dict):
            continue
        target_tuple = (
            target.get("cik"),
            normalize_report_date(target.get("report_date")),
        )
        if target_tuple not in healthy_keys:
            continue
        pending.pop(key, None)
        for accession in target.get("source_accessions", []) or []:
            if _quarter_health_source_is_referenced(pending, accession):
                continue
            processed.add(accession)
            diagnostic = quarantined.get(accession)
            if (
                isinstance(diagnostic, dict)
                and diagnostic.get("reason") == "QuarterHealthError"
            ):
                quarantined.pop(accession, None)

    affected: dict[int, set[str]] = defaultdict(set)
    for path in sorted(FUNDS_DIR.glob("*.json")):
        try:
            with open(path) as handle:
                fund = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        cik = fund.get("cik") if isinstance(fund, dict) else None
        if not isinstance(cik, int) or cik <= 0:
            continue
        for quarter in fund.get("quarters", []) or []:
            if not isinstance(quarter, dict):
                continue
            report_date = normalize_report_date(quarter.get("report_date"))
            issues = issues_by_key.get((cik, report_date))
            if not issues:
                continue
            affected[cik].add(report_date)
            source_accessions = _quarter_source_accessions(quarter)
            message = "; ".join(
                f"{issue.code}: {issue.detail}" for issue in issues
            )[:500]
            key = quarter_health_key(cik, report_date)
            previous_target = pending.get(key)
            previous_sources = set(
                previous_target.get("source_accessions", []) or []
            ) if isinstance(previous_target, dict) else set()
            pending[key] = {
                "cik": cik,
                "report_date": report_date,
                "reason": ",".join(sorted({issue.code for issue in issues})),
                "message": message,
                "last_attempt_at": attempted_at,
                "source_accessions": source_accessions,
            }
            for obsolete_accession in previous_sources - set(source_accessions):
                if _quarter_health_source_is_referenced(
                    pending,
                    obsolete_accession,
                ):
                    continue
                processed.add(obsolete_accession)
                diagnostic = quarantined.get(obsolete_accession)
                if (
                    isinstance(diagnostic, dict)
                    and diagnostic.get("reason") == "QuarterHealthError"
                ):
                    quarantined.pop(obsolete_accession, None)
            for accession in source_accessions:
                processed.discard(accession)
                quarantined[accession] = {
                    "cik": cik,
                    "report_date": report_date,
                    "reason": "QuarterHealthError",
                    "message": message,
                    "last_attempt_at": attempted_at,
                }

    # Queue durability precedes destructive publication changes.
    save_state(state)
    _verify_durable_quarter_health_queue(state, affected)
    withheld = 0
    for cik, report_dates in sorted(affected.items()):
        fund = load_fund(cik)
        if not fund:
            continue
        before = len(fund.get("quarters", []))
        fund["quarters"] = [
            quarter
            for quarter in fund.get("quarters", [])
            if not (
                isinstance(quarter, dict)
                and quarter.get("report_date") in report_dates
            )
        ]
        removed = before - len(fund["quarters"])
        if removed:
            save_fund(cik, fund)
            withheld += removed

    if withheld:
        log.warning(
            "withheld %s unhealthy quarter(s); %s target(s) remain queued "
            "for automatic SEC replay",
            withheld,
            len(pending),
        )
    else:
        log.info(
            "  no unhealthy published quarters; %s target(s) remain withheld",
            len(pending),
        )
    return withheld


def quarter_health_retry_due(
    state: dict,
    *,
    now: datetime | None = None,
) -> bool:
    if not state.get("quarter_health_pending"):
        return False
    last_retry = state.get("quarter_health_last_retry")
    if not isinstance(last_retry, str) or not last_retry:
        return True
    try:
        parsed = datetime.fromisoformat(last_retry.replace("Z", "+00:00"))
    except ValueError:
        return True
    if parsed.tzinfo is None:
        return True
    current = now or datetime.now(timezone.utc)
    return current >= parsed + timedelta(
        days=QUARTER_HEALTH_RETRY_INTERVAL_DAYS
    )


@_serialize_pipeline_maintenance
def retry_pending_quarter_health(
    state: dict,
    cusip_map: dict[str, str],
) -> int:
    """Replay exact withheld dates weekly; the final health scan decides."""
    pending = state.setdefault("quarter_health_pending", {})
    by_cik: dict[int, list[dict]] = defaultdict(list)
    for key, raw_target in sorted(pending.items()):
        if not isinstance(raw_target, dict):
            log.error("malformed quarter health target %s", key)
            continue
        cik = raw_target.get("cik")
        report_date = normalize_report_date(raw_target.get("report_date"))
        if (
            not isinstance(cik, int)
            or cik <= 0
            or report_quarter_code(report_date) is None
            or key != quarter_health_key(cik, report_date)
        ):
            log.error("malformed quarter health target %s", key)
            continue
        by_cik[cik].append({"cik": cik, "report_date": report_date})
    if not by_cik:
        return 0

    log.info(
        "retrying %s withheld quarter-health target(s) across %s CIK(s)",
        sum(len(targets) for targets in by_cik.values()),
        len(by_cik),
    )
    replayed = 0
    state_lock = threading.Lock()
    for cik, targets in sorted(by_cik.items()):
        target_dates = {target["report_date"] for target in targets}
        try:
            discovered, name = _discover_submission_filings(
                cik, include_archives=False
            )
            if not target_dates.issubset(
                {row.get("report_date") for row in discovered}
            ):
                discovered, name = _discover_submission_filings(
                    cik, include_archives=True
                )
            triggers = [
                row for row in discovered
                if row.get("report_date") in target_dates
            ]
            if not triggers:
                raise FilingDiscoveryError(
                    "SEC submissions metadata has no filing chain for "
                    + ", ".join(sorted(target_dates))
                )
            replayed += replay_quarters_for_cik(
                cik,
                triggers,
                cusip_map,
                1,
                state,
                state_lock=state_lock,
                force=True,
                include_archives=True,
                preserve_history=True,
                quarantine_failures=True,
                quarantine_reason_override="QuarterHealthError",
                replace_only=True,
                discovered_submission=(discovered, name),
            )
        except Exception as exc:
            log.warning(
                "quarter health retry failed for CIK %s; targets remain "
                "withheld: %s",
                cik,
                exc,
            )
            now_text = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            for target in targets:
                key = quarter_health_key(cik, target["report_date"])
                if key in pending:
                    pending[key]["reason"] = "replay_failed"
                    pending[key]["message"] = str(exc)[:500]
                    pending[key]["last_attempt_at"] = now_text
    return replayed


def _active_unverified_targets_by_cik(
    state: dict,
) -> dict[int, dict[str, set[str]]]:
    """Return every unresolved report date and its reasons by filer."""
    targets: dict[int, dict[str, set[str]]] = {}

    def add_target(raw_target: object, reason: str) -> None:
        if not isinstance(raw_target, dict):
            return
        try:
            cik = int(raw_target.get("cik"))
        except (TypeError, ValueError):
            return
        report_date = normalize_report_date(raw_target.get("report_date"))
        if cik <= 0 or report_quarter_code(report_date) is None:
            return
        targets.setdefault(cik, {}).setdefault(report_date, set()).add(reason)

    quarantined = state.get("_quarantined")
    if not isinstance(quarantined, dict):
        quarantined = state.get("quarantined", {})
    for target in (quarantined or {}).values():
        add_target(target, "SEC filing verification pending")

    pending_sources = (
        ("amendment_migration_pending", "amendment verification pending"),
        (
            "security_identity_migration_pending",
            "security identity verification pending",
        ),
        ("quarter_health_pending", "quarter data-quality verification pending"),
    )
    for field, reason in pending_sources:
        pending = state.get(field, {})
        if not isinstance(pending, dict):
            continue
        for target in pending.values():
            add_target(target, reason)
    return targets


def _active_withheld_targets_by_cik(state: dict) -> dict[int, dict]:
    """Return the newest unresolved filing target for each affected filer.

    A fund is marked withheld only when that target is at least as new as its
    newest published quarter. Older targets are separately exposed as exact
    unverified dates so current counts can remain usable while comparisons
    crossing those dates fail closed.
    """
    newest: dict[int, dict] = {}
    for cik, by_date in _active_unverified_targets_by_cik(state).items():
        report_date = max(by_date)
        newest[cik] = {
            "report_date": report_date,
            "reasons": set(by_date[report_date]),
        }
    return newest


def infer_proven_split_adjustments(
    holders: list[dict],
    *,
    min_observations: int = 20,
    min_support_fraction: float = 0.55,
) -> list[dict]:
    """Infer only filing-wide split factors with strong independent support.

    A split is published when a majority of continuing, non-imputed holders
    show the same near-integer share ratio and their median per-share value
    moves inversely. Ambiguous cases emit no metadata, which makes the
    frontend suppress (rather than invent) a position-change percentage.
    """
    histories: list[dict[str, dict]] = []
    all_dates: set[str] = set()
    for holder in holders:
        rows = holder.get("history") if isinstance(holder, dict) else None
        if not isinstance(rows, list):
            continue
        by_date = {
            row.get("date"): row
            for row in rows
            if (
                isinstance(row, dict)
                and report_quarter_code(row.get("date")) is not None
            )
        }
        if by_date:
            histories.append(by_date)
            all_dates.update(by_date)

    ordered_dates = sorted(all_dates)
    candidate_factors = (0.1, 0.2, 0.25, 1 / 3, 0.5, 2, 3, 4, 5, 10)
    adjustments: list[dict] = []
    for previous_date, current_date in zip(ordered_dates, ordered_dates[1:]):
        previous_code = report_quarter_code(previous_date)
        current_code = report_quarter_code(current_date)
        if previous_code is None or current_code is None:
            continue
        previous_ordinal = (previous_code // 10) * 4 + (previous_code % 10)
        current_ordinal = (current_code // 10) * 4 + (current_code % 10)
        if current_ordinal != previous_ordinal + 1:
            continue

        observations: list[tuple[float, float]] = []
        for history in histories:
            previous = history.get(previous_date)
            current = history.get(current_date)
            if not previous or not current:
                continue
            if previous.get("shares_imputed") or current.get("shares_imputed") or previous.get("quantity_unknown") or current.get("quantity_unknown"):
                continue
            raw_numbers = (
                previous.get("shares"),
                current.get("shares"),
                previous.get("value"),
                current.get("value"),
            )
            numbers: list[float] = []
            for value in raw_numbers:
                if isinstance(value, bool) or not isinstance(
                    value, (int, float)
                ):
                    break
                try:
                    number = float(value)
                except (OverflowError, TypeError, ValueError):
                    break
                if not math.isfinite(number) or number <= 0:
                    break
                numbers.append(number)
            if len(numbers) != len(raw_numbers):
                continue
            (
                previous_shares,
                current_shares,
                previous_value,
                current_value,
            ) = numbers
            try:
                share_ratio = current_shares / previous_shares
                previous_price = previous_value / previous_shares
                current_price = current_value / current_shares
                price_ratio = current_price / previous_price
            except (OverflowError, ZeroDivisionError):
                continue
            if (
                not math.isfinite(share_ratio)
                or not math.isfinite(price_ratio)
                or share_ratio <= 0
                or price_ratio <= 0
            ):
                continue
            observations.append(
                (
                    share_ratio,
                    price_ratio,
                )
            )
        if len(observations) < min_observations:
            continue

        supported: list[tuple[int, float, list[float]]] = []
        for factor in candidate_factors:
            price_ratios = [
                price_ratio
                for share_ratio, price_ratio in observations
                if abs(share_ratio - factor) / factor <= 0.10
            ]
            supported.append((len(price_ratios), factor, price_ratios))
        supported.sort(key=lambda row: (row[0], row[1]), reverse=True)
        support, factor, price_ratios = supported[0]
        if (
            support < min_observations
            or support / len(observations) < min_support_fraction
        ):
            continue

        expected_price_ratio = 1 / factor
        median_price_ratio = statistics.median(price_ratios)
        if (
            abs(median_price_ratio - expected_price_ratio)
            / expected_price_ratio
            > 0.40
        ):
            continue
        adjustments.append({
            "from_report_date": previous_date,
            "to_report_date": current_date,
            "factor": round(factor, 8),
            "proven": True,
            "support": support,
            "observations": len(observations),
        })
    return adjustments


@_serialize_pipeline_maintenance
def regenerate_stock_files_and_index(*, state: dict | None = None) -> None:
    """Rebuild stock files, the full search index, and the fund bootstrap.

    Display tickers come only from the provenance-bearing CUSIP registry. A
    missing registry row fails closed to an identifier-only display and the
    immutable as-filed issuer; it never reuses a legacy holding ticker. The
    per-holding `holding_type` still drives the stock_id suffix so one CUSIP can
    host both equity and option holdings on separate stock files.
    """
    log.info("Rebuilding stock files and search index...")
    _recover_interrupted_derived_publishes()

    if not FUNDS_DIR.exists():
        log.info("  no funds directory; nothing to rebuild")
        return

    registry = load_cusip_registry()
    if registry:
        log.info(f"  using CUSIP registry ({len(registry)} entries) as display source")
    else:
        log.warning("  no CUSIP registry found; all display tickers will fail closed")

    funds_summary: list[dict] = []
    stocks: dict[str, dict] = {}
    registry_fallback_count = 0
    if state is None:
        state = load_state()
    withheld_by_cik = _active_withheld_targets_by_cik(state)
    unverified_by_cik = _active_unverified_targets_by_cik(state)

    stock_fund_paths = sorted(FUNDS_DIR.glob("*.json"))
    stock_total = len(stock_fund_paths)
    fund_revision = hashlib.sha256()
    log.info(f"  building stock files from {stock_total} fund files...")
    for idx, fp in enumerate(stock_fund_paths):
        if idx % 2000 == 0 and idx > 0:
            log.info(f"    stock build progress: {idx}/{stock_total} funds ({len(stocks)} tickers so far)")
        try:
            raw_fund = fp.read_bytes()
            fund = json.loads(raw_fund)
        except (OSError, json.JSONDecodeError) as exc:
            raise FundDataError(
                f"cannot rebuild from malformed fund file: {fp}"
            ) from exc
        if not isinstance(fund, dict):
            raise FundDataError(f"fund file must contain an object: {fp}")
        fund_revision.update(fp.name.encode("utf-8"))
        fund_revision.update(b"\0")
        fund_revision.update(raw_fund)
        fund_revision.update(b"\0")
        cik = fund.get("cik")
        name = fund.get("name", "")
        if cik is None:
            raise FundDataError(f"fund file is missing CIK: {fp}")
        try:
            normalized_cik = int(cik)
        except (TypeError, ValueError) as exc:
            raise FundDataError(f"fund file has invalid CIK: {fp}") from exc
        fund_summary = {
            "cik": cik,
            "name": name,
            "q": fund_report_quarter_codes(fund.get("quarters")),
        }
        published_dates = sorted({
            quarter.get("report_date")
            for quarter in fund.get("quarters", [])
            if (
                isinstance(quarter, dict)
                and report_quarter_code(quarter.get("report_date")) is not None
            )
        }, reverse=True)
        unverified_report_dates = sorted(
            set(published_dates[:4])
            & set(unverified_by_cik.get(normalized_cik, {})),
            reverse=True,
        )
        if unverified_report_dates:
            fund_summary["unverified_report_dates"] = (
                unverified_report_dates
            )
        withheld = withheld_by_cik.get(normalized_cik)
        if (
            withheld
            and (
                not published_dates
                or withheld["report_date"] >= max(published_dates)
            )
        ):
            fund_summary.update({
                "status": "WITHHELD",
                "latest_withheld_report_date": withheld["report_date"],
                "withheld_reason": "; ".join(sorted(withheld["reasons"])),
            })
        funds_summary.append(fund_summary)

        for q in fund.get("quarters", []):
            rep_date = q.get("report_date")
            total_value = q.get("total_value", 0) or 0
            for h in q.get("holdings", []):
                cusip = str(h.get("cusip") or "").strip().upper()
                stock_key = cusip
                if not stock_key:
                    continue

                reg_entry = registry.get(cusip) if cusip else None
                holding_type = published_holding_instrument_type(
                    h,
                    reg_entry,
                )
                if reg_entry is not None:
                    # Registry is authoritative for display. A null ticker in
                    # the registry means "we know this CUSIP has no resolvable
                    # ticker" — show the CUSIP rather than backing off to
                    # whatever the filer happened to type.
                    registry_ticker = _registry_position_ticker(
                        reg_entry,
                        holding_type,
                    )
                    display_ticker = registry_ticker or cusip or stock_key
                    display_issuer = (
                        reg_entry.get("name")
                        or normalize_security_label(
                            h.get("reported_issuer"),
                            identifier=cusip,
                        )
                        or cusip
                    )
                else:
                    registry_fallback_count += 1
                    registry_ticker = None
                    display_ticker = cusip
                    display_issuer = (
                        normalize_security_label(
                            h.get("reported_issuer"),
                            identifier=cusip,
                        )
                        or cusip
                    )

                stock_id = stock_lookup_id(stock_key, holding_type)
                s = stocks.setdefault(stock_id, {
                    "stock_id": stock_id,
                    "cusip": cusip,
                    "ticker": display_ticker,
                    "issuer": display_issuer,
                    "search_ticker": registry_ticker,
                    "instrument_type": holding_type,
                    "holders": {},
                    "_meta_key": ("", -1),
                })
                # Registry-driven display is stable across all holdings of a
                # given stock_id by construction, so the meta-key update only
                # matters for the fallback path. Keeping the update symmetric
                # still keeps cusip pinned to the winning row's value for
                # parity with prior behavior.
                meta_key = (rep_date or "", h.get("value", 0) or 0)
                if meta_key >= s.get("_meta_key", ("", -1)):
                    s["_meta_key"] = meta_key
                    s["cusip"] = cusip
                    s["ticker"] = display_ticker
                    s["issuer"] = display_issuer
                    s["search_ticker"] = registry_ticker
                holder = s["holders"].setdefault(cik, {
                    "cik": cik,
                    "name": name,
                    "history": {},
                })
                holder["name"] = name

                # Pre-compute percent-of-portfolio so the website doesn't need
                # to lazily fetch ~6000 fund JSONs per stock view (was eating
                # ~100 MB per page). Rounded to 3 dp; 0 when total_value is
                # missing so downstream can safely treat it as numeric.
                new_value = h.get("value", 0) or 0
                pct = (new_value / total_value * 100.0) if total_value > 0 else 0.0
                new_entry = {
                    "date": rep_date,
                    "shares": h.get("shares", 0) or 0,
                    "value": new_value,
                    "pct_of_fund": pct,
                }
                if h.get("shares_imputed"):
                    new_entry["shares_imputed"] = True
                if h.get("quantity_unknown"):
                    new_entry["quantity_unknown"] = True

                existing = holder["history"].setdefault(rep_date, {
                    "date": rep_date,
                    "shares": 0,
                    "value": 0,
                    "pct_of_fund": 0.0,
                })
                existing["shares"] += new_entry["shares"]
                existing["value"] += new_entry["value"]
                existing["pct_of_fund"] += new_entry["pct_of_fund"]
                if new_entry.get("shares_imputed"):
                    existing["shares_imputed"] = True
                if new_entry.get("quantity_unknown"):
                    existing["quantity_unknown"] = True

    # Build a complete replacement beside the live outputs. The current
    # generation remains untouched unless every stock and both indexes render.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    staging = tempfile.TemporaryDirectory(
        prefix=".derived-stage-",
        dir=DATA_DIR.parent,
    )
    staging_root = Path(staging.name)
    staged_stocks_dir = staging_root / "stocks"
    try:
        staged_stocks_dir.mkdir()

        current_reporting_quarter = _modal_latest_reporting_quarter(
            funds_summary
        )
        current_fund_quarters = _current_fund_quarters(
            funds_summary,
            current_reporting_quarter,
        )

        tickers: list[dict] = []
        proven_split_adjustments: dict[str, list[dict]] = {}
        for _stock_id, s in stocks.items():
            holders_list = []
            for _cik, holder in s["holders"].items():
                history = list(holder["history"].values())
                for entry in history:
                    entry["pct_of_fund"] = round(entry.get("pct_of_fund", 0.0), 3)
                history.sort(key=lambda x: x.get("date") or "", reverse=True)
                holders_list.append({
                    "cik": holder["cik"],
                    "name": holder["name"],
                    "history": history,
                })
            holders_list.sort(
                key=lambda h: h["history"][0]["value"] if h["history"] else 0,
                reverse=True,
            )
            last_seen = max(
                (
                    holder["history"][0].get("date") or ""
                    for holder in holders_list
                    if holder["history"]
                ),
                default="",
            )
            current_holder_count = sum(
                1
                for holder in holders_list
                if (
                    (current_quarter := current_fund_quarters.get(
                        int(holder["cik"])
                    ))
                    is not None
                    and any(
                        report_quarter_code(record.get("date"))
                        == current_quarter
                        for record in holder["history"]
                    )
                )
            )
            out = {
                "stock_id": s["stock_id"],
                "cusip": s.get("cusip", ""),
                "ticker": s["ticker"],
                "issuer": s["issuer"],
                "instrument_type": s.get("instrument_type", "EQUITY"),
                "holders": holders_list,
            }
            if s.get("instrument_type", "EQUITY") == "EQUITY":
                split_adjustments = infer_proven_split_adjustments(holders_list)
                if split_adjustments:
                    out["split_adjustments"] = split_adjustments
                    proven_split_adjustments[s["stock_id"]] = split_adjustments
            filename_base = s["stock_id"].split("|", 1)[0]
            _atomic_write_json(
                staged_stocks_dir
                / stock_filename(filename_base, s.get("instrument_type")),
                out,
                fsync_parent=False,
            )
            # Search index should only contain canonical ticker-backed rows.
            # Uncovered/synthetic identifiers still get stock files for direct
            # fund-page drill-down, but should not appear as pseudo-tickers.
            if s.get("search_ticker"):
                tickers.append({
                    "stock_id": s["stock_id"],
                    "cusip": s.get("cusip", ""),
                    "ticker": s["search_ticker"],
                    "issuer": s["issuer"],
                    "instrument_type": s.get("instrument_type", "EQUITY"),
                    # Search uses this compact evidence to choose the canonical
                    # representative when historical or typo CUSIPs share a
                    # ticker. Current holder coverage uses the same modal reporting
                    # baseline as the frontend's stock-page aggregates.
                    "last_seen": last_seen,
                    "current_holder_count": current_holder_count,
                    "holder_count": len(holders_list),
                })

        funds_summary.sort(key=lambda x: (x.get("name") or "").upper())
        tickers.sort(key=lambda x: ((x.get("ticker") or "").upper(), x.get("stock_id") or ""))
        proven_split_adjustments = {
            stock_id: proven_split_adjustments[stock_id]
            for stock_id in sorted(proven_split_adjustments)
        }

        funds_index_semantic = {
            "data_contract_version": DATA_CONTRACT_VERSION,
            "fund_data_revision": fund_revision.hexdigest(),
            "funds": funds_summary,
            "proven_split_adjustments": proven_split_adjustments,
            "total_filers": len(funds_summary),
            "total_tickers": len(tickers),
        }
        index_semantic = {
            **funds_index_semantic,
            "tickers": tickers,
        }
        previous_index = _read_json_object(INDEX_PATH)
        previous_semantic = dict(previous_index or {})
        previous_last_updated = previous_semantic.pop("last_updated", None)
        if (
            previous_semantic == index_semantic
            and _is_strict_utc_timestamp(previous_last_updated)
        ):
            generated_at = previous_last_updated
        else:
            generated_at = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        funds_index = {
            **funds_index_semantic,
            "last_updated": generated_at,
        }
        index = {
            **index_semantic,
            "last_updated": generated_at,
        }
        # This is the render-blocking homepage bootstrap, so keep it compact.
        _atomic_write_json(
            staging_root / "funds-index.json",
            funds_index,
            indent=None,
            fsync_parent=False,
        )
        _atomic_write_json(
            staging_root / "index.json",
            index,
            fsync_parent=False,
        )
        _fsync_directory(staged_stocks_dir)
        _fsync_directory(staging_root)
        _fsync_directory(DATA_DIR.parent)
        _publish_staged_derived_outputs(staging_root)
    finally:
        staging.cleanup()

    log.info(
        f"  wrote {len(stocks)} stock files; index has "
        f"{len(funds_summary)} funds, {len(tickers)} tickers"
    )
    if registry_fallback_count:
        log.warning(
            f"  {registry_fallback_count} holdings had no registry proof; "
            "published identifier-only displays with no search ticker"
        )


# ----------------------------------------------------------------------------
# Ticker health report
# ----------------------------------------------------------------------------

# Match SEC evidence results that look like debt/preferred/warrant symbols rather
# than plain equity tickers (e.g. "MITT 8.25 PERP A", "BAC-PB", "GM 5.5 NOTE").
# Kept intentionally loose — false positives go in the "suspicious_symbol"
# bucket for human review, not auto-quarantine.
_SUSPICIOUS_TICKER_RE = re.compile(
    r"^\d|\s|\d+\.\d+|(?:PERP|PFD|NOTE|WARRANT)$",
    re.IGNORECASE,
)


def _classify_ticker_health(
    cusip: str,
    ticker: str | None,
    instrument_type: str | None = None,
) -> str | None:
    """Return a health bucket name if the ticker looks bad, else None.

    synthetic_identifier obvious zero-padded filler like 00000AAPL
    unresolved       ticker is missing or fell back to the raw CUSIP
    suspicious_symbol ticker looks like a bond/preferred/warrant symbol
    """
    t = str(ticker or "").strip().upper()
    if is_synthetic_identifier(cusip):
        return "synthetic_identifier"
    if not t or t == cusip.upper():
        return "unresolved"
    if normalize_instrument_type(instrument_type) == "NOTE":
        return (
            None
            if normalize_note_security_label(t)
            else "suspicious_symbol"
        )
    if _SUSPICIOUS_TICKER_RE.search(t):
        return "suspicious_symbol"
    return None


def _has_complete_sec_ftd_health_proof(
    registry_entry: dict,
    sec_title: str,
) -> bool:
    """Recognize a resolved FTD symbol with the full current SEC proof chain.

    ``security_kind_source == sec_13f_list`` is emitted only when the master
    has an active, non-deleted official-list row.  ``sec_company_tickers`` is
    added to public provenance only from the master record's current-symbol
    validation sources.  Requiring both keeps a ticker such as the real NYSE
    symbol ``PFD`` out of the text-only heuristic without forgiving a symbol
    merely because its spelling resembles the issuer name.

    Official-list issuer fields are fixed-width, so the diagnostic comparison
    admits a substantial normalized prefix in addition to equality.  This is
    only a health-report suppression after exact CUSIP-based resolution; it is
    never used to publish a ticker.
    """

    if not isinstance(registry_entry, dict) or not sec_title:
        return False
    sources = {
        str(source).strip()
        for source in (registry_entry.get("sources") or [])
        if str(source).strip()
    }
    if not (
        registry_entry.get("mapping_status") == "resolved"
        and registry_entry.get("ticker_source") == "sec_ftd"
        and registry_entry.get("security_kind_source") == "sec_13f_list"
        and {"sec_ftd", "sec_company_tickers", "sec_13f_list"} <= sources
    ):
        return False

    normalized_title = normalize_name(sec_title)
    normalized_issuer = normalize_name(
        registry_entry.get("name")
        or registry_entry.get("dominant_issuer")
        or ""
    )
    if not normalized_title or not normalized_issuer:
        return False
    if normalized_title == normalized_issuer:
        return True
    shorter, longer = sorted(
        (normalized_title, normalized_issuer),
        key=len,
    )
    return len(shorter) >= 12 and longer.startswith(shorter)


def write_ticker_health_report() -> dict:
    """Scan every fund file and emit data/ticker_health.json.

    Ticker health and display-label coverage are deliberately separate:
    non-traded notes and pools may have no canonical ticker while still having
    a useful human label. Ticker buckets are informational diagnostics only;
    SEC-master discovery and retry state are driven by exact source evidence.
    Label coverage is the release-facing guarantee that the UI never needs a
    raw CUSIP as its primary security name.
    """
    log.info("Writing ticker health report...")
    if not FUNDS_DIR.exists():
        log.info("  no funds directory; skipping health report")
        return {}

    registry = load_cusip_registry()
    records: dict[str, dict] = {}
    for fp in sorted(FUNDS_DIR.glob("*.json")):
        try:
            with open(fp) as f:
                fund = json.load(f)
        except json.JSONDecodeError:
            continue
        cik = fund.get("cik")
        for q in fund.get("quarters", []):
            rep_date = q.get("report_date") or ""
            for h in q.get("holdings", []):
                cusip = normalize_security_identifier(
                    h.get("reported_cusip") or h.get("cusip")
                )
                if not cusip:
                    continue
                instrument_type = classify_saved_holding(h)
                registry_entry = registry.get(cusip)
                ticker = (
                    _registry_position_ticker(registry_entry, instrument_type)
                    if registry_entry is not None
                    else None
                )
                value = int(h.get("value", 0) or 0)
                rec = records.setdefault(cusip, {
                    "cusip": cusip,
                    "ticker": ticker,
                    "issuer": _reported_descriptor_text(
                        h,
                        "reported_issuer",
                    ),
                    "holder_ciks": set(),
                    "ticker_variants": set(),
                    "type_value": Counter(),
                    "type_count": Counter(),
                    "max_value": 0,
                    "first_seen": rep_date,
                    "last_seen": rep_date,
                })
                rec["type_value"][instrument_type] += value
                rec["type_count"][instrument_type] += 1
                if cik is not None:
                    rec["holder_ciks"].add(cik)
                if ticker:
                    rec["ticker_variants"].add(ticker)
                if value > rec["max_value"]:
                    rec["max_value"] = value
                    rec["issuer"] = (
                        _reported_descriptor_text(h, "reported_issuer")
                        or rec["issuer"]
                    )
                    rec["ticker"] = ticker
                if rep_date:
                    if not rec["first_seen"] or rep_date < rec["first_seen"]:
                        rec["first_seen"] = rep_date
                    if rep_date > (rec["last_seen"] or ""):
                        rec["last_seen"] = rep_date

    resolved_equity_prefixes = {
        cusip[:6]
        for cusip, entry in registry.items()
        if (
            len(cusip) >= 6
            and normalize_instrument_type(entry.get("type")) == "EQUITY"
            and str(entry.get("ticker") or "").strip()
        )
    }
    company_ticker_data = _read_json_object(
        DATA_DIR / "company_tickers.json"
    ) or {}
    sec_titles = sec_ticker_titles(company_ticker_data)
    buckets_out: dict[str, list[dict]] = defaultdict(list)
    unlabeled_cusips: list[str] = []
    for rec in records.values():
        registry_entry = registry.get(rec["cusip"]) or {}
        if (
            normalize_security_label(
                registry_entry.get("security_label"),
                identifier=rec["cusip"],
            )
            is None
        ):
            unlabeled_cusips.append(rec["cusip"])
        registry_type = registry_entry.get("type")
        instrument_type = (
            normalize_instrument_type(registry_type)
            if registry_type
            else max(
                rec["type_count"],
                key=lambda candidate: (
                    rec["type_value"][candidate],
                    rec["type_count"][candidate],
                    -_HOLDING_TYPE_PRIORITY.get(candidate, 99),
                    candidate,
                ),
            )
        )
        ticker = (
            _registry_position_ticker(registry_entry, instrument_type)
            if registry_entry
            else None
        )
        bucket = _classify_ticker_health(
            rec["cusip"],
            ticker,
            instrument_type,
        )
        if (
            bucket == "unresolved"
            and instrument_type == "EQUITY"
            and len(rec["cusip"]) == 9
            and rec["cusip"][6] == "9"
            and rec["cusip"][:6] in resolved_equity_prefixes
        ):
            # 13F rows sometimes omit putCall and leave an option-family CUSIP
            # looking like a second common share. Keep it unresolved, but
            # separate this structurally actionable queue from ordinary debt,
            # retired securities, and safe ticker-collision demotions.
            bucket = "option_family_artifact"
        sec_title = sec_titles.get(ticker) if ticker else None
        normalized_sec_title = normalize_name(sec_title or "")
        normalized_registry_issuer = normalize_name(
            registry_entry.get("name")
            or registry_entry.get("dominant_issuer")
            or ""
        )
        if (
            bucket == "suspicious_symbol"
            and ticker
            and (
                (
                    normalized_sec_title
                    and normalized_sec_title == normalized_registry_issuer
                )
                or _has_complete_sec_ftd_health_proof(
                    registry_entry,
                    sec_title or "",
                )
            )
        ):
            # A word such as NOTE can be both a debt descriptor and a real
            # listed-company ticker. Exact SEC title/issuer proof keeps those
            # symbols out of a false-positive retry queue.
            bucket = None
        entry_base = {
            "cusip": rec["cusip"],
            "ticker": ticker,
            "issuer": registry_entry.get("name") or rec["issuer"],
            "instrument_type": instrument_type,
            "holder_count": len(rec["holder_ciks"]),
            "max_value": rec["max_value"],
            "first_seen": rec["first_seen"],
            "last_seen": rec["last_seen"],
        }
        ticker_hint = synthetic_identifier_ticker_hint(rec["cusip"])
        if ticker_hint:
            entry_base["ticker_hint"] = ticker_hint
        if bucket:
            buckets_out[bucket].append(dict(entry_base))
        # Cross-time ambiguity: same CUSIP persisted under >1 ticker. Emitted
        # even when the top-value ticker looks clean, because the other
        # variants are a signal that a re-resolution split the CUSIP over
        # time and older fund files didn't get rewritten.
        if len(rec["ticker_variants"]) > 1:
            buckets_out["ambiguous"].append({
                **entry_base,
                "ticker_variants": sorted(rec["ticker_variants"]),
            })

    for entries in buckets_out.values():
        entries.sort(key=lambda e: e["max_value"], reverse=True)

    summary = {bucket: len(entries) for bucket, entries in buckets_out.items()}
    label_coverage = {
        "total": len(records),
        "labeled": len(records) - len(unlabeled_cusips),
        "unlabeled": len(unlabeled_cusips),
        "unlabeled_samples": sorted(unlabeled_cusips)[:10],
    }
    report_semantic = {
        "summary": summary,
        "buckets": dict(buckets_out),
        "label_coverage": label_coverage,
    }
    previous = _read_json_object(TICKER_HEALTH_PATH)
    previous_semantic = dict(previous or {})
    previous_generated_at = previous_semantic.pop("generated_at", None)
    if (
        previous_semantic == report_semantic
        and _is_strict_utc_timestamp(previous_generated_at)
    ):
        generated_at = previous_generated_at
    else:
        generated_at = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    report = {
        "generated_at": generated_at,
        **report_semantic,
    }

    _atomic_write_json(TICKER_HEALTH_PATH, report)

    if summary:
        parts = ", ".join(f"{n} {b}" for b, n in sorted(summary.items()))
        log.info(f"  ticker_health.json: {parts}")
    else:
        log.info("  ticker_health.json: all CUSIPs resolved cleanly")
    log.info(
        "  security labels: %s/%s covered",
        label_coverage["labeled"],
        label_coverage["total"],
    )
    return report


# ----------------------------------------------------------------------------
# CUSIP registry
# ----------------------------------------------------------------------------


def _aggregate_cusip_evidence() -> dict[str, dict]:
    """Walk fund files once and return per-CUSIP aggregated evidence.

    For each CUSIP we collect:
      total_value     sum of holding values across all quarters/filers
      holder_ciks     set of CIKs that have ever held it
      issuer_value    {filer-typed issuer name -> cumulative value}
      class_value     {filer-typed class string -> cumulative value}
      put_call_value  {PUT|CALL -> cumulative value}
      instrument_type_value/count
                      per-row canonical instrument evidence
      non_option_*    issuer/class evidence from direct-security rows only
      first_seen      earliest report_date
      last_seen       latest report_date
    Value-weighting is used so a single tiny-dollar typo can't outvote
    thousands of correct filings.
    """
    evidence: dict[str, dict] = {}
    for fp in sorted(FUNDS_DIR.glob("*.json")):
        try:
            with open(fp) as f:
                fund = json.load(f)
        except json.JSONDecodeError:
            continue
        cik = fund.get("cik")
        for q in fund.get("quarters", []):
            rep_date = q.get("report_date") or ""
            for h in q.get("holdings", []):
                cusip = normalize_security_identifier(
                    h.get("reported_cusip") or h.get("cusip")
                )
                if not cusip:
                    continue
                rec = evidence.setdefault(cusip, {
                    "total_value": 0,
                    "holder_ciks": set(),
                    "issuer_value": defaultdict(int),
                    "class_value": defaultdict(int),
                    "put_call_value": defaultdict(int),
                    "instrument_type_value": defaultdict(int),
                    "instrument_type_count": defaultdict(int),
                    "non_option_issuer_value": defaultdict(int),
                    "non_option_issuer_count": defaultdict(int),
                    "non_option_class_value": defaultdict(int),
                    "non_option_class_count": defaultdict(int),
                    "first_seen": rep_date,
                    "last_seen": rep_date,
                })
                value = int(h.get("value", 0) or 0)
                row_instrument_type = holding_instrument_type(h)
                rec["total_value"] += value
                rec["instrument_type_value"][row_instrument_type] += value
                rec["instrument_type_count"][row_instrument_type] += 1
                if cik is not None:
                    rec["holder_ciks"].add(cik)
                issuer = _reported_descriptor_text(
                    h,
                    "reported_issuer",
                ).upper()
                if issuer:
                    rec["issuer_value"][issuer] += value
                cls = _reported_descriptor_text(
                    h,
                    "reported_class",
                ).upper()
                if cls:
                    rec["class_value"][cls] += value
                pc = str(h.get("put_call") or "").strip().upper()
                if pc in ("PUT", "CALL"):
                    rec["put_call_value"][pc] += value
                if row_instrument_type not in {"CALL", "PUT", "OPT"}:
                    if issuer:
                        rec["non_option_issuer_value"][issuer] += value
                        rec["non_option_issuer_count"][issuer] += 1
                    if cls:
                        rec["non_option_class_value"][cls] += value
                        rec["non_option_class_count"][cls] += 1
                if rep_date:
                    if not rec["first_seen"] or rep_date < rec["first_seen"]:
                        rec["first_seen"] = rep_date
                    if rep_date > (rec["last_seen"] or ""):
                        rec["last_seen"] = rep_date
    return evidence


def _filer_exclusive_etf_issuer(entry: dict | None) -> bool:
    """Return whether an exact filer issuer is an ETF-only series vehicle."""

    if not isinstance(entry, dict):
        return False
    for field in ("name", "dominant_issuer"):
        issuer = " ".join(str(entry.get(field) or "").upper().split())
        if issuer and _FILER_EXCLUSIVE_ETF_ISSUER_RE.fullmatch(issuer):
            return True
    return False


def _filer_guarded_series_etf(entry: dict | None) -> bool:
    """Identify mixed-series ETF shares without swallowing mutual funds."""

    if not isinstance(entry, dict):
        return False
    ticker = str(entry.get("ticker") or "").strip().upper()
    sources = set(entry.get("sources") or [])
    dominant_class = " ".join(
        str(entry.get("dominant_class") or "").upper().split()
    )
    if (
        not _SEC_PLAIN_TICKER_RE.fullmatch(ticker)
        or is_mutual_fund_ticker(ticker)
        or "ticker_collision_demoted" in sources
        or not (sources & _FUND_IDENTITY_TICKER_SOURCES)
    ):
        return False
    for field in ("name", "dominant_issuer"):
        issuer = " ".join(str(entry.get(field) or "").upper().split())
        if _FILER_RBB_FD_INC_RE.fullmatch(issuer):
            return True
        if (
            _FILER_SCHWAB_STRATEGIC_TR_RE.fullmatch(issuer)
            and "SELF-DIRECTED ACCOUNT" not in dominant_class
        ):
            return True
    return False


def _filer_security_kind(entry: dict | None) -> str | None:
    """Infer only explicit display kinds from retained filing metadata."""

    if not isinstance(entry, dict):
        return None
    issuer_text = " ".join(
        str(entry.get(field) or "").strip()
        for field in ("name", "dominant_issuer")
    )
    dominant_class = str(entry.get("dominant_class") or "").strip()
    combined = " ".join((issuer_text, dominant_class))
    normalized_class = " ".join(dominant_class.upper().split())

    # Funds precede class-only UNIT/RIGHT checks: ETF filings commonly use
    # classes such as TR UNIT, and ETFs are sometimes described as mutual funds.
    if _FILER_CLOSED_END_KIND_RE.search(combined):
        return "CLOSED-END FUND"
    if _FILER_ETN_KIND_RE.search(combined):
        return "ETN"
    if _filer_exclusive_etf_issuer(entry):
        return "ETF"
    if _filer_guarded_series_etf(entry):
        return "ETF"
    if (
        _FILER_ETF_KIND_RE.search(combined)
        or _FILER_SPONSOR_TRUST_ETF_RE.search(combined)
        or (
            _FILER_ABBREVIATED_SPONSOR_TR_RE.search(issuer_text)
            and any(hint in normalized_class for hint in _IS_ETF_HINT)
        )
    ):
        return "ETF"
    if _FILER_MUTUAL_FUND_KIND_RE.search(combined):
        return "MUTUAL FUND"
    if (
        normalized_class in {"MF", "MMF"}
        or "NON-SWEEP MMF" in normalized_class
    ):
        return "MUTUAL FUND"
    if _FILER_RIGHT_KIND_RE.search(dominant_class):
        return "RIGHT"
    if _FILER_UNIT_KIND_RE.search(dominant_class):
        return "UNIT"
    if _FILER_PREFERRED_KIND_RE.search(dominant_class):
        return "PREFERRED"
    if _FILER_WARRANT_KIND_RE.search(dominant_class):
        return "WARRANT"
    instrument_type = normalize_instrument_type(entry.get("type"))
    if instrument_type == "PREF":
        return "PREFERRED"
    if instrument_type == "WARRANT":
        return "WARRANT"
    # EQUITY is the broad SEC 13F bucket, not a reader-facing claim that the
    # instrument is common stock.  Use COMMON only for a canonical public
    # company ticker backed by SEC title metadata and an explicit common-share
    # titleOfClass.  The exclusions keep fund, depositary receipt, debt, and
    # other special-security descriptions from being promoted merely because
    # a noisy filing also called them COM.
    entry_sources = {
        str(source).strip()
        for source in (entry.get("sources") or [])
        if str(source).strip()
    }
    if instrument_type == "EQUITY" and (
        _FILER_COMMON_KIND_RE.search(normalized_class)
        or _FILER_COMMON_CLASS_ONLY_RE.fullmatch(normalized_class)
    ) and (
        str(entry.get("ticker") or "").strip()
        and "sec_company_tickers" in entry_sources
        and not _FILER_COMMON_CLASS_EXCLUSION_RE.search(dominant_class)
        and not _FILER_COMMON_ISSUER_EXCLUSION_RE.search(issuer_text)
    ):
        return "COMMON"
    return None


def write_security_labels(registry: dict[str, dict]) -> None:
    """Write compact browser metadata without changing public identities."""

    labels: dict[str, str] = {}
    kinds: dict[str, str] = {}
    product_names: dict[str, str] = {}
    fund_identities: list[str] = []
    for identifier, entry in sorted(registry.items()):
        label = normalize_security_label(
            entry.get("security_label"),
            identifier=identifier,
        )
        if not label:
            raise FundDataError(
                f"registry entry {identifier} has no safe security label"
            )
        labels[identifier] = label
        raw_kind = entry.get("security_kind")
        kind = normalize_security_kind(raw_kind)
        if raw_kind and not kind:
            raise FundDataError(
                f"registry entry {identifier} has invalid security kind"
            )
        if kind:
            kinds[identifier] = kind
        if _registry_entry_has_equity_fund_identity(entry):
            fund_identities.append(identifier)
        raw_product_name = entry.get("product_name")
        if raw_product_name is not None:
            product_name = normalize_security_label(
                raw_product_name,
                identifier=identifier,
            )
            if (
                not product_name
                or product_name != raw_product_name
                or kind not in _FUND_PRODUCT_NAME_KINDS
            ):
                raise FundDataError(
                    f"registry entry {identifier} has invalid fund product name"
                )
            product_names[identifier] = product_name
    _atomic_write_json(
        SECURITY_LABELS_PATH,
        {
            "data_contract_version": DATA_CONTRACT_VERSION,
            "fund_identities": fund_identities,
            "kinds": kinds,
            "labels": labels,
            "product_names": product_names,
        },
        indent=None,
        sort_keys=True,
    )

@_serialize_pipeline_maintenance
def canonicalize_fund_files(
    *,
    preserve_position_identity: bool = False,
) -> int:
    """Normalize row type and refresh display metadata in every fund file.

    Phase 3: filer-typed strings are no longer trusted for display. After
    this pass, every holding's ticker and issuer are registry-derived while
    holding type remains row-derived:

      ticker         registry.ticker (canonical, possibly null for
                     uncovered CUSIPs — don't fall back to filer string)
      issuer         registry.name (SEC title or filer-dominant) when
    registry has a name; otherwise uses only immutable as-filed issuer
                     evidence or the identifier itself
      holding_type   hash-v3 parser identity when present; otherwise
                     classify_saved_holding(h) from put_call + class, NOT
                     registry.type (a CUSIP can host both equity and option
                     holdings on different rows)

    Fields left alone: cusip, class (raw filer text for audit), put_call,
    value, shares, shares_imputed, reported_issuer, reported_class,
    reported_cusip, reported_figi, accession, report_date.

    Stale legacy field `option_type` is removed when present. Registry data is
    never allowed to rewrite public position identity. Must run AFTER
    build_cusip_registry."""
    log.info("Canonicalizing holding types and registry display metadata...")
    if not FUNDS_DIR.exists():
        log.info("  no funds directory; skipping canonicalization")
        return 0

    registry = load_cusip_registry()
    if not registry:
        log.warning("  no registry; clearing every unproven display ticker")

    fund_paths = sorted(FUNDS_DIR.glob("*.json"))
    total = len(fund_paths)
    updated = 0
    ticker_changes = 0
    issuer_changes = 0
    type_changes = 0
    missing_registry = 0
    ambiguous_legacy_options = 0
    for idx, fp in enumerate(fund_paths):
        if idx % 2000 == 0 and idx > 0:
            log.info(
                f"    canonicalize progress: {idx}/{total} "
                f"({updated} changed, {ticker_changes} tickers, "
                f"{issuer_changes} issuers, {type_changes} types rewritten)"
            )
        try:
            with open(fp) as f:
                fund = json.load(f)
        except json.JSONDecodeError:
            continue
        changed = False
        for q in fund.get("quarters", []):
            allow_missing_option_side_reclassification = (
                _quarter_retains_raw_put_call(q)
            )
            for h in q.get("holdings", []):
                cusip = str(h.get("cusip") or "").strip().upper()
                if not cusip:
                    continue

                # Establish public position identity from the SEC row before
                # registry display metadata can replace issuer text.
                if (
                    not allow_missing_option_side_reclassification
                    and not h.get("put_call")
                    and normalize_instrument_type(
                        h.get("holding_type") or h.get("option_type")
                    )
                    in {"CALL", "PUT", "OPT"}
                    and str(h.get("class") or "").strip().upper()
                    not in {"CALL", "PUT"}
                ):
                    ambiguous_legacy_options += 1
                # An explicit security-master refresh is a display-metadata
                # operation.  Preserve the exact identity visible to the
                # pre-cutover projection; separate SEC filing replay owns
                # evidence-backed type corrections.  Canonicalizing a legacy
                # ``option_type`` spelling to ``holding_type`` is harmless
                # because holding_instrument_type already projects it this
                # way (and explicit put_call still has precedence).
                new_type = (
                    holding_instrument_type(h)
                    if preserve_position_identity
                    else _canonical_holding_type_for_quarter(q, h)
                )
                if h.get("holding_type") != new_type:
                    h["holding_type"] = new_type
                    type_changes += 1
                    changed = True
                if "option_type" in h:
                    del h["option_type"]
                    changed = True

                reg_entry = registry.get(cusip)
                if reg_entry is None:
                    missing_registry += 1
                    if h.get("ticker") is not None:
                        h["ticker"] = None
                        ticker_changes += 1
                        changed = True
                    new_issuer = (
                        normalize_security_label(
                            h.get("reported_issuer"),
                            identifier=cusip,
                        )
                        or cusip
                    )
                    if h.get("issuer") != new_issuer:
                        h["issuer"] = new_issuer
                        issuer_changes += 1
                        changed = True
                    continue

                new_ticker = _registry_position_ticker(reg_entry, new_type)
                if h.get("ticker") != new_ticker:
                    h["ticker"] = new_ticker
                    ticker_changes += 1
                    changed = True

                new_issuer = (
                    normalize_security_label(
                        reg_entry.get("name"),
                        identifier=cusip,
                    )
                    or normalize_security_label(
                        h.get("issuer"),
                        identifier=cusip,
                    )
                    or ""
                )
                if h.get("issuer") != new_issuer:
                    h["issuer"] = new_issuer
                    issuer_changes += 1
                    changed = True
        if changed:
            _atomic_write_json(fp, fund)
            updated += 1

    log.info(
        f"  canonicalized {updated}/{total} fund files: "
        f"{ticker_changes} tickers, {issuer_changes} issuers, "
        f"{type_changes} types rewritten"
    )
    if missing_registry:
        log.warning(
            f"  {missing_registry} holdings had CUSIPs missing from registry "
            "(display ticker cleared; as-filed label retained)"
        )
    if ambiguous_legacy_options:
        log.warning(
            f"  retained {ambiguous_legacy_options} legacy option labels "
            "without raw put_call or decisive parser-era evidence"
        )
    return updated


@_serialize_pipeline_maintenance
def upgrade_composition_hashes_in_place() -> int:
    """Bind exact parser-backed filing identity into retained hashes.

    Hash v2 added holding type. Hash v3 also binds the immutable as-filed
    issuer, class, CUSIP, optional FIGI, accession, and report date. Retained
    v1/v2 quarters can be upgraded only when their old hash still verifies,
    every applied source carries the parser identity marker, and every holding
    already has the complete exact-row provenance. Anything else is left
    untouched so validation fails closed until the SEC identity backfill or a
    normal replay repairs it.
    """
    if not FUNDS_DIR.exists():
        return 0

    upgraded = 0
    for fund_path in sorted(FUNDS_DIR.glob("*.json")):
        try:
            with open(fund_path) as f:
                fund = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(fund, dict):
            continue

        changed = False
        quarters = fund.get("quarters")
        if not isinstance(quarters, list):
            continue
        for quarter in quarters:
            if (
                not isinstance(quarter, dict)
                or quarter.get("composition_version")
                != AMENDMENT_REDUCER_VERSION
                or quarter.get("security_identity_version")
                != SECURITY_IDENTITY_VERSION
                or quarter.get("composition_hash_version", 1) not in {1, 2}
            ):
                continue

            holdings = quarter.get("holdings")
            applied_accessions = quarter.get("applied_accessions")
            source_filings = quarter.get("source_filings")
            # Empty parser-proven filings have no information-table rows to
            # reference. Upgrade their verified legacy hash with an explicit
            # empty evidence list; nonempty quarters still require exact refs.
            empty_identity_sources = (
                holdings == []
                and quarter.get("num_holdings") == 0
                and quarter.get("total_value") == 0
                and "reported_identity_sources" not in quarter
            )
            if (
                not isinstance(holdings, list)
                or not all(isinstance(holding, dict) for holding in holdings)
                or not isinstance(applied_accessions, list)
                or not applied_accessions
                or not all(
                    isinstance(accession, str) and accession.strip()
                    for accession in applied_accessions
                )
                or len(applied_accessions) != len(set(applied_accessions))
                or not isinstance(source_filings, list)
                or not all(
                    isinstance(source, dict) for source in source_filings
                )
                or not isinstance(quarter.get("report_date"), str)
                or not isinstance(quarter.get("base_accession"), str)
                or not (
                    empty_identity_sources
                    or _quarter_has_complete_reported_identity_sources(quarter)
                )
                or any(
                    not _holding_has_hashable_reported_identity(holding)
                    or holding.get("report_date") != quarter.get("report_date")
                    for holding in holdings
                )
            ):
                continue

            applied_sources = [
                source
                for source in source_filings
                if source.get("applied") is True
            ]
            if (
                [source.get("accession") for source in applied_sources]
                != applied_accessions
                or any(
                    source.get("security_identity_version")
                    != SECURITY_IDENTITY_VERSION
                    for source in applied_sources
                )
            ):
                continue
            source_by_accession = {
                source.get("accession"): source for source in source_filings
                if isinstance(source.get("accession"), str)
            }
            if (
                len(source_by_accession) != len(source_filings)
                or any(
                    accession not in source_by_accession
                    or not isinstance(
                        source_by_accession[accession].get("source_hash"),
                        str,
                    )
                    or not re.fullmatch(
                        r"[0-9a-f]{64}",
                        source_by_accession[accession]["source_hash"],
                    )
                    for accession in applied_accessions
                )
            ):
                continue

            hash_args = {
                "report_date": quarter["report_date"],
                "base_accession": quarter["base_accession"],
                "applied_accessions": applied_accessions,
                "applied_source_hashes": [
                    source_by_accession[accession]["source_hash"]
                    for accession in applied_accessions
                ],
                "holdings": holdings,
                "composition_version": AMENDMENT_REDUCER_VERSION,
                "source_filings": source_filings,
                "security_identity_version": SECURITY_IDENTITY_VERSION,
            }
            prior_hash_version = quarter.get("composition_hash_version", 1)
            legacy_hash = calculate_composition_hash(
                **hash_args,
                composition_hash_version=prior_hash_version,
            )
            if quarter.get("composition_hash") != legacy_hash:
                continue

            if empty_identity_sources:
                quarter["reported_identity_sources"] = []
            quarter["composition_hash_version"] = COMPOSITION_HASH_VERSION
            quarter["composition_hash"] = calculate_composition_hash(
                **hash_args,
                composition_hash_version=COMPOSITION_HASH_VERSION,
            )
            changed = True
            upgraded += 1

        if changed:
            _atomic_write_json(fund_path, fund)

    log.info(
        "  upgraded %s retained parser-proof composition hash(es) to v%s",
        upgraded,
        COMPOSITION_HASH_VERSION,
    )
    return upgraded


@_serialize_pipeline_maintenance
def rebuild_registry_backed_outputs(
    *,
    full_refresh: bool = False,
    company_ticker_data: dict | list | None = None,
    refresh_official_fund_names: bool = True,
    preserve_position_economics: bool = False,
    apply_quantity_policy: bool = False,
) -> None:
    """Refresh all registry-driven derived artifacts from current fund files."""
    registry = build_cusip_registry(
        full_refresh=full_refresh,
        company_ticker_data=company_ticker_data,
        refresh_official_fund_names=refresh_official_fund_names,
    )
    if isinstance(registry, dict):
        write_security_labels(registry)
    if isinstance(registry, CusipRegistry):
        registry_issues = validate_cusip_registry(
            current_cusips=registry.observed_cusips,
        )
    else:
        # Compatibility for injected/mocked builders that predate the
        # observation-carrying registry result.
        registry_issues = validate_cusip_registry()
    if registry_issues:
        raise FundDataError(
            "SEC registry publication gate failed: "
            + "; ".join(registry_issues)
        )
    if preserve_position_economics:
        canonicalize_fund_files(preserve_position_identity=True)
    else:
        canonicalize_fund_files()
    # Quantity estimates are routine maintenance with separate frozen price
    # evidence. Keep a security-master-only transaction scoped to identities;
    # the normal ingestion path applies the quantity policy afterwards.
    if not preserve_position_economics or apply_quantity_policy:
        repair_zero_share_holdings_in_place()
    upgrade_composition_hashes_in_place()
    regenerate_stock_files_and_index()
    write_ticker_health_report()


def refresh_security_master_incremental() -> int:
    """Refresh new SEC source files and return the changed-record count."""

    before = load_security_master(SEC_SECURITY_MASTER_PATH)
    before_records = before.get("records", {})
    result = refresh_sec_security_master_from_funds(full_rebuild=False)
    if result is None:
        return 0
    after_records = result.master.get("records", {})
    keys = set(before_records) | set(after_records)
    changed = sum(
        before_records.get(key) != after_records.get(key)
        for key in keys
    )
    log.info("  incremental SEC security refresh changed %s record(s)", changed)
    return changed

# ----------------------------------------------------------------------------
# Main pipeline orchestration
# ----------------------------------------------------------------------------

def _merge_replay_triggers(
    discovered: list[dict],
    triggers: list[dict],
    prefetched: dict[str, dict],
) -> tuple[list[dict], set[str], dict[str, str]]:
    """Merge feed/index triggers into authoritative submissions metadata."""
    rows = {row["accession"]: dict(row) for row in discovered}
    target_dates: set[str] = set()
    trigger_dates: dict[str, str] = {}
    for trigger in triggers:
        accession = trigger["accession"]
        existing = rows.get(accession)
        if existing is not None:
            trigger_form = trigger.get("form_type")
            if trigger_form and existing.get("form_type") != trigger_form:
                raise FilingDiscoveryError(
                    f"form-type conflict for trigger accession {accession}"
                )
            for key in ("name", "date_filed", "accepted_at", "filename"):
                if not existing.get(key) and trigger.get(key):
                    existing[key] = trigger[key]
            target_dates.add(existing["report_date"])
            trigger_dates[accession] = existing["report_date"]
            continue

        parsed = prefetched.get(accession)
        if parsed is None:
            parsed = fetch_filing_holdings(
                trigger["cik"], accession, filing=trigger
            )
            prefetched[accession] = parsed
        report_date = parsed["report_date"]
        target_dates.add(report_date)
        trigger_dates[accession] = report_date
        rows[accession] = {
            **trigger,
            "report_date": report_date,
            "accepted_at": _normalize_accepted_at(trigger.get("accepted_at")),
        }
    return list(rows.values()), target_dates, trigger_dates

def _compose_replay_targets(
    cik: int,
    rows: list[dict],
    target_dates: set[str],
    prefetched: dict[str, dict],
) -> list[dict]:
    composed: list[dict] = []
    for report_date in sorted(target_dates, reverse=True):
        chain_rows = [row for row in rows if row.get("report_date") == report_date]
        if not chain_rows:
            raise FilingDiscoveryError(
                f"no submissions rows found for CIK {cik} report date {report_date}"
            )
        components: list[dict] = []
        parse_failures: list[tuple[dict, FilingParseError]] = []
        for row in chain_rows:
            accession = row["accession"]
            component = prefetched.get(accession)
            if component is None:
                try:
                    component = fetch_filing_holdings(cik, accession, filing=row)
                except FilingIdentityError:
                    # Identity failures are never made safe by a later
                    # restatement. Suppressing one could hide a cross-CIK
                    # component in the discovered filing chain.
                    raise
                except FilingParseError as exc:
                    parse_failures.append((row, exc))
                    continue
                else:
                    prefetched[accession] = component
            components.append(component)
        if not components and parse_failures:
            raise parse_failures[0][1]
        quarter = compose_quarter_filings(components)
        structural_issues = structural_quarter_health_issues(quarter)
        if structural_issues:
            issue_text = "; ".join(
                f"{issue.code}: {issue.detail}"
                for issue in structural_issues
            )
            raise FilingParseError(
                f"quarter health failed for {cik}/{report_date}: "
                f"{issue_text}"
            )
        base_source = next(
            source for source in quarter["source_filings"]
            if source["accession"] == quarter["base_accession"]
        )
        base_accepted_at = base_source.get("accepted_at")
        for failed_row, error in parse_failures:
            failed_accepted_at = _normalize_accepted_at(failed_row.get("accepted_at"))
            if (
                not base_accepted_at
                or not failed_accepted_at
                or failed_accepted_at >= base_accepted_at
            ):
                raise error
            log.warning(
                "     ignored superseded unparseable accession %s before complete "
                "base %s: %s",
                failed_row["accession"],
                quarter["base_accession"],
                error,
            )
        composed.append(quarter)
    return composed

@_serialize_pipeline_maintenance
def replay_quarters_for_cik(
    cik: int,
    triggers: list[dict],
    cusip_map: dict[str, str],
    max_quarters: int,
    state: dict,
    state_lock: threading.Lock | None = None,
    *,
    force: bool = False,
    include_archives: bool = False,
    preserve_history: bool = True,
    quarantine_failures: bool = False,
    quarantine_reason_override: str | None = None,
    replace_only: bool = False,
    track_migration_targets: bool = False,
    discovered_submission: tuple[list[dict], str] | None = None,
) -> int:
    """Replay every affected quarter from its complete immutable SEC chain.

    Discovery, parsing, and composition complete before any public fund/state
    mutation. If any target is ambiguous, the last-known-good fund and state
    remain unchanged.
    """
    lock = state_lock or threading.Lock()
    with lock:
        processed_set = state.setdefault("_processed_set", set())
        pending = [
            dict(trigger) for trigger in triggers
            if force
            or (
                trigger["accession"] not in processed_set
                and accession_retry_due(state, trigger["accession"])
            )
        ]
    if not pending:
        return 0
    if any(int(trigger.get("cik", cik)) != cik for trigger in pending):
        raise FilingDiscoveryError("replay trigger mixes multiple CIKs")

    log.info(
        "  -> replaying CIK %s from %s trigger(s): %s",
        cik,
        len(pending),
        ", ".join(trigger["accession"] for trigger in pending),
    )
    prefetched: dict[str, dict] = {}
    if discovered_submission is None:
        discovered, discovered_name = _discover_submission_filings(
            cik, include_archives=include_archives
        )
    else:
        discovered, discovered_name = discovered_submission
        discovered = [dict(row) for row in discovered]
    rows, target_dates, trigger_dates = _merge_replay_triggers(
        discovered, pending, prefetched
    )

    retained_dates: set[str] | None = None
    if replace_only:
        existing_fund = load_fund(cik)
        retained_dates = {
            quarter.get("report_date")
            for quarter in (existing_fund or {}).get("quarters", [])
            if isinstance(quarter, dict) and quarter.get("report_date")
        }
        with lock:
            for pending_field in (
                "amendment_migration_pending",
                "security_identity_migration_pending",
                "quarter_health_pending",
            ):
                retained_dates.update(
                    target.get("report_date")
                    for target in state.get(pending_field, {}).values()
                    if (
                        isinstance(target, dict)
                        and target.get("cik") == cik
                        and target.get("report_date")
                    )
                )
        target_dates.intersection_update(retained_dates)
        with lock:
            migration_pending = state.setdefault(
                "amendment_migration_pending", {}
            )
            expired_accessions = [
                accession
                for accession, target in migration_pending.items()
                if isinstance(target, dict)
                and target.get("cik") == cik
                and target.get("report_date") not in retained_dates
            ]
            quarantined = state.setdefault("_quarantined", {})
            for accession in expired_accessions:
                migration_pending.pop(accession, None)
                quarantined.pop(accession, None)

    if track_migration_targets:
        with lock:
            migration_pending = state.setdefault(
                "amendment_migration_pending", {}
            )
            for trigger in pending:
                accession = trigger["accession"]
                report_date = trigger_dates.get(accession)
                if (
                    trigger.get("form_type") == "13F-HR/A"
                    and report_date in target_dates
                ):
                    migration_pending[accession] = {
                        "cik": cik,
                        "report_date": report_date,
                    }

    if not target_dates:
        return 0

    composed: list[dict] = []
    successful_dates: set[str] = set()
    archived_rows: list[dict] | None = rows if include_archives else None
    for target_date in sorted(target_dates, reverse=True):
        try:
            target_composed = _compose_replay_targets(
                cik, rows, {target_date}, prefetched
            )
        except FilingChainError as exc:
            # Late supplements can outlive their base in submissions.recent.
            if include_archives or exc.reason not in {
                "missing_base", "amendment_number_conflict"
            }:
                target_error: Exception = exc
            else:
                try:
                    archived, archive_name = _discover_submission_filings(
                        cik, include_archives=True
                    )
                    archived_rows, _, archived_trigger_dates = _merge_replay_triggers(
                        archived, pending, prefetched
                    )
                    rows = archived_rows
                    trigger_dates.update(archived_trigger_dates)
                    discovered_name = archive_name or discovered_name
                    target_composed = _compose_replay_targets(
                        cik, archived_rows, {target_date}, prefetched
                    )
                except (FilingChainError, FilingParseError) as retry_error:
                    target_error = retry_error
                else:
                    composed.extend(target_composed)
                    successful_dates.add(target_date)
                    continue
        except FilingParseError as exc:
            target_error = exc
        else:
            composed.extend(target_composed)
            successful_dates.add(target_date)
            continue

        if not quarantine_failures:
            raise target_error
        target_triggers = [
            {**trigger, "report_date": target_date} for trigger in pending
            if trigger_dates.get(trigger["accession"]) == target_date
        ]
        quarantine_replay_failure(
            state,
            cik,
            target_triggers,
            target_error,
            state_lock=lock,
            reason_override=quarantine_reason_override,
        )
        log.warning(
            "     quarantined CIK %s report date %s; retaining last-known-good: %s",
            cik,
            target_date,
            target_error,
        )

    if not composed:
        return 0

    with lock:
        map_snapshot = dict(cusip_map)
    candidate_map = dict(map_snapshot)
    for quarter in composed:
        update_cusip_map(candidate_map, quarter["holdings"])
    map_updates = {
        key: value for key, value in candidate_map.items()
        if map_snapshot.get(key) != value
    }

    name = discovered_name or next(
        (str(trigger.get("name") or "") for trigger in pending if trigger.get("name")),
        "",
    )
    candidate_fund = merge_composed_quarters_into_fund(
        cik,
        name,
        composed,
        max_quarters,
        preserve_history=preserve_history,
    )
    save_fund(cik, candidate_fund)

    published_accessions = {
        source["accession"]
        for quarter in composed
        for source in quarter.get("source_filings", [])
    }
    successful_triggers = [
        trigger for trigger in pending
        if trigger_dates.get(trigger["accession"]) in successful_dates
    ]
    published_accessions.update(
        trigger["accession"] for trigger in successful_triggers
    )
    with lock:
        cusip_map.update(map_updates)
        quarter_health_sources = {
            accession
            for target in state.get("quarter_health_pending", {}).values()
            if isinstance(target, dict)
            for accession in (target.get("source_accessions", []) or [])
            if isinstance(accession, str) and accession
        }
        state.setdefault("_processed_set", set()).update(
            published_accessions - quarter_health_sources
        )
        quarantined = state.setdefault("_quarantined", {})
        migration_pending = state.setdefault("amendment_migration_pending", {})
        for accession in published_accessions:
            if accession not in quarter_health_sources:
                quarantined.pop(accession, None)
            migration_pending.pop(accession, None)
        identity_pending = state.setdefault(
            "security_identity_migration_pending", {}
        )
        for report_date in successful_dates:
            target = {"cik": cik, "report_date": report_date}
            if _security_identity_target_is_resolved(
                target, candidate_fund
            ):
                identity_pending.pop(
                    security_identity_migration_key(cik, report_date),
                    None,
                )
    return len(successful_triggers)


@_serialize_pipeline_maintenance
def run_for_cik(
    cik: int,
    quarters_n: int,
    *,
    rebuild_outputs: bool = True,
) -> bool:
    log.info(f"=== Single-CIK mode: CIK {cik}, {quarters_n} quarters ===")
    state = load_state()
    cusip_map = load_cusip_map()
    try:
        filings, _name = get_13f_filings_for_cik(cik, quarters_n)
        processed = replay_quarters_for_cik(
            cik,
            filings,
            cusip_map,
            quarters_n,
            state,
            preserve_history=True,
            force=True,
        )
    except (Exception, KeyboardInterrupt) as exc:
        log.error("single-CIK replay failed for CIK %s: %s", cik, exc)
        # Replay can mutate the durable retry/quarantine state and ticker map
        # before a later filing in the same CIK fails. Preserve that progress
        # so the next run resumes instead of repeating completed lookups.
        try:
            save_state(state)
        except Exception as checkpoint_exc:
            log.error(
                "failed to checkpoint state after CIK %s failure: %s",
                cik,
                checkpoint_exc,
            )
        try:
            save_cusip_map(cusip_map)
        except Exception as checkpoint_exc:
            log.error(
                "failed to checkpoint CUSIP map after CIK %s failure: %s",
                cik,
                checkpoint_exc,
            )
        return False

    if rebuild_outputs:
        enforce_published_quarter_health(state)
    save_state(state)
    save_cusip_map(cusip_map)
    log.info("processed %s filing trigger(s) for CIK %s", processed, cik)
    if rebuild_outputs:
        rebuild_registry_backed_outputs()
    return True


@_serialize_pipeline_maintenance
def retry_pending_amendment_migrations(
    state: dict,
    cusip_map: dict[str, str],
    max_quarters: int,
) -> int:
    """Retry only retained migration targets that previously quarantined.

    The reducer version records that the bounded inventory completed.  Failed
    targets remain in a durable queue and are retried independently so one
    malformed filing cannot block unrelated current-quarter ingestion.
    """
    pending = state.setdefault("amendment_migration_pending", {})
    by_cik: dict[int, list[dict]] = defaultdict(list)
    for accession, target in list(pending.items()):
        if not isinstance(target, dict):
            log.error("malformed amendment migration target %s", accession)
            continue
        cik = target.get("cik")
        report_date = normalize_report_date(target.get("report_date"))
        if not isinstance(cik, int) or not report_date:
            log.error("malformed amendment migration target %s", accession)
            continue
        by_cik[cik].append({
            "cik": cik,
            "name": "",
            "form_type": "13F-HR/A",
            "accession": accession,
            "date_filed": None,
            "accepted_at": None,
            "report_date": report_date,
            "filename": "",
        })

    if not by_cik:
        return 0

    log.info(
        "retrying %s pending amendment migration target(s) across %s CIK(s)",
        sum(len(rows) for rows in by_cik.values()),
        len(by_cik),
    )
    retried = 0
    state_lock = threading.Lock()
    for cik in sorted(by_cik):
        triggers = by_cik[cik]
        try:
            retried += replay_quarters_for_cik(
                cik,
                triggers,
                cusip_map,
                max_quarters,
                state,
                state_lock=state_lock,
                force=True,
                include_archives=True,
                preserve_history=True,
                quarantine_failures=True,
                replace_only=True,
            )
        except Exception as exc:
            quarantine_replay_failure(
                state, cik, triggers, exc, state_lock=state_lock
            )
            log.warning(
                "pending amendment migration retry failed for CIK %s; "
                "retaining last-known-good: %s",
                cik,
                exc,
            )
    return retried


def amendment_migration_retry_due(
    state: dict,
    *,
    now: datetime | None = None,
) -> bool:
    """Limit unchanged quarantine retries to one automatic attempt per week."""
    if not state.get("amendment_migration_pending"):
        return False
    last_retry = state.get("amendment_migration_last_retry")
    if not isinstance(last_retry, str) or not last_retry:
        return True
    try:
        parsed = datetime.fromisoformat(last_retry.replace("Z", "+00:00"))
    except ValueError:
        return True
    if parsed.tzinfo is None:
        return True
    current = now or datetime.now(timezone.utc)
    return current >= parsed + timedelta(
        days=AMENDMENT_MIGRATION_RETRY_INTERVAL_DAYS
    )


@_serialize_pipeline_maintenance
def run_all(
    quarters_n: int,
    *,
    rebuild_outputs: bool = True,
    migrations_only: bool = False,
) -> bool:
    """Discover recent accessions and replay affected quarters by CIK."""
    log.info(f"=== All-filers mode: {quarters_n} quarters, {WORKER_COUNT} workers ===")
    state = load_state()
    migration_was_pending = (
        state.get("amendment_reducer_version", 0) < AMENDMENT_REDUCER_VERSION
    )
    if migration_was_pending:
        migration_quarters = max(quarters_n, AMENDMENT_MIGRATION_FILING_QUARTERS)
        log.info(
            "amendment reducer migration is pending; force-replaying retained "
            "amendment quarters across %s filing quarters",
            migration_quarters,
        )
        if not repair_amendments(
            migration_quarters,
            rebuild_outputs=False,
            quarantine_failures=True,
            mark_migration=True,
        ):
            return False
        state = load_state()
    identity_migration_was_pending = (
        state.get("security_identity_migration_version", 0)
        < SECURITY_IDENTITY_VERSION
    )
    if identity_migration_was_pending:
        log.info(
            "security identity migration is pending; replaying exact unsafe "
            "retained quarters from SEC source filings"
        )
        if not repair_security_identity_migration(rebuild_outputs=False):
            return False
        state = load_state()
    if migrations_only:
        # One-time migrations can consume most of a hosted runner's budget.
        # Publish their validated result first; the next scheduled run catches
        # up routine filing ingestion without redoing completed migrations.
        if rebuild_outputs:
            enforce_published_quarter_health(state)
            save_state(state)
            rebuild_registry_backed_outputs()
        log.info(
            "one-time migrations completed; deferring routine filing ingestion "
            "to the next scheduled run"
        )
        return True
    cusip_map = load_cusip_map()
    retry_checkpoint_required = False
    try:
        if (
            not migration_was_pending
            and amendment_migration_retry_due(state)
        ):
            retry_pending_amendment_migrations(state, cusip_map, quarters_n)
            retry_checkpoint_required = True
            state["amendment_migration_last_retry"] = (
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        if (
            not identity_migration_was_pending
            and security_identity_migration_retry_due(state)
        ):
            retry_pending_security_identity_migrations(state, cusip_map)
            retry_checkpoint_required = True
            state["security_identity_migration_last_retry"] = (
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        if quarter_health_retry_due(state):
            retry_pending_quarter_health(state, cusip_map)
            retry_checkpoint_required = True
            state["quarter_health_last_retry"] = (
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            )
    except KeyboardInterrupt:
        # Hosted-runner timeouts arrive before worker discovery too. Persist
        # any retry that completed before the signal so automatic continuation
        # does not repeat completed SEC filing work.
        save_state(state)
        save_cusip_map(cusip_map)
        log.warning("interrupted during pending-target retries; checkpointed")
        return False
    if retry_checkpoint_required:
        # Retry routines can publish recovered fund quarters before the
        # routine filing-index discovery begins. Finalize health ownership and
        # checkpoint state/map now so a later SEC index outage cannot leave
        # durable queues out of sync with those fund files.
        enforce_published_quarter_health(state)
        save_state(state)
        save_cusip_map(cusip_map)
    quarters = get_recent_filing_quarters(quarters_n)
    log.info(f"checking filing quarters: {quarters}")

    all_filings: list[dict] = []
    seen_accessions: set[str] = set()
    try:
        for year, q in quarters:
            for filing in download_company_idx(year, q, strict=True):
                if filing["accession"] in seen_accessions:
                    continue
                seen_accessions.add(filing["accession"])
                all_filings.append(filing)
    except FilingDiscoveryError as exc:
        log.error("filing discovery failed: %s", exc)
        return False

    log.info(f"discovered {len(all_filings)} unique 13F-HR filings")
    by_cik: dict[int, list[dict]] = defaultdict(list)
    for filing in all_filings:
        by_cik[filing["cik"]].append(filing)

    task_queue: queue.Queue = queue.Queue()
    for cik, filings in by_cik.items():
        task_queue.put((cik, filings))
    total_ciks = task_queue.qsize()
    log.info(f"queued {total_ciks} CIK groups for {WORKER_COUNT} workers")

    stop_event = threading.Event()
    io_lock = threading.Lock()
    progress_lock = threading.Lock()
    processed = [0]
    completed = [0]
    failures: list[str] = []

    @_inherit_pipeline_maintenance
    def worker() -> None:
        while not stop_event.is_set():
            try:
                cik, filings = task_queue.get_nowait()
            except queue.Empty:
                return
            try:
                with io_lock:
                    processed_set = state.setdefault("_processed_set", set())
                    pending_filings = [
                        filing for filing in filings
                        if filing["accession"] not in processed_set
                        and accession_retry_due(state, filing["accession"])
                    ]
                if not pending_filings:
                    continue
                count = replay_quarters_for_cik(
                    cik,
                    pending_filings,
                    cusip_map,
                    quarters_n,
                    state,
                    state_lock=io_lock,
                    preserve_history=True,
                    quarantine_failures=True,
                )
                with progress_lock:
                    processed[0] += count
            except Exception as exc:
                if isinstance(exc, (FilingChainError, FilingParseError)):
                    quarantine_replay_failure(
                        state, cik, pending_filings, exc, state_lock=io_lock
                    )
                    log.warning(
                        "  quarantined CIK %s replay; retaining last-known-good: %s",
                        cik,
                        exc,
                    )
                else:
                    log.error("  error replaying CIK %s: %s", cik, exc)
                    with progress_lock:
                        failures.append(f"CIK {cik}: {exc}")
            finally:
                with progress_lock:
                    completed[0] += 1
                task_queue.task_done()

    threads = [
        threading.Thread(target=worker, name=f"worker-{index}", daemon=True)
        for index in range(WORKER_COUNT)
    ]
    for thread in threads:
        thread.start()

    try:
        last_checkpoint = 0
        while any(thread.is_alive() for thread in threads):
            time.sleep(5)
            with progress_lock:
                current = completed[0]
                current_processed = processed[0]
            if current - last_checkpoint >= 25:
                with io_lock:
                    save_state(state)
                    save_cusip_map(cusip_map)
                log.info(
                    "checkpoint: %s CIK groups complete, %s new trigger filings",
                    current,
                    current_processed,
                )
                last_checkpoint = current
    except KeyboardInterrupt:
        log.warning("interrupted; signalling workers to stop")
        stop_event.set()
        with progress_lock:
            failures.append("pipeline interrupted")
        # A worker can remain inside an SEC filing retry longer than the
        # bounded join below. Persist every mutation completed before the
        # signal now; otherwise the alive-worker return path would discard the
        # newest quarantine/cooldown state and repeat those requests.
        with io_lock:
            save_state(state)
            save_cusip_map(cusip_map)
        log.info("checkpointed pipeline state after interruption")

    for thread in threads:
        thread.join(timeout=15)
    alive = [thread.name for thread in threads if thread.is_alive()]
    if alive:
        # Capture any short state update that landed while workers were being
        # joined. The lock keeps the state/map pair internally consistent.
        with io_lock:
            save_state(state)
            save_cusip_map(cusip_map)
        log.error("workers did not stop cleanly: %s", ", ".join(alive))
        return False

    if rebuild_outputs:
        enforce_published_quarter_health(state)
    with io_lock:
        save_state(state)
        save_cusip_map(cusip_map)
    log.info(f"processed {processed[0]} new filings total")
    if failures:
        log.error("pipeline failed for %s CIK group(s)", len(failures))
        return False
    if rebuild_outputs:
        rebuild_registry_backed_outputs()
    return True


def retained_new_holdings_migration_triggers() -> list[dict]:
    """Inventory retained v1 quarters that need the v2 amendment reducer."""
    triggers: list[dict] = []
    for path in sorted(FUNDS_DIR.glob("*.json")):
        try:
            with open(path) as handle:
                fund = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise FundDataError(
                f"cannot inventory amendment migration target {path}"
            ) from exc
        cik = fund.get("cik") if isinstance(fund, dict) else None
        if not isinstance(cik, int) or cik <= 0:
            raise FundDataError(
                f"cannot inventory amendment migration target {path}: invalid CIK"
            )
        for quarter in fund.get("quarters", []):
            if (
                not isinstance(quarter, dict)
                or quarter.get("composition_version") != 1
            ):
                continue
            report_date = normalize_report_date(quarter.get("report_date"))
            for source in quarter.get("source_filings", []) or []:
                if (
                    not isinstance(source, dict)
                    or source.get("amendment_kind") != "NEW_HOLDINGS"
                ):
                    continue
                accession = str(source.get("accession") or "").strip()
                if not accession or not report_date:
                    raise FundDataError(
                        f"cannot inventory v1 NEW_HOLDINGS source in {path}"
                    )
                triggers.append({
                    "cik": cik,
                    "name": str(fund.get("name") or ""),
                    "form_type": "13F-HR/A",
                    "accession": accession,
                    "date_filed": source.get("filing_date"),
                    "accepted_at": source.get("accepted_at"),
                    "report_date": report_date,
                    "filename": "",
                })
    return triggers


def _migration_target_was_published(
    target: dict,
    quarter: dict | None,
    validated: dict[tuple[int, str], tuple[bool, dict[str, dict]]],
) -> bool:
    import validate_data

    cik = target["cik"]
    report_date = target["report_date"]
    accession = target["accession"]
    quarter_key = (cik, report_date)
    if quarter_key not in validated:
        validation_errors: list[str] = []
        if (
            isinstance(quarter, dict)
            and quarter.get("composition_version")
            == AMENDMENT_REDUCER_VERSION
        ):
            validate_data.validate_amendment_composition(
                quarter,
                f"migration outcome CIK {cik} {report_date}",
                validation_errors,
            )
        else:
            validation_errors.append("quarter is not reducer v2")
        source_by_accession = {
            source["accession"]: source
            for source in (quarter or {}).get("source_filings", [])
            if (
                isinstance(source, dict)
                and isinstance(source.get("accession"), str)
            )
        }
        validated[quarter_key] = (
            not validation_errors,
            source_by_accession,
        )

    quarter_is_valid, source_by_accession = validated[quarter_key]
    if not quarter_is_valid:
        return False
    if accession in source_by_accession:
        return True
    base_source = source_by_accession.get((quarter or {}).get("base_accession"))
    target_accepted_at = _normalize_accepted_at(target.get("accepted_at"))
    return bool(
        base_source
        and base_source.get("amendment_kind") == "RESTATEMENT"
        and target_accepted_at
        and base_source.get("accepted_at")
        and base_source["accepted_at"] > target_accepted_at
    )


def amendment_migration_outcome_errors(
    targets: list[dict],
    state: dict,
) -> list[str]:
    """Require each retained v1 target to publish v2 or remain fail-closed."""
    errors: list[str] = []
    pending = state.get("amendment_migration_pending", {})
    quarantined = state.get("_quarantined", {})
    processed = state.get("_processed_set", set())
    funds: dict[int, dict | None] = {}
    validated: dict[tuple[int, str], tuple[bool, dict[str, dict]]] = {}
    for target in targets:
        cik = target["cik"]
        report_date = target["report_date"]
        accession = target["accession"]
        if cik not in funds:
            funds[cik] = load_fund(cik)
        quarter = next(
            (
                quarter
                for quarter in (funds[cik] or {}).get("quarters", [])
                if isinstance(quarter, dict)
                and quarter.get("report_date") == report_date
            ),
            None,
        )
        if _migration_target_was_published(target, quarter, validated):
            continue
        pending_target = pending.get(accession)
        if not (
            isinstance(pending_target, dict)
            and pending_target.get("cik") == cik
            and pending_target.get("report_date") == report_date
            and accession in quarantined
            and accession not in processed
        ):
            errors.append(
                f"CIK {cik} {report_date} accession {accession} is neither "
                "published with reducer v2 nor durably quarantined"
            )
    return errors


def amendment_migration_quarantine_budget(state: dict) -> int:
    """Count distinct, durably diagnosed amendment quarters before replay."""
    pending = state.get("amendment_migration_pending", {})
    quarantined = state.get("_quarantined", {})
    processed = state.get("_processed_set", set())
    quarters: set[tuple[int, str]] = set()
    for accession, target in pending.items():
        diagnostic = quarantined.get(accession)
        if (
            not isinstance(target, dict)
            or not isinstance(diagnostic, dict)
            or accession in processed
        ):
            continue
        cik = target.get("cik")
        report_date = normalize_report_date(target.get("report_date"))
        if (
            isinstance(cik, int)
            and cik > 0
            and report_date
            and diagnostic.get("cik") == cik
            and normalize_report_date(diagnostic.get("report_date"))
            == report_date
        ):
            quarters.add((cik, report_date))
    return len(quarters)


@_serialize_pipeline_maintenance
def withhold_unmigrated_new_holdings_quarters(
    targets: list[dict],
) -> int:
    """Remove unresolved v1 quarters while preserving their retry targets."""
    targets_by_cik: dict[int, list[dict]] = defaultdict(list)
    for target in targets:
        targets_by_cik[target["cik"]].append(target)

    withheld = 0
    validated: dict[tuple[int, str], tuple[bool, dict[str, dict]]] = {}
    for cik, cik_targets in targets_by_cik.items():
        fund = load_fund(cik)
        if not fund:
            continue
        by_report_date = {
            quarter.get("report_date"): quarter
            for quarter in fund.get("quarters", [])
            if isinstance(quarter, dict)
        }
        withheld_dates = {
            target["report_date"]
            for target in cik_targets
            if not _migration_target_was_published(
                target,
                by_report_date.get(target["report_date"]),
                validated,
            )
        }
        if not withheld_dates:
            continue
        fund["quarters"] = [
            quarter
            for quarter in fund.get("quarters", [])
            if not (
                isinstance(quarter, dict)
                and quarter.get("report_date") in withheld_dates
            )
        ]
        save_fund(cik, fund)
        withheld += len(withheld_dates)
    return withheld


@_serialize_pipeline_maintenance
def repair_amendments(
    quarters_n: int,
    *,
    rebuild_outputs: bool = True,
    quarantine_failures: bool = False,
    mark_migration: bool = False,
) -> bool:
    """Replay every amendment in a bounded filing window, not a known-CIK list."""
    log.info("=== Amendment repair mode: %s filing quarters ===", quarters_n)
    state = load_state()
    cusip_map = load_cusip_map()
    by_cik: dict[int, list[dict]] = defaultdict(list)
    seen: set[str] = set()
    retained_targets: list[dict] = []
    target_quarters: set[tuple[int, str]] = set()
    baseline_quarantine_budget = 0
    if mark_migration:
        # Snapshot the durable corpus baseline before authoritative replay can
        # resolve old entries or add newly ambiguous retained quarters.
        baseline_quarantine_budget = (
            amendment_migration_quarantine_budget(state)
        )
        try:
            retained_targets = retained_new_holdings_migration_triggers()
        except FundDataError as exc:
            log.error("amendment migration inventory failed: %s", exc)
            return False
        target_quarters = {
            (target["cik"], target["report_date"])
            for target in retained_targets
        }
        for trigger in retained_targets:
            accession = trigger["accession"]
            if accession in seen:
                continue
            seen.add(accession)
            by_cik[trigger["cik"]].append(trigger)
        log.info(
            "inventoried %s retained v1 NEW_HOLDINGS source(s)",
            len(retained_targets),
        )
    try:
        for year, quarter in get_recent_filing_quarters(quarters_n):
            for filing in download_company_idx(year, quarter, strict=True):
                if filing.get("form_type") != "13F-HR/A":
                    continue
                if filing["accession"] in seen:
                    continue
                seen.add(filing["accession"])
                by_cik[filing["cik"]].append(filing)
    except FilingDiscoveryError as exc:
        log.error("amendment repair discovery failed: %s", exc)
        return False

    failures: list[str] = []
    repaired = 0
    state_lock = threading.Lock()
    try:
        for cik in sorted(by_cik):
            try:
                repaired += replay_quarters_for_cik(
                    cik,
                    by_cik[cik],
                    cusip_map,
                    quarters_n,
                    state,
                    state_lock=state_lock,
                    force=True,
                    include_archives=True,
                    preserve_history=True,
                    quarantine_failures=quarantine_failures,
                    replace_only=True,
                    track_migration_targets=mark_migration,
                )
            except Exception as exc:
                failures.append(f"CIK {cik}: {exc}")
                log.error("  amendment repair failed for CIK %s: %s", cik, exc)
    finally:
        # Each replay may resolve map entries or update durable retry queues.
        # Commit all completed groups on ordinary failure and interruption.
        save_state(state)
        save_cusip_map(cusip_map)

    if failures:
        log.error("amendment repair failed for %s CIK group(s)", len(failures))
        return False
    if mark_migration:
        outcome_errors = amendment_migration_outcome_errors(
            retained_targets, state
        )
        if outcome_errors:
            for error in outcome_errors[:20]:
                log.error("amendment migration incomplete: %s", error)
            log.error(
                "amendment migration failed its postcondition for %s target(s)",
                len(outcome_errors),
            )
            return False
        pending_accessions = set(
            state.get("amendment_migration_pending", {})
        )
        unresolved_quarters = {
            (target["cik"], target["report_date"])
            for target in retained_targets
            if target["accession"] in pending_accessions
        }
        health_error = initial_migration_health_error(
            "amendment migration",
            total=len(target_quarters),
            resolved=len(target_quarters - unresolved_quarters),
            unresolved=len(unresolved_quarters),
            quarantine_budget=baseline_quarantine_budget,
        )
        if health_error:
            log.error(health_error)
            save_state(state)
            save_cusip_map(cusip_map)
            return False
        # Persist the retry queue before removing any unresolved quarter. If
        # the process stops during the file writes below, the next run still
        # knows exactly which SEC accession and report date must be restored.
        save_state(state)
        try:
            withheld = withhold_unmigrated_new_holdings_quarters(
                retained_targets
            )
        except (OSError, FundDataError) as exc:
            log.error("failed to withhold unresolved amendment quarters: %s", exc)
            return False
        if withheld:
            log.warning(
                "withheld %s unresolved v1 amendment quarter(s) from published "
                "fund and derived outputs",
                withheld,
            )
        state["amendment_reducer_version"] = AMENDMENT_REDUCER_VERSION
        state["amendment_migration_last_retry"] = (
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
    save_state(state)
    save_cusip_map(cusip_map)
    if rebuild_outputs:
        rebuild_registry_backed_outputs()
    log.info("replayed %s amendment accession(s)", repaired)
    return True


def security_identity_migration_key(cik: int, report_date: str) -> str:
    """Return the stable queue key for one retained fund quarter."""
    normalized = normalize_report_date(report_date)
    if not isinstance(cik, int) or cik <= 0 or not normalized:
        raise ValueError("security identity target requires CIK and report date")
    return f"{cik}:{normalized}"


def retained_security_identity_migration_targets() -> list[dict]:
    """Inventory retained quarters with option labels unsafe to canonicalize."""
    targets: dict[str, dict] = {}
    for path in sorted(FUNDS_DIR.glob("*.json")):
        try:
            with open(path) as handle:
                fund = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise FundDataError(
                f"cannot inventory security identity target {path}"
            ) from exc
        cik = fund.get("cik") if isinstance(fund, dict) else None
        if not isinstance(cik, int) or cik <= 0:
            raise FundDataError(
                f"cannot inventory security identity target {path}: invalid CIK"
            )
        for quarter in fund.get("quarters", []):
            if not isinstance(quarter, dict):
                continue
            if not quarter_has_unsafe_legacy_option_identity(quarter):
                continue
            report_date = normalize_report_date(quarter.get("report_date"))
            if not report_date:
                raise FundDataError(
                    f"cannot inventory security identity target {path}: "
                    "invalid report date"
                )
            key = security_identity_migration_key(cik, report_date)
            targets[key] = {
                "cik": cik,
                "report_date": report_date,
            }
    return [targets[key] for key in sorted(targets)]


def _security_identity_pending_targets(state: dict) -> list[dict]:
    pending = state.setdefault("security_identity_migration_pending", {})
    targets: list[dict] = []
    for key, raw_target in sorted(pending.items()):
        if not isinstance(raw_target, dict):
            raise FundDataError(
                f"malformed security identity migration target {key}"
            )
        cik = raw_target.get("cik")
        report_date = normalize_report_date(raw_target.get("report_date"))
        if (
            not isinstance(cik, int)
            or cik <= 0
            or not report_date
            or key != security_identity_migration_key(cik, report_date)
        ):
            raise FundDataError(
                f"malformed security identity migration target {key}"
            )
        targets.append({"cik": cik, "report_date": report_date})
    return targets


def _security_identity_target_is_resolved(
    target: dict,
    fund: dict | None = None,
) -> bool:
    if fund is None:
        fund = load_fund(target["cik"])
    quarter = next(
        (
            quarter
            for quarter in (fund or {}).get("quarters", [])
            if isinstance(quarter, dict)
            and quarter.get("report_date") == target["report_date"]
        ),
        None,
    )
    if not isinstance(quarter, dict):
        return False
    if (
        quarter.get("security_identity_version")
        != SECURITY_IDENTITY_VERSION
        or not _quarter_retains_raw_put_call(quarter)
    ):
        return False
    return not any(
        isinstance(holding, dict)
        and has_unsafe_legacy_option_identity(holding)
        for holding in quarter.get("holdings", [])
    )


def _set_security_identity_pending(
    state: dict,
    target: dict,
    *,
    reason: str,
    message: str,
) -> None:
    key = security_identity_migration_key(
        target["cik"], target["report_date"]
    )
    normalized_message = str(message).strip()[:500] or reason
    state.setdefault("security_identity_migration_pending", {})[key] = {
        "cik": target["cik"],
        "report_date": target["report_date"],
        "reason": reason,
        "message": normalized_message,
        "last_attempt_at": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
    }


def _replay_security_identity_target_group(
    cik: int,
    targets: list[dict],
    state: dict,
    cusip_map: dict[str, str],
    state_lock: threading.Lock,
) -> int:
    """Replay exact retained dates for one CIK from authoritative SEC rows."""
    target_dates = {target["report_date"] for target in targets}
    used_archives = False
    try:
        discovered, name = _discover_submission_filings(
            cik, include_archives=False
        )
        discovered_dates = {
            row.get("report_date") for row in discovered
        }
        if not target_dates.issubset(discovered_dates):
            discovered, name = _discover_submission_filings(
                cik, include_archives=True
            )
            used_archives = True
    except FilingDiscoveryError as exc:
        with state_lock:
            for target in targets:
                _set_security_identity_pending(
                    state,
                    target,
                    reason="discovery_failed",
                    message=str(exc),
                )
        log.warning(
            "security identity discovery failed for CIK %s; targets remain "
            "withheld: %s",
            cik,
            exc,
        )
        return 0

    available_dates = {
        row.get("report_date") for row in discovered
    }
    missing_dates = target_dates - available_dates
    if missing_dates:
        with state_lock:
            for target in targets:
                if target["report_date"] in missing_dates:
                    _set_security_identity_pending(
                        state,
                        target,
                        reason="report_date_not_discovered",
                        message=(
                            "SEC submissions metadata has no 13F filing chain "
                            f"for {target['report_date']}"
                        ),
                    )

    triggers = [
        row for row in discovered
        if row.get("report_date") in target_dates - missing_dates
    ]
    if triggers:
        replay_quarters_for_cik(
            cik,
            triggers,
            cusip_map,
            1,
            state,
            state_lock=state_lock,
            force=True,
            include_archives=used_archives,
            preserve_history=True,
            quarantine_failures=True,
            replace_only=True,
            discovered_submission=(discovered, name),
        )

    fund = load_fund(cik)
    resolved = 0
    with state_lock:
        pending = state.setdefault(
            "security_identity_migration_pending", {}
        )
        for target in targets:
            key = security_identity_migration_key(
                target["cik"], target["report_date"]
            )
            if _security_identity_target_is_resolved(target, fund):
                pending.pop(key, None)
                resolved += 1
            elif target["report_date"] not in missing_dates:
                _set_security_identity_pending(
                    state,
                    target,
                    reason="replay_incomplete",
                    message=(
                        "authoritative replay did not publish a current, "
                        "internally consistent security identity"
                    ),
                )
    return resolved


def _run_security_identity_replays(
    targets: list[dict],
    state: dict,
    cusip_map: dict[str, str],
) -> tuple[bool, int]:
    by_cik: dict[int, list[dict]] = defaultdict(list)
    for target in targets:
        by_cik[target["cik"]].append(target)
    if not by_cik:
        return True, 0

    tasks: queue.Queue = queue.Queue()
    for cik in sorted(by_cik):
        tasks.put((cik, by_cik[cik]))
    state_lock = threading.Lock()
    progress_lock = threading.Lock()
    stop_event = threading.Event()
    resolved = [0]
    completed_ciks = [0]
    failures: list[str] = []

    @_inherit_pipeline_maintenance
    def worker() -> None:
        while not stop_event.is_set():
            try:
                cik, cik_targets = tasks.get_nowait()
            except queue.Empty:
                return
            try:
                count = _replay_security_identity_target_group(
                    cik,
                    cik_targets,
                    state,
                    cusip_map,
                    state_lock,
                )
                with progress_lock:
                    resolved[0] += count
                    completed_ciks[0] += 1
            except Exception as exc:
                log.error(
                    "security identity replay failed unexpectedly for CIK %s: %s",
                    cik,
                    exc,
                )
                with progress_lock:
                    failures.append(f"CIK {cik}: {exc}")
                stop_event.set()
            finally:
                tasks.task_done()

    threads = [
        threading.Thread(
            target=worker,
            name=f"identity-worker-{index}",
            daemon=True,
        )
        for index in range(max(1, min(WORKER_COUNT, len(by_cik))))
    ]
    for thread in threads:
        thread.start()

    try:
        last_checkpoint = 0
        while any(thread.is_alive() for thread in threads):
            time.sleep(5)
            with progress_lock:
                current = completed_ciks[0]
            if current - last_checkpoint >= 100:
                with state_lock:
                    save_state(state)
                    save_cusip_map(cusip_map)
                log.info(
                    "security identity checkpoint: %s/%s CIKs",
                    current,
                    len(by_cik),
                )
                last_checkpoint = current
    except KeyboardInterrupt:
        log.warning("security identity migration interrupted")
        stop_event.set()
        failures.append("pipeline interrupted")
        with state_lock:
            save_state(state)
            save_cusip_map(cusip_map)
        log.info("checkpointed security identity state after interruption")

    for thread in threads:
        thread.join(timeout=15)
    alive = [thread.name for thread in threads if thread.is_alive()]
    if alive:
        # A worker may complete a short mutation during the bounded join.
        # Take one last consistent state/map snapshot before returning failure.
        with state_lock:
            save_state(state)
            save_cusip_map(cusip_map)
        failures.append(
            "security identity workers did not stop: " + ", ".join(alive)
        )
    return not failures, resolved[0]


@_serialize_pipeline_maintenance
def withhold_pending_security_identity_quarters(state: dict) -> int:
    """Remove unresolved identity targets while retaining their retry queue."""
    by_cik: dict[int, set[str]] = defaultdict(set)
    for target in _security_identity_pending_targets(state):
        by_cik[target["cik"]].add(target["report_date"])

    withheld = 0
    for cik, dates in sorted(by_cik.items()):
        fund = load_fund(cik)
        if not fund:
            continue
        before = len(fund.get("quarters", []))
        fund["quarters"] = [
            quarter
            for quarter in fund.get("quarters", [])
            if not (
                isinstance(quarter, dict)
                and quarter.get("report_date") in dates
            )
        ]
        removed = before - len(fund["quarters"])
        if removed:
            save_fund(cik, fund)
            withheld += removed
    return withheld


def security_identity_migration_outcome_errors(
    targets: list[dict],
    state: dict,
) -> list[str]:
    """Require every target to be resolved or durably queued and withheld."""
    pending = state.get("security_identity_migration_pending", {})
    funds: dict[int, dict | None] = {}
    errors: list[str] = []
    for target in targets:
        cik = target["cik"]
        report_date = target["report_date"]
        key = security_identity_migration_key(cik, report_date)
        if cik not in funds:
            funds[cik] = load_fund(cik)
        if key not in pending:
            if not _security_identity_target_is_resolved(
                target, funds[cik]
            ):
                errors.append(
                    f"{key} is neither resolved nor durably pending"
                )
            continue
        pending_target = pending.get(key)
        if (
            not isinstance(pending_target, dict)
            or not isinstance(pending_target.get("reason"), str)
            or not pending_target["reason"].strip()
            or not isinstance(pending_target.get("message"), str)
            or not pending_target["message"].strip()
            or not isinstance(pending_target.get("last_attempt_at"), str)
            or not pending_target["last_attempt_at"].strip()
        ):
            errors.append(
                f"{key} is pending without completed-attempt diagnostics"
            )
            continue
        if any(
            isinstance(quarter, dict)
            and quarter.get("report_date") == report_date
            for quarter in (funds[cik] or {}).get("quarters", [])
        ):
            errors.append(f"{key} remains published while pending")
    return errors


def initial_migration_failure_limit(target_count: int) -> int:
    """Return the bounded isolated-failure allowance for a migration."""
    if target_count <= 0:
        return 0
    return min(
        SECURITY_IDENTITY_MIGRATION_MAX_FAILURES,
        max(1, target_count // 100),
    )


def security_identity_migration_failure_limit(target_count: int) -> int:
    """Allow scoped corpus quarantines while rejecting systemic replay loss."""
    if target_count <= 0:
        return 0
    return max(1, target_count // 10)


def security_identity_migration_health_error(
    *,
    total: int,
    resolved: int,
    unresolved: int,
) -> str | None:
    """Require complete accounting and a corpus-scale 90% success floor."""
    if resolved + unresolved != total:
        return (
            "security identity migration health gate failed: "
            f"accounted for {resolved + unresolved}/{total} targets "
            f"({resolved} resolved, {unresolved} unresolved)"
        )
    unresolved_limit = security_identity_migration_failure_limit(total)
    if total and (resolved <= 0 or unresolved > unresolved_limit):
        return (
            "security identity migration health gate failed: "
            f"resolved {resolved}/{total}; {unresolved} unresolved; "
            f"allowed {unresolved_limit} (corpus-scale 90% success floor)"
        )
    return None


def initial_migration_health_error(
    label: str,
    *,
    total: int,
    resolved: int,
    unresolved: int,
    quarantine_budget: int = 0,
) -> str | None:
    """Reject zero progress or failures beyond an established safe budget."""
    new_failure_limit = initial_migration_failure_limit(total)
    baseline_budget = min(total, max(0, quarantine_budget))
    unresolved_limit = max(new_failure_limit, baseline_budget)
    if total and (resolved <= 0 or unresolved > unresolved_limit):
        return (
            f"{label} health gate failed: resolved {resolved}/{total}; "
            f"{unresolved} unresolved; allowed {unresolved_limit} "
            f"(new-failure limit {new_failure_limit}, "
            f"baseline quarantine budget {baseline_budget})"
        )
    return None


@_serialize_pipeline_maintenance
def repair_security_identity_migration(
    *,
    rebuild_outputs: bool = True,
) -> bool:
    """Run the one-time authoritative repair of unsafe retained identities."""
    log.info("=== Security identity migration ===")
    state = load_state()
    cusip_map = load_cusip_map()
    try:
        inventoried = retained_security_identity_migration_targets()
        prior_pending = _security_identity_pending_targets(state)
    except FundDataError as exc:
        log.error("security identity migration inventory failed: %s", exc)
        return False

    targets_by_key = {
        security_identity_migration_key(
            target["cik"], target["report_date"]
        ): target
        for target in [*inventoried, *prior_pending]
    }
    targets = [targets_by_key[key] for key in sorted(targets_by_key)]
    pending = state.setdefault("security_identity_migration_pending", {})
    for key, target in targets_by_key.items():
        pending.setdefault(key, {
            "cik": target["cik"],
            "report_date": target["report_date"],
            "reason": "awaiting_replay",
            "message": "awaiting authoritative SEC replay",
            "last_attempt_at": None,
        })
    # Persist the complete queue before any target quarter can be replaced or
    # withheld. Replaying an already-resolved key after interruption is safe.
    save_state(state)

    log.info(
        "inventoried %s unsafe retained quarter(s) across %s CIK(s)",
        len(targets),
        len({target["cik"] for target in targets}),
    )
    succeeded, resolved = _run_security_identity_replays(
        targets, state, cusip_map
    )
    if not succeeded:
        save_state(state)
        save_cusip_map(cusip_map)
        return False

    unresolved = sum(
        security_identity_migration_key(
            target["cik"], target["report_date"]
        )
        in state.get("security_identity_migration_pending", {})
        for target in targets
    )
    health_error = security_identity_migration_health_error(
        total=len(targets),
        resolved=resolved,
        unresolved=unresolved,
    )
    if health_error:
        log.error(health_error)
        save_state(state)
        save_cusip_map(cusip_map)
        return False

    # Queue durability precedes withholding, matching the amendment migration.
    save_state(state)
    try:
        withheld = withhold_pending_security_identity_quarters(state)
    except (OSError, FundDataError) as exc:
        log.error("failed to withhold unresolved identity quarters: %s", exc)
        return False
    outcome_errors = security_identity_migration_outcome_errors(
        targets, state
    )
    if outcome_errors:
        for error in outcome_errors[:20]:
            log.error("security identity migration incomplete: %s", error)
        return False

    state["security_identity_migration_version"] = (
        SECURITY_IDENTITY_VERSION
    )
    state["security_identity_migration_last_retry"] = (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    save_state(state)
    save_cusip_map(cusip_map)
    if withheld:
        log.warning(
            "withheld %s unresolved security identity quarter(s); they will "
            "retry automatically",
            withheld,
        )
    if rebuild_outputs:
        rebuild_registry_backed_outputs()
    log.info(
        "resolved %s/%s security identity target(s); %s remain pending",
        resolved,
        len(targets),
        len(state.get("security_identity_migration_pending", {})),
    )
    return True


@_serialize_pipeline_maintenance
def retry_pending_security_identity_migrations(
    state: dict,
    cusip_map: dict[str, str],
) -> int:
    """Retry only fail-closed identity quarters and restore successes."""
    try:
        targets = _security_identity_pending_targets(state)
    except FundDataError as exc:
        log.error("security identity retry queue is malformed: %s", exc)
        return 0
    if not targets:
        return 0
    log.info(
        "retrying %s pending security identity quarter(s) across %s CIK(s)",
        len(targets),
        len({target["cik"] for target in targets}),
    )
    succeeded, resolved = _run_security_identity_replays(
        targets, state, cusip_map
    )
    if not succeeded:
        return 0
    withhold_pending_security_identity_quarters(state)
    return resolved


def security_identity_migration_retry_due(
    state: dict,
    *,
    now: datetime | None = None,
) -> bool:
    """Limit unchanged identity quarantine retries to once per week."""
    if not state.get("security_identity_migration_pending"):
        return False
    last_retry = state.get("security_identity_migration_last_retry")
    if not isinstance(last_retry, str) or not last_retry:
        return True
    try:
        parsed = datetime.fromisoformat(last_retry.replace("Z", "+00:00"))
    except ValueError:
        return True
    if parsed.tzinfo is None:
        return True
    current = now or datetime.now(timezone.utc)
    return current >= parsed + timedelta(
        days=SECURITY_IDENTITY_MIGRATION_RETRY_INTERVAL_DAYS
    )


@_serialize_pipeline_maintenance
def main() -> int:
    parser = argparse.ArgumentParser(
        description="SEC 13F-HR data pipeline for Super Investor Seeker",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="process all 13F filers")
    group.add_argument("--cik", type=int, help="process a single filer by CIK")
    group.add_argument(
        "--repair-amendments",
        action="store_true",
        help="force-replay every 13F-HR/A found in the requested filing window",
    )
    group.add_argument(
        "--migrations-only",
        action="store_true",
        help="complete pending one-time data migrations without routine ingest",
    )
    group.add_argument(
        "--regenerate-only",
        action="store_true",
        help="rebuild registry-backed fund/stock/index data from existing fund "
             "files; no SEC filing fetches. Used by the GH Actions workflows as "
             "a dedicated post-pipeline step so partial-run cancellations "
             "still leave the static site in a consistent state.",
    )
    parser.add_argument(
        "--refresh-security-master",
        action="store_true",
        help="with --regenerate-only, ingest newly published SEC evidence, "
             "discover changed EDGAR exceptions, refresh the private security "
             "master, and regenerate derived data",
    )
    parser.add_argument(
        "--rebuild-security-master",
        action="store_true",
        help="with --regenerate-only, reconstruct immutable filing identity "
             "from SEC 13F data, backfill all SEC FTD history, discover exact "
             "EDGAR exceptions, and deterministically rebuild derived data",
    )
    parser.add_argument(
        "--apply-quantity-policy",
        action="store_true",
        help="with --regenerate-only, also apply validated quantity estimates "
             "during a clean security-master rebuild; incremental refreshes "
             "already apply this policy",
    )
    parser.add_argument(
        "--quarters",
        type=int,
        default=4,
        help="number of recent filing quarters to search (default: 4); "
             "stored fund history is retained",
    )
    parser.add_argument(
        "--defer-regeneration",
        action="store_true",
        help="ingest and persist fund/state changes without rebuilding derived "
             "stock/index outputs; intended for workflows that run one dedicated "
             "regeneration step after all ingestion sources succeed",
    )
    args = parser.parse_args()

    if args.quarters < 1:
        parser.error("--quarters must be positive")

    DATA_DIR.mkdir(exist_ok=True)
    FUNDS_DIR.mkdir(exist_ok=True)
    STOCKS_DIR.mkdir(exist_ok=True)

    if args.refresh_security_master and not args.regenerate_only:
        log.error("--refresh-security-master requires --regenerate-only")
        return 2
    if args.apply_quantity_policy and not args.regenerate_only:
        log.error("--apply-quantity-policy requires --regenerate-only")
        return 2
    if args.rebuild_security_master and not args.regenerate_only:
        log.error("--rebuild-security-master requires --regenerate-only")
        return 2
    if args.refresh_security_master and args.rebuild_security_master:
        log.error(
            "--refresh-security-master and --rebuild-security-master are "
            "mutually exclusive"
        )
        return 2
    if args.defer_regeneration and args.regenerate_only:
        log.error("--defer-regeneration cannot be used with --regenerate-only")
        return 2

    # Plain --regenerate-only is offline. The two explicit security-master
    # modes fetch only official SEC-hosted sources and therefore require the
    # declared SEC user agent.
    if args.regenerate_only:
        log.info("=== Regenerate-only mode ===")
        network_refresh = (
            args.refresh_security_master or args.rebuild_security_master
        )
        if network_refresh and (
            USER_AGENT == DEFAULT_USER_AGENT
            or "@" not in USER_AGENT
            or "example.com" in USER_AGENT
        ):
            log.error(
                "SEC_USER_AGENT with a real contact email is required for "
                "security-master refreshes"
            )
            return 2
        state = load_state()
        enforce_published_quarter_health(state)
        save_state(state)
        cutover_baseline = (
            capture_cutover_projection(FUNDS_DIR, load_cusip_registry())
            if args.rebuild_security_master
            else None
        )
        company_ticker_data = {}
        # First pass: refresh exact SEC security evidence when requested and
        # rewrite stored fund files from the resulting master.
        rebuild_tickers_in_place(
            full_refresh=args.rebuild_security_master,
            refresh_master=network_refresh,
            company_ticker_data=company_ticker_data,
        )
        rebuild_registry_backed_outputs(
            full_refresh=args.rebuild_security_master,
            company_ticker_data=company_ticker_data,
            # Selected fund series/class pages are refreshed by the explicit
            # security-master mode and are already checksum-bound here.
            refresh_official_fund_names=False,
            preserve_position_economics=network_refresh,
            apply_quantity_policy=args.refresh_security_master or args.apply_quantity_policy,
        )
        if cutover_baseline is not None:
            master = load_security_master(SEC_SECURITY_MASTER_PATH)
            cutover_result = capture_cutover_projection(
                FUNDS_DIR,
                load_cusip_registry(),
            )
            migration_report = build_cutover_difference_report(
                cutover_baseline,
                cutover_result,
                generated_at=master.get("generated_at"),
            )
            write_cutover_difference_report(
                migration_report,
                SEC_SECURITY_MASTER_MIGRATION_REPORT_PATH,
            )
            mapping_summary = migration_report["mapping_summary"]
            log.info(
                "SEC cutover shadow report: %s mapping difference(s); "
                "position invariants %s",
                mapping_summary["differences"],
                (
                    "passed"
                    if migration_report["corpus_invariants_ok"]
                    else "FAILED"
                ),
            )
            if not migration_report["corpus_invariants_ok"]:
                raise SecurityMasterRefreshError(
                    "SEC cutover shadow comparison changed retained fund "
                    "position identity or value"
                )
        return 0

    # Fail fast on a missing / placeholder SEC_USER_AGENT. SEC 403s every
    # request when the UA doesn't contain a real contact email, so catching
    # it up-front is far better than spinning on retries.
    ua_bad_reason = None
    if USER_AGENT == DEFAULT_USER_AGENT:
        ua_bad_reason = "SEC_USER_AGENT env var not set"
    elif "MYEMAIL" in USER_AGENT or "example.com" in USER_AGENT:
        ua_bad_reason = "SEC_USER_AGENT contains a placeholder"
    elif "@" not in USER_AGENT:
        ua_bad_reason = "SEC_USER_AGENT must contain a contact email"
    if ua_bad_reason:
        log.error(
            f"{ua_bad_reason}. Set SEC_USER_AGENT to something like "
            f"'YourName your.real@email.com'. SEC blocks requests without a "
            f"valid contact email."
        )
        return 2

    if args.cik is not None:
        succeeded = run_for_cik(
            args.cik,
            args.quarters,
            rebuild_outputs=not args.defer_regeneration,
        )
    elif args.repair_amendments:
        succeeded = repair_amendments(
            args.quarters,
            rebuild_outputs=not args.defer_regeneration,
            quarantine_failures=True,
            mark_migration=(
                args.quarters >= AMENDMENT_MIGRATION_FILING_QUARTERS
            ),
        )
    else:
        succeeded = run_all(
            args.quarters,
            rebuild_outputs=not args.defer_regeneration,
            migrations_only=args.migrations_only,
        )
    return 0 if succeeded else 1


if __name__ == "__main__":
    sys.exit(main())
