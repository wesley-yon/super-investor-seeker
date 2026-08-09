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
    python3 pipeline.py --regenerate-only --full-cusip-refresh
                                                    # rebuild from existing fund
                                                    # files, fully refresh the
                                                    # private CUSIP cache, and
                                                    # rebuild the snapshot CUSIP
                                                    # registry + derived outputs

Reads SEC_USER_AGENT from env. SEC requires a real contact email in the UA.
Optionally reads OPENFIGI_API_KEY for higher-rate CUSIP->ticker lookups.
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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from lxml import etree
from lxml import html as lxml_html

from data_contract import DATA_CONTRACT_VERSION
from quarter_health import (
    add_quarter_peer_observations,
    compile_peer_price_index,
    peer_price_quarter_health_issue,
    same_date_peer_price_references,
    structural_quarter_health_issues,
)
from security_identity import (
    compose_security_label,
    holding_instrument_type,
    normalize_instrument_type,
    normalize_note_security_label,
    normalize_security_identifier,
    normalize_security_kind,
    normalize_security_label,
    sec_issuer_proof_key,
    sec_ticker_titles,
    stock_filename,
    stock_lookup_id,
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
LEGACY_CUSIP_MAP_PATH = DATA_DIR / "cusip_map.json"
CUSIP_MAP_PATH = CACHE_DIR / "cusip_map.json"
OPENFIGI_DETAILS_PATH = CACHE_DIR / "openfigi_details.json"
SEC_FUND_NAMES_PATH = CACHE_DIR / "sec_fund_names.json"
SEC_FUND_TICKERS_PATH = CACHE_DIR / "company_tickers_mf.json"
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

# Concurrent workers. The SEC rate limiter is shared + thread-safe, so all
# workers collectively stay under MIN_REQUEST_INTERVAL. Parallelism exists
# purely to absorb network round-trip latency (~200-300ms per SEC request)
# and to keep forward progress when some workers are blocked in downstream
# ticker-resolution calls like OpenFIGI.
WORKER_COUNT = int(os.environ.get("PIPELINE_WORKERS", "8"))
AMENDMENT_REDUCER_VERSION = 2
COMPOSITION_HASH_VERSION = 2
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

class FilingFetchError(FilingParseError):
    """One SEC filing resource remained unavailable after HTTP retries."""

class FilingChainError(RuntimeError):
    """A quarter's filing chain is ambiguous or incomplete and must not publish."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason

class FundDataError(RuntimeError):
    """Existing materialized fund data is unsafe to read or replace."""


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
        delay = RETRY_BASE
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            self._claim_slot()
            try:
                resp = self.session.get(url, timeout=HTTP_TIMEOUT, **kwargs)
                if resp.status_code in (403, 429, 503):
                    log.warning(
                        f"  HTTP {resp.status_code} on {url} "
                        f"(attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(min(delay, RETRY_MAX))
                        delay *= 2
                        continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                last_exc = e
                log.warning(
                    f"  request error on {url}: {e} "
                    f"(attempt {attempt + 1}/{MAX_RETRIES})"
                )
                if attempt < MAX_RETRIES - 1:
                    time.sleep(min(delay, RETRY_MAX))
                    delay *= 2
                    continue
        raise RuntimeError(f"GET failed after {MAX_RETRIES} retries: {url}") from last_exc


HTTP = RateLimitedSession()
OPENFIGI_LOCK = threading.Lock()
# Process-local only: avoid asking OpenFIGI about the same identifier on every
# replayed quarter. ``None`` records that no ticker string can be returned for
# this run; durable matched/no-match provenance lives in openfigi_details.json.
_OPENFIGI_RUN_CACHE: dict[str, str | None] = {}


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
    except ValueError:
        if strict:
            raise FilingDiscoveryError(
                f"missing company.idx columns for {year} QTR{quarter}"
            )
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
        parsed = parse_information_table(xml_resp.content)
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
    row should not inherit a plain common-stock symbol when OpenFIGI or the
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


def _canonical_json_hash(payload) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

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

def _composition_holdings_payload(
    holdings: list[dict],
    *,
    include_holding_type: bool = False,
) -> list[dict]:
    """Return stable SEC-derived fields, excluding mutable display metadata."""
    payload: list[dict] = []
    for holding in holdings:
        row = {
            "cusip": holding.get("cusip"),
            "class": holding.get("class"),
            "value": holding.get("value"),
            # The downstream zero-share repair is explicitly marked; hash the
            # source value (zero) so a derived imputation does not invalidate
            # immutable SEC composition provenance.
            "shares": 0 if holding.get("shares_imputed") else holding.get("shares"),
            "put_call": holding.get("put_call"),
        }
        if include_holding_type:
            row["holding_type"] = holding_instrument_type(holding)
        payload.append(row)
    payload.sort(key=lambda row: json.dumps(
        row, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ))
    return payload

def _composition_source_decisions(source_filings: list[dict]) -> list[dict]:
    """Return the immutable v2 amendment decisions included in the hash."""
    decisions: list[dict] = []
    for source in source_filings:
        decision = {
            "accession": source.get("accession"),
            "source_hash": source.get("source_hash"),
            "form_type": source.get("form_type"),
            "accepted_at": source.get("accepted_at"),
            "amendment_number": source.get("amendment_number"),
            "amendment_kind": source.get("amendment_kind"),
            "composition_action": source.get("composition_action"),
            "new_holdings_overlap": source.get("new_holdings_overlap"),
        }
        if "security_identity_version" in source:
            decision["security_identity_version"] = source.get(
                "security_identity_version"
            )
        decisions.append(decision)
    return decisions

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
    payload = {
        "composition_version": composition_version,
        "report_date": report_date,
        "base_accession": base_accession,
        "applied_accessions": applied_accessions,
        "applied_source_hashes": applied_source_hashes,
        "holdings": _composition_holdings_payload(
            holdings,
            include_holding_type=composition_hash_version >= 2,
        ),
    }
    if composition_hash_version >= 2:
        payload["composition_hash_version"] = composition_hash_version
    if composition_version == 2:
        if source_filings is None:
            raise ValueError("v2 composition hashes require source filing decisions")
        payload["source_decisions"] = _composition_source_decisions(source_filings)
        if security_identity_version is not None:
            payload["security_identity_version"] = security_identity_version
    return _canonical_json_hash(payload)


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
        signature = _canonical_json_hash({
            key: component.get(key)
            for key in (
                "cik", "report_date", "filing_date", "accepted_at", "accession",
                "form_type", "amendment_number", "amendment_kind", "source_hash",
                "reported_entry_total", "reported_value_total",
                "normalized_value_total", "value_unit_policy_version",
                "value_multiplier", "value_unit_method",
                "value_unit_confidence", "value_unit_evidence", "holdings",
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
        "source_hash": component["source_hash"],
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


_SYNTHETIC_IDENTIFIER_RE = re.compile(r"^0{3,}([A-Z]{2,7})$")
_SYNTHETIC_IDENTIFIER_LITERALS = frozenset({
    "000000NAN",
    "0LOOKITUP",
    "MONEYMRKT",
    "OOOOOOOOO",
})


def synthetic_identifier_ticker_hint(identifier: str | None) -> str | None:
    """Return a ticker-like suffix for obvious zero-padded fake identifiers.

    This is only an observability hint. We never treat the suffix as a
    canonical CUSIP->ticker mapping.
    """
    raw = str(identifier or "").strip().upper()
    match = _SYNTHETIC_IDENTIFIER_RE.match(raw)
    if not match:
        return None
    suffix = match.group(1)
    if 1 < len(suffix) <= 5 and suffix[0].isalpha():
        return suffix
    return None


def is_synthetic_identifier(identifier: str | None) -> bool:
    """Whether an identifier is obviously synthetic filler, not a real CUSIP."""
    raw = str(identifier or "").strip().upper()
    if raw in _SYNTHETIC_IDENTIFIER_LITERALS:
        return True
    match = _SYNTHETIC_IDENTIFIER_RE.match(raw)
    return bool(match)


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
                    by_report_position[
                        (report_date, cusip, holding_type)
                    ].append(price)
                by_position[(cusip, holding_type)].append(price)

    report_refs = {
        key: statistics.median(values)
        for key, values in by_report_position.items()
        if values
    }
    position_refs = {
        key: statistics.median(values)
        for key, values in by_position.items()
        if values
    }
    return report_refs, position_refs


@_serialize_pipeline_maintenance
def repair_zero_share_holdings_in_place() -> int:
    """Impute obvious missing share counts using cross-filer quarter prices.

    Some filers report a positive market value with sshPrnamt rounded down to
    zero. When the row's value is at least one plausible share at the quarter's
    median price for that CUSIP and instrument type, estimate the share count
    from that price while preserving the original reported zero.

    Existing imputations are reset in a complete first pass before reference
    prices are rebuilt. This makes the repair self-healing when canonical
    identity or peer evidence changes: stale derived shares cannot vote on
    their own replacement, and rows without a current reference fail closed
    to their reported zero."""
    log.info("Repairing zero-share holdings in place...")

    if not FUNDS_DIR.exists():
        log.info("  no funds directory; nothing to repair")
        return 0

    fund_paths = sorted(FUNDS_DIR.glob("*.json"))
    total = len(fund_paths)
    reset_files = 0
    reset_rows = 0

    # Phase 1: restore every derived row to its immutable reported value before
    # any peer reference is calculated. Persist this pass so a later crash
    # leaves conservative reported zeros rather than stale derived shares.
    for fp in fund_paths:
        try:
            with open(fp) as f:
                fund = json.load(f)
        except json.JSONDecodeError:
            continue

        changed = False
        for quarter in fund.get("quarters", []):
            for holding in quarter.get("holdings", []):
                if holding.get("shares_imputed") is not True:
                    continue
                holding["shares"] = 0
                holding["reported_shares"] = 0
                del holding["shares_imputed"]
                changed = True
                reset_rows += 1

        if changed:
            _atomic_write_json(fp, fund)
            reset_files += 1

    if reset_rows:
        log.info(
            f"  reset {reset_rows} prior imputations in {reset_files}/{total} "
            "fund files"
        )

    # Phase 2: rebuild references only after all stale derived shares have
    # disappeared, then reproduce each still-qualifying estimate.
    report_refs, position_refs = build_zero_share_price_reference_maps()
    if not report_refs and not position_refs:
        log.info(
            "  no price references available; prior imputations remain "
            "at reported zero"
        )
        return 0

    updated_files = 0
    imputed_rows = 0

    for idx, fp in enumerate(fund_paths):
        if idx % 2000 == 0 and idx > 0:
            log.info(
                f"    zero-share repair progress: {idx}/{total} files "
                f"({updated_files} changed, {imputed_rows} rows imputed)"
            )
        try:
            with open(fp) as f:
                fund = json.load(f)
        except json.JSONDecodeError:
            continue

        changed = False
        for quarter in fund.get("quarters", []):
            report_date = quarter.get("report_date") or ""
            for holding in quarter.get("holdings", []):
                value = holding.get("value") or 0
                shares = holding.get("shares") or 0
                if value <= 0 or shares != 0:
                    continue
                cusip = str(holding.get("cusip") or "").strip().upper()
                if not cusip:
                    continue
                holding_type = classify_saved_holding(holding)
                price = report_refs.get(
                    (report_date, cusip, holding_type)
                ) or position_refs.get((cusip, holding_type))
                if price is None or price <= 0 or value < price:
                    continue
                imputed = round(value / price, 6)
                if imputed <= 0:
                    continue
                holding["reported_shares"] = 0
                holding["shares"] = int(imputed) if float(imputed).is_integer() else imputed
                holding["shares_imputed"] = True
                changed = True
                imputed_rows += 1

        if changed:
            _atomic_write_json(fp, fund)
            updated_files += 1

    log.info(
        f"  zero-share repair updated {updated_files}/{total} fund files "
        f"and imputed {imputed_rows} rows"
    )
    return imputed_rows


def parse_information_table(xml_bytes: bytes) -> list[dict] | None:
    """Parse the 13F informationTable XML into a list of holding dicts.
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
                h["cusip"] = text.upper()
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
        if cusip in ("000000000", "000000NAN", "N/A", ""):
            continue
        if "cusip" in h and "value" in h:
            holding_type = _classify_holding(h)
            entry = {
                "ticker": None,
                "issuer": h.get("issuer", ""),
                "cusip": h["cusip"],
                "class": h.get("class", ""),
                "value": h["value"],
                "shares": h.get("shares", 0),
                "holding_type": holding_type,
            }
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
    "RLTY": "REALTY",
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
    "BANCSH": "BANCSHARES",
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

# Stocks where different share classes trade under different tickers but
# normalize to the same issuer name (so name-only lookup in company_tickers.json
# can't distinguish them). We disambiguate via the 13F filing's titleOfClass
# field, which typically contains "CL A", "CL B", "CLASS A", etc.
#
# Keys are normalized issuer names as produced by normalize_name() below.
# Values map the class letter to the correct ticker.
MULTI_CLASS_DISPATCH: dict[str, dict[str, str]] = {
    "BERKSHIREHATHAWAY":        {"A": "BRK-A", "B": "BRK-B"},
    "ALPHABET":                 {"A": "GOOGL", "C": "GOOG"},
    "FOXCORPORATION":           {"A": "FOXA",  "B": "FOX"},
    "FOX":                      {"A": "FOXA",  "B": "FOX"},
    "LIBERTYMEDIA":             {"A": "LSXMA", "B": "LSXMB", "K": "LSXMK"},
    "LIBERTYBROADBAND":         {"A": "LBRDA", "C": "LBRDK"},
    "LIBERTYLATINAMERICA":      {"A": "LILA",  "C": "LILAK"},
    "LIBERTYLIVE":              {"A": "LLYVA", "C": "LLYVK"},
    "LIBERTYGLOBAL":            {"A": "LBTYA", "B": "LBTYB", "C": "LBTYK"},
    "NEWSCORP":                 {"A": "NWSA",  "B": "NWS"},
    "NEWSCORPORATION":          {"A": "NWSA",  "B": "NWS"},
    "DISCOVERY":                {"A": "DISCA", "B": "DISCB", "C": "DISCK"},
    "ZILLOWGROUP":              {"A": "ZG",    "C": "Z"},
    "LENNAR":                   {"A": "LEN",   "B": "LEN-B"},
    "VIACOMCBS":                {"A": "PARAA", "B": "PARA"},
    "PARAMOUNT":                {"A": "PARAA", "B": "PARA"},
    "MOOG":                     {"A": "MOG-A", "B": "MOG-B"},
}

CLASS_LETTER_RE = re.compile(r"\bCL(?:ASS)?\s*([A-K])\b", re.IGNORECASE)


def resolve_multi_class(normalized_issuer: str, title_of_class: str) -> str | None:
    """For a known multi-class security, look at titleOfClass to figure out
    which share class this holding is (A/B/C/K) and return the matching ticker.
    Returns None if the issuer isn't in MULTI_CLASS_DISPATCH or the class
    letter can't be extracted from the title."""
    dispatch = MULTI_CLASS_DISPATCH.get(normalized_issuer)
    if not dispatch:
        return None
    m = CLASS_LETTER_RE.search(title_of_class or "")
    if not m:
        return None
    letter = m.group(1).upper()
    return dispatch.get(letter)


def normalize_name(name: str) -> str:
    """Normalize an issuer name into a tight key for fuzzy CUSIP->ticker matching.
    Collapses away entity suffixes, share classes, state markers, and common
    13F abbreviations, then joins tokens without spaces for robust matching."""
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


def _ticker_preference_key(ticker: str) -> tuple:
    """Sort key for picking the "primary" ticker when multiple share classes
    or preferreds all normalize to the same company name. Prefer plain
    tickers (no dash or dot) first, then shorter, then alphabetical."""
    has_special = ("-" in ticker) or ("." in ticker)
    return (has_special, len(ticker), ticker)


def _load_company_tickers_data() -> dict | list:
    """Fetch SEC's raw company_tickers payload, falling back to the cached copy."""
    url = "https://www.sec.gov/files/company_tickers.json"
    cache_path = DATA_DIR / "company_tickers.json"
    log.info("Fetching company_tickers.json")
    data = None
    try:
        data = HTTP.get(url).json()
        DATA_DIR.mkdir(exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        log.warning(f"  remote fetch failed: {e}")
        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    data = json.load(f)
                log.info(f"  using cached copy from {cache_path}")
            except Exception as e2:
                log.warning(f"  cache read failed: {e2}")

    if not data:
        log.warning("  no ticker data available — CUSIPs will not resolve on this run")
        return {}

    return data


def load_sec_fund_name_cache() -> dict[str, dict]:
    """Load private SEC series/class names keyed by listed fund symbol."""

    if not SEC_FUND_NAMES_PATH.exists():
        return {}
    try:
        payload = json.loads(SEC_FUND_NAMES_PATH.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        log.warning(f"  SEC fund-name cache read failed: {exc}")
        return {}
    if not isinstance(payload, dict):
        log.warning("  SEC fund-name cache was not an object; ignoring")
        return {}
    return {
        str(symbol).strip().upper(): entry
        for symbol, entry in payload.items()
        if (
            _OPENFIGI_PLAIN_TICKER_RE.fullmatch(
                str(symbol).strip().upper()
            )
            and isinstance(entry, dict)
            and normalize_security_label(entry.get("name"))
        )
    }


def _sec_fund_name_map(cache: dict[str, dict]) -> dict[str, str]:
    """Return only validated symbol -> official-name cache values."""

    names: dict[str, str] = {}
    for symbol, entry in cache.items():
        name = normalize_security_label(entry.get("name"))
        if name:
            names[symbol] = name
    return names


def _load_sec_fund_tickers_data() -> dict:
    """Fetch SEC's fund-symbol map, falling back to a private cached copy."""

    url = "https://www.sec.gov/files/company_tickers_mf.json"
    data = None
    try:
        data = HTTP.get(url).json()
        if isinstance(data, dict):
            _atomic_write_json(SEC_FUND_TICKERS_PATH, data)
    except Exception as exc:
        log.warning(f"  SEC fund-symbol fetch failed: {exc}")
        if SEC_FUND_TICKERS_PATH.exists():
            try:
                data = json.loads(SEC_FUND_TICKERS_PATH.read_text())
                log.info(
                    f"  using cached copy from {SEC_FUND_TICKERS_PATH}"
                )
            except (OSError, json.JSONDecodeError) as cache_exc:
                log.warning(
                    f"  SEC fund-symbol cache read failed: {cache_exc}"
                )
    return data if isinstance(data, dict) else {}


def _parse_sec_fund_series_page(
    page_text: str,
) -> tuple[dict[str, str], dict[str, str]]:
    """Parse official series and class names from one registrant page."""

    if not page_text:
        return {}, {}
    try:
        document = lxml_html.fromstring(page_text)
    except (etree.ParserError, ValueError):
        return {}, {}

    series_names: dict[str, str] = {}
    class_names: dict[str, str] = {}
    conflicts: set[str] = set()

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
    return series_names, class_names


def _sec_official_fund_name(
    series_name: str | None,
    class_name: str | None,
) -> str | None:
    """Compose one full SEC series/class name without duplicating the series."""

    series = normalize_security_label(series_name)
    class_contract = normalize_security_label(class_name)
    if not series:
        return None
    if (
        not class_contract
        or class_contract.casefold() == series.casefold()
    ):
        return series
    if series.casefold() in class_contract.casefold():
        return class_contract
    return normalize_security_label(f"{series} — {class_contract}")


def refresh_sec_fund_names(tickers: set[str]) -> dict[str, str]:
    """Resolve selected fund symbols to official SEC series/class names.

    SEC publishes symbol -> registrant/series/class IDs in
    company_tickers_mf.json. One registrant-level EDGAR page then supplies all
    official series and class names for that sponsor, so this remains bounded
    even for large same-name ETF families.
    """

    cache = load_sec_fund_name_cache()
    names = _sec_fund_name_map(cache)
    requested = {
        str(ticker).strip().upper()
        for ticker in tickers
        if _OPENFIGI_PLAIN_TICKER_RE.fullmatch(
            str(ticker).strip().upper()
        )
    }
    if not requested:
        return names

    ticker_data = _load_sec_fund_tickers_data()
    fields = ticker_data.get("fields")
    rows = ticker_data.get("data")
    if not (
        isinstance(fields, list)
        and isinstance(rows, list)
        and {"cik", "seriesId", "classId", "symbol"}.issubset(fields)
    ):
        log.warning("  SEC fund-symbol payload has an unsupported shape")
        for symbol in requested:
            names.pop(symbol, None)
        return names
    indexes = {field: fields.index(field) for field in fields}
    records_by_symbol: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for row in rows:
        if not isinstance(row, list) or len(row) < len(fields):
            continue
        symbol = str(row[indexes["symbol"]] or "").strip().upper()
        if symbol not in requested:
            continue
        cik = str(row[indexes["cik"]] or "").strip()
        series_id = str(row[indexes["seriesId"]] or "").strip().upper()
        class_id = str(row[indexes["classId"]] or "").strip().upper()
        if (
            cik.isdigit()
            and re.fullmatch(r"S\d+", series_id)
            and re.fullmatch(r"C\d+", class_id)
        ):
            records_by_symbol[symbol].add(
                (cik.zfill(10), series_id, class_id)
            )

    unique_records = {
        symbol: next(iter(records))
        for symbol, records in records_by_symbol.items()
        if len(records) == 1
    }
    pending: set[str] = set()
    fallback_names: dict[str, str] = {}
    cache_changed = False
    for symbol in requested:
        names.pop(symbol, None)
        current_record = unique_records.get(symbol)
        cached_entry = cache.get(symbol)
        cached_record = None
        if isinstance(cached_entry, dict):
            cached_cik = str(cached_entry.get("cik") or "").strip()
            cached_series_id = str(
                cached_entry.get("series_id") or ""
            ).strip().upper()
            cached_class_id = str(
                cached_entry.get("class_id") or ""
            ).strip().upper()
            if (
                cached_cik.isdigit()
                and re.fullmatch(r"S\d+", cached_series_id)
                and re.fullmatch(r"C\d+", cached_class_id)
            ):
                cached_record = (
                    cached_cik.zfill(10),
                    cached_series_id,
                    cached_class_id,
                )
        cached_name = normalize_security_label(
            cached_entry.get("name")
            if isinstance(cached_entry, dict)
            else None
        )
        if (
            current_record
            and cached_record == current_record
            and cached_name
        ):
            # Re-fetch the bounded registrant page so a same-series rename is
            # observed. The tuple-matched cache is only an availability
            # fallback when the SEC page cannot be fetched or parsed.
            fallback_names[symbol] = cached_name
        elif symbol in cache:
            cache.pop(symbol, None)
            cache_changed = True
        if current_record:
            pending.add(symbol)

    by_cik: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for symbol in pending:
        cik, series_id, class_id = unique_records[symbol]
        by_cik[cik].append((symbol, series_id, class_id))

    added = 0
    for cik, records in sorted(by_cik.items()):
        url = (
            "https://www.sec.gov/cgi-bin/browse-edgar"
            f"?scd=series&CIK={cik}&action=getcompany"
        )
        try:
            page_text = HTTP.get(url).text
        except Exception as exc:
            log.warning(
                f"  SEC fund series lookup failed for CIK {cik}: {exc}"
            )
            for symbol, _series_id, _class_id in records:
                if fallback_name := fallback_names.get(symbol):
                    names[symbol] = fallback_name
            continue
        series_names, class_names = _parse_sec_fund_series_page(page_text)
        for symbol, series_id, class_id in records:
            name = _sec_official_fund_name(
                series_names.get(series_id),
                class_names.get(class_id),
            )
            if not name:
                if fallback_name := fallback_names.get(symbol):
                    names[symbol] = fallback_name
                continue
            cache_entry = {
                "cik": cik,
                "class_id": class_id,
                "name": name,
                "series_id": series_id,
            }
            if cache.get(symbol) != cache_entry:
                cache[symbol] = cache_entry
                cache_changed = True
                added += 1
            names[symbol] = name

    if added or cache_changed:
        _atomic_write_json(SEC_FUND_NAMES_PATH, cache, sort_keys=True)
    if added:
        log.info(
            f"  cached {added} new or updated SEC fund series/class name(s)"
        )
    return names


def fetch_company_ticker_maps() -> tuple[dict[str, str], dict[str, set[str]]]:
    """Build both normalized-name -> ticker and ticker -> normalized-name maps.

    When multiple tickers normalize to the same name, use a plain/short ticker
    only when it is the unique structural preference. Equally plausible plain
    tickers are intentionally omitted from issuer-name fallback."""
    data = _load_company_tickers_data()
    if not data:
        return {}, {}

    buckets: dict[str, set[str]] = {}
    ticker_to_norms: dict[str, set[str]] = defaultdict(set)
    entries = data.values() if isinstance(data, dict) else data
    for entry in entries:
        ticker = (entry.get("ticker") or "").upper()
        title = entry.get("title") or ""
        if not (ticker and title):
            continue
        norm = normalize_name(title)
        if norm:
            buckets.setdefault(norm, set()).add(ticker)
            ticker_to_norms[ticker].add(norm)

    name_to_ticker: dict[str, str] = {}
    ambiguous_dispatched = 0
    ambiguous_resolved = 0
    ambiguous_unresolved = 0
    for norm, tickers in buckets.items():
        if len(tickers) == 1:
            name_to_ticker[norm] = next(iter(tickers))
            continue
        if norm in MULTI_CLASS_DISPATCH:
            ambiguous_dispatched += 1
            continue
        # A common ticker can be structurally distinct from its preferred or
        # warrant suffixes (BAC versus BAC-PB). If multiple candidates tie on
        # that structural preference (PRU versus PFH), issuer text alone is not
        # independent proof and must fail closed.
        ranked = sorted(
            tickers,
            key=_ticker_preference_key,
        )
        best_shape = _ticker_preference_key(ranked[0])[:2]
        best = [
            ticker
            for ticker in ranked
            if _ticker_preference_key(ticker)[:2] == best_shape
        ]
        if len(best) != 1:
            ambiguous_unresolved += 1
            continue
        name_to_ticker[norm] = best[0]
        ambiguous_resolved += 1

    log.info(
        f"  loaded {len(name_to_ticker)} unique normalized names "
        f"({ambiguous_dispatched} multi-class via dispatch, "
        f"{ambiguous_resolved} resolved by unique preference, "
        f"{ambiguous_unresolved} left ambiguous)"
    )
    return name_to_ticker, ticker_to_norms


MIN_PREFIX_LEN = 8


def prefix_lookup(norm: str, name_to_ticker: dict[str, str],
                  _cache: dict[str, str | None] = {}) -> str | None:
    """Return a ticker when ``norm`` is a unique truncated prefix of one name.

    This intentionally only allows the issuer name from the filing to be
    *shorter* than the canonical SEC company title. The reverse direction
    (`norm.startswith(known)`) caused false positives like mapping
    "SOUTHERN MO BANCORP" to "SO" (Southern Co) just because both names start
    with "SOUTHERN". Min 8 chars to avoid broad fuzzy matches."""
    if norm in _cache:
        return _cache[norm]
    if len(norm) < MIN_PREFIX_LEN:
        _cache[norm] = None
        return None
    matches = []
    for known in name_to_ticker:
        if known.startswith(norm):
            matches.append(known)
            if len(matches) > 1:
                break
    result = name_to_ticker[matches[0]] if len(matches) == 1 else None
    _cache[norm] = result
    return result


def resolve_ticker_from_name(
    issuer: str | None,
    title_of_class: str | None,
    name_to_ticker: dict[str, str],
) -> str | None:
    """Resolve a ticker from issuer/class text using exact + safe-prefix rules."""
    norm = normalize_name(issuer or "")
    if not norm:
        return None
    multi = resolve_multi_class(norm, title_of_class or "")
    if multi:
        return multi
    return name_to_ticker.get(norm) or prefix_lookup(norm, name_to_ticker)


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
    indent: int | None = 2,
    sort_keys: bool = False,
    fsync_parent: bool = True,
) -> None:
    """Write JSON atomically: render to a sibling temp file, fsync, then
    os.replace() into place. A SIGTERM or power loss mid-write leaves either
    the old file or the new file — never a half-flushed one that
    json.load() would reject."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Sibling temp file to guarantee same filesystem (os.replace is atomic
    # only within a filesystem).
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    try:
        with open(tmp, "w") as f:
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
        removed = False
        try:
            tmp.unlink()
            removed = True
        except FileNotFoundError:
            pass
        if removed:
            _fsync_directory(path.parent)
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
            # multi-hour OpenFIGI re-resolve on the next run. Rename the bad
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


def _merge_committed_registry_display_metadata(
    private_registry: dict,
    committed_registry: dict,
) -> dict:
    """Use snapshot display metadata as a floor for a stale private cache.

    The private cache carries current operational/filing evidence, but it can
    predate display enrichments already present in the snapshot's data copy.
    Merge only reader-facing identity fields so a restored cache cannot
    silently erase a known kind, label, or descriptive fund name.
    """

    merged = {
        identifier: (
            dict(entry) if isinstance(entry, dict) else entry
        )
        for identifier, entry in private_registry.items()
    }
    for identifier, committed_entry in committed_registry.items():
        if not isinstance(committed_entry, dict):
            continue
        private_entry = merged.get(identifier)
        if not isinstance(private_entry, dict):
            merged[identifier] = dict(committed_entry)
            continue

        entry = dict(private_entry)
        committed_label = normalize_security_label(
            committed_entry.get("security_label"),
            identifier=identifier,
        )
        private_label = normalize_security_label(
            entry.get("security_label"),
            identifier=identifier,
        )
        if committed_label and not private_label:
            entry["security_label"] = committed_label
            committed_source = str(
                committed_entry.get("label_source") or ""
            ).strip()
            if committed_source:
                entry["label_source"] = committed_source
        elif (
            committed_label
            and private_label == committed_label
            and not str(entry.get("label_source") or "").strip()
        ):
            committed_source = str(
                committed_entry.get("label_source") or ""
            ).strip()
            if committed_source:
                entry["label_source"] = committed_source

        committed_kind = normalize_security_kind(
            committed_entry.get("security_kind")
        )
        private_kind = normalize_security_kind(entry.get("security_kind"))
        if committed_kind and not private_kind:
            entry["security_kind"] = committed_kind
            committed_source = str(
                committed_entry.get("security_kind_source") or ""
            ).strip()
            if committed_source:
                entry["security_kind_source"] = committed_source
        elif (
            committed_kind
            and private_kind == committed_kind
            and not str(entry.get("security_kind_source") or "").strip()
        ):
            committed_source = str(
                committed_entry.get("security_kind_source") or ""
            ).strip()
            if committed_source:
                entry["security_kind_source"] = committed_source

        committed_product = normalize_security_label(
            committed_entry.get("product_name"),
            identifier=identifier,
        )
        private_product = normalize_security_label(
            entry.get("product_name"),
            identifier=identifier,
        )
        committed_product_source = str(
            committed_entry.get("product_name_source") or ""
        ).strip()
        private_product_source = str(
            entry.get("product_name_source") or ""
        ).strip()
        committed_fund_symbol = _registry_fund_symbol(
            identifier=identifier,
            entry=committed_entry,
        )
        private_fund_symbol = _registry_fund_symbol(
            identifier=identifier,
            entry=entry,
        )
        authoritative_case_only_product = bool(
            committed_product
            and private_product
            and committed_product.casefold() == private_product.casefold()
            and committed_fund_symbol
            and committed_fund_symbol == private_fund_symbol
            and committed_product_source.startswith("sec_fund_")
            and (
                not private_product_source
                or private_product_source.startswith("openfigi")
            )
        )
        aliases = (
            identifier,
            entry.get("ticker"),
            entry.get("security_label"),
            committed_entry.get("ticker"),
            committed_entry.get("security_label"),
        )
        if committed_product and (
            not private_product
            or authoritative_case_only_product
            or _fund_product_name_degrades_existing(
                committed_product,
                private_product,
                aliases=aliases,
            )
        ):
            entry["product_name"] = committed_product
            if committed_product_source:
                entry["product_name_source"] = committed_product_source
            else:
                entry.pop("product_name_source", None)
        elif (
            committed_product
            and private_product == committed_product
            and not str(entry.get("product_name_source") or "").strip()
        ):
            if committed_product_source:
                entry["product_name_source"] = committed_product_source

        merged[identifier] = entry
    return merged


def load_cusip_map() -> dict[str, str]:
    return _load_json_dict_with_fallback(
        CUSIP_MAP_PATH,
        LEGACY_CUSIP_MAP_PATH,
        sort_keys=True,
    )


def save_cusip_map(cusip_map: dict[str, str]) -> None:
    _atomic_write_json(CUSIP_MAP_PATH, cusip_map, sort_keys=True)


def load_openfigi_details() -> dict[str, dict]:
    """Load private per-identifier OpenFIGI metadata, if available."""

    if not OPENFIGI_DETAILS_PATH.exists():
        return {}
    payload = _load_json_dict_with_fallback(
        OPENFIGI_DETAILS_PATH,
        OPENFIGI_DETAILS_PATH,
        sort_keys=True,
    )
    return {
        str(identifier).strip().upper(): detail
        for identifier, detail in payload.items()
        if (
            str(identifier).strip()
            and isinstance(detail, dict)
            and detail.get("status") in {"matched", "no_match"}
        )
    }


def save_openfigi_details(details: dict[str, dict]) -> None:
    """Persist selected OpenFIGI metadata without publishing the private map."""

    _atomic_write_json(OPENFIGI_DETAILS_PATH, details, sort_keys=True)


def load_cusip_registry() -> dict:
    private_registry = _load_json_dict_with_fallback(
        CUSIP_REGISTRY_PATH,
        LEGACY_CUSIP_REGISTRY_PATH,
        sort_keys=True,
    )
    committed_registry = _read_json_object(
        LEGACY_CUSIP_REGISTRY_PATH
    ) or {}
    return _merge_committed_registry_display_metadata(
        private_registry,
        committed_registry,
    )


def save_cusip_registry(registry: dict) -> None:
    for path in (CUSIP_REGISTRY_PATH, LEGACY_CUSIP_REGISTRY_PATH):
        _atomic_write_json(path, registry, sort_keys=True)


OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
OPENFIGI_FREE_BATCH = 10
OPENFIGI_KEYED_BATCH = 100
OPENFIGI_FREE_INTERVAL = 2.5
OPENFIGI_KEYED_INTERVAL = 0.3
_OPENFIGI_DETAIL_FIELDS = (
    "ticker",
    "name",
    "securityDescription",
    "marketSector",
    "securityType",
    "securityType2",
    "exchCode",
)
_OPENFIGI_PLAIN_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,15}$")
_OPENFIGI_DISPLAY_TICKER_RE = re.compile(
    r"^[A-Z][A-Z0-9.-]{0,15}(?:/(?:W|WS|RT))?$"
)
_DISPLAY_ONLY_SECURITY_CLASS_RE = re.compile(
    r"\b(?:RIGHT|RIGHTS|WARRANT|WARRANTS|WT|WTS)\b",
    re.IGNORECASE,
)
_DISPLAY_ONLY_TICKER_SUFFIX_RE = re.compile(
    r"(?:[-./](?:R|RT|RIGHT|RIGHTS|W|WS|WT|WTS))$",
    re.IGNORECASE,
)
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
_FILER_MUTUAL_FUND_TICKER_RE = re.compile(r"^[A-Z]{4}X$")
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
_FUND_KIND_DISCOVERY_RE = re.compile(
    r"\bETFS?\b|\bETNS?\b|\bFUNDS?\b|\bFDS\b|\bPORTFOLIOS?\b|"
    r"EXCHANGE[ -]TRADED|MONEY MARKET|\bINDEX FDS\b|"
    r"\bTRUST\b|\bUNITS?\b",
    re.IGNORECASE,
)
_FUND_PRODUCT_NAME_KINDS = frozenset({
    "ETF",
    "ETN",
    "MUTUAL FUND",
    "CLOSED-END FUND",
})
_EQUITY_FUND_SECURITY_KINDS = frozenset({
    "ETF",
    "MUTUAL FUND",
    "CLOSED-END FUND",
})
_FUND_IDENTITY_TICKER_SOURCES = frozenset({
    "cusip_map_vetted",
    "manual_override",
    "openfigi_plain_ticker",
    "openfigi_prior_registry_ticker",
})
_FUND_PRODUCT_CLASS_GENERIC_TOKENS = frozenset({
    "ADR",
    "BEN",
    "BENEFICIAL",
    "BENEF",
    "BENF",
    "BENFIN",
    "BD",
    "BOND",
    "BONDS",
    "CALL",
    "CL",
    "CLASS",
    "CLOSED",
    "CEF",
    "CEM",
    "CREATION",
    "CM",
    "CMN",
    "CO",
    "COM",
    "COMPANY",
    "COMMON",
    "CORP",
    "CORPORATION",
    "END",
    "EQ",
    "EQUITY",
    "EQUITIES",
    "ETF",
    "ETFS",
    "ETP",
    "EXCHANGE",
    "FD",
    "FDS",
    "FND",
    "FNDS",
    "FEN",
    "FRACTI",
    "FRACTIONAL",
    "FUND",
    "FUNDS",
    "GROUP",
    "HLDG",
    "HLDGS",
    "HOLDINGS",
    "INT",
    "IDX",
    "INC",
    "INCORPORATED",
    "ISHARES",
    "LOAD",
    "LLC",
    "LIMITED",
    "LP",
    "LTD",
    "M",
    "MF",
    "MFA",
    "MFB",
    "MFC",
    "MFD",
    "MMF",
    "MPL",
    "MONEY",
    "MARKET",
    "MUTL",
    "MUT",
    "MUTUAL",
    "NEW",
    "NON",
    "NTF",
    "OF",
    "OPEN",
    "OPT",
    "OPTION",
    "OPTIONS",
    "OTHER",
    "ORD",
    "ORDINARY",
    "PORTFOLIO",
    "PORTFOLIOS",
    "PLC",
    "PUT",
    "REP",
    "REPRESENT",
    "RET",
    "SBI",
    "SER",
    "SERIES",
    "SH",
    "SHARE",
    "SHARES",
    "SHS",
    "SOLUTIONS",
    "SPD",
    "SPDR",
    "STATE",
    "STK",
    "STOCK",
    "STREET",
    "SWEEP",
    "TAXABLE",
    "THE",
    "TR",
    "TRD",
    "TRADED",
    "ADDED",
    "UIT",
    "UNIT",
    "UNITS",
    "UNDIVIDED",
    "UNI",
    "USD",
    "UT",
})
_FUND_PRODUCT_SHORT_SPECIFIC_TOKENS = frozenset({
    "AI",
    "AU",
    "CA",
    "CN",
    "DM",
    "EM",
    "EU",
    "FX",
    "HY",
    "IG",
    "JP",
    "KR",
    "UK",
    "US",
})
_FUND_PRODUCT_VEHICLE_TOKENS = frozenset({
    "ETF",
    "ETFS",
    "EXCHANGE",
    "FD",
    "FDS",
    "FUND",
    "FUNDS",
    "INC",
    "LLC",
    "PORTFOLIO",
    "PORTFOLIOS",
    "SER",
    "SERIES",
    "TR",
    "TRADED",
    "TRUST",
})
_FUND_PRODUCT_ABBREVIATION_NOISE_TOKENS = frozenset({
    "ADM",
    "ADMIRAL",
    "AND",
    "INST",
    "INSTITUTIONAL",
    "INV",
    "INVESTOR",
    "OF",
    "THE",
})
_OPENFIGI_COMPACT_NAME_LIMIT = 28
_FUND_PRODUCT_NAME_GENERIC_TOKENS = (
    _FUND_PRODUCT_CLASS_GENERIC_TOKENS
    | frozenset({
        "ACTIVE",
        "I",
        "II",
        "III",
        "IV",
        "INDEX",
        "TRUST",
        "V",
        "VI",
    })
)
_OPENFIGI_TICKER_LIKE_TOKEN_RE = re.compile(
    r"^[A-Z0-9][A-Z0-9./*-]{0,31}$",
    re.IGNORECASE,
)
_OPENFIGI_STRUCTURED_TERMS_RE = re.compile(
    r"(?:\bPERP\b|\b(?:FLT|VAR)\b|\d)",
    re.IGNORECASE,
)
_OPENFIGI_US_EXCHANGE_CODES = frozenset({
    "US",
    "UN",
    "UA",
    "UB",
    "UC",
    "UD",
    "UM",
    "UW",
    "UQ",
    "UF",
    "UP",
    "UR",
    "UX",
    "NEW YORK",
    "NYSE ARCA",
    "NYSE AMERICAN",
    "BATS",
    "IEX",
    "OTC US",
})

# Verified historical exceptions that current live resolvers miss.
# These keep obvious non-obscure names from regressing to raw CUSIPs when a
# company is later acquired/delisted or absent from SEC's company_tickers file.
MANUAL_CUSIP_TICKER_OVERRIDES: dict[str, str] = {
    # DTCC OTC Important Notice 044 (2015-03-06):
    # https://www.dtcc.com/globals/pdfs/2015/march/06/otc-044
    "45669R701": "IACH",
    "G9001E110": "LILAK",
    "M2682V108": "CYBR",
}
MANUAL_CUSIP_NAME_OVERRIDES: dict[str, str] = {
    "45669R701": "INFORMATION ARCHITECTS CORP",
    # Official Schwab product page:
    # https://www.schwabassetmanagement.com/products/swgxx
    # Some retained 13F rows misstate the issuer as APPLE INC; keep that raw
    # dominant_issuer for provenance while using the CUSIP-specific fund name.
    "808515209": "SCHWAB GOVERNMENT MONEY FUND - SWEEP SHARES",
    # SEC 13F: IONQ INC, *W EXP 10/01/202, CUSIP 46222L116.
    # https://www.sec.gov/Archives/edgar/data/1819275/000108514623002289/xslForm13F_X02/infotable.xml
    "46222L116": "IONQ INC",
    # SEC prospectus: existing Innoviz warrants expire April 5, 2026.
    # https://www.sec.gov/Archives/edgar/data/1835654/000117891322002295/zk2227957.htm
    "M5R635116": "INNOVIZ TECHNOLOGIES LTD",
    # SEC 13F: https://www.sec.gov/Archives/edgar/data/2024532/000139834425009327/xslForm13F_X02/fp0093565-1_13fhr-table.xml
    "714920113": "PERSHING SQUARE SPARC HOLDINGS, LTD.",
    # SEC 13G: https://www.sec.gov/Archives/edgar/data/2025396/000110465924099307/tm2423867d1_sc13g.htm
    "G93Y09123": "VINE HILL CAPITAL INVESTMENT CORP.",
}
MANUAL_SECURITY_LABEL_OVERRIDES: dict[str, str] = {
    # These are malformed/synthetic identifiers preserved in historical
    # filings. There is no issuer identity to resolve, so state that explicitly
    # instead of displaying the raw identifier or placeholder filer text.
    "056517388": "UNIDENTIFIED EQUITY SECURITY",
    "056517389": "UNIDENTIFIED EQUITY SECURITY",
    "464287294": "UNIDENTIFIED PUT SECURITY",
    "MONEYMRKT": "MONEY MARKET FUND",
    "OOOOOOOOO": "UNIDENTIFIED EQUITY SECURITY",
}
MANUAL_VERIFIED_SECURITY_LABEL_OVERRIDES: dict[str, str] = {
    "46222L116": "IONQ/WS — WARRANT EXP 10/01/26",
    "M5R635116": "INVZW — WARRANT EXP 04/05/26",
    # SEC final term sheet: 4.125% Junior Subordinated Notes due 2060,
    # CUSIP 744320 888, listed on the NYSE under symbol PFH.
    # https://www.sec.gov/Archives/edgar/data/1137774/000119312520223799/d91345dfwp.htm
    "744320888": "PFH — 4.125% JUNIOR SUBORDINATED NOTES DUE 2060",
}
MANUAL_SECURITY_KIND_OVERRIDES: dict[str, str] = {
    "46222L116": "WARRANT",
    # SEC issuer materials identify these iPath products as exchange-traded
    # notes. OpenFIGI's broad ETP classification otherwise presents them as
    # ETFs.
    # https://www.sec.gov/Archives/edgar/data/312070/000119312512299581/d377519dfwp.htm
    "06738C786": "ETN",
    # https://www.sec.gov/Archives/edgar/data/312070/000119312512401196/d417380dfwp.htm
    "06740C527": "ETN",
    # https://www.sec.gov/Archives/edgar/data/312070/000095010325006107/dp228819_424b2-vxxvxzetnps.htm
    "06748M188": "ETN",
    # SEC 10-K: INDEXPLUS Trust Certificates Series 2003-1 represent
    # interests in a portfolio of underlying debt obligations.
    "590188108": "BOND",
    # SEC final term sheet: 4.125% Junior Subordinated Notes due 2060,
    # CUSIP 744320 888. Retained 13F rows and OpenFIGI misclassify the
    # exchange-listed debt as common/preferred equity.
    # https://www.sec.gov/Archives/edgar/data/1137774/000119312520223799/d91345dfwp.htm
    "744320888": "BOND",
    # SEC 8-K: WINVR is a right to acquire 1/15 of one common share.
    "97655B125": "RIGHT",
    # SEC 12(b) data: IMAQR is the issuer's listed acquisition right.
    "459867123": "RIGHT",
    # Repeated retained 13F rows identify these W-suffixed securities as
    # warrants even though a higher-value filer row mislabeled the class COM.
    "09032H113": "WARRANT",
    "128745114": "WARRANT",
    "74319X116": "WARRANT",
}


def _response_excerpt(resp: requests.Response, limit: int = 300) -> str:
    text = (resp.text or "").strip().replace("\n", " ")
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text or "<empty body>"


def manual_cusip_ticker_overrides(cusips: set[str] | list[str]) -> dict[str, str]:
    return {
        cusip: MANUAL_CUSIP_TICKER_OVERRIDES[cusip]
        for cusip in cusips
        if cusip in MANUAL_CUSIP_TICKER_OVERRIDES
    }


def get_openfigi_api_key() -> str:
    return os.environ.get("OPENFIGI_API_KEY", "").strip()


def openfigi_batch_size() -> int:
    return OPENFIGI_KEYED_BATCH if get_openfigi_api_key() else OPENFIGI_FREE_BATCH


def openfigi_target_interval() -> float:
    return OPENFIGI_KEYED_INTERVAL if get_openfigi_api_key() else OPENFIGI_FREE_INTERVAL


def _openfigi_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    api_key = get_openfigi_api_key()
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    return headers


def _parse_openfigi_rate_limit(resp: requests.Response) -> tuple[int | None, float | None]:
    remaining_text = resp.headers.get("ratelimit-remaining")
    reset_text = resp.headers.get("ratelimit-reset")
    try:
        remaining = int(remaining_text) if remaining_text is not None else None
    except (TypeError, ValueError):
        remaining = None
    try:
        reset = float(reset_text) if reset_text is not None else None
    except (TypeError, ValueError):
        reset = None
    return remaining, reset


def _openfigi_pause_seconds(
    resp: requests.Response | None,
    fallback: float,
) -> float:
    if resp is None:
        return fallback
    remaining, reset = _parse_openfigi_rate_limit(resp)
    if remaining is None or reset is None or reset <= 0:
        return fallback
    if remaining <= 1:
        return max(reset, fallback)
    return max(reset / remaining, 0.0)


def _openfigi_post(payload: list[dict]) -> requests.Response | None:
    """POST to OpenFIGI with bounded retry on rate limits and transient 5xx."""
    for attempt in range(5):
        try:
            resp = requests.post(
                OPENFIGI_URL, json=payload,
                headers=_openfigi_headers(),
                timeout=30,
            )
            if resp.status_code == 429:
                _, reset = _parse_openfigi_rate_limit(resp)
                wait = max(reset or 0, 30 * (attempt + 1))
                log.warning(f"    rate-limited; waiting {wait}s (attempt {attempt + 1}/5)")
                time.sleep(wait)
                continue
            if resp.status_code in {500, 502, 503, 504}:
                if attempt == 4:
                    log.warning(
                        f"    OpenFIGI HTTP {resp.status_code} after 5 attempts: "
                        f"{_response_excerpt(resp)}"
                    )
                    return resp
                wait = min(2 ** attempt, 16)
                log.warning(
                    f"    OpenFIGI HTTP {resp.status_code}; retrying in "
                    f"{wait}s (attempt {attempt + 1}/5)"
                )
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                log.warning(
                    f"    OpenFIGI HTTP {resp.status_code}: {_response_excerpt(resp)}"
                )
            return resp
        except Exception as e:
            log.warning(f"    request error: {e}")
            time.sleep(10)
    return None


def _openfigi_identifier_type(identifier: str) -> str:
    """Use CINS for letter-leading identifiers and CUSIP otherwise."""

    return "ID_CINS" if identifier[:1].isalpha() else "ID_CUSIP"


def _select_openfigi_candidate(candidates: list[dict]) -> dict | None:
    """Select one mapping result while preserving historical ticker behavior."""

    for candidate in candidates:
        if candidate.get("ticker") and _openfigi_is_us_exchange(candidate):
            return candidate
    for candidate in candidates:
        if candidate.get("ticker"):
            return candidate
    return candidates[0] if candidates else None


def _openfigi_detail(candidate: dict) -> dict:
    """Return the compact, normalized metadata retained for one match."""

    detail: dict[str, str | None] = {"status": "matched"}
    for field in _OPENFIGI_DETAIL_FIELDS:
        value = candidate.get(field)
        if isinstance(value, str):
            value = " ".join(value.strip().split()) or None
        else:
            value = None
        if field == "ticker" and value:
            value = value.upper()
        detail[field] = value
    return detail


def _openfigi_is_definitive_no_match(entry: dict) -> bool:
    """Whether one per-identifier response is safe to persist as no-match."""

    warning = entry.get("warning")
    if isinstance(warning, str) and warning.strip():
        return True
    error = str(entry.get("error") or "").strip().lower().rstrip(".")
    return error in {
        "invalid idvalue format",
        "no identifier found",
    }


class OpenFIGIFullRefreshError(RuntimeError):
    """Raised when a full OpenFIGI refresh cannot verify every batch."""


def _openfigi_batch_failure(
    message: str,
    *,
    strict: bool,
) -> None:
    """Log a batch failure, raising when the caller requires completeness."""

    if strict:
        raise OpenFIGIFullRefreshError(message)
    log.warning(message)


def resolve_cusips_via_openfigi(
    cusips: list[str],
    *,
    force_refresh: bool = False,
) -> dict[str, str]:
    """Batch-resolve CUSIPs to tickers via the OpenFIGI API.

    Returns the historical ``identifier -> ticker`` shape for compatibility,
    while separately persisting the selected candidate's descriptive and type
    metadata. Letter-leading identifiers are submitted as CINS; numeric-leading
    identifiers retain the CUSIP route. When OPENFIGI_API_KEY is present we use
    the keyed request size and pace more aggressively; otherwise we honor the
    lower public limits. Durable matched/no-match details short-circuit routine
    retries. ``force_refresh`` is reserved for the weekly full refresh and is
    therefore also strict: every requested identifier must receive a complete,
    structurally valid batch response or the refresh fails rather than silently
    succeeding with retained stale mappings. Routine resolution remains
    best-effort so a transient OpenFIGI outage cannot block daily SEC updates.
    """
    if not cusips:
        return {}
    with OPENFIGI_LOCK:
        requested = list(dict.fromkeys(
            str(cusip or "").strip().upper()
            for cusip in cusips
            if str(cusip or "").strip()
        ))
        details = load_openfigi_details()
        if not force_refresh:
            for cusip in requested:
                if cusip in _OPENFIGI_RUN_CACHE:
                    continue
                detail = details.get(cusip)
                if not isinstance(detail, dict):
                    continue
                ticker = detail.get("ticker")
                _OPENFIGI_RUN_CACHE[cusip] = (
                    str(ticker).strip().upper()
                    if detail.get("status") == "matched" and ticker
                    else None
                )
        result = {} if force_refresh else {
            cusip: ticker
            for cusip in requested
            if (
                cusip in _OPENFIGI_RUN_CACHE
                and (ticker := _OPENFIGI_RUN_CACHE[cusip]) is not None
            )
        }
        uncached = requested if force_refresh else [
            cusip for cusip in requested
            if cusip not in _OPENFIGI_RUN_CACHE
        ]
        if not uncached:
            return result

        batch_size = openfigi_batch_size()
        pause_seconds = openfigi_target_interval()
        total_batches = (len(uncached) + batch_size - 1) // batch_size
        mode = "with API key" if get_openfigi_api_key() else "without API key"
        est_minutes = int((total_batches * pause_seconds) // 60)
        log.info(
            f"  OpenFIGI: resolving {len(uncached)} CUSIPs in {total_batches} batches "
            f"(~{est_minutes} min, {batch_size}/request, {mode})"
        )
        for i in range(0, len(uncached), batch_size):
            batch_num = i // batch_size + 1
            if batch_num % 10 == 1 or batch_num == total_batches:
                log.info(f"    batch {batch_num}/{total_batches} ({len(result)} resolved so far)")
            batch = uncached[i : i + batch_size]
            payload = [
                {
                    "idType": _openfigi_identifier_type(identifier),
                    "idValue": identifier,
                }
                for identifier in batch
            ]
            resp = _openfigi_post(payload)
            if resp is None:
                _openfigi_batch_failure(
                    f"OpenFIGI batch {batch_num}/{total_batches} failed after "
                    "all request attempts: no response",
                    strict=force_refresh,
                )
            elif resp.status_code != 200:
                _openfigi_batch_failure(
                    f"OpenFIGI batch {batch_num}/{total_batches} failed with "
                    f"HTTP {resp.status_code}: {_response_excerpt(resp)}",
                    strict=force_refresh,
                )
            else:
                try:
                    data = resp.json()
                except ValueError:
                    _openfigi_batch_failure(
                        f"OpenFIGI batch {batch_num}/{total_batches} returned "
                        f"invalid JSON: {_response_excerpt(resp)}",
                        strict=force_refresh,
                    )
                    data = []
                if not isinstance(data, list):
                    _openfigi_batch_failure(
                        f"OpenFIGI batch {batch_num}/{total_batches} JSON was "
                        f"not a list ({type(data).__name__}): "
                        f"{_response_excerpt(resp)}",
                        strict=force_refresh,
                    )
                    data = []
                if len(data) != len(batch):
                    _openfigi_batch_failure(
                        f"OpenFIGI batch {batch_num}/{total_batches} returned "
                        f"{len(data)} result(s) for {len(batch)} identifier(s)",
                        strict=force_refresh,
                    )
                details_changed = False
                for cusip, entry in zip(batch, data):
                    if not isinstance(entry, dict):
                        _openfigi_batch_failure(
                            f"OpenFIGI batch {batch_num}/{total_batches} returned "
                            f"a non-object result for {cusip}",
                            strict=force_refresh,
                        )
                        continue
                    if force_refresh:
                        result_keys = {
                            key for key in ("data", "error", "warning")
                            if key in entry
                        }
                        if len(result_keys) != 1:
                            _openfigi_batch_failure(
                                f"OpenFIGI batch {batch_num}/{total_batches} "
                                f"returned a non-exclusive result shape for "
                                f"{cusip}: {sorted(result_keys)!r}",
                                strict=True,
                            )
                        result_key = next(iter(result_keys))
                        # These two recognized per-identifier negatives are
                        # complete answers, not batch/provider failures. SEC
                        # filings can contain placeholder or malformed CUSIPs,
                        # so persist them as no-match while keeping every
                        # unknown error fatal in full-refresh mode.
                        if (
                            result_key == "error"
                            and not _openfigi_is_definitive_no_match(entry)
                        ):
                            _openfigi_batch_failure(
                                f"OpenFIGI batch {batch_num}/{total_batches} "
                                f"returned an error result for {cusip}: "
                                f"{entry.get('error')!r}",
                                strict=True,
                            )
                        if result_key == "warning" and not (
                            isinstance(entry.get("warning"), str)
                            and entry["warning"].strip()
                        ):
                            _openfigi_batch_failure(
                                f"OpenFIGI batch {batch_num}/{total_batches} "
                                f"returned an empty warning for {cusip}",
                                strict=True,
                            )
                    if "data" not in entry:
                        if _openfigi_is_definitive_no_match(entry):
                            _OPENFIGI_RUN_CACHE[cusip] = None
                            no_match = {"status": "no_match"}
                            if details.get(cusip) != no_match:
                                details[cusip] = no_match
                                details_changed = True
                        else:
                            _openfigi_batch_failure(
                                f"OpenFIGI batch {batch_num}/{total_batches} "
                                f"returned an incomplete result for {cusip}: "
                                f"{entry!r}",
                                strict=force_refresh,
                            )
                        continue
                    inner = entry.get("data")
                    if not isinstance(inner, list) or any(
                        not isinstance(item, dict) for item in inner
                    ):
                        _openfigi_batch_failure(
                            f"OpenFIGI batch {batch_num}/{total_batches} returned "
                            f"malformed mapping data for {cusip}",
                            strict=force_refresh,
                        )
                        continue
                    if not inner:
                        if force_refresh:
                            _openfigi_batch_failure(
                                f"OpenFIGI batch {batch_num}/{total_batches} "
                                f"returned empty mapping data for {cusip}; "
                                "a no-match must use the warning response shape",
                                strict=True,
                            )
                        _OPENFIGI_RUN_CACHE[cusip] = None
                        no_match = {"status": "no_match"}
                        if details.get(cusip) != no_match:
                            details[cusip] = no_match
                            details_changed = True
                        continue
                    if force_refresh and any(
                        not isinstance(item.get("figi"), str)
                        or not item["figi"].strip()
                        for item in inner
                    ):
                        _openfigi_batch_failure(
                            f"OpenFIGI batch {batch_num}/{total_batches} "
                            f"returned a mapping result without a non-empty "
                            f"FIGI for {cusip}",
                            strict=True,
                        )

                    candidate = _select_openfigi_candidate(inner)
                    if candidate is None:
                        continue
                    detail = _openfigi_detail(candidate)
                    if details.get(cusip) != detail:
                        details[cusip] = detail
                        details_changed = True

                    ticker = detail.get("ticker")
                    if isinstance(ticker, str) and ticker:
                        result[cusip] = ticker
                        _OPENFIGI_RUN_CACHE[cusip] = ticker
                    else:
                        _OPENFIGI_RUN_CACHE[cusip] = None
                if details_changed:
                    save_openfigi_details(details)
            if batch_num < total_batches:
                time.sleep(_openfigi_pause_seconds(resp, pause_seconds))
        log.info(f"  OpenFIGI resolved {len(result)}/{len(requested)} CUSIPs")
        return result


def update_cusip_map(
    cusip_map: dict[str, str],
    holdings: list[dict],
) -> None:
    """Assign tickers to holdings using only CUSIP-based data.

    We deliberately avoid issuer-name heuristics here. CUSIP is the canonical
    security identity; tickers are just display metadata derived from the
    mapping cache or OpenFIGI."""
    missing_cusips = sorted({
        str(h.get("cusip") or "").strip().upper()
        for h in holdings
        if h.get("cusip") and str(h["cusip"]).strip().upper() not in cusip_map
    })
    if missing_cusips:
        cusip_map.update(manual_cusip_ticker_overrides(missing_cusips))
        unresolved_missing = [cusip for cusip in missing_cusips if cusip not in cusip_map]
        if unresolved_missing:
            cusip_map.update(resolve_cusips_via_openfigi(unresolved_missing))

    for h in holdings:
        cusip = str(h.get("cusip") or "").strip().upper()
        if cusip and cusip in cusip_map:
            h["ticker"] = display_ticker_for_holding_type(
                cusip_map[cusip],
                classify_saved_holding(h),
            )

    suspect_cusips = sorted(find_ambiguous_ticker_cusips(holdings))
    if suspect_cusips:
        refreshed = manual_cusip_ticker_overrides(suspect_cusips)
        unresolved_suspects = [cusip for cusip in suspect_cusips if cusip not in refreshed]
        if unresolved_suspects:
            refreshed.update(resolve_cusips_via_openfigi(unresolved_suspects))
        if refreshed:
            cusip_map.update(refreshed)
            for h in holdings:
                cusip = str(h.get("cusip") or "").strip().upper()
                if cusip in refreshed:
                    h["ticker"] = display_ticker_for_holding_type(
                        refreshed[cusip],
                        classify_saved_holding(h),
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
_TICKER_COLLISION_DOMINANCE_RATIO = 10.0


def _detect_ticker_collisions() -> tuple[list[tuple], list[tuple]]:
    """Scan every equity holding and find tickers claimed by multiple CUSIPs.

    Returns:
      demote: list of (ticker, kept_cusip, kept_value, losing_cusip, losing_value)
              — each losing CUSIP should have its ticker nulled.
      ambiguous: list of (ticker, [cusip,...]) — no CUSIP dominates the
                 others by >= _TICKER_COLLISION_DOMINANCE_RATIO; left alone.
    """
    claims: dict[str, dict[str, dict]] = defaultdict(dict)
    for fp in sorted(FUNDS_DIR.glob("*.json")):
        try:
            with open(fp) as f:
                fund = json.load(f)
        except json.JSONDecodeError:
            continue
        cik = fund.get("cik")
        for q in fund.get("quarters", []):
            for h in q.get("holdings", []):
                cusip = str(h.get("cusip") or "").strip().upper()
                ticker = str(h.get("ticker") or "").strip().upper()
                htype = classify_saved_holding(h)
                if not (cusip and ticker) or htype != "EQUITY":
                    continue
                rec = claims[ticker].setdefault(cusip, {"value": 0, "ciks": set()})
                rec["value"] += int(h.get("value", 0) or 0)
                if cik is not None:
                    rec["ciks"].add(cik)

    demote: list[tuple] = []
    ambiguous: list[tuple] = []
    for ticker, by_cusip in claims.items():
        if len(by_cusip) < 2:
            continue
        ranked = sorted(
            by_cusip.items(),
            key=lambda item: (item[1]["value"], len(item[1]["ciks"])),
            reverse=True,
        )
        top_cusip, top_rec = ranked[0]
        _runner_cusip, runner_rec = ranked[1]
        top_v = top_rec["value"]
        runner_v = runner_rec["value"]
        if runner_v == 0 or top_v / max(runner_v, 1) >= _TICKER_COLLISION_DOMINANCE_RATIO:
            for losing_cusip, losing_rec in ranked[1:]:
                demote.append((
                    ticker, top_cusip, top_v, losing_cusip, losing_rec["value"],
                ))
        else:
            ambiguous.append((ticker, [c for c, _ in ranked]))

    return demote, ambiguous


def apply_ticker_collision_fixes(
    cusip_map: dict[str, str],
    *,
    protected_tickers: set[str] | None = None,
) -> int:
    """Prune losing entries from cusip_map when multiple CUSIPs claim a ticker.

    For each ticker where a dominant CUSIP exists (>= 10x value of next),
    drop the losing CUSIPs from cusip_map so the registry build doesn't
    inherit the bad mapping. Ambiguous cases (no clear dominance) are
    logged and left alone so legitimate multi-CUSIP scenarios (reverse
    splits, dual listings, leveraged-ETF share-class history) don't get
    silently collapsed.

    Phase 3: fund-file mutation is NO LONGER done here — canonicalize_fund_files()
    runs after build_cusip_registry and rewrites ticker/issuer/holding_type
    on every holding from the (now-clean) registry. This function's sole
    remaining job is keeping cusip_map honest so the registry stays
    honest.

    Returns the number of losing claims pruned from cusip_map."""
    demote, ambiguous = _detect_ticker_collisions()
    protected = {
        str(ticker).strip().upper()
        for ticker in (protected_tickers or set())
        if str(ticker).strip()
    }
    protected_demotions = [
        row for row in demote if str(row[0]).strip().upper() in protected
    ]
    demote = [
        row for row in demote if str(row[0]).strip().upper() not in protected
    ]
    if protected_demotions:
        log.info(
            "  retained %s collision claim(s) across %s SEC-proven "
            "historical ticker alias(es)",
            len(protected_demotions),
            len({row[0] for row in protected_demotions}),
        )

    for ticker, cusips in ambiguous:
        log.warning(
            f"  ambiguous equity ticker {ticker!r} claimed by "
            f"{len(cusips)} CUSIPs without clear dominance: {', '.join(cusips)}"
        )

    if not demote:
        log.info("  no ticker/CUSIP collisions to resolve")
        return 0

    for ticker, kept_cusip, kept_v, losing_cusip, losing_v in sorted(
        demote, key=lambda d: d[4], reverse=True
    )[:10]:
        log.info(
            f"  demote {ticker}: keep {kept_cusip} (${kept_v:,}) "
            f"drop {losing_cusip} (${losing_v:,})"
        )
    if len(demote) > 10:
        log.info(f"  ... and {len(demote) - 10} more demotions")

    losing_cusips = {d[3] for d in demote}
    removed_from_map = 0
    for losing_cusip in losing_cusips:
        if losing_cusip in cusip_map:
            del cusip_map[losing_cusip]
            removed_from_map += 1

    log.info(
        f"  removed {removed_from_map} losing CUSIPs from cusip_map "
        f"({len(demote)} spurious ticker claims across {len(losing_cusips)} CUSIPs)"
    )
    return removed_from_map


# Stripped before tokenizing issuer names — drops corporate-entity noise so
# "WALT DISNEY CO/THE" and "DISNEY WALT CO" don't hinge on whether "CO" or
# "THE" happen to match.
_ISSUER_STOPWORDS = frozenset({
    "CO", "COMPANY", "CORP", "CORPORATION", "INC", "INCORPORATED", "THE",
    "LTD", "LIMITED", "LLC", "LP", "PLC", "SA", "AG", "NA", "NV", "AB",
    "CLASS", "CL", "COM", "COMMON", "STOCK", "ADR", "SHARES", "SHS", "NEW",
    "OLD", "ORD", "ORDINARY", "HOLDINGS", "HLDGS", "HLDG", "GROUP", "GRP",
    "TR", "TRUST", "FUND", "ETF", "INDEX",
})


def _issuer_tokens(s: str) -> set[str]:
    """Significant words from an issuer name — drops punctuation, stopwords,
    and 1-char fragments so word-reorder / abbreviation differences don't
    register as mismatches."""
    tokens = re.split(r"[^A-Z0-9]+", (s or "").upper())
    return {t for t in tokens if len(t) >= 2 and t not in _ISSUER_STOPWORDS}


def _issuer_alnum(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


def _issuers_likely_same(a: str, b: str) -> bool:
    """Loose same-company check. Returns True for:
      - word-reorders ("ELI LILLY" vs "LILLY ELI")
      - abbreviated variants that still share a significant token
      - typos / fragments that share a >=5-char substring
    and False when the significant tokens are disjoint and no long
    substring overlaps (TESLA vs AIRBNB, APPLE vs APOGEE)."""
    ta = _issuer_tokens(a)
    tb = _issuer_tokens(b)
    if ta and tb and ta & tb:
        return True
    # Fall back to substring: catches "TESLA" vs "1TESLA", "ESLA INC" vs
    # "TESLA INC", or "TELSA MOTORS" vs "TESLA INC" (shared "ESLA"/"TESL").
    # 5-char minimum is tight enough that TESLA vs AIRBNB still fails.
    a_clean = _issuer_alnum(a)
    b_clean = _issuer_alnum(b)
    if len(a_clean) < 5 or len(b_clean) < 5:
        return False
    short, long = (a_clean, b_clean) if len(a_clean) <= len(b_clean) else (b_clean, a_clean)
    for i in range(len(short) - 4):
        if short[i : i + 5] in long:
            return True
    return False


@_serialize_pipeline_maintenance
def rebuild_tickers_in_place(
    *,
    full_refresh: bool = False,
    company_ticker_data: dict | list | None = None,
) -> int:
    """Refresh CUSIP->ticker mappings across stored fund files.

    In normal mode this is an incremental repair pass: it re-resolves missing
    CUSIPs plus CUSIPs that collide under the same ticker/type within a quarter,
    then rewrites every fund file with the refreshed cache.

    In full-refresh mode it prunes the persisted map down to CUSIPs that still
    appear in current fund files, re-resolves *all* current CUSIPs via OpenFIGI,
    and then rewrites every fund file with the refreshed/pruned cache. Any
    current CUSIP that OpenFIGI does not resolve keeps its existing per-holding
    ticker fallback, but stale map entries for no-longer-present CUSIPs are
    removed.

    Returns the number of fund files that actually changed."""
    if full_refresh:
        log.info("Fully refreshing the CUSIP map across all fund files...")
    else:
        log.info("Re-resolving tickers across all fund files...")

    if not FUNDS_DIR.exists():
        log.info("  no funds directory; nothing to rebuild")
        return 0

    old_cusip_map: dict[str, str] = load_cusip_map()
    log.info(f"  loaded {len(old_cusip_map)} entries from the private CUSIP cache")
    if company_ticker_data is None:
        company_ticker_data = _load_company_tickers_data()
    sec_titles, _name_to_ticker = _company_ticker_indexes(
        company_ticker_data
    )
    prior_registry = load_cusip_registry()
    prior_aliases = _proven_registry_ticker_aliases(
        prior_registry,
        sec_titles,
    )
    prior_openfigi_details = load_openfigi_details()
    missing_cusips: set[str] = set()
    suspect_cusips: set[str] = set()
    current_cusips: set[str] = set()
    seed_candidates: dict[str, dict[str, tuple[int, int, str]]] = defaultdict(dict)
    synthetic_cusips: set[str] = set()

    fund_paths = sorted(FUNDS_DIR.glob("*.json"))
    total = len(fund_paths)
    log.info(f"  scanning {total} fund files for missing/suspect CUSIPs...")
    for idx, fp in enumerate(fund_paths):
        if idx % 2000 == 0 and idx > 0:
            log.info(
                f"    scan progress: {idx}/{total} files "
                f"({len(missing_cusips)} missing, {len(suspect_cusips)} suspect)"
            )
        try:
            with open(fp) as f:
                fund = json.load(f)
        except json.JSONDecodeError:
            continue
        for q in fund.get("quarters", []):
            holdings = q.get("holdings", [])
            rep_date = q.get("report_date") or ""
            suspect_cusips.update(find_ambiguous_ticker_cusips(holdings))
            for h in holdings:
                cusip = h.get("cusip")
                if not cusip:
                    continue
                current_cusips.add(cusip)
                if is_synthetic_identifier(cusip):
                    synthetic_cusips.add(cusip)
                    continue
                ticker = str(h.get("ticker") or "").strip().upper()
                if ticker and ticker != cusip:
                    score = seed_candidates[cusip].get(ticker, (0, 0, ""))
                    seed_candidates[cusip][ticker] = (
                        score[0] + 1,
                        score[1] + int(h.get("value", 0) or 0),
                        max(score[2], rep_date),
                    )
                if full_refresh or cusip not in old_cusip_map:
                    missing_cusips.add(cusip)

    if full_refresh:
        stale_removed = len(set(old_cusip_map) - current_cusips)
        retained_current = {
            cusip: ticker
            for cusip, ticker in old_cusip_map.items()
            if cusip in current_cusips
        }
        cusip_map: dict[str, str] = retained_current
        log.info(
            f"  pruned {stale_removed} stale cached mappings; "
            f"retained {len(cusip_map)} current entries before OpenFIGI refresh"
        )
    else:
        cusip_map = dict(old_cusip_map)

    active_aliases = {
        cusip: alias
        for cusip, alias in prior_aliases.items()
        if cusip in current_cusips
    }
    for cusip, (source_ticker, _canonical_ticker) in active_aliases.items():
        # Keep the mechanically provable source symbol in the private map.
        # The snapshot registry canonicalizes it after this repair pass.
        cusip_map[cusip] = source_ticker
    suspect_cusips.difference_update(active_aliases)
    if active_aliases:
        log.info(
            "  retained %s SEC-proven historical ticker alias source(s)",
            len(active_aliases),
        )

    # Claim set of tickers already mapped to a canonical CUSIP (from the
    # cached map or a retained full-refresh entry). Used to reject seed
    # candidates whose ticker is already taken — the most common cause of
    # duplicates is a filer that typo'd the CUSIP but entered the correct
    # ticker, which would otherwise pollute the map with a second CUSIP
    # claiming the same ticker.
    claimed_tickers: set[str] = {
        t.upper() for t in cusip_map.values() if t
    }
    seeded_from_holdings: dict[str, str] = {}
    skipped_collisions = 0
    for cusip, ticker_scores in seed_candidates.items():
        if cusip in cusip_map or not ticker_scores:
            continue
        best_ticker = max(
            ticker_scores.items(),
            key=lambda item: (item[1][0], item[1][1], item[1][2], item[0]),
        )[0]
        if best_ticker.upper() in claimed_tickers:
            skipped_collisions += 1
            continue
        seeded_from_holdings[cusip] = best_ticker
        claimed_tickers.add(best_ticker.upper())
    if seeded_from_holdings:
        cusip_map.update(seeded_from_holdings)
        log.info(
            f"  seeded {len(seeded_from_holdings)} CUSIP mappings from stored fund holdings"
        )
    if skipped_collisions:
        log.info(
            f"  skipped {skipped_collisions} seed candidates whose ticker was "
            "already claimed by another CUSIP (likely filer CUSIP typos)"
        )

    manual_overrides = manual_cusip_ticker_overrides(current_cusips)
    if manual_overrides:
        cusip_map.update(manual_overrides)
        log.info(f"  applied {len(manual_overrides)} manual CUSIP ticker overrides")
    if synthetic_cusips:
        log.info(
            f"  detected {len(synthetic_cusips)} synthetic identifiers; "
            "skipping OpenFIGI / seeded ticker resolution for them"
        )

    # Public OpenFIGI mode has lower batch/rate limits but is fully supported.
    # The resolver logs its keyed/unkeyed mode and estimated duration.
    resolve_missing = True

    manual_override_cusips = set(manual_overrides)
    kind_enrichment_cusips = {
        cusip
        for cusip in current_cusips
        if (
            cusip not in synthetic_cusips
            and cusip not in manual_override_cusips
            and not (
                isinstance(prior_openfigi_details.get(cusip), dict)
                and prior_openfigi_details[cusip].get("status") == "matched"
            )
            and _FUND_KIND_DISCOVERY_RE.search(
                " ".join(
                    str((prior_registry.get(cusip) or {}).get(field) or "")
                    for field in (
                        "name",
                        "dominant_issuer",
                        "dominant_class",
                    )
                )
            )
        )
    }
    if full_refresh:
        to_resolve = sorted(
            cusip
            for cusip in (current_cusips if resolve_missing else set())
            if cusip not in manual_override_cusips
            and cusip not in synthetic_cusips
            and cusip not in active_aliases
        )
    else:
        to_resolve = sorted(
            (suspect_cusips - manual_override_cusips)
            | kind_enrichment_cusips
            | {
                cusip
                for cusip in missing_cusips
                if resolve_missing
                and cusip not in cusip_map
                and cusip not in synthetic_cusips
            }
        )
    if to_resolve:
        if full_refresh:
            log.info(
                f"  fully refreshing {len(to_resolve)} current CUSIPs via OpenFIGI"
            )
        else:
            log.info(
                f"  resolving {len(to_resolve)} CUSIPs via OpenFIGI "
                f"({len(missing_cusips) if resolve_missing else 0} missing, "
                f"{len(suspect_cusips)} suspect, "
                f"{len(kind_enrichment_cusips)} fund-kind enrichment)"
            )
        figi_map = resolve_cusips_via_openfigi(
            to_resolve,
            force_refresh=full_refresh,
        )
        cusip_map.update(figi_map)
        unresolved = len(to_resolve) - len(figi_map)
        if unresolved:
            log.info(f"  {unresolved} CUSIPs still unresolved after OpenFIGI")
    else:
        log.info("  no CUSIPs need OpenFIGI refresh")

    # Prune losing-CUSIP entries from cusip_map when multiple CUSIPs claim a
    # ticker (filer typos). Runs AFTER OpenFIGI so dominant-vs-losing CUSIPs
    # can be judged from the final map + fund-file values. Phase 3:
    # fund-file mutation for mismatches is handled by canonicalize_fund_files,
    # which rewrites ticker/issuer from the registry after build_cusip_registry.
    apply_ticker_collision_fixes(
        cusip_map,
        protected_tickers={
            canonical_ticker
            for _source_ticker, canonical_ticker in active_aliases.values()
        },
    )

    log.info(f"  writing {len(cusip_map)} ticker mappings to {total} fund files...")
    updated = 0
    reassigned = 0
    for idx, fp in enumerate(fund_paths):
        if idx % 2000 == 0 and idx > 0:
            log.info(f"    write progress: {idx}/{total} files ({updated} changed, {reassigned} holdings reassigned)")
        try:
            with open(fp) as f:
                fund = json.load(f)
        except json.JSONDecodeError:
            continue
        changed = False
        for q in fund.get("quarters", []):
            for h in q.get("holdings", []):
                cusip = h.get("cusip")
                if not cusip:
                    continue
                new_ht = _canonical_holding_type_for_quarter(q, h)
                new_ticker = display_ticker_for_holding_type(
                    cusip_map.get(cusip, h.get("ticker")),
                    new_ht,
                )
                if h.get("ticker") != new_ticker:
                    h["ticker"] = new_ticker
                    changed = True
                    reassigned += 1
                # Classify holding type
                if h.get("holding_type") != new_ht:
                    h["holding_type"] = new_ht
                    changed = True
                if "option_type" in h:
                    del h["option_type"]
                    changed = True
        if changed:
            _atomic_write_json(fp, fund)
            updated += 1

    # Replace the persisted cusip_map with the freshly-built one.
    save_cusip_map(cusip_map)
    if full_refresh:
        unresolved_current = len(current_cusips - set(cusip_map))
        log.info(
            f"  updated {updated}/{total} fund files "
            f"({reassigned} individual holding ticker changes); "
            f"cusip_map now has {len(cusip_map)} current entries with "
            f"{unresolved_current} current CUSIPs still unmapped"
        )
    else:
        log.info(
            f"  updated {updated}/{total} fund files "
            f"({reassigned} individual holding ticker changes); "
            f"cusip_map now has {len(cusip_map)} entries"
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
            if previous.get("shares_imputed") or current.get("shares_imputed"):
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


def published_holding_instrument_type(
    holding: dict,
    registry_entry: dict | None = None,
) -> str:
    """Return the canonical instrument type used by public stock artifacts.

    Fund files preserve the parser's original filing evidence. A later,
    stronger structural classification may prove that a row saved as an
    option is actually debt, or that a non-option NOTE/PREF/WARRANT parse is
    really a listed fund share. Explicit fund options remain separate.
    """
    raw_type = holding_instrument_type(holding)
    if (
        isinstance(registry_entry, dict)
        and normalize_security_kind(registry_entry.get("security_kind"))
        == "BOND"
        and normalize_instrument_type(registry_entry.get("type")) == "NOTE"
    ):
        return "NOTE"
    if (
        raw_type not in {"CALL", "PUT", "OPT"}
        and isinstance(registry_entry, dict)
        and normalize_instrument_type(registry_entry.get("type")) == "EQUITY"
        and _registry_entry_has_equity_fund_identity(registry_entry)
    ):
        return "EQUITY"
    return raw_type


@_serialize_pipeline_maintenance
def regenerate_stock_files_and_index(*, state: dict | None = None) -> None:
    """Rebuild stock files, the full search index, and the fund bootstrap.

    Phase 2 of the CUSIP-as-source-of-truth refactor: the displayed
    ticker and issuer name come from the CUSIP registry when available.
    Filer-typed `ticker` and `issuer` fields on individual holdings are
    a fallback only, used when a CUSIP somehow isn't in the registry
    (shouldn't happen since Phase 1 builds a registry entry for every
    fund-file CUSIP). The per-holding `holding_type` still drives the
    stock_id suffix so one CUSIP can host both equity and option holdings on
    separate stock files. Registry-confirmed bonds publish under NOTE, while
    registry-confirmed listed funds collapse every non-option parser bucket to
    EQUITY. The raw filing evidence remains unchanged.
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
        log.info("  no CUSIP registry found; falling back to per-holding filer strings")

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
                ticker_raw = h.get("ticker")
                issuer_raw = h.get("issuer") or ""
                cusip = str(h.get("cusip") or "").strip().upper()
                stock_key = cusip or str(ticker_raw or "").strip().upper()
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
                    registry_ticker = display_ticker_for_holding_type(
                        reg_entry.get("ticker"),
                        holding_type,
                    )
                    display_ticker = registry_ticker or cusip or stock_key
                    display_issuer = reg_entry.get("name") or issuer_raw or cusip
                else:
                    registry_fallback_count += 1
                    registry_ticker = None
                    display_ticker = (
                        display_ticker_for_holding_type(
                            ticker_raw,
                            holding_type,
                        )
                        or cusip
                        or stock_key
                    )
                    display_issuer = issuer_raw or cusip

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
            f"  {registry_fallback_count} holdings fell back to filer fields "
            "(CUSIP not in registry); registry build may have been skipped"
        )


# ----------------------------------------------------------------------------
# Ticker health report
# ----------------------------------------------------------------------------

# Match OpenFIGI results that look like debt/preferred/warrant symbols rather
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


def write_ticker_health_report() -> dict:
    """Scan every fund file and emit data/ticker_health.json.

    Ticker health and display-label coverage are deliberately separate:
    non-traded notes and pools may have no canonical ticker while still having
    a useful human label. Ticker buckets drive resolver retries; label coverage
    is the release-facing guarantee that the UI never needs a raw CUSIP as its
    primary security name.
    """
    log.info("Writing ticker health report...")
    if not FUNDS_DIR.exists():
        log.info("  no funds directory; skipping health report")
        return {}

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
                cusip = str(h.get("cusip") or "").strip().upper()
                if not cusip:
                    continue
                ticker = str(h.get("ticker") or "").strip().upper() or None
                value = int(h.get("value", 0) or 0)
                rec = records.setdefault(cusip, {
                    "cusip": cusip,
                    "ticker": ticker,
                    "issuer": h.get("issuer") or "",
                    "holder_ciks": set(),
                    "ticker_variants": set(),
                    "type_value": Counter(),
                    "type_count": Counter(),
                    "max_value": 0,
                    "first_seen": rep_date,
                    "last_seen": rep_date,
                })
                instrument_type = classify_saved_holding(h)
                rec["type_value"][instrument_type] += value
                rec["type_count"][instrument_type] += 1
                if cik is not None:
                    rec["holder_ciks"].add(cik)
                if ticker:
                    rec["ticker_variants"].add(ticker)
                if value > rec["max_value"]:
                    rec["max_value"] = value
                    rec["issuer"] = h.get("issuer") or rec["issuer"]
                    rec["ticker"] = ticker
                if rep_date:
                    if not rec["first_seen"] or rep_date < rec["first_seen"]:
                        rec["first_seen"] = rep_date
                    if rep_date > (rec["last_seen"] or ""):
                        rec["last_seen"] = rep_date

    registry = load_cusip_registry()
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
    sec_titles, _name_to_ticker = _company_ticker_indexes(
        company_ticker_data
    )
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
            display_ticker_for_holding_type(
                registry_entry.get("ticker"),
                instrument_type,
            )
            if registry_entry
            else display_ticker_for_holding_type(
                rec["ticker"],
                instrument_type,
            )
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
        if (
            bucket == "suspicious_symbol"
            and ticker
            and normalize_name(sec_titles.get(ticker) or "")
            and normalize_name(sec_titles.get(ticker) or "")
            == normalize_name(
                registry_entry.get("name")
                or registry_entry.get("dominant_issuer")
                or ""
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
                cusip = str(h.get("cusip") or "").strip().upper()
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
                issuer = str(h.get("issuer") or "").strip().upper()
                if issuer:
                    rec["issuer_value"][issuer] += value
                cls = str(h.get("class") or "").strip().upper()
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


def _company_ticker_indexes(
    data: dict | list | None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Return SEC title and normalized-name indexes from one raw payload."""
    if not data:
        return {}, {}

    entries = data.values() if isinstance(data, dict) else data
    buckets: dict[str, set[str]] = {}
    for entry in entries:
        ticker = (entry.get("ticker") or "").upper()
        title = (entry.get("title") or "").strip()
        if not (ticker and title):
            continue
        norm = normalize_name(title)
        if norm:
            buckets.setdefault(norm, set()).add(ticker)

    name_to_ticker: dict[str, str] = {}
    for norm, tickers in buckets.items():
        if len(tickers) == 1:
            name_to_ticker[norm] = next(iter(tickers))
    return sec_ticker_titles(data), name_to_ticker


_LEGACY_TICKER_CURRENCY_SUFFIX_RE = re.compile(
    r"^([A-Z0-9][A-Z0-9.-]*?)[0-9]?(?:EUR|GBP|GBX|USD|CHF|CAD|JPY)$"
)


def _validated_sec_ticker_alias(
    source_ticker: str | None,
    dominant_issuer: str | None,
    sec_titles: dict[str, str],
) -> str | None:
    """Return a narrowly normalized legacy ticker when SEC proves the alias.

    This deliberately does not resolve a ticker from the issuer name.  It only
    makes one of two mechanical edits to the existing ticker:

    * replace a slash with the SEC share-class separator (``BF/B`` -> ``BF-B``)
    * remove an explicit currency suffix and at most one preceding digit

    The edited candidate must be present in SEC's ticker list and its SEC title
    must normalize exactly to the filing corpus' dominant issuer.  Any
    ambiguity therefore fails closed and leaves the legacy ticker untouched.
    """
    raw_ticker = str(source_ticker or "").strip().upper()
    issuer_norm = normalize_name(dominant_issuer or "")
    if not raw_ticker or not issuer_norm:
        return None
    if raw_ticker in sec_titles:
        return None

    candidates: list[str] = []
    if "/" in raw_ticker:
        candidates.append(raw_ticker.replace("/", "-"))

    suffix_match = _LEGACY_TICKER_CURRENCY_SUFFIX_RE.fullmatch(raw_ticker)
    if suffix_match:
        candidates.append(suffix_match.group(1))

    for candidate in dict.fromkeys(candidates):
        sec_title = sec_titles.get(candidate)
        if sec_title and normalize_name(sec_title) == issuer_norm:
            return candidate
    return None


def _proven_registry_ticker_aliases(
    registry: dict[str, dict],
    sec_titles: dict[str, str],
) -> dict[str, tuple[str, str]]:
    """Return CUSIP -> (source ticker, canonical ticker) proof.

    Existing marker-backed aliases remain stable across repeated rebuilds.
    Before the first normalized rebuild, the current registry ticker itself
    can supply the same mechanical proof.
    """
    aliases: dict[str, tuple[str, str]] = {}
    for cusip, entry in registry.items():
        if not isinstance(entry, dict) or entry.get("type") != "EQUITY":
            continue
        source_ticker = (
            entry.get("source_ticker")
            if "sec_validated_ticker_alias" in set(entry.get("sources") or [])
            else entry.get("ticker")
        )
        canonical = _validated_sec_ticker_alias(
            source_ticker,
            entry.get("dominant_issuer"),
            sec_titles,
        )
        normalized_source = str(source_ticker or "").strip().upper()
        if canonical and normalized_source:
            aliases[str(cusip).strip().upper()] = (
                normalized_source,
                canonical,
            )
    return aliases


def _registry_type_from_evidence(
    rec: dict,
    openfigi_detail: dict | None = None,
    *,
    identifier: str | None = None,
    prior_entry: dict | None = None,
    filer_kind: str | None = None,
    filer_fund_identity: bool = False,
) -> str:
    """Infer a CUSIP's security type from its aggregated class/put_call.

    Options often reuse the underlying equity's CUSIP and distinguish
    themselves solely via the putCall field.  Any non-option position is
    therefore direct evidence for the CUSIP's canonical security identity;
    option notional must not turn that underlying identity into CALL or PUT.
    Dedicated option CUSIPs, which have no non-option evidence, retain their
    option identity so their per-holding stock files remain separate.

    Exact CUSIP-level BOND evidence is canonical instrument evidence, not just
    a display label. It therefore normalizes legacy EQUITY/PREF/option parser
    buckets to NOTE. Confirmed listed-fund evidence normalizes only non-option
    parser buckets to EQUITY. Trusted prior evidence is retained on cold-cache
    rebuilds so canonical identity cannot drift with a private cache."""
    current_kind = _openfigi_security_kind(openfigi_detail)
    manual_kind = normalize_security_kind(
        MANUAL_SECURITY_KIND_OVERRIDES.get(
            normalize_security_identifier(identifier)
        )
    )
    prior_kind = normalize_security_kind(
        (prior_entry or {}).get("security_kind")
    )
    prior_source = str(
        (prior_entry or {}).get("security_kind_source") or ""
    ).strip()
    normalized_filer_kind = normalize_security_kind(filer_kind)
    trusted_prior_kind = (
        current_kind is None
        and prior_kind is not None
        and (
            prior_source.startswith("openfigi")
            or prior_source == "manual_verified"
        )
    )
    trusted_prior_bond = (
        trusted_prior_kind and prior_kind == "BOND"
    )
    prior_untyped_fund_identity = (
        prior_kind is None
        and _registry_entry_has_equity_fund_identity(prior_entry)
    )
    generic_fund_identity = (
        filer_fund_identity or prior_untyped_fund_identity
    )
    trusted_prior_conflicting_kind = (
        prior_kind
        if (
            prior_source.startswith("openfigi")
            or prior_source == "manual_verified"
        )
        else None
    )
    generic_fund_identity_is_compatible = all(
        kind in {None, "COMMON", *_EQUITY_FUND_SECURITY_KINDS}
        for kind in (
            manual_kind,
            current_kind,
            normalized_filer_kind,
            trusted_prior_conflicting_kind,
        )
    )
    if (
        current_kind == "BOND"
        or manual_kind == "BOND"
        or trusted_prior_bond
    ):
        return "NOTE"
    confirmed_fund_kind = None
    if manual_kind in _EQUITY_FUND_SECURITY_KINDS:
        confirmed_fund_kind = manual_kind
    elif (
        current_kind in _EQUITY_FUND_SECURITY_KINDS
        and manual_kind != "ETN"
        and normalized_filer_kind != "ETN"
    ):
        confirmed_fund_kind = current_kind
    elif (
        current_kind in {None, "COMMON"}
        and normalized_filer_kind in {"ETF", "MUTUAL FUND"}
    ):
        confirmed_fund_kind = normalized_filer_kind
    elif (
        trusted_prior_kind
        and prior_kind in _EQUITY_FUND_SECURITY_KINDS
        and manual_kind != "ETN"
        and normalized_filer_kind != "ETN"
    ):
        confirmed_fund_kind = prior_kind
    elif generic_fund_identity and generic_fund_identity_is_compatible:
        # A vetted five-letter-X symbol proves generic fund-share identity,
        # but not whether the vehicle is open-end or closed-end. Preserve the
        # Equity instrument identity on a cold-cache rebuild without inventing
        # a reader-facing subtype.
        confirmed_fund_kind = "UNTYPED FUND"

    def canonical_non_option_type(instrument_type: str) -> str:
        if (
            confirmed_fund_kind
            and instrument_type not in {"CALL", "PUT", "OPT"}
        ):
            return "EQUITY"
        return instrument_type

    type_values = rec.get("instrument_type_value") or {}
    type_counts = rec.get("instrument_type_count") or {}
    non_option_types = {
        instrument_type
        for instrument_type, count in type_counts.items()
        if count > 0 and instrument_type not in {"CALL", "PUT", "OPT"}
    }
    if non_option_types:
        selected_type = max(
            non_option_types,
            key=lambda instrument_type: (
                type_values.get(instrument_type, 0),
                type_counts.get(instrument_type, 0),
                instrument_type,
            ),
        )
        if selected_type != "EQUITY":
            return canonical_non_option_type(selected_type)

        class_values = (
            rec.get("non_option_class_value")
            or rec.get("class_value")
            or {}
        )
        class_counts = rec.get("non_option_class_count") or {}
        top_cls = max(
            class_values,
            key=lambda cls: (
                class_values.get(cls, 0),
                class_counts.get(cls, 0),
                cls,
            ),
            default="",
        )
        issuer_values = (
            rec.get("non_option_issuer_value")
            or rec.get("issuer_value")
            or {}
        )
        issuer_counts = rec.get("non_option_issuer_count") or {}
        top_issuer = max(
            issuer_values,
            key=lambda issuer: (
                issuer_values.get(issuer, 0),
                issuer_counts.get(issuer, 0),
                issuer,
            ),
            default="",
        )
        return canonical_non_option_type(_classify_holding({
            "class": top_cls,
            "issuer": top_issuer,
            "put_call": "",
        }))

    option_types = {
        instrument_type
        for instrument_type, count in type_counts.items()
        if count > 0 and instrument_type in {"CALL", "PUT", "OPT"}
    }
    if option_types:
        selected_option = max(
            option_types,
            key=lambda instrument_type: (
                type_values.get(instrument_type, 0),
                type_counts.get(instrument_type, 0),
                {"OPT": 0, "PUT": 1, "CALL": 2}[instrument_type],
            ),
        )
        return selected_option

    # Compatibility for direct unit fixtures and any older in-memory evidence
    # object that predates structural row counts.
    call_value = rec["put_call_value"].get("CALL", 0)
    put_value = rec["put_call_value"].get("PUT", 0)
    non_option_value = rec["total_value"] - call_value - put_value
    if non_option_value > 0:
        top_cls = max(
            rec["class_value"],
            key=rec["class_value"].get,
            default="",
        )
        top_issuer = max(
            rec["issuer_value"],
            key=rec["issuer_value"].get,
            default="",
        )
        return canonical_non_option_type(_classify_holding({
            "class": top_cls,
            "issuer": top_issuer,
            "put_call": "",
        }))
    return "CALL" if call_value >= put_value else "PUT"


_OPTION_ROOT_TICKER_RE = re.compile(r"\bROOT\s*=\s*([A-Z]{1,6})\b")
_OPTION_LEADING_OCC_TICKER_RE = re.compile(
    r"^\s*([A-Z]{1,6})\s+\d{6,8}[CP]\d+\b"
)
_OPTION_PREFIX_ISSUER_RE = re.compile(r"^\s*(?:PUT|CALL)\s+\d+\s+", re.IGNORECASE)


def _strip_option_underlying_name(raw_name: str | None) -> str:
    """Best-effort issuer text for an option row after removing option noise."""
    name = str(raw_name or "").upper().strip()
    if not name:
        return ""

    name = _OPTION_ROOT_TICKER_RE.sub("", name)
    name = _OPTION_LEADING_OCC_TICKER_RE.sub("", name)
    name = _OPTION_PREFIX_ISSUER_RE.sub("", name)
    name = re.sub(r"\s+(?:PUT|CALL|CLL)\s+OPT\b.*$", "", name)
    name = re.sub(r"\s+OPT(?:ION|IONS)?\b.*$", "", name)
    name = re.sub(r"\s+OPTION\b.*$", "", name)
    name = re.sub(r"\s+EXP\b.*$", "", name)
    name = re.sub(r"\s+", " ", name).strip(" -/\t")
    return name


def _option_ticker_candidates(
    raw_name: str | None,
    name_to_ticker: dict[str, str],
) -> list[str]:
    """Ordered unique ticker candidates extracted from an option label."""
    name = str(raw_name or "").upper().strip()
    out: list[str] = []

    def add(candidate: str | None) -> None:
        ticker = str(candidate or "").strip().upper()
        if ticker and ticker not in out:
            out.append(ticker)

    root = _OPTION_ROOT_TICKER_RE.search(name)
    if root:
        add(root.group(1))

    leading = _OPTION_LEADING_OCC_TICKER_RE.match(name)
    if leading:
        add(leading.group(1))

    stripped = _strip_option_underlying_name(name)
    if stripped:
        add(resolve_ticker_from_name(stripped, "", name_to_ticker))

    return out


def _apply_option_underlying_derivations(
    registry: dict[str, dict],
    *,
    name_to_ticker: dict[str, str],
    sec_titles: dict[str, str],
) -> tuple[int, int]:
    """Backfill option entries from underlying equity records when safe."""
    equity_by_prefix6: dict[str, set[str]] = defaultdict(set)
    equity_by_ticker: dict[str, set[str]] = defaultdict(set)
    for cusip, entry in registry.items():
        if entry.get("type") != "EQUITY":
            continue
        if len(cusip) >= 6:
            equity_by_prefix6[cusip[:6]].add(cusip)
        ticker = str(entry.get("ticker") or "").strip().upper()
        if ticker:
            equity_by_ticker[ticker].add(cusip)

    derived_tickers = 0
    linked_underlyings = 0
    for cusip, entry in registry.items():
        if entry.get("type") not in {"CALL", "PUT", "OPT"}:
            continue

        sources: list[str] = list(entry.get("sources") or [])
        underlying_cusip = str(entry.get("underlying_cusip") or "").strip().upper() or None
        candidate_ticker = str(entry.get("ticker") or "").strip().upper() or None

        prefix_matches = sorted(equity_by_prefix6.get(cusip[:6], ()))
        if not underlying_cusip and len(prefix_matches) == 1:
            underlying_cusip = prefix_matches[0]
            if underlying_cusip != cusip and "derived_prefix6" not in sources:
                sources.append("derived_prefix6")

        candidates = _option_ticker_candidates(entry.get("dominant_issuer"), name_to_ticker)
        if not candidate_ticker and len(candidates) == 1:
            ticker_guess = candidates[0]
            if (
                ticker_guess in equity_by_ticker
                or ticker_guess in sec_titles
                or ticker_guess in MANUAL_CUSIP_TICKER_OVERRIDES.values()
            ):
                candidate_ticker = ticker_guess
                if "derived_option_text" not in sources:
                    sources.append("derived_option_text")

        if not underlying_cusip and candidate_ticker:
            ticker_matches = sorted(equity_by_ticker.get(candidate_ticker, ()))
            if len(ticker_matches) == 1:
                underlying_cusip = ticker_matches[0]
                if "derived_underlying" not in sources:
                    sources.append("derived_underlying")

        underlying_entry = registry.get(underlying_cusip or "") if underlying_cusip else None
        if not candidate_ticker and underlying_entry:
            candidate_ticker = underlying_entry.get("ticker") or None
            if candidate_ticker and "derived_underlying" not in sources:
                sources.append("derived_underlying")

        if candidate_ticker and not entry.get("ticker"):
            entry["ticker"] = candidate_ticker
            derived_tickers += 1

        if underlying_cusip and underlying_cusip != cusip:
            if entry.get("underlying_cusip") != underlying_cusip:
                entry["underlying_cusip"] = underlying_cusip
                linked_underlyings += 1

        canonical_name = ""
        if candidate_ticker and candidate_ticker in sec_titles:
            canonical_name = sec_titles[candidate_ticker]
        elif underlying_entry and underlying_entry.get("name"):
            canonical_name = underlying_entry["name"]
        if canonical_name and entry.get("name") != canonical_name:
            entry["name"] = canonical_name

        entry["sources"] = sources

    return derived_tickers, linked_underlyings


def _backfill_equity_tickers_from_option_consensus(
    registry: dict[str, dict],
    *,
    sec_titles: dict[str, str],
    openfigi_details: dict[str, dict] | None = None,
    prior_registry: dict[str, dict] | None = None,
) -> int:
    """Recover a missing equity ticker from one unambiguous option family.

    This is intentionally fail-closed. At least one CALL and one PUT sharing
    the equity's CUSIP prefix and SEC-proof issuer key must agree on one
    SEC-verified ticker. The equity must be at least as current as every proof
    row and every same-family equity, and no other equity may already own the
    ticker. When a prefix has multiple equity CUSIPs, only one unique newest
    successor can receive it. Trusted non-common security-kind evidence also
    excludes a candidate before any option-derived ticker is considered.
    """

    openfigi_details = openfigi_details or {}
    prior_registry = prior_registry or {}

    equity_ticker_owners: dict[str, set[str]] = defaultdict(set)
    family_equities: dict[
        tuple[str, str],
        list[tuple[str, dict]],
    ] = defaultdict(list)
    equity_candidates: dict[
        tuple[str, str],
        list[tuple[str, dict]],
    ] = defaultdict(list)
    option_families: dict[
        tuple[str, str],
        list[tuple[str, dict]],
    ] = defaultdict(list)

    for cusip, entry in registry.items():
        instrument_type = normalize_instrument_type(entry.get("type"))
        ticker = str(entry.get("ticker") or "").strip().upper()
        issuer_key = sec_issuer_proof_key(
            entry.get("dominant_issuer") or entry.get("name") or ""
        )
        if instrument_type == "EQUITY":
            if ticker:
                equity_ticker_owners[ticker].add(cusip)
            if len(cusip) >= 6 and issuer_key:
                family_key = (cusip[:6], issuer_key)
                family_equities[family_key].append((cusip, entry))
            else:
                family_key = None
            candidate_kind, _candidate_kind_source = _registry_security_kind(
                identifier=cusip,
                openfigi_detail=openfigi_details.get(cusip),
                prior_entry=prior_registry.get(cusip),
                entry=entry,
            )
            if (
                not ticker
                and family_key is not None
                and not is_synthetic_identifier(cusip)
                and "ticker_collision_demoted"
                not in set(entry.get("sources") or [])
                and candidate_kind in {None, "COMMON"}
                and _filer_security_kind(entry)
                not in {
                    "ETF",
                    "ETN",
                    "MUTUAL FUND",
                    "CLOSED-END FUND",
                    "PREFERRED",
                    "WARRANT",
                    "RIGHT",
                    "UNIT",
                }
            ):
                equity_candidates[family_key].append(
                    (cusip, entry)
                )
        elif (
            instrument_type in {"CALL", "PUT", "OPT"}
            and len(cusip) >= 6
            and issuer_key
            and "derived_option_text" in set(entry.get("sources") or [])
        ):
            option_families[(cusip[:6], issuer_key)].append((cusip, entry))

    proposals: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for family_key, options in option_families.items():
        candidates = equity_candidates.get(family_key, [])
        all_family_equities = family_equities.get(family_key, [])
        option_cusips = {cusip for cusip, _entry in options}
        option_types = {
            normalize_instrument_type(entry.get("type"))
            for _cusip, entry in options
        }
        if (
            not candidates
            or not all_family_equities
            or len(option_cusips) < 2
            or not {"CALL", "PUT"}.issubset(option_types)
        ):
            continue
        option_tickers = {
            str(entry.get("ticker") or "").strip().upper()
            for _cusip, entry in options
        }
        if len(option_tickers) != 1 or "" in option_tickers:
            continue
        ticker = next(iter(option_tickers))
        family_issuer_key = family_key[1]
        if (
            equity_ticker_owners.get(ticker)
            or ticker not in sec_titles
            or not _OPENFIGI_PLAIN_TICKER_RE.fullmatch(ticker)
            or sec_issuer_proof_key(sec_titles[ticker]) != family_issuer_key
        ):
            continue

        linked_targets = {
            str(entry.get("underlying_cusip") or "").strip().upper()
            for _cusip, entry in options
            if str(entry.get("underlying_cusip") or "").strip()
        }
        if len(linked_targets) > 1:
            continue
        if linked_targets:
            target_cusip = next(iter(linked_targets))
            winners = [
                candidate
                for candidate in candidates
                if candidate[0] == target_cusip
            ]
        else:
            newest_equity_date = max(
                str(entry.get("last_seen") or "")
                for _cusip, entry in all_family_equities
            )
            newest_family_equities = [
                family_equity
                for family_equity in all_family_equities
                if str(family_equity[1].get("last_seen") or "")
                == newest_equity_date
            ]
            winners = (
                newest_family_equities
                if len(newest_family_equities) == 1
                and newest_family_equities[0] in candidates
                else []
            )
        if len(winners) != 1:
            continue

        target_cusip, target_entry = winners[0]
        target_date = str(target_entry.get("last_seen") or "")
        newest_proof_date = max(
            str(entry.get("last_seen") or "")
            for _cusip, entry in options
        )
        newest_family_equity_date = max(
            str(entry.get("last_seen") or "")
            for _cusip, entry in all_family_equities
        )
        if (
            not target_date
            or target_date < newest_proof_date
            or target_date < newest_family_equity_date
            or target_entry.get("ticker")
            or is_synthetic_identifier(target_cusip)
            or "ticker_collision_demoted"
            in set(target_entry.get("sources") or [])
        ):
            continue
        proposals[ticker].append((target_cusip, target_entry))

    backfilled = 0
    for ticker, ticker_proposals in proposals.items():
        if len(ticker_proposals) != 1:
            continue
        target_cusip, entry = ticker_proposals[0]
        family_key = (
            target_cusip[:6],
            sec_issuer_proof_key(
                entry.get("dominant_issuer") or entry.get("name") or ""
            ),
        )
        entry["ticker"] = ticker
        sources = list(entry.get("sources") or [])
        if "option_family_consensus" not in sources:
            sources.append("option_family_consensus")
        entry["name"] = sec_titles[ticker]
        if "sec_title" not in sources:
            sources.append("sec_title")
        entry["sources"] = sources
        entry["ticker_evidence_cusips"] = sorted(
            cusip for cusip, _option_entry in option_families[family_key]
        )
        backfilled += 1

    return backfilled


def _deduplicate_registry_equity_tickers(
    registry: dict[str, dict],
) -> int:
    """Keep one current canonical equity CUSIP per searchable ticker."""

    by_ticker: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for cusip, entry in registry.items():
        ticker = str(entry.get("ticker") or "").strip().upper()
        if ticker and entry.get("type") == "EQUITY":
            by_ticker[ticker].append((cusip, entry))

    demoted = 0
    ticker_sources = {
        "cusip_map_vetted",
        "manual_override",
        "openfigi_plain_ticker",
        "openfigi_prior_registry_ticker",
        "sec_validated_ticker_alias",
        "sec_title",
    }
    for ticker, claims in by_ticker.items():
        if len(claims) < 2:
            continue
        alias_claims = [
            entry
            for _cusip, entry in claims
            if "sec_validated_ticker_alias"
            in set(entry.get("sources") or [])
        ]
        alias_names = {
            normalize_name(entry.get("name") or "")
            for entry in alias_claims
        }
        issuer_names = {
            normalize_name(
                entry.get("dominant_issuer") or entry.get("name") or ""
            )
            for _cusip, entry in claims
        }
        if (
            alias_claims
            and len(alias_names) == 1
            and "" not in alias_names
            and issuer_names == alias_names
        ):
            continue

        def rank(claim: tuple[str, dict]) -> tuple:
            cusip, entry = claim
            sources = set(entry.get("sources") or [])
            return (
                "manual_override" in sources,
                str(entry.get("last_seen") or ""),
                str(entry.get("first_seen") or ""),
                bool({
                    "openfigi_plain_ticker",
                    "openfigi_prior_registry_ticker",
                } & sources),
                int(entry.get("holder_count") or 0),
                int(entry.get("total_value") or 0),
                cusip,
            )

        winner_cusip, _winner = max(claims, key=rank)
        for cusip, entry in claims:
            if cusip == winner_cusip:
                continue
            sources = [
                source
                for source in list(entry.get("sources") or [])
                if source not in ticker_sources
            ]
            dominant_name = normalize_security_label(
                entry.get("dominant_issuer"),
                identifier=cusip,
            )
            if dominant_name:
                entry["name"] = dominant_name
                if "filer_dominant" not in sources:
                    sources.append("filer_dominant")
            else:
                entry["name"] = ""
            if "ticker_collision_demoted" not in sources:
                sources.append("ticker_collision_demoted")
            entry["sources"] = sources
            entry["ticker"] = None
            entry.pop("source_ticker", None)
            demoted += 1
        log.info(
            "  canonical ticker %s retained on %s; demoted %s duplicate "
            "equity CUSIP(s)",
            ticker,
            winner_cusip,
            len(claims) - 1,
        )
    return demoted


def _allow_legacy_registry_ticker(
    *,
    cusip: str,
    ticker: str | None,
    instrument_type: str,
    legacy_equity_claims: Counter[str],
    dominant_class: str | None = None,
    openfigi_detail: dict | None = None,
) -> bool:
    """Whether a CUSIP-cache value is safe to expose as a canonical ticker.

    Canonical tickers remain unique, plain equity-style symbols. Descriptive
    OpenFIGI note strings are label metadata and are handled separately.
    """
    raw_ticker = str(ticker or "").strip().upper()
    if not raw_ticker:
        return False
    if instrument_type != "EQUITY":
        return False
    if is_synthetic_identifier(cusip):
        return False
    if _is_display_only_security_symbol(raw_ticker, dominant_class):
        return False
    if _classify_ticker_health(cusip, raw_ticker):
        return False
    if (
        isinstance(openfigi_detail, dict)
        and openfigi_detail.get("status") == "matched"
        and str(openfigi_detail.get("ticker") or "").strip().upper()
        == raw_ticker
        and not _openfigi_plain_ticker_is_vetted(
            openfigi_detail,
            raw_ticker,
        )
    ):
        return False
    if legacy_equity_claims.get(raw_ticker, 0) > 1:
        return False
    return True


def _openfigi_is_us_exchange(detail: dict) -> bool:
    exchange_code = str(detail.get("exchCode") or "").strip().upper()
    return (
        exchange_code in _OPENFIGI_US_EXCHANGE_CODES
        or exchange_code.startswith("NASDAQ")
        or exchange_code.startswith("NYSE")
    )


def _is_display_only_security_symbol(
    ticker: str | None,
    dominant_class: str | None = None,
) -> bool:
    """Whether a symbol describes a right/warrant rather than common stock."""

    raw_ticker = str(ticker or "").strip().upper()
    raw_class = str(dominant_class or "").strip().upper()
    return bool(
        _DISPLAY_ONLY_TICKER_SUFFIX_RE.search(raw_ticker)
        or _DISPLAY_ONLY_SECURITY_CLASS_RE.search(raw_class)
    )


def _openfigi_is_structured_terms_label(label: str) -> bool:
    return (
        not label[:1].isdigit()
        and bool(re.search(r"\s", label))
        and bool(_OPENFIGI_STRUCTURED_TERMS_RE.search(label))
    )


def _openfigi_plain_ticker_is_vetted(
    detail: dict,
    ticker: str,
) -> bool:
    """Accept a public symbol only when it is plain and on a US venue."""

    return bool(
        _OPENFIGI_PLAIN_TICKER_RE.fullmatch(ticker)
        and _openfigi_is_us_exchange(detail)
    )


def _openfigi_display_ticker_is_vetted(
    detail: dict,
    ticker: str,
) -> bool:
    """Accept useful US display symbols, including warrant/right suffixes."""

    return bool(
        _OPENFIGI_DISPLAY_TICKER_RE.fullmatch(ticker)
        and _openfigi_is_us_exchange(detail)
    )


def _openfigi_security_label(
    detail: dict | None,
    identifier: str,
) -> str | None:
    """Return useful FIGI metadata without promoting opaque venue symbols."""

    if not isinstance(detail, dict) or detail.get("status") != "matched":
        return None

    ticker = normalize_security_label(
        detail.get("ticker"),
        identifier=identifier,
    )
    if ticker and _openfigi_is_structured_terms_label(ticker):
        return ticker

    description = normalize_security_label(
        detail.get("securityDescription"),
        identifier=identifier,
    )
    structured_description = normalize_note_security_label(description)
    if structured_description and description != ticker:
        return structured_description

    if ticker and _openfigi_display_ticker_is_vetted(detail, ticker):
        return ticker

    if (
        description
        and description != ticker
        and not description[:1].isdigit()
        and not _OPENFIGI_TICKER_LIKE_TOKEN_RE.fullmatch(description)
    ):
        return description

    name = normalize_security_label(
        detail.get("name"),
        identifier=identifier,
    )
    if name:
        return name
    return None


def _openfigi_security_kind(detail: dict | None) -> str | None:
    """Map high-confidence FIGI metadata to a display-only kind."""

    if not isinstance(detail, dict) or detail.get("status") != "matched":
        return None
    security_type = str(detail.get("securityType") or "").strip().upper()
    security_type2 = str(detail.get("securityType2") or "").strip().upper()
    market_sector = str(detail.get("marketSector") or "").strip().upper()
    combined = f"{security_type} {security_type2} {market_sector}"
    descriptive = " ".join(
        (
            combined,
            str(detail.get("name") or ""),
            str(detail.get("securityDescription") or ""),
        )
    )

    # ETP is structural but does not distinguish a fund from a note. Use an
    # explicit ETN description only to refine that structural ETP category.
    # Check it before the broader debt terms because real ETNs can also carry
    # Corp or Note in FIGI's secondary structural fields.
    if security_type == "ETP":
        if _FILER_ETN_KIND_RE.search(descriptive):
            return "ETN"
        return "ETF"
    # Prefer FIGI's remaining structural taxonomy to words in a ticker, name,
    # or description. For example, Eaton's corporate bond description begins
    # with "ETN", but its security type is GLOBAL and securityType2 is Corp.
    if re.search(
        r"\b(?:CORP|BOND|NOTE|ABS|CMBS|MTGE|MBS|MUNI|GOVT|LL)\b"
        r"|\bASSET[- ]BACKED\b|\bMORTGAGE\b|\bWHOLE LOAN\b",
        combined,
    ):
        return "BOND"
    if re.search(r"\bCLOSED-END FUND\b", combined):
        return "CLOSED-END FUND"
    if (
        re.search(r"\bPREFER(?:RED|ENCE)\b", combined)
        or market_sector == "PFD"
    ):
        return "PREFERRED"
    if re.search(r"\b(?:WARRANT|EQUITY WRT)\b", combined):
        return "WARRANT"
    if re.search(r"\bRIGHTS?\b", combined):
        return "RIGHT"
    if re.search(r"\bUNITS?\b", combined):
        return "UNIT"
    if re.search(r"\b(?:OPEN-END|MUTUAL) FUND\b", combined):
        return "MUTUAL FUND"
    if _openfigi_is_depositary_receipt(detail):
        return None
    if re.search(r"\bCOMMON STOCK\b", combined):
        return "COMMON"
    if _FILER_ETN_KIND_RE.search(descriptive):
        return "ETN"
    return None


def _openfigi_is_depositary_receipt(detail: dict | None) -> bool:
    """Return whether FIGI structurally identifies a depositary receipt."""

    if not isinstance(detail, dict) or detail.get("status") != "matched":
        return False
    combined = " ".join(
        str(detail.get(field) or "").strip().upper()
        for field in ("securityType", "securityType2", "marketSector")
    )
    return bool(
        re.search(r"\b(?:DEPOSITARY RECEIPT|ADR|ADS)\b", combined)
    )


def _filer_is_depositary_receipt(entry: dict | None) -> bool:
    """Return whether retained filer metadata identifies a depositary receipt."""

    if not isinstance(entry, dict):
        return False
    combined = " ".join(
        str(entry.get(field) or "").strip()
        for field in ("name", "dominant_issuer", "dominant_class")
    )
    return bool(
        re.search(
            r"\b(?:ADRS?|ADS|DEPOSITARY|DEP(?:OSITARY)?(?:\s+SHS?)?)\b",
            combined,
            re.IGNORECASE,
        )
    )


def _openfigi_fund_conflicts_with_filer_common(
    entry: dict | None,
    detail: dict | None,
) -> bool:
    """Fail closed when a fund match contradicts explicit common-stock data."""

    if (
        not isinstance(entry, dict)
        or _openfigi_security_kind(detail) not in _FUND_PRODUCT_NAME_KINDS
        or normalize_instrument_type(entry.get("type")) != "EQUITY"
    ):
        return False
    dominant_class = " ".join(
        str(entry.get("dominant_class") or "").upper().split()
    )
    if not (
        _FILER_COMMON_KIND_RE.search(dominant_class)
        or _FILER_COMMON_CLASS_ONLY_RE.fullmatch(dominant_class)
    ):
        return False
    issuer_text = " ".join(
        str(entry.get(field) or "").strip()
        for field in ("name", "dominant_issuer")
    )
    combined = " ".join(
        (issuer_text, dominant_class)
    )
    if (
        _FILER_COMMON_CLASS_EXCLUSION_RE.search(dominant_class)
        or _FILER_COMMON_ISSUER_EXCLUSION_RE.search(issuer_text)
        or _FUND_KIND_DISCOVERY_RE.search(combined)
    ):
        return False

    detail_ticker = str(
        (detail or {}).get("ticker") or ""
    ).strip().upper()
    if (
        detail_ticker
        and _openfigi_plain_ticker_is_vetted(detail, detail_ticker)
    ):
        return False

    existing_name = str(
        entry.get("name")
        or entry.get("dominant_issuer")
        or ""
    )
    detail_name = str((detail or {}).get("name") or "")
    return not _issuers_likely_same(existing_name, detail_name)


def _registry_security_kind(
    *,
    identifier: str,
    openfigi_detail: dict | None,
    prior_entry: dict | None,
    entry: dict | None = None,
) -> tuple[str | None, str | None]:
    manual = normalize_security_kind(
        MANUAL_SECURITY_KIND_OVERRIDES.get(identifier)
    )
    if manual:
        return manual, "manual_verified"
    filer_kind = _filer_security_kind(entry)
    current = normalize_security_kind(
        _openfigi_security_kind(openfigi_detail)
    )
    openfigi_depositary_receipt = _openfigi_is_depositary_receipt(
        openfigi_detail
    )
    filer_depositary_receipt = _filer_is_depositary_receipt(entry)
    is_depositary_receipt = (
        openfigi_depositary_receipt or filer_depositary_receipt
    )
    openfigi_fund_conflict = _openfigi_fund_conflicts_with_filer_common(
        entry,
        openfigi_detail,
    )
    if openfigi_fund_conflict:
        current = None
    # OpenFIGI's broad ETP taxonomy includes exchange-traded notes. Preserve
    # an explicit filer ETN description instead of presenting debt as an ETF.
    if filer_kind == "ETN" and current == "ETF":
        return "ETN", "filer_metadata"
    # A specific SEC titleOfClass is stronger evidence than OpenFIGI's generic
    # common-stock bucket. This prevents units, preferreds, warrants, and funds
    # from being flattened to Common Stock.
    if current == "COMMON" and filer_kind not in {None, "COMMON"}:
        return filer_kind, "filer_metadata"
    # Generic OpenFIGI COMMON metadata must not erase filer-side ADR/ADS
    # evidence. Depositary receipts remain broad Equity unless a more specific
    # kind (for example PREFERRED) is present.
    if current == "COMMON" and filer_depositary_receipt:
        current = None
    if current:
        return current, "openfigi"
    # Depositary receipts are equity securities, but they are not the issuer's
    # common stock. Suppress generic COMMON fallbacks while retaining any
    # specific filer classification such as PREFERRED.
    if is_depositary_receipt and filer_kind == "COMMON":
        filer_kind = None
    if (
        not openfigi_fund_conflict
        and isinstance(prior_entry, dict)
    ):
        prior_source = str(
            prior_entry.get("security_kind_source") or ""
        ).strip()
        prior = normalize_security_kind(
            prior_entry.get("security_kind")
        )
        compatible_prior = not (
            prior == "COMMON"
            and (
                normalize_instrument_type(
                    (entry or {}).get("type")
                ) != "EQUITY"
                or is_depositary_receipt
            )
        )
        # Filer-derived kinds must be reproducible from today's retained SEC
        # evidence.  Otherwise a retired inference rule becomes permanent via
        # the prior registry.  Keep only the legacy COMMON compatibility path;
        # older registry rows predate the current common-stock classifier.
        if prior_source == "filer_metadata":
            if filer_kind:
                return filer_kind, "filer_metadata"
            if prior == "COMMON" and compatible_prior:
                return prior, prior_source
            prior = None
        trusted_prior = (
            prior_source.startswith("openfigi")
            or prior_source == "manual_verified"
        )
        if prior and trusted_prior and compatible_prior:
            if prior_source.startswith("openfigi"):
                return prior, "openfigi_prior_registry"
            return prior, prior_source
    if filer_kind:
        return filer_kind, "filer_metadata"
    return None, None


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
        not _OPENFIGI_PLAIN_TICKER_RE.fullmatch(ticker)
        or _FILER_MUTUAL_FUND_TICKER_RE.fullmatch(ticker)
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


def _entry_has_trusted_fund_symbol_evidence(entry: dict | None) -> bool:
    """Recognize an untyped fund symbol without guessing its legal subtype."""

    if not isinstance(entry, dict):
        return False
    kind = normalize_security_kind(entry.get("security_kind"))
    if kind is not None:
        return False
    sources = set(entry.get("sources") or [])
    if (
        "ticker_collision_demoted" in sources
        or not (sources & _FUND_IDENTITY_TICKER_SOURCES)
    ):
        return False
    ticker = str(entry.get("ticker") or "").strip().upper()
    return bool(_FILER_MUTUAL_FUND_TICKER_RE.fullmatch(ticker))


def _registry_entry_has_equity_fund_identity(
    entry: dict | None,
) -> bool:
    """Identify fund shares without guessing their exact legal fund kind."""

    if not isinstance(entry, dict):
        return False
    if normalize_instrument_type(entry.get("type")) != "EQUITY":
        return False
    kind = normalize_security_kind(entry.get("security_kind"))
    if kind in _EQUITY_FUND_SECURITY_KINDS:
        return True
    return _entry_has_trusted_fund_symbol_evidence(entry)


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
        and "sec_title" in entry_sources
        and not _FILER_COMMON_CLASS_EXCLUSION_RE.search(dominant_class)
        and not _FILER_COMMON_ISSUER_EXCLUSION_RE.search(issuer_text)
    ):
        return "COMMON"
    return None


def _fund_product_name_terms(name: str | None) -> set[str]:
    """Return semantic product terms without legal/feed boilerplate."""

    tokens = set(re.findall(r"[A-Z0-9]+", str(name or "").upper()))
    # OpenFIGI commonly prefixes State Street fund names with "SS". It is
    # publisher shorthand rather than reader-facing product detail.
    return {
        token
        for token in tokens
        if (
            len(token) >= 2
            and token not in _FUND_PRODUCT_NAME_GENERIC_TOKENS
            and token not in _ISSUER_STOPWORDS
            and token != "SS"
        )
    }


def _fund_product_name_is_probable_truncation(
    current_name: str | None,
    prior_name: str | None,
) -> bool:
    """Recognize the cache's fixed-width prefix truncation, not fund renames."""

    current = " ".join(str(current_name or "").upper().split())
    prior = " ".join(str(prior_name or "").upper().split())
    if (
        len(current) < _OPENFIGI_COMPACT_NAME_LIMIT
        or len(prior) <= len(current)
    ):
        return False
    current_key = re.sub(r"[^A-Z0-9]", "", current)
    prior_key = re.sub(r"[^A-Z0-9]", "", prior)
    return bool(current_key and prior_key.startswith(current_key))


def _fund_product_name_only_abbreviates_existing(
    existing_name: str | None,
    candidate_name: str | None,
) -> bool:
    """Return whether a shorter candidate adds no non-abbreviated terms."""

    existing = " ".join(str(existing_name or "").upper().split())
    candidate = " ".join(str(candidate_name or "").upper().split())
    if not existing or not candidate or len(candidate) >= len(existing):
        return False
    existing_tokens = set(re.findall(r"[A-Z0-9]+", existing))
    candidate_tokens = {
        token
        for token in re.findall(r"[A-Z0-9]+", candidate)
        if (
            token not in _FUND_PRODUCT_NAME_GENERIC_TOKENS
            and token not in _FUND_PRODUCT_ABBREVIATION_NOISE_TOKENS
            and token not in _ISSUER_STOPWORDS
        )
    }

    return bool(candidate_tokens) and all(
        any(
            _fund_product_token_is_abbreviation(token, existing_token)
            for existing_token in existing_tokens
        )
        for token in candidate_tokens
    )


def _fund_product_token_is_abbreviation(
    token: str,
    expanded: str,
) -> bool:
    """Return whether token is an ordered abbreviation of expanded."""

    if token == expanded:
        return True
    if not token or not expanded or token[0] != expanded[0]:
        return False
    expanded_chars = iter(expanded[1:])
    return all(
        any(char == expanded_char for expanded_char in expanded_chars)
        for char in token[1:]
    )


def _fund_product_name_is_self_referential(
    name: str | None,
    *,
    aliases: tuple[object, ...],
) -> bool:
    """Reject ticker-only names and pipeline-added ``name — TICKER`` labels."""

    normalized = " ".join(str(name or "").upper().split())
    normalized_aliases = {
        " ".join(str(alias or "").upper().split())
        for alias in aliases
        if str(alias or "").strip()
    }
    if not normalized or not normalized_aliases:
        return False
    if normalized in normalized_aliases:
        return True
    return any(
        normalized.endswith(f" — {alias}")
        for alias in normalized_aliases
    )


def _fund_product_name_degrades_existing(
    existing_name: str | None,
    candidate_name: str | None,
    *,
    aliases: tuple[object, ...] = (),
) -> bool:
    """Return whether candidate loses readable detail from an existing name."""

    existing = " ".join(str(existing_name or "").upper().split())
    candidate = " ".join(str(candidate_name or "").upper().split())
    if not existing:
        return False
    if not candidate:
        return True
    if _fund_product_name_is_self_referential(
        candidate,
        aliases=aliases,
    ):
        return True
    if _fund_product_name_is_probable_truncation(candidate, existing):
        return True
    if _fund_product_name_only_abbreviates_existing(existing, candidate):
        return True

    existing_terms = _fund_product_name_terms(existing)
    candidate_terms = _fund_product_name_terms(candidate)
    if (
        existing_terms
        and candidate_terms == existing_terms
        and len(candidate) < len(existing)
    ):
        return True

    alias_terms = {
        token
        for alias in aliases
        for token in re.findall(r"[A-Z0-9]+", str(alias or "").upper())
    }
    comparable_candidate_terms = candidate_terms - alias_terms
    if not existing_terms or not comparable_candidate_terms:
        return False
    abbreviation_matches = {
        token
        for token in comparable_candidate_terms
        if any(
            _fund_product_token_is_abbreviation(token, existing_token)
            for existing_token in existing_terms
        )
    }
    if (
        len(abbreviation_matches) < 2
        or len(abbreviation_matches) * 5
        < len(comparable_candidate_terms) * 3
    ):
        return False

    existing_vowels = sum(
        char in "AEIOU"
        for token in existing_terms
        for char in token
    )
    candidate_vowels = sum(
        char in "AEIOU"
        for token in comparable_candidate_terms
        for char in token
    )
    return candidate_vowels < existing_vowels


def _fund_product_name_materially_adds_detail(
    *,
    identifier: str,
    entry: dict,
    existing_name: str,
    candidate: str,
) -> bool:
    """Require real product detail before overriding an existing name."""

    if _fund_product_name_is_probable_truncation(
        candidate,
        existing_name,
    ):
        return False
    if _fund_product_name_only_abbreviates_existing(
        existing_name,
        candidate,
    ):
        return False

    existing_terms = _fund_product_name_terms(existing_name)
    candidate_terms = _fund_product_name_terms(candidate)
    existing_semantic_score = (
        len(existing_terms),
        sum(len(term) for term in existing_terms),
    )
    candidate_semantic_score = (
        len(candidate_terms),
        sum(len(term) for term in candidate_terms),
    )
    if candidate_semantic_score <= existing_semantic_score:
        return False

    missing_class_detail = _informative_fund_product_class(
        entry.get("dominant_class"),
        identifier=identifier,
        existing_name=existing_name,
        aliases=(
            entry.get("ticker"),
            entry.get("security_label"),
        ),
    )
    if missing_class_detail:
        return True

    additions = candidate_terms - existing_terms
    substantial_additions = {
        token
        for token in additions
        if (
            len(re.sub(r"[^A-Z]", "", token)) >= 3
            or (
                token.isdigit()
                and len(token) >= 2
            )
        )
    }
    existing_raw_tokens = set(
        re.findall(r"[A-Z0-9]+", existing_name.upper())
    )
    is_legal_vehicle_name = bool(
        existing_raw_tokens & _FUND_PRODUCT_VEHICLE_TOKENS
    )
    if (
        is_legal_vehicle_name
        and existing_terms.issubset(candidate_terms)
        and substantial_additions
    ):
        return True

    # A legal/sponsor name and a CUSIP-specific product name can use different
    # abbreviations. Require two new semantic terms in that case so a venue
    # prefix or country suffix alone cannot displace a descriptive name.
    return len(existing_terms) <= 2 and len(additions) >= 2


def _openfigi_fund_product_name(
    detail: dict | None,
    *,
    identifier: str,
) -> str | None:
    """Return a safe CUSIP-specific fund name, never a ticker or note label."""

    if not isinstance(detail, dict) or detail.get("status") != "matched":
        return None
    name = normalize_security_label(
        detail.get("name"),
        identifier=identifier,
    )
    if not name or normalize_note_security_label(name):
        return None

    aliases = {
        str(detail.get(field) or "").strip().casefold()
        for field in ("ticker", "securityDescription")
        if str(detail.get(field) or "").strip()
    }
    if name.casefold() in aliases:
        return None

    # A handful of compact Bloomberg names arrive as one long concatenated
    # token. The retained filer issuer/class is more readable in those cases.
    if (
        not re.search(r"\s", name)
        and _OPENFIGI_TICKER_LIKE_TOKEN_RE.fullmatch(name)
    ):
        return None
    return name


def _informative_fund_product_class(
    class_name: object | None,
    *,
    identifier: str,
    existing_name: str | None,
    aliases: tuple[object, ...] = (),
) -> str | None:
    """Return filer class detail only when it adds product-specific terms."""

    normalized = normalize_security_label(
        class_name,
        identifier=identifier,
    )
    if not normalized:
        return None
    normalized_aliases = {
        normalized_alias
        for alias in aliases
        if (normalized_alias := normalize_security_label(alias))
    }
    if normalized.casefold() in {
        alias.casefold()
        for alias in normalized_aliases
    }:
        return None
    alias_tokens = {
        token
        for alias in normalized_aliases
        for token in re.findall(r"[A-Z0-9]+", alias.upper())
    }
    class_tokens = set(re.findall(r"[A-Z0-9]+", normalized.upper()))
    specific_tokens = {
        token
        for token in class_tokens
        if (
            token not in _FUND_PRODUCT_CLASS_GENERIC_TOKENS
            and token not in alias_tokens
        )
    }
    existing_tokens = set(
        re.findall(r"[A-Z0-9]+", str(existing_name or "").upper())
    )
    uncovered_tokens = {
        token
        for token in specific_tokens
        if not any(
            token == existing_token
            or (
                len(token) >= 3
                and len(existing_token) >= 3
                and token[:3] == existing_token[:3]
            )
            for existing_token in existing_tokens
        )
    }
    if not any(
        (
            len(re.sub(r"[^A-Z]", "", token)) >= 3
            or token in _FUND_PRODUCT_SHORT_SPECIFIC_TOKENS
            or (
                token.isdigit()
                and len(token) >= 2
            )
        )
        for token in uncovered_tokens
    ):
        return None
    return normalized


def _fund_product_name_fallback(
    *,
    identifier: str,
    entry: dict,
    existing_name: str | None,
) -> tuple[str | None, str | None]:
    """Compose a readable issuer/class fallback with auditable provenance."""

    class_name = _informative_fund_product_class(
        entry.get("dominant_class"),
        identifier=identifier,
        existing_name=existing_name,
        aliases=(
            entry.get("ticker"),
            entry.get("security_label"),
        ),
    )
    if not class_name:
        return None, None
    product_name = normalize_security_label(
        (
            f"{existing_name} — {class_name}"
            if existing_name
            else class_name
        ),
        identifier=identifier,
    )
    if not product_name:
        return None, None

    sources = set(entry.get("sources") or [])
    if existing_name:
        if "manual_name_override" in sources:
            source = "manual_name_class"
        elif "sec_title" in sources:
            source = "sec_title_class"
        else:
            source = "filer_issuer_class"
    else:
        source = "filer_class"
    return product_name, source


def _registry_fund_product_name(
    *,
    identifier: str,
    entry: dict,
    openfigi_detail: dict | None,
    prior_entry: dict | None,
) -> tuple[str | None, str | None]:
    """Choose a display-only fund product name without changing issuer identity."""

    kind = normalize_security_kind(entry.get("security_kind"))
    if kind not in _FUND_PRODUCT_NAME_KINDS:
        return None, None
    if _openfigi_fund_conflicts_with_filer_common(
        entry,
        openfigi_detail,
    ):
        return None, None

    existing_name = (
        normalize_security_label(
            entry.get("name"),
            identifier=identifier,
        )
        or normalize_security_label(
            entry.get("dominant_issuer"),
            identifier=identifier,
        )
    )
    # Publish an explicit product name as well as the canonical registry name.
    # This lets already-generated fund rows display the correction immediately
    # through security_labels.json, even before their retained filer issuer is
    # rewritten by the next canonicalization pass.
    if "manual_name_override" in set(entry.get("sources") or []):
        return (
            (existing_name, "manual_name_override")
            if existing_name
            else (None, None)
        )
    filer_etn_name = (
        existing_name
        if (
            kind == "ETN"
            and existing_name
            and _FILER_ETN_KIND_RE.search(existing_name)
        )
        else None
    )
    filer_etn_source = (
        "sec_title"
        if "sec_title" in set(entry.get("sources") or [])
        else "filer_issuer"
    )

    current_candidate = _openfigi_fund_product_name(
        openfigi_detail,
        identifier=identifier,
    )
    prior_candidate = None
    prior_candidate_source = None
    if isinstance(prior_entry, dict):
        prior_source = str(
            prior_entry.get("product_name_source") or ""
        ).strip()
        normalized_prior_candidate = normalize_security_label(
            prior_entry.get("product_name"),
            identifier=identifier,
        )
        # Older warm-cache runs could relabel a pipeline-decorated
        # ``OpenFIGI name — filer class/ticker`` as openfigi_prior_registry.
        # The separator proves this is not the raw vendor name, so do not let
        # it hide the original duplicate group from SEC disambiguation.
        if (
            prior_source
            and normalized_prior_candidate
            and not (
                prior_source.startswith("openfigi")
                and " — " in normalized_prior_candidate
            )
            and not _fund_product_name_is_self_referential(
                normalized_prior_candidate,
                aliases=(
                    identifier,
                    entry.get("ticker"),
                    entry.get("security_label"),
                    prior_entry.get("ticker"),
                    prior_entry.get("security_label"),
                ),
            )
        ):
            prior_candidate = normalized_prior_candidate
            prior_candidate_source = (
                "openfigi_prior_registry"
                if prior_source.startswith("openfigi")
                else prior_source
            )

    candidate = current_candidate
    candidate_source = "openfigi"
    selected_prior_candidate = False
    if prior_candidate and (
        not candidate
        or _fund_product_name_degrades_existing(
            prior_candidate,
            candidate,
            aliases=(
                identifier,
                entry.get("ticker"),
                entry.get("security_label"),
            ),
        )
    ):
        candidate = prior_candidate
        candidate_source = prior_candidate_source
        selected_prior_candidate = True

    if candidate:
        if not existing_name:
            return candidate, candidate_source
        if (
            selected_prior_candidate
            and candidate.casefold() != existing_name.casefold()
        ):
            return candidate, candidate_source
        candidate_is_better = _fund_product_name_materially_adds_detail(
            identifier=identifier,
            entry=entry,
            existing_name=existing_name,
            candidate=candidate,
        )
        if (
            candidate_is_better
            and candidate.casefold() != existing_name.casefold()
        ):
            return candidate, candidate_source
        if filer_etn_name:
            return filer_etn_name, filer_etn_source
        # A compact OpenFIGI name can be less descriptive than the SEC/filer
        # name. In that case retain the fuller existing name, but still add
        # genuinely product-specific class detail when the issuer alone lacks
        # it.
        return _fund_product_name_fallback(
            identifier=identifier,
            entry=entry,
            existing_name=existing_name,
        )

    if filer_etn_name:
        return filer_etn_name, filer_etn_source
    return _fund_product_name_fallback(
        identifier=identifier,
        entry=entry,
        existing_name=existing_name,
    )


def _registry_fund_symbol(
    *,
    identifier: str,
    entry: dict,
) -> str | None:
    """Return one plain listed symbol for fund-name disambiguation only."""

    for candidate in (
        entry.get("security_label"),
        entry.get("ticker"),
    ):
        symbol = str(candidate or "").strip().upper()
        if (
            symbol != identifier
            and _OPENFIGI_PLAIN_TICKER_RE.fullmatch(symbol)
        ):
            return symbol
    return None


def _duplicate_fund_product_name_groups(
    registry: dict[str, dict],
) -> list[list[tuple[str, dict]]]:
    """Return same-name product groups that still contain multiple entries."""

    grouped: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for identifier, entry in registry.items():
        product_name = str(entry.get("product_name") or "").strip()
        if product_name:
            grouped[product_name.casefold()].append((identifier, entry))
    return [group for group in grouped.values() if len(group) > 1]


def _fund_product_name_needs_official_name(entry: dict) -> bool:
    """Return whether a listed ETF/MF needs the SEC's complete product name."""

    product_name = " ".join(
        str(entry.get("product_name") or "").split()
    )
    if not product_name:
        return True

    source = str(entry.get("product_name_source") or "").strip()
    if (
        source.startswith("openfigi")
        and len(product_name) == _OPENFIGI_COMPACT_NAME_LIMIT
    ):
        return True

    raw_tokens = set(re.findall(r"[A-Z0-9]+", product_name.upper()))
    return bool(
        raw_tokens & _FUND_PRODUCT_VEHICLE_TOKENS
        and len(_fund_product_name_terms(product_name)) <= 1
    )


def _apply_registry_fund_product_names(
    registry: dict[str, dict],
    *,
    openfigi_details: dict[str, dict],
    prior_registry: dict[str, dict],
    sec_fund_names: dict[str, str] | None = None,
    ambiguous_symbols: set[str] | None = None,
    force_official_symbols: set[str] | None = None,
) -> int:
    """Populate sparse display names without rebuilding registry identities."""

    for entry in registry.values():
        entry.pop("product_name", None)
        entry.pop("product_name_source", None)

    product_name_count = 0
    for identifier, entry in registry.items():
        product_name, product_name_source = _registry_fund_product_name(
            identifier=identifier,
            entry=entry,
            openfigi_detail=openfigi_details.get(identifier),
            prior_entry=prior_registry.get(identifier),
        )
        if product_name:
            entry["product_name"] = product_name
            entry["product_name_source"] = product_name_source
            product_name_count += 1

    # Prefer the SEC's official series/class name for listed ETFs and mutual
    # funds when the vendor name is absent, is only a sponsor/trust vehicle, or
    # hits OpenFIGI's fixed-width compact-name limit. Including retained recent
    # symbols avoids dropping lagging filers during quarter transitions. The
    # same resolver also disambiguates compact names shared by multiple
    # products. Collection is symbol-based so the caller can refresh all names
    # in one bounded batch.
    official_names = {
        str(symbol).strip().upper(): name
        for symbol, raw_name in (sec_fund_names or {}).items()
        if (
            _OPENFIGI_PLAIN_TICKER_RE.fullmatch(
                str(symbol).strip().upper()
            )
            and (name := normalize_security_label(raw_name))
        )
    }
    official_name_symbols = {
        str(symbol).strip().upper()
        for symbol in (force_official_symbols or set())
        if _OPENFIGI_PLAIN_TICKER_RE.fullmatch(
            str(symbol).strip().upper()
        )
    }
    for identifier, entry in registry.items():
        if (
            normalize_security_kind(
                entry.get("security_kind")
            ) not in {"ETF", "MUTUAL FUND"}
            or not _fund_product_name_needs_official_name(entry)
        ):
            continue
        symbol = _registry_fund_symbol(
            identifier=identifier,
            entry=entry,
        )
        if symbol:
            official_name_symbols.add(symbol)

    for group in _duplicate_fund_product_name_groups(registry):
        symbols = {
            symbol
            for identifier, entry in group
            if (
                normalize_security_kind(
                    entry.get("security_kind")
                ) in {"ETF", "MUTUAL FUND"}
                and (
                    symbol := _registry_fund_symbol(
                        identifier=identifier,
                        entry=entry,
                    )
                )
            )
        }
        if len(symbols) <= 1:
            continue
        official_name_symbols.update(symbols)

    if ambiguous_symbols is not None:
        ambiguous_symbols.update(official_name_symbols)
    for identifier, entry in registry.items():
        if normalize_security_kind(
            entry.get("security_kind")
        ) not in {"ETF", "MUTUAL FUND"}:
            continue
        symbol = _registry_fund_symbol(
            identifier=identifier,
            entry=entry,
        )
        if symbol not in official_name_symbols:
            continue
        official_name = official_names.get(symbol or "")
        if official_name:
            entry["product_name"] = official_name
            entry["product_name_source"] = "sec_fund_series"

    # Bloomberg product names are compact and can collapse distinct monthly
    # series or share classes to the same truncated text. Keep the CUSIP as
    # identity, and restore only missing filer class detail for those duplicate
    # display names.
    duplicate_groups: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for identifier, entry in registry.items():
        product_name = str(entry.get("product_name") or "").strip()
        source = str(entry.get("product_name_source") or "").strip()
        if product_name and source.startswith("openfigi"):
            duplicate_groups[product_name.casefold()].append(
                (identifier, entry)
            )

    for group in duplicate_groups.values():
        if len(group) <= 1:
            continue
        tickers = {
            str(entry.get("ticker") or "").strip().upper()
            for _, entry in group
            if str(entry.get("ticker") or "").strip()
        }
        if len(tickers) <= 1:
            continue
        differentiators: dict[str, str] = {}
        for identifier, entry in group:
            product_name = str(entry.get("product_name") or "").strip()
            class_name = _informative_fund_product_class(
                entry.get("dominant_class"),
                identifier=identifier,
                existing_name=product_name,
                aliases=(
                    entry.get("ticker"),
                    entry.get("security_label"),
                ),
            )
            if class_name:
                differentiators[identifier] = class_name
        # Append filer class text only when the duplicate group itself proves
        # that the classes differ. This avoids decorating every alias with
        # boilerplate such as CMN while restoring JAN/FEB monthly-series or
        # other genuinely distinguishing detail.
        if len({
            differentiators.get(identifier, "").casefold()
            for identifier, _ in group
        }) <= 1:
            continue
        for identifier, entry in group:
            class_name = differentiators.get(identifier)
            if not class_name:
                continue
            product_name = str(entry.get("product_name") or "").strip()
            source = str(entry.get("product_name_source") or "").strip()
            differentiated_name = normalize_security_label(
                f"{product_name} — {class_name}",
                identifier=identifier,
            )
            if differentiated_name:
                entry["product_name"] = differentiated_name
                entry["product_name_source"] = (
                    source if source.endswith("_class") else f"{source}_class"
                )

    # If neither SEC series data nor filer class text can distinguish a
    # remaining same-name group, append the already-visible listed symbol.
    # This is an explicit last resort: it prevents two different CUSIPs from
    # claiming the same product label without inventing unsupported terms.
    for group in _duplicate_fund_product_name_groups(registry):
        symbols = {
            identifier: _registry_fund_symbol(
                identifier=identifier,
                entry=entry,
            )
            for identifier, entry in group
        }
        distinct_symbols = {
            symbol for symbol in symbols.values() if symbol
        }
        if len(distinct_symbols) <= 1:
            continue
        for identifier, entry in group:
            symbol = symbols.get(identifier)
            if not symbol:
                continue
            product_name = str(entry.get("product_name") or "").strip()
            disambiguated_name = normalize_security_label(
                f"{product_name} — {symbol}",
                identifier=identifier,
            )
            if not disambiguated_name:
                continue
            source = str(entry.get("product_name_source") or "").strip()
            entry["product_name"] = disambiguated_name
            entry["product_name_source"] = (
                source if source.endswith("_ticker") else f"{source}_ticker"
            )
    return sum(
        bool(entry.get("product_name"))
        for entry in registry.values()
    )


def _openfigi_canonical_ticker(
    detail: dict | None,
    *,
    identifier: str,
    instrument_type: str,
    dominant_class: str | None = None,
) -> str | None:
    """Return an exchange-style OpenFIGI symbol, never a debt description."""

    if (
        not isinstance(detail, dict)
        or detail.get("status") != "matched"
        or instrument_type not in {"EQUITY", "PREF", "WARRANT"}
    ):
        return None
    market_sector = str(detail.get("marketSector") or "").strip().upper()
    if market_sector not in {"EQUITY", "PFD"}:
        return None
    ticker = str(detail.get("ticker") or "").strip().upper()
    if (
        not ticker
        or ticker == identifier
        or is_synthetic_identifier(identifier)
        or (
            instrument_type == "EQUITY"
            and _is_display_only_security_symbol(ticker, dominant_class)
        )
        or _SUSPICIOUS_TICKER_RE.search(ticker)
        or not _openfigi_plain_ticker_is_vetted(detail, ticker)
    ):
        return None
    return ticker


def _prior_registry_fund_ticker(
    prior_entry: dict | None,
    *,
    identifier: str,
    instrument_type: str,
    filer_kind: str | None = None,
) -> str | None:
    """Retain a vetted listed-fund symbol when the private cache is cold."""

    if (
        not isinstance(prior_entry, dict)
        or instrument_type != "EQUITY"
        or "ticker_collision_demoted"
        in set(prior_entry.get("sources") or [])
    ):
        return None
    prior_kind = normalize_security_kind(
        prior_entry.get("security_kind")
    )
    untyped_fund_identity = (
        prior_kind is None
        and _registry_entry_has_equity_fund_identity(prior_entry)
    )
    if (
        prior_kind not in _EQUITY_FUND_SECURITY_KINDS
        and not untyped_fund_identity
    ):
        return None
    kind_source = str(
        prior_entry.get("security_kind_source") or ""
    ).strip()
    label_source = str(prior_entry.get("label_source") or "").strip()
    sources = set(prior_entry.get("sources") or [])
    if not (
        kind_source.startswith("openfigi")
        or kind_source == "manual_verified"
        or (
            kind_source == "filer_metadata"
            and normalize_security_kind(filer_kind) == prior_kind
            and bool(sources & _FUND_IDENTITY_TICKER_SOURCES)
        )
        or untyped_fund_identity
    ) or not (
        label_source.startswith("openfigi")
        or label_source in {"canonical_ticker", "manual_verified"}
    ):
        return None
    # Display labels may come from a vendor name or description. Only a prior
    # canonical ticker is strong enough to republish as a search symbol.
    ticker = str(prior_entry.get("ticker") or "").strip().upper()
    if (
        not _OPENFIGI_PLAIN_TICKER_RE.fullmatch(ticker)
        or ticker == identifier
        or is_synthetic_identifier(identifier)
        or _SUSPICIOUS_TICKER_RE.search(ticker)
    ):
        return None
    return ticker


def _fund_identity_ticker_candidate(
    *,
    identifier: str,
    dominant_class: str,
    legacy_ticker: str | None,
    legacy_ticker_claims: Counter[str],
    openfigi_detail: dict | None,
    prior_entry: dict | None,
    filer_kind: str | None,
) -> tuple[str | None, str | None]:
    """Return a vetted symbol usable during pre-type fund classification.

    This is an evidence-only first pass. The normal registry ticker resolver
    runs again after canonical type inference and remains authoritative.
    """

    identifier = normalize_security_identifier(identifier)
    manual_ticker = str(
        MANUAL_CUSIP_TICKER_OVERRIDES.get(identifier) or ""
    ).strip().upper()
    if manual_ticker:
        return manual_ticker, "manual_override"
    openfigi_ticker = _openfigi_canonical_ticker(
        openfigi_detail,
        identifier=identifier,
        instrument_type="EQUITY",
        dominant_class=dominant_class,
    )
    if openfigi_ticker:
        return openfigi_ticker, "openfigi_plain_ticker"
    prior_ticker = _prior_registry_fund_ticker(
        prior_entry,
        identifier=identifier,
        instrument_type="EQUITY",
        filer_kind=filer_kind,
    )
    if prior_ticker:
        return prior_ticker, "openfigi_prior_registry_ticker"
    if _allow_legacy_registry_ticker(
        cusip=identifier,
        ticker=legacy_ticker,
        instrument_type="EQUITY",
        legacy_equity_claims=legacy_ticker_claims,
        dominant_class=dominant_class,
        openfigi_detail=openfigi_detail,
    ):
        return str(legacy_ticker).strip().upper(), "cusip_map_vetted"
    return None, None


def _registry_security_label(
    *,
    identifier: str,
    entry: dict,
    openfigi_detail: dict | None,
    prior_entry: dict | None,
    legacy_openfigi_label: str | None,
) -> tuple[str, str]:
    """Choose one universal label without changing ticker/search semantics."""

    verified_manual_label = normalize_security_label(
        MANUAL_VERIFIED_SECURITY_LABEL_OVERRIDES.get(identifier),
        identifier=identifier,
    )
    if verified_manual_label:
        return verified_manual_label, "manual_verified"

    manual_label = normalize_security_label(
        MANUAL_SECURITY_LABEL_OVERRIDES.get(identifier),
        identifier=identifier,
    )
    if manual_label:
        source = (
            "synthetic_identifier"
            if is_synthetic_identifier(identifier)
            else "historical_invalid_identifier"
        )
        return manual_label, source

    ticker_collision_demoted = (
        "ticker_collision_demoted" in set(entry.get("sources") or [])
    )
    openfigi_fund_conflict = _openfigi_fund_conflicts_with_filer_common(
        entry,
        openfigi_detail,
    )
    if openfigi_fund_conflict:
        current_openfigi_label = None
        current_openfigi_source = "filer_common_conflict"
    elif ticker_collision_demoted and isinstance(openfigi_detail, dict):
        current_openfigi_label = normalize_security_label(
            openfigi_detail.get("name"),
            identifier=identifier,
        )
        current_openfigi_source = "openfigi_collision_name"
    else:
        current_openfigi_label = _openfigi_security_label(
            openfigi_detail,
            identifier,
        )
        current_openfigi_source = "openfigi"
    current_openfigi_note_label = normalize_note_security_label(
        current_openfigi_label
    )
    if current_openfigi_note_label:
        return current_openfigi_note_label, current_openfigi_source

    prior_openfigi_label = None
    if (
        not ticker_collision_demoted
        and not openfigi_fund_conflict
        and isinstance(prior_entry, dict)
        and str(prior_entry.get("label_source") or "").startswith("openfigi")
    ):
        prior_openfigi_label = normalize_security_label(
            prior_entry.get("security_label"),
            identifier=identifier,
        )
        prior_openfigi_note_label = normalize_note_security_label(
            prior_openfigi_label
        )
        if prior_openfigi_note_label:
            return prior_openfigi_note_label, "openfigi_prior_registry"

    if isinstance(prior_entry, dict) and (
        "note_label_vetted" in set(prior_entry.get("sources") or [])
    ):
        prior_note_label = normalize_security_label(
            normalize_note_security_label(prior_entry.get("ticker")),
            identifier=identifier,
        )
        if prior_note_label:
            return prior_note_label, "openfigi_prior_registry"

    migrated_legacy_label = normalize_security_label(
        None if openfigi_fund_conflict else legacy_openfigi_label,
        identifier=identifier,
    )
    if migrated_legacy_label:
        return migrated_legacy_label, "openfigi_legacy_ticker"

    if current_openfigi_label:
        return current_openfigi_label, current_openfigi_source

    if prior_openfigi_label:
        return prior_openfigi_label, "openfigi_prior_registry"

    ticker_label = normalize_security_label(
        entry.get("ticker"),
        identifier=identifier,
    )
    if ticker_label:
        return ticker_label, "canonical_ticker"

    dominant_issuer = (
        normalize_security_label(
            entry.get("name"),
            identifier=identifier,
        )
        or entry.get("dominant_issuer")
    )
    dominant_class = entry.get("dominant_class")
    fallback = compose_security_label(
        dominant_issuer,
        dominant_class,
        entry.get("type"),
        identifier=identifier,
    )
    issuer_label = normalize_security_label(
        dominant_issuer,
        identifier=identifier,
    )
    type_fallback = (
        f"{normalize_instrument_type(entry.get('type'))} SECURITY"
    )
    if is_synthetic_identifier(identifier) and fallback == type_fallback:
        instrument_type = normalize_instrument_type(entry.get("type"))
        return (
            f"UNIDENTIFIED {instrument_type} SECURITY",
            "synthetic_identifier",
        )
    entry_sources = set(entry.get("sources") or [])
    if issuer_label and fallback.startswith(f"{issuer_label} — "):
        if "manual_name_override" in entry_sources:
            fallback_source = "manual_name_class"
        elif "sec_title" in entry_sources:
            fallback_source = "sec_title_class"
        else:
            fallback_source = "filer_issuer_class"
    elif issuer_label and fallback == issuer_label:
        if "manual_name_override" in entry_sources:
            fallback_source = "manual_name"
        elif "sec_title" in entry_sources:
            fallback_source = "sec_title"
        else:
            fallback_source = "filer_issuer"
    elif fallback != type_fallback:
        fallback_source = "filer_class"
    else:
        fallback_source = "instrument_type"
    return fallback, fallback_source


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


def build_cusip_registry(
    *,
    full_refresh: bool = False,
    company_ticker_data: dict | list | None = None,
    refresh_official_fund_names: bool | None = None,
) -> dict:
    """Build the canonical per-CUSIP registry from current fund-file evidence.

    Sources, by priority:
      ticker   manual overrides -> SEC-validated normalization of an existing
               cusip_map ticker -> existing vetted cusip_map ticker
      name     SEC company_tickers title when ticker is known and listed
               -> dominant filer issuer name (fallback)
      type     put_call majority if set, else _classify_holding against
               the dominant (class, issuer) pair
      label    current OpenFIGI description -> prior OpenFIGI-backed registry
               label -> canonical ticker -> dominant filer issuer/class
      kind     current OpenFIGI type -> prior OpenFIGI-backed display kind;
               exact/manual BOND evidence also canonicalizes type to NOTE
      product  display-only ETF/ETN/mutual/closed-end fund name; preserves a
               full SEC/filer name, uses CUSIP-specific OpenFIGI metadata,
               resolves incomplete ETF/mutual-fund names through official SEC
               series/class data, then falls back to informative filer class
               text
    """
    log.info("Building CUSIP registry...")
    if not FUNDS_DIR.exists():
        log.info("  no funds directory; skipping registry build")
        return {}

    if refresh_official_fund_names is None:
        # Preserve the side-effect-free injected-data path used by callers
        # that provide fixtures, while normal production builds refresh SEC
        # series/class metadata.
        refresh_official_fund_names = company_ticker_data is None
    prior_registry = load_cusip_registry()
    openfigi_details = load_openfigi_details()
    evidence = _aggregate_cusip_evidence()
    log.info(f"  collected evidence for {len(evidence)} CUSIPs from fund files")

    cusip_map = load_cusip_map()
    if company_ticker_data is None:
        company_ticker_data = _load_company_tickers_data()
    sec_titles, name_to_ticker = _company_ticker_indexes(company_ticker_data)
    log.info(
        f"  ticker source: {len(cusip_map)} cached mappings; "
        f"SEC title index has {len(sec_titles)} tickers"
    )
    # full_refresh is accepted for API parity with rebuild_tickers_in_place;
    # label fallback deliberately retains prior OpenFIGI-backed descriptions
    # when a fresh lookup has no safe descriptive result.
    _ = full_refresh

    # The pre-type fund-identity pass must be at least as conservative as the
    # final ticker resolver. Count every current raw-map claim so a duplicate
    # symbol cannot be used to turn NOTE/PREF evidence into Equity.
    all_legacy_ticker_claims: Counter[str] = Counter(
        raw_ticker
        for cusip in evidence
        if (
            raw_ticker := str(cusip_map.get(cusip) or "").strip().upper()
        )
    )

    dominant_issuers: dict[str, str] = {}
    dominant_classes: dict[str, str] = {}
    filer_kinds: dict[str, str | None] = {}
    instrument_types: dict[str, str] = {}
    legacy_openfigi_labels: dict[str, str] = {}
    for cusip, rec in evidence.items():
        dominant_issuer = ""
        if rec["issuer_value"]:
            dominant_issuer = max(
                rec["issuer_value"].items(), key=lambda kv: kv[1]
            )[0]
        dominant_class = ""
        if rec["class_value"]:
            dominant_class = max(
                rec["class_value"].items(), key=lambda kv: kv[1]
            )[0]
        dominant_issuers[cusip] = dominant_issuer
        dominant_classes[cusip] = dominant_class
        base_filer_probe = {
            "name": dominant_issuer,
            "dominant_issuer": dominant_issuer,
            "dominant_class": dominant_class,
        }
        prior_entry = prior_registry.get(cusip)
        prior_filer_probe = dict(base_filer_probe)
        if isinstance(prior_entry, dict):
            prior_sources = set(prior_entry.get("sources") or [])
            if (
                prior_entry.get("ticker")
                and "ticker_collision_demoted" not in prior_sources
                and bool(prior_sources & _FUND_IDENTITY_TICKER_SOURCES)
            ):
                # Reapply a prior vetted symbol only as evidence alongside
                # current issuer/class metadata. This lets guarded mixed-series
                # trusts prove ETF identity before raw NOTE/PREF parsing chooses
                # the canonical instrument type.
                prior_filer_probe["ticker"] = prior_entry["ticker"]
                prior_filer_probe["sources"] = sorted(prior_sources)
        prior_filer_kind = _filer_security_kind(prior_filer_probe)
        legacy_ticker = str(
            cusip_map.get(cusip) or ""
        ).strip().upper() or None
        identity_ticker, identity_ticker_source = (
            _fund_identity_ticker_candidate(
                identifier=cusip,
                dominant_class=dominant_class,
                legacy_ticker=legacy_ticker,
                legacy_ticker_claims=all_legacy_ticker_claims,
                openfigi_detail=openfigi_details.get(cusip),
                prior_entry=prior_entry,
                filer_kind=prior_filer_kind,
            )
        )
        filer_probe = dict(base_filer_probe)
        if identity_ticker:
            filer_probe["ticker"] = identity_ticker
            filer_probe["sources"] = [identity_ticker_source]
        filer_kind = _filer_security_kind(filer_probe)
        filer_kinds[cusip] = filer_kind
        filer_fund_identity = _entry_has_trusted_fund_symbol_evidence(
            filer_probe
        )
        instrument_types[cusip] = _registry_type_from_evidence(
            rec,
            openfigi_details.get(cusip),
            identifier=cusip,
            prior_entry=prior_entry,
            filer_kind=filer_kind,
            filer_fund_identity=filer_fund_identity,
        )

    legacy_equity_claims: Counter[str] = Counter()
    for cusip, ticker in cusip_map.items():
        raw_cusip = str(cusip or "").strip().upper()
        raw_ticker = str(ticker or "").strip().upper()
        if instrument_types.get(raw_cusip) == "EQUITY" and raw_ticker:
            legacy_equity_claims[raw_ticker] += 1

    registry: dict[str, dict] = {}
    counts = {
        "with_ticker": 0,
        "with_sec_name": 0,
        "fallback_issuer_name": 0,
        "null_ticker": 0,
        "by_type": defaultdict(int),
    }

    for cusip, rec in evidence.items():
        dominant_issuer = dominant_issuers.get(cusip, "")
        dominant_class = dominant_classes.get(cusip, "")
        instrument_type = instrument_types[cusip]
        legacy_ticker = str(cusip_map.get(cusip) or "").strip().upper() or None
        if instrument_type == "NOTE":
            legacy_note_label = normalize_note_security_label(legacy_ticker)
            if legacy_note_label:
                legacy_openfigi_labels[cusip] = legacy_note_label

        ticker = None
        ticker_source = None
        source_ticker = None
        if cusip in MANUAL_CUSIP_TICKER_OVERRIDES:
            ticker = MANUAL_CUSIP_TICKER_OVERRIDES[cusip]
            ticker_source = "manual_override"
        elif (
            instrument_type == "EQUITY"
            and not is_synthetic_identifier(cusip)
            and (
                normalized_alias := _validated_sec_ticker_alias(
                    legacy_ticker,
                    dominant_issuer,
                    sec_titles,
                )
            )
        ):
            ticker = normalized_alias
            ticker_source = "sec_validated_ticker_alias"
            source_ticker = legacy_ticker
        elif (
            openfigi_ticker := _openfigi_canonical_ticker(
                openfigi_details.get(cusip),
                identifier=cusip,
                instrument_type=instrument_type,
                dominant_class=dominant_class,
            )
        ):
            ticker = openfigi_ticker
            ticker_source = "openfigi_plain_ticker"
        elif (
            prior_fund_ticker := _prior_registry_fund_ticker(
                prior_registry.get(cusip),
                identifier=cusip,
                instrument_type=instrument_type,
                filer_kind=filer_kinds.get(cusip),
            )
        ):
            ticker = prior_fund_ticker
            ticker_source = "openfigi_prior_registry_ticker"
        elif _allow_legacy_registry_ticker(
            cusip=cusip,
            ticker=legacy_ticker,
            instrument_type=instrument_type,
            legacy_equity_claims=legacy_equity_claims,
            dominant_class=dominant_class,
            openfigi_detail=openfigi_details.get(cusip),
        ):
            ticker = legacy_ticker
            ticker_source = "cusip_map_vetted"

        if ticker:
            counts["with_ticker"] += 1
        else:
            counts["null_ticker"] += 1

        # Canonical name: prefer the SEC title for the resolved ticker;
        # fall back to the dominant filer-typed issuer when we can't
        # reach SEC (ADRs, foreign issuers, options, notes).
        sources: list[str] = []
        name = ""
        if cusip in MANUAL_CUSIP_NAME_OVERRIDES:
            name = MANUAL_CUSIP_NAME_OVERRIDES[cusip]
            counts["fallback_issuer_name"] += 1
            sources.append("manual_name_override")
        elif ticker and ticker in sec_titles:
            name = sec_titles[ticker]
            counts["with_sec_name"] += 1
            sources.append("sec_title")
        elif (
            dominant_issuer_label := normalize_security_label(
                dominant_issuer,
                identifier=cusip,
            )
        ):
            name = dominant_issuer_label
            counts["fallback_issuer_name"] += 1
            sources.append("filer_dominant")

        if ticker_source:
            sources.append(ticker_source)

        counts["by_type"][instrument_type] += 1

        registry_entry = {
            "ticker": ticker,
            "name": name,
            "type": instrument_type,
            "dominant_issuer": dominant_issuer,
            "dominant_class": dominant_class,
            "holder_count": len(rec["holder_ciks"]),
            "total_value": rec["total_value"],
            "first_seen": rec["first_seen"],
            "last_seen": rec["last_seen"],
            "sources": sources,
        }
        if source_ticker:
            registry_entry["source_ticker"] = source_ticker
        registry[cusip] = registry_entry

    deduplicated_tickers = _deduplicate_registry_equity_tickers(registry)
    derived_tickers, linked_underlyings = _apply_option_underlying_derivations(
        registry,
        name_to_ticker=name_to_ticker,
        sec_titles=sec_titles,
    )
    consensus_tickers = _backfill_equity_tickers_from_option_consensus(
        registry,
        sec_titles=sec_titles,
        openfigi_details=openfigi_details,
        prior_registry=prior_registry,
    )
    counts["with_ticker"] = sum(
        bool(entry.get("ticker")) for entry in registry.values()
    )
    counts["null_ticker"] = len(registry) - counts["with_ticker"]
    for cusip, entry in registry.items():
        security_label, label_source = _registry_security_label(
            identifier=cusip,
            entry=entry,
            openfigi_detail=openfigi_details.get(cusip),
            prior_entry=prior_registry.get(cusip),
            legacy_openfigi_label=legacy_openfigi_labels.get(cusip),
        )
        entry["security_label"] = security_label
        entry["label_source"] = label_source
        security_kind, security_kind_source = _registry_security_kind(
            identifier=cusip,
            openfigi_detail=openfigi_details.get(cusip),
            prior_entry=prior_registry.get(cusip),
            entry=entry,
        )
        if security_kind:
            entry["security_kind"] = security_kind
            entry["security_kind_source"] = security_kind_source

    sec_fund_names = _sec_fund_name_map(load_sec_fund_name_cache())
    for identifier, prior_entry in prior_registry.items():
        prior_source = str(
            prior_entry.get("product_name_source") or ""
        ).strip()
        prior_name = normalize_security_label(
            prior_entry.get("product_name"),
            identifier=identifier,
        )
        prior_symbol = _registry_fund_symbol(
            identifier=identifier,
            entry=prior_entry,
        )
        if (
            prior_source.startswith("sec_fund_")
            and not prior_source.endswith("_ticker")
            and prior_name
            and prior_symbol
        ):
            sec_fund_names.setdefault(prior_symbol, prior_name)

    prior_sec_fund_metadata: dict[str, tuple[str, str, str]] = {}
    for identifier, entry in registry.items():
        prior_entry = prior_registry.get(identifier)
        if (
            not isinstance(prior_entry, dict)
            or normalize_security_kind(entry.get("security_kind"))
            not in {"ETF", "MUTUAL FUND"}
        ):
            continue
        prior_source = str(
            prior_entry.get("product_name_source") or ""
        ).strip()
        prior_name = normalize_security_label(
            prior_entry.get("product_name"),
            identifier=identifier,
        )
        current_symbol = _registry_fund_symbol(
            identifier=identifier,
            entry=entry,
        )
        prior_symbol = _registry_fund_symbol(
            identifier=identifier,
            entry=prior_entry,
        )
        if (
            prior_source.startswith("sec_fund_")
            and prior_name
            and current_symbol
            and current_symbol == prior_symbol
        ):
            prior_sec_fund_metadata[identifier] = (
                current_symbol,
                prior_name,
                prior_source,
            )

    forced_official_symbols = {
        symbol
        for symbol, _name, _source in prior_sec_fund_metadata.values()
    }
    official_fund_name_symbols: set[str] = set()
    product_name_count = _apply_registry_fund_product_names(
        registry,
        openfigi_details=openfigi_details,
        prior_registry=prior_registry,
        sec_fund_names=sec_fund_names,
        ambiguous_symbols=official_fund_name_symbols,
        force_official_symbols=(
            forced_official_symbols
            if refresh_official_fund_names
            else None
        ),
    )
    if not refresh_official_fund_names:
        # The snapshot registry is the durability floor when a daily runner
        # has a cold or partial private cache. Restore only exact CUSIP + symbol
        # matches; full refreshes deliberately skip this guard so SEC-confirmed
        # renames, liquidations, and symbol reuse can replace stale metadata.
        for identifier, (
            _symbol,
            prior_name,
            prior_source,
        ) in prior_sec_fund_metadata.items():
            registry[identifier]["product_name"] = prior_name
            registry[identifier]["product_name_source"] = prior_source
        product_name_count = sum(
            bool(entry.get("product_name")) for entry in registry.values()
        )
    if refresh_official_fund_names:
        if official_fund_name_symbols:
            # The current SEC symbol -> CIK/series/class join is authoritative
            # for incomplete, ambiguous, and previously SEC-backed fund names.
            # Drop prior values first so renamed, liquidated, or reused symbols
            # cannot retain stale product names.
            refreshed_sec_fund_names = {
                symbol: name
                for symbol, name in sec_fund_names.items()
                if symbol not in official_fund_name_symbols
            }
            refreshed_sec_fund_names.update(
                refresh_sec_fund_names(official_fund_name_symbols)
            )
            if refreshed_sec_fund_names != sec_fund_names:
                product_name_count = _apply_registry_fund_product_names(
                    registry,
                    openfigi_details=openfigi_details,
                    prior_registry=prior_registry,
                    sec_fund_names=refreshed_sec_fund_names,
                    force_official_symbols=forced_official_symbols,
                )
    save_cusip_registry(registry)

    by_type = ", ".join(
        f"{n} {t}" for t, n in sorted(counts["by_type"].items(), key=lambda kv: -kv[1])
    )
    log.info(
        f"  wrote registry: {len(registry)} entries "
        f"({counts['with_ticker']} with ticker, "
        f"{counts['with_sec_name']} SEC-named, "
            f"{counts['fallback_issuer_name']} filer-named, "
            f"{counts['null_ticker']} null ticker)"
    )
    if derived_tickers or linked_underlyings:
        log.info(
            f"  option derivations: {derived_tickers} tickers backfilled, "
            f"{linked_underlyings} underlying links added"
        )
    if consensus_tickers:
        log.info(
            "  option-family consensus: "
            f"{consensus_tickers} equity tickers backfilled"
        )
    if deduplicated_tickers:
        log.info(
            f"  demoted {deduplicated_tickers} duplicate canonical equity "
            "ticker claim(s)"
        )
    log.info(
        f"  published {product_name_count} descriptive fund product name(s)"
    )
    log.info(f"  types: {by_type}")
    return registry


def validate_cusip_registry() -> list[str]:
    """Return human-readable warnings about the snapshot registry copies."""
    issues: list[str] = []
    registry = load_cusip_registry()
    if not registry:
        issues.append("registry is empty")
        return issues

    if not LEGACY_CUSIP_REGISTRY_PATH.exists():
        issues.append(
            f"snapshot data copy missing at {LEGACY_CUSIP_REGISTRY_PATH.name}"
        )
    else:
        try:
            with open(LEGACY_CUSIP_REGISTRY_PATH) as f:
                snapshot_registry = json.load(f)
        except json.JSONDecodeError:
            issues.append(
                f"snapshot data copy {LEGACY_CUSIP_REGISTRY_PATH.name} is invalid JSON"
            )
        else:
            if snapshot_registry != registry:
                issues.append(
                    f"snapshot data copy {LEGACY_CUSIP_REGISTRY_PATH.name} differs from cache registry"
                )

    missing_name = sum(1 for e in registry.values() if not e.get("name"))
    missing_type = sum(1 for e in registry.values() if not e.get("type"))
    if missing_name:
        issues.append(f"{missing_name}/{len(registry)} entries have no name")
    if missing_type:
        issues.append(f"{missing_type}/{len(registry)} entries have no type")

    raw_legacy_sources = sorted(
        cusip for cusip, entry in registry.items()
        if "cusip_map" in set(entry.get("sources") or [])
    )
    if raw_legacy_sources:
        issues.append(
            f"{len(raw_legacy_sources)} entries still record raw cusip_map as a source; "
            f"samples: {raw_legacy_sources[:5]}"
        )

    vetted_claims: Counter[str] = Counter(
        str(entry.get("ticker") or "").strip().upper()
        for entry in registry.values()
        if (
            entry.get("type") == "EQUITY"
            and "cusip_map_vetted" in set(entry.get("sources") or [])
            and entry.get("ticker")
        )
    )
    bad_vetted = []
    for cusip, entry in registry.items():
        sources = set(entry.get("sources") or [])
        if "cusip_map_vetted" not in sources:
            continue
        if not _allow_legacy_registry_ticker(
            cusip=cusip,
            ticker=entry.get("ticker"),
            instrument_type=normalize_instrument_type(entry.get("type")),
            legacy_equity_claims=vetted_claims,
            dominant_class=entry.get("dominant_class"),
        ):
            bad_vetted.append(cusip)
    if bad_vetted:
        issues.append(
            f"{len(bad_vetted)} vetted legacy ticker entries failed plausibility checks; "
            f"samples: {bad_vetted[:5]}"
        )

    bad_note_labels = sorted(
        cusip
        for cusip, entry in registry.items()
        if (
            (
                normalize_instrument_type(entry.get("type")) == "NOTE"
                and entry.get("ticker")
                and (
                    normalize_note_security_label(entry.get("ticker"))
                    != entry.get("ticker")
                    or "note_label_vetted"
                    not in set(entry.get("sources") or [])
                )
            )
            or (
                "note_label_vetted" in set(entry.get("sources") or [])
                and (
                    normalize_instrument_type(entry.get("type")) != "NOTE"
                    or normalize_note_security_label(entry.get("ticker"))
                    != entry.get("ticker")
                )
            )
            or (
                normalize_instrument_type(entry.get("type")) != "NOTE"
                and normalize_note_security_label(entry.get("ticker"))
            )
        )
    )
    if bad_note_labels:
        issues.append(
            f"{len(bad_note_labels)} vetted note labels failed format/type checks; "
            f"samples: {bad_note_labels[:5]}"
        )

    manual_kind_mismatches = sorted(
        f"{cusip}:{expected_kind}"
        for cusip, raw_expected_kind in MANUAL_SECURITY_KIND_OVERRIDES.items()
        if cusip in registry
        and (
            (expected_kind := normalize_security_kind(raw_expected_kind))
            != normalize_security_kind(
                registry[cusip].get("security_kind")
            )
            or str(
                registry[cusip].get("security_kind_source") or ""
            ).strip()
            != "manual_verified"
        )
    )
    if manual_kind_mismatches:
        issues.append(
            f"{len(manual_kind_mismatches)} entries differ from manual "
            f"security-kind proof; samples: {manual_kind_mismatches[:5]}"
        )

    validated_aliases: set[str] = set()
    malformed_aliases: list[str] = []
    for cusip, entry in registry.items():
        sources = set(entry.get("sources") or [])
        if "sec_validated_ticker_alias" not in sources:
            continue
        ticker = str(entry.get("ticker") or "").strip().upper()
        reconstructed = _validated_sec_ticker_alias(
            entry.get("source_ticker"),
            entry.get("dominant_issuer"),
            {ticker: str(entry.get("name") or "")},
        )
        if ticker and reconstructed == ticker:
            validated_aliases.add(cusip)
        else:
            malformed_aliases.append(cusip)
    if malformed_aliases:
        issues.append(
            f"{len(malformed_aliases)} SEC-validated ticker aliases lost their proof; "
            f"samples: {malformed_aliases[:5]}"
        )

    # A correct registry normally has one EQUITY CUSIP per ticker.  A narrow
    # exception covers SEC-validated aliases for the same issuer: historical
    # CUSIPs can legitimately converge on one current ticker.  We still flag
    # the group if any claim names a different issuer, so an unrelated
    # collision cannot hide behind one approved alias.
    by_ticker: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for cusip, entry in registry.items():
        ticker = entry.get("ticker")
        if ticker and entry.get("type") == "EQUITY":
            by_ticker[ticker].append((cusip, entry))

    collisions: dict[str, list[str]] = {}
    for ticker, claims in by_ticker.items():
        if len(claims) <= 1:
            continue
        alias_claims = [
            entry for cusip, entry in claims if cusip in validated_aliases
        ]
        alias_norms = {
            normalize_name(entry.get("name") or "") for entry in alias_claims
        }
        issuer_norms = {
            normalize_name(entry.get("dominant_issuer") or entry.get("name") or "")
            for _cusip, entry in claims
        }
        same_sec_confirmed_issuer = (
            alias_claims
            and len(alias_norms) == 1
            and "" not in alias_norms
            and issuer_norms == alias_norms
        )
        if not same_sec_confirmed_issuer:
            collisions[ticker] = [cusip for cusip, _entry in claims]
    if collisions:
        sample = sorted(collisions.items())[:5]
        issues.append(
            f"{len(collisions)} equity tickers still claimed by multiple CUSIPs; "
            f"samples: {sample}"
        )

    current_cusips = set(_aggregate_cusip_evidence())
    missing_cusips = sorted(current_cusips - set(registry))
    if missing_cusips:
        issues.append(
            f"{len(missing_cusips)} fund-file CUSIPs missing from registry; "
            f"samples: {missing_cusips[:5]}"
        )

    apple = registry.get("037833100")
    if apple and apple.get("ticker") != "AAPL":
        issues.append("037833100 should resolve to ticker AAPL")

    typo = registry.get("378331003")
    if typo and typo.get("ticker"):
        issues.append("378331003 should stay null-ticker (not inherit AAPL)")

    for option_cusip in ("99QA1RO84", "7769499XX", "7879869CC"):
        entry = registry.get(option_cusip)
        if entry and entry.get("ticker") != "AAPL":
            issues.append(
                f"{option_cusip} should derive ticker AAPL from the underlying option text"
            )

    return issues


@_serialize_pipeline_maintenance
def canonicalize_fund_files() -> int:
    """Normalize row type and refresh display metadata in every fund file.

    Phase 3: filer-typed strings are no longer trusted for display. After
    this pass, every holding's ticker and issuer are registry-derived while
    holding type remains row-derived:

      ticker         registry.ticker (canonical, possibly null for
                     uncovered CUSIPs — don't fall back to filer string)
      issuer         registry.name (SEC title or filer-dominant) when
                     registry has a name; otherwise retains only a safe
                     non-identifier filer label and clears raw-CUSIP text
      holding_type   hash-v2 parser identity when present; otherwise
                     classify_saved_holding(h) from put_call + class, NOT
                     registry.type (a CUSIP can host both equity and option
                     holdings on different rows)

    Fields left alone: cusip, class (raw filer text for audit), put_call,
    value, shares, shares_imputed.

    Stale legacy field `option_type` is removed when present. Registry data is
    never allowed to rewrite public position identity. Must run AFTER
    build_cusip_registry."""
    log.info("Canonicalizing holding types and registry display metadata...")
    if not FUNDS_DIR.exists():
        log.info("  no funds directory; skipping canonicalization")
        return 0

    registry = load_cusip_registry()
    if not registry:
        log.info("  no registry; normalizing holding types without display refresh")

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
                new_type = _canonical_holding_type_for_quarter(q, h)
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
                    continue

                new_ticker = display_ticker_for_holding_type(
                    reg_entry.get("ticker"),
                    new_type,
                )
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
            "(type normalized; display refresh skipped)"
        )
    if ambiguous_legacy_options:
        log.warning(
            f"  retained {ambiguous_legacy_options} legacy option labels "
            "without raw put_call or decisive parser-era evidence"
        )
    return updated


@_serialize_pipeline_maintenance
def upgrade_composition_hashes_in_place() -> int:
    """Bind current parser-backed security identity into retained hashes.

    Older composition-v2 quarters were created before holding type became part
    of the immutable composition hash. They can be upgraded without another
    SEC fetch only when the retained legacy hash still verifies and every
    applied source carries the current parser identity marker. Anything else
    is left untouched so the release validator can fail closed or the normal
    replay queue can repair it.
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
                or quarter.get("composition_hash_version", 1) != 1
            ):
                continue

            holdings = quarter.get("holdings")
            applied_accessions = quarter.get("applied_accessions")
            source_filings = quarter.get("source_filings")
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
            legacy_hash = calculate_composition_hash(
                **hash_args,
                composition_hash_version=1,
            )
            if quarter.get("composition_hash") != legacy_hash:
                continue

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
) -> None:
    """Refresh all registry-driven derived artifacts from current fund files."""
    registry = build_cusip_registry(
        full_refresh=full_refresh,
        company_ticker_data=company_ticker_data,
        refresh_official_fund_names=refresh_official_fund_names,
    )
    if isinstance(registry, dict):
        write_security_labels(registry)
    registry_issues = validate_cusip_registry()
    critical_registry_issues = [
        issue for issue in registry_issues
        if (
            "SEC-validated ticker aliases lost their proof" in issue
            or "equity tickers still claimed by multiple CUSIPs" in issue
        )
    ]
    if critical_registry_issues:
        raise FundDataError(
            "registry identity gate failed: "
            + "; ".join(critical_registry_issues)
        )
    for issue in registry_issues:
        log.warning(f"  registry: {issue}")
    canonicalize_fund_files()
    repair_zero_share_holdings_in_place()
    upgrade_composition_hashes_in_place()
    regenerate_stock_files_and_index()
    write_ticker_health_report()


def retry_unresolved_cusips() -> int:
    """Re-resolve CUSIPs flagged in data/ticker_health.json.

    Cheaper than `--full-cusip-refresh`: daily runs prioritize recent
    Equity/Preferred/Warrant rows plus suspicious and option-family records.
    Stable debt and direct-option no-matches remain observable in the report
    and are retried by the weekly full refresh. Results that pass the health
    check update the private cache before rebuild_tickers_in_place propagates
    them into fund files and the registry rebuild runs.

    Returns the number of CUSIPs whose mapping actually changed."""
    log.info("Retrying previously-flagged CUSIPs...")
    if not TICKER_HEALTH_PATH.exists():
        log.info("  no ticker_health.json yet; nothing to retry")
        return 0
    try:
        with open(TICKER_HEALTH_PATH) as f:
            report = json.load(f)
    except json.JSONDecodeError:
        log.warning("  ticker_health.json is invalid JSON; skipping retry")
        return 0

    buckets = report.get("buckets", {}) or {}
    registry = load_cusip_registry()
    priority: set[str] = set()
    priority_types: dict[str, str] = {}
    option_family_artifacts: set[str] = set()
    unresolved_entries = list(buckets.get("unresolved", []) or [])
    observed_dates = sorted({
        str(entry.get("last_seen") or "")
        for entry in unresolved_entries
        if re.fullmatch(
            r"\d{4}-\d{2}-\d{2}",
            str(entry.get("last_seen") or ""),
        )
    })
    recent_dates = set(observed_dates[-2:])
    daily_types = {"EQUITY", "PREF", "WARRANT"}
    flagged_count = 0
    for name in (
        "unresolved",
        "suspicious_symbol",
        "option_family_artifact",
    ):
        for entry in buckets.get(name, []) or []:
            flagged_count += 1
            cusip = str(entry.get("cusip") or "").strip().upper()
            if not cusip:
                continue
            raw_type = (
                entry.get("instrument_type")
                or (registry.get(cusip) or {}).get("type")
            )
            instrument_type = (
                normalize_instrument_type(raw_type)
                if raw_type
                else None
            )
            last_seen = str(entry.get("last_seen") or "")
            legacy_report_entry = not last_seen or instrument_type is None
            should_retry = (
                name != "unresolved"
                or legacy_report_entry
                or (
                    instrument_type in daily_types
                    and (not recent_dates or last_seen in recent_dates)
                )
                or cusip in MANUAL_CUSIP_TICKER_OVERRIDES
            )
            if not should_retry:
                continue
            priority.add(cusip)
            if name == "option_family_artifact":
                option_family_artifacts.add(cusip)
            if instrument_type:
                priority_types[cusip] = instrument_type

    if not priority:
        log.info("  no daily-priority CUSIPs flagged; nothing to retry")
        return 0

    deferred = max(0, flagged_count - len(priority))
    if deferred:
        log.info(
            "  deferred %s stable debt/option or stale specialized CUSIP(s) "
            "to the weekly full refresh",
            deferred,
        )

    # Manual overrides win over OpenFIGI — honor them before spending requests.
    overrides = manual_cusip_ticker_overrides(priority)
    to_resolve = sorted(priority - set(overrides))
    log.info(
        f"  retrying {len(to_resolve)} CUSIPs via OpenFIGI "
        f"({len(overrides)} covered by manual overrides)"
    )

    figi_map = resolve_cusips_via_openfigi(to_resolve) if to_resolve else {}

    cusip_map = load_cusip_map()
    resolved_equity_family_tickers: dict[str, set[str]] = defaultdict(set)
    for registry_cusip, registry_entry in registry.items():
        registry_ticker = str(
            registry_entry.get("ticker") or ""
        ).strip().upper()
        if (
            len(registry_cusip) >= 6
            and registry_ticker
            and normalize_instrument_type(registry_entry.get("type"))
            == "EQUITY"
        ):
            resolved_equity_family_tickers[registry_cusip[:6]].add(
                registry_ticker
            )
    removed_family_conflicts = 0
    changed_cusips: set[str] = set()
    for cusip in option_family_artifacts - set(overrides):
        cached_ticker = str(cusip_map.get(cusip) or "").strip().upper()
        if (
            cached_ticker
            and cached_ticker
            in resolved_equity_family_tickers.get(cusip[:6], set())
        ):
            del cusip_map[cusip]
            removed_family_conflicts += 1
            changed_cusips.add(cusip)
    reassigned = 0
    still_bad = 0
    family_conflicts = 0
    for cusip, new_ticker in {**figi_map, **overrides}.items():
        instrument_type = priority_types.get(cusip)
        if instrument_type == "NOTE":
            normalized_note_label = normalize_note_security_label(new_ticker)
            if normalized_note_label:
                new_ticker = normalized_note_label
        # Plain equity-like tickers and strict note labels are safe to retain.
        # Other bond/preferred/warrant strings remain flagged for review.
        if _classify_ticker_health(cusip, new_ticker, instrument_type):
            still_bad += 1
            continue
        normalized_new_ticker = str(new_ticker or "").strip().upper()
        if (
            cusip in option_family_artifacts
            and cusip not in overrides
            and normalized_new_ticker
            in resolved_equity_family_tickers.get(cusip[:6], set())
        ):
            # OpenFIGI can return the common-share ticker for an option-like
            # identifier. Keep the resolver details as evidence, but never let
            # a structurally quarantined sibling compete for the common
            # equity's canonical ticker.
            still_bad += 1
            family_conflicts += 1
            continue
        if cusip_map.get(cusip) != new_ticker:
            cusip_map[cusip] = new_ticker
            reassigned += 1
            changed_cusips.add(cusip)

    if reassigned or removed_family_conflicts:
        save_cusip_map(cusip_map)
        if reassigned:
            log.info(
                f"  updated {reassigned} CUSIP mappings; rewriting fund files"
            )
        if removed_family_conflicts:
            log.info(
                "  removed %s cached option-family mapping(s) that reused "
                "an existing sibling equity ticker",
                removed_family_conflicts,
            )
    else:
        log.info("  retry produced no new resolutions")
    if still_bad:
        log.info(
            f"  {still_bad} CUSIPs still resolve to suspicious/unresolved symbols "
            "after retry — will stay flagged in ticker_health.json"
        )
    if family_conflicts:
        log.info(
            "  withheld %s option-family resolution(s) that reused an "
            "existing sibling equity ticker",
            family_conflicts,
        )
    return len(changed_cusips)


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
    log.info(f"processed {processed} new filings for CIK {cik}")
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
        # does not repeat SEC/OpenFIGI work.
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
        # A worker can remain inside an SEC/OpenFIGI retry longer than the
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
        "--full-cusip-refresh",
        action="store_true",
        help="with --regenerate-only, prune the private CUSIP cache to current "
             "holdings, re-resolve all current CUSIPs via OpenFIGI, rebuild the "
             "snapshot CUSIP registry, and regenerate derived data",
    )
    parser.add_argument(
        "--retry-unresolved",
        action="store_true",
        help="with --regenerate-only, before the normal rebuild, re-resolve "
             "recent Equity/Preferred/Warrant, suspicious-symbol, and "
             "option-family CUSIPs from data/ticker_health.json. Cheaper than "
             "--full-cusip-refresh and meant to run on every daily update "
             "before the registry rebuild.",
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

    if args.full_cusip_refresh and not args.regenerate_only:
        log.error("--full-cusip-refresh can only be used with --regenerate-only")
        return 2
    if args.retry_unresolved and not args.regenerate_only:
        log.error("--retry-unresolved can only be used with --regenerate-only")
        return 2
    if args.retry_unresolved and args.full_cusip_refresh:
        log.error(
            "--retry-unresolved is redundant with --full-cusip-refresh; "
            "the full refresh already re-resolves everything"
        )
        return 2
    if args.defer_regeneration and args.regenerate_only:
        log.error("--defer-regeneration cannot be used with --regenerate-only")
        return 2

    # --regenerate-only does not fetch 13F filing payloads or depend on the SEC
    # user agent. Ordinary runs may still call OpenFIGI for suspect CUSIPs;
    # official SEC fund-name revalidation is reserved for full refreshes.
    if args.regenerate_only:
        log.info("=== Regenerate-only mode ===")
        # Optional targeted retry pass — runs before the main rebuild so the
        # updated CUSIP map is picked up when fund files are rewritten below.
        if args.retry_unresolved:
            retry_unresolved_cusips()
        state = load_state()
        enforce_published_quarter_health(state)
        save_state(state)
        company_ticker_data = _load_company_tickers_data()
        # First pass: refresh missing/suspect CUSIP mappings and rewrite the
        # stored fund files with the repaired tickers.
        rebuild_tickers_in_place(
            full_refresh=args.full_cusip_refresh,
            company_ticker_data=company_ticker_data,
        )
        rebuild_registry_backed_outputs(
            full_refresh=args.full_cusip_refresh,
            company_ticker_data=company_ticker_data,
            # Fund names are durable display metadata. Daily runs reuse the
            # snapshot last-known-good names; only weekly/manual full
            # refreshes may query the SEC for current series/class names.
            refresh_official_fund_names=args.full_cusip_refresh,
        )
        return 0

    # Fail fast on a missing / placeholder SEC_USER_AGENT. SEC 403s every
    # request when the UA doesn't contain a real contact email, so catching
    # it up-front is far better than spinning on retries.
    ua_bad_reason = None
    if USER_AGENT == DEFAULT_USER_AGENT:
        ua_bad_reason = "SEC_USER_AGENT env var not set"
    elif "MYEMAIL" in USER_AGENT or "example.com" in USER_AGENT:
        ua_bad_reason = f"SEC_USER_AGENT contains a placeholder: {USER_AGENT!r}"
    elif "@" not in USER_AGENT:
        ua_bad_reason = f"SEC_USER_AGENT must contain a contact email, got {USER_AGENT!r}"
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
