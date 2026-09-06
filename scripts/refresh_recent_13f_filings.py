#!/usr/bin/env python3
"""Refresh recently accepted 13F filings from SEC's current-filings feed.

The quarterly full-index files used by pipeline.py are batch indexes. They can
lag EDGAR acceptance near filing deadlines, which means an hourly run can miss a
filing that is already visible on SEC. This script supplements the all-filer
index pass with SEC's current-filings feed and processes any recent 13F-HR or
13F-HR/A accession that has not reached pipeline_state.json yet.
"""

from __future__ import annotations

import os
import re
import sys
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pipeline  # noqa: E402
from lxml import etree  # noqa: E402

CURRENT_FILINGS_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
ACCESSION_FROM_URL_RE = re.compile(r"/(\d{10})/(\d{18})/")
FORM_RE = re.compile(r"\b13F-HR(?:/A)?\b")
CIK_FROM_TEXT_RE = re.compile(r"\((\d{10})\)")


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        pipeline.log.warning("  invalid %s=%r; using %s", name, raw, default)
        return default


def parse_atom_time(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def accession_from_url(url: str) -> tuple[int, str] | None:
    match = ACCESSION_FROM_URL_RE.search(url)
    if not match:
        return None
    cik_text, accession_no_dashes = match.groups()
    accession = (
        f"{accession_no_dashes[:10]}-"
        f"{accession_no_dashes[10:12]}-"
        f"{accession_no_dashes[12:]}"
    )
    return int(cik_text), accession


def entry_text(entry: etree._Element, path: str) -> str:
    found = entry.find(path, namespaces=ATOM_NS)
    return "" if found is None or found.text is None else found.text.strip()


def entry_link(entry: etree._Element) -> str:
    for link in entry.findall("atom:link", namespaces=ATOM_NS):
        href = link.get("href") or ""
        if href:
            return href
    return ""


def filing_from_entry(entry: etree._Element) -> tuple[dict, datetime | None] | None:
    title = entry_text(entry, "atom:title")
    summary = entry_text(entry, "atom:summary")
    updated = parse_atom_time(entry_text(entry, "atom:updated"))
    href = entry_link(entry)

    form_match = FORM_RE.search(" ".join([title, summary]))
    if not form_match:
        return None

    parsed_accession = accession_from_url(href)
    if parsed_accession:
        cik, accession = parsed_accession
    else:
        cik_match = CIK_FROM_TEXT_RE.search(" ".join([title, summary]))
        if not cik_match:
            return None
        accession_match = pipeline.ACCESSION_RE.search(" ".join([href, title, summary]))
        if not accession_match:
            return None
        cik = int(cik_match.group(1))
        accession = accession_match.group(1)

    # Common title shape: "13F-HR - Manager Name (0000123456) (Filer)".
    name = title
    if " - " in name:
        name = name.split(" - ", 1)[1]
    name = re.sub(r"\s*\(\d{10}\).*$", "", name).strip() or title

    filing_date = updated.date().isoformat() if updated else ""
    return {
        "cik": cik,
        "name": name,
        "form_type": form_match.group(0),
        "date_filed": filing_date,
        "accepted_at": (
            updated.isoformat().replace("+00:00", "Z") if updated else None
        ),
        "filename": href,
        "accession": accession,
    }, updated


def fetch_recent_feed_filings(lookback_days: int, max_pages: int, page_size: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    filings: list[dict] = []
    seen: set[str] = set()

    for page in range(max_pages):
        start = page * page_size
        params = {
            "action": "getcurrent",
            "type": "13F-HR",
            "owner": "include",
            "start": start,
            "count": page_size,
            "output": "atom",
        }
        url = f"{CURRENT_FILINGS_URL}?{urlencode(params)}"
        pipeline.log.info("Downloading current 13F feed page %s", page + 1)
        try:
            resp = pipeline.HTTP.get(url)
            root = etree.fromstring(resp.content)
        except Exception as exc:
            pipeline.log.warning("  current feed page %s failed: %s", page + 1, exc)
            raise RuntimeError(
                f"current feed discovery failed on page {page + 1}"
            ) from exc

        entries = root.findall("atom:entry", namespaces=ATOM_NS)
        if not entries:
            break

        saw_entry_newer_than_cutoff = False
        for entry in entries:
            parsed = filing_from_entry(entry)
            if not parsed:
                continue
            filing, updated = parsed
            if updated and updated < cutoff:
                continue
            if updated is None or updated >= cutoff:
                saw_entry_newer_than_cutoff = True
            accession = filing["accession"]
            if accession in seen:
                continue
            seen.add(accession)
            filings.append(filing)

        if not saw_entry_newer_than_cutoff:
            break

    filings.sort(key=lambda f: (f.get("date_filed") or "", f["accession"]))
    return filings


@pipeline._serialize_pipeline_maintenance
def main() -> int:
    lookback_days = env_int("RECENT_13F_LOOKBACK_DAYS", 3)
    # 200 pages covers 20,000 current-feed entries, enough for full 13F
    # deadline bursts while still adding only small, rate-limited SEC reads.
    max_pages = env_int("RECENT_13F_MAX_PAGES", 200)
    page_size = env_int("RECENT_13F_PAGE_SIZE", 100)
    quarters_n = env_int("RECENT_13F_QUARTERS", 4)

    pipeline.DATA_DIR.mkdir(exist_ok=True)
    pipeline.FUNDS_DIR.mkdir(exist_ok=True)
    pipeline.STOCKS_DIR.mkdir(exist_ok=True)

    state = pipeline.load_state()
    initially_processed = set(state["_processed_set"])
    cusip_map = pipeline.load_cusip_map()
    state_lock = threading.Lock()

    pipeline.log.info(
        "=== Recent 13F feed refresh: lookback=%s day(s), max_pages=%s, page_size=%s ===",
        lookback_days,
        max_pages,
        page_size,
    )
    try:
        filings = fetch_recent_feed_filings(lookback_days, max_pages, page_size)
    except Exception as exc:
        pipeline.log.error("current feed discovery failed: %s", exc)
        return 1
    # Persist discovery before processing so time limits, cooldowns and feed
    # age cannot silently discard already discovered accessions.
    queue = dict(state.get("recent_feed_pending", {}))
    for filing in filings:
        queue[filing["accession"]] = filing
    queue = {key: filing for key, filing in queue.items()
             if key not in state["_processed_set"]}
    state["recent_feed_pending"] = queue
    pipeline.save_state(state)
    pending = [filing for key, filing in queue.items()
               if pipeline.accession_retry_due(state, key)]
    pipeline.log.info(
        "current feed returned %s recent 13F filing(s), %s pending",
        len(filings),
        len(pending),
    )

    pending_by_cik: dict[int, list[dict]] = defaultdict(list)
    for filing in pending:
        pending_by_cik[filing["cik"]].append(filing)

    processed = 0
    errors = 0
    quarantined = 0
    interrupted = False
    checkpoint_errors = 0
    try:
        # Oldest outstanding acceptance first; bound a batch so publication
        # does not wait for a deadline-day backlog to drain completely.
        ordered_ciks = sorted(pending_by_cik, key=lambda cik: min(
            (f.get("accepted_at") or f.get("date_filed") or "", f["accession"])
            for f in pending_by_cik[cik]))
        for cik in ordered_ciks[:env_int("RECENT_13F_MAX_CIKS", 50)]:
            triggers = pending_by_cik[cik]
            try:
                quarantined_before = set(state.get("_quarantined", {}))
                processed += pipeline.replay_quarters_for_cik(
                    cik,
                    triggers,
                    cusip_map,
                    quarters_n,
                    state,
                    state_lock=state_lock,
                    preserve_history=True,
                    quarantine_failures=True,
                )
                quarantined_after = set(state.get("_quarantined", {}))
                quarantined += len(quarantined_after - quarantined_before)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                if isinstance(
                    exc,
                    (pipeline.FilingChainError, pipeline.FilingParseError),
                ):
                    quarantined += 1
                    pipeline.quarantine_replay_failure(
                        state, cik, triggers, exc, state_lock=state_lock
                    )
                    pipeline.log.warning(
                        "  quarantined current-feed replay for CIK %s (%s); "
                        "retaining last-known-good: %s",
                        cik,
                        ", ".join(
                            filing["accession"] for filing in triggers
                        ),
                        exc,
                    )
                else:
                    errors += 1
                    pipeline.log.error(
                        "  error replaying current-feed filings for CIK %s "
                        "(%s): %s",
                        cik,
                        ", ".join(
                            filing["accession"] for filing in triggers
                        ),
                        exc,
                    )
    except KeyboardInterrupt:
        interrupted = True
        pipeline.log.warning(
            "current feed refresh interrupted; checkpointing replay state"
        )
    except Exception as exc:
        errors += 1
        pipeline.log.error(
            "unexpected current-feed replay-loop failure: %s",
            exc,
        )
    finally:
        try:
            state["recent_feed_pending"] = {
                key: filing for key, filing in queue.items()
                if key not in state["_processed_set"]}
            pipeline.save_state(state)
        except Exception as exc:
            checkpoint_errors += 1
            pipeline.log.error(
                "could not checkpoint current-feed replay state: %s",
                exc,
            )
        try:
            pipeline.save_cusip_map(cusip_map)
        except Exception as exc:
            checkpoint_errors += 1
            pipeline.log.error(
                "could not checkpoint current-feed CUSIP map: %s",
                exc,
            )

    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        remaining_due = sum(
            pipeline.accession_retry_due(state, accession)
            for accession in state.get("recent_feed_pending", {})
        )
        with Path(output_path).open("a") as output:
            output.write(f"processed_accessions={len(state['_processed_set'] - initially_processed)}\n")
            output.write(f"remaining_due={remaining_due}\n")

    if interrupted or errors or checkpoint_errors:
        pipeline.log.error(
            "current feed refresh stopped with %s failed CIK group(s), "
            "%s checkpoint failure(s)%s",
            errors,
            checkpoint_errors,
            " after interruption" if interrupted else "",
        )
        return 1

    pipeline.log.info(
        "current feed refresh processed %s new filing(s); quarantined %s CIK group(s)",
        processed,
        quarantined,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
