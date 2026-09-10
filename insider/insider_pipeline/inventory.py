"""Build the resumable, market-wide ownership filing inventory from SEC archives.

This imports as-filed metadata and independent row counts, not verified XML.
The queue remains pending until original filings have been fetched and audited.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
import csv
from datetime import date, datetime, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import zipfile

from .locking import writer_lock

FORMS = {"3", "3/A", "4", "4/A", "5", "5/A"}
TABLES = ("NONDERIV_TRANS", "DERIV_TRANS", "NONDERIV_HOLDING", "DERIV_HOLDING",
          "REPORTINGOWNER", "FOOTNOTES", "OWNER_SIGNATURE")
MONTHS = {name: i for i, name in enumerate(
    ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1)}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def filed_date(value):
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return date.fromisoformat(value).isoformat()
    day, month, year = value.split("-")
    return date(int(year), MONTHS[month.upper()], int(day)).isoformat()


def connect(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / "inventory.sqlite3", timeout=60)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sources(
            source_key TEXT PRIMARY KEY, url TEXT NOT NULL, sha256 TEXT NOT NULL,
            path TEXT NOT NULL, bytes INTEGER NOT NULL, imported_at TEXT NOT NULL,
            filing_count INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS filings(
            accession TEXT PRIMARY KEY, issuer_cik INTEGER, form TEXT NOT NULL,
            filing_date TEXT NOT NULL, source_url TEXT, bulk_source TEXT,
            bulk_metadata BLOB, expected_counts TEXT,
            status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT, retry_after REAL NOT NULL DEFAULT 0,
            source_sha256 TEXT, parsed_sha256 TEXT, shard TEXT,
            updated_at TEXT, review_count INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS work_queue ON filings(status, retry_after, filing_date, accession);
        CREATE INDEX IF NOT EXISTS issuer_filings ON filings(issuer_cik, filing_date);
        CREATE INDEX IF NOT EXISTS pending_chronology ON filings(status, filing_date, accession);
    """)
    columns = {r[1] for r in db.execute('PRAGMA table_info(filings)')}
    if 'discovery_issues' not in columns:
        db.execute("ALTER TABLE filings ADD COLUMN discovery_issues TEXT NOT NULL DEFAULT '[]'")
        db.commit()
    return db


def read_quarter(task, *, max_records=None):
    metadata, body_path, start, end = task
    digest = hashlib.sha256()
    with open(body_path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != metadata["sha256"]:
        raise ValueError("Cached SEC archive checksum mismatch: " + str(body_path))
    records = {}
    with zipfile.ZipFile(body_path) as archive:
        names = {Path(n).stem.upper(): n for n in archive.namelist() if n.lower().endswith((".tsv", ".txt"))}
        missing = set(("SUBMISSION",) + TABLES) - set(names)
        if missing:
            raise ValueError("SEC tables missing: " + str(sorted(missing)))
        with archive.open(names["SUBMISSION"]) as stream:
            reader = csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig"), delimiter="\t")
            required = {"ACCESSION_NUMBER", "ISSUERCIK", "FILING_DATE", "DOCUMENT_TYPE"}
            if not required.issubset(reader.fieldnames or []):
                raise ValueError("SEC SUBMISSION schema changed")
            for row in reader:
                filed = filed_date(row["FILING_DATE"])
                if not start <= filed <= end:
                    continue
                if row["DOCUMENT_TYPE"] not in FORMS:
                    raise ValueError("Unexpected ownership form: " + row["DOCUMENT_TYPE"])
                accession = row["ACCESSION_NUMBER"]
                if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession):
                    raise ValueError("Invalid accession: " + accession)
                if accession in records:
                    raise ValueError("Duplicate quarterly accession: " + accession)
                if max_records is not None and len(records) >= max_records:
                    raise ValueError('Quarterly source exceeds the configured filing-record bound')
                issuer = int(row["ISSUERCIK"])
                records[accession] = [issuer, row["DOCUMENT_TYPE"], filed,
                                      gzip.compress(canonical(row), mtime=0), dict.fromkeys(TABLES, 0)]
        for table in TABLES:
            with archive.open(names[table]) as stream:
                reader = csv.reader(io.TextIOWrapper(stream, encoding="utf-8-sig"), delimiter="\t")
                header = next(reader)
                index = header.index("ACCESSION_NUMBER")
                for row in reader:
                    record = records.get(row[index])
                    if record is not None:
                        record[4][table] += 1
    return metadata, str(body_path), records


def import_cached(root, cache, start, end, workers=4):
    with writer_lock(root):
        return _import_cached(root, cache, start, end, workers)


def _import_cached(root, cache, start, end, workers=4):
    date.fromisoformat(start); date.fromisoformat(end)
    root, cache = Path(root), Path(cache)
    db = connect(root)
    for key, value in (("window_start", start), ("window_end", end), ("schema_version", "1")):
        previous = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        if previous and previous["value"] != value:
            raise ValueError("Inventory scope/schema mismatch: " + key)
        db.execute("INSERT OR IGNORE INTO settings VALUES(?,?)", (key, value))
    db.commit()
    tasks = []
    for path in cache.glob("*.json"):
        metadata = json.loads(path.read_text())
        match = re.search(r"(\d{4})q([1-4])_form345\.zip", metadata.get("url", ""), re.I)
        if not match:
            continue
        year, quarter = map(int, match.groups())
        q_start = date(year, (quarter - 1) * 3 + 1, 1).isoformat()
        q_end = date(year + (quarter == 4), quarter * 3 % 12 + 1, 1).isoformat()
        if q_end <= start or q_start > end:
            continue
        key = f"{year}Q{quarter}"
        metadata["source_key"] = key
        existing = db.execute("SELECT sha256 FROM sources WHERE source_key=?", (key,)).fetchone()
        if existing:
            if existing["sha256"] != metadata["sha256"]:
                raise ValueError("An imported quarterly source changed: " + key)
            continue
        tasks.append((metadata, path.with_suffix(".body"), start, end))
    tasks.sort(key=lambda t: t[0]["source_key"])
    source_dir = root / "sources" / "quarterly"
    source_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for metadata, body_path, records in pool.map(read_quarter, tasks, chunksize=1):
            key = metadata["source_key"]
            destination = source_dir / (key + "-" + metadata["sha256"][:16] + ".zip")
            if not destination.exists():
                os.link(body_path, destination)
            if hashlib.sha256(destination.read_bytes()).hexdigest() != metadata['sha256']:
                raise ValueError('Retained SEC archive checksum mismatch: ' + key)
            stamp = datetime.now(timezone.utc).isoformat()
            with db:
                for accession, (issuer, form, filed, bulk, counts) in records.items():
                    existing = db.execute('SELECT issuer_cik,form,filing_date,bulk_source FROM filings WHERE accession=?', (accession,)).fetchone()
                    if existing and existing['bulk_source'] and tuple(existing) != (issuer, form, filed, key):
                        raise ValueError('Cross-quarter bulk identity conflict: ' + accession)
                    db.execute("""INSERT INTO filings(accession,issuer_cik,form,filing_date,bulk_source,
                        bulk_metadata,expected_counts) VALUES(?,?,?,?,?,?,?)
                        ON CONFLICT(accession) DO UPDATE SET bulk_source=excluded.bulk_source,
                        bulk_metadata=excluded.bulk_metadata,expected_counts=excluded.expected_counts""",
                        (accession, issuer, form, filed, key, bulk, canonical(counts).decode()))
                db.execute("INSERT INTO sources VALUES(?,?,?,?,?,?,?)", (
                    key, metadata["url"], metadata["sha256"], str(destination.relative_to(root)),
                    metadata["bytes"], stamp, len(records)))
            print(json.dumps({"quarter_imported": key, "filings": len(records),
                              "elapsed_seconds": round(time.monotonic() - started, 2)}), flush=True)
    report = status(db)
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    db.close()
    return report


def status(db):
    quarters = [r[0] for r in db.execute('SELECT source_key FROM sources ORDER BY source_key')]
    missing = []
    if quarters:
        scope = dict(db.execute('SELECT key,value FROM settings'))
        first = date.fromisoformat(scope['window_start'])
        year, quarter = first.year, (first.month - 1) // 3 + 1
        while f'{year}Q{quarter}' <= quarters[-1]:
            key = f'{year}Q{quarter}'
            if key not in quarters:
                missing.append(key)
            year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    return {
        "scope": dict(db.execute("SELECT key,value FROM settings")),
        "filings_by_status": dict(db.execute("SELECT status,count(*) FROM filings GROUP BY status")),
        "issuers": db.execute("SELECT count(DISTINCT issuer_cik) FROM filings").fetchone()[0],
        "imported_quarters": quarters,
        "missing_quarters_before_last_imported": missing,
        "coverage_note": "Quarterly inventory only. Original XML and current-quarter discovery require separate verification."
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end", default="2026-09-09")
    parser.add_argument("--workers", type=int, default=4, choices=range(1, 9))
    args = parser.parse_args()
    if args.cache:
        report = import_cached(args.root, args.cache, args.start, args.end, args.workers)
    else:
        db = connect(args.root)
        report = status(db)
        db.close()
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
