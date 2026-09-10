"""Market-wide discovery using the SEC's complete quarterly filing indexes."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
import os
from pathlib import Path
import re

from .http import SecClient
from .inventory import FORMS, canonical, connect
from .locking import writer_lock


def quarters(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    year, quarter = first.year, (first.month - 1) // 3 + 1
    while date(year, (quarter - 1) * 3 + 1, 1) <= last:
        yield year, quarter
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)


def parse_index(body, start, end, *, allow_empty=False):
    text = body.decode('utf-8-sig', errors='strict')
    if 'CIK|Company Name|Form Type|Date Filed|Filename' not in text:
        raise ValueError('SEC master index schema changed')
    records = {}
    for line in text.splitlines():
        cells = line.split('|')
        if cells[0].isdigit() and len(cells) != 5:
            raise ValueError('Malformed SEC index data row')
        if len(cells) != 5:
            continue
        cik, name, form, filed, filename = cells
        if form not in FORMS:
            continue
        if not cik.isdigit() or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', filed):
            raise ValueError('Malformed SEC ownership index identity or date')
        date.fromisoformat(filed)
        if not start <= filed <= end:
            continue
        if not re.fullmatch(r'edgar/data/\d+/\d{10}-\d{2}-\d{6}\.txt', filename):
            raise ValueError('Unexpected complete-filing path: ' + filename)
        accession = Path(filename).stem
        record = (form, filed, 'https://www.sec.gov/Archives/' + filename)
        previous = records.get(accession)
        # The index lists the same joint filing under issuer and reporting owners.
        # Form/date must agree; either SEC-indexed path contains the same filing.
        if previous and previous[:2] != record[:2]:
            raise ValueError('Conflicting duplicate index accession: ' + accession)
        records.setdefault(accession, record)
    if not records and not allow_empty:
        raise ValueError('SEC ownership index unexpectedly empty')
    return records


def discover(root, client, workers=4, refresh=False):
    root = Path(root)
    with writer_lock(root):
        db = connect(root)
        scope = dict(db.execute('SELECT key,value FROM settings'))
        start, end = scope['window_start'], scope['window_end']
        db.executescript('''CREATE TABLE IF NOT EXISTS index_sources(
            source_key TEXT PRIMARY KEY, url TEXT NOT NULL, sha256 TEXT NOT NULL,
            bytes INTEGER NOT NULL, retrieved_at TEXT NOT NULL, filing_count INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS index_membership(
            source_key TEXT NOT NULL, accession TEXT NOT NULL,
            PRIMARY KEY(source_key, accession));
            CREATE TABLE IF NOT EXISTS index_observations(
            source_key TEXT NOT NULL, accession TEXT NOT NULL, form TEXT NOT NULL,
            filing_date TEXT NOT NULL, source_url TEXT NOT NULL,
            PRIMARY KEY(source_key, accession));''')
        all_quarters = list(quarters(start, end))

        def fetch(item):
            year, quarter = item
            url = f'https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx'
            body, metadata = client.cached(url, root / 'sources' / 'indexes', refresh=refresh or item == all_quarters[-1])
            records = parse_index(body, start, end)
            return f'{year}Q{quarter}', records, metadata

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for key, records, meta in pool.map(fetch, all_quarters):
                additions = 0
                with db:
                    db.execute('DELETE FROM index_membership WHERE source_key=?', (key,))
                    for accession, (form, filed, url) in records.items():
                        old = db.execute('SELECT form,filing_date,discovery_issues FROM filings WHERE accession=?', (accession,)).fetchone()
                        issues = json.loads(old['discovery_issues']) if old else []
                        if old and (old['form'], old['filing_date']) != (form, filed):
                            issue = f'BULK_INDEX_IDENTITY_DISAGREEMENT:{key}:{form}:{filed}'
                            issues = sorted(set(issues + [issue]))
                        if old is None:
                            additions += 1
                        db.execute('''INSERT INTO filings(accession,form,filing_date,source_url)
                            VALUES(?,?,?,?) ON CONFLICT(accession) DO UPDATE SET source_url=excluded.source_url''',
                            (accession, form, filed, url))
                        db.execute('INSERT INTO index_membership VALUES(?,?)', (key, accession))
                        db.execute('INSERT OR REPLACE INTO index_observations VALUES(?,?,?,?,?)', (key, accession, form, filed, url))
                        if issues:
                            db.execute('UPDATE filings SET discovery_issues=? WHERE accession=?', (canonical(issues).decode(), accession))
                    db.execute('INSERT OR REPLACE INTO index_sources VALUES(?,?,?,?,?,?)', (
                        key, meta['url'], meta['sha256'], meta['bytes'], meta['retrieved_at_utc'], len(records)))
                print(json.dumps({'index_imported': key, 'unique_filings': len(records), 'added_beyond_bulk': additions}), flush=True)
        report = {
            'inventory_filings': db.execute('SELECT count(*) FROM filings').fetchone()[0],
            'bulk_without_index_url': db.execute('SELECT count(*) FROM filings WHERE source_url IS NULL').fetchone()[0],
            'index_without_bulk': db.execute('SELECT count(*) FROM filings WHERE bulk_source IS NULL').fetchone()[0],
            'quarterly_indexes': db.execute('SELECT count(*) FROM index_sources').fetchone()[0],
            'identity_disagreements': db.execute("SELECT count(*) FROM filings WHERE discovery_issues<>'[]'").fetchone()[0],
            'scope': scope,
            'current_quarter_retrieved_at': db.execute('SELECT retrieved_at FROM index_sources ORDER BY source_key DESC LIMIT 1').fetchone()[0],
            'note': 'SEC indexes can lag the current filing day; the current quarter must be refreshed before final completion.'}
        db.execute("INSERT OR REPLACE INTO settings VALUES('index_discovery',?)", (canonical(report).decode(),))
        db.commit()
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        db.close()
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=4, choices=range(1, 5))
    parser.add_argument('--refresh', action='store_true')
    args = parser.parse_args()
    client = SecClient(os.environ.get('SEC_USER_AGENT', ''))
    print(json.dumps(discover(args.root, client, args.workers, args.refresh), indent=2), flush=True)


if __name__ == '__main__':
    main()
