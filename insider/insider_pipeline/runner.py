"""Resumable original-filing collection into compressed, checksummed SQLite shards.

Only a durable source plus successful identity/count checks becomes `verified`.
Source/parser discrepancies are retained in `review`; network failures retry.
One process owns the queue; four HTTP workers share a 2.5 request/second limit.
"""
from __future__ import annotations
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import time

from .http import SecClient, atomic_write
from .inventory import TABLES, canonical, connect
from .locking import writer_lock
from .parser import PARSER_VERSION, parse_ownership_xml

SHARD_LIMIT = 400 * 1024 * 1024


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def extract_xml(body, accession):
    """Read ownership XML from its actual SEC SGML document, retaining raw bytes."""
    header = re.search(br'ACCESSION NUMBER:\s*(\d{10}-\d{2}-\d{6})', body[:20000])
    if header and header.group(1).decode() != accession:
        raise ValueError('Complete submission accession mismatch')
    documents = re.findall(br'<DOCUMENT>\s*([\s\S]*?)</DOCUMENT>', body, re.I)
    candidates = []
    for document in documents or [body]:
        if not re.search(br'<(?:\w+:)?ownershipDocument\b', document):
            continue
        wrapped = re.search(br'<XML>\s*([\s\S]*?)\s*</XML>', document, re.I)
        if wrapped:
            candidates.append(wrapped.group(1))
        else:
            match = re.search(br'(<(?P<prefix>\w+:)?ownershipDocument\b[\s\S]*?</(?P=prefix)ownershipDocument>)', document)
            # An absent optional backreference does not match in Python regex.
            if not match:
                match = re.search(br'(<ownershipDocument\b[\s\S]*?</ownershipDocument>)', document)
            if match:
                candidates.append(match.group(1))
    if len(candidates) != 1:
        raise ValueError(f'Expected exactly one ownership XML document, found {len(candidates)}')
    return candidates[0]


def audit_parsed(parsed, row):
    filing = parsed['filing']
    issues = json.loads(row.get('discovery_issues', '[]'))
    form = filing['document_type'].replace('/A', 'A')
    if form != row['form'].replace('/A', 'A'):
        issues.append('DOCUMENT_TYPE_MISMATCH')
    try:
        issuer = int(filing['issuer_cik'])
        if issuer <= 0:
            raise ValueError('nonpositive CIK')
        if row.get('issuer_cik') and issuer != row['issuer_cik']:
            issues.append('ISSUER_CIK_MISMATCH')
    except ValueError:
        issuer = None
        issues.append('INVALID_ISSUER_CIK')
    counts = dict.fromkeys(TABLES, 0)
    for key, table in [('transactions', 'TRANS'), ('holdings', 'HOLDING')]:
        for record in parsed[key]:
            name = ('NONDERIV_' if record['table'] == 'non_derivative' else 'DERIV_') + table
            counts[name] += 1
    for key, table in [('owners', 'REPORTINGOWNER'), ('footnotes', 'FOOTNOTES'), ('signatures', 'OWNER_SIGNATURE')]:
        counts[table] = len(parsed[key])
    if row.get('expected_counts'):
        expected = json.loads(row['expected_counts'])
        for table in TABLES:
            if counts[table] != expected[table]:
                issues.append(f'BULK_COUNT_MISMATCH:{table}:{expected[table]}:{counts[table]}')
    warnings = list(filing['warnings'])
    for record in parsed['transactions'] + parsed['holdings']:
        warnings.extend(record['warnings'])
    return {'issues': issues, 'warnings': dict(Counter(warnings)), 'actual_counts': counts,
            'bulk_counts_checked': bool(row.get('expected_counts')), 'issuer_cik': issuer,
            'parser_version': PARSER_VERSION}


def prepare(body, row):
    source_hash = hashlib.sha256(body).hexdigest()
    try:
        xml = extract_xml(body, row['accession'])
        parsed = parse_ownership_xml(xml, row['accession'], row['source_url'], row['filing_date'])
        audit = audit_parsed(parsed, row)
        audit['xml_sha256'] = hashlib.sha256(xml).hexdigest()
    except Exception as exc:
        parsed = None
        audit = {'issues': [type(exc).__name__ + ': ' + str(exc)], 'warnings': {}, 'parser_version': PARSER_VERSION}
    normalized = canonical(parsed)
    return {'accession': row['accession'], 'source_url': row['source_url'],
            'source_sha256': source_hash, 'source_gzip': gzip.compress(body, compresslevel=6, mtime=0),
            'source_bytes': len(body), 'parsed_sha256': hashlib.sha256(normalized).hexdigest(),
            'parsed_gzip': gzip.compress(normalized, compresslevel=6, mtime=0),
            'parsed_bytes': len(normalized), 'audit_json': canonical(audit).decode(),
            'fetched_at': utcnow(), 'parser_version': PARSER_VERSION,
            'status': 'review' if audit['issues'] else 'verified',
            'issuer_cik': audit.get('issuer_cik'),
            'review_count': len(audit['issues']) + sum(audit['warnings'].values())}


def shard_connect(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=60)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript('''CREATE TABLE IF NOT EXISTS documents(
        accession TEXT PRIMARY KEY, source_url TEXT NOT NULL, source_sha256 TEXT NOT NULL,
        source_gzip BLOB NOT NULL, source_bytes INTEGER NOT NULL,
        parsed_sha256 TEXT NOT NULL, parsed_gzip BLOB NOT NULL, parsed_bytes INTEGER NOT NULL,
        audit_json TEXT NOT NULL, fetched_at TEXT NOT NULL, parser_version TEXT NOT NULL,
        status TEXT NOT NULL, issuer_cik INTEGER, review_count INTEGER NOT NULL);''')
    return db


def verified_document(db, accession):
    record = db.execute('SELECT * FROM documents WHERE accession=?', (accession,)).fetchone()
    if record is None:
        return None
    record = dict(record)
    for key in ('source', 'parsed'):
        body = gzip.decompress(record[key + '_gzip'])
        if len(body) != record[key + '_bytes'] or hashlib.sha256(body).hexdigest() != record[key + '_sha256']:
            raise ValueError('Stored document checksum mismatch: ' + accession + ':' + key)
    return record


class Shards:
    def __init__(self, root):
        self.root = Path(root)
        self.databases = {}

    def database(self, relative):
        if relative not in self.databases:
            self.databases[relative] = shard_connect(self.root / relative)
        return self.databases[relative]

    def existing(self, row):
        if row.get('shard'):
            record = verified_document(self.database(row['shard']), row['accession'])
            if record and record['parser_version'] != PARSER_VERSION:
                raise ValueError('Stored parser version requires explicit migration')
            return record
        return None

    def choose(self, row, record):
        directory = self.root / 'shards'
        directory.mkdir(parents=True, exist_ok=True)
        month = row['filing_date'][:7]
        matches = sorted(directory.glob(month + '-*.sqlite3'))
        last = matches[-1] if matches else directory / (month + '-0001.sqlite3')
        current_size = sum(p.stat().st_size for p in [last, Path(str(last) + '-wal')] if p.exists())
        added = len(record['source_gzip']) + len(record['parsed_gzip']) + len(record['audit_json']) + 4096
        if current_size and current_size + added > SHARD_LIMIT:
            last = directory / (month + f'-{int(last.stem[-4:]) + 1:04d}.sqlite3')
        return str(last.relative_to(self.root))

    def save(self, relative, record):
        db = self.database(relative)
        old = verified_document(db, record['accession'])
        if old:
            if (old['source_sha256'], old['parsed_sha256']) != (record['source_sha256'], record['parsed_sha256']):
                raise ValueError('Attempt to replace a durable document with changed content')
            return old
        columns = list(record)
        with db:
            db.execute('INSERT INTO documents(' + ','.join(columns) + ') VALUES(' + ','.join('?' for _ in columns) + ')',
                       tuple(record[k] for k in columns))
        return verified_document(db, record['accession'])

    def close(self):
        for db in self.databases.values():
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            db.close()


def finish(db, row, record, relative):
    with db:
        db.execute('''UPDATE filings SET status=?,issuer_cik=COALESCE(issuer_cik,?),
            source_sha256=?,parsed_sha256=?,shard=?,updated_at=?,review_count=?,last_error=NULL,retry_after=0
            WHERE accession=?''', (record['status'], record['issuer_cik'], record['source_sha256'],
            record['parsed_sha256'], relative, utcnow(), record['review_count'], row['accession']))


def run(root, client, workers=4, limit=0, seconds=0, start='', end='', sample_each_form=0):
    root = Path(root)
    started = time.monotonic()
    stop = False
    completed = errors = 0
    pending = {}

    def stop_soon(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, stop_soon)
    signal.signal(signal.SIGINT, stop_soon)
    with writer_lock(root):
        db = connect(root)
        shards = Shards(root)
        with db:
            db.execute("UPDATE filings SET status='pending' WHERE status='inflight'")
        clauses = ['source_url IS NOT NULL']
        params = []
        if start:
            clauses.append('filing_date>=?'); params.append(start)
        if end:
            clauses.append('filing_date<=?'); params.append(end)
        selected = None
        if sample_each_form:
            selected = []
            for form in ['3', '3/A', '4', '4/A', '5', '5/A']:
                selected += [r[0] for r in db.execute(
                    "SELECT accession FROM filings WHERE status='pending' AND " + ' AND '.join(clauses) +
                    " AND form=? ORDER BY filing_date,accession LIMIT ?", params + [form, sample_each_form])]
            if not selected:
                return {'completed_this_run': 0, 'note': 'No eligible form sample remains'}
            clauses.append('accession IN (' + ','.join('?' for _ in selected) + ')')
            params += selected
        query = 'SELECT * FROM filings WHERE status=? AND ' + ' AND '.join(clauses) + ' ORDER BY filing_date,accession LIMIT ?'
        retry_query = 'SELECT * FROM filings WHERE status=\'retry\' AND retry_after<=? AND ' + ' AND '.join(clauses) + ' ORDER BY filing_date,accession LIMIT ?'
        last_report = 0
        report = {}

        def progress(force=False, phase='running'):
            nonlocal last_report, report
            if not force and time.monotonic() - last_report < 30:
                return
            elapsed = time.monotonic() - started
            report = {'pid': os.getpid(), 'phase': phase, 'updated_at': utcnow(),
                      'elapsed_seconds': round(elapsed, 1), 'completed_this_run': completed,
                      'network_errors_this_run': errors, 'http_requests_this_run': client.requests,
                      'download_bytes_this_run': client.download_bytes,
                      'filings_per_second': round(completed / elapsed, 3) if elapsed else 0,
                      'inflight': len(pending),
                      'counts': dict(db.execute('SELECT status,count(*) FROM filings GROUP BY status'))}
            atomic_write(root / 'progress.json', canonical(report))
            print(json.dumps(report), flush=True)
            last_report = time.monotonic()

        def fetch(row):
            return prepare(client.get(row['source_url']), row)

        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                progress(True)
                while True:
                    if client.blocked.is_set() or (seconds and time.monotonic() - started >= seconds):
                        stop = True
                    capacity = workers - len(pending)
                    if limit:
                        capacity = min(capacity, limit - completed - len(pending))
                    if not stop and capacity > 0:
                        if shutil.disk_usage(root).free < 10 * 1024 ** 3:
                            raise RuntimeError('Less than 10 GiB free: pausing collection before disk exhaustion')
                        ready = [dict(r) for r in db.execute(query, ['pending'] + params + [capacity])]
                        retries = [dict(r) for r in db.execute(retry_query, [time.time()] + params + [capacity])]
                        rows = sorted(ready + retries, key=lambda r: (r['filing_date'], r['accession']))[:capacity]
                        for row in rows:
                            existing = shards.existing(row)
                            if existing:
                                finish(db, row, existing, row['shard'])
                                completed += 1
                                continue
                            with db:
                                db.execute("UPDATE filings SET status='inflight',attempts=attempts+1,updated_at=? WHERE accession=?",
                                           (utcnow(), row['accession']))
                            pending[pool.submit(fetch, row)] = row
                    if not pending and not stop and capacity > 0 and rows and (not limit or completed < limit):
                        continue
                    if not pending:
                        break
                    done, _ = wait(pending, timeout=1, return_when=FIRST_COMPLETED)
                    for future in done:
                        row = pending.pop(future)
                        try:
                            record = future.result()
                        except Exception as exc:
                            errors += 1
                            attempts = row['attempts'] + 1
                            status = 'error' if attempts >= 5 else 'retry'
                            delay = min(3600, 60 * 2 ** attempts)
                            with db:
                                db.execute('UPDATE filings SET status=?,last_error=?,retry_after=?,updated_at=? WHERE accession=?',
                                           (status, type(exc).__name__ + ': ' + str(exc), time.time() + delay, utcnow(), row['accession']))
                            print(json.dumps({'accession': row['accession'], 'status': status,
                                              'error': type(exc).__name__ + ': ' + str(exc)}), flush=True)
                            continue
                        relative = shards.choose(row, record)
                        # Record the intended location before committing its document.
                        # A crash after the shard commit recovers it without redownloading.
                        with db:
                            db.execute('UPDATE filings SET shard=? WHERE accession=?', (relative, row['accession']))
                        record = shards.save(relative, record)
                        finish(db, row, record, relative)
                        completed += 1
                    progress()
                progress(True, 'paused' if stop else 'batch_finished')
        except BaseException:
            progress(True, 'failed')
            raise
        finally:
            shards.close()
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            db.close()
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=4, choices=range(1, 5))
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--seconds', type=int, default=0)
    parser.add_argument('--start', default='')
    parser.add_argument('--end', default='')
    parser.add_argument('--sample-each-form', type=int, default=0)
    args = parser.parse_args()
    run(args.root, SecClient(os.environ.get('SEC_USER_AGENT', '')), args.workers,
        args.limit, args.seconds, args.start, args.end, args.sample_each_form)


if __name__ == '__main__':
    main()
