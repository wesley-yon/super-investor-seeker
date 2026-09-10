"""Resumable quarter-at-a-time audits with immutable selection and streamed details.

The audit never writes the collection databases. Each worker retains at most one
quarter's compact financial rows, and writes findings directly to a gzip stream.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import csv
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

from .audit import FIELDS, compare
from .http import atomic_write
from .inventory import canonical
from .locking import writer_lock
from .source_audit import compare_source

AUDIT_VERSION = '3.1.0'
SELECTION_COLUMNS = ('accession', 'filing_date', 'shard', 'source_sha256', 'parsed_sha256', 'bulk_source', 'status')


def readonly(path):
    db = sqlite3.connect('file:' + str(Path(path).resolve()) + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    return db


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def validate_selection(db, metadata):
    digest = hashlib.sha256()
    groups = Counter()
    for row in db.execute('SELECT ' + ','.join(SELECTION_COLUMNS) + ',audit_group FROM selected ORDER BY filing_date,accession'):
        record = {key: row[index] for index, key in enumerate(SELECTION_COLUMNS)}
        digest.update(canonical(record) + b'\n')
        groups[row[-1]] += 1
    if (digest.hexdigest() != metadata['selection_sha256'] or dict(groups) != metadata['groups']
            or sum(groups.values()) != metadata['selected_documents']):
        raise ValueError('Audit selection checksum or coverage changed')


def snapshot(root, destination, limit=0, reuse=None, inventory_snapshot=None, parent_inventory=None):
    root, destination = Path(root).resolve(), Path(destination)
    if reuse and inventory_snapshot:
        raise ValueError('Choose a reused selection or a frozen inventory, not both')
    if parent_inventory and (not inventory_snapshot or reuse or limit):
        raise ValueError('Incremental selection requires a frozen target and excludes reuse or a sample limit')
    frozen = None
    parent = None
    if inventory_snapshot:
        frozen = {'path': str(Path(inventory_snapshot).resolve()), 'sha256': file_hash(inventory_snapshot)}
    if parent_inventory:
        parent = {'path': str(Path(parent_inventory).resolve()), 'sha256': file_hash(parent_inventory)}
    if destination.exists():
        db = readonly(destination)
        metadata = json.loads(db.execute('SELECT value FROM metadata').fetchone()[0])
        validate_selection(db, metadata)
        db.close()
        if metadata['root'] != str(root) or metadata['selection_schema'] != 1:
            raise ValueError('Existing audit selection belongs to another source or schema')
        if frozen and metadata.get('inventory_snapshot') != frozen:
            raise ValueError('Existing audit selection belongs to a different frozen inventory')
        if parent and metadata.get('parent_inventory_snapshot') != parent:
            raise ValueError('Existing audit selection belongs to a different parent inventory')
        if frozen and bool(metadata.get('parent_inventory_snapshot')) != bool(parent):
            raise ValueError('Existing audit selection has a different complete or incremental scope')
        return metadata
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix('.creating.sqlite3')
    if temporary.exists():
        raise ValueError('An unfinished audit snapshot exists; inspect it before retrying')
    target = sqlite3.connect(temporary)
    source = None
    try:
        if reuse:
            source = readonly(reuse)
            source.backup(target)
            source.close()
            metadata = json.loads(target.execute('SELECT value FROM metadata').fetchone()[0])
            validate_selection(target, metadata)
            if metadata['root'] != str(root) or metadata['selection_schema'] != 1:
                raise ValueError('Reused audit selection belongs to another source or schema')
        else:
            target.executescript('''CREATE TABLE selected(
                accession TEXT PRIMARY KEY, filing_date TEXT, shard TEXT,
                source_sha256 TEXT, parsed_sha256 TEXT, bulk_source TEXT, status TEXT,
                audit_group TEXT NOT NULL);
                CREATE INDEX selected_group ON selected(audit_group,filing_date,accession);
                CREATE TABLE metadata(value TEXT NOT NULL);''')
            source = readonly(frozen['path'] if frozen else root / 'inventory.sqlite3')
            source.execute('BEGIN')
            counts = dict(source.execute('SELECT status,count(*) FROM filings GROUP BY status'))
            sources = {r['source_key']: dict(r) for r in source.execute('SELECT * FROM sources')}
            scope = dict(source.execute('SELECT key,value FROM settings WHERE key IN (\'window_start\',\'window_end\')'))
            if parent:
                source.execute('ATTACH DATABASE ? AS previous', (Path(parent['path']).as_uri() + '?mode=ro',))
                columns = [tuple(row) for row in source.execute('PRAGMA main.table_info(filings)')]
                if columns != [tuple(row) for row in source.execute('PRAGMA previous.table_info(filings)')]:
                    raise ValueError('Incremental selection cannot cross an inventory schema change')
                names = [row[1] for row in columns]
                if any(not re.fullmatch(r'[a-z][a-z0-9_]*', name) for name in names):
                    raise ValueError('Unsupported inventory column name')
                changed = ' OR '.join('f.' + name + ' IS NOT p.' + name for name in names)
                sql = ('SELECT ' + ','.join('f.' + name for name in SELECTION_COLUMNS)
                       + " FROM filings f LEFT JOIN previous.filings p ON p.accession=f.accession"
                       + " WHERE f.status IN ('verified','review') AND (p.accession IS NULL OR " + changed + ')'
                       + ' ORDER BY f.filing_date,f.accession')
            else:
                sql = 'SELECT ' + ','.join(SELECTION_COLUMNS) + " FROM filings WHERE status IN ('verified','review') ORDER BY filing_date,accession"
            if limit:
                sql += ' LIMIT ' + str(int(limit))
            digest = hashlib.sha256()
            groups = Counter()
            selected = 0
            for row in source.execute(sql):
                record = dict(row)
                if not all(record[k] for k in ('shard', 'source_sha256', 'parsed_sha256')):
                    raise ValueError('Committed queue entry lacks durable provenance: ' + record['accession'])
                group = record['bulk_source'] or (record['filing_date'][:4] + 'Q' + str((int(record['filing_date'][5:7]) - 1) // 3 + 1) + '_index_only')
                if not re.fullmatch(r'\d{4}Q[1-4](?:_index_only)?', group):
                    raise ValueError('Unexpected audit group')
                target.execute('INSERT INTO selected VALUES(?,?,?,?,?,?,?,?)', tuple(row) + (group,))
                digest.update(canonical(record) + b'\n')
                groups[group] += 1
                selected += 1
            source.close()
            metadata = {'root': str(root), 'selection_schema': 1, 'selection_sha256': digest.hexdigest(),
                        'selected_documents': selected, 'groups': dict(groups), 'queue_counts_at_selection': counts,
                        'sources': sources, 'scope': scope, 'selected_at': datetime.now(timezone.utc).isoformat()}
            if frozen:
                metadata['inventory_snapshot'] = frozen
            if parent:
                metadata['parent_inventory_snapshot'] = parent
                metadata['selection_kind'] = 'changed_committed_inventory_rows'
            target.execute('INSERT INTO metadata VALUES(?)', (canonical(metadata).decode(),))
        target.commit()
        target.close()
        os.replace(temporary, destination)
    except BaseException:
        target.close()
        raise
    finally:
        if source is not None:
            source.close()
    return metadata


def audit_group(task):
    root, selection_path, output, group = task
    root, selection_path, output = Path(root), Path(selection_path), Path(output)
    selection = readonly(selection_path)
    metadata = json.loads(selection.execute('SELECT value FROM metadata').fetchone()[0])
    summary_path = output / (group + '.json')
    detail_path = output / (group + '.jsonl.gz')
    if summary_path.exists():
        old = json.loads(summary_path.read_text())
        if old['audit_version'] != AUDIT_VERSION or old['selection_sha256'] != metadata['selection_sha256']:
            raise ValueError('Existing group result has a different audit version or selection')
        if not detail_path.exists() or file_hash(detail_path) != old['details_sha256']:
            raise ValueError('Existing audit details are missing or corrupt')
        selection.close()
        return old
    temporary = detail_path.with_suffix('.writing')
    counts = Counter()
    comparisons = Counter()
    compact = {}
    databases = {}
    started = time.monotonic()
    output.mkdir(parents=True, exist_ok=True)
    try:
        with temporary.open('wb') as raw:
            with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0, compresslevel=6) as details:
                def finding(kind, accession, **values):
                    counts[kind] += 1
                    details.write(canonical({'kind': kind, 'accession': accession, **values}) + b'\n')

                for row in selection.execute('SELECT * FROM selected WHERE audit_group=? ORDER BY filing_date,accession', (group,)):
                    counts['selected_documents'] += 1
                    accession = row['accession']
                    try:
                        if row['shard'] not in databases:
                            databases[row['shard']] = readonly(root / row['shard'])
                        stored = databases[row['shard']].execute('SELECT * FROM documents WHERE accession=?', (accession,)).fetchone()
                        if stored is None:
                            raise ValueError('Queue points to a missing stored document')
                        bodies = {}
                        for key in ('source', 'parsed'):
                            body = gzip.decompress(stored[key + '_gzip'])
                            digest = hashlib.sha256(body).hexdigest()
                            if (digest != stored[key + '_sha256'] or digest != row[key + '_sha256']
                                    or len(body) != stored[key + '_bytes']):
                                raise ValueError(key + ' checksum/length differs from stored provenance')
                            bodies[key] = body
                        counts['checksum_documents_passed'] += 1
                        document_audit = json.loads(stored['audit_json'])
                        if document_audit['issues']:
                            finding('source_review', accession, issues=document_audit['issues'])
                        parsed = json.loads(bodies['parsed'])
                        if parsed:
                            if parsed['filing']['source_url'] != stored['source_url']:
                                raise ValueError('Normalized filing source URL differs from retained request provenance')
                            checked, failures = compare_source(bodies['source'], parsed)
                            for category, count in checked.items():
                                counts['original_xml_' + category + '_checked'] += count
                            counts['original_xml_fields_checked'] += sum(checked.values())
                            if failures:
                                finding('original_xml_field_failure', accession, failures=failures)
                            if row['bulk_source']:
                                compact[accession] = {
                                    category: [{k: record[k] for k in list(FIELDS) + ['table']} for record in parsed[category]]
                                    for category in ['transactions', 'holdings']}
                        else:
                            counts['unparsed_documents'] += 1
                    except Exception as exc:
                        finding('document_failure', accession, error=type(exc).__name__ + ': ' + str(exc))
                source_sha256 = None
                if compact:
                    source = metadata['sources'][group]
                    path = root / source['path']
                    source_sha256 = file_hash(path)
                    if source_sha256 != source['sha256']:
                        raise ValueError('Quarterly source checksum changed: ' + group)
                    tables = {'NONDERIV_TRANS': ('transactions', 'non_derivative'), 'DERIV_TRANS': ('transactions', 'derivative'),
                              'NONDERIV_HOLDING': ('holdings', 'non_derivative'), 'DERIV_HOLDING': ('holdings', 'derivative')}
                    with zipfile.ZipFile(path) as archive:
                        names = {Path(n).stem.upper(): n for n in archive.namelist()}
                        for table, (category, kind) in tables.items():
                            bulk = defaultdict(list)
                            with archive.open(names[table]) as stream:
                                reader = csv.DictReader(io.TextIOWrapper(stream, encoding='utf-8-sig'), delimiter='\t')
                                headers = reader.fieldnames
                                keep_fields = {pair[0] for pair in FIELDS.values()} | {'EXERCISE_DATE'}
                                for record in reader:
                                    accession = record['ACCESSION_NUMBER']
                                    if accession in compact:
                                        bulk[accession].append({k: v for k, v in record.items() if k in keep_fields})
                            for accession, document in compact.items():
                                xml_rows = [r for r in document[category] if r['table'] == kind]
                                result = compare(xml_rows, bulk[accession], headers)
                                comparisons[result['status']] += 1
                                if result['status'] != 'exact':
                                    finding('bulk_difference', accession, table=table, **result)
                            del bulk
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temporary, detail_path)
        report = {'audit_version': AUDIT_VERSION, 'group': group,
                  'selection_sha256': metadata['selection_sha256'], 'source_sha256': source_sha256,
                  'counts': dict(counts), 'financial_table_comparisons': dict(comparisons),
                  'details_file': detail_path.name, 'details_sha256': file_hash(detail_path),
                  'details_bytes': detail_path.stat().st_size}
        # Timing is excluded from the result so serial and parallel results can
        # be compared exactly and resumed without changing their evidence hash.
        atomic_write(summary_path, canonical(report))
        print(json.dumps({'audit_group': group, 'documents': counts['selected_documents'],
                          'elapsed_seconds': round(time.monotonic() - started, 3)}), flush=True)
        return report
    finally:
        for db in databases.values():
            db.close()
        selection.close()


def run(root, output, workers=4, limit=0, selection=None, inventory_snapshot=None, parent_inventory=None):
    with writer_lock(output):
        return _run(root, output, workers, limit, selection, inventory_snapshot, parent_inventory)


def _run(root, output, workers=1, limit=0, selection=None, inventory_snapshot=None, parent_inventory=None):
    root, output = Path(root).resolve(), Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    metadata = snapshot(root, output / 'selection.sqlite3', limit, selection, inventory_snapshot, parent_inventory)
    tasks = [(str(root), str(output / 'selection.sqlite3'), str(output), group) for group in sorted(metadata['groups'])]
    if workers == 1:
        results = [audit_group(task) for task in tasks]
    else:
        with ProcessPoolExecutor(max_workers=min(workers, max(1, len(tasks)))) as pool:
            results = [future.result() for future in as_completed([pool.submit(audit_group, task) for task in tasks])]
        results.sort(key=lambda r: r['group'])
    counts, comparisons = Counter(), Counter()
    for result in results:
        counts.update(result['counts'])
        comparisons.update(result['financial_table_comparisons'])
    semantic = {'selection_sha256': metadata['selection_sha256'], 'audit_version': AUDIT_VERSION,
                'counts': dict(counts), 'financial_table_comparisons': dict(comparisons), 'groups': results}
    report = {**semantic, 'semantic_sha256': hashlib.sha256(canonical(semantic)).hexdigest(),
              'queue_counts_at_selection': metadata['queue_counts_at_selection'], 'scope': metadata['scope'],
              'elapsed_seconds': round(time.monotonic() - started, 3), 'workers': workers,
              'effective_workers': min(workers, len(tasks)),
              'selected_documents': metadata['selected_documents'], 'complete_backfill': False,
              'limitation': 'Audits this immutable selection only. Filing metadata, owner fields and joins, footnotes and links, signatures, financial fields, calculated row values, and raw XML paths are checked against originals. Independent bulk field comparisons cover the four financial tables; bulk differences remain review evidence.'}
    atomic_write(output / 'report.json', json.dumps(report, indent=2).encode())
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=4, choices=range(1, 5))
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--selection', type=Path)
    parser.add_argument('--inventory-snapshot', type=Path)
    parser.add_argument('--parent-inventory', type=Path, help='Select all committed target rows changed from this frozen parent')
    args = parser.parse_args()
    report = run(args.root, args.output, args.workers, args.limit, args.selection, args.inventory_snapshot, args.parent_inventory)
    print(json.dumps({k: v for k, v in report.items() if k not in {'groups', 'limitation'}}), flush=True)
    if any(report['counts'].get(k, 0) for k in ('document_failure', 'original_xml_field_failure')):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
