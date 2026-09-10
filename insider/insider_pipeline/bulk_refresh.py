"""Refresh selected ownership ZIPs without replacing archived original filings.

Sources are parsed into quarter-local scratch databases. One inventory writer
then handles removals before additions, making cross-quarter movement independent
of input order. Every committed original associated with changed source bytes is
selected for rechecking, even if submission metadata and row counts are equal.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import zipfile

from .audit_batches import file_hash, readonly
from .discovery import quarters
from .discovery_refresh import (COMMITTED, MAX_METADATA_BYTES, MAX_SOURCE_BYTES, changed_committed,
                                checked_source, complete_parent, staging_directory)
from .http import SecClient, atomic_write
from .increment import frozen_hash, source_path
from .inventory import TABLES, canonical, read_quarter
from .inventory_delta import schema, state

MANIFEST = 'bulk-refresh-plan.json'
MAX_QUARTERS = 4
MAX_DECODED_QUARTER_BYTES = 1_000_000_000
MAX_QUARTER_RECORDS = 250_000


def bulk_url(key):
    if not isinstance(key, str) or not re.fullmatch(r'\d{4}Q[1-4]', key):
        raise ValueError('Invalid SEC ownership quarter')
    return ('https://www.sec.gov/files/datastandardsinnovation/data/insider-transactions-data-sets/'
            + key.lower() + '_form345.zip')


def validate_source_url(key, url):
    bulk_url(key)
    expected = (r'https://www\.sec\.gov/files/(?:structureddata|datastandardsinnovation)/data/'
                r'insider-transactions-data-sets/' + key.lower() + r'_form345\.zip')
    if not isinstance(url, str) or re.fullmatch(expected, url) is None:
        raise ValueError('Ownership source URL must identify its exact quarter on the SEC website')
    return url


def selected_quarters(scope, values):
    if isinstance(values, str):
        values = values.split()
    if (not isinstance(values, (list, tuple)) or not 1 <= len(values) <= MAX_QUARTERS
            or any(not isinstance(key, str) or not re.fullmatch(r'\d{4}Q[1-4]', key) for key in values)
            or len(set(values)) != len(values)):
        raise ValueError('Select one to four distinct published ownership quarters')
    available = {f'{year}Q{quarter}' for year, quarter in quarters(scope['window_start'], scope['window_end'])}
    if not set(values) <= available:
        raise ValueError('Selected ownership quarters fall outside the inventory scope')
    return sorted(values)


def fetch_sources(keys, directory, *, cache, client, workers, urls):
    if (cache is None) == (client is None):
        raise ValueError('Choose a verified bulk cache or an explicit SEC retrieval client')
    directory.mkdir(parents=True)

    def fetch(key):
        url = urls[key]
        stem = hashlib.sha256(url.encode()).hexdigest()
        if cache is not None:
            body_path, meta_path = Path(cache) / (stem + '.body'), Path(cache) / (stem + '.json')
            if (body_path.is_symlink() or meta_path.is_symlink() or not body_path.is_file() or not meta_path.is_file()
                    or not 0 < body_path.stat().st_size <= MAX_SOURCE_BYTES
                    or not 0 < meta_path.stat().st_size <= MAX_METADATA_BYTES):
                raise ValueError('Missing, oversized, or linked cached bulk source')
            body, meta = body_path.read_bytes(), json.loads(meta_path.read_text())
        else:
            body, meta = client.cached(url, directory / key, refresh=True)
        meta = checked_source(body, meta, url)
        path = directory / (key + '.zip')
        atomic_write(path, body)
        return key, meta, str(path)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(fetch, keys))


def parse_source(task):
    key, meta, path, scope, records_path = task
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        stems = [Path(row.filename).stem.upper() for row in entries]
        if (not 1 <= len(entries) <= 32 or len(set(stems)) != len(stems)
                or any(row.flag_bits & 1 or row.file_size < 0 for row in entries)
                or sum(row.file_size for row in entries) > MAX_DECODED_QUARTER_BYTES):
            raise ValueError('Ownership ZIP has duplicate, encrypted, or oversized tables')
        if not set(('SUBMISSION',) + TABLES) <= set(stems):
            raise ValueError('Ownership ZIP is missing required tables')
    _, _, records = read_quarter((meta, path, scope['window_start'], scope['window_end']), max_records=MAX_QUARTER_RECORDS)
    destination = Path(records_path)
    db = sqlite3.connect(destination)
    try:
        db.execute('CREATE TABLE records(accession TEXT PRIMARY KEY,issuer INTEGER,form TEXT,filed TEXT,bulk BLOB,counts TEXT)')
        for accession, (issuer, form, filed, bulk, counts) in sorted(records.items()):
            if issuer <= 0:
                raise ValueError('Ownership source contains a nonpositive issuer CIK')
            db.execute('INSERT INTO records VALUES(?,?,?,?,?,?)', (accession, issuer, form, filed, bulk, canonical(counts).decode()))
        db.commit()
    finally:
        db.close()
    return {'source_key': key, 'metadata': meta, 'zip': path, 'records': str(destination), 'filing_count': len(records)}


def parsed_sources(fetched, scope, workers):
    tasks = [(key, meta, path, scope, str(Path(path).with_suffix('.sqlite3'))) for key, meta, path in fetched]
    if workers == 1 or len(tasks) == 1:
        return [parse_source(task) for task in tasks]
    with ProcessPoolExecutor(max_workers=min(workers, len(tasks))) as pool:
        return list(pool.map(parse_source, tasks))


def retain(db, prefix, value):
    body = canonical(value)
    db.execute('INSERT OR IGNORE INTO settings VALUES(?,?)', (prefix + ':' + hashlib.sha256(body).hexdigest(), body.decode()))


def observation(row):
    return {'accession': row['accession'], 'bulk_source': row['bulk_source'],
            'bulk_metadata': json.loads(gzip.decompress(row['bulk_metadata'])) if row['bulk_metadata'] is not None else None,
            'expected_counts': json.loads(row['expected_counts']) if row['expected_counts'] is not None else None}


def set_issues(db, row, extra):
    issues = sorted(set(json.loads(row['discovery_issues'])) | set(extra))
    db.execute('UPDATE filings SET discovery_issues=? WHERE accession=?', (canonical(issues).decode(), row['accession']))


def import_sources(db, parsed):
    old_sources = {row['source_key']: dict(row) for row in db.execute('SELECT * FROM sources')}
    changed = [item for item in parsed if item['source_key'] not in old_sources
               or (old_sources[item['source_key']]['sha256'], old_sources[item['source_key']]['bytes'], old_sources[item['source_key']]['url'])
               != (item['metadata']['sha256'], item['metadata']['bytes'], item['metadata']['url'])]
    changes = []
    # First remove withdrawn associations across every selected quarter. An
    # original remains in the inventory and its prior bulk observation is kept.
    for item in changed:
        key, meta = item['source_key'], item['metadata']
        if key in old_sources:
            retain(db, 'bulk_source_history', old_sources[key])
        records = readonly(item['records'])
        try:
            members = {row[0] for row in records.execute('SELECT accession FROM records')}
            old = list(db.execute('SELECT * FROM filings WHERE bulk_source=? ORDER BY accession', (key,)))
            if old and not members:
                raise ValueError('A previously populated ownership quarter became empty')
            removed = 0
            for row in old:
                if row['status'] not in COMMITTED and (row['source_sha256'] or row['parsed_sha256']):
                    raise ValueError('Recover interrupted committed documents before bulk refresh')
                if row['accession'] in members:
                    continue
                retain(db, 'bulk_observation_history', {'observation': observation(row), 'source': old_sources.get(key),
                                                       'removed_by_source_sha256': meta['sha256']})
                set_issues(db, row, ['BULK_MEMBERSHIP_REMOVED:' + key])
                db.execute('UPDATE filings SET bulk_source=NULL,bulk_metadata=NULL,expected_counts=NULL WHERE accession=?', (row['accession'],))
                removed += 1
            changes.append({'source_key': key, 'removed_associations': removed, 'new_filings': 0,
                            'cross_quarter_conflicts': 0, 'filings_in_source': item['filing_count'], 'sha256': meta['sha256']})
        finally:
            records.close()
    by_key = {row['source_key']: row for row in changes}
    for item in changed:
        key, meta = item['source_key'], item['metadata']
        records = readonly(item['records'])
        try:
            for record in records.execute('SELECT * FROM records ORDER BY accession'):
                accession = record['accession']
                row = db.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone()
                if row is None:
                    db.execute('INSERT INTO filings(accession,issuer_cik,form,filing_date,bulk_source,bulk_metadata,expected_counts) VALUES(?,?,?,?,?,?,?)',
                               (accession, record['issuer'], record['form'], record['filed'], key, record['bulk'], record['counts']))
                    by_key[key]['new_filings'] += 1
                    continue
                row = dict(row)
                if row['status'] not in COMMITTED and (row['source_sha256'] or row['parsed_sha256']):
                    raise ValueError('Recover interrupted committed documents before bulk refresh')
                previous = observation(row)
                incoming = {'accession': accession, 'bulk_source': key,
                            'bulk_metadata': json.loads(gzip.decompress(record['bulk'])), 'expected_counts': json.loads(record['counts'])}
                if previous == incoming:
                    continue
                if row['bulk_source'] is not None and row['bulk_source'] != key:
                    retain(db, 'bulk_competing_observation', {'observation': incoming, 'source': meta})
                    set_issues(db, row, ['BULK_CROSS_QUARTER_CONFLICT:' + key])
                    by_key[key]['cross_quarter_conflicts'] += 1
                    continue
                if row['bulk_source'] is not None:
                    retain(db, 'bulk_observation_history', {'observation': previous, 'source': old_sources.get(key)})
                if ((row['form'], row['filing_date']) != (record['form'], record['filed'])
                        or row['issuer_cik'] is not None and row['issuer_cik'] != record['issuer']):
                    set_issues(db, row, [f'BULK_IDENTITY_DISAGREEMENT:{key}:{record["issuer"]}:{record["form"]}:{record["filed"]}'])
                db.execute('UPDATE filings SET bulk_source=?,bulk_metadata=?,expected_counts=? WHERE accession=?',
                           (key, record['bulk'], record['counts'], accession))
        finally:
            records.close()
        relative = f'sources/quarterly/{key}-{meta["sha256"][:16]}.zip'
        db.execute('INSERT OR REPLACE INTO sources VALUES(?,?,?,?,?,?,?)',
                   (key, meta['url'], meta['sha256'], relative, meta['bytes'], meta['retrieved_at_utc'], item['filing_count']))
    return changes


def required_committed(db, parent, shape, changed_keys):
    result = set(changed_committed(db, parent, shape))
    if changed_keys:
        db.execute('ATTACH DATABASE ? AS previous', (Path(parent).resolve().as_uri() + '?mode=ro&immutable=1',))
        try:
            marks = ','.join('?' for _ in changed_keys)
            result.update(row[0] for row in db.execute(
                "SELECT f.accession FROM filings f JOIN previous.filings p USING(accession) WHERE p.status IN ('verified','review')"
                f' AND (p.bulk_source IN ({marks}) OR f.bulk_source IN ({marks}))', changed_keys + changed_keys))
        finally:
            db.execute('DETACH DATABASE previous')
    return sorted(result)


def required_sources(db, selected, provided_keys):
    return sorted({row[0] for accession in selected for row in db.execute(
        'SELECT bulk_source FROM filings WHERE accession=? AND bulk_source IS NOT NULL', (accession,))} - set(provided_keys))


def prepare(inventory, output, *, source_quarters, cache=None, client=None, workers=2, source_urls=None):
    if type(workers) is not int or not 1 <= workers <= 4:
        raise ValueError('Bulk refresh supports one to four shared-rate workers')
    inventory = Path(inventory).resolve()
    before_hash = frozen_hash(inventory)
    parent = readonly(inventory)
    try:
        scope = dict(parent.execute("SELECT key,value FROM settings WHERE key IN ('window_start','window_end')"))
        keys = selected_quarters(scope, source_quarters)
        if source_urls is None:
            existing = dict(parent.execute('SELECT source_key,url FROM sources'))
            if not set(keys) <= set(existing):
                raise ValueError('New ownership quarters require explicit published SEC source URLs')
            source_urls = {key: existing[key] for key in keys}
        if not isinstance(source_urls, dict) or set(source_urls) != set(keys):
            raise ValueError('Provide exactly one source URL per selected ownership quarter')
        urls = {key: validate_source_url(key, source_urls[key]) for key in keys}
        shape = schema(parent); complete_parent(parent, shape, scope)
        parent_state = state(parent, shape)
    finally:
        parent.close()
    output, work = staging_directory(output)
    work = work.resolve()
    target = None
    try:
        fetched = fetch_sources(keys, work / 'downloads', cache=cache, client=client, workers=workers, urls=urls)
        parsed = parsed_sources(fetched, scope, workers)
        shutil.copyfile(inventory, work / 'inventory.sqlite3')
        target = sqlite3.connect(work / 'inventory.sqlite3', uri=True); target.row_factory = sqlite3.Row
        target.execute('PRAGMA journal_mode=DELETE')
        with target:
            changes = import_sources(target, parsed)
        changed_keys = [row['source_key'] for row in changes]
        selected = required_committed(target, inventory, shape, changed_keys)
        needed = required_sources(target, selected, keys)
        source_rows = {row['source_key']: dict(row) for row in target.execute('SELECT * FROM sources')}
        target_state = state(target, shape)
        if schema(target) != shape:
            raise ValueError('Bulk refresh changed the inventory schema')
        target.close(); target = None
        selection = canonical(selected)
        if len(selection) > MAX_METADATA_BYTES:
            raise ValueError('Bulk refresh original selection exceeds the bounded metadata size')
        atomic_write(work / 'committed-accessions.json', selection)
        assets = []
        for item in parsed:
            row = source_rows[item['source_key']]
            path = source_path(work, 'sources', row)
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(item['zip'], path)
            if file_hash(path) != row['sha256'] or path.stat().st_size != row['bytes']:
                raise ValueError('Bulk refresh retained bytes differ from their source catalog')
            meta_path = path.with_suffix('.json'); atomic_write(meta_path, canonical(item['metadata']))
            for asset_path in (path, meta_path):
                assets.append({'file': str(asset_path.relative_to(work)), 'bytes': asset_path.stat().st_size, 'sha256': file_hash(asset_path)})
        shutil.rmtree(work / 'downloads')
        if frozen_hash(inventory) != before_hash:
            raise ValueError('Frozen parent changed during bulk preparation')
        manifest = {'bulk_refresh_schema': 1, 'parent_scope': scope, 'target_scope': scope,
                    'parent_inventory_state': parent_state, 'target_inventory_state': target_state,
                    'target_inventory_file_sha256': frozen_hash(work / 'inventory.sqlite3'),
                    'source_quarters': keys, 'source_files': assets, 'source_catalog': [source_rows[key] for key in keys],
                    'source_changes': changes, 'changed_source_quarters': changed_keys, 'required_bulk_quarters': needed,
                    'committed_accessions_sha256': hashlib.sha256(selection).hexdigest(), 'committed_documents': len(selected),
                    'new_filings': sum(row['new_filings'] for row in changes),
                    'retrieval_mode': 'fresh_sec' if client else 'verified_cache', 'all_existing_filings_preserved': True,
                    'current_cutoff_completeness_verified': False, 'cloud_daily_maintenance_active': False, 'complete_backfill': False}
        atomic_write(work / MANIFEST, canonical(manifest))
        if output.exists() or output.is_symlink():
            raise ValueError('Bulk refresh output was created by another process')
        os.rename(work, output)
        return {'plan_sha256': file_hash(output / MANIFEST), 'source_quarters': keys, 'changed_source_quarters': changed_keys,
                'committed_documents_to_restore': len(selected), 'required_bulk_quarters': needed,
                'new_filings': manifest['new_filings'], 'retrieval_mode': manifest['retrieval_mode'],
                'complete_backfill': False, 'cloud_daily_maintenance_active': False}
    finally:
        if target is not None:
            target.close()
        if work.exists():
            shutil.rmtree(work)


def read_plan(directory, pin):
    directory = Path(directory).resolve()
    path = directory / MANIFEST
    if (not isinstance(pin, str) or not re.fullmatch('[0-9a-f]{64}', pin) or path.is_symlink()
            or not 0 < path.stat().st_size <= MAX_METADATA_BYTES or file_hash(path) != pin):
        raise ValueError('Bulk refresh plan pin, path, or size differs')
    plan = json.loads(path.read_text())
    if (plan.get('bulk_refresh_schema') != 1 or plan.get('parent_scope') != plan.get('target_scope')
            or set(plan['parent_scope']) != {'window_start', 'window_end'}
            or plan.get('retrieval_mode') not in ('fresh_sec', 'verified_cache')
            or type(plan.get('committed_documents')) is not int or plan['committed_documents'] < 0
            or type(plan.get('new_filings')) is not int or plan['new_filings'] < 0
            or plan.get('all_existing_filings_preserved') is not True
            or any(plan.get(key) is not False for key in
                   ('current_cutoff_completeness_verified', 'cloud_daily_maintenance_active', 'complete_backfill'))):
        raise ValueError('Unsupported bulk refresh plan')
    keys = selected_quarters(plan['parent_scope'], plan['source_quarters'])
    if (keys != plan['source_quarters'] or plan['changed_source_quarters'] != sorted(set(plan['changed_source_quarters']))
            or not set(plan['changed_source_quarters']) <= set(keys)
            or [row['source_key'] for row in plan['source_changes']] != plan['changed_source_quarters']
            or [row['source_key'] for row in plan['source_catalog']] != keys):
        raise ValueError('Bulk refresh plan source membership differs')
    expected = set()
    for row in plan['source_catalog']:
        source = source_path(directory, 'sources', row)
        validate_source_url(row['source_key'], row['url'])
        expected.update(str(p.relative_to(directory)) for p in (source, source.with_suffix('.json')))
    if {asset['file'] for asset in plan['source_files']} != expected or len(plan['source_files']) != len(expected):
        raise ValueError('Bulk refresh source file membership differs')
    for asset in plan['source_files']:
        path = directory / asset['file']
        maximum = MAX_METADATA_BYTES if path.suffix == '.json' else MAX_SOURCE_BYTES
        if (set(asset) != {'file', 'bytes', 'sha256'} or path.is_symlink() or not path.resolve().is_relative_to(directory)
                or type(asset['bytes']) is not int or not 0 < asset['bytes'] <= maximum
                or path.stat().st_size != asset['bytes'] or file_hash(path) != asset['sha256']):
            raise ValueError('Bulk refresh source file checksum or bounds differ')
    selection = directory / 'committed-accessions.json'
    if (selection.is_symlink() or not 0 < selection.stat().st_size <= MAX_METADATA_BYTES
            or file_hash(selection) != plan['committed_accessions_sha256']):
        raise ValueError('Bulk refresh document selection checksum or bounds differ')
    values = json.loads(selection.read_text())
    if (not isinstance(values, list) or len(values) != plan['committed_documents']
            or any(not isinstance(a, str) or not re.fullmatch(r'\d{10}-\d{2}-\d{6}', a) for a in values)
            or values != sorted(set(values))):
        raise ValueError('Bulk refresh document selection membership differs')
    target = directory / 'inventory.sqlite3'
    if target.is_symlink() or frozen_hash(target) != plan['target_inventory_file_sha256']:
        raise ValueError('Bulk refresh target inventory checksum differs')
    return directory, plan, values


def validate_candidate(db, directory, plan, selected, shape, parent_path):
    """Independently reconstruct all proposed inventory changes from source ZIPs."""
    import tempfile
    if required_committed(db, parent_path, shape, plan['changed_source_quarters']) != selected:
        raise ValueError('Bulk plan omitted or added an affected committed original')
    if required_sources(db, selected, plan['source_quarters']) != plan['required_bulk_quarters']:
        raise ValueError('Bulk plan omitted or added an inherited source')
    # Replaying into scratch also detects arbitrary changes to unselected rows,
    # invented provenance, or a plan that omits a changed source quarter.
    with tempfile.TemporaryDirectory(prefix='bulk-plan-check-', dir=Path(parent_path).parent.parent) as temporary:
        scratch = Path(temporary)
        parsed = []
        for row in plan['source_catalog']:
            path = source_path(directory, 'sources', row)
            meta = checked_source(path.read_bytes(), json.loads(path.with_suffix('.json').read_text()), row['url'])
            parsed.append(parse_source((row['source_key'], meta, str(path), plan['target_scope'],
                                        str(scratch / (row['source_key'] + '.sqlite3')))))
        recreated = scratch / 'inventory.sqlite3'; shutil.copyfile(parent_path, recreated)
        candidate = sqlite3.connect(recreated, uri=True); candidate.row_factory = sqlite3.Row
        try:
            with candidate:
                changes = import_sources(candidate, parsed)
            if changes != plan['source_changes'] or state(candidate, shape) != plan['target_inventory_state']:
                raise ValueError('Bulk plan inventory differs from independently replayed original source changes')
            actual = [dict(candidate.execute('SELECT * FROM sources WHERE source_key=?', (key,)).fetchone())
                      for key in plan['source_quarters']]
            if actual != plan['source_catalog']:
                raise ValueError('Bulk source catalog differs from reconstructed provenance')
        finally:
            candidate.close()


def materialize(directory, pin, restored_parent, output):
    from .discovery_refresh import materialize_prepared
    return materialize_prepared(directory, pin, restored_parent, output, read_plan=read_plan,
                                validate_candidate=validate_candidate,
                                report_key='bulk_metadata_refresh_verified', report_filename='bulk-refresh.json')


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest='command', required=True)
    prepare_parser = commands.add_parser('prepare')
    prepare_parser.add_argument('--inventory', type=Path, required=True)
    prepare_parser.add_argument('--output', type=Path, required=True)
    prepare_parser.add_argument('--source-quarters', nargs='+', required=True)
    prepare_parser.add_argument('--source-url', action='append', metavar='YYYYQn=URL',
                                help='Exact published SEC URL per selected quarter; defaults to recorded URLs for existing quarters')
    source = prepare_parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--cache', type=Path)
    source.add_argument('--fetch-sec', action='store_true')
    prepare_parser.add_argument('--workers', type=int, choices=range(1, 5), default=2)
    apply = commands.add_parser('materialize')
    apply.add_argument('--plan', type=Path, required=True)
    apply.add_argument('--plan-sha256', required=True)
    apply.add_argument('--restored-parent', type=Path, required=True)
    apply.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        client = SecClient(os.environ.get('SEC_USER_AGENT', '')) if args.fetch_sec else None
        urls = None
        if args.source_url is not None:
            pairs = [value.split('=', 1) for value in args.source_url]
            if any(len(pair) != 2 for pair in pairs) or len({pair[0] for pair in pairs}) != len(pairs):
                parser.error('Use one YYYYQn=URL value per selected quarter')
            urls = dict(pairs)
        result = prepare(args.inventory, args.output, source_quarters=args.source_quarters,
                         cache=args.cache, client=client, workers=args.workers, source_urls=urls)
    else:
        result = materialize(args.plan, args.plan_sha256, args.restored_parent, args.output)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
