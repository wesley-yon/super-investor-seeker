"""Prepare selected SEC index changes and materialize an isolated collection root.

Preparation needs only a frozen complete inventory and selected index sources.
It identifies every changed committed filing before its original is recovered.
Materialization reads those originals from the unchanged parent and rechecks them
in fresh shards. Neither operation updates an active collection root.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile

from .audit_batches import file_hash, readonly
from .discovery import parse_index, quarters
from .http import SecClient, atomic_write
from .increment import frozen_hash, source_path
from .inventory import canonical
from .inventory_delta import schema, state
from .locking import writer_lock
from .runner import Shards, finish, prepare as prepare_document

MANIFEST = 'refresh-plan.json'
MAX_SOURCE_BYTES = 150_000_000
MAX_METADATA_BYTES = 2_000_000
ACCESSION = r'[0-9]{10}-[0-9]{2}-[0-9]{6}'
COMMITTED = ('verified', 'review')


def iso_date(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('Discovery cutoff must be an ISO calendar date')
    return date.fromisoformat(value).isoformat()


def requested_quarters(scope, through, supplied=None):
    start, previous = iso_date(scope['window_start']), iso_date(scope['window_end'])
    through = iso_date(through)
    if not start <= previous <= through:
        raise ValueError('Discovery refresh must preserve its start and cannot move the cutoff backward')
    available = [f'{year}Q{quarter}' for year, quarter in quarters(start, through)]
    required = {f'{year}Q{quarter}' for year, quarter in quarters(previous, through)}
    selected = set(supplied.split() if isinstance(supplied, str) else supplied or [])
    if supplied is None:
        selected = required | set(available[-2:])
    if (not selected or len(selected) > 64 or any(not isinstance(key, str) or
            not re.fullmatch(r'\d{4}Q[1-4]', key) for key in selected)
            or not required <= selected or not selected <= set(available)):
        raise ValueError('Selected quarters must cover the old cutoff through the new cutoff within the inventory scope')
    return sorted(selected)


def complete_parent(db, shape, scope):
    if not {'index_sources', 'index_membership', 'index_observations'} <= set(shape['columns']):
        raise ValueError('Targeted refresh requires complete prior index discovery')
    expected = {f'{year}Q{quarter}' for year, quarter in quarters(scope['window_start'], scope['window_end'])}
    rows = {row['source_key']: row for row in db.execute('SELECT * FROM index_sources')}
    counts = dict(db.execute('SELECT source_key,count(*) FROM index_membership GROUP BY source_key'))
    if set(rows) != expected or set(counts) - expected:
        raise ValueError('Prior index coverage has missing or out-of-scope quarters')
    if any(counts.get(key, 0) != row['filing_count'] for key, row in rows.items()):
        raise ValueError('Prior index membership differs from its recorded complete coverage')
    if db.execute('''SELECT 1 FROM index_membership m LEFT JOIN index_observations o
                     USING(source_key,accession) LEFT JOIN filings f ON f.accession=m.accession
                     WHERE o.accession IS NULL OR f.accession IS NULL LIMIT 1''').fetchone():
        raise ValueError('Prior index membership lacks its observation or inventory row')


def index_url(key):
    if not re.fullmatch(r'\d{4}Q[1-4]', key):
        raise ValueError('Invalid SEC index quarter')
    return f'https://www.sec.gov/Archives/edgar/full-index/{key[:4]}/QTR{key[-1]}/master.idx'


def checked_source(body, meta, url):
    if (not isinstance(body, bytes) or not 0 < len(body) <= MAX_SOURCE_BYTES or not isinstance(meta, dict)
            or meta.get('url') != url or meta.get('bytes') != len(body)
            or meta.get('sha256') != hashlib.sha256(body).hexdigest()
            or not isinstance(meta.get('retrieved_at_utc'), str)
            or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)',
                                meta['retrieved_at_utc'])):
        raise ValueError('Selected SEC index bytes or metadata failed verification')
    return {key: meta[key] for key in ('url', 'bytes', 'sha256', 'retrieved_at_utc')}


def source_files(directory, key):
    stem = hashlib.sha256(index_url(key).encode()).hexdigest()
    return directory / (stem + '.body'), directory / (stem + '.json')


def fetch_sources(selected, directory, *, cache, client, workers):
    directory.mkdir(parents=True)
    if client is None and cache is None:
        raise ValueError('Provide a verified index cache or an explicit SEC fetch client')
    if client is not None and cache is not None:
        raise ValueError('Choose cached evidence or fresh SEC retrieval')

    def fetch(key):
        url = index_url(key)
        if client is None:
            body_path, meta_path = source_files(Path(cache), key)
            if (body_path.is_symlink() or meta_path.is_symlink() or
                    not 0 < body_path.stat().st_size <= MAX_SOURCE_BYTES or
                    not 0 < meta_path.stat().st_size <= MAX_METADATA_BYTES):
                raise ValueError('Missing, oversized, or linked cached index source')
            body, meta = body_path.read_bytes(), json.loads(meta_path.read_text())
        else:
            body, meta = client.cached(url, directory, refresh=True)
        meta = checked_source(body, meta, url)
        body_path, meta_path = source_files(directory, key)
        atomic_write(body_path, body)
        atomic_write(meta_path, canonical(meta))
        # Do not retain every quarter's full parsed index in memory. The single
        # inventory writer parses one staged source at a time after retrieval.
        return key, meta

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(fetch, selected))


def retain_observation(db, observation, source, *, removed_by=None):
    value = {'observation': observation, 'source': source}
    if removed_by is not None:
        value['removed_by_source_sha256'] = removed_by
    body = canonical(value)
    key = 'index_observation_history:' + hashlib.sha256(body).hexdigest()
    db.execute('INSERT OR IGNORE INTO settings VALUES(?,?)', (key, body.decode()))


def set_issues(db, row, issues):
    text = canonical(sorted(issues)).decode()
    if text != row['discovery_issues']:
        db.execute('UPDATE filings SET discovery_issues=? WHERE accession=?', (text, row['accession']))


def import_index(db, key, records, meta):
    old_source = db.execute('SELECT * FROM index_sources WHERE source_key=?', (key,)).fetchone()
    old_source = dict(old_source) if old_source else None
    previous_members = {row[0] for row in db.execute('SELECT accession FROM index_membership WHERE source_key=?', (key,))}
    if previous_members and not records:
        raise ValueError('A previously populated SEC ownership index became empty')
    additions = changed_observations = 0
    for accession, (form, filed, url) in sorted(records.items()):
        observation = {'source_key': key, 'accession': accession, 'form': form,
                       'filing_date': filed, 'source_url': url}
        previous = db.execute('SELECT * FROM index_observations WHERE source_key=? AND accession=?',
                              (key, accession)).fetchone()
        previous = dict(previous) if previous else None
        if previous and previous != observation:
            retain_observation(db, previous, old_source)
            changed_observations += 1
        row = db.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone()
        if row is None:
            db.execute('INSERT INTO filings(accession,form,filing_date,source_url) VALUES(?,?,?,?)',
                       (accession, form, filed, url))
            additions += 1
        else:
            row = dict(row)
            issues = set(json.loads(row['discovery_issues']))
            # An unchanged observation may already have been investigated and
            # corrected against the original; do not undo that source review.
            if previous != observation and (row['form'], row['filing_date']) != (form, filed):
                issues.add(f'BULK_INDEX_IDENTITY_DISAGREEMENT:{key}:{form}:{filed}')
            set_issues(db, row, issues)
            collected = row['status'] in COMMITTED
            if not collected and (row['source_sha256'] or row['parsed_sha256']):
                raise ValueError('An interrupted committed document needs collection recovery before refresh')
            # Keep the exact provenance URL used for an archived original.
            # Pending documents can adopt the current SEC-indexed location.
            if not row['source_url'] or (not collected and previous != observation):
                db.execute('UPDATE filings SET source_url=? WHERE accession=?', (url, accession))
        db.execute('INSERT OR REPLACE INTO index_observations VALUES(?,?,?,?,?)',
                   tuple(observation[name] for name in ('source_key', 'accession', 'form', 'filing_date', 'source_url')))
    removed = sorted(previous_members - set(records))
    for accession in removed:
        previous = dict(db.execute('SELECT * FROM index_observations WHERE source_key=? AND accession=?',
                                   (key, accession)).fetchone())
        retain_observation(db, previous, old_source, removed_by=meta['sha256'])
        row = dict(db.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
        set_issues(db, row, set(json.loads(row['discovery_issues'])) | {f'INDEX_MEMBERSHIP_REMOVED:{key}'})
    db.execute('DELETE FROM index_membership WHERE source_key=?', (key,))
    db.executemany('INSERT INTO index_membership VALUES(?,?)', [(key, accession) for accession in sorted(records)])
    db.execute('INSERT OR REPLACE INTO index_sources VALUES(?,?,?,?,?,?)',
               (key, meta['url'], meta['sha256'], meta['bytes'], meta['retrieved_at_utc'], len(records)))
    return {'source_key': key, 'filings_in_index': len(records), 'new_filings': additions,
            'changed_observations': changed_observations, 'removed_memberships': len(removed),
            'latest_indexed_filing_date': max((row[1] for row in records.values()), default=None),
            'retrieved_at_utc': meta['retrieved_at_utc'], 'sha256': meta['sha256']}


def changed_committed(db, parent_path, shape):
    db.execute('ATTACH DATABASE ? AS previous',
               (Path(parent_path).resolve().as_uri() + '?mode=ro&immutable=1',))
    try:
        differences = ' OR '.join('f.' + name + ' IS NOT p.' + name for name in shape['columns']['filings'])
        if db.execute('''SELECT 1 FROM previous.filings p LEFT JOIN filings f USING(accession)
                         WHERE f.accession IS NULL LIMIT 1''').fetchone():
            raise ValueError('Discovery refresh cannot remove an existing filing')
        return [row[0] for row in db.execute(
            "SELECT f.accession FROM filings f JOIN previous.filings p USING(accession)"
            " WHERE p.status IN ('verified','review') AND (" + differences + ') ORDER BY f.accession')]
    finally:
        db.execute('DETACH DATABASE previous')


def staging_directory(output):
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('Discovery output requires a new destination')
    output.parent.mkdir(parents=True, exist_ok=True)
    return output, Path(tempfile.mkdtemp(prefix=output.name + '.creating-', dir=output.parent))


def prepare(inventory, output, through, *, source_quarters=None, cache=None, client=None, workers=4):
    if type(workers) is not int or not 1 <= workers <= 4:
        raise ValueError('Discovery refresh supports one to four shared-rate workers')
    inventory = Path(inventory).resolve()
    before_hash = frozen_hash(inventory)
    source = readonly(inventory)
    try:
        scope = {key: value for key, value in source.execute(
            "SELECT key,value FROM settings WHERE key IN ('window_start','window_end')")}
        selected = requested_quarters(scope, through, source_quarters)
        shape = schema(source)
        complete_parent(source, shape, scope)
        parent_state = state(source, shape)
    finally:
        source.close()
    output, work = staging_directory(output)
    target = None
    try:
        fetched = fetch_sources(selected, work / 'sources' / 'indexes',
                                cache=cache, client=client, workers=workers)
        shutil.copyfile(inventory, work / 'inventory.sqlite3')
        target = sqlite3.connect(work / 'inventory.sqlite3', uri=True)
        target.row_factory = sqlite3.Row
        target.execute('PRAGMA journal_mode=DELETE')
        with target:
            changes = []
            for key, meta in fetched:
                body_path, _ = source_files(work / 'sources' / 'indexes', key)
                body = body_path.read_bytes()
                checked_source(body, meta, index_url(key))
                records = parse_index(body, scope['window_start'], through, allow_empty=True)
                changes.append(import_index(target, key, records, meta))
                del body, records
            new_scope = {'window_start': scope['window_start'], 'window_end': through}
            summary = {'scope': new_scope, 'refreshed_quarters': changes,
                       'retrieval_mode': 'fresh_sec' if client else 'verified_cache',
                       'current_cutoff_completeness_verified': False}
            target.execute("UPDATE settings SET value=? WHERE key='window_end'", (through,))
            target.execute("INSERT OR REPLACE INTO settings VALUES('index_discovery',?)",
                           (canonical(summary).decode(),))
        if schema(target) != shape:
            raise ValueError('Discovery refresh unexpectedly changed the inventory schema')
        complete_parent(target, shape, new_scope)
        selected_documents = changed_committed(target, inventory, shape)
        needed_bulk = sorted({row[0] for accession in selected_documents for row in target.execute(
            'SELECT bulk_source FROM filings WHERE accession=? AND bulk_source IS NOT NULL', (accession,))})
        target_state = state(target, shape)
        target.close(); target = None
        if frozen_hash(inventory) != before_hash:
            raise ValueError('Frozen parent inventory changed during discovery preparation')
        selection_body = canonical(selected_documents)
        if len(selection_body) > MAX_METADATA_BYTES:
            raise ValueError('Changed-document selection exceeds the bounded refresh size')
        atomic_write(work / 'committed-accessions.json', selection_body)
        assets = []
        for key in selected:
            for path in source_files(work / 'sources' / 'indexes', key):
                assets.append({'file': str(path.relative_to(work)), 'bytes': path.stat().st_size, 'sha256': file_hash(path)})
        manifest = {'discovery_refresh_schema': 1, 'parent_inventory_state': parent_state,
                    'target_inventory_state': target_state, 'parent_scope': scope, 'target_scope': new_scope,
                    'target_inventory_file_sha256': frozen_hash(work / 'inventory.sqlite3'),
                    'source_files': assets, 'source_quarters': selected, 'source_changes': changes,
                    'retrieval_mode': summary['retrieval_mode'], 'required_bulk_quarters': needed_bulk,
                    'committed_documents': len(selected_documents),
                    'committed_accessions_sha256': hashlib.sha256(selection_body).hexdigest(),
                    'new_filings': sum(change['new_filings'] for change in changes),
                    'all_existing_filings_preserved': True, 'current_cutoff_completeness_verified': False,
                    'cloud_daily_maintenance_active': False, 'complete_backfill': False}
        atomic_write(work / MANIFEST, canonical(manifest))
        if output.exists() or output.is_symlink():
            raise ValueError('Discovery output was created by another process')
        os.rename(work, output)
        return {'plan_sha256': file_hash(output / MANIFEST), 'source_quarters': selected,
                'committed_documents_to_restore': len(selected_documents), 'required_bulk_quarters': needed_bulk,
                'new_filings': manifest['new_filings'], 'target_scope': new_scope,
                'retrieval_mode': manifest['retrieval_mode'], 'complete_backfill': False,
                'cloud_daily_maintenance_active': False}
    finally:
        if target is not None:
            target.close()
        if work.exists():
            shutil.rmtree(work)


def read_plan(directory, pin):
    directory = Path(directory).resolve()
    path = directory / MANIFEST
    if (not re.fullmatch(r'[0-9a-f]{64}', pin) or path.is_symlink()
            or not 0 < path.stat().st_size <= MAX_METADATA_BYTES or file_hash(path) != pin):
        raise ValueError('Discovery plan pin, path, or size differs')
    value = json.loads(path.read_text())
    if (value.get('discovery_refresh_schema') != 1
            or set(value['parent_scope']) != {'window_start', 'window_end'}
            or set(value['target_scope']) != {'window_start', 'window_end'}
            or value.get('retrieval_mode') not in ('verified_cache', 'fresh_sec')
            or type(value.get('committed_documents')) is not int or value['committed_documents'] < 0
            or type(value.get('new_filings')) is not int or value['new_filings'] < 0
            or value.get('all_existing_filings_preserved') is not True
            or any(value.get(key) is not False for key in
                   ('current_cutoff_completeness_verified', 'cloud_daily_maintenance_active', 'complete_backfill'))):
        raise ValueError('Unsupported discovery plan')
    selected = requested_quarters(value['parent_scope'], value['target_scope']['window_end'], value['source_quarters'])
    if selected != value['source_quarters'] or value['target_scope']['window_start'] != value['parent_scope']['window_start']:
        raise ValueError('Discovery plan scope differs')
    expected_files = set()
    for key in selected:
        expected_files.update(str(path.relative_to(directory)) for path in source_files(directory / 'sources' / 'indexes', key))
    if {asset['file'] for asset in value['source_files']} != expected_files or len(value['source_files']) != len(expected_files):
        raise ValueError('Discovery plan source membership differs')
    for asset in value['source_files']:
        path = directory / asset['file']
        limit = MAX_METADATA_BYTES if path.suffix == '.json' else MAX_SOURCE_BYTES
        if (set(asset) != {'file', 'bytes', 'sha256'} or path.is_symlink()
                or not path.resolve().is_relative_to(directory)
                or type(asset['bytes']) is not int or not 0 < asset['bytes'] <= limit
                or path.stat().st_size != asset['bytes']
                or file_hash(path) != asset['sha256']):
            raise ValueError('Discovery plan source checksum, path, or size differs')
    selected_path = directory / 'committed-accessions.json'
    if (selected_path.is_symlink() or not 0 < selected_path.stat().st_size <= MAX_METADATA_BYTES
            or file_hash(selected_path) != value['committed_accessions_sha256']):
        raise ValueError('Discovery document selection checksum or size differs')
    accessions = json.loads(selected_path.read_text())
    if (not isinstance(accessions, list) or len(accessions) != value['committed_documents']
            or any(not isinstance(a, str) or not re.fullmatch(ACCESSION, a) for a in accessions)
            or accessions != sorted(set(accessions))):
        raise ValueError('Discovery document selection membership differs')
    if frozen_hash(directory / 'inventory.sqlite3') != value['target_inventory_file_sha256']:
        raise ValueError('Discovery target inventory checksum differs')
    return directory, value, accessions


def validate_target_sources(db, directory, plan, selected, shape):
    scope = dict(db.execute("SELECT key,value FROM settings WHERE key IN ('window_start','window_end')"))
    if scope != plan['target_scope']:
        raise ValueError('Discovery target cutoff differs from its plan')
    complete_parent(db, shape, scope)
    for key in plan['source_quarters']:
        body_path, meta_path = source_files(directory / 'sources' / 'indexes', key)
        body = body_path.read_bytes()
        meta = checked_source(body, json.loads(meta_path.read_text()), index_url(key))
        records = parse_index(body, scope['window_start'], scope['window_end'], allow_empty=True)
        row = db.execute('SELECT * FROM index_sources WHERE source_key=?', (key,)).fetchone()
        expected = (key, meta['url'], meta['sha256'], meta['bytes'], meta['retrieved_at_utc'], len(records))
        if row is None or tuple(row) != expected:
            raise ValueError('Selected index source catalog differs from the planned inventory')
        members = {r[0] for r in db.execute('SELECT accession FROM index_membership WHERE source_key=?', (key,))}
        if members != set(records):
            raise ValueError('Selected index membership differs from its original SEC source')
        for accession, (form, filed, url) in records.items():
            row = db.execute('SELECT form,filing_date,source_url FROM index_observations WHERE source_key=? AND accession=?',
                             (key, accession)).fetchone()
            if row is None or tuple(row) != (form, filed, url):
                raise ValueError('Selected index observation differs from its original SEC source')
    required = sorted({row[0] for accession in selected for row in db.execute(
        'SELECT bulk_source FROM filings WHERE accession=? AND bulk_source IS NOT NULL', (accession,))})
    if required != plan['required_bulk_quarters']:
        raise ValueError('Discovery plan omitted or added a required original bulk source')


def materialize(directory, pin, restored_parent, output):
    directory, plan, selected = read_plan(directory, pin)
    restored_parent = Path(restored_parent).resolve()
    parent_path = restored_parent / 'inventory.sqlite3'
    with writer_lock(restored_parent):
        before_hash = frozen_hash(parent_path)
        parent = readonly(parent_path)
        target = None
        readers = writers = None
        work = None
        try:
            shape = schema(parent)
            if state(parent, shape) != plan['parent_inventory_state']:
                raise ValueError('Restored parent differs from the complete discovery plan inventory')
            target_check = readonly(directory / 'inventory.sqlite3')
            try:
                if schema(target_check) != shape or state(target_check, shape) != plan['target_inventory_state']:
                    raise ValueError('Discovery target logical state differs')
                if changed_committed(target_check, parent_path, shape) != selected:
                    raise ValueError('Discovery plan omitted or added a changed committed document')
                validate_target_sources(target_check, directory, plan, selected, shape)
                difference = (plan['target_inventory_state']['tables']['filings']['rows']
                              - plan['parent_inventory_state']['tables']['filings']['rows'])
                if difference != plan['new_filings']:
                    raise ValueError('Discovery plan new-filing count differs from the complete inventories')
            finally:
                target_check.close()
            readers = Shards(restored_parent, parent)
            # Validate every required original and bulk source before writing.
            for accession in selected:
                row = dict(parent.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
                if readers.existing(row) is None:
                    raise ValueError('A changed committed original must be restored before refresh')
            bulk_rows = []
            for key in plan['required_bulk_quarters']:
                row = parent.execute('SELECT * FROM sources WHERE source_key=?', (key,)).fetchone()
                if row is None:
                    raise ValueError('A required bulk source is absent from the parent inventory')
                row = dict(row)
                path = source_path(restored_parent, 'sources', row)
                if not path.is_file() or path.stat().st_size != row['bytes'] or file_hash(path) != row['sha256']:
                    raise ValueError('A required original bulk source must be restored before refresh')
                bulk_rows.append(row)
            output, work = staging_directory(output)
            shutil.copyfile(directory / 'inventory.sqlite3', work / 'inventory.sqlite3')
            target = sqlite3.connect(work / 'inventory.sqlite3', uri=True)
            target.row_factory = sqlite3.Row
            target.execute('PRAGMA journal_mode=DELETE')
            writers = Shards(work, target)
            for asset in plan['source_files']:
                path = work / asset['file']
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(directory / asset['file'], path)
                if file_hash(path) != asset['sha256']:
                    raise ValueError('Discovery source changed while materializing')
            for row in bulk_rows:
                source, path = source_path(restored_parent, 'sources', row), source_path(work, 'sources', row)
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, path)
                if file_hash(path) != row['sha256']:
                    raise ValueError('Original bulk source changed while materializing')
            originals = hashlib.sha256()
            reviews = 0
            for accession in selected:
                old_row = dict(parent.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
                new_row = dict(target.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
                old = readers.existing(old_row)
                record = prepare_document(gzip.decompress(old['source_gzip']), new_row)
                for name in ('source_sha256', 'source_bytes'):
                    if record[name] != old[name]:
                        raise ValueError('Discovery refresh changed an archived original')
                record['source_gzip'], record['fetched_at'] = old['source_gzip'], old['fetched_at']
                if record['parsed_sha256'] == old['parsed_sha256']:
                    record['parsed_gzip'] = old['parsed_gzip']
                relative = writers.choose(new_row, record)
                stored = writers.save(relative, record)
                if stored != record:
                    raise ValueError('Refreshed document differs after independent shard readback')
                finish(target, new_row, stored, relative)
                originals.update(canonical([accession, hashlib.sha256(old['source_gzip']).hexdigest(), old['source_sha256']]) + b'\n')
                reviews += stored['status'] == 'review'
            writers.close(); writers = None
            actual_state = state(target, shape)
            target.close(); target = None
            if frozen_hash(parent_path) != before_hash:
                raise ValueError('Restored parent inventory changed during materialization')
            if frozen_hash(directory / 'inventory.sqlite3') != plan['target_inventory_file_sha256']:
                raise ValueError('Discovery plan inventory changed during materialization')
            report = {'discovery_metadata_refresh_verified': True, 'plan_sha256': pin,
                      'target_scope': plan['target_scope'], 'source_quarters': plan['source_quarters'],
                      'new_filings': plan['new_filings'], 'rechecked_committed_documents': len(selected),
                      'rechecked_documents_in_review': reviews, 'original_bytes_and_compressed_blobs_preserved': True,
                      'original_documents_sha256': originals.hexdigest(), 'inventory_state': actual_state,
                      'parent_inventory_unchanged': True, 'fresh_shards_only': True,
                      'retrieval_mode': plan['retrieval_mode'], 'source_audit_performed': False,
                      'current_cutoff_completeness_verified': False, 'cloud_daily_maintenance_active': False,
                      'complete_backfill': False}
            atomic_write(work / 'discovery-refresh.json', canonical(report))
            if output.exists() or output.is_symlink():
                raise ValueError('Discovery output was created by another process')
            os.rename(work, output)
            return report
        finally:
            for obj in (readers, writers, parent, target):
                if obj is not None:
                    obj.close()
            if work is not None and work.exists():
                shutil.rmtree(work)


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest='command', required=True)
    build = commands.add_parser('prepare')
    build.add_argument('--inventory', type=Path, required=True)
    build.add_argument('--output', type=Path, required=True)
    build.add_argument('--through', required=True)
    build.add_argument('--source-quarters')
    source = build.add_mutually_exclusive_group(required=True)
    source.add_argument('--cache', type=Path)
    source.add_argument('--fetch', action='store_true')
    build.add_argument('--workers', type=int, default=4, choices=range(1, 5))
    apply = commands.add_parser('materialize')
    apply.add_argument('--plan', type=Path, required=True)
    apply.add_argument('--plan-sha256', required=True)
    apply.add_argument('--restored-parent', type=Path, required=True)
    apply.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        result = prepare(args.inventory, args.output, args.through, source_quarters=args.source_quarters,
                         cache=args.cache, client=SecClient(os.environ.get('SEC_USER_AGENT', '')) if args.fetch else None,
                         workers=args.workers)
    else:
        result = materialize(args.plan, args.plan_sha256, args.restored_parent, args.output)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
