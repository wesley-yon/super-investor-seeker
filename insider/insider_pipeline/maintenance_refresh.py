"""Reconcile published ownership ZIPs and SEC indexes before one original recovery.

One frozen parent, one metadata candidate, and one affected-original union avoid
intermediate inventory states that cannot be recovered from the archive chain.
The retained SEC dataset page determines exact quarterly download URLs.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
from urllib.parse import urljoin

from . import bulk_refresh as bulk, discovery_refresh as index
from .audit_batches import file_hash, readonly
from .discovery import parse_index, quarters
from .http import SecClient, atomic_write
from .increment import frozen_hash, source_path
from .inventory import canonical
from .inventory_delta import schema, state

MANIFEST = 'maintenance-plan.json'
CATALOG_URL = 'https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets'
CATALOG_STEM = hashlib.sha256(CATALOG_URL.encode()).hexdigest()


def published_sources(body):
    if not isinstance(body, bytes) or not 0 < len(body) <= index.MAX_METADATA_BYTES:
        raise ValueError('Ownership publication page exceeds its bounded size')
    result = {}

    class Links(HTMLParser):
        def handle_starttag(self, tag, attrs):
            if tag != 'a':
                return
            hrefs = [value for name, value in attrs if name == 'href']
            if len(hrefs) != 1 or not isinstance(hrefs[0], str):
                return
            url = urljoin(CATALOG_URL, hrefs[0])
            match = re.search(r'/(\d{4})q([1-4])_form345\.zip$', url)
            if not match:
                return
            key = match[1] + 'Q' + match[2]
            bulk.validate_source_url(key, url)
            if key in result and result[key] != url:
                raise ValueError('Ownership publication page has conflicting URLs for one quarter')
            result[key] = url

    parser = Links(convert_charrefs=True)
    parser.feed(body.decode('utf-8', errors='strict')); parser.close()
    if not result:
        raise ValueError('Ownership publication page has no recognized SEC quarterly downloads')
    return dict(sorted(result.items()))


def publication_files(directory):
    return tuple(Path(directory) / 'sources/catalog' / (CATALOG_STEM + suffix) for suffix in ('.body', '.json'))


def fetch_publication(work, cache, client):
    if (cache is None) == (client is None):
        raise ValueError('Choose one verified maintenance cache or an explicit SEC client')
    if client is not None:
        body, meta = client.cached(CATALOG_URL, work / 'sources/catalog', refresh=True)
    else:
        paths = [Path(cache) / 'catalog' / (CATALOG_STEM + suffix) for suffix in ('.body', '.json')]
        if any(p.is_symlink() or not p.is_file() or not 0 < p.stat().st_size <= index.MAX_METADATA_BYTES for p in paths):
            raise ValueError('Missing, linked, or oversized cached publication evidence')
        body, meta = paths[0].read_bytes(), json.loads(paths[1].read_text())
    meta = index.checked_source(body, meta, CATALOG_URL)
    published = published_sources(body)
    body_path, meta_path = publication_files(work)
    atomic_write(body_path, body); atomic_write(meta_path, canonical(meta))
    return published


def selected_bulk(scope, existing, published, supplied=None):
    allowed = {f'{year}Q{quarter}' for year, quarter in quarters(scope['window_start'], scope['window_end'])}
    available = sorted(set(published) & allowed)
    missing = set(available) - set(existing)
    selected = bulk.selected_quarters(scope, supplied if supplied is not None else sorted(missing | set(available[-2:])))
    if not set(selected) <= set(available) or not missing <= set(selected):
        raise ValueError('Maintenance must include every newly published in-scope quarter and use published URLs')
    return selected


def apply_indexes(db, directory, keys, scope, through, mode):
    changes = []
    for key in keys:
        path, metadata = index.source_files(directory / 'sources/indexes', key)
        body = path.read_bytes()
        meta = index.checked_source(body, json.loads(metadata.read_text()), index.index_url(key))
        changes.append(index.import_index(db, key, parse_index(body, scope['window_start'], through, allow_empty=True), meta))
    new_scope = {'window_start': scope['window_start'], 'window_end': through}
    summary = {'scope': new_scope, 'refreshed_quarters': changes, 'retrieval_mode': mode,
               'current_cutoff_completeness_verified': False}
    db.execute("UPDATE settings SET value=? WHERE key='window_end'", (through,))
    db.execute("INSERT OR REPLACE INTO settings VALUES('index_discovery',?)", (canonical(summary).decode(),))
    return changes


def prepare(inventory, output, through, *, index_quarters=None, bulk_quarters=None, cache=None, client=None, workers=2):
    if type(workers) is not int or not 1 <= workers <= 4:
        raise ValueError('Maintenance supports one to four shared-rate workers')
    inventory = Path(inventory).resolve(); before = frozen_hash(inventory)
    parent = readonly(inventory)
    try:
        scope = dict(parent.execute("SELECT key,value FROM settings WHERE key IN ('window_start','window_end')"))
        keys = index.requested_quarters(scope, through, index_quarters)
        shape = schema(parent); index.complete_parent(parent, shape, scope)
        existing = [row[0] for row in parent.execute('SELECT source_key FROM sources')]
        parent_state = state(parent, shape)
    finally:
        parent.close()
    new_scope = {'window_start': scope['window_start'], 'window_end': through}
    output, work = index.staging_directory(output); work = work.resolve()
    target = None
    try:
        published = fetch_publication(work, cache, client)
        bulk_keys = selected_bulk(new_scope, existing, published, bulk_quarters)
        urls = {key: published[key] for key in bulk_keys}
        # Split a fixed worker budget between independent fetch pools; both use
        # the same SecClient clock. Inventory writes remain ordered and single.
        def fetch_indexes():
            return index.fetch_sources(keys, work / 'sources/indexes', cache=Path(cache) / 'indexes' if cache else None,
                                       client=client, workers=max(1, workers - workers // 2))

        def fetch_bulk():
            return bulk.fetch_sources(bulk_keys, work / 'downloads', cache=Path(cache) / 'quarterly' if cache else None,
                                      client=client, workers=max(1, workers // 2), urls=urls)

        if workers == 1:
            fetch_indexes(); fetched = fetch_bulk()
        else:
            with ThreadPoolExecutor(max_workers=2) as pool:
                indexes = pool.submit(fetch_indexes); sources = pool.submit(fetch_bulk)
                indexes.result(); fetched = sources.result()
        parsed = bulk.parsed_sources(fetched, new_scope, workers)
        shutil.copyfile(inventory, work / 'inventory.sqlite3')
        target = sqlite3.connect(work / 'inventory.sqlite3', uri=True); target.row_factory = sqlite3.Row
        target.execute('PRAGMA journal_mode=DELETE')
        mode = 'fresh_sec' if client else 'verified_cache'
        with target:
            index_changes = apply_indexes(target, work, keys, scope, through, mode)
            bulk_changes = bulk.import_sources(target, parsed)
        changed_keys = [row['source_key'] for row in bulk_changes]
        selected = bulk.required_committed(target, inventory, shape, changed_keys)
        needed = bulk.required_sources(target, selected, bulk_keys)
        catalog = [dict(target.execute('SELECT * FROM sources WHERE source_key=?', (key,)).fetchone()) for key in bulk_keys]
        if schema(target) != shape:
            raise ValueError('Maintenance changed the inventory schema')
        index.complete_parent(target, shape, new_scope)
        target_state = state(target, shape); target.close(); target = None
        selection = canonical(selected)
        if len(selection) > index.MAX_METADATA_BYTES:
            raise ValueError('Maintenance affected-original selection exceeds the bounded size')
        atomic_write(work / 'committed-accessions.json', selection)
        paths = list(publication_files(work))
        for key in keys:
            paths.extend(index.source_files(work / 'sources/indexes', key))
        for item, row in zip(parsed, catalog, strict=True):
            path = source_path(work, 'sources', row); path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(item['zip'], path)
            if file_hash(path) != row['sha256'] or path.stat().st_size != row['bytes']:
                raise ValueError('Maintenance source bytes differ from the target catalog')
            meta_path = path.with_suffix('.json'); atomic_write(meta_path, canonical(item['metadata']))
            paths.extend((path, meta_path))
        shutil.rmtree(work / 'downloads')
        if frozen_hash(inventory) != before:
            raise ValueError('Frozen maintenance parent changed during preparation')
        assets = [{'file': str(p.relative_to(work)), 'bytes': p.stat().st_size, 'sha256': file_hash(p)} for p in paths]
        plan = {'maintenance_refresh_schema': 1, 'parent_scope': scope, 'target_scope': new_scope,
                'parent_inventory_state': parent_state, 'target_inventory_state': target_state,
                'target_inventory_file_sha256': frozen_hash(work / 'inventory.sqlite3'),
                'source_quarters': sorted(set(keys + bulk_keys)), 'index_quarters': keys, 'bulk_quarters': bulk_keys,
                'source_catalog': catalog, 'source_files': assets, 'published_quarters': published,
                'index_changes': index_changes, 'bulk_changes': bulk_changes, 'changed_bulk_quarters': changed_keys,
                'required_bulk_quarters': needed, 'committed_accessions_sha256': hashlib.sha256(selection).hexdigest(),
                'committed_documents': len(selected), 'new_filings': sum(r['new_filings'] for r in index_changes + bulk_changes),
                'retrieval_mode': mode, 'all_existing_filings_preserved': True,
                'current_cutoff_completeness_verified': False, 'cloud_daily_maintenance_active': False, 'complete_backfill': False}
        atomic_write(work / MANIFEST, canonical(plan))
        if output.exists() or output.is_symlink():
            raise ValueError('Maintenance output was created by another process')
        os.rename(work, output)
        return {'plan_sha256': file_hash(output / MANIFEST), 'index_quarters': keys, 'bulk_quarters': bulk_keys,
                'changed_bulk_quarters': changed_keys, 'committed_documents_to_restore': len(selected),
                'required_bulk_quarters': needed, 'new_filings': plan['new_filings'], 'target_scope': new_scope,
                'retrieval_mode': mode, 'complete_backfill': False, 'cloud_daily_maintenance_active': False}
    finally:
        if target is not None:
            target.close()
        if work.exists():
            shutil.rmtree(work)


def read_plan(directory, pin):
    directory = Path(directory).resolve(); path = directory / MANIFEST
    if (not isinstance(pin, str) or not re.fullmatch('[0-9a-f]{64}', pin) or path.is_symlink()
            or not 0 < path.stat().st_size <= index.MAX_METADATA_BYTES or file_hash(path) != pin):
        raise ValueError('Maintenance plan checksum or bounds differ')
    plan = json.loads(path.read_text())
    if (plan.get('maintenance_refresh_schema') != 1
            or set(plan['parent_scope']) != {'window_start', 'window_end'}
            or set(plan['target_scope']) != {'window_start', 'window_end'}
            or plan['parent_scope']['window_start'] != plan['target_scope']['window_start']
            or plan.get('retrieval_mode') not in ('fresh_sec', 'verified_cache')
            or any(type(plan.get(key)) is not int or plan[key] < 0 for key in ('committed_documents', 'new_filings'))
            or plan.get('all_existing_filings_preserved') is not True
            or any(plan.get(key) is not False for key in
                   ('current_cutoff_completeness_verified', 'cloud_daily_maintenance_active', 'complete_backfill'))):
        raise ValueError('Unsupported maintenance plan')
    keys = index.requested_quarters(plan['parent_scope'], plan['target_scope']['window_end'], plan['index_quarters'])
    bulk_keys = bulk.selected_quarters(plan['target_scope'], plan['bulk_quarters'])
    if (keys != plan['index_quarters'] or bulk_keys != plan['bulk_quarters']
            or plan['source_quarters'] != sorted(set(keys + bulk_keys))
            or [row['source_key'] for row in plan['source_catalog']] != bulk_keys
            or [row['source_key'] for row in plan['index_changes']] != keys
            or [row['source_key'] for row in plan['bulk_changes']] != plan['changed_bulk_quarters']
            or not set(plan['changed_bulk_quarters']) <= set(bulk_keys)):
        raise ValueError('Maintenance plan quarter membership differs')
    expected = set(publication_files(directory))
    for key in keys:
        expected.update(index.source_files(directory / 'sources/indexes', key))
    for row in plan['source_catalog']:
        bulk.validate_source_url(row['source_key'], row['url'])
        path = source_path(directory, 'sources', row); expected.update((path, path.with_suffix('.json')))
    if ({asset['file'] for asset in plan['source_files']} != {str(p.relative_to(directory)) for p in expected}
            or len(plan['source_files']) != len(expected)):
        raise ValueError('Maintenance source file membership differs')
    for asset in plan['source_files']:
        path = directory / asset['file']
        limit = index.MAX_METADATA_BYTES if path.suffix == '.json' or '/catalog/' in asset['file'] else index.MAX_SOURCE_BYTES
        if (set(asset) != {'file', 'bytes', 'sha256'} or path.is_symlink() or not path.resolve().is_relative_to(directory)
                or type(asset['bytes']) is not int or not 0 < asset['bytes'] <= limit
                or path.stat().st_size != asset['bytes'] or file_hash(path) != asset['sha256']):
            raise ValueError('Maintenance source checksum or bounds differ')
    page, metadata = publication_files(directory)
    body = page.read_bytes(); index.checked_source(body, json.loads(metadata.read_text()), CATALOG_URL)
    published = published_sources(body)
    if published != plan['published_quarters'] or any(published.get(row['source_key']) != row['url'] for row in plan['source_catalog']):
        raise ValueError('Maintenance quarterly URLs differ from retained SEC publication evidence')
    selection = directory / 'committed-accessions.json'
    if (selection.is_symlink() or not 0 < selection.stat().st_size <= index.MAX_METADATA_BYTES
            or file_hash(selection) != plan['committed_accessions_sha256']):
        raise ValueError('Maintenance selection checksum or bounds differ')
    selected = json.loads(selection.read_text())
    if (not isinstance(selected, list) or len(selected) != plan['committed_documents']
            or any(not isinstance(a, str) or not re.fullmatch(index.ACCESSION, a) for a in selected)
            or selected != sorted(set(selected))):
        raise ValueError('Maintenance affected-original selection differs')
    target = directory / 'inventory.sqlite3'
    if target.is_symlink() or frozen_hash(target) != plan['target_inventory_file_sha256']:
        raise ValueError('Maintenance target inventory checksum differs')
    return directory, plan, selected


def validate_candidate(db, directory, plan, selected, shape, parent_path):
    if bulk.required_committed(db, parent_path, shape, plan['changed_bulk_quarters']) != selected:
        raise ValueError('Maintenance plan omitted or added an affected original')
    if bulk.required_sources(db, selected, plan['bulk_quarters']) != plan['required_bulk_quarters']:
        raise ValueError('Maintenance plan inherited-source selection differs')
    with tempfile.TemporaryDirectory(prefix='maintenance-check-', dir=Path(parent_path).parent.parent) as temporary:
        scratch = Path(temporary); recreated = scratch / 'inventory.sqlite3'; shutil.copyfile(parent_path, recreated)
        candidate = sqlite3.connect(recreated, uri=True); candidate.row_factory = sqlite3.Row
        try:
            index.complete_parent(candidate, shape, plan['parent_scope'])
            existing = [row[0] for row in candidate.execute('SELECT source_key FROM sources')]
            selected_bulk(plan['target_scope'], existing, plan['published_quarters'], plan['bulk_quarters'])
            parsed = []
            for row in plan['source_catalog']:
                path = source_path(directory, 'sources', row)
                meta = index.checked_source(path.read_bytes(), json.loads(path.with_suffix('.json').read_text()), row['url'])
                parsed.append(bulk.parse_source((row['source_key'], meta, str(path), plan['target_scope'],
                                                str(scratch / (row['source_key'] + '.sqlite3')))))
            with candidate:
                indexes = apply_indexes(candidate, directory, plan['index_quarters'], plan['parent_scope'],
                                        plan['target_scope']['window_end'], plan['retrieval_mode'])
                sources = bulk.import_sources(candidate, parsed)
            index.complete_parent(candidate, shape, plan['target_scope'])
            catalog = [dict(candidate.execute('SELECT * FROM sources WHERE source_key=?', (key,)).fetchone()) for key in plan['bulk_quarters']]
            if (indexes != plan['index_changes'] or sources != plan['bulk_changes'] or catalog != plan['source_catalog']
                    or state(candidate, shape) != plan['target_inventory_state']):
                raise ValueError('Maintenance candidate differs from independently replayed SEC sources')
        finally:
            candidate.close()


def materialize(directory, pin, restored_parent, output):
    return index.materialize_prepared(directory, pin, restored_parent, output, read_plan=read_plan,
                                      validate_candidate=validate_candidate,
                                      report_key='combined_metadata_refresh_verified', report_filename='maintenance-refresh.json')


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest='command', required=True)
    build = commands.add_parser('prepare')
    build.add_argument('--inventory', type=Path, required=True); build.add_argument('--output', type=Path, required=True)
    build.add_argument('--through', required=True); build.add_argument('--index-quarters', nargs='+')
    build.add_argument('--bulk-quarters', nargs='+'); build.add_argument('--workers', type=int, choices=range(1, 5), default=2)
    source = build.add_mutually_exclusive_group(required=True)
    source.add_argument('--cache', type=Path); source.add_argument('--fetch-sec', action='store_true')
    apply = commands.add_parser('materialize')
    apply.add_argument('--plan', type=Path, required=True); apply.add_argument('--plan-sha256', required=True)
    apply.add_argument('--restored-parent', type=Path, required=True); apply.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        client = SecClient(os.environ.get('SEC_USER_AGENT', '')) if args.fetch_sec else None
        result = prepare(args.inventory, args.output, args.through, index_quarters=args.index_quarters,
                         bulk_quarters=args.bulk_quarters, cache=args.cache, client=client, workers=args.workers)
    else:
        result = materialize(args.plan, args.plan_sha256, args.restored_parent, args.output)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
