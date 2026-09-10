"""Load selected SEC source versions from an already pinned checkpoint chain.

Catalogs plan the bounded download. The restored inventory must then match every
catalog row before any source payload is downloaded or a prepared root is exposed.
Filing documents remain in their archives; this is not a collection runner.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
from pathlib import Path
import re

from .audit_batches import file_hash, readonly
from .github_chain import MAX_METADATA_BYTES, blob_name, valid_hash
from .http import atomic_write, check_url
from .increment import source_path
from .inventory import canonical

SOURCE_TABLES = ('sources', 'index_sources')
MAX_SOURCE_BYTES = 1_000_000_000


def quarter_keys(values):
    if (not isinstance(values, (list, tuple)) or not 1 <= len(values) <= 64
            or any(not isinstance(value, str) or not re.fullmatch(r'\d{4}Q[1-4]', value) for value in values)
            or len(set(values)) != len(values)):
        raise ValueError('Specify one to 64 distinct source quarters as YYYYQ1 through YYYYQ4')
    return sorted(values)


def remote_name(node, asset):
    return blob_name(asset['sha256']) if node['locator']['layout'] == 'content_addressed' else asset['file']


def metadata_file(node, name, pin, downloader):
    asset = node['expected'].get(name)
    if asset is None or asset['sha256'] != pin or not 0 < asset['bytes'] <= MAX_METADATA_BYTES:
        raise ValueError('Source metadata differs from its pinned checkpoint declaration')
    path = downloader.get(node['locator']['tag'], remote_name(node, asset), asset['bytes'], pin)
    return json.loads(path.read_text())


def source_catalog(node, downloader):
    checkpoint = node['manifest']
    baseline = node['reference']['kind'] == 'baseline'
    documents = metadata_file(node, 'manifest.json', checkpoint['document_manifest_sha256'], downloader)
    scope = checkpoint['scope'] if baseline else checkpoint['target_inventory']['scope']
    selection = checkpoint['selection_sha256'] if baseline else checkpoint['changed_selection_sha256']
    if (documents.get('package_schema') != 2 or documents.get('include_sources') is not baseline
            or documents.get('scope') != scope or documents.get('selection_sha256') != selection):
        raise ValueError('Source document manifest scope differs from its checkpoint')
    assets = documents.get('assets')
    if not isinstance(assets, list) or not 1 <= len(assets) <= 999:
        raise ValueError('Invalid source metadata asset membership')
    declared = {}
    for asset in assets:
        if (not isinstance(asset, dict) or not isinstance(asset.get('file'), str)
                or asset['file'] in declared or any(key not in asset for key in ('bytes', 'sha256'))
                or {key: asset[key] for key in ('file', 'bytes', 'sha256')} != node['expected'].get(asset['file'])):
            raise ValueError('Source metadata assets differ from their checkpoint')
        declared[asset['file']] = asset
    catalogs = [asset for asset in assets if asset.get('kind') == 'collection_catalog']
    if len(catalogs) != 1:
        raise ValueError('Exactly one pinned source catalog is required')
    asset = catalogs[0]
    catalog = metadata_file(node, asset['file'], asset['sha256'], downloader)
    if catalog.get('catalog_schema') != 1 or catalog.get('scope') != scope or catalog.get('selection_sha256') != selection:
        raise ValueError('Source catalog scope differs from its checkpoint')
    if not isinstance(catalog.get('sources'), dict) or not isinstance(catalog.get('index_sources'), list):
        raise ValueError('Invalid source catalog tables')
    rows = {}
    for table in SOURCE_TABLES:
        values = list(catalog[table].values()) if table == 'sources' else catalog[table]
        for row in values:
            if (not isinstance(row, dict) or not isinstance(row.get('source_key'), str)
                    or not re.fullmatch(r'\d{4}Q[1-4]', row['source_key'])
                    or not valid_hash(row.get('sha256')) or type(row.get('bytes')) is not int
                    or not 0 < row['bytes'] <= MAX_SOURCE_BYTES or not isinstance(row.get('url'), str)):
                raise ValueError('Invalid source catalog record')
            key = (table, row['source_key'])
            if key in rows or (table == 'sources' and catalog[table].get(row['source_key']) != row):
                raise ValueError('Duplicate or mismatched source catalog identity')
            check_url(row['url'])
            source_path(Path('/source-cache-check'), table, row)
            rows[key] = row
    if not baseline:
        for table in SOURCE_TABLES:
            if sum(key[0] == table for key in rows) != checkpoint['target_inventory']['tables'].get(table, 0):
                raise ValueError('Source catalog counts differ from the target inventory')
    return rows, assets


def changed_source_assets(node):
    assets = node['manifest'].get('source_assets')
    if not isinstance(assets, list):
        raise ValueError('Missing incremental source asset declarations')
    result = {}
    for asset in assets:
        if (not isinstance(asset, dict) or asset.get('source_table') not in SOURCE_TABLES
                or not isinstance(asset.get('source_key'), str) or not re.fullmatch(r'\d{4}Q[1-4]', asset['source_key'])
                or not valid_hash(asset.get('raw_sha256')) or type(asset.get('raw_bytes')) is not int
                or not 0 < asset['raw_bytes'] <= MAX_SOURCE_BYTES
                or any(key not in asset for key in ('file', 'bytes', 'sha256'))
                or {key: asset[key] for key in ('file', 'bytes', 'sha256')} != node['expected'].get(asset['file'])):
            raise ValueError('Incremental source asset differs from its checkpoint')
        key = (asset['source_table'], asset['source_key'])
        if key in result:
            raise ValueError('Duplicate incremental source asset identity')
        result[key] = asset
    return result


def plan(chain, downloader, quarters):
    quarters = quarter_keys(quarters)
    target_rows, _ = source_catalog(chain[-1], downloader)
    base_rows, base_assets = source_catalog(chain[0], downloader)
    changes = [(node, changed_source_assets(node)) for node in chain[1:]]
    selected, destinations = [], set()
    for quarter in quarters:
        if not any((table, quarter) in target_rows for table in SOURCE_TABLES):
            raise ValueError('Requested source quarter is absent from the pinned inventory: ' + quarter)
        for table in SOURCE_TABLES:
            key = (table, quarter)
            row = target_rows.get(key)
            if row is None:
                continue
            asset = node = None
            for candidate, available in reversed(changes):
                if key in available:
                    asset, node = available[key], candidate
                    if asset['raw_sha256'] != row['sha256'] or asset['raw_bytes'] != row['bytes']:
                        raise ValueError('Latest source version differs from the target catalog')
                    break
            if node is None:
                old = base_rows.get(key)
                if old is None or old['sha256'] != row['sha256'] or old['bytes'] != row['bytes']:
                    raise ValueError('Source version is missing from its pinned ancestor chain')
                kind = 'SEC_quarter_zip' if table == 'sources' else 'SEC_index_gzip'
                matches = [item for item in base_assets if item.get('kind') == kind and item.get('url') == old['url']]
                if len(matches) != 1:
                    raise ValueError('Baseline source does not have exactly one matching asset')
                asset, node = matches[0], chain[0]
                digest = asset['sha256'] if table == 'sources' else asset.get('raw_sha256')
                if digest != row['sha256']:
                    raise ValueError('Baseline source checksum differs from its catalog')
            if table == 'sources' and (asset['sha256'] != row['sha256'] or asset['bytes'] != row['bytes']):
                raise ValueError('Quarter ZIP differs from its original source bytes')
            relative = source_path(Path('/source-cache-check'), table, row).relative_to('/source-cache-check')
            paths = [relative, relative.with_suffix('.json')] if table == 'index_sources' else [relative]
            if any(path in destinations for path in paths):
                raise ValueError('Selected source cache paths collide')
            destinations.update(paths)
            downloader.plan(node['locator']['tag'], remote_name(node, asset), asset['bytes'], asset['sha256'])
            selected.append({'table': table, 'row': row, 'node': node, 'asset': asset, 'relative': relative})
    return {'quarters': quarters, 'catalog_rows': target_rows, 'selected': selected,
            'raw_bytes': sum(item['row']['bytes'] for item in selected)}


def restore(selection, downloader, root):
    root = Path(root).resolve()
    db = readonly(root / 'inventory.sqlite3')
    try:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        actual = {(table, row['source_key']): dict(row) for table in SOURCE_TABLES if table in tables
                  for row in db.execute('SELECT * FROM ' + table)}
        if actual != selection['catalog_rows']:
            raise ValueError('Source catalog rows differ from the fully restored inventory')
    finally:
        db.close()
    tasks = {}
    for item in selection['selected']:
        node, asset = item['node'], item['asset']
        tag, name = node['locator']['tag'], remote_name(node, asset)
        tasks[(tag, name)] = (tag, name, asset['bytes'], asset['sha256'])
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda task: downloader.get(*task), tasks.values()))
    retained = []
    for item in selection['selected']:
        table, row, node, asset = (item[key] for key in ('table', 'row', 'node', 'asset'))
        source = downloader.downloaded[(node['locator']['tag'], remote_name(node, asset))]
        destination = source_path(root, table, row)
        destination.parent.mkdir(parents=True, exist_ok=True)
        size = 0
        opener = gzip.open if table == 'index_sources' else open
        with opener(source, 'rb') as stream, destination.open('xb') as target:
            for body in iter(lambda: stream.read(1024 * 1024), b''):
                size += len(body)
                if size > row['bytes']:
                    raise ValueError('Selected source exceeds its declared original byte length')
                target.write(body)
        if size != row['bytes'] or file_hash(destination) != row['sha256']:
            raise ValueError('Selected source bytes differ from the restored inventory')
        if table == 'index_sources':
            metadata = destination.with_suffix('.json')
            if metadata.exists():
                raise ValueError('Selected index cache metadata already exists')
            atomic_write(metadata, canonical({'url': row['url'], 'sha256': row['sha256'], 'bytes': row['bytes'],
                                             'retrieved_at_utc': row['retrieved_at']}))
        retained.append({'table': table, 'source_key': row['source_key'], 'sha256': row['sha256'],
                         'bytes': size, 'path': str(destination.relative_to(root))})
    report = {'source_cache_verified': True, 'source_catalog_matches_inventory': True,
              'source_quarters': selection['quarters'], 'selected_source_files_verified': len(retained),
              'selected_source_raw_bytes': selection['raw_bytes'],
              'selected_sources_sha256': hashlib.sha256(canonical(retained)).hexdigest(),
              'quarter_zip_keys': [item['source_key'] for item in retained if item['table'] == 'sources'],
              'index_source_keys': [item['source_key'] for item in retained if item['table'] == 'index_sources']}
    atomic_write(root / 'selected-source-report.json', canonical({**report, 'files': retained}))
    return report
