"""Recover pinned checkpoint chains from private, content-addressed releases.

Each small, independently pinned transport descriptor names an immutable
increment and its exact parent locator. Blob names are full SHA-256 hashes, so
multiple checkpoints can share one monthly release without filename collisions.
Only the legacy baseline uses its original logical release-asset names.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import shutil
import tempfile
import time

from .audit_batches import file_hash, run as audit
from .github_read import MAX_DOWNLOAD_BYTES, MIN_FREE_BYTES, check_remote_assets, validate_release
from .github_stage import REPOSITORY, api, command
from .http import atomic_write
from .increment import MANIFEST, restore as restore_increment, verify as verify_increment
from .inventory import canonical
from .inventory_archive import describe, restore as restore_inventory
from .inventory_delta import MANIFEST as DELTA_MANIFEST, MAX_RAW_BYTES, restore as replay_delta

BUCKET_MARKER = 'Insider content-addressed checkpoint storage (v1).'
MAX_CHAIN = 32
MAX_METADATA_BYTES = 2_000_000


def valid_hash(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def validate_locator(value):
    if (not isinstance(value, dict) or set(value) != {'layout', 'tag', 'sha256'}
            or value['layout'] not in ('legacy_baseline', 'content_addressed')
            or not isinstance(value['tag'], str) or not re.fullmatch(r'insider-[A-Za-z0-9._-]+', value['tag'])
            or not valid_hash(value['sha256'])):
        raise ValueError('An exact insider tag, layout, and pinned SHA-256 are required')
    if value['layout'] == 'content_addressed' and not re.fullmatch(r'insider-archives-\d{6}-\d{3}', value['tag']):
        raise ValueError('Content-addressed checkpoints require an insider archive bucket')
    return value


def descriptor(increment, increment_pin, parent):
    validate_locator(parent)
    if increment.get('increment_schema') != 1 or not valid_hash(increment_pin):
        raise ValueError('Expected a pinned incremental checkpoint')
    return {'transport_schema': 1, 'checkpoint': {'kind': 'increment', 'manifest_sha256': increment_pin},
            'parent': dict(parent)}


def descriptor_name(pin):
    if not valid_hash(pin):
        raise ValueError('Invalid transport checksum')
    return 'checkpoint-' + pin + '.json'


def blob_name(pin):
    if not valid_hash(pin):
        raise ValueError('Invalid blob checksum')
    return 'blob-' + pin


def validate_bucket(release, tag, writable=False):
    if (not re.fullmatch(r'insider-archives-\d{6}-\d{3}', tag)
            or release.get('tag_name') != tag or (release.get('body') or '').splitlines()[:1] != [BUCKET_MARKER]
            or (release.get('draft') is not True and (release.get('prerelease') is not True or not release.get('published_at')))
            or (writable and release.get('immutable') is True)):
        raise ValueError('Expected the matching insider archive bucket with latest-release protection')


def asset_map(assets, bucket=False):
    if len(assets) > 1000 or len({row['name'] for row in assets}) != len(assets):
        raise ValueError('Release assets exceed capacity or contain duplicate names')
    if bucket and any(not re.fullmatch(r'(blob-[0-9a-f]{64}|checkpoint-[0-9a-f]{64}\.json)', row['name']) for row in assets):
        raise ValueError('Archive bucket contains an unrelated asset')
    return {row['name']: row for row in assets}


def check_asset(remote, name, size, pin):
    asset = remote.get(name)
    if (asset is None or asset.get('state') != 'uploaded' or asset.get('size') != size
            or asset.get('digest') not in (None, 'sha256:' + pin)):
        raise ValueError('Remote checkpoint asset is missing or differs from its pinned identity')


def declared_files(manifest, manifest_name, pin, manifest_size):
    expected = {}
    files = manifest.get('files')
    if not isinstance(files, list) or not 1 <= len(files) <= 999:
        raise ValueError('Invalid checkpoint asset count')
    for row in files:
        if (set(row) != {'file', 'bytes', 'sha256'} or not isinstance(row['file'], str)
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', row['file'])
                or row['file'] in expected or row['file'] == manifest_name
                or not valid_hash(row['sha256']) or type(row['bytes']) is not int
                or not 0 < row['bytes'] <= 1_000_000_000):
            raise ValueError('Invalid checkpoint asset declaration')
        expected[row['file']] = row
    expected[manifest_name] = {'file': manifest_name, 'sha256': pin, 'bytes': manifest_size}
    return expected


class Downloader:
    """Download into a private cache; verify every byte before exposing a file."""
    def __init__(self, directory, budget=MAX_DOWNLOAD_BYTES):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True)
        self.budget = budget
        self.planned = {}
        self.downloaded = {}

    def plan(self, tag, name, size, pin):
        if type(size) is not int or not 0 < size <= 1_000_000_000 or not valid_hash(pin):
            raise ValueError('Invalid declared download size or checksum')
        key = (tag, name)
        wanted = (size, pin)
        if key in self.planned and self.planned[key] != wanted:
            raise ValueError('One remote name has conflicting checkpoint identities')
        self.planned[key] = wanted
        if sum(item[0] for item in self.planned.values()) > self.budget:
            raise ValueError('The complete checkpoint chain exceeds its cloud download budget')

    def get(self, tag, name, size, pin):
        key = (tag, name)
        self.plan(tag, name, size, pin)
        if key in self.downloaded:
            return self.downloaded[key]
        folder = Path(tempfile.mkdtemp(prefix='asset-', dir=self.directory))
        command(['release', 'download', tag, '--repo', REPOSITORY, '--pattern', name, '--dir', str(folder)])
        path = folder / name
        if ({p.name for p in folder.iterdir()} != {name} or path.is_symlink()
                or path.stat().st_size != size or file_hash(path) != pin):
            raise ValueError('Downloaded checkpoint bytes differ from their pinned identity')
        self.downloaded[key] = path
        return path


def metadata(locator, directory, downloader, seen=None):
    """Resolve exact parents before downloading the large archive assets."""
    validate_locator(locator)
    seen = set() if seen is None else seen
    identity = (locator['layout'], locator['sha256'])
    if identity in seen or len(seen) >= MAX_CHAIN:
        raise ValueError('Checkpoint chain cycles or requires compaction')
    seen.add(identity)
    tag, pin = locator['tag'], locator['sha256']
    release = api('repos/' + REPOSITORY + '/releases/tags/' + tag)
    validate_release({'full_name': REPOSITORY, 'private': True}, release, tag)
    is_blob = locator['layout'] == 'content_addressed'
    if is_blob:
        validate_bucket(release, tag)
    remote = asset_map(api('repos/' + REPOSITORY + '/releases/' + str(release['id']) + '/assets?per_page=100', pages=True), bucket=is_blob)
    name = descriptor_name(pin) if is_blob else 'baseline.json'
    head = remote.get(name)
    if head is None or type(head['size']) is not int or not 0 < head['size'] <= MAX_METADATA_BYTES:
        raise ValueError('Missing or oversized checkpoint metadata')
    check_asset(remote, name, head['size'], pin)
    downloaded = downloader.get(tag, name, head['size'], pin)
    value = json.loads(downloaded.read_text())
    if is_blob:
        if (set(value) != {'transport_schema', 'checkpoint', 'parent'} or value['transport_schema'] != 1
                or set(value['checkpoint']) != {'kind', 'manifest_sha256'} or value['checkpoint']['kind'] != 'increment'
                or not valid_hash(value['checkpoint']['manifest_sha256'])):
            raise ValueError('Unsupported checkpoint transport descriptor')
        validate_locator(value['parent'])
        checkpoint_pin, manifest_name = value['checkpoint']['manifest_sha256'], MANIFEST
        remote_name = blob_name(checkpoint_pin)
        head = remote.get(remote_name)
        if head is None or type(head['size']) is not int or not 0 < head['size'] <= MAX_METADATA_BYTES:
            raise ValueError('Missing or oversized incremental manifest')
        check_asset(remote, remote_name, head['size'], checkpoint_pin)
        manifest_path = downloader.get(tag, remote_name, head['size'], checkpoint_pin)
        manifest = json.loads(manifest_path.read_text())
        if manifest.get('increment_schema') != 1:
            raise ValueError('Expected an incremental manifest')
    else:
        checkpoint_pin, manifest_name, manifest_path, manifest = pin, 'baseline.json', downloaded, value
        if manifest.get('baseline_schema') != 1:
            raise ValueError('Expected a legacy baseline manifest')
    expected = declared_files(manifest, manifest_name, checkpoint_pin, manifest_path.stat().st_size)
    for asset in expected.values():
        remote_name = blob_name(asset['sha256']) if is_blob else asset['file']
        check_asset(remote, remote_name, asset['bytes'], asset['sha256'])
    if not is_blob:
        check_remote_assets(list(remote.values()), expected)
    local = Path(directory) / checkpoint_pin
    local.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(manifest_path, local / manifest_name)
    node = {'locator': locator, 'reference': {'kind': 'increment' if is_blob else 'baseline', 'manifest_sha256': checkpoint_pin},
            'manifest': manifest, 'manifest_name': manifest_name, 'expected': expected, 'directory': local,
            'release_id': release['id']}
    parents = metadata(value['parent'], directory, downloader, seen) if is_blob else []
    if is_blob and parents[-1]['reference'] != manifest.get('parent'):
        raise ValueError('Transport parent differs from the parent pinned by the increment')
    return [*parents, node]


def download_archives(chain, downloader, inventory_only=False):
    tasks = {}
    for node in chain:
        tag = node['locator']['tag']
        for asset in node['inventory_assets' if inventory_only else 'expected'].values():
            remote_name = blob_name(asset['sha256']) if node['locator']['layout'] == 'content_addressed' else asset['file']
            downloader.plan(tag, remote_name, asset['bytes'], asset['sha256'])
            tasks[(tag, remote_name)] = (tag, remote_name, asset['bytes'], asset['sha256'])
    # Planning is serial; independent downloads use a small bounded pool.
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda task: downloader.get(*task), tasks.values()))
    for node in chain:
        for asset in node['inventory_assets' if inventory_only else 'expected'].values():
            remote_name = blob_name(asset['sha256']) if node['locator']['layout'] == 'content_addressed' else asset['file']
            path = downloader.downloaded[(node['locator']['tag'], remote_name)]
            shutil.copyfile(path, node['directory'] / asset['file'])


def select_inventory_assets(chain, downloader, output):
    """Bind each inventory manifest to its checkpoint before planning its parts.

    Original documents, source ZIPs, indexes, and audits stay in their archives.
    Their remote declarations are checked by metadata(); their bytes are not read.
    """
    previous = None
    growth = 0
    for index, node in enumerate(chain):
        checkpoint = node['manifest']
        name = 'inventory-manifest.json' if index == 0 else DELTA_MANIFEST
        pin_key = 'inventory_manifest_sha256' if index == 0 else 'inventory_delta_manifest_sha256'
        asset = node['expected'].get(name)
        if (asset is None or asset['sha256'] != checkpoint.get(pin_key)
                or not 0 < asset['bytes'] <= MAX_METADATA_BYTES):
            raise ValueError('Inventory metadata differs from the pinned checkpoint')
        remote_name = asset['file'] if index == 0 else blob_name(asset['sha256'])
        path = downloader.get(node['locator']['tag'], remote_name, asset['bytes'], asset['sha256'])
        value = json.loads(path.read_text())
        raw_bytes = value.get('raw_bytes')
        if (type(raw_bytes) is not int or not 0 <= raw_bytes <= MAX_RAW_BYTES
                or not valid_hash(value.get('raw_sha256'))
                or value.get('compression') != 'gzip_concatenated_parts'):
            raise ValueError('Inventory metadata has an invalid schema or decoded size')
        if index == 0:
            if (value.get('inventory_archive_schema') != 1 or not raw_bytes
                    or value['scope'] != checkpoint['scope']
                    or value['tables']['filings'] != checkpoint['inventory_filings']
                    or value['committed_documents'] != checkpoint['documents']
                    or value['committed_selection_sha256'] != checkpoint['selection_sha256']
                    or value['queue_counts'] != checkpoint['queue_counts_at_checkpoint']):
                raise ValueError('Base inventory coverage differs from the pinned baseline')
            estimated_inventory = raw_bytes
        else:
            if (value.get('inventory_delta_schema') != 1
                    or checkpoint.get('requires_parent_checkpoint') is not True
                    or checkpoint.get('standalone_restore_ready') is not False
                    or checkpoint.get('complete_backfill') is not False
                    or value['target'] != checkpoint['target_inventory_state']
                    or {table: row['rows'] for table, row in value['target']['tables'].items()} != checkpoint['target_inventory']['tables']):
                raise ValueError('Incremental inventory coverage differs from its pinned checkpoint')
            if index == 1:
                if (checkpoint['parent_inventory_file_sha256'] != previous['raw_sha256']
                        or {table: row['rows'] for table, row in value['parent']['tables'].items()} != previous['tables']):
                    raise ValueError('First inventory delta differs from its baseline parent')
            elif value['parent'] != previous['target']:
                raise ValueError('Inventory deltas do not form the pinned sequence of states')
            growth += raw_bytes
        parts = value.get('parts')
        if not isinstance(parts, list) or not 1 <= len(parts) <= 997:
            raise ValueError('Invalid selected inventory part count')
        selected = {node['manifest_name']: node['expected'][node['manifest_name']], name: asset}
        prefix = 'inventory' if index == 0 else 'inventory-delta'
        for number, part in enumerate(parts, 1):
            if (not isinstance(part, dict) or set(part) != {'file', 'bytes', 'sha256'}
                    or not valid_hash(part['sha256'])
                    or part['file'] != f'{prefix}-{number:05d}-{part["sha256"][:16]}.gz.part'
                    or part['file'] in selected or part != node['expected'].get(part['file'])):
                raise ValueError('Inventory parts differ from the pinned checkpoint assets')
            selected[part['file']] = part
        node['inventory_assets'], node['inventory_metadata'] = selected, value
        shutil.copyfile(path, node['directory'] / name)
        previous = value
    # Plan the entire selected chain before downloading any large part. Include
    # both inventory copies and SQLite journal growth during sequential replay.
    for node in chain:
        for asset in node['inventory_assets'].values():
            name = asset['file'] if node['locator']['layout'] == 'legacy_baseline' else blob_name(asset['sha256'])
            downloader.plan(node['locator']['tag'], name, asset['bytes'], asset['sha256'])
    required_free = 3 * (estimated_inventory + 4 * growth) + 2 * sum(size for size, _ in downloader.planned.values()) + 2 * 1024**3
    if shutil.disk_usage(output).free < required_free:
        raise ValueError('Insufficient free disk for the declared inventory chain')
    return required_free


def restore_parent_inventory(chain, output):
    """Rebuild the immediate parent's inventory for changed-document audit."""
    first = chain[0]
    destination = output / '0'
    restore_inventory(first['directory'], destination, first['manifest']['inventory_manifest_sha256'])
    for index, node in enumerate(chain[1:], 1):
        previous, destination = destination, output / str(index)
        replay_delta(previous / 'inventory.sqlite3', node['directory'], destination,
                     node['manifest']['inventory_delta_manifest_sha256'])
        if describe(destination / 'inventory.sqlite3') != node['manifest']['target_inventory']:
            raise ValueError('Parent inventory coverage differs from the pinned checkpoint')
        shutil.rmtree(previous)
    return destination / 'inventory.sqlite3'


def inventory_chain(chain, downloader, output, source_quarters=None):
    required_free = select_inventory_assets(chain, downloader, output)
    source_selection = None
    if source_quarters is not None:
        from . import github_sources
        planned_before = sum(size for size, _ in downloader.planned.values())
        source_selection = github_sources.plan(chain, downloader, source_quarters)
        extra_downloads = sum(size for size, _ in downloader.planned.values()) - planned_before
        if shutil.disk_usage(output).free < required_free + 2 * extra_downloads + source_selection['raw_bytes']:
            raise ValueError('Insufficient free disk for the selected inventory and source cache')
    download_archives(chain, downloader, inventory_only=True)
    final_inventory = restore_parent_inventory(chain, output / 'inventory-work')
    root = chain[-1]
    verified = describe(final_inventory)
    if verified != root['manifest']['target_inventory']:
        raise ValueError('Final inventory coverage differs from its pinned checkpoint')
    sources = github_sources.restore(source_selection, downloader, final_inventory.parent) if source_selection else {}
    # Every replay checked every table row, not only the changed or committed rows.
    final_inventory.parent.rename(output / 'restored')
    return {'documents': verified['committed_documents'], 'inventory_filings': verified['tables']['filings'],
            'inventory_only_restore_verified': True, 'inventory_logical_state_verified': True,
            'every_inventory_row_verified': True, 'inventory_byte_identical': False,
            'base_inventory_byte_identical': True,
            'inventory_state_sha256': root['inventory_metadata']['target']['state_sha256'],
            'inventory_tables': verified['tables'], 'inventory_queue_counts': verified['queue_counts'],
            'inventory_sqlite_integrity_check': verified['sqlite_integrity_check'],
            'inventory_file_sha256': file_hash(output / 'restored/inventory.sqlite3'),
            'full_restore_verified': False, 'includes_original_documents': False,
            'collection_resume_ready': False, 'source_audit_performed': False, **sources}


def read_chain(tag, transport_pin, output, workers=4, inventory_only=False, source_quarters=None):
    if source_quarters is not None:
        from .github_sources import quarter_keys
        source_quarters = quarter_keys(source_quarters)
        if not inventory_only:
            raise ValueError('Selected source quarters require inventory-only mode')
    locator = validate_locator({'layout': 'content_addressed', 'tag': tag, 'sha256': transport_pin})
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('Cloud chain verification output must be a fresh directory')
    output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output.parent).free < MIN_FREE_BYTES:
        raise ValueError('Insufficient free disk for the checkpoint chain restore')
    repository = api('repos/' + REPOSITORY)
    if repository['full_name'] != REPOSITORY or repository['private'] is not True:
        raise ValueError('The insider archive repository must remain private')
    latest_before = api('repos/' + REPOSITORY + '/releases/latest')
    started = time.monotonic()
    downloader = Downloader(output / 'cache', budget=MAX_DOWNLOAD_BYTES)
    chain = metadata(locator, output / 'archives', downloader)
    if inventory_only:
        verified = inventory_chain(chain, downloader, output, source_quarters)
        latest_after = api('repos/' + REPOSITORY + '/releases/latest')
        if not latest_after['tag_name'].startswith('dataset-'):
            raise ValueError('The normal dataset release pointer is not selected')
        report = {**verified, 'repository': REPOSITORY, 'tag': tag, 'transport_sha256': transport_pin,
                  'release_id': chain[-1]['release_id'], 'checkpoints': len(chain),
                  'assets': len(downloader.downloaded), 'asset_bytes': sum(size for size, _ in downloader.planned.values()),
                  'read_only_operations': True, 'latest_dataset_release_before': latest_before['id'],
                  'latest_dataset_release_after': latest_after['id'],
                  'latest_release_unchanged': latest_before['id'] == latest_after['id'],
                  'elapsed_seconds': round(time.monotonic() - started, 3),
                  'remaining_free_bytes': shutil.disk_usage(output).free,
                  'cloud_daily_maintenance_active': False, 'complete_backfill': False}
        atomic_write(output / 'cloud-verification.json', canonical(report))
        return report
    download_archives(chain, downloader)
    root, parent = chain[-1], chain[-2]
    verify_increment(root['directory'], root['reference']['manifest_sha256'])
    # Include scratch parent inventories and recursively restored parent states.
    inventory_manifest = json.loads((chain[0]['directory'] / 'inventory-manifest.json').read_text())
    required_free = ((len(chain) + 3) * inventory_manifest['raw_bytes']
                     + 4 * sum(size for size, _ in downloader.planned.values()) + 2 * 1024**3)
    if shutil.disk_usage(output).free < required_free:
        raise ValueError('Insufficient free disk for the declared checkpoint chain')
    restored = restore_increment(root['directory'], parent['directory'] / parent['manifest_name'], output / 'restored',
                                 root['reference']['manifest_sha256'],
                                 ancestor_manifests=[node['directory'] / node['manifest_name'] for node in chain[:-2]])
    parent_inventory = restore_parent_inventory(chain[:-1], output / 'parent-inventory')
    changed = audit(output / 'restored', output / 'changed-audit', workers=workers,
                    inventory_snapshot=output / 'restored/inventory.sqlite3', parent_inventory=parent_inventory)
    if (changed['semantic_sha256'] != root['manifest']['changed_source_audit_sha256']
            or changed['selected_documents'] != restored['changed_documents']):
        raise ValueError('Cloud-restored changed-document audit differs from its archived audit')
    full = audit(output / 'restored', output / 'full-audit', workers=workers)
    if (full['selected_documents'] != restored['restored_documents']
            or any(full['counts'].get(key, 0) for key in ('document_failure', 'original_xml_field_failure'))):
        raise ValueError('Full source audit did not cover every restored document')
    latest_after = api('repos/' + REPOSITORY + '/releases/latest')
    if not latest_after['tag_name'].startswith('dataset-'):
        raise ValueError('The normal dataset release pointer is not selected')
    report = {**restored, 'repository': REPOSITORY, 'tag': tag, 'transport_sha256': transport_pin,
              'release_id': root['release_id'], 'checkpoints': len(chain),
              'documents': restored['restored_documents'], 'inventory_filings': root['manifest']['target_inventory']['tables']['filings'],
              'assets': len(downloader.downloaded), 'asset_bytes': sum(size for size, _ in downloader.planned.values()),
              'full_restore_verified': True, 'source_audit_performed': True, 'read_only_operations': True,
              'source_checks': full['counts'].get('original_xml_fields_checked', 0), 'source_audit_semantic_sha256': full['semantic_sha256'],
              'changed_source_checks': changed['counts'].get('original_xml_fields_checked', 0),
              'changed_source_audit_matches_archive': True, 'changed_source_audit_semantic_sha256': changed['semantic_sha256'],
              'latest_dataset_release_before': latest_before['id'], 'latest_dataset_release_after': latest_after['id'],
              'latest_release_unchanged': latest_before['id'] == latest_after['id'],
              'elapsed_seconds': round(time.monotonic() - started, 3), 'remaining_free_bytes': shutil.disk_usage(output).free,
              'cloud_daily_maintenance_active': False, 'complete_backfill': False}
    atomic_write(output / 'cloud-verification.json', canonical(report))
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tag', required=True)
    parser.add_argument('--transport-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workers', type=int, choices=range(1, 5), default=4)
    parser.add_argument('--inventory-only', action='store_true', help='Replay the complete inventory without filing documents; selected source loading is optional')
    parser.add_argument('--source-quarters', help='Space-separated YYYYQ1..YYYYQ4 source quarters; requires --inventory-only')
    args = parser.parse_args()
    quarters = args.source_quarters.split() if args.source_quarters is not None else None
    print(json.dumps(read_chain(args.tag, args.transport_sha256, args.output, args.workers, args.inventory_only, quarters), indent=2), flush=True)


if __name__ == '__main__':
    main()
