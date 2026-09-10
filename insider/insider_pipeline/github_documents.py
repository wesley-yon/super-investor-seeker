"""Retrieve selected archived documents through a complete pinned lookup index."""
from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import re
import shutil
import zipfile

from . import document_index as index
from . import github_chain as transport
from .audit_batches import readonly
from .http import atomic_write
from .increment import shard_path
from .inventory import canonical
from .restore import insert_row, update_digest
from .runner import shard_connect


def remote_json(tag, pin, remote, downloader):
    name = transport.blob_name(pin)
    asset = remote.get(name)
    if asset is None or type(asset['size']) is not int or not 0 < asset['size'] <= transport.MAX_METADATA_BYTES:
        raise ValueError('Missing or oversized pinned document-index metadata')
    transport.check_asset(remote, name, asset['size'], pin)
    path = downloader.get(tag, name, asset['size'], pin)
    return path, index.pinned_json(path, pin)


def plan(chain, downloader, index_pin, selection_pin):
    if not transport.valid_hash(index_pin) or not transport.valid_hash(selection_pin):
        raise ValueError('Selected documents require pinned index and private selection checksums')
    return _plan(chain, downloader, index_pin, selection_pin=selection_pin)


def local_selection(index_pin, accessions):
    if (not transport.valid_hash(index_pin) or not isinstance(accessions, (list, tuple))
            or not 1 <= len(accessions) <= 100000
            or any(not isinstance(value, str) or not re.fullmatch(index.ACCESSION, value) for value in accessions)
            or len(set(accessions)) != len(accessions)):
        raise ValueError('Local document recovery requires distinct accessions and a pinned index')
    selected = {'filing_selection_schema': 1, 'document_index_sha256': index_pin, 'accessions': sorted(accessions)}
    if len(canonical(selected)) > transport.MAX_METADATA_BYTES:
        raise ValueError('Local document selection exceeds the bounded metadata size')
    return selected


def plan_local(chain, downloader, index_pin, accessions):
    selected = local_selection(index_pin, accessions)
    return _plan(chain, downloader, index_pin, selected=selected)


def cache_index(chain, downloader, index_pin):
    """Load the complete pinned index without selecting or downloading originals."""
    target = chain[-1]
    tag = target['locator']['tag']
    remote = transport.asset_map(transport.api('repos/' + transport.REPOSITORY + '/releases/' +
                                              str(target['release_id']) + '/assets?per_page=100', pages=True), bucket=True)
    manifest_path, manifest = remote_json(tag, index_pin, remote, downloader)
    index.validate_manifest(manifest, chain)
    part = manifest['index_file']
    transport.check_asset(remote, transport.blob_name(part['sha256']), part['bytes'], part['sha256'])
    index_path = downloader.get(tag, transport.blob_name(part['sha256']), part['bytes'], part['sha256'])
    directory = downloader.directory.parent / 'document-index'
    if directory.is_symlink():
        raise ValueError('Document-index cache must not be linked')
    directory.mkdir(exist_ok=True)
    for source, path in ((manifest_path, directory / index.MANIFEST), (index_path, directory / part['file'])):
        if path.exists() or path.is_symlink():
            if path.is_symlink() or not path.is_file() or path.stat().st_size != source.stat().st_size or index.file_hash(path) != index.file_hash(source):
                raise ValueError('Retained document-index cache differs from the pinned source')
        else:
            shutil.copyfile(source, path)
    return {'tag': tag, 'remote': remote, 'manifest': manifest, 'directory': directory,
            'index_path': directory / part['file']}


def _plan(chain, downloader, index_pin, selection_pin=None, selected=None):
    cached = cache_index(chain, downloader, index_pin)
    tag, remote, manifest = cached['tag'], cached['remote'], cached['manifest']
    selection_source = 'local' if selected is not None else 'private_archive_asset'
    if selected is None:
        selection_path, selected = remote_json(tag, selection_pin, remote, downloader)
        if (not isinstance(selected, dict) or set(selected) != {'filing_selection_schema', 'document_index_sha256', 'accessions'}
                or type(selected['filing_selection_schema']) is not int
                or selected != index.selection(index_pin, selected['accessions'])):
            raise ValueError('Private filing selection differs from its pinned document index')
    else:
        selection_pin = hashlib.sha256(canonical(selected)).hexdigest()
    index_path = cached['index_path']
    wanted = set(selected['accessions'])
    chosen = []
    for row in index.records(index_path, manifest):
        if row[0] in wanted:
            chosen.append(row)
    if len(chosen) != len(wanted):
        raise ValueError('A selected filing is absent from the complete committed document index')
    nodes = {node['reference']['manifest_sha256']: node for node in chain}
    tasks = {}
    for number in sorted({row[2] for row in chosen}):
        chunk = manifest['chunks'][number]
        node = nodes[chunk['checkpoint_sha256']]
        name = chunk['file'] if node['locator']['layout'] == 'legacy_baseline' else transport.blob_name(chunk['sha256'])
        task = (node['locator']['tag'], name, chunk['bytes'], chunk['sha256'])
        downloader.plan(*task)
        tasks[number] = task
    # Keep a reusable index for the next incremental writer. Private selections
    # remain in local files; only their hashes and counts enter the public report.
    directory = cached['directory']
    if selection_source == 'private_archive_asset':
        shutil.copyfile(selection_path, directory / 'filing-selection.json')
    else:
        atomic_write(directory / 'filing-selection.json', canonical(selected))
    return {'manifest': manifest, 'index_path': index_path, 'chosen': chosen, 'tasks': tasks,
            'index_pin': index_pin, 'selection_pin': selection_pin, 'selection_source': selection_source,
            'decoded_bytes': sum(row[4] for row in chosen),
            'selection_sha256': hashlib.sha256(canonical(selected['accessions'])).hexdigest()}


def restore(selection, downloader, root):
    root = Path(root).resolve()
    if (root / 'shards').exists() or (root / 'shards').is_symlink():
        raise ValueError('Selected document shards require a fresh inventory-only root')
    manifest = selection['manifest']
    index.verify_inventory(selection['index_path'], manifest, root / 'inventory.sqlite3')
    inventory_db = readonly(root / 'inventory.sqlite3')
    connections = OrderedDict()
    try:
        targets = {}
        for row in selection['chosen']:
            inventory = dict(inventory_db.execute('SELECT * FROM filings WHERE accession=?', (row[0],)).fetchone())
            shard_path(root, inventory['shard'])
            targets[row[0]] = inventory
        tasks = set(selection['tasks'].values())
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda task: downloader.get(*task), sorted(tasks)))
        expected = {}
        for number, task in selection['tasks'].items():
            path = downloader.downloaded[(task[0], task[1])]
            with zipfile.ZipFile(path) as archive:
                names = archive.namelist()
                if (not 1 <= len(names) <= 50000 or len(names) != len(set(names))
                        or any(not re.fullmatch('filings/' + index.ACCESSION + r'\.json\.gz', name) for name in names)):
                    raise ValueError('Selected document chunk has invalid or repeated member paths')
                for accession, inventory_hash, chunk_number, compressed, decoded in selection['chosen']:
                    if chunk_number != number:
                        continue
                    inventory, document, _, _ = index.member(archive, accession, compressed, decoded)
                    if inventory != targets[accession] or index.row_hash(inventory) != inventory_hash:
                        raise ValueError('Selected archived document differs from its complete inventory row')
                    relative = inventory['shard']
                    if relative not in connections:
                        if len(connections) >= 16:
                            _, oldest = connections.popitem(last=False)
                            oldest.execute('PRAGMA wal_checkpoint(TRUNCATE)'); oldest.close()
                        connections[relative] = shard_connect(shard_path(root, relative))
                    connections.move_to_end(relative)
                    with connections[relative]:
                        insert_row(connections[relative], 'documents', document)
                    digest = hashlib.sha256(); update_digest(digest, inventory, document)
                    expected[accession] = digest.hexdigest()
        for db in connections.values():
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)'); db.close()
        connections.clear()
        digest = hashlib.sha256()
        for accession in sorted(targets):
            inventory = targets[accession]
            db = readonly(shard_path(root, inventory['shard']))
            try:
                document = db.execute('SELECT * FROM documents WHERE accession=?', (accession,)).fetchone()
                current = hashlib.sha256(); update_digest(current, inventory, document)
                if current.hexdigest() != expected[accession]:
                    raise ValueError('Selected document database readback differs from archived rows')
                update_digest(digest, inventory, document)
            finally:
                db.close()
        report = {'document_index_complete_membership_verified': True, 'document_index_sha256': selection['index_pin'],
                  'filing_selection_sha256': selection['selection_pin'],
                  'filing_selection_source': selection['selection_source'],
                  'selected_documents_verified': len(targets), 'selected_document_chunks': len(tasks),
                  'selected_accession_set_sha256': selection['selection_sha256'],
                  'selected_documents_rows_sha256': digest.hexdigest(), 'includes_original_documents': True,
                  'selected_documents_scope': 'requested_accessions_only'}
        atomic_write(root / 'selected-document-report.json', canonical(report))
        return report
    finally:
        inventory_db.close()
        for db in connections.values():
            db.close()
