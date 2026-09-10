"""Create bounded immutable transport archives from an audited selection.

Each filing is a gzip JSON envelope inside a ZIP_STORED chunk. SQLite BLOBs use
explicit base64 encoding, preserving the original inventory and document rows.
The manifest proves membership and byte integrity, not complete market coverage.
"""
from __future__ import annotations
import argparse
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import zipfile

from .audit_batches import SELECTION_COLUMNS, file_hash, readonly, snapshot
from .http import atomic_write
from .inventory import canonical
from .locking import writer_lock

MAX_ASSET_BYTES = 1_000_000_000
DEFAULT_CHUNK_BYTES = 500 * 1024 * 1024
PACKAGE_SCHEMA = 2


def encode_row(row):
    return {key: {'bytes_base64': base64.b64encode(value).decode('ascii')} if isinstance(value, bytes) else value
            for key, value in dict(row).items()}


def decode_row(row):
    return {key: base64.b64decode(value['bytes_base64'], validate=True)
            if isinstance(value, dict) and set(value) == {'bytes_base64'} else value
            for key, value in row.items()}


def check_document(inventory, document):
    if inventory['accession'] != document['accession'] or inventory['source_url'] != document['source_url']:
        raise ValueError('Inventory/document source provenance differs')
    for name in ['source', 'parsed']:
        raw = gzip.decompress(document[name + '_gzip'])
        digest = hashlib.sha256(raw).hexdigest()
        if (digest != inventory[name + '_sha256'] or digest != document[name + '_sha256']
                or len(raw) != document[name + '_bytes']):
            raise ValueError('Archived ' + name + ' bytes fail checksum/length verification')


def copy_asset(source, output, name, expected):
    source, output = Path(source), Path(output)
    if file_hash(source) != expected:
        raise ValueError('Source asset checksum differs: ' + name)
    destination = output / name
    if destination.exists():
        if file_hash(destination) != expected:
            raise ValueError('Existing immutable asset changed: ' + name)
    else:
        os.link(source, destination)
    size = destination.stat().st_size
    if size > MAX_ASSET_BYTES:
        raise ValueError('Source asset needs additional chunking: ' + name)
    return {'file': name, 'sha256': expected, 'bytes': size}


def verify(output, expected_manifest_sha256=None):
    output = Path(output)
    if expected_manifest_sha256 and file_hash(output / 'manifest.json') != expected_manifest_sha256:
        raise ValueError('Manifest differs from its independently pinned checksum')
    manifest = json.loads((output / 'manifest.json').read_text())
    if manifest.get('package_schema') not in (1, PACKAGE_SCHEMA):
        raise ValueError('Unsupported package schema')
    filenames = [item['file'] for item in manifest['chunks'] + manifest['assets']]
    if (len(set(filenames)) != len(filenames) or any(not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', name) for name in filenames)):
        raise ValueError('Duplicate or unsafe package asset path')
    if any((output / name).resolve().parent != output.resolve() for name in filenames):
        raise ValueError('Package asset points outside the package directory')
    digest = hashlib.sha256()
    count = 0
    previous = None
    for chunk in manifest['chunks']:
        path = output / chunk['file']
        if path.stat().st_size != chunk['bytes'] or file_hash(path) != chunk['sha256'] or chunk['bytes'] > manifest['max_chunk_bytes']:
            raise ValueError('Chunk checksum or size differs: ' + chunk['file'])
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            if len(names) != chunk['documents'] or len(set(names)) != len(names):
                raise ValueError('Chunk member count or uniqueness differs')
            for name in names:
                if not re.fullmatch(r'filings/[0-9]{10}-[0-9]{2}-[0-9]{6}\.json\.gz', name):
                    raise ValueError('Unexpected archive member path')
                envelope = json.loads(gzip.decompress(archive.read(name)))
                inventory, document = decode_row(envelope['inventory']), decode_row(envelope['document'])
                if envelope['schema'] != 1 or name != 'filings/' + inventory['accession'] + '.json.gz':
                    raise ValueError('Filing identity differs from archive member')
                check_document(inventory, document)
                key = (inventory['filing_date'], inventory['accession'])
                if previous is not None and key <= previous:
                    raise ValueError('Archive membership is duplicated or out of order')
                previous = key
                digest.update(canonical({k: inventory[k] for k in SELECTION_COLUMNS}) + b'\n')
                count += 1
    if count != manifest['documents'] or digest.hexdigest() != manifest['selection_sha256']:
        raise ValueError('Archive membership differs from the audited selection')
    for asset in manifest['assets']:
        path = output / asset['file']
        if path.stat().st_size != asset['bytes'] or file_hash(path) != asset['sha256']:
            raise ValueError('Supporting asset checksum differs: ' + asset['file'])
        if asset['bytes'] > MAX_ASSET_BYTES:
            raise ValueError('Supporting asset exceeds the configured asset limit')
        if asset.get('raw_sha256'):
            if hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest() != asset['raw_sha256']:
                raise ValueError('Compressed source archive does not reproduce its original bytes')
    return {'documents': count, 'chunks': len(manifest['chunks']), 'assets': len(manifest['assets']),
            'manifest_sha256': file_hash(output / 'manifest.json'), 'verified': True,
            'complete_backfill': False}


def package(root, audit_directory, output, max_bytes=DEFAULT_CHUNK_BYTES, max_documents=50000, include_sources=True):
    root, audit_directory, output = Path(root).resolve(), Path(audit_directory).resolve(), Path(output).resolve()
    if not 1 <= max_bytes <= MAX_ASSET_BYTES or not 1 <= max_documents <= 50000:
        raise ValueError('Chunk size/count must stay within configured archive limits')
    with writer_lock(output):
        audit = json.loads((audit_directory / 'report.json').read_text())
        semantic = {k: audit[k] for k in ['selection_sha256', 'audit_version', 'counts', 'financial_table_comparisons', 'groups']}
        if hashlib.sha256(canonical(semantic)).hexdigest() != audit['semantic_sha256']:
            raise ValueError('Audit report integrity differs')
        if tuple(map(int, audit['audit_version'].split('.'))) < (3, 1, 0):
            raise ValueError('Submission-header audit is required before packaging')
        if any(audit['counts'].get(k, 0) for k in ['document_failure', 'original_xml_field_failure']):
            raise ValueError('Source audit failures must be resolved before packaging normalized data')
        metadata = snapshot(root, output / 'selection.sqlite3', reuse=audit_directory / 'selection.sqlite3')
        if metadata['selection_sha256'] != audit['selection_sha256'] or metadata['selected_documents'] <= 0:
            raise ValueError('Package selection is empty or differs from the audit')
        config = {'package_schema': PACKAGE_SCHEMA, 'selection_sha256': metadata['selection_sha256'], 'audit_semantic_sha256': audit['semantic_sha256'],
                  'max_chunk_bytes': max_bytes, 'max_documents_per_chunk': max_documents, 'include_sources': include_sources}
        frozen = metadata.get('inventory_snapshot')
        if frozen:
            if file_hash(frozen['path']) != frozen['sha256']:
                raise ValueError('Frozen inventory changed after the audit selection')
            config['inventory_snapshot_sha256'] = frozen['sha256']
        if (output / 'manifest.json').exists():
            existing = json.loads((output / 'manifest.json').read_text())
            if any(existing.get(k) != value for k, value in config.items()):
                raise ValueError('Existing package belongs to a different audit or configuration')
            return verify(output)
        progress_path = output / 'package-progress.json'
        if progress_path.exists():
            progress = json.loads(progress_path.read_text())
            if progress['config'] != config:
                raise ValueError('Resume configuration differs from this package')
            for chunk in progress['chunks']:
                if file_hash(output / chunk['file']) != chunk['sha256']:
                    raise ValueError('A completed package chunk changed')
        else:
            progress = {'config': config, 'chunks': [], 'after': None}
        inventory_db = readonly(frozen['path'] if frozen else root / 'inventory.sqlite3')
        selection_db = readonly(output / 'selection.sqlite3')
        databases = {}
        temporary = output / 'chunk-building.zip'
        writer = None
        chunk_count = 0
        predicted_bytes = 22
        last_key = None

        def seal():
            nonlocal writer, chunk_count, predicted_bytes
            if writer is None:
                return
            writer.close(); writer = None
            size = temporary.stat().st_size
            if size != predicted_bytes or size > max_bytes:
                raise ValueError('Sealed chunk exceeds its size proof')
            sha = file_hash(temporary)
            name = f'filings-{len(progress["chunks"]) + 1:05d}-{sha[:16]}.zip'
            destination = output / name
            if destination.exists() and file_hash(destination) != sha:
                raise ValueError('Existing immutable chunk has different contents')
            os.replace(temporary, destination)
            progress['chunks'].append({'file': name, 'sha256': sha, 'bytes': size, 'documents': chunk_count})
            progress['after'] = list(last_key)
            atomic_write(progress_path, canonical(progress))
            print(json.dumps({'package_chunk': name, 'documents': chunk_count, 'bytes': size}), flush=True)
            chunk_count, predicted_bytes = 0, 22

        try:
            query = 'SELECT * FROM selected'
            params = []
            if progress['after']:
                query += ' WHERE (filing_date,accession)>(?,?)'; params = progress['after']
            query += ' ORDER BY filing_date,accession'
            for selected in selection_db.execute(query, params):
                inventory = dict(inventory_db.execute('SELECT * FROM filings WHERE accession=?', (selected['accession'],)).fetchone())
                if any(inventory[key] != selected[key] for key in SELECTION_COLUMNS):
                    raise ValueError('Selected filing changed after its audit: ' + selected['accession'])
                if inventory['shard'] not in databases:
                    databases[inventory['shard']] = readonly(root / inventory['shard'])
                document = dict(databases[inventory['shard']].execute('SELECT * FROM documents WHERE accession=?', (inventory['accession'],)).fetchone())
                check_document(inventory, document)
                payload = gzip.compress(canonical({'schema': 1, 'inventory': encode_row(inventory), 'document': encode_row(document)}), mtime=0)
                name = 'filings/' + inventory['accession'] + '.json.gz'
                added = len(payload) + 76 + 2 * len(name.encode('ascii'))
                if added + 22 > max_bytes:
                    raise ValueError('One filing exceeds the configured chunk limit: ' + inventory['accession'])
                if writer and (predicted_bytes + added > max_bytes or chunk_count >= max_documents):
                    seal()
                if writer is None:
                    writer = zipfile.ZipFile(temporary, 'w', compression=zipfile.ZIP_STORED, allowZip64=False)
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.create_system = 3
                info.external_attr = 0o600 << 16
                writer.writestr(info, payload)
                chunk_count += 1; predicted_bytes += added
                last_key = (inventory['filing_date'], inventory['accession'])
            seal()
            assets = []
            if include_sources:
                for key, source in sorted(metadata['sources'].items()):
                    name = f'quarter-{key}-{source["sha256"][:16]}.zip'
                    asset = copy_asset(root / source['path'], output, name, source['sha256'])
                    assets.append({**asset, 'kind': 'SEC_quarter_zip', 'url': source['url']})
                if inventory_db.execute("SELECT 1 FROM sqlite_master WHERE name='index_sources'").fetchone():
                    for source in inventory_db.execute('SELECT * FROM index_sources ORDER BY source_key'):
                        body_path = root / 'sources' / 'indexes' / (hashlib.sha256(source['url'].encode()).hexdigest() + '.body')
                        if file_hash(body_path) != source['sha256']:
                            raise ValueError('SEC index cache differs from retained provenance')
                        name = f'index-{source["source_key"]}-{source["sha256"][:16]}.idx.gz'
                        compressed = gzip.compress(body_path.read_bytes(), compresslevel=6, mtime=0)
                        if (output / name).exists() and (output / name).read_bytes() != compressed:
                            raise ValueError('Existing immutable SEC index asset changed')
                        atomic_write(output / name, compressed)
                        assets.append({'file': name, 'sha256': hashlib.sha256(compressed).hexdigest(),
                                       'bytes': len(compressed), 'raw_sha256': source['sha256'],
                                       'kind': 'SEC_index_gzip', 'url': source['url']})
            for group in audit['groups']:
                asset = copy_asset(audit_directory / group['details_file'], output,
                                   'audit-' + group['details_file'], group['details_sha256'])
                assets.append({**asset, 'kind': 'audit_details'})
            atomic_write(output / 'source-audit.json', canonical(audit))
            assets.append({'file': 'source-audit.json', 'sha256': file_hash(output / 'source-audit.json'),
                           'bytes': (output / 'source-audit.json').stat().st_size, 'kind': 'source_audit_report'})
            # Preserve the catalog and unresolved discovery evidence alongside the
            # original indexes. This is not a backup of every uncollected queue row.
            inventory_db.execute('BEGIN')
            tables = {r[0] for r in inventory_db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            exceptions = []
            for row in inventory_db.execute("SELECT * FROM filings WHERE source_url IS NULL OR discovery_issues<>'[]' ORDER BY accession"):
                observations = []
                if 'index_observations' in tables:
                    observations = [dict(r) for r in inventory_db.execute(
                        'SELECT * FROM index_observations WHERE accession=? ORDER BY source_key', (row['accession'],))]
                exceptions.append({'inventory': encode_row(row), 'index_observations': observations})
            catalog = {'catalog_schema': 1, 'sources': metadata['sources'],
                       'index_sources': [dict(r) for r in inventory_db.execute('SELECT * FROM index_sources ORDER BY source_key')]
                           if 'index_sources' in tables else [],
                       'discovery_exceptions': exceptions,
                       'scope': metadata['scope'], 'selection_sha256': metadata['selection_sha256'],
                       'limitation': 'Source catalogs and discovery exceptions only. Unselected queue rows and documents are not a complete inventory backup.'}
            inventory_db.rollback()
            atomic_write(output / 'collection-catalog.json', canonical(catalog))
            assets.append({'file': 'collection-catalog.json', 'sha256': file_hash(output / 'collection-catalog.json'),
                           'bytes': (output / 'collection-catalog.json').stat().st_size, 'kind': 'collection_catalog'})
            manifest = {**config, 'documents': metadata['selected_documents'],
                        'scope': metadata['scope'], 'queue_counts_at_selection': metadata['queue_counts_at_selection'],
                        'chunks': progress['chunks'], 'assets': assets, 'complete_backfill': False,
                        'coverage_note': 'Immutable audited selection. Pending, inaccessible and later-discovered filings are not represented as collected.'}
            atomic_write(output / 'manifest.json', canonical(manifest))
            verified = verify(output)
            atomic_write(output / 'verification.json', canonical(verified))
            return verified
        finally:
            if writer is not None:
                writer.close()
            for db in databases.values():
                db.close()
            inventory_db.close(); selection_db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--audit', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-bytes', type=int, default=DEFAULT_CHUNK_BYTES)
    parser.add_argument('--max-documents', type=int, default=50000)
    parser.add_argument('--verify-only', action='store_true')
    parser.add_argument('--expected-manifest-sha256')
    args = parser.parse_args()
    if args.verify_only:
        result = verify(args.output, args.expected_manifest_sha256)
    else:
        if args.audit is None:
            parser.error('--audit is required for packaging')
        result = package(args.root, args.audit, args.output, args.max_bytes, args.max_documents)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
