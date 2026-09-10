"""Restore an audited package into a new, selection-only local database.

The original collection is never opened. Only manifest-listed files are read;
archive members are decoded, never extracted as filesystem paths.
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import zipfile

from .audit_batches import file_hash, readonly
from .http import atomic_write
from .inventory import canonical, connect
from .locking import writer_lock
from .package import check_document, decode_row, encode_row, verify
from .runner import shard_connect


def records(package_path, manifest):
    for chunk in manifest['chunks']:
        with zipfile.ZipFile(package_path / chunk['file']) as archive:
            for name in archive.namelist():
                value = json.loads(gzip.decompress(archive.read(name)))
                yield decode_row(value['inventory']), decode_row(value['document'])


def insert_row(db, table, row):
    columns = [r[1] for r in db.execute('PRAGMA table_info(' + table + ')')]
    if set(columns) != set(row):
        raise ValueError('Archive row schema differs from local ' + table + ' schema')
    db.execute('INSERT INTO ' + table + '(' + ','.join(columns) + ') VALUES(' + ','.join('?' for _ in columns) + ')',
               tuple(row[k] for k in columns))


def update_digest(digest, inventory, document):
    digest.update(canonical({'inventory': encode_row(inventory), 'document': encode_row(document)}) + b'\n')


def restore(package_path, destination, expected_manifest_sha256):
    package_path, destination = Path(package_path).resolve(), Path(destination).absolute()
    if not re.fullmatch('[0-9a-f]{64}', expected_manifest_sha256 or ''):
        raise ValueError('An independently pinned manifest SHA-256 is required')
    if destination.exists() or destination.is_symlink():
        raise ValueError('Restore destination must not already exist')
    verification = verify(package_path, expected_manifest_sha256)
    manifest = json.loads((package_path / 'manifest.json').read_text())
    catalog_assets = [r for r in manifest['assets'] if r['kind'] == 'collection_catalog']
    if len(catalog_assets) != 1 or not manifest['include_sources']:
        raise ValueError('Restore requires one source catalog and retained original source assets')
    catalog = json.loads((package_path / catalog_assets[0]['file']).read_text())
    if (catalog['catalog_schema'] != 1 or catalog['scope'] != manifest['scope']
            or catalog['selection_sha256'] != manifest['selection_sha256']):
        raise ValueError('Catalog scope or selection differs from the manifest')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(destination.parent / ('.' + destination.name + '.restore-lock')):
        if destination.exists() or destination.is_symlink():
            raise ValueError('Restore destination must not already exist')
        temporary = Path(tempfile.mkdtemp(prefix='.' + destination.name + '.restoring-', dir=destination.parent))
        db = None
        shards = {}
        try:
            db = connect(temporary)
            settings = {**manifest['scope'], 'schema_version': '1', 'archive_scope': 'selection_only',
                        'archive_manifest_sha256': expected_manifest_sha256,
                        'archive_documents': str(manifest['documents']), 'complete_backfill': 'false'}
            db.executemany('INSERT INTO settings VALUES(?,?)', settings.items())
            expected_rows = hashlib.sha256()
            count = 0
            for inventory, document in records(package_path, manifest):
                relative = inventory['shard']
                if not isinstance(relative, str) or not re.fullmatch(r'shards/\d{4}-\d{2}-\d{4}\.sqlite3', relative):
                    raise ValueError('Unsafe restored shard path')
                check_document(inventory, document)
                insert_row(db, 'filings', inventory)
                if relative not in shards:
                    shards[relative] = shard_connect(temporary / relative)
                insert_row(shards[relative], 'documents', document)
                update_digest(expected_rows, inventory, document)
                count += 1
                if count % 1000 == 0:
                    db.commit()
                    for shard in shards.values():
                        shard.commit()
            by_kind_url = {(a['kind'], a.get('url')): a for a in manifest['assets'] if a.get('url')}
            for key, source in catalog['sources'].items():
                if not re.fullmatch(r'\d{4}Q[1-4]', key) or source['source_key'] != key:
                    raise ValueError('Unexpected source catalog key')
                asset = by_kind_url.get(('SEC_quarter_zip', source['url']))
                if not asset or asset['sha256'] != source['sha256'] or asset['bytes'] != source['bytes']:
                    raise ValueError('Catalog quarterly source differs from retained asset')
                relative = f'sources/quarterly/{key}-{source["sha256"][:16]}.zip'
                target = temporary / relative; target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(package_path / asset['file'], target)
                insert_row(db, 'sources', {**source, 'path': relative})
            for source in catalog['index_sources']:
                asset = by_kind_url.get(('SEC_index_gzip', source['url']))
                if not asset or asset['raw_sha256'] != source['sha256']:
                    raise ValueError('Catalog index differs from retained asset')
                body = gzip.decompress((package_path / asset['file']).read_bytes())
                if len(body) != source['bytes']:
                    raise ValueError('Original index byte length differs')
                stem = hashlib.sha256(source['url'].encode()).hexdigest()
                target = temporary / 'sources' / 'indexes' / stem
                atomic_write(target.with_suffix('.body'), body)
                atomic_write(target.with_suffix('.json'), canonical({
                    'url': source['url'], 'sha256': source['sha256'], 'bytes': source['bytes'],
                    'retrieved_at_utc': source['retrieved_at']}))
            # Keep full source metadata and exception variants as evidence. Do not
            # insert unselected exceptions as if their source documents were restored.
            atomic_write(temporary / 'archive-catalog.json', canonical(catalog))
            db.commit()
            for shard in shards.values():
                shard.commit(); shard.execute('PRAGMA wal_checkpoint(TRUNCATE)'); shard.close()
            shards.clear()
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)'); db.close(); db = None

            # Reopen the finished databases and compare every full row, including
            # exact compressed BLOB bytes, rather than only derived field hashes.
            db = readonly(temporary / 'inventory.sqlite3')
            actual_rows = hashlib.sha256()
            actual_count = 0
            for inventory in db.execute('SELECT * FROM filings ORDER BY filing_date,accession'):
                relative = inventory['shard']
                if relative not in shards:
                    shards[relative] = readonly(temporary / relative)
                document = shards[relative].execute('SELECT * FROM documents WHERE accession=?', (inventory['accession'],)).fetchone()
                check_document(inventory, document)
                update_digest(actual_rows, inventory, document)
                actual_count += 1
            if (actual_count != count or count != manifest['documents']
                    or actual_rows.hexdigest() != expected_rows.hexdigest()):
                raise ValueError('Restored inventory/document rows differ from the package')
            for source in db.execute('SELECT * FROM sources'):
                if file_hash(temporary / source['path']) != source['sha256']:
                    raise ValueError('Restored quarterly source bytes differ')
            report = {**verification, 'restored_documents': count, 'restored_rows_sha256': actual_rows.hexdigest(),
                      'database_readback_verified': True, 'archive_scope': 'selection_only',
                      'discovery_exceptions_preserved': len(catalog['discovery_exceptions']),
                      'restored_quarterly_sources': len(catalog['sources']),
                      'restored_index_sources': len(catalog['index_sources']),
                      'complete_backfill': False}
            atomic_write(temporary / 'restore-report.json', canonical(report))
            for shard in shards.values():
                shard.close()
            shards.clear(); db.close(); db = None
            if destination.exists() or destination.is_symlink():
                raise ValueError('Restore destination was created by another process')
            os.rename(temporary, destination)
            return report
        finally:
            for shard in shards.values():
                shard.close()
            if db is not None:
                db.close()
            if temporary.exists():
                shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--package', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    parser.add_argument('--expected-manifest-sha256', required=True)
    args = parser.parse_args()
    print(json.dumps(restore(args.package, args.destination, args.expected_manifest_sha256), indent=2), flush=True)


if __name__ == '__main__':
    main()
