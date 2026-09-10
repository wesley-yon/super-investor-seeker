"""Preserve inventory changes against an exact logical parent, without documents.

Inputs are read in consistent SQLite transactions. Every row in every supported
table contributes to both state hashes. Replays copy the parent to a new location,
check each operation's old row, and verify the complete resulting state before
publishing it. This avoids uploading the full inventory for every daily change.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile

from .audit_batches import file_hash, readonly
from .http import atomic_write
from .inventory import canonical
from .inventory_archive import MAX_PART_BYTES, PART_LIMIT, PartsWriter, TABLES
from .locking import writer_lock
from .package import decode_row, encode_row

MANIFEST = 'inventory-delta.json'
MAX_RECORD_BYTES = 16 * 1024 * 1024
MAX_RAW_BYTES = 16 * 1024 ** 3
MAX_COMPRESSED_BYTES = 2_000_000_000
PRIMARY_KEYS = {
    'settings': ['key'], 'sources': ['source_key'], 'filings': ['accession'],
    'index_sources': ['source_key'], 'index_membership': ['source_key', 'accession'],
    'index_observations': ['source_key', 'accession'],
}


def sha(body):
    return hashlib.sha256(body).hexdigest()


def schema(db):
    objects = [dict(row) for row in db.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name")]
    present = {row['name'] for row in objects if row['type'] == 'table'}
    if (present - set(TABLES) or not {'settings', 'sources', 'filings'} <= present
            or any(row['type'] not in ('table', 'index') or row['tbl_name'] not in present for row in objects)):
        raise ValueError('Inventory delta requires only supported tables and indexes')
    columns = {}
    for table in TABLES:
        if table not in present:
            continue
        info = [dict(row) for row in db.execute('PRAGMA table_info(' + table + ')')]
        names = [row['name'] for row in info]
        keys = [row['name'] for row in sorted(info, key=lambda row: row['pk']) if row['pk']]
        if keys != PRIMARY_KEYS[table] or any(not re.fullmatch(r'[a-z][a-z0-9_]*', name) for name in names):
            raise ValueError('Unsupported inventory column or primary key')
        columns[table] = names
    identity = {'objects': objects, 'columns': columns,
                'user_version': db.execute('PRAGMA user_version').fetchone()[0],
                'application_id': db.execute('PRAGMA application_id').fetchone()[0]}
    return {'sha256': sha(canonical(identity)), 'columns': columns}


def row_bytes(row):
    if any(isinstance(value, float) and not math.isfinite(value) for value in row.values()):
        raise ValueError('Non-finite inventory number cannot be archived')
    return canonical(encode_row(row)) + b'\n'


def rows(db, table):
    for row in db.execute('SELECT * FROM ' + table + ' ORDER BY ' + ','.join(PRIMARY_KEYS[table])):
        item = dict(row)
        key = tuple(item[name] for name in PRIMARY_KEYS[table])
        if any(type(value) is not str for value in key):
            raise ValueError('Inventory primary keys must be non-null strings')
        yield key, item, row_bytes(item)


def finish_state(schema_hash, tables):
    body = {'schema_sha256': schema_hash, 'tables': tables}
    return {**body, 'state_sha256': sha(canonical(body))}


def state(db, shape):
    tables = {}
    for table in shape['columns']:
        digest, count = hashlib.sha256(), 0
        for _, _, body in rows(db, table):
            digest.update(body); count += 1
        tables[table] = {'rows': count, 'sha256': digest.hexdigest()}
    return finish_state(shape['sha256'], tables)


def check_integrity(db):
    if [row[0] for row in db.execute('PRAGMA integrity_check')] != ['ok']:
        raise ValueError('Inventory SQLite integrity check failed')


def build(parent, target, output, max_bytes=PART_LIMIT, progress=None):
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_PART_BYTES:
        raise ValueError('Inventory delta part limit is outside the configured range')
    parent, target, output = Path(parent), Path(target), Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('Inventory delta output must not exist')
    output.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(output.parent / ('.' + output.name + '.delta-lock')):
        if output.exists() or output.is_symlink():
            raise ValueError('Inventory delta output must not exist')
        temporary = Path(tempfile.mkdtemp(prefix='.' + output.name + '.building-', dir=output.parent))
        before, after = None, None
        writer = PartsWriter(temporary, max_bytes, prefix='inventory-delta')
        try:
            before, after = readonly(parent), readonly(target)
            before.execute('BEGIN'); after.execute('BEGIN')
            shape = schema(before)
            if shape != schema(after):
                raise ValueError('Inventory schema changed; create a new full baseline')
            check_integrity(before); check_integrity(after)
            parent_tables, target_tables, operations = {}, {}, {}
            raw_hash, raw_bytes = hashlib.sha256(), 0
            with gzip.GzipFile(filename='', fileobj=writer, mode='wb', mtime=0, compresslevel=6) as stream:
                for table in shape['columns']:
                    old_rows, new_rows = iter(rows(before, table)), iter(rows(after, table))
                    old, new = next(old_rows, None), next(new_rows, None)
                    old_hash, new_hash = hashlib.sha256(), hashlib.sha256()
                    old_count, new_count = 0, 0
                    counts = {'insert': 0, 'update': 0, 'delete': 0}
                    while old is not None or new is not None:
                        take_old = old is not None and (new is None or old[0] <= new[0])
                        take_new = new is not None and (old is None or new[0] <= old[0])
                        left, right = old if take_old else None, new if take_new else None
                        if left is None or right is None or left[2] != right[2]:
                            kind = 'insert' if left is None else 'delete' if right is None else 'update'
                            operation = {'table': table, 'key': list((left or right)[0]),
                                         'before_sha256': sha(left[2]) if left else None,
                                         'after': encode_row(right[1]) if right else None}
                            body = canonical(operation) + b'\n'
                            raw_bytes += len(body)
                            if len(body) > MAX_RECORD_BYTES or raw_bytes > MAX_RAW_BYTES:
                                raise ValueError('Inventory delta exceeds its decoded size budget')
                            stream.write(body); raw_hash.update(body); counts[kind] += 1
                            if sum(part['bytes'] for part in writer.parts) + writer.size > MAX_COMPRESSED_BYTES:
                                raise ValueError('Inventory delta exceeds its compressed size budget')
                        if take_old:
                            old_hash.update(old[2]); old_count += 1; old = next(old_rows, None)
                        if take_new:
                            new_hash.update(new[2]); new_count += 1; new = next(new_rows, None)
                    parent_tables[table] = {'rows': old_count, 'sha256': old_hash.hexdigest()}
                    target_tables[table] = {'rows': new_count, 'sha256': new_hash.hexdigest()}
                    operations[table] = counts
                    if progress:
                        progress({'table': table, 'parent_rows': old_count, 'target_rows': new_count, **counts})
            writer.seal()
            manifest = {'inventory_delta_schema': 1, 'compression': 'gzip_concatenated_parts',
                        'parent': finish_state(shape['sha256'], parent_tables),
                        'target': finish_state(shape['sha256'], target_tables),
                        'operations': operations, 'parts': writer.parts, 'max_part_bytes': max_bytes,
                        'raw_bytes': raw_bytes, 'raw_sha256': raw_hash.hexdigest(),
                        'includes_original_documents': False, 'collection_resume_ready': False,
                        'complete_backfill': False}
            atomic_write(temporary / MANIFEST, canonical(manifest))
            verify(temporary, file_hash(temporary / MANIFEST))
            if output.exists() or output.is_symlink():
                raise ValueError('Inventory delta output was created by another process')
            os.rename(temporary, output)
            return manifest
        finally:
            writer.close()
            if before is not None:
                before.close()
            if after is not None:
                after.close()
            if temporary.exists():
                shutil.rmtree(temporary)


def valid_hash(value):
    return isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) is not None


def verify(directory, expected_manifest_sha256):
    directory = Path(directory).resolve()
    manifest_path = directory / MANIFEST
    if (not valid_hash(expected_manifest_sha256) or manifest_path.is_symlink()
            or manifest_path.stat().st_size > 2_000_000 or file_hash(manifest_path) != expected_manifest_sha256):
        raise ValueError('Inventory delta differs from its independently pinned checksum')
    manifest = json.loads(manifest_path.read_text())
    if (manifest.get('inventory_delta_schema') != 1 or manifest.get('compression') != 'gzip_concatenated_parts'
            or type(manifest.get('raw_bytes')) is not int or not 0 <= manifest['raw_bytes'] <= MAX_RAW_BYTES
            or not valid_hash(manifest.get('raw_sha256'))
            or type(manifest.get('max_part_bytes')) is not int or not 1 <= manifest['max_part_bytes'] <= MAX_PART_BYTES):
        raise ValueError('Unsupported inventory delta schema or size budget')
    for side in ('parent', 'target'):
        item = manifest[side]
        if (not valid_hash(item['schema_sha256']) or not {'settings', 'sources', 'filings'} <= set(item['tables']) <= set(TABLES)
                or any(type(t['rows']) is not int or t['rows'] < 0 or not valid_hash(t['sha256']) for t in item['tables'].values())
                or finish_state(item['schema_sha256'], item['tables']) != item):
            raise ValueError('Invalid inventory delta state identity')
    if (manifest['parent']['schema_sha256'] != manifest['target']['schema_sha256']
            or set(manifest['parent']['tables']) != set(manifest['target']['tables'])
            or set(manifest['operations']) != set(manifest['parent']['tables'])):
        raise ValueError('Inventory delta schema or table membership differs')
    for table, counts in manifest['operations'].items():
        if (set(counts) != {'insert', 'update', 'delete'}
                or any(type(n) is not int or n < 0 for n in counts.values())
                or manifest['target']['tables'][table]['rows'] != manifest['parent']['tables'][table]['rows'] + counts['insert'] - counts['delete']):
            raise ValueError('Inventory delta operation counts differ')
    parts = manifest['parts']
    if not 1 <= len(parts) <= 999:
        raise ValueError('Invalid inventory delta part count')
    total = 0
    for index, part in enumerate(parts, 1):
        if not valid_hash(part['sha256']) or part['file'] != f'inventory-delta-{index:05d}-{part["sha256"][:16]}.gz.part':
            raise ValueError('Unsafe or unordered inventory delta part')
        path = directory / part['file']
        if (path.is_symlink() or path.resolve().parent != directory or type(part['bytes']) is not int
                or not 1 <= part['bytes'] <= manifest['max_part_bytes']
                or path.stat().st_size != part['bytes'] or file_hash(path) != part['sha256']):
            raise ValueError('Inventory delta part checksum, size or path differs')
        total += part['bytes']
    if total > MAX_COMPRESSED_BYTES:
        raise ValueError('Inventory delta exceeds its compressed size budget')
    return manifest


def apply_operations(db, compressed, manifest, shape):
    counts = {table: dict.fromkeys(('insert', 'update', 'delete'), 0) for table in shape['columns']}
    previous, raw_bytes, digest = None, 0, hashlib.sha256()
    with gzip.open(compressed, 'rb') as stream:
        while body := stream.readline(MAX_RECORD_BYTES + 1):
            raw_bytes += len(body)
            if len(body) > MAX_RECORD_BYTES or raw_bytes > manifest['raw_bytes'] or not body.endswith(b'\n'):
                raise ValueError('Inventory delta exceeds its declared record or decoded size')
            digest.update(body)
            operation = json.loads(body)
            table, key = operation['table'], operation['key']
            if (set(operation) != {'table', 'key', 'before_sha256', 'after'} or table not in shape['columns']
                    or not isinstance(key, list) or len(key) != len(PRIMARY_KEYS[table])
                    or any(type(value) is not str for value in key)):
                raise ValueError('Invalid inventory delta operation identity')
            position = (TABLES.index(table), tuple(key))
            if previous is not None and position <= previous:
                raise ValueError('Inventory delta operations are duplicated or out of order')
            previous = position
            where = ' AND '.join(name + '=?' for name in PRIMARY_KEYS[table])
            old = db.execute('SELECT * FROM ' + table + ' WHERE ' + where, key).fetchone()
            if (sha(row_bytes(dict(old))) if old is not None else None) != operation['before_sha256']:
                raise ValueError('Inventory delta operation does not match its expected old row')
            after = operation['after']
            if after is None:
                if old is None:
                    raise ValueError('Inventory delta cannot delete an absent row')
                db.execute('DELETE FROM ' + table + ' WHERE ' + where, key); kind = 'delete'
            else:
                if not isinstance(after, dict) or set(after) != set(shape['columns'][table]):
                    raise ValueError('Inventory delta row columns differ')
                decoded = decode_row(after)
                if (encode_row(decoded) != after or any(value is not None and type(value) not in (str, int, float, bytes) for value in decoded.values())
                        or [decoded[name] for name in PRIMARY_KEYS[table]] != key):
                    raise ValueError('Inventory delta row values or key differ')
                row_bytes(decoded)
                columns = shape['columns'][table]
                values = [decoded[name] for name in columns]
                if old is None:
                    db.execute('INSERT INTO ' + table + '(' + ','.join(columns) + ') VALUES(' + ','.join('?' for _ in columns) + ')', values)
                    kind = 'insert'
                else:
                    db.execute('UPDATE ' + table + ' SET ' + ','.join(name + '=?' for name in columns) + ' WHERE ' + where, values + key)
                    kind = 'update'
            counts[table][kind] += 1
    if raw_bytes != manifest['raw_bytes'] or digest.hexdigest() != manifest['raw_sha256'] or counts != manifest['operations']:
        raise ValueError('Inventory delta content or operation counts differ from its manifest')


def restore(parent, directory, destination, expected_manifest_sha256):
    parent, directory, destination = Path(parent), Path(directory), Path(destination).absolute()
    if destination.exists() or destination.is_symlink():
        raise ValueError('Inventory delta restore destination must not exist')
    manifest = verify(directory, expected_manifest_sha256)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(destination.parent / ('.' + destination.name + '.delta-restore-lock')):
        if destination.exists() or destination.is_symlink():
            raise ValueError('Inventory delta restore destination must not exist')
        temporary = Path(tempfile.mkdtemp(prefix='.' + destination.name + '.restoring-', dir=destination.parent))
        source, db = None, None
        try:
            source = readonly(parent); source.execute('BEGIN')
            shape = schema(source)
            if state(source, shape) != manifest['parent']:
                raise ValueError('Inventory delta parent state does not match')
            db = sqlite3.connect(temporary / 'inventory.sqlite3')
            db.row_factory = sqlite3.Row
            source.backup(db, pages=1024, sleep=.01)
            db.execute('PRAGMA journal_mode=DELETE'); db.execute('PRAGMA synchronous=FULL')
            source.close(); source = None
            compressed = temporary / 'delta.gz'
            with compressed.open('wb') as target:
                for part in manifest['parts']:
                    with (directory / part['file']).open('rb') as stream:
                        shutil.copyfileobj(stream, target, length=1024 * 1024)
            db.execute('BEGIN IMMEDIATE')
            apply_operations(db, compressed, manifest, shape)
            if schema(db) != shape or state(db, shape) != manifest['target']:
                raise ValueError('Reconstructed inventory differs from the complete target state')
            check_integrity(db)
            db.commit(); db.close(); db = None
            compressed.unlink()
            report = {'inventory_delta_manifest_sha256': expected_manifest_sha256,
                      'parent_state_sha256': manifest['parent']['state_sha256'],
                      'target_state_sha256': manifest['target']['state_sha256'],
                      'tables': manifest['target']['tables'], 'operations': manifest['operations'],
                      'every_inventory_row_verified': True, 'sqlite_integrity_check': 'ok',
                      'includes_original_documents': False, 'collection_resume_ready': False,
                      'complete_backfill': False}
            atomic_write(temporary / 'inventory-delta-restore.json', canonical(report))
            if destination.exists() or destination.is_symlink():
                raise ValueError('Inventory delta restore destination was created by another process')
            os.rename(temporary, destination)
            return report
        finally:
            if source is not None:
                source.close()
            if db is not None:
                db.close()
            if temporary.exists():
                shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--parent', type=Path, required=True)
    parser.add_argument('--target', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-bytes', type=int, default=PART_LIMIT)
    parser.add_argument('--restore-from', type=Path)
    parser.add_argument('--expected-manifest-sha256')
    args = parser.parse_args()
    if args.restore_from:
        report = restore(args.parent, args.restore_from, args.output, args.expected_manifest_sha256)
    else:
        if not args.target:
            parser.error('--target is required when creating an inventory delta')
        report = build(args.parent, args.target, args.output, args.max_bytes,
                       progress=lambda item: print(json.dumps(item), flush=True))
    print(json.dumps(report, indent=2), flush=True)


if __name__ == '__main__':
    main()
