"""Index complete checkpoint document membership without changing its archives.

The independently pinned index maps every committed inventory row to its newest
archive envelope. An extension reads the prior index and new chunks only.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
from itertools import zip_longest
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile

from .audit_batches import SELECTION_COLUMNS, file_hash, readonly
from .github_chain import MAX_CHAIN, MAX_METADATA_BYTES, declared_files, valid_hash
from .http import atomic_write
from .increment import frozen_hash
from .inventory import canonical
from .inventory_archive import describe
from .inventory_delta import schema, state
from .locking import writer_lock
from .package import decode_row, encode_row

MANIFEST = 'document-index.json'
MAX_INDEX_BYTES = 250_000_000
MAX_INDEX_RAW_BYTES = 1_000_000_000
MAX_ENVELOPE_BYTES = 128 * 1024**2
MAX_DOCUMENT_BYTES = 128 * 1024**2
MAX_SELECTION = 1000
ACCESSION = r'[0-9]{10}-[0-9]{2}-[0-9]{6}'


def row_hash(row):
    return hashlib.sha256(canonical(encode_row(row))).hexdigest()


def pinned_json(path, pin):
    path = Path(path)
    if (not valid_hash(pin) or path.is_symlink() or not 0 < path.stat().st_size <= MAX_METADATA_BYTES
            or file_hash(path) != pin):
        raise ValueError('Pinned document-index metadata checksum, size, or path differs')
    return json.loads(path.read_text())


def local_chain(manifest_path, pin, ancestors):
    paths = [Path(manifest_path).resolve(), *(Path(path).resolve() for path in ancestors)]
    supplied = {file_hash(path): path for path in paths}
    if len(supplied) != len(paths) or pin not in supplied or supplied[pin] != paths[0]:
        raise ValueError('Supply distinct ancestors and the independently pinned target checkpoint')
    chain, seen = [], set()
    while True:
        if pin in seen or len(seen) >= MAX_CHAIN or pin not in supplied:
            raise ValueError('Document index needs the complete acyclic checkpoint chain')
        seen.add(pin)
        path = supplied[pin]
        value = pinned_json(path, pin)
        kind = 'increment' if value.get('increment_schema') == 1 else 'baseline'
        name = 'increment.json' if kind == 'increment' else 'baseline.json'
        if path.name != name or (kind == 'baseline' and value.get('baseline_schema') != 1):
            raise ValueError('Unsupported document-index checkpoint schema')
        node = {'reference': {'kind': kind, 'manifest_sha256': pin}, 'manifest': value,
                'directory': path.parent, 'expected': declared_files(value, name, pin, path.stat().st_size)}
        if chain and chain[-1]['manifest'].get('parent') != node['reference']:
            raise ValueError('Document-index ancestors differ from the pinned parent sequence')
        chain.append(node)
        if kind == 'baseline':
            break
        parent = value.get('parent', {})
        if set(parent) != {'kind', 'manifest_sha256'} or not valid_hash(parent['manifest_sha256']):
            raise ValueError('Invalid document-index parent reference')
        pin = parent['manifest_sha256']
    if seen != set(supplied) or chain[0]['reference']['kind'] != 'increment':
        raise ValueError('Document index requires one incremental target and exactly its ancestors')
    return list(reversed(chain))


def document_manifest(node):
    outer = node['manifest']
    asset = node['expected'].get('manifest.json')
    if asset is None or asset['sha256'] != outer.get('document_manifest_sha256'):
        raise ValueError('Document manifest differs from its checkpoint declaration')
    path = node['directory'] / 'manifest.json'
    value = pinned_json(path, asset['sha256'])
    if path.stat().st_size != asset['bytes']:
        raise ValueError('Document manifest has a different declared length')
    baseline = node['reference']['kind'] == 'baseline'
    count = outer['documents'] if baseline else outer['changed_committed_documents']
    selection = outer['selection_sha256'] if baseline else outer['changed_selection_sha256']
    scope = outer['scope'] if baseline else outer['target_inventory']['scope']
    if (value.get('package_schema') not in (1, 2) or value.get('documents') != count
            or value.get('selection_sha256') != selection or value.get('scope') != scope
            or not isinstance(value.get('chunks'), list) or len(value['chunks']) > 999):
        raise ValueError('Document manifest coverage differs from its checkpoint')
    seen = set()
    for chunk in value['chunks']:
        if (not isinstance(chunk, dict) or set(chunk) != {'file', 'bytes', 'sha256', 'documents'}
                or not isinstance(chunk['file'], str)
                or not re.fullmatch(r'filings-[0-9]{5}-[0-9a-f]{16}\.zip', chunk['file'])
                or chunk['file'] in seen or type(chunk['documents']) is not int
                or not 1 <= chunk['documents'] <= 50000
                or {key: chunk[key] for key in ('file', 'bytes', 'sha256')} != node['expected'].get(chunk['file'])):
            raise ValueError('Document chunk differs from its checkpoint declaration')
        seen.add(chunk['file'])
    if sum(chunk['documents'] for chunk in value['chunks']) != count:
        raise ValueError('Document chunk membership is incomplete')
    return value


def bounded_gzip(body, maximum, expected=None):
    if expected is not None and (type(expected) is not int or not 0 <= expected <= maximum):
        raise ValueError('Archived document decoded size exceeds its supported bound')
    limit = maximum if expected is None else expected
    with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
        decoded = stream.read(limit + 1)
    if len(decoded) > limit or (expected is not None and len(decoded) != expected):
        raise ValueError('Archived document decoded bytes differ from their declared length')
    return decoded


def member(archive, accession, compressed_bytes=None, decoded_bytes=None):
    import zipfile
    info = archive.getinfo('filings/' + accession + '.json.gz')
    if (info.compress_type != zipfile.ZIP_STORED or info.file_size != info.compress_size
            or not 0 < info.file_size <= MAX_ENVELOPE_BYTES
            or (compressed_bytes is not None and info.file_size != compressed_bytes)):
        raise ValueError('Selected archive member size or storage method differs')
    raw = bounded_gzip(archive.read(info), MAX_ENVELOPE_BYTES, decoded_bytes)
    envelope = json.loads(raw)
    if set(envelope) != {'schema', 'inventory', 'document'} or envelope['schema'] != 1:
        raise ValueError('Unsupported archived document envelope')
    inventory, document = decode_row(envelope['inventory']), decode_row(envelope['document'])
    if (inventory['accession'] != accession or document['accession'] != accession
            or inventory['source_url'] != document['source_url']):
        raise ValueError('Archived document identity or provenance differs')
    for key in ('source', 'parsed'):
        decoded = bounded_gzip(document[key + '_gzip'], MAX_DOCUMENT_BYTES, document[key + '_bytes'])
        digest = hashlib.sha256(decoded).hexdigest()
        if digest != document[key + '_sha256'] or digest != inventory[key + '_sha256']:
            raise ValueError('Archived document original or normalized checksum differs')
    return inventory, document, info.file_size, len(raw)


def validate_manifest(value, chain):
    required = {'document_index_schema', 'checkpoint', 'inventory_state_sha256', 'committed_documents',
                'committed_selection_sha256', 'index_file', 'chunks', 'complete_backfill'}
    target = chain[-1]['manifest']
    if (not isinstance(value, dict) or set(value) != required or type(value['document_index_schema']) is not int
            or value['document_index_schema'] != 1
            or value['checkpoint'] != chain[-1]['reference'] or value['complete_backfill'] is not False
            or value['inventory_state_sha256'] != target['target_inventory_state']['state_sha256']
            or type(value['committed_documents']) is not int or value['committed_documents'] < 0
            or value['committed_documents'] != target['target_inventory']['committed_documents']
            or value['committed_selection_sha256'] != target['target_inventory']['committed_selection_sha256']):
        raise ValueError('Document index is not bound to the complete target checkpoint')
    part = value['index_file']
    if (not isinstance(part, dict) or set(part) != {'file', 'bytes', 'sha256', 'raw_bytes', 'raw_sha256'}
            or not valid_hash(part['sha256']) or not valid_hash(part['raw_sha256'])
            or part['file'] != 'document-index-' + part['raw_sha256'][:16] + '.jsonl.gz'
            or type(part['bytes']) is not int or not 0 < part['bytes'] <= MAX_INDEX_BYTES
            or type(part['raw_bytes']) is not int or not 0 <= part['raw_bytes'] <= MAX_INDEX_RAW_BYTES):
        raise ValueError('Document index file has invalid size, checksum, or path')
    nodes = {node['reference']['manifest_sha256']: node for node in chain}
    if not isinstance(value['chunks'], list) or len(value['chunks']) > MAX_CHAIN * 999:
        raise ValueError('Invalid document-index chunk catalog')
    seen = set()
    for chunk in value['chunks']:
        if (not isinstance(chunk, dict) or set(chunk) != {'checkpoint_sha256', 'file', 'bytes', 'sha256'}
                or chunk['checkpoint_sha256'] not in nodes or not isinstance(chunk['file'], str)
                or not re.fullmatch(r'filings-[0-9]{5}-[0-9a-f]{16}\.zip', chunk['file'])):
            raise ValueError('Document-index chunk has an invalid checkpoint or filename')
        identity = (chunk['checkpoint_sha256'], chunk['file'])
        if (identity in seen or {key: chunk[key] for key in ('file', 'bytes', 'sha256')}
                != nodes[chunk['checkpoint_sha256']]['expected'].get(chunk['file'])):
            raise ValueError('Document-index chunk differs from a pinned archive asset')
        seen.add(identity)
    return value


def records(path, value):
    part = value['index_file']
    if Path(path).is_symlink() or Path(path).stat().st_size != part['bytes'] or file_hash(path) != part['sha256']:
        raise ValueError('Compressed document-index bytes differ')
    digest, size, count, previous = hashlib.sha256(), 0, 0, None
    with gzip.open(path, 'rb') as stream:
        while True:
            line = stream.readline(16385)
            if not line:
                break
            size += len(line); count += 1; digest.update(line)
            if len(line) > 16384 or size > part['raw_bytes'] or count > value['committed_documents']:
                raise ValueError('Document index exceeds its declared decoded length or membership')
            row = json.loads(line)
            if (not isinstance(row, list) or len(row) != 5 or not isinstance(row[0], str)
                    or not re.fullmatch(ACCESSION, row[0]) or not valid_hash(row[1])
                    or type(row[2]) is not int or not 0 <= row[2] < len(value['chunks'])
                    or any(type(n) is not int or not 0 < n <= MAX_ENVELOPE_BYTES for n in row[3:])
                    or row[3] > value['chunks'][row[2]]['bytes']
                    or (previous is not None and row[0] <= previous) or line != canonical(row) + b'\n'):
                raise ValueError('Document index has invalid, repeated, or unordered membership')
            previous = row[0]
            yield row
    if size != part['raw_bytes'] or digest.hexdigest() != part['raw_sha256'] or count != value['committed_documents']:
        raise ValueError('Complete document-index membership or decoded checksum differs')


def verify_inventory(path, value, inventory_path):
    db = readonly(inventory_path)
    try:
        rows = db.execute("SELECT * FROM filings WHERE status IN ('verified','review') ORDER BY accession")
        for indexed, row in zip_longest(records(path, value), rows):
            if indexed is None or row is None or indexed[0] != row['accession'] or indexed[1] != row_hash(row):
                raise ValueError('Document index differs from complete committed inventory rows')
    finally:
        db.close()


def selection(index_pin, accessions):
    if (not valid_hash(index_pin) or not isinstance(accessions, (list, tuple))
            or not 1 <= len(accessions) <= MAX_SELECTION
            or any(not isinstance(item, str) or not re.fullmatch(ACCESSION, item) for item in accessions)
            or len(set(accessions)) != len(accessions)):
        raise ValueError('Select one to 1000 distinct accessions and an independently pinned document index')
    return {'filing_selection_schema': 1, 'document_index_sha256': index_pin, 'accessions': sorted(accessions)}


def build(manifest_path, pin, inventory_path, output, ancestors=(), parent_index=None, parent_index_pin=None):
    import zipfile
    chain = local_chain(manifest_path, pin, ancestors)
    target = chain[-1]['manifest']
    inventory_hash = frozen_hash(inventory_path)
    db = readonly(inventory_path)
    try:
        if state(db, schema(db)) != target['target_inventory_state'] or describe(inventory_path) != target['target_inventory']:
            raise ValueError('Frozen index inventory differs from the pinned target checkpoint')
    finally:
        db.close()
    if (parent_index is None) != (parent_index_pin is None):
        raise ValueError('A previous document index requires its independently pinned checksum')
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('Document-index output must be a new directory')
    output.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(output.parent / ('.' + output.name + '.index-lock')):
        temporary = Path(tempfile.mkdtemp(prefix='.' + output.name + '.indexing-', dir=output.parent))
        registry = sqlite3.connect(temporary / 'registry.sqlite3')
        registry.execute('CREATE TABLE entries(accession TEXT PRIMARY KEY,inventory_sha256 TEXT,chunk INTEGER,compressed INTEGER,decoded INTEGER)')
        chunks, first = [], 0
        try:
            if parent_index is not None:
                parent_index = Path(parent_index)
                previous = pinned_json(parent_index, parent_index_pin)
                positions = [i for i, node in enumerate(chain[:-1]) if node['reference'] == previous.get('checkpoint')]
                if len(positions) != 1:
                    raise ValueError('The previous document index is not a pinned target ancestor')
                first = positions[0] + 1
                validate_manifest(previous, chain[:first])
                chunks.extend(previous['chunks'])
                registry.executemany('INSERT INTO entries VALUES(?,?,?,?,?)',
                                     records(parent_index.parent / previous['index_file']['file'], previous))
            for node in chain[first:]:
                documents = document_manifest(node)
                count, digest, before = 0, hashlib.sha256(), None
                for chunk in documents['chunks']:
                    path = node['directory'] / chunk['file']
                    if path.is_symlink() or path.stat().st_size != chunk['bytes'] or file_hash(path) != chunk['sha256']:
                        raise ValueError('Document-index source chunk bytes differ from the pinned checkpoint')
                    position = len(chunks)
                    chunks.append({'checkpoint_sha256': node['reference']['manifest_sha256'],
                                   **{key: chunk[key] for key in ('file', 'bytes', 'sha256')}})
                    with zipfile.ZipFile(path) as archive:
                        names = archive.namelist()
                        if len(names) != chunk['documents'] or len(names) != len(set(names)):
                            raise ValueError('Document-index source chunk membership differs')
                        for name in names:
                            if not re.fullmatch('filings/' + ACCESSION + r'\.json\.gz', name):
                                raise ValueError('Unsafe document-index source member')
                            accession = name[8:-8]
                            inventory, _, compressed, decoded = member(archive, accession)
                            key = (inventory['filing_date'], accession)
                            if before is not None and key <= before:
                                raise ValueError('Document-index source membership is repeated or unordered')
                            before = key; count += 1
                            digest.update(canonical({key: inventory[key] for key in SELECTION_COLUMNS}) + b'\n')
                            registry.execute('INSERT OR REPLACE INTO entries VALUES(?,?,?,?,?)',
                                             (accession, row_hash(inventory), position, compressed, decoded))
                    registry.commit()
                if count != documents['documents'] or digest.hexdigest() != documents['selection_sha256']:
                    raise ValueError('Document-index source membership differs from its complete audited selection')
            data = temporary / 'index-building.gz'
            db = readonly(inventory_path)
            raw_hash, raw_bytes, count = hashlib.sha256(), 0, 0
            try:
                with data.open('wb') as raw, gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0) as stream:
                    for row in db.execute("SELECT * FROM filings WHERE status IN ('verified','review') ORDER BY accession"):
                        found = registry.execute('SELECT * FROM entries WHERE accession=?', (row['accession'],)).fetchone()
                        if found is None or found[1] != row_hash(row):
                            raise ValueError('Newest archived document does not match its complete target inventory row')
                        line = canonical(list(found)) + b'\n'
                        raw_hash.update(line); raw_bytes += len(line); count += 1
                        if raw_bytes > MAX_INDEX_RAW_BYTES:
                            raise ValueError('Document index exceeds its supported decoded size')
                        stream.write(line)
            finally:
                db.close()
            name = 'document-index-' + raw_hash.hexdigest()[:16] + '.jsonl.gz'
            data.rename(temporary / name)
            value = {'document_index_schema': 1, 'checkpoint': chain[-1]['reference'],
                     'inventory_state_sha256': target['target_inventory_state']['state_sha256'],
                     'committed_documents': count,
                     'committed_selection_sha256': target['target_inventory']['committed_selection_sha256'],
                     'index_file': {'file': name, 'bytes': (temporary / name).stat().st_size,
                                    'sha256': file_hash(temporary / name), 'raw_bytes': raw_bytes,
                                    'raw_sha256': raw_hash.hexdigest()}, 'chunks': chunks, 'complete_backfill': False}
            validate_manifest(value, chain)
            verify_inventory(temporary / name, value, inventory_path)
            if frozen_hash(inventory_path) != inventory_hash:
                raise ValueError('Frozen document-index inventory changed during construction')
            atomic_write(temporary / MANIFEST, canonical(value))
            if (temporary / MANIFEST).stat().st_size > MAX_METADATA_BYTES:
                raise ValueError('Document-index metadata requires compaction')
            result = {**value, 'document_index_manifest_sha256': file_hash(temporary / MANIFEST),
                      'indexed_from_previous_checkpoint': parent_index is not None,
                      'new_checkpoint_chunks_read': sum(len(document_manifest(node)['chunks']) for node in chain[first:])}
            registry.close(); registry = None
            (temporary / 'registry.sqlite3').unlink()
            if output.exists() or output.is_symlink():
                raise ValueError('Document-index destination was created by another process')
            os.rename(temporary, output)
            return result
        finally:
            if registry is not None:
                registry.close()
            if temporary.exists():
                shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--checkpoint-manifest', type=Path, required=True)
    parser.add_argument('--checkpoint-sha256', required=True)
    parser.add_argument('--inventory-snapshot', type=Path, required=True)
    parser.add_argument('--ancestor-manifest', type=Path, action='append', default=[])
    parser.add_argument('--parent-index', type=Path)
    parser.add_argument('--parent-index-sha256')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.checkpoint_manifest, args.checkpoint_sha256, args.inventory_snapshot, args.output,
                           args.ancestor_manifest, args.parent_index, args.parent_index_sha256), indent=2))


if __name__ == '__main__':
    main()
