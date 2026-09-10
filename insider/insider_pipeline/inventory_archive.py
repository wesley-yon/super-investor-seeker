"""Archive a consistent full queue snapshot without interrupting collection.

This preserves all inventory, discovery, and retry state. Original documents are
separate assets; an inventory backup alone never proves restored document coverage.
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
import sqlite3
import tempfile

from .audit_batches import SELECTION_COLUMNS, file_hash, readonly
from .http import atomic_write
from .inventory import canonical
from .locking import writer_lock

PART_LIMIT = 500 * 1024 * 1024
MAX_PART_BYTES = 1_000_000_000
TABLES = ('settings', 'sources', 'filings', 'index_sources', 'index_membership', 'index_observations')


def describe(path):
    db = readonly(path)
    try:
        integrity = [r[0] for r in db.execute('PRAGMA integrity_check')]
        if integrity != ['ok']:
            raise ValueError('Inventory SQLite integrity check failed')
        present = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if present - set(TABLES) or not {'settings', 'sources', 'filings'} <= present:
            raise ValueError('Inventory schema contains missing or unsupported tables')
        counts = {table: db.execute('SELECT count(*) FROM ' + table).fetchone()[0] for table in TABLES if table in present}
        settings = dict(db.execute('SELECT key,value FROM settings'))
        digest = hashlib.sha256()
        committed = 0
        for row in db.execute('SELECT ' + ','.join(SELECTION_COLUMNS) +
                              " FROM filings WHERE status IN ('verified','review') ORDER BY filing_date,accession"):
            if not all(row[k] for k in ('shard', 'source_sha256', 'parsed_sha256')):
                raise ValueError('Committed inventory row has incomplete document provenance')
            digest.update(canonical(dict(row)) + b'\n'); committed += 1
        return {'tables': counts, 'queue_counts': dict(db.execute('SELECT status,count(*) FROM filings GROUP BY status')),
                'scope': {k: settings[k] for k in ('window_start', 'window_end')},
                'committed_documents': committed, 'committed_selection_sha256': digest.hexdigest(),
                'sqlite_integrity_check': 'ok'}
    finally:
        db.close()


def capture_snapshot(root, destination, progress=None):
    source, target = readonly(Path(root) / 'inventory.sqlite3'), sqlite3.connect(destination)
    try:
        source.execute('BEGIN')
        source.execute('SELECT count(*) FROM settings').fetchone()  # Establish the read snapshot before copying pages.
        source.backup(target, pages=1024, progress=progress, sleep=.01)
        target.execute('PRAGMA journal_mode=DELETE')
        target.commit()
    finally:
        target.close(); source.close()


class PartsWriter:
    def __init__(self, output, max_bytes, prefix='inventory'):
        if not re.fullmatch(r'[a-z][a-z0-9-]*', prefix):
            raise ValueError('Unsafe inventory part prefix')
        self.output, self.max_bytes, self.prefix = output, max_bytes, prefix
        self.parts, self.stream, self.size = [], None, 0

    def write(self, body):
        original_size = len(body)
        while body:
            if self.stream is None:
                self.stream = (self.output / 'part-building').open('wb')
                self.size = 0
            take = min(len(body), self.max_bytes - self.size)
            self.stream.write(body[:take]); body = body[take:]; self.size += take
            if self.size == self.max_bytes:
                self.seal()
        return original_size

    def seal(self):
        if self.stream is None:
            return
        self.stream.flush(); os.fsync(self.stream.fileno()); self.stream.close(); self.stream = None
        path = self.output / 'part-building'
        sha = file_hash(path)
        name = f'{self.prefix}-{len(self.parts) + 1:05d}-{sha[:16]}.gz.part'
        destination = self.output / name
        if destination.exists() and file_hash(destination) != sha:
            raise ValueError('Existing immutable inventory part differs')
        os.replace(path, destination)
        self.parts.append({'file': name, 'bytes': self.size, 'sha256': sha})

    def close(self):
        if self.stream is not None:
            self.stream.close(); self.stream = None


def verify_parts(directory, expected_manifest_sha256=None):
    directory = Path(directory).resolve()
    path = directory / 'inventory-manifest.json'
    if expected_manifest_sha256 and file_hash(path) != expected_manifest_sha256:
        raise ValueError('Inventory manifest differs from its independently pinned checksum')
    manifest = json.loads(path.read_text())
    if manifest['inventory_archive_schema'] != 1 or manifest['compression'] != 'gzip_concatenated_parts':
        raise ValueError('Unsupported inventory archive schema')
    parts = manifest['parts']
    if not parts or len(parts) > 999 or len({r['file'] for r in parts}) != len(parts):
        raise ValueError('Invalid inventory part membership')
    for index, part in enumerate(parts, 1):
        expected = f'inventory-{index:05d}-{part["sha256"][:16]}.gz.part'
        if part['file'] != expected or not re.fullmatch('[0-9a-f]{64}', part['sha256']):
            raise ValueError('Unsafe or unordered inventory part name')
        path = directory / part['file']
        if path.resolve().parent != directory:
            raise ValueError('Inventory part points outside the archive')
        if not 1 <= part['bytes'] <= min(manifest['max_part_bytes'], MAX_PART_BYTES):
            raise ValueError('Inventory part exceeds its size limit')
        if path.stat().st_size != part['bytes'] or file_hash(path) != part['sha256']:
            raise ValueError('Inventory part checksum or length differs')
    return manifest


def archive(root, output, max_bytes=PART_LIMIT):
    root, output = Path(root).resolve(), Path(output).resolve()
    if not 1 <= max_bytes <= MAX_PART_BYTES:
        raise ValueError('Inventory part limit is outside the configured range')
    with writer_lock(output):
        snapshot = output / 'inventory-snapshot.sqlite3'
        origin = output / 'snapshot-origin.json'
        if (output / 'inventory-manifest.json').exists():
            manifest = verify_parts(output)
            if not origin.exists() or json.loads(origin.read_text())['root'] != str(root):
                raise ValueError('Existing archive belongs to a different inventory root')
            if manifest['max_part_bytes'] != max_bytes:
                raise ValueError('Existing archive uses a different part limit')
            return manifest
        if snapshot.exists():
            if (not origin.exists() or json.loads(origin.read_text())['root'] != str(root)
                    or file_hash(snapshot) != json.loads(origin.read_text())['sha256']):
                raise ValueError('Existing inventory snapshot provenance differs')
        else:
            temporary = output / 'snapshot-building.sqlite3'
            if temporary.exists():
                raise ValueError('An unfinished inventory copy needs inspection before retrying')
            capture_snapshot(root, temporary)
            os.replace(temporary, snapshot)
            atomic_write(origin, canonical({'root': str(root), 'sha256': file_hash(snapshot)}))
        metadata = describe(snapshot)
        writer = PartsWriter(output, max_bytes)
        try:
            with snapshot.open('rb') as source, gzip.GzipFile(filename='', fileobj=writer, mode='wb', mtime=0, compresslevel=6) as compressed:
                shutil.copyfileobj(source, compressed, length=1024 * 1024)
            writer.seal()
        finally:
            writer.close()
        manifest = {'inventory_archive_schema': 1, 'compression': 'gzip_concatenated_parts',
                    'max_part_bytes': max_bytes, 'raw_bytes': snapshot.stat().st_size,
                    'raw_sha256': file_hash(snapshot), 'parts': writer.parts, **metadata,
                    'includes_original_documents': False, 'complete_backfill': False,
                    'limitation': 'Full inventory and discovery state only. Verify every committed document against separate filing archives before resuming collection or claiming a complete restored baseline.'}
        atomic_write(output / 'inventory-manifest.json', canonical(manifest))
        verify_parts(output)
        return manifest


def restore(directory, destination, expected_manifest_sha256):
    directory, destination = Path(directory).resolve(), Path(destination).absolute()
    if not re.fullmatch('[0-9a-f]{64}', expected_manifest_sha256 or ''):
        raise ValueError('An independently pinned inventory manifest SHA-256 is required')
    if destination.exists() or destination.is_symlink():
        raise ValueError('Inventory restore destination must not exist')
    manifest = verify_parts(directory, expected_manifest_sha256)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(destination.parent / ('.' + destination.name + '.inventory-restore-lock')):
        if destination.exists() or destination.is_symlink():
            raise ValueError('Inventory restore destination must not exist')
        temporary = Path(tempfile.mkdtemp(prefix='.' + destination.name + '.inventory-', dir=destination.parent))
        try:
            compressed_path = temporary / 'inventory.gz'
            with compressed_path.open('wb') as joined:
                for part in manifest['parts']:
                    with (directory / part['file']).open('rb') as source:
                        shutil.copyfileobj(source, joined, length=1024 * 1024)
            raw = temporary / 'inventory.sqlite3'
            digest, size = hashlib.sha256(), 0
            with gzip.open(compressed_path, 'rb') as compressed, raw.open('wb') as target:
                for body in iter(lambda: compressed.read(1024 * 1024), b''):
                    size += len(body)
                    if size > manifest['raw_bytes']:
                        raise ValueError('Inventory decompression exceeds the declared byte length')
                    digest.update(body); target.write(body)
                target.flush(); os.fsync(target.fileno())
            if size != manifest['raw_bytes'] or digest.hexdigest() != manifest['raw_sha256']:
                raise ValueError('Restored inventory bytes differ')
            metadata = describe(raw)
            if any(metadata[key] != manifest[key] for key in metadata):
                raise ValueError('Restored inventory membership or scope differs')
            compressed_path.unlink()
            report = {**metadata, 'inventory_manifest_sha256': expected_manifest_sha256,
                      'raw_sha256': manifest['raw_sha256'], 'raw_bytes': size,
                      'byte_identical': True, 'includes_original_documents': False,
                      'archive_scope': 'inventory_only', 'complete_backfill': False}
            atomic_write(temporary / 'inventory-restore-report.json', canonical(report))
            if destination.exists() or destination.is_symlink():
                raise ValueError('Inventory restore destination was created by another process')
            os.rename(temporary, destination)
            return report
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-bytes', type=int, default=PART_LIMIT)
    parser.add_argument('--restore-from', type=Path)
    parser.add_argument('--expected-manifest-sha256')
    args = parser.parse_args()
    if args.restore_from:
        result = restore(args.restore_from, args.output, args.expected_manifest_sha256)
    else:
        if not args.root:
            parser.error('--root is required when making an archive')
        result = archive(args.root, args.output, args.max_bytes)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
