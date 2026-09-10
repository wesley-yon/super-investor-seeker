"""Pair a full inventory delta with exactly its changed committed documents.

An increment references an independently pinned parent checkpoint. Unchanged
documents and source files remain in that parent; full restoration rebuilds the
parent chain from pinned archives. A daily writer can build an increment without loading unchanged
historical documents. This module does not discover filings or publish releases.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile

from .audit_batches import SELECTION_COLUMNS, file_hash, readonly, run as audit
from .baseline import restore as restore_baseline
from .http import atomic_write
from .inventory import canonical
from .inventory_archive import describe
from .inventory_delta import MANIFEST as DELTA_MANIFEST, MAX_RECORD_BYTES, PRIMARY_KEYS, build as make_delta, restore as replay_delta, verify as verify_delta
from .locking import writer_lock
from .package import check_document, copy_asset, decode_row, package, verify as verify_documents
from .restore import insert_row, records, update_digest
from .runner import shard_connect

MANIFEST = 'increment.json'
COMMITTED = ('verified', 'review')


class PartsInput(io.RawIOBase):
    def __init__(self, paths):
        self.paths, self.stream = iter(paths), None

    def readable(self):
        return True

    def readinto(self, buffer):
        while True:
            if self.stream is None:
                path = next(self.paths, None)
                if path is None:
                    return 0
                self.stream = path.open('rb')
            size = self.stream.readinto(buffer)
            if size:
                return size
            self.stream.close(); self.stream = None

    def close(self):
        if self.stream is not None:
            self.stream.close(); self.stream = None
        super().close()


def delta_document_selection(directory, delta):
    """Derive membership from the complete delta, independently of the audit."""
    counts = {table: dict.fromkeys(('insert', 'update', 'delete'), 0) for table in delta['operations']}
    order = list(PRIMARY_KEYS)
    digest, size, previous = hashlib.sha256(), 0, None
    with tempfile.TemporaryDirectory(prefix='insider-increment-membership-') as folder:
        db = sqlite3.connect(Path(folder) / 'selection.sqlite3')
        try:
            db.execute('CREATE TABLE selected(' + ','.join(name + ' TEXT' for name in SELECTION_COLUMNS) + ', PRIMARY KEY(accession))')
            with io.BufferedReader(PartsInput(Path(directory) / part['file'] for part in delta['parts'])) as joined:
                with gzip.GzipFile(fileobj=joined, mode='rb') as stream:
                    while body := stream.readline(MAX_RECORD_BYTES + 1):
                        size += len(body)
                        if len(body) > MAX_RECORD_BYTES or size > delta['raw_bytes'] or not body.endswith(b'\n'):
                            raise ValueError('Inventory delta exceeds its decoded record or size bound')
                        digest.update(body)
                        operation = json.loads(body)
                        table, key = operation['table'], operation['key']
                        if (set(operation) != {'table', 'key', 'before_sha256', 'after'} or table not in counts
                                or not isinstance(key, list) or len(key) != len(PRIMARY_KEYS[table])
                                or any(type(item) is not str for item in key)):
                            raise ValueError('Inventory delta operation identity differs')
                        position = (order.index(table), tuple(key))
                        if previous is not None and position <= previous:
                            raise ValueError('Inventory delta operations are duplicated or out of order')
                        previous = position
                        before, after = operation['before_sha256'], operation['after']
                        if before is not None and (not isinstance(before, str) or not re.fullmatch('[0-9a-f]{64}', before)):
                            raise ValueError('Inventory delta old-row digest differs')
                        if before is None and after is None:
                            raise ValueError('Inventory delta operation has no old or new row')
                        counts[table]['insert' if before is None else 'delete' if after is None else 'update'] += 1
                        if after is not None:
                            row = decode_row(after)
                            if [row[name] for name in PRIMARY_KEYS[table]] != key:
                                raise ValueError('Inventory delta row key differs')
                            if table == 'filings' and row['status'] in COMMITTED:
                                if not all(row[name] for name in ('shard', 'source_sha256', 'parsed_sha256')):
                                    raise ValueError('Committed delta row lacks document provenance')
                                db.execute('INSERT INTO selected VALUES(' + ','.join('?' for _ in SELECTION_COLUMNS) + ')',
                                           tuple(row[name] for name in SELECTION_COLUMNS))
            if size != delta['raw_bytes'] or digest.hexdigest() != delta['raw_sha256'] or counts != delta['operations']:
                raise ValueError('Inventory delta content differs from its manifest')
            selection, documents = hashlib.sha256(), 0
            for row in db.execute('SELECT * FROM selected ORDER BY filing_date,accession'):
                selection.update(canonical(dict(zip(SELECTION_COLUMNS, row))) + b'\n'); documents += 1
            return documents, selection.hexdigest()
        finally:
            db.close()


def pinned_json(path, digest):
    path = Path(path)
    if (not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest)
            or path.is_symlink() or path.stat().st_size > 2_000_000 or file_hash(path) != digest):
        raise ValueError('Checkpoint metadata differs from its independently pinned checksum')
    return json.loads(path.read_text())


def checkpoint(path, digest):
    path = Path(path)
    value = pinned_json(path, digest)
    if value.get('baseline_schema') == 1:
        nested = 'inventory-manifest.json'
        inventory = pinned_json(path.parent / nested, value['inventory_manifest_sha256'])
        if (not any(row['file'] == nested and row['sha256'] == value['inventory_manifest_sha256'] for row in value['files'])
                or inventory['scope'] != value['scope'] or inventory['tables']['filings'] != value['inventory_filings']
                or inventory['committed_documents'] != value['documents']
                or inventory['committed_selection_sha256'] != value['selection_sha256']):
            raise ValueError('Parent baseline inventory metadata differs')
        return {'kind': 'baseline', 'manifest_sha256': digest}, {'raw_sha256': inventory['raw_sha256']}
    if value.get('increment_schema') == 1:
        delta = pinned_json(path.parent / DELTA_MANIFEST, value['inventory_delta_manifest_sha256'])
        if (not any(row['file'] == DELTA_MANIFEST and row['sha256'] == value['inventory_delta_manifest_sha256'] for row in value['files'])
                or delta['target'] != value['target_inventory_state']):
            raise ValueError('Parent incremental inventory metadata differs')
        return {'kind': 'increment', 'manifest_sha256': digest}, {'state': delta['target']}
    raise ValueError('Unsupported parent checkpoint schema')


def frozen_hash(path):
    path = Path(path)
    wal = Path(str(path) + '-wal')
    if wal.exists() and wal.stat().st_size:
        raise ValueError('Use a frozen inventory copy, not a live WAL database')
    return file_hash(path)


def source_rows(db):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for table in ('sources', 'index_sources'):
        if table in tables:
            for row in db.execute('SELECT * FROM ' + table + ' ORDER BY source_key'):
                yield table, dict(row)


def source_path(root, table, row):
    root = Path(root).resolve()
    if not re.fullmatch(r'\d{4}Q[1-4]', row['source_key']):
        raise ValueError('Unsafe source key')
    if table == 'sources':
        relative = Path(row['path'])
        if (relative.is_absolute() or relative.suffix != '.zip'
                or any(not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', part) for part in relative.parts)):
            raise ValueError('Unsafe quarterly source path')
        path = root / relative
    else:
        stem = hashlib.sha256(row['url'].encode()).hexdigest()
        path = root / 'sources' / 'indexes' / (stem + '.body')
    if not path.resolve().is_relative_to(root):
        raise ValueError('Source path points outside its checkpoint')
    return path


def source_matches(first, second):
    return first is not None and first['sha256'] == second['sha256'] and first['bytes'] == second['bytes']


def source_assets(root, parent_inventory, target_inventory, output):
    parent, target = readonly(parent_inventory), readonly(target_inventory)
    try:
        previous = {(table, row['source_key']): row for table, row in source_rows(parent)}
        assets = []
        for table, row in source_rows(target):
            if source_matches(previous.get((table, row['source_key'])), row):
                continue
            source = source_path(root, table, row)
            if source.stat().st_size != row['bytes'] or file_hash(source) != row['sha256']:
                raise ValueError('Changed source bytes differ from the target inventory')
            if table == 'sources':
                name = f'quarter-{row["source_key"]}-{row["sha256"][:16]}.zip'
                asset = copy_asset(source, output, name, row['sha256'])
            else:
                name = f'index-{row["source_key"]}-{row["sha256"][:16]}.idx.gz'
                with source.open('rb') as stream, (output / name).open('wb') as raw:
                    with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0, compresslevel=6) as packed:
                        shutil.copyfileobj(stream, packed, length=1024 * 1024)
                asset = {'file': name, 'bytes': (output / name).stat().st_size, 'sha256': file_hash(output / name)}
            assets.append({**asset, 'source_table': table, 'source_key': row['source_key'],
                           'raw_sha256': row['sha256'], 'raw_bytes': row['bytes']})
        return assets
    finally:
        parent.close(); target.close()


def build(root, parent_manifest, parent_pin, parent_inventory, target_inventory, output, workers=4):
    root, output = Path(root), Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('Increment output must not exist')
    parent_ref, parent_identity = checkpoint(parent_manifest, parent_pin)
    parent_hash, target_hash = frozen_hash(parent_inventory), frozen_hash(target_inventory)
    if parent_identity.get('raw_sha256') not in (None, parent_hash):
        raise ValueError('Parent inventory bytes differ from the pinned baseline')
    output.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(output.parent / ('.' + output.name + '.increment-lock')):
        if output.exists() or output.is_symlink():
            raise ValueError('Increment output must not exist')
        temporary = Path(tempfile.mkdtemp(prefix='.' + output.name + '.building-', dir=output.parent))
        failed = False
        try:
            delta_dir, audit_dir, documents = temporary / 'delta-work', temporary / 'audit-work', temporary / 'documents-work'
            delta = make_delta(parent_inventory, target_inventory, delta_dir,
                               progress=lambda event: print(json.dumps({'increment_inventory': event}), flush=True))
            if 'state' in parent_identity and parent_identity['state'] != delta['parent']:
                raise ValueError('Parent inventory state differs from the pinned increment')
            evidence = audit(root, audit_dir, workers=workers, inventory_snapshot=target_inventory, parent_inventory=parent_inventory)
            package(root, audit_dir, documents, include_sources=False)
            document_manifest = json.loads((documents / 'manifest.json').read_text())
            if (document_manifest['inventory_snapshot_sha256'] != target_hash
                    or document_manifest['parent_inventory_snapshot_sha256'] != parent_hash):
                raise ValueError('Document selection and inventory delta use different frozen inputs')
            files = []
            for directory, assets in ((delta_dir, delta['parts']), (documents, document_manifest['chunks'] + document_manifest['assets'])):
                for asset in assets:
                    files.append(copy_asset(directory / asset['file'], temporary, asset['file'], asset['sha256']))
            for directory, name in ((delta_dir, DELTA_MANIFEST), (documents, 'manifest.json')):
                files.append(copy_asset(directory / name, temporary, name, file_hash(directory / name)))
            sources = source_assets(root, parent_inventory, target_inventory, temporary)
            files.extend({key: asset[key] for key in ('file', 'sha256', 'bytes')} for asset in sources)
            metadata = describe(target_inventory)
            value = {'increment_schema': 1, 'parent': parent_ref,
                     'inventory_delta_manifest_sha256': file_hash(delta_dir / DELTA_MANIFEST),
                     'document_manifest_sha256': file_hash(documents / 'manifest.json'),
                     'parent_inventory_file_sha256': parent_hash, 'target_inventory_file_sha256': target_hash,
                     'target_inventory_state': delta['target'], 'target_inventory': metadata,
                     'changed_committed_documents': document_manifest['documents'],
                     'changed_selection_sha256': document_manifest['selection_sha256'],
                     'changed_source_audit_sha256': evidence['semantic_sha256'],
                     'source_assets': sources, 'files': files,
                     'requires_parent_checkpoint': True, 'standalone_restore_ready': False, 'complete_backfill': False}
            atomic_write(temporary / MANIFEST, canonical(value))
            manifest_pin = file_hash(temporary / MANIFEST)
            verify(temporary, manifest_pin)
            if frozen_hash(parent_inventory) != parent_hash or frozen_hash(target_inventory) != target_hash:
                raise ValueError('A frozen input changed while the increment was being built')
            for directory in (delta_dir, audit_dir, documents):
                shutil.rmtree(directory)
            if output.exists() or output.is_symlink():
                raise ValueError('Increment output was created by another process')
            os.rename(temporary, output)
            return {**value, 'increment_manifest_sha256': manifest_pin}
        except BaseException as error:
            failed = True
            atomic_write(temporary / 'failure.json', canonical({'error_type': type(error).__name__, 'error': str(error),
                         'checkpoint_published': False, 'complete_backfill': False}))
            print(json.dumps({'failed_increment_work': str(temporary), 'checkpoint_published': False}), flush=True)
            raise
        finally:
            if temporary.exists() and not failed:
                shutil.rmtree(temporary)


def verify(directory, expected_manifest_sha256):
    directory = Path(directory).resolve()
    value = pinned_json(directory / MANIFEST, expected_manifest_sha256)
    if (value.get('increment_schema') != 1 or value.get('requires_parent_checkpoint') is not True
            or value.get('standalone_restore_ready') is not False or value.get('complete_backfill') is not False
            or value['parent']['kind'] not in ('baseline', 'increment')
            or not re.fullmatch('[0-9a-f]{64}', value['parent']['manifest_sha256'])):
        raise ValueError('Unsupported incremental checkpoint schema or parent')
    files = {}
    for asset in value['files']:
        name = asset['file']
        if (not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', name) or name in files or name == MANIFEST
                or type(asset['bytes']) is not int or not 0 < asset['bytes'] <= 1_000_000_000
                or not re.fullmatch('[0-9a-f]{64}', asset['sha256'])):
            raise ValueError('Unsafe or duplicate incremental checkpoint asset')
        path = directory / name
        if path.is_symlink() or path.resolve().parent != directory or path.stat().st_size != asset['bytes'] or file_hash(path) != asset['sha256']:
            raise ValueError('Incremental checkpoint asset bytes differ')
        files[name] = asset
    if not 1 <= len(files) <= 999:
        raise ValueError('Increment exceeds one release asset count')
    delta = verify_delta(directory, value['inventory_delta_manifest_sha256'])
    verify_documents(directory, value['document_manifest_sha256'])
    documents = json.loads((directory / 'manifest.json').read_text())
    changed_documents, selection_hash = delta_document_selection(directory, delta)
    source_audit = json.loads((directory / 'source-audit.json').read_text())
    semantic = {key: source_audit[key] for key in ('selection_sha256', 'audit_version', 'counts', 'financial_table_comparisons', 'groups')}
    if (documents['include_sources'] is not False or documents.get('selection_kind') != 'changed_committed_inventory_rows'
            or documents['inventory_snapshot_sha256'] != value['target_inventory_file_sha256']
            or documents['parent_inventory_snapshot_sha256'] != value['parent_inventory_file_sha256']
            or documents['documents'] != value['changed_committed_documents']
            or documents['selection_sha256'] != value['changed_selection_sha256']
            or documents['audit_semantic_sha256'] != value['changed_source_audit_sha256']
            or documents['documents'] != changed_documents or documents['selection_sha256'] != selection_hash
            or source_audit['selected_documents'] != changed_documents or source_audit['selection_sha256'] != selection_hash
            or hashlib.sha256(canonical(semantic)).hexdigest() != documents['audit_semantic_sha256']
            or any(source_audit['counts'].get(key, 0) for key in ('document_failure', 'original_xml_field_failure'))
            or delta['target'] != value['target_inventory_state']
            or documents['scope'] != value['target_inventory']['scope']
            or {table: item['rows'] for table, item in delta['target']['tables'].items()} != value['target_inventory']['tables']):
        raise ValueError('Inventory, document, and source-audit checkpoint bindings differ')
    source_keys = set()
    for asset in value['source_assets']:
        key = (asset['source_table'], asset['source_key'])
        if (key in source_keys or key[0] not in ('sources', 'index_sources') or not re.fullmatch(r'\d{4}Q[1-4]', key[1])
                or type(asset['raw_bytes']) is not int or asset['raw_bytes'] <= 0 or not re.fullmatch('[0-9a-f]{64}', asset['raw_sha256'])
                or files.get(asset['file']) != {name: asset[name] for name in ('file', 'sha256', 'bytes')}):
            raise ValueError('Incremental source asset identity differs')
        source_keys.add(key)
        if asset['source_table'] == 'sources':
            if asset['sha256'] != asset['raw_sha256'] or asset['bytes'] != asset['raw_bytes']:
                raise ValueError('Quarterly source bytes differ from their raw identity')
        else:
            size, digest = 0, hashlib.sha256()
            with gzip.open(directory / asset['file'], 'rb') as stream:
                for body in iter(lambda: stream.read(1024 * 1024), b''):
                    size += len(body)
                    if size > asset['raw_bytes']:
                        raise ValueError('Index asset exceeds its declared raw length')
                    digest.update(body)
            if size != asset['raw_bytes'] or digest.hexdigest() != asset['raw_sha256']:
                raise ValueError('Index asset differs from its original source bytes')
    required = {part['file'] for part in delta['parts'] + documents['chunks'] + documents['assets'] + value['source_assets']}
    required.update((DELTA_MANIFEST, 'manifest.json'))
    if required != set(files):
        raise ValueError('Incremental checkpoint asset membership is incomplete')
    return value


def shard_path(root, relative):
    if not isinstance(relative, str) or not re.fullmatch(r'shards/\d{4}-\d{2}-\d{4}\.sqlite3', relative):
        raise ValueError('Unsafe restored shard path')
    path = Path(root) / relative
    if not path.resolve().is_relative_to(Path(root).resolve()):
        raise ValueError('Shard points outside its checkpoint')
    return path


def restore_sources(parent_root, root, directory, parent_db, target_db, assets):
    previous = {(table, row['source_key']): row for table, row in source_rows(parent_db)}
    available = {(asset['source_table'], asset['source_key']): asset for asset in assets}
    used, count = set(), 0
    for table, row in source_rows(target_db):
        key = (table, row['source_key'])
        destination = source_path(root, table, row)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source_matches(previous.get(key), row):
            if key in available:
                raise ValueError('Unchanged source was unexpectedly included in the increment')
            source = source_path(parent_root, table, previous[key])
            shutil.copyfile(source, destination)
        else:
            asset = available.get(key)
            if asset is None or asset['raw_sha256'] != row['sha256'] or asset['raw_bytes'] != row['bytes']:
                raise ValueError('Changed source is missing from the increment')
            used.add(key)
            source = directory / asset['file']
            if table == 'sources':
                shutil.copyfile(source, destination)
            else:
                size = 0
                with gzip.open(source, 'rb') as stream, destination.open('wb') as target:
                    for body in iter(lambda: stream.read(1024 * 1024), b''):
                        size += len(body)
                        if size > row['bytes']:
                            raise ValueError('Index source exceeds its declared raw byte length')
                        target.write(body)
        if destination.stat().st_size != row['bytes'] or file_hash(destination) != row['sha256']:
            raise ValueError('Restored source bytes differ from the target inventory')
        if table == 'index_sources':
            atomic_write(destination.with_suffix('.json'), canonical({'url': row['url'], 'sha256': row['sha256'],
                         'bytes': row['bytes'], 'retrieved_at_utc': row['retrieved_at']}))
        count += 1
    if used != set(available):
        raise ValueError('Increment contains an unrelated source asset')
    return count


def _restore_from_verified_parent(directory, parent_manifest, parent_root, destination, expected_manifest_sha256, value):
    """Internal step; the public restorer constructs the parent from pinned assets."""
    directory, parent_root, destination = Path(directory).resolve(), Path(parent_root).resolve(), Path(destination).absolute()
    if destination.exists() or destination.is_symlink():
        raise ValueError('Increment restore destination must not exist')
    parent_ref, parent_identity = checkpoint(parent_manifest, value['parent']['manifest_sha256'])
    if parent_ref != value['parent']:
        raise ValueError('Increment parent checkpoint identity differs')
    parent_inventory = parent_root / 'inventory.sqlite3'
    if parent_identity.get('raw_sha256') not in (None, frozen_hash(parent_inventory)):
        raise ValueError('Restored parent inventory differs from the pinned baseline')
    delta = json.loads((directory / DELTA_MANIFEST).read_text())
    if 'state' in parent_identity and parent_identity['state'] != delta['parent']:
        raise ValueError('Inventory delta starts from a different parent checkpoint')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(destination.parent / ('.' + destination.name + '.increment-restore-lock')):
        if destination.exists() or destination.is_symlink():
            raise ValueError('Increment restore destination must not exist')
        temporary = Path(tempfile.mkdtemp(prefix='.' + destination.name + '.restoring-', dir=destination.parent))
        root = temporary / 'state'
        parent_db, target_db = None, None
        targets, parents = {}, {}
        try:
            replay_delta(parent_inventory, directory, root, value['inventory_delta_manifest_sha256'])
            if describe(root / 'inventory.sqlite3') != value['target_inventory']:
                raise ValueError('Restored inventory coverage differs from its checkpoint')
            parent_db, target_db = readonly(parent_inventory), readonly(root / 'inventory.sqlite3')
            documents = json.loads((directory / 'manifest.json').read_text())
            changed = 0
            for inventory, document in records(directory, documents):
                target = target_db.execute('SELECT * FROM filings WHERE accession=?', (inventory['accession'],)).fetchone()
                if target is None or dict(target) != inventory or inventory['status'] not in COMMITTED:
                    raise ValueError('Increment document row differs from its target inventory')
                check_document(inventory, document)
                relative = inventory['shard']
                if relative not in targets:
                    targets[relative] = shard_connect(shard_path(root, relative))
                insert_row(targets[relative], 'documents', document); changed += 1
            if changed != value['changed_committed_documents']:
                raise ValueError('Increment document membership is incomplete')
            expected_rows, restored = hashlib.sha256(), 0
            for row in target_db.execute("SELECT * FROM filings WHERE status IN ('verified','review') ORDER BY filing_date,accession"):
                inventory = dict(row)
                old = parent_db.execute('SELECT * FROM filings WHERE accession=?', (row['accession'],)).fetchone()
                same = old is not None and dict(old) == inventory
                relative = inventory['shard']
                if relative not in targets:
                    targets[relative] = shard_connect(shard_path(root, relative))
                document = targets[relative].execute('SELECT * FROM documents WHERE accession=?', (row['accession'],)).fetchone()
                if same:
                    if document is not None:
                        raise ValueError('Unchanged document was unexpectedly included in the increment')
                    if relative not in parents:
                        parents[relative] = readonly(shard_path(parent_root, relative))
                    document = parents[relative].execute('SELECT * FROM documents WHERE accession=?', (row['accession'],)).fetchone()
                    if document is None:
                        raise ValueError('Parent checkpoint is missing an inherited document')
                    insert_row(targets[relative], 'documents', dict(document))
                elif document is None:
                    raise ValueError('Changed committed document is missing from the increment')
                check_document(inventory, document)
                update_digest(expected_rows, inventory, document); restored += 1
                if restored % 1000 == 0:
                    for db in targets.values():
                        db.commit()
            if restored != value['target_inventory']['committed_documents']:
                raise ValueError('Full restored document membership differs')
            source_count = restore_sources(parent_root, root, directory, parent_db, target_db, value['source_assets'])
            for db in targets.values():
                db.commit(); db.execute('PRAGMA wal_checkpoint(TRUNCATE)'); db.close()
            targets.clear()
            actual_rows, actual_count = hashlib.sha256(), 0
            for row in target_db.execute("SELECT * FROM filings WHERE status IN ('verified','review') ORDER BY filing_date,accession"):
                relative = row['shard']
                if relative not in targets:
                    targets[relative] = readonly(shard_path(root, relative))
                document = targets[relative].execute('SELECT * FROM documents WHERE accession=?', (row['accession'],)).fetchone()
                check_document(row, document); update_digest(actual_rows, row, document); actual_count += 1
            if actual_count != restored or actual_rows.hexdigest() != expected_rows.hexdigest():
                raise ValueError('Restored document rows changed during database readback')
            report = {'increment_manifest_sha256': expected_manifest_sha256, 'parent': value['parent'],
                      'restored_documents': restored, 'changed_documents': changed, 'inherited_documents': restored - changed,
                      'restored_rows_sha256': actual_rows.hexdigest(), 'source_files_verified': source_count,
                      'every_committed_document_restored': True, 'database_readback_verified': True,
                      'collection_resume_ready': True, 'complete_backfill': False}
            atomic_write(root / 'increment-restore-report.json', canonical(report))
            for db in targets.values():
                db.close()
            targets.clear(); target_db.close(); target_db = None
            if destination.exists() or destination.is_symlink():
                raise ValueError('Increment restore destination was created by another process')
            os.rename(root, destination)
            return report
        finally:
            for db in [*targets.values(), *parents.values(), parent_db, target_db]:
                if db is not None:
                    db.close()
            if temporary.exists():
                shutil.rmtree(temporary)


def restore(directory, parent_manifest, destination, expected_manifest_sha256, ancestor_manifests=(), _depth=0):
    """Rebuild the complete parent chain from archives; never trust a state folder."""
    directory, parent_manifest, destination = Path(directory).resolve(), Path(parent_manifest).resolve(), Path(destination).absolute()
    if destination.exists() or destination.is_symlink():
        raise ValueError('Increment restore destination must not exist')
    if _depth >= 64:
        raise ValueError('Checkpoint chain requires compaction before full restoration')
    value = verify(directory, expected_manifest_sha256)
    reference, _ = checkpoint(parent_manifest, value['parent']['manifest_sha256'])
    if reference != value['parent']:
        raise ValueError('Increment parent checkpoint identity differs')
    ancestors = {file_hash(path): Path(path).resolve() for path in ancestor_manifests}
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.' + destination.name + '.parent-', dir=destination.parent) as folder:
        parent_state = Path(folder) / 'state'
        if reference['kind'] == 'baseline':
            restore_baseline(parent_manifest.parent, parent_state, reference['manifest_sha256'])
        else:
            parent_value = pinned_json(parent_manifest, reference['manifest_sha256'])
            grandparent = ancestors.get(parent_value['parent']['manifest_sha256'])
            if grandparent is None:
                raise ValueError('The pinned ancestor manifest is required for full restoration')
            restore(parent_manifest.parent, grandparent, parent_state, reference['manifest_sha256'], ancestor_manifests, _depth + 1)
        return _restore_from_verified_parent(directory, parent_manifest, parent_state, destination, expected_manifest_sha256, value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--parent-manifest', type=Path, required=True)
    parser.add_argument('--parent-sha256')
    parser.add_argument('--parent-inventory', type=Path)
    parser.add_argument('--inventory-snapshot', type=Path)
    parser.add_argument('--root', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=4, choices=range(1, 5))
    parser.add_argument('--restore-from', type=Path)
    parser.add_argument('--ancestor-manifest', type=Path, action='append', default=[], help='An older parent manifest required to rebuild the complete chain')
    parser.add_argument('--expected-manifest-sha256')
    args = parser.parse_args()
    if args.restore_from:
        result = restore(args.restore_from, args.parent_manifest, args.output, args.expected_manifest_sha256, args.ancestor_manifest)
    else:
        if not all((args.root, args.parent_sha256, args.parent_inventory, args.inventory_snapshot)):
            parser.error('--root, --parent-sha256, --parent-inventory and --inventory-snapshot are required to build an increment')
        result = build(args.root, args.parent_manifest, args.parent_sha256, args.parent_inventory, args.inventory_snapshot, args.output, args.workers)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
