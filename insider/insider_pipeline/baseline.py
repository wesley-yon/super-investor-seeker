"""Pair a full inventory snapshot with exactly its audited collected documents."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile

from .audit_batches import file_hash, readonly
from .http import atomic_write
from .inventory import canonical
from .inventory_archive import restore as restore_inventory, verify_parts
from .locking import writer_lock
from .package import copy_asset, verify as verify_documents
from .restore import restore as restore_documents


def alignment(inventory, documents):
    if (not documents['include_sources'] or inventory['scope'] != documents['scope']
            or inventory['committed_documents'] != documents['documents']
            or inventory['committed_selection_sha256'] != documents['selection_sha256']
            or inventory['raw_sha256'] != documents.get('inventory_snapshot_sha256')):
        raise ValueError('Inventory and document archives are not the same complete collected checkpoint')


def build(inventory_directory, document_directory, output):
    inventory_directory, document_directory, output = map(Path, (inventory_directory, document_directory, output))
    with writer_lock(output):
        inventory = verify_parts(inventory_directory)
        verify_documents(document_directory)
        documents = json.loads((document_directory / 'manifest.json').read_text())
        alignment(inventory, documents)
        config = {'inventory_manifest_sha256': file_hash(inventory_directory / 'inventory-manifest.json'),
                  'document_manifest_sha256': file_hash(document_directory / 'manifest.json')}
        if (output / 'baseline.json').exists():
            old = json.loads((output / 'baseline.json').read_text())
            if any(old[k] != value for k, value in config.items()):
                raise ValueError('Existing baseline belongs to a different checkpoint')
            return verify(output, file_hash(output / 'baseline.json'))
        files = []
        tasks = [(inventory_directory, p) for p in inventory['parts']]
        tasks += [(document_directory, p) for p in documents['chunks'] + documents['assets']]
        for directory, name in [(inventory_directory, 'inventory-manifest.json'), (document_directory, 'manifest.json')]:
            tasks.append((directory, {'file': name, 'sha256': file_hash(directory / name)}))
        names = [p['file'] for _, p in tasks]
        if len(set(names)) != len(names) or len(names) + 1 > 1000:
            raise ValueError('Baseline filenames collide or exceed one release asset count')
        for directory, asset in tasks:
            files.append(copy_asset(directory / asset['file'], output, asset['file'], asset['sha256']))
        manifest = {'baseline_schema': 1, **config, 'scope': inventory['scope'],
                    'documents': inventory['committed_documents'], 'inventory_filings': inventory['tables']['filings'],
                    'selection_sha256': inventory['committed_selection_sha256'],
                    'queue_counts_at_checkpoint': inventory['queue_counts'], 'files': files,
                    'documents_match_inventory_selection': True, 'complete_backfill': False,
                    'limitation': 'A complete checkpoint of collected documents and the full queue. Pending and review work still require resolution; this is not a completed market-wide backfill.'}
        atomic_write(output / 'baseline.json', canonical(manifest))
        return verify(output, file_hash(output / 'baseline.json'))


def verify(directory, expected_baseline_sha256):
    directory = Path(directory).resolve()
    if (not re.fullmatch('[0-9a-f]{64}', expected_baseline_sha256 or '')
            or file_hash(directory / 'baseline.json') != expected_baseline_sha256):
        raise ValueError('Baseline differs from its independently pinned checksum')
    baseline = json.loads((directory / 'baseline.json').read_text())
    if baseline['baseline_schema'] != 1:
        raise ValueError('Unsupported baseline schema')
    files = baseline['files']
    if len({r['file'] for r in files}) != len(files):
        raise ValueError('Duplicate baseline assets')
    for asset in files:
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', asset['file']):
            raise ValueError('Unsafe baseline asset path')
        path = directory / asset['file']
        if (path.resolve().parent != directory or path.stat().st_size != asset['bytes']
                or file_hash(path) != asset['sha256']):
            raise ValueError('Baseline asset checksum, length or path differs')
    inventory = verify_parts(directory, baseline['inventory_manifest_sha256'])
    verify_documents(directory, baseline['document_manifest_sha256'])
    documents = json.loads((directory / 'manifest.json').read_text())
    alignment(inventory, documents)
    required = {r['file'] for r in inventory['parts'] + documents['chunks'] + documents['assets']}
    required.update(('manifest.json', 'inventory-manifest.json'))
    if required != {r['file'] for r in files}:
        raise ValueError('Baseline asset list is incomplete')
    if (baseline['scope'] != inventory['scope'] or baseline['documents'] != inventory['committed_documents']
            or baseline['selection_sha256'] != inventory['committed_selection_sha256']
            or baseline['inventory_filings'] != inventory['tables']['filings']
            or baseline['queue_counts_at_checkpoint'] != inventory['queue_counts']):
        raise ValueError('Baseline coverage differs from the inventory')
    return {'baseline_sha256': expected_baseline_sha256, 'documents': baseline['documents'],
            'inventory_filings': baseline['inventory_filings'], 'assets': len(files) + 1,
            'asset_bytes': sum(r['bytes'] for r in files) + (directory / 'baseline.json').stat().st_size,
            'matched_checkpoint_verified': True, 'complete_backfill': False}


def restore(directory, destination, expected_baseline_sha256):
    directory, destination = Path(directory).resolve(), Path(destination).absolute()
    if destination.exists() or destination.is_symlink():
        raise ValueError('Baseline restore destination must not exist')
    verification = verify(directory, expected_baseline_sha256)
    baseline = json.loads((directory / 'baseline.json').read_text())
    destination.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(destination.parent / ('.' + destination.name + '.baseline-restore-lock')):
        if destination.exists() or destination.is_symlink():
            raise ValueError('Baseline restore destination must not exist')
        temporary = Path(tempfile.mkdtemp(prefix='.' + destination.name + '.baseline-', dir=destination.parent))
        try:
            full, selected = temporary / 'full', temporary / 'selected'
            inventory_report = restore_inventory(directory, full, baseline['inventory_manifest_sha256'])
            document_report = restore_documents(directory, selected, baseline['document_manifest_sha256'])
            inventory_db, document_db = readonly(full / 'inventory.sqlite3'), readonly(selected / 'inventory.sqlite3')
            try:
                count = 0
                for row in document_db.execute('SELECT * FROM filings ORDER BY accession'):
                    original = inventory_db.execute('SELECT * FROM filings WHERE accession=?', (row['accession'],)).fetchone()
                    if original is None or dict(original) != dict(row):
                        raise ValueError('Archived filing row differs from its inventory checkpoint')
                    count += 1
                if count != inventory_report['committed_documents'] or count != document_report['restored_documents']:
                    raise ValueError('Restored committed document membership is incomplete')
                for name in ('shards', 'sources'):
                    os.rename(selected / name, full / name)
                for row in inventory_db.execute('SELECT * FROM sources'):
                    relative = Path(row['path'])
                    if (not re.fullmatch(r'\d{4}Q[1-4]', row['source_key'])
                            or relative.is_absolute() or '..' in relative.parts or relative.suffix != '.zip'
                            or not all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', p) for p in relative.parts)):
                        raise ValueError('Unsafe inventory source path')
                    target = full / relative
                    standard = full / 'sources' / 'quarterly' / f'{row["source_key"]}-{row["sha256"][:16]}.zip'
                    if not target.exists():
                        target.parent.mkdir(parents=True, exist_ok=True)
                        os.link(standard, target)
                    if file_hash(target) != row['sha256']:
                        raise ValueError('Restored source does not match full inventory provenance')
                if inventory_db.execute("SELECT 1 FROM sqlite_master WHERE name='index_sources'").fetchone():
                    for row in inventory_db.execute('SELECT * FROM index_sources'):
                        stem = hashlib.sha256(row['url'].encode()).hexdigest()
                        body = full / 'sources' / 'indexes' / (stem + '.body')
                        if body.stat().st_size != row['bytes'] or file_hash(body) != row['sha256']:
                            raise ValueError('Restored index does not match full inventory provenance')
            finally:
                inventory_db.close(); document_db.close()
            for source_name, destination_name in [('archive-catalog.json', 'archive-catalog.json'),
                                                   ('restore-report.json', 'document-restore-report.json')]:
                os.rename(selected / source_name, full / destination_name)
            if file_hash(full / 'inventory.sqlite3') != inventory_report['raw_sha256']:
                raise ValueError('Full inventory changed during checkpoint assembly')
            report = {**verification, 'inventory_rows_byte_identical': True,
                      'every_committed_filing_row_matched': True, 'source_assets_verified': True,
                      'restored_collected_checkpoint_complete': True, 'collection_resume_ready': True,
                      'queue_counts_at_checkpoint': inventory_report['queue_counts'], 'complete_backfill': False}
            atomic_write(full / 'baseline-restore-report.json', canonical(report))
            if destination.exists() or destination.is_symlink():
                raise ValueError('Baseline restore destination was created by another process')
            os.rename(full, destination)
            return report
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--inventory', type=Path)
    parser.add_argument('--documents', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--restore-from', type=Path)
    parser.add_argument('--expected-baseline-sha256')
    args = parser.parse_args()
    if args.restore_from:
        result = restore(args.restore_from, args.output, args.expected_baseline_sha256)
    else:
        if not args.inventory or not args.documents:
            parser.error('--inventory and --documents are required to build a baseline')
        result = build(args.inventory, args.documents, args.output)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
