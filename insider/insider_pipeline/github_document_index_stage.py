"""Append a verified lookup index and private selection to its checkpoint bucket."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

from . import document_index as index
from . import github_chain as transport
from . import github_increment_stage as stage
from .audit_batches import readonly
from .http import atomic_write
from .inventory import canonical
from .inventory_delta import schema, state
from .locking import writer_lock


def stage_index(directory, evidence, index_pin, accessions, inventory_path, tag, transport_pin, expected_latest_id):
    directory, evidence = Path(directory).resolve(), Path(evidence).absolute()
    inventory_hash = index.frozen_hash(inventory_path)
    value = index.pinned_json(directory / index.MANIFEST, index_pin)
    selected = index.selection(index_pin, accessions)
    selection_body = canonical(selected)
    selection_pin = hashlib.sha256(selection_body).hexdigest()
    locator = transport.validate_locator({'layout': 'content_addressed', 'tag': tag, 'sha256': transport_pin})
    with writer_lock(evidence):
        stage.check_repository(stage.api('repos/' + stage.REPOSITORY))
        latest_before = stage.api('repos/' + stage.REPOSITORY + '/releases/latest')
        if latest_before['id'] != expected_latest_id or not latest_before['tag_name'].startswith('dataset-'):
            raise ValueError('The dataset pointer changed; refresh its preflight before staging the index')
        work = Path(tempfile.mkdtemp(prefix='index-stage-', dir=evidence))
        chain = transport.metadata(locator, work / 'archives', transport.Downloader(work / 'metadata-cache'))
        index.validate_manifest(value, chain)
        db = readonly(inventory_path)
        try:
            if state(db, schema(db)) != chain[-1]['manifest']['target_inventory_state']:
                raise ValueError('Index staging inventory differs from the complete pinned checkpoint')
        finally:
            db.close()
        data = directory / value['index_file']['file']
        index.verify_inventory(data, value, inventory_path)
        wanted = set(selected['accessions'])
        if sum(row[0] in wanted for row in index.records(data, value)) != len(wanted):
            raise ValueError('A selected filing is absent from the complete document index')
        release = stage.release_for_tag(tag)
        transport.validate_bucket(release, tag, writable=True)
        if release['id'] != chain[-1]['release_id']:
            raise ValueError('Document-index bucket identity differs from its checkpoint')
        endpoint = 'repos/' + stage.REPOSITORY + '/releases/' + str(release['id']) + '/assets?per_page=100'
        before = transport.asset_map(stage.api(endpoint, pages=True), bucket=True)
        upload = work / 'upload'; upload.mkdir()
        files = {transport.blob_name(index_pin): (directory / index.MANIFEST, index_pin),
                 transport.blob_name(value['index_file']['sha256']): (data, value['index_file']['sha256'])}
        for name, (path, _) in files.items():
            shutil.copyfile(path, upload / name)
        selection_name = transport.blob_name(selection_pin)
        atomic_write(upload / selection_name, selection_body)
        files[selection_name] = (upload / selection_name, selection_pin)
        expected = {name: {'file': name, 'bytes': (upload / name).stat().st_size, 'sha256': pin}
                    for name, (_, pin) in files.items()}
        stage.check_capacity(before, expected)
        manifest_name = transport.blob_name(index_pin)
        payloads = sorted(set(expected) - {manifest_name})

        def send(name):
            stage.command(['release', 'upload', tag, str(upload / name), '--repo', stage.REPOSITORY])

        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(send, [name for name in payloads if name not in before]))
        current = transport.asset_map(stage.api(endpoint, pages=True), bucket=True)
        for name in payloads:
            asset = expected[name]
            transport.check_asset(current, name, asset['bytes'], asset['sha256'])
        reader = transport.Downloader(work / 'readback', budget=sum(asset['bytes'] for asset in expected.values()))
        for name in payloads:
            reader.plan(tag, name, expected[name]['bytes'], expected[name]['sha256'])
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda name: reader.get(tag, name, expected[name]['bytes'], expected[name]['sha256']), payloads))
        copied = reader.downloaded[(tag, transport.blob_name(value['index_file']['sha256']))]
        index.verify_inventory(copied, value, inventory_path)
        if json.loads(reader.downloaded[(tag, selection_name)].read_text()) != selected:
            raise ValueError('Private filing selection differs after independent readback')
        if index.frozen_hash(inventory_path) != inventory_hash:
            raise ValueError('Frozen index staging inventory changed before publication')
        if manifest_name not in before:
            send(manifest_name)
        current = transport.asset_map(stage.api(endpoint, pages=True), bucket=True)
        for name, asset in expected.items():
            transport.check_asset(current, name, asset['bytes'], asset['sha256'])
        copied_manifest = reader.get(tag, manifest_name, expected[manifest_name]['bytes'], index_pin)
        if index.pinned_json(copied_manifest, index_pin) != value:
            raise ValueError('Document index differs after independent readback')
        for name, old in before.items():
            now = current.get(name)
            if now is None or any(now.get(key) != old.get(key) for key in ('id', 'size', 'digest', 'state')):
                raise ValueError('A pre-existing archive asset changed during document-index staging')
        stage.check_capacity(current, expected)
        final_release = stage.release_for_tag(tag)
        transport.validate_bucket(final_release, tag, writable=True)
        if final_release['id'] != release['id']:
            raise ValueError('Document-index bucket identity changed during staging')
        stage.check_repository(stage.api('repos/' + stage.REPOSITORY))
        latest_after = stage.api('repos/' + stage.REPOSITORY + '/releases/latest')
        if latest_after['id'] == release['id'] or not latest_after['tag_name'].startswith('dataset-'):
            raise ValueError('The protected dataset release pointer is no longer selected')
        report = {'repository': stage.REPOSITORY, 'private_confirmed': True, 'tag': tag,
                  'release_id': release['id'], 'transport_sha256': transport_pin,
                  'document_index_sha256': index_pin, 'filing_selection_sha256': selection_pin,
                  'indexed_documents': value['committed_documents'], 'selected_documents': len(wanted),
                  'assets': len(expected), 'asset_bytes': sum(asset['bytes'] for asset in expected.values()),
                  'new_assets_uploaded': len(set(expected) - set(before)), 'bucket_assets': len(current),
                  'existing_assets_preserved': True, 'independently_downloaded_and_verified': True,
                  'index_manifest_uploaded_after_payload_verification': True,
                  'latest_dataset_release_before': latest_before['id'], 'latest_dataset_release_after': latest_after['id'],
                  'latest_release_unchanged': latest_before['id'] == latest_after['id'],
                  'cloud_daily_maintenance_active': False, 'complete_backfill': False}
        atomic_write(evidence / 'document-index-stage-report.json', canonical(report))
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', type=Path, required=True)
    parser.add_argument('--index-sha256', required=True)
    parser.add_argument('--accessions-file', type=Path, required=True)
    parser.add_argument('--inventory-snapshot', type=Path, required=True)
    parser.add_argument('--bucket-tag', required=True)
    parser.add_argument('--transport-sha256', required=True)
    parser.add_argument('--expected-latest-release-id', type=int, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(stage_index(args.index, args.evidence, args.index_sha256, args.accessions_file.read_text().split(),
                                 args.inventory_snapshot, args.bucket_tag, args.transport_sha256,
                                 args.expected_latest_release_id), indent=2))


if __name__ == '__main__':
    main()
