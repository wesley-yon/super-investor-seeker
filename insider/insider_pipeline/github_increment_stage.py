"""Append verified checkpoint assets to an isolated private archive bucket.

New buckets start as drafts. Existing published buckets must be prereleases.
This command never publishes, sets latest, deletes, or overwrites an asset.
The pinned transport descriptor is uploaded last, after independent readback.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import re
import shutil
import tempfile
import time

from .github_chain import (BUCKET_MARKER, Downloader, asset_map, blob_name, check_asset, declared_files,
                           descriptor, descriptor_name, metadata, validate_bucket, validate_locator)
from .github_stage import REPOSITORY, api, check_repository, command, release_for_tag
from .http import atomic_write
from .increment import MANIFEST, verify
from .inventory import canonical
from .locking import writer_lock

MAX_BUCKET_ASSETS = 900  # Leave room below GitHub's 1,000-asset hard limit.


def plan(directory, pin, parent_locator):
    value = verify(directory, pin)
    transport = descriptor(value, pin, parent_locator)
    body = canonical(transport)
    transport_pin = hashlib.sha256(body).hexdigest()
    expected = declared_files(value, MANIFEST, pin, (directory / MANIFEST).stat().st_size)
    blobs = {}
    for asset in expected.values():
        name = blob_name(asset['sha256'])
        wanted = {'file': name, 'bytes': asset['bytes'], 'sha256': asset['sha256']}
        if name in blobs and blobs[name] != wanted:
            raise ValueError('A content hash has conflicting byte lengths')
        blobs[name] = wanted
    return value, transport, transport_pin, body, expected, blobs


def check_capacity(remote, expected):
    if len(set(remote) | set(expected)) > MAX_BUCKET_ASSETS:
        raise ValueError('Archive bucket is full; use a fresh numbered bucket without deleting existing assets')
    for name, want in expected.items():
        if name in remote:
            check_asset(remote, name, want['bytes'], want['sha256'])


def stage_increment(directory, evidence, pin, parent_locator, bucket_tag, expected_latest_id):
    directory, evidence = Path(directory).resolve(), Path(evidence).absolute()
    validate_locator(parent_locator)
    if not re.fullmatch(r'insider-archives-\d{6}-\d{3}', bucket_tag):
        raise ValueError('An explicit monthly insider archive bucket tag is required')
    value, transport, transport_pin, body, logical_files, blobs = plan(directory, pin, parent_locator)
    transport_name = descriptor_name(transport_pin)
    all_assets = {**blobs, transport_name: {'file': transport_name, 'bytes': len(body), 'sha256': transport_pin}}
    check_capacity({}, all_assets)
    with writer_lock(evidence):
        check_repository(api('repos/' + REPOSITORY))
        latest_before = api('repos/' + REPOSITORY + '/releases/latest')
        if latest_before['id'] != expected_latest_id or not latest_before['tag_name'].startswith('dataset-'):
            raise ValueError('The dataset pointer changed; refresh its preflight before staging')
        # Validate the remote parent's complete metadata and asset declarations.
        # Full restoration is a separate, subsequently verified cloud operation.
        parent_work = Path(tempfile.mkdtemp(prefix='parent-check-', dir=evidence))
        parents = metadata(parent_locator, parent_work / 'archives', Downloader(parent_work / 'cache'))
        if parents[-1]['reference'] != value['parent']:
            raise ValueError('The remote parent differs from the locally pinned increment parent')
        release = release_for_tag(bucket_tag)
        if release is None:
            attempted = evidence / 'bucket-create-attempt.json'
            if attempted.exists():
                raise RuntimeError('A prior bucket creation is unconfirmed; inspect the same tag before retrying creation')
            repository = api('repos/' + REPOSITORY)
            check_repository(repository)
            target = api('repos/' + REPOSITORY + '/commits/' + repository['default_branch'])['sha']
            if not re.fullmatch('[0-9a-f]{40}', target):
                raise ValueError('Unexpected private archive commit identity')
            note = (BUCKET_MARKER + '\n\nImmutable assets use their complete SHA-256 names. '
                    'Checkpoint descriptors pin their manifest and exact parent locator. '
                    'Retain referenced parent checkpoints. This release must remain a prerelease and must never be latest.\n\n'
                    'The 2018-present backfill is incomplete. This storage bucket does not activate daily maintenance.\n')
            atomic_write(evidence / 'bucket-notes.md', note.encode())
            atomic_write(attempted, canonical({'tag': bucket_tag, 'transport_sha256': transport_pin}))
            result = command(['release', 'create', bucket_tag, '--repo', REPOSITORY, '--draft', '--prerelease',
                              '--latest=false', '--target', target, '--title', 'Insider checkpoint archives ' + bucket_tag.rsplit('-', 2)[1],
                              '--notes-file', str(evidence / 'bucket-notes.md')])
            atomic_write(evidence / 'bucket-create-response.json', canonical({'url': result.strip(), 'tag': bucket_tag}))
            for delay in (1, 2, 4, 8, 10):
                release = release_for_tag(bucket_tag)
                if release is not None:
                    break
                time.sleep(delay)
            if release is None:
                raise RuntimeError('The created bucket is not visible yet; inspect the same tag before another creation')
        validate_bucket(release, bucket_tag, writable=True)
        endpoint = 'repos/' + REPOSITORY + '/releases/' + str(release['id']) + '/assets?per_page=100'
        before = asset_map(api(endpoint, pages=True), bucket=True)
        check_capacity(before, all_assets)
        upload_directory = Path(tempfile.mkdtemp(prefix='upload-', dir=evidence))
        for asset in logical_files.values():
            destination = upload_directory / blob_name(asset['sha256'])
            if not destination.exists():
                shutil.copyfile(directory / asset['file'], destination)
        atomic_write(upload_directory / transport_name, body)
        def upload(name):
            command(['release', 'upload', bucket_tag, str(upload_directory / name), '--repo', REPOSITORY])
            print(json.dumps({'uploaded_asset': name}), flush=True)
        missing = sorted(set(blobs) - set(before))
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(upload, missing))
        current_release = release_for_tag(bucket_tag)
        validate_bucket(current_release, bucket_tag, writable=True)
        if current_release['id'] != release['id']:
            raise ValueError('The archive bucket changed identity during upload')
        current = asset_map(api(endpoint, pages=True), bucket=True)
        check_capacity(current, all_assets)
        for name, asset in blobs.items():
            check_asset(current, name, asset['bytes'], asset['sha256'])
        downloaded = Path(tempfile.mkdtemp(prefix='readback-', dir=evidence))
        downloader = Downloader(downloaded / 'cache', budget=sum(asset['bytes'] for asset in all_assets.values()))
        for name, asset in blobs.items():
            downloader.plan(bucket_tag, name, asset['bytes'], asset['sha256'])
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda item: downloader.get(bucket_tag, item[0], item[1]['bytes'], item[1]['sha256']), blobs.items()))
        restored_files = downloaded / 'increment'; restored_files.mkdir()
        for asset in logical_files.values():
            shutil.copyfile(downloader.downloaded[(bucket_tag, blob_name(asset['sha256']))], restored_files / asset['file'])
        if verify(restored_files, pin) != value:
            raise ValueError('Independent readback differs from the locally verified increment')
        # Only a verified complete blob set can be advertised by this descriptor.
        if transport_name not in current:
            upload(transport_name)
        current = asset_map(api(endpoint, pages=True), bucket=True)
        for name, asset in all_assets.items():
            check_asset(current, name, asset['bytes'], asset['sha256'])
        transport_path = downloader.get(bucket_tag, transport_name, len(body), transport_pin)
        if json.loads(transport_path.read_text()) != transport:
            raise ValueError('The independently downloaded transport descriptor differs')
        # Every pre-existing asset must survive unchanged, including other checkpoints.
        for name, old in before.items():
            now = current.get(name)
            if now is None or any(now.get(key) != old.get(key) for key in ('id', 'size', 'digest', 'state')):
                raise ValueError('A pre-existing bucket asset changed during staging')
        check_repository(api('repos/' + REPOSITORY))
        latest_after = api('repos/' + REPOSITORY + '/releases/latest')
        if latest_after['id'] == release['id'] or not latest_after['tag_name'].startswith('dataset-'):
            raise ValueError('The protected dataset release pointer is no longer selected')
        final_release = release_for_tag(bucket_tag)
        validate_bucket(final_release, bucket_tag, writable=True)
        report = {'repository': REPOSITORY, 'private_confirmed': True, 'release_id': release['id'],
                  'tag': bucket_tag, 'release_url': release['html_url'], 'draft': final_release['draft'],
                  'locator': {'layout': 'content_addressed', 'tag': bucket_tag, 'sha256': transport_pin},
                  'transport_sha256': transport_pin, 'increment_manifest_sha256': pin, 'parent': parent_locator,
                  'changed_documents': value['changed_committed_documents'],
                  'checkpoint_documents': value['target_inventory']['committed_documents'],
                  'assets': len(all_assets), 'asset_bytes': sum(asset['bytes'] for asset in all_assets.values()),
                  'new_assets_uploaded': len(missing) + int(transport_name not in before),
                  'bucket_assets': len(current), 'existing_assets_preserved': True,
                  'independently_downloaded_and_verified': True, 'descriptor_uploaded_after_blob_verification': True,
                  'latest_dataset_release_before': latest_before['id'], 'latest_dataset_release_after': latest_after['id'],
                  'latest_release_unchanged': latest_before['id'] == latest_after['id'],
                  'cloud_daily_maintenance_active': False, 'complete_backfill': False}
        atomic_write(evidence / 'increment-stage-report.json', canonical(report))
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--increment', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--increment-sha256', required=True)
    parser.add_argument('--parent-locator', type=Path, required=True)
    parser.add_argument('--bucket-tag', required=True)
    parser.add_argument('--expected-latest-release-id', type=int, required=True)
    args = parser.parse_args()
    parent = json.loads(args.parent_locator.read_text())
    print(json.dumps(stage_increment(args.increment, args.evidence, args.increment_sha256, parent,
                                    args.bucket_tag, args.expected_latest_release_id), indent=2), flush=True)


if __name__ == '__main__':
    main()
