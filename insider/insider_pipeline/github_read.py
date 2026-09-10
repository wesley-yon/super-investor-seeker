"""Read a pinned, published insider checkpoint using read-only GitHub access."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import re
import shutil
import time

from .audit_batches import file_hash, run as audit
from .baseline import restore, verify
from .github_stage import REPOSITORY, api, command
from .http import atomic_write
from .inventory import canonical

MAX_DOWNLOAD_BYTES = 2_000_000_000
MIN_FREE_BYTES = 10 * 1024 ** 3


def validate_release(repository, release, tag):
    if repository['full_name'] != REPOSITORY or repository['private'] is not True:
        raise ValueError('The configured insider archive repository must be private')
    if (release.get('tag_name') != tag or release.get('draft') is not False
            or not release.get('published_at')):
        raise ValueError('Cloud readers require the exact published insider checkpoint')


def expected_assets(baseline, baseline_path):
    files = baseline['files']
    if baseline.get('baseline_schema') != 1 or not 1 <= len(files) <= 999:
        raise ValueError('Unsupported baseline schema or asset count')
    expected = {}
    for asset in files:
        name = asset['file']
        if (not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', name) or name in expected or name == 'baseline.json'
                or not re.fullmatch(r'[0-9a-f]{64}', asset['sha256'])
                or type(asset['bytes']) is not int or not 0 < asset['bytes'] <= 1_000_000_000):
            raise ValueError('Invalid baseline asset declaration')
        expected[name] = asset
    expected['baseline.json'] = {'file': 'baseline.json', 'bytes': baseline_path.stat().st_size,
                                 'sha256': file_hash(baseline_path)}
    if sum(a['bytes'] for a in expected.values()) > MAX_DOWNLOAD_BYTES:
        raise ValueError('This checkpoint exceeds the bounded cloud verification download budget')
    return expected


def check_remote_assets(assets, expected):
    if len({a['name'] for a in assets}) != len(assets) or {a['name'] for a in assets} != set(expected):
        raise ValueError('Release asset membership differs from the pinned baseline')
    for asset in assets:
        want = expected[asset['name']]
        if (asset['state'] != 'uploaded' or asset['size'] != want['bytes']
                or asset.get('digest') not in (None, 'sha256:' + want['sha256'])):
            raise ValueError('Release asset is incomplete or differs from its pinned identity')


def read_checkpoint(tag, digest, output, workers=4):
    if not re.fullmatch(r'insider-[A-Za-z0-9._-]+', tag) or not re.fullmatch('[0-9a-f]{64}', digest):
        raise ValueError('An explicit insider tag and pinned baseline SHA-256 are required')
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('Cloud verification output must be a fresh directory')
    output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output.parent).free < MIN_FREE_BYTES:
        raise ValueError('Insufficient free disk for the bounded checkpoint restore test')
    repository = api('repos/' + REPOSITORY)
    release = api('repos/' + REPOSITORY + '/releases/tags/' + tag)
    validate_release(repository, release, tag)
    assets = api('repos/' + REPOSITORY + '/releases/' + str(release['id']) + '/assets?per_page=100', pages=True)
    candidates = [a for a in assets if a['name'] == 'baseline.json']
    if len(candidates) != 1 or not 0 < candidates[0]['size'] <= 2_000_000:
        raise ValueError('Expected exactly one bounded baseline manifest')
    latest_before = api('repos/' + REPOSITORY + '/releases/latest')
    downloaded = output / 'download'; downloaded.mkdir(parents=True)
    started = time.monotonic()
    command(['release', 'download', tag, '--repo', REPOSITORY, '--pattern', 'baseline.json', '--dir', str(downloaded)])
    baseline_path = downloaded / 'baseline.json'
    if file_hash(baseline_path) != digest:
        raise ValueError('Downloaded baseline differs from its independently pinned checksum')
    expected = expected_assets(json.loads(baseline_path.read_text()), baseline_path)
    check_remote_assets(assets, expected)
    command(['release', 'download', tag, '--repo', REPOSITORY, '--dir', str(downloaded), '--skip-existing'])
    if {p.name for p in downloaded.iterdir()} != set(expected):
        raise ValueError('Downloaded asset membership differs')
    verified = verify(downloaded, digest)
    restored = restore(downloaded, output / 'restored', digest)
    source_audit = audit(output / 'restored', output / 'audit', workers=workers)
    packaged_audit = json.loads((downloaded / 'source-audit.json').read_text())
    if (source_audit['semantic_sha256'] != packaged_audit['semantic_sha256']
            or source_audit['selected_documents'] != restored['documents']):
        raise ValueError('Cloud-restored source audit differs from the archived source audit')
    latest_after = api('repos/' + REPOSITORY + '/releases/latest')
    if not latest_after['tag_name'].startswith('dataset-'):
        raise ValueError('The normal dataset release pointer is not selected')
    report = {**verified, 'repository': REPOSITORY, 'tag': tag, 'release_id': release['id'],
              'read_only_operations': True, 'full_restore_verified': True,
              'source_audit_semantic_sha256': source_audit['semantic_sha256'],
              'source_checks': source_audit['counts']['original_xml_fields_checked'],
              'source_audit_matches_archive': True, 'elapsed_seconds': round(time.monotonic() - started, 3),
              'latest_dataset_release_before': latest_before['id'], 'latest_dataset_release_after': latest_after['id'],
              'latest_release_unchanged': latest_before['id'] == latest_after['id'],
              'remaining_free_bytes': shutil.disk_usage(output).free,
              'cloud_daily_maintenance_active': False, 'complete_backfill': False}
    atomic_write(output / 'cloud-verification.json', canonical(report))
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tag', required=True)
    parser.add_argument('--baseline-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=4, choices=range(1, 5))
    args = parser.parse_args()
    print(json.dumps(read_checkpoint(args.tag, args.baseline_sha256, args.output, args.workers), indent=2))


if __name__ == '__main__':
    main()
