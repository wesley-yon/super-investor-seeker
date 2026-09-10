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
from .inventory_archive import restore as restore_inventory

MAX_DOWNLOAD_BYTES = 2_000_000_000
MIN_FREE_BYTES = 10 * 1024 ** 3


def validate_release(repository, release, tag):
    if repository['full_name'] != REPOSITORY or repository['private'] is not True:
        raise ValueError('The configured insider archive repository must be private')
    if (release.get('tag_name') != tag or release.get('draft') is not False
            or not release.get('published_at')):
        raise ValueError('Cloud readers require the exact published insider checkpoint')


def expected_assets(baseline, baseline_path, download_budget=MAX_DOWNLOAD_BYTES):
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
    if download_budget is not None and sum(a['bytes'] for a in expected.values()) > download_budget:
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


def inventory_subset(tag, digest, downloaded, output, baseline, expected):
    name = 'inventory-manifest.json'
    if name not in expected or not 0 < expected[name]['bytes'] <= 2_000_000:
        raise ValueError('Expected a bounded inventory manifest in the pinned baseline')
    command(['release', 'download', tag, '--repo', REPOSITORY, '--pattern', name, '--dir', str(downloaded)])
    path = downloaded / name
    if (path.stat().st_size != expected[name]['bytes'] or file_hash(path) != expected[name]['sha256']
            or expected[name]['sha256'] != baseline['inventory_manifest_sha256']):
        raise ValueError('Inventory manifest differs from the pinned baseline')
    inventory = json.loads(path.read_text())
    if (inventory['scope'] != baseline['scope'] or inventory['tables']['filings'] != baseline['inventory_filings']
            or inventory['committed_documents'] != baseline['documents']
            or inventory['committed_selection_sha256'] != baseline['selection_sha256']
            or inventory['queue_counts'] != baseline['queue_counts_at_checkpoint']):
        raise ValueError('Inventory coverage differs from the pinned baseline')
    parts = inventory['parts']
    if not 1 <= len(parts) <= 997 or len({part['file'] for part in parts}) != len(parts):
        raise ValueError('Invalid inventory asset membership')
    selected = {'baseline.json': expected['baseline.json'], name: expected[name]}
    for part in parts:
        if part['file'] in selected or part != expected.get(part['file']):
            raise ValueError('Inventory parts differ from the pinned baseline')
        selected[part['file']] = part
    total_bytes = sum(asset['bytes'] for asset in selected.values())
    if total_bytes > MAX_DOWNLOAD_BYTES:
        raise ValueError('Selected inventory exceeds the cloud download budget')
    if (type(inventory['raw_bytes']) is not int or inventory['raw_bytes'] <= 0
            or inventory['raw_bytes'] + 2 * total_bytes + 2 * 1024**3 > shutil.disk_usage(output).free):
        raise ValueError('Insufficient free disk for the declared inventory restore')
    args = ['release', 'download', tag, '--repo', REPOSITORY, '--dir', str(downloaded)]
    for part in parts:
        args.extend(('--pattern', part['file']))
    command(args)
    if {path.name for path in downloaded.iterdir()} != set(selected):
        raise ValueError('Downloaded inventory asset membership differs')
    restored = restore_inventory(downloaded, output / 'restored', baseline['inventory_manifest_sha256'])
    return {'baseline_sha256': digest, 'inventory_manifest_sha256': baseline['inventory_manifest_sha256'],
            'documents': restored['committed_documents'], 'inventory_filings': restored['tables']['filings'],
            'assets': len(selected), 'asset_bytes': total_bytes,
            'inventory_only_restore_verified': True, 'inventory_byte_identical': restored['byte_identical'],
            'inventory_raw_sha256': restored['raw_sha256'],
            'full_restore_verified': False, 'includes_original_documents': False,
            'collection_resume_ready': False, 'source_audit_performed': False}


def read_checkpoint(tag, digest, output, workers=4, inventory_only=False):
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
    baseline = json.loads(baseline_path.read_text())
    expected = expected_assets(baseline, baseline_path, download_budget=None if inventory_only else MAX_DOWNLOAD_BYTES)
    check_remote_assets(assets, expected)
    if inventory_only:
        verified = inventory_subset(tag, digest, downloaded, output, baseline, expected)
    else:
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
        verified.update({'full_restore_verified': True, 'source_audit_performed': True,
                         'source_audit_semantic_sha256': source_audit['semantic_sha256'],
                         'source_checks': source_audit['counts']['original_xml_fields_checked'],
                         'source_audit_matches_archive': True})
    latest_after = api('repos/' + REPOSITORY + '/releases/latest')
    if not latest_after['tag_name'].startswith('dataset-'):
        raise ValueError('The normal dataset release pointer is not selected')
    report = {**verified, 'repository': REPOSITORY, 'tag': tag, 'release_id': release['id'],
              'read_only_operations': True, 'elapsed_seconds': round(time.monotonic() - started, 3),
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
    parser.add_argument('--inventory-only', action='store_true', help='Restore only queue/discovery state; exclude documents and source audit')
    args = parser.parse_args()
    print(json.dumps(read_checkpoint(args.tag, args.baseline_sha256, args.output, args.workers, args.inventory_only), indent=2))


if __name__ == '__main__':
    main()
