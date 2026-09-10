"""Stage and independently download a checkpoint in an isolated private draft.

This tool never publishes a release, marks one latest, deletes an asset, or uses
an existing dataset-* release. Authentication stays inside the installed gh CLI.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time

from .baseline import verify
from .http import atomic_write
from .inventory import canonical
from .locking import writer_lock

REPOSITORY = 'wesley-yon/super-investor-seeker-data'


def command(args, missing_ok=False):
    result = subprocess.run(['gh', *args], capture_output=True, text=True)
    if result.returncode:
        if missing_ok and 'HTTP 404' in result.stderr:
            return None
        status = re.search(r'HTTP (\d{3})', result.stderr)
        raise RuntimeError('GitHub command failed' + (' with HTTP ' + status.group(1) if status else '') +
                           '; inspect authentication/network status before retrying')
    return result.stdout


def api(path, missing_ok=False, pages=False):
    args = ['api', path]
    if pages:
        args += ['--paginate', '--slurp']
    response = command(args, missing_ok)
    if response is None:
        return None
    data = json.loads(response)
    return [row for page in data for row in page] if pages else data


def check_repository(repository):
    if (repository['full_name'] != REPOSITORY or repository['private'] is not True
            or not repository.get('permissions', {}).get('push')):
        raise ValueError('The configured archive must remain private and writable')


def release_for_tag(tag):
    # Drafts need not yet have a Git tag. Paginated release listing includes
    # drafts visible to this authenticated publisher.
    matches = [r for r in api('repos/' + REPOSITORY + '/releases?per_page=100', pages=True) if r['tag_name'] == tag]
    if len(matches) > 1:
        raise ValueError('More than one release claims the staging tag')
    return matches[0] if matches else None


def check_release(release, tag, digest):
    if (not release or release['tag_name'] != tag or release['draft'] is not True
            or ('Baseline SHA-256: ' + digest) not in (release.get('body') or '')):
        raise ValueError('Existing release is not this matching private draft checkpoint')


def check_assets(assets, expected, complete=False):
    names = [a['name'] for a in assets]
    if len(set(names)) != len(names) or set(names) - set(expected):
        raise ValueError('The draft contains duplicate or unrelated assets')
    if complete and set(names) != set(expected):
        raise ValueError('The draft upload is incomplete')
    for asset in assets:
        want = expected[asset['name']]
        if (asset.get('state') != 'uploaded' or asset['size'] != want['bytes']
                or asset.get('digest') not in (None, 'sha256:' + want['sha256'])):
            raise ValueError('Existing draft asset differs; no asset will be overwritten')


def stage(directory, evidence, expected_baseline_sha256, expected_latest_id):
    directory, evidence = Path(directory).resolve(), Path(evidence).resolve()
    checked = verify(directory, expected_baseline_sha256)
    baseline = json.loads((directory / 'baseline.json').read_text())
    tag = 'insider-preflight-' + baseline['scope']['window_end'].replace('-', '') + '-' + expected_baseline_sha256[:12]
    if not re.fullmatch(r'insider-preflight-\d{8}-[0-9a-f]{12}', tag):
        raise ValueError('Unexpected insider staging tag')
    expected = {a['file']: a for a in baseline['files']}
    expected['baseline.json'] = {'file': 'baseline.json', 'sha256': expected_baseline_sha256,
                                 'bytes': (directory / 'baseline.json').stat().st_size}
    with writer_lock(evidence):
        repository = api('repos/' + REPOSITORY)
        check_repository(repository)
        latest_before = api('repos/' + REPOSITORY + '/releases/latest')
        if latest_before['id'] != expected_latest_id or not latest_before['tag_name'].startswith('dataset-'):
            raise ValueError('The existing dataset release pointer changed; refresh its preflight before staging')
        notes = ('Private staging checkpoint for the SEC ownership backfill.\n\n'
                 f"Filing-date scope: {baseline['scope']['window_start']} through {baseline['scope']['window_end']}.\n"
                 f"Collected originals: {baseline['documents']:,}. Full inventory: {baseline['inventory_filings']:,} filings.\n\n"
                 'This is a partial backfill checkpoint. Pending filings and source-review work remain. '
                 'Cloud daily maintenance is not active. Read this checkpoint by its explicit tag and pinned manifest.\n\n'
                 'Baseline SHA-256: ' + expected_baseline_sha256 + '\n')
        atomic_write(evidence / 'release-notes.md', notes.encode())
        release = release_for_tag(tag)
        if release is None:
            attempt_path = evidence / 'draft-create-attempt.json'
            if attempt_path.exists():
                raise RuntimeError('A prior draft creation is still unconfirmed; inspect the same tag before attempting another creation')
            commit = api('repos/' + REPOSITORY + '/commits/' + repository['default_branch'])['sha']
            if not re.fullmatch('[0-9a-f]{40}', commit):
                raise ValueError('Unexpected archive repository commit identity')
            atomic_write(attempt_path, canonical({'tag': tag, 'baseline_sha256': expected_baseline_sha256}))
            created_url = command(['release', 'create', tag, '--repo', REPOSITORY, '--draft', '--latest=false', '--target', commit,
                     '--title', 'Insider backfill checkpoint preflight', '--notes-file', str(evidence / 'release-notes.md')])
            atomic_write(evidence / 'draft-create-response.json', canonical({'url': created_url.strip(), 'tag': tag}))
            for delay in (1, 2, 4, 8, 10):
                release = release_for_tag(tag)
                if release is not None:
                    break
                time.sleep(delay)
        check_release(release, tag, expected_baseline_sha256)
        asset_endpoint = 'repos/' + REPOSITORY + '/releases/' + str(release['id']) + '/assets?per_page=100'
        assets = api(asset_endpoint, pages=True)
        check_assets(assets, expected)
        present = {a['name'] for a in assets}
        missing = [name for name in sorted(expected) if name not in present]
        def upload(name):
            command(['release', 'upload', tag, str(directory / name), '--repo', REPOSITORY])
            print(json.dumps({'uploaded_asset': name}), flush=True)
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(upload, missing))
        release = release_for_tag(tag)
        check_release(release, tag, expected_baseline_sha256)
        check_assets(api(asset_endpoint, pages=True), expected, complete=True)
        downloaded = Path(tempfile.mkdtemp(prefix='download-', dir=evidence))
        command(['release', 'download', tag, '--repo', REPOSITORY, '--dir', str(downloaded)])
        if {p.name for p in downloaded.iterdir()} != set(expected):
            raise ValueError('Independent download has missing or unexpected assets')
        downloaded_check = verify(downloaded, expected_baseline_sha256)
        if downloaded_check != checked:
            raise ValueError('Downloaded checkpoint differs from the local verified archive')
        check_repository(api('repos/' + REPOSITORY))
        latest_after = api('repos/' + REPOSITORY + '/releases/latest')
        if latest_after['id'] == release['id'] or not latest_after['tag_name'].startswith('dataset-'):
            raise ValueError('The protected dataset release pointer is no longer selected')
        report = {**checked, 'repository': REPOSITORY, 'private_confirmed': True,
                  'release_id': release['id'], 'tag': tag, 'release_url': release['html_url'],
                  'draft': True, 'independently_downloaded_and_verified': True,
                  'download_directory': str(downloaded),
                  'latest_dataset_release_before': {'id': latest_before['id'], 'tag': latest_before['tag_name']},
                  'latest_dataset_release_after': {'id': latest_after['id'], 'tag': latest_after['tag_name']},
                  'latest_release_unchanged': latest_after['id'] == latest_before['id'],
                  'cloud_daily_maintenance_active': False, 'complete_backfill': False}
        atomic_write(evidence / 'stage-report.json', canonical(report))
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--expected-baseline-sha256', required=True)
    parser.add_argument('--expected-latest-release-id', type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(stage(args.baseline, args.evidence, args.expected_baseline_sha256,
                           args.expected_latest_release_id), indent=2), flush=True)


if __name__ == '__main__':
    main()
