#!/usr/bin/env python3
"""Restore only the immutable, reviewed private mapping selected by code."""
import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from atomic_files import atomic_text_output  # noqa: E402
from reviewed_ticker_map import (  # noqa: E402
    CACHE_RELATIVE_PATH, REVIEW_COMMIT, REVIEW_BLOB, validate_review_bytes,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', required=True)
    args = parser.parse_args()
    env = dict(os.environ)
    if env.get('DATA_ARCHIVE_TOKEN'):
        env['GH_TOKEN'] = env['DATA_ARCHIVE_TOKEN']
    repo = subprocess.run(['gh', 'api', f'repos/{args.repository}', '--jq',
                           '.private'], check=True, capture_output=True, env=env)
    if repo.stdout.strip() != b'true':
        raise RuntimeError('reviewed data repository must be private')
    result = subprocess.run([
        'gh', 'api', f'repos/{args.repository}/git/blobs/{REVIEW_BLOB}',
    ], check=True, capture_output=True, env=env)
    blob = json.loads(result.stdout)
    if blob.get("encoding") != "base64" or blob.get("sha") != REVIEW_BLOB:
        raise RuntimeError("GitHub did not return the pinned reviewed blob")
    raw = base64.b64decode("".join(blob["content"].split()), validate=True)
    document = validate_review_bytes(raw)
    path = ROOT / CACHE_RELATIVE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with atomic_text_output(path, private_mode=0o600) as handle:
        handle.write(raw.decode())
    print(f"Prepared {len(document['mappings'])} reviewed identities and "
          f"{len(document.get('display_mappings', {}))} typed display labels from {REVIEW_COMMIT}")


if __name__ == '__main__':
    main()
