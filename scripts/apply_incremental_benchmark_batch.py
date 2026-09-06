#!/usr/bin/env python3
"""Apply a bounded, checksum-bound filing fixture in a nonpublishing runner test."""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
BATCH_NAME = 'incremental-benchmark-batch.json.gz'
MAX_BYTES = 64 * 1024 * 1024


def apply_batch(root: Path, expected_source_sha: str) -> dict:
    root = root.resolve()
    if not re.fullmatch(r'[0-9a-f]{40}', expected_source_sha):
        raise ValueError('Expected code SHA is invalid')
    with gzip.open(root / '.cache' / BATCH_NAME, 'rb') as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError('Benchmark batch exceeds its bounded size')
    batch = json.loads(raw)
    if batch.get('schema_version') != 1 or batch.get('source_sha') != expected_source_sha:
        raise ValueError('Benchmark batch is not bound to the tested code')
    entries = batch.get('funds')
    if not isinstance(entries, list) or not 1 <= len(entries) <= 50:
        raise ValueError('Benchmark batch requires 1 to 50 fund histories')
    funds = root / 'data/funds'
    if funds.is_symlink() or not funds.is_dir():
        raise ValueError('Benchmark requires an isolated restored fund directory')
    staged = []
    names = set()
    for entry in entries:
        name = entry.get('filename')
        if not isinstance(name, str) or not re.fullmatch(r'[1-9][0-9]*\.json', name) or name in names:
            raise ValueError('Invalid or duplicated benchmark fund filename')
        names.add(name)
        target = funds / name
        if target.is_symlink() or not target.is_file():
            raise ValueError('Benchmark target is not an existing regular fund file')
        if hashlib.sha256(target.read_bytes()).hexdigest() != entry.get('baseline_sha256'):
            raise ValueError('Benchmark target changed after the verified baseline')
        contents = base64.b64decode(entry.get('content_base64', ''), validate=True)
        if hashlib.sha256(contents).hexdigest() != entry.get('incoming_sha256'):
            raise ValueError('Incoming benchmark filing checksum mismatch')
        fund = json.loads(contents)
        if type(fund.get('cik')) is not int or str(fund['cik']) != Path(name).stem:
            raise ValueError('Incoming benchmark fund identity mismatch')
        staged.append((target, contents))
    # Verify the entire batch before any mutation. These are captured real SEC
    # histories, applied only to the private, nonpublishing test checkout.
    for target, contents in staged:
        descriptor, temporary = tempfile.mkstemp(prefix='.benchmark-', dir=funds)
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(contents)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    result = {'funds_applied': len(staged), 'production_publication': False}
    print(json.dumps(result, sort_keys=True))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--expected-source-sha', required=True)
    args = parser.parse_args()
    try:
        apply_batch(args.root, args.expected_source_sha)
    except (ValueError, OSError) as exc:
        print(f'Benchmark batch rejected: {exc}', file=sys.stderr)
        raise SystemExit(1) from None
