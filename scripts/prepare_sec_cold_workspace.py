#!/usr/bin/env python3
"""Reduce an isolated cold-rebuild workspace without changing reported data."""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sec_security_master import security_master_pair_lock  # noqa: E402

JSON_TOKENS = re.compile(rb'("(?:[^"\\]|\\.)*")|[ \t\r\n]+')
STREAM_TOKENS = re.compile(rb'\\.|\\$|"|[ \t\r\n]+', re.DOTALL)


def compact_stream(source, output, *, chunk_size: int = 1024 * 1024) -> tuple[str, str, int, int]:
    """Strip JSON whitespace with bounded memory, including split escapes."""
    original_sha, compact_sha = hashlib.sha256(), hashlib.sha256()
    before = after = 0
    in_string = escaped_boundary = False
    while chunk := source.read(chunk_size):
        original_sha.update(chunk)
        before += len(chunk)
        cursor = 0
        scan_from = 1 if escaped_boundary else 0
        escaped_boundary = False
        pieces = []
        for match in STREAM_TOKENS.finditer(chunk, scan_from):
            token = match.group()
            if token == b'"':
                in_string = not in_string
            elif token == b'\\':
                escaped_boundary = True
            elif token[:1] in b' \t\r\n' and not in_string:
                pieces.append(chunk[cursor:match.start()])
                cursor = match.end()
        pieces.append(chunk[cursor:])
        compact = b''.join(pieces)
        output.write(compact)
        compact_sha.update(compact)
        after += len(compact)
    if in_string or escaped_boundary:
        raise ValueError('Unterminated JSON string during streaming compaction')
    output.write(b'\n')
    compact_sha.update(b'\n')
    return original_sha.hexdigest(), compact_sha.hexdigest(), before, after + 1


def compact_validated_pair_file(path: Path) -> tuple[int, int]:
    """Compact an already validated, locked pair member without loading it again."""
    regular(path)
    descriptor, name = tempfile.mkstemp(prefix=f'.{path.name}.', suffix='.compact', dir=path.parent)
    temporary = Path(name)
    try:
        with path.open('rb') as source, os.fdopen(descriptor, 'wb') as output:
            original_sha, compact_sha, before, after = compact_stream(source, output)
            output.flush()
            os.fsync(output.fileno())
        with path.open('rb') as source:
            if hashlib.file_digest(source, 'sha256').hexdigest() != original_sha:
                raise ValueError(f'JSON source changed during compaction: {path.name}')
        with temporary.open('rb') as source:
            if hashlib.file_digest(source, 'sha256').hexdigest() != compact_sha:
                raise ValueError(f'Written JSON differs from its verified tokens: {path.name}')
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
        return before, after
    finally:
        temporary.unlink(missing_ok=True)


def compact_bytes(raw: bytes) -> bytes:
    # Retain every string and number token verbatim, including decimal spelling.
    return JSON_TOKENS.sub(lambda match: match.group(1) or b'', raw) + b'\n'


def regular(path: Path, *, directory: bool = False) -> None:
    mode = path.lstat().st_mode
    if not (stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)):
        raise ValueError(f'Expected a regular {"directory" if directory else "file"}: {path}')


def inspect_file(path: Path) -> tuple[Path, str, str, int, int]:
    regular(path)
    raw = path.read_bytes()
    compact = compact_bytes(raw)
    if json.loads(raw) != json.loads(compact):
        raise ValueError(f'JSON compaction changes values: {path.name}')
    return path, hashlib.sha256(raw).hexdigest(), hashlib.sha256(compact).hexdigest(), len(raw), len(compact)


def replace_compact(path: Path, raw: bytes, compact: bytes) -> None:
    if raw == compact:
        return
    regular(path)
    descriptor, name = tempfile.mkstemp(prefix=f'.{path.name}.', suffix='.compact', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, 'wb') as output:
            output.write(compact)
            output.flush()
            os.fsync(output.fileno())
        if path.read_bytes() != raw:
            raise ValueError(f'JSON source changed during compaction: {path.name}')
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        temporary.unlink(missing_ok=True)


def apply_file(plan: tuple[Path, str, str, int, int]) -> tuple[str, int, int]:
    path, original_sha, compact_sha, before, after = plan
    regular(path)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != original_sha:
        raise ValueError(f'JSON source changed after preflight: {path.name}')
    compact = compact_bytes(raw)
    if hashlib.sha256(compact).hexdigest() != compact_sha:
        raise ValueError(f'JSON token preservation failed: {path.name}')
    replace_compact(path, raw, compact)
    if hashlib.sha256(path.read_bytes()).hexdigest() != compact_sha:
        raise ValueError(f'Written JSON differs from its verified tokens: {path.name}')
    return compact_sha, before, after


def prepare(root: Path, *, discard_derived_stocks: bool) -> dict:
    root = root.absolute()
    regular(root, directory=True)
    regular(root / 'data', directory=True)
    funds = root / 'data/funds'
    stocks = root / 'data/stocks'
    regular(funds, directory=True)
    regular(stocks, directory=True)
    paths = sorted(funds.iterdir())
    if not paths or any(path.suffix != '.json' for path in paths):
        raise ValueError('Cold workspace requires a nonempty flat fund JSON corpus')
    stock_paths = sorted(stocks.iterdir())
    for path in stock_paths:
        regular(path)
        if path.suffix != '.json':
            raise ValueError('Derived stock directory contains an unexpected file')
    master = root / '.cache/sec_security_master.json'
    state = root / '.cache/sec_source_state.json'
    has_pair = any(path.exists() or path.is_symlink() for path in (master, state))
    if has_pair and not (master.is_file() and state.is_file()):
        raise ValueError('Cold workspace has an incomplete SEC master/source pair')
    workers = min(4, os.cpu_count() or 1) if len(paths) >= 32 else 1
    with ProcessPoolExecutor(max_workers=workers) if workers > 1 else nullcontext() as pool:
        mapper = pool.map if pool else map
        # Validate every fund before changing any file or discarding stocks.
        plans = list(mapper(inspect_file, paths))
    pair_before = pair_after = 0
    if has_pair:
        # Validate the pair and retain its lock during both whitespace-only
        # replacements. Each intermediate pair has identical JSON tokens,
        # so its semantic source-state binding also remains valid on a crash.
        with security_master_pair_lock(master_path=master, source_state_path=state):
            for path in (master, state):
                before, after = compact_validated_pair_file(path)
                pair_before += before
                pair_after += after
    with ProcessPoolExecutor(max_workers=workers) if workers > 1 else nullcontext() as pool:
        mapper = pool.map if pool else map
        results = list(mapper(apply_file, plans))
    token_digest = hashlib.sha256()
    for path, result in zip(paths, results):
        token_digest.update(path.name.encode() + b'\0' + result[0].encode() + b'\n')
    discarded_bytes = 0
    if discard_derived_stocks:
        discarded_bytes = sum(path.stat().st_size for path in stock_paths)
        for path in stock_paths:
            path.unlink()
    return {
        'fund_files': len(paths), 'fund_json_tokens_preserved': True,
        'fund_token_digest': token_digest.hexdigest(),
        'fund_bytes_before': sum(row[1] for row in results),
        'fund_bytes_after': sum(row[2] for row in results),
        'master_pair_present': has_pair,
        'master_pair_bytes_before': pair_before, 'master_pair_bytes_after': pair_after,
        'discarded_derived_stock_files': len(stock_paths) if discard_derived_stocks else 0,
        'discarded_derived_stock_bytes': discarded_bytes,
        'free_bytes_after': shutil.disk_usage(root).free, 'workers': workers,
        'regeneration_required': discard_derived_stocks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--discard-derived-stocks', action='store_true',
                        help='Remove reproducible stock JSON files before the required cold regeneration')
    args = parser.parse_args()
    print(json.dumps(prepare(args.root, discard_derived_stocks=args.discard_derived_stocks), sort_keys=True))


if __name__ == '__main__':
    main()
