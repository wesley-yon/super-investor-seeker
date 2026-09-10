"""Optional, content-bound acceleration for derived-data regeneration.

This cache is never publication authority. Source and corpus validation still
run after regeneration. Missing, damaged, or incompatible state selects the
complete builders; neither mtimes nor previous workflow success certify bytes.
"""
from __future__ import annotations

from datetime import date
import hashlib
from importlib import metadata
import json
from pathlib import Path
import sys

from atomic_files import atomic_text_output

CACHE_RELATIVE_PATH = Path('.cache/selective_rebuild.json')
VERSION = 1


def _json_default(value):
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    raise TypeError(f'cannot encode {type(value).__name__}')


def encoded(value) -> bytes:
    return json.dumps(value, separators=(',', ':'), default=_json_default).encode()


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     default=_json_default).encode()).hexdigest()


def file_hash(path: Path) -> str | None:
    if path.is_symlink():
        raise ValueError(f'rebuild dependency must not be a symlink: {path}')
    if not path.exists():
        return None
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def file_hashes(paths) -> dict[str, str | None]:
    return {str(path): file_hash(path) for path in sorted(set(paths))}


def code_fingerprint(root: Path) -> str:
    """Bind actual working bytes, dependencies, Python, and date-sensitive rules."""
    # These are the program modules and reviewed policy/configuration inputs.
    # Frontend, documentation, and tests do not change a derived-data result.
    paths = [path for path in root.iterdir() if path.is_file()
             and path.suffix in {'.py', '.json', '.toml', '.txt', '.yaml', '.yml'}]
    paths.extend((root / 'scripts').rglob('*.py'))
    entries = [(str(path.relative_to(root)), file_hash(path)) for path in sorted(set(paths))]
    dependencies = sorted((distribution.metadata['Name'], distribution.version)
                          for distribution in metadata.distributions())
    return digest([VERSION, list(sys.version_info[:2]), date.today().isoformat(), entries, dependencies])


def fund_hashes(funds_dir: Path) -> dict[str, str]:
    return {path.name: file_hash(path) for path in sorted(funds_dir.glob('*.json'))}


def dirty_funds(before: dict, after: dict) -> set[str]:
    return {name for name in before.keys() | after.keys()
            if before.get(name, {}).get('sha256') != after.get(name, {}).get('sha256')}


def evidence_cusips(inventory: dict, names=None) -> set[str]:
    return {cusip for name in (inventory if names is None else names)
            for cusip in inventory.get(name, {}).get('evidence_order', [])}


def dependency_paths(funds_dir: Path, inventory: dict, cusips: set[str]) -> list[Path]:
    return [funds_dir / name for name, entry in sorted(inventory.items())
            if cusips.intersection(entry['evidence_order'])]


def evidence_order(inventory: dict) -> list[str]:
    return list(dict.fromkeys(cusip for name in sorted(inventory)
                              for cusip in inventory[name]['evidence_order']))


class RebuildCache:
    def __init__(self, root: Path, *, enabled: bool = True):
        self.root = root
        self.path = root / CACHE_RELATIVE_PATH
        self.code = code_fingerprint(root)
        self.data = {}
        self.reason = 'explicit full rebuild' if not enabled else 'missing or incompatible cache'
        if self.path.is_symlink():
            raise ValueError('selective rebuild cache must not be a symlink')
        if enabled:
            try:
                envelope = json.loads(self.path.read_bytes())
                payload = envelope['payload']
                if (envelope['version'] == VERSION and envelope['code'] == self.code
                        and envelope['sha256'] == digest(payload)
                        and isinstance(payload, dict)
                        and isinstance(payload.get('funds'), dict)
                        and isinstance(payload.get('outputs'), dict)):
                    self.data = payload
                    self.reason = ''
            except (OSError, ValueError, KeyError, TypeError):
                pass

    @property
    def usable(self) -> bool:
        return bool(self.data)

    def relative_hashes(self, paths) -> dict[str, str | None]:
        return {str(path.relative_to(self.root)): file_hash(path) for path in sorted(set(paths))}

    def output_matches(self, name: str, paths) -> bool:
        return self.usable and self.data['outputs'].get(name) == self.relative_hashes(paths)

    def save(self, payload: dict) -> None:
        if code_fingerprint(self.root) != self.code:
            raise ValueError('regeneration code or date changed while rebuilding; rerun before publication')
        envelope = {'version': VERSION, 'code': self.code, 'sha256': digest(payload), 'payload': payload}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with atomic_text_output(self.path, private_mode=0o600) as output:
            output.write(encoded(envelope).decode())
