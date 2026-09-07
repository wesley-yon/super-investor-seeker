#!/usr/bin/env python3
"""Create, verify, and restore private data snapshots.

The snapshot format is deliberately small and strict. It contains only the
known derived publication files under ``data/`` and the durable cache files
named in ``CACHE_FILES``. Raw SEC downloads and temporary working directories
are outside this allowlist. The sidecar manifest is the integrity boundary used
by both local restores and GitHub Actions.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import http.client
import json
import os
import re
import shutil
import stat
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Iterable, Optional


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sec_security_master import (  # noqa: E402
    SecurityMasterError,
    load_security_master_pair,
    save_security_master_pair,
    security_master_pair_lock,
)

CONTRACT_VERSION = 2
LEGACY_CONTRACT_VERSION = 1
DEFAULT_MAX_ARCHIVE_BYTES = 1_932_735_283
MAX_MANIFEST_BYTES = 1_000_000
MAX_API_RESPONSE_BYTES = 10_000_000
GITHUB_RETRY_DELAYS_SECONDS = (1, 3)
# The Python restore helpers perform GET requests only. A newly minted GitHub App
# token can briefly return 403 while repository permissions propagate, so those
# read-only paths may retry it without replaying a mutation. The CLI wrapper
# exposes the same behavior only through an explicit read-only opt-in; mutation
# callers never use that mode.
GITHUB_RETRY_STATUS_CODES = frozenset({403, 429, 500, 502, 503, 504})
GITHUB_RETRY_EXCEPTIONS = (
    urllib.error.URLError,
    ConnectionError,
    TimeoutError,
    http.client.HTTPException,
)
TOKEN_ENV = "DATA_ARCHIVE_TOKEN"
API_BASE = "https://api.github.com"
ARCHIVE_PREFIX = "super-investor-data-"
ARCHIVE_SUFFIX = ".tar.gz"
MANIFEST_SUFFIX = ".manifest.json"
SHA_RE = re.compile(r"[0-9a-f]{64}")
SOURCE_SHA_RE = re.compile(r"[0-9a-f]{40}")
CREATED_AT_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
REPOSITORY_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
CACHE_FILES = (
    Path(".cache/sec_security_master.json"),
    Path(".cache/sec_source_state.json"),
)
QUANTITY_CACHE_FILES = (
    Path(".cache/quantity_estimation_evidence.json"),
    Path(".cache/quarter_close_prices.json"),
)
OPTIONAL_CACHE_FILES = (*QUANTITY_CACHE_FILES, Path(".cache/validation_cache.sqlite3"))
# Accept and hash older archives, but never extract or republish the retired queue.
ARCHIVED_CACHE_FILES = (Path(".cache/quarter_close_price_requests.json"),)
PAIR_TRANSACTION_ARTIFACT_PREFIX = ".sec-security-master-pair."
# Contract v1 is accepted for one migration release. Unprovenanced cache
# members are verified as part of the signed
# archive digest but intentionally not extracted. These source-neutral files
# are the only legacy cache state restored before the first v2 SEC rebuild.
LEGACY_RESTORABLE_CACHE_FILES = (
    Path(".cache/company_tickers_mf.json"),
    Path(".cache/sec_fund_names.json"),
)
# These files belonged to the retired registry contract. They are
# never authoritative after a restore, including when they predate a v2
# snapshot in the local working tree. Keep common historical spellings here so
# none can survive and override the SEC-derived data copy.
RETIRED_CACHE_FILES = (
    *ARCHIVED_CACHE_FILES,
    Path(".cache/cusip_map.json"),
    Path(".cache/cusip_registry.json"),
    Path(".cache/cusip-map.json"),
)
RETIRED_DATA_SUBTREES = frozenset({Path("data/insiders")})
RETIRED_PRIVATE_DATA_PREFIX = "data/insiders/private"
DATA_ROOT_FILES = frozenset({
    Path("data/company_tickers.json"),
    Path("data/cusip_registry.json"),
    Path("data/funds-index.json"),
    Path("data/index.json"),
    Path("data/pipeline_state.json"),
    Path("data/security_labels.json"),
    Path("data/ticker_health.json"),
})
DATA_COLLECTION_DIRS = frozenset({
    Path("data/funds"),
    Path("data/stocks"),
})


class SnapshotError(ValueError):
    """Raised when a snapshot fails a closed validation rule."""


@dataclass(frozen=True)
class SourceEntry:
    name: str
    path: Path
    is_dir: bool
    size: int


def _regular_file(path: Path, label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise SnapshotError(f"{label} is missing: {path}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SnapshotError(f"{label} must be a regular file: {path}")
    return metadata


def _regular_directory(path: Path, label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise SnapshotError(f"{label} is missing: {path}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise SnapshotError(f"{label} must be a real directory: {path}")
    return metadata


def _validate_positive_limit(value: int) -> None:
    if type(value) is not int or value < 1:
        raise SnapshotError("maximum archive size must be a positive integer")


def _is_allowed_v2_data_member(name: str, *, is_dir: bool) -> bool:
    path = Path(name)
    if is_dir:
        return path in DATA_COLLECTION_DIRS
    return path in DATA_ROOT_FILES or (
        path.parent in DATA_COLLECTION_DIRS
        and path.suffix == ".json"
        and not path.name.startswith(".")
    )


def _scan_source(root: Path) -> list[SourceEntry]:
    root = root.resolve()
    data_root = root / "data"
    cache_root = root / ".cache"
    _regular_directory(data_root, "data directory")
    _regular_directory(cache_root, "cache directory")

    entries = [
        SourceEntry(name=".cache", path=cache_root, is_dir=True, size=0),
        SourceEntry(name="data", path=data_root, is_dir=True, size=0),
    ]

    for current, directory_names, file_names in os.walk(
        data_root,
        topdown=True,
        followlinks=False,
    ):
        current_path = Path(current)
        directory_names.sort()
        file_names.sort()
        current_relative = current_path.relative_to(root)
        directory_names[:] = [
            directory_name
            for directory_name in directory_names
            if current_relative / directory_name not in RETIRED_DATA_SUBTREES
        ]
        for directory_name in directory_names:
            path = current_path / directory_name
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(
                metadata.st_mode
            ):
                raise SnapshotError(
                    f"data contains an unsupported entry: {path}"
                )
            relative = path.relative_to(root).as_posix()
            if not _is_allowed_v2_data_member(relative, is_dir=True):
                raise SnapshotError(
                    f"data contains a non-derived or unexpected entry: {path}"
                )
            entries.append(
                SourceEntry(
                    name=relative,
                    path=path,
                    is_dir=True,
                    size=0,
                )
            )
        for file_name in file_names:
            path = current_path / file_name
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                metadata.st_mode
            ):
                raise SnapshotError(
                    f"data contains an unsupported entry: {path}"
                )
            relative = path.relative_to(root).as_posix()
            if not _is_allowed_v2_data_member(relative, is_dir=False):
                raise SnapshotError(
                    f"data contains a non-derived or unexpected entry: {path}"
                )
            entries.append(
                SourceEntry(
                    name=relative,
                    path=path,
                    is_dir=False,
                    size=metadata.st_size,
                )
            )

    for relative in (*CACHE_FILES, *OPTIONAL_CACHE_FILES):
        path = root / relative
        if relative in OPTIONAL_CACHE_FILES and not path.exists() and not path.is_symlink():
            continue
        metadata = _regular_file(path, "required cache file")
        entries.append(
            SourceEntry(
                name=relative.as_posix(),
                path=path,
                is_dir=False,
                size=metadata.st_size,
            )
        )

    entries.sort(key=lambda entry: entry.name)
    names = [entry.name for entry in entries]
    if len(names) != len(set(names)):
        raise SnapshotError("source contains duplicate archive paths")
    return entries


def _digest_header(
    digest: "hashlib._Hash",
    *,
    name: str,
    is_dir: bool,
    size: int,
) -> None:
    try:
        encoded_name = name.encode("utf-8")
    except UnicodeEncodeError as error:
        raise SnapshotError(f"archive path is not valid UTF-8: {name!r}") from error
    digest.update(b"D" if is_dir else b"F")
    digest.update(len(encoded_name).to_bytes(8, "big"))
    digest.update(encoded_name)
    digest.update(size.to_bytes(8, "big"))


def _source_content_digest(entries: Iterable[SourceEntry]) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    file_count = 0
    content_bytes = 0
    for entry in entries:
        _digest_header(
            digest,
            name=entry.name,
            is_dir=entry.is_dir,
            size=entry.size,
        )
        if entry.is_dir:
            continue
        file_count += 1
        content_bytes += entry.size
        with entry.path.open("rb") as handle:
            remaining = entry.size
            while remaining:
                block = handle.read(min(1024 * 1024, remaining))
                if not block:
                    raise SnapshotError(
                        f"source file changed while being read: {entry.path}"
                    )
                digest.update(block)
                remaining -= len(block)
            if handle.read(1):
                raise SnapshotError(
                    f"source file changed while being read: {entry.path}"
                )
    return digest.hexdigest(), file_count, content_bytes


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _archive_mode(name: str, *, is_dir: bool) -> int:
    is_retired_private_data = (
        name == RETIRED_PRIVATE_DATA_PREFIX
        or name.startswith(f"{RETIRED_PRIVATE_DATA_PREFIX}/")
    )
    if is_retired_private_data:
        return 0o700 if is_dir else 0o600
    return 0o755 if is_dir else 0o644


def _tar_info(entry: SourceEntry) -> tarfile.TarInfo:
    info = tarfile.TarInfo(entry.name)
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    if entry.is_dir:
        info.type = tarfile.DIRTYPE
        info.mode = _archive_mode(entry.name, is_dir=True)
        info.size = 0
    else:
        info.type = tarfile.REGTYPE
        info.mode = _archive_mode(entry.name, is_dir=False)
        info.size = entry.size
    return info


def _write_archive(entries: Iterable[SourceEntry], destination: Path) -> None:
    with destination.open("xb") as raw_output:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_output,
            compresslevel=6,
            mtime=0,
        ) as gzip_output:
            with tarfile.open(
                fileobj=gzip_output,
                mode="w",
                format=tarfile.USTAR_FORMAT,
            ) as archive:
                for entry in entries:
                    info = _tar_info(entry)
                    if entry.is_dir:
                        archive.addfile(info)
                    else:
                        with entry.path.open("rb") as source:
                            archive.addfile(info, source)


def _manifest_names(dataset_sha256: str) -> tuple[str, str]:
    archive_name = f"{ARCHIVE_PREFIX}{dataset_sha256}{ARCHIVE_SUFFIX}"
    manifest_name = f"{ARCHIVE_PREFIX}{dataset_sha256}{MANIFEST_SUFFIX}"
    return archive_name, manifest_name


def _manifest_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    dataset = manifest["dataset"]
    archive = manifest["archive"]
    return {
        "contract_version": manifest["contract_version"],
        "legacy_snapshot": (
            manifest["contract_version"] == LEGACY_CONTRACT_VERSION
        ),
        "created_at": manifest["created_at"],
        "dataset_id": manifest["dataset_id"],
        "dataset_sha256": dataset["sha256"],
        "source_sha": manifest["source_sha"],
        "archive_filename": archive["filename"],
        "archive_sha256": archive["sha256"],
        "archive_bytes": archive["bytes"],
        "file_count": dataset["file_count"],
        "content_bytes": dataset["bytes"],
    }


def _require_exact_keys(
    value: object,
    expected: set[str],
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SnapshotError(f"{label} must be a JSON object")
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        details = []
        if missing:
            details.append(f"missing {missing}")
        if unexpected:
            details.append(f"unexpected {unexpected}")
        raise SnapshotError(f"{label} fields are invalid: {', '.join(details)}")
    return value


def _load_manifest(path: Path) -> dict[str, Any]:
    metadata = _regular_file(path, "manifest")
    if metadata.st_size > MAX_MANIFEST_BYTES:
        raise SnapshotError("manifest is too large")
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SnapshotError(f"manifest is not valid UTF-8 JSON: {error}") from error
    manifest = _require_exact_keys(
        parsed,
        {
            "archive",
            "contract_version",
            "created_at",
            "dataset",
            "dataset_id",
            "source_sha",
        },
        "manifest",
    )
    if type(manifest["contract_version"]) is not int:
        raise SnapshotError("manifest contract_version must be an integer")
    if manifest["contract_version"] not in {
        LEGACY_CONTRACT_VERSION,
        CONTRACT_VERSION,
    }:
        raise SnapshotError(
            "unsupported snapshot contract version: "
            f"{manifest['contract_version']}"
        )
    source_sha = manifest["source_sha"]
    if not isinstance(source_sha, str) or not SOURCE_SHA_RE.fullmatch(source_sha):
        raise SnapshotError("manifest source_sha must be 40 lowercase hex characters")
    created_at = manifest["created_at"]
    if not isinstance(created_at, str) or not CREATED_AT_RE.fullmatch(created_at):
        raise SnapshotError("manifest created_at must be UTC in YYYY-MM-DDTHH:MM:SSZ")
    try:
        parsed_created_at = datetime.strptime(
            created_at,
            "%Y-%m-%dT%H:%M:%SZ",
        ).replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise SnapshotError(
            "manifest created_at is not a valid UTC timestamp"
        ) from error
    if parsed_created_at.strftime("%Y-%m-%dT%H:%M:%SZ") != created_at:
        raise SnapshotError("manifest created_at is not canonical")

    dataset = _require_exact_keys(
        manifest["dataset"],
        {"bytes", "file_count", "sha256"},
        "manifest dataset",
    )
    dataset_sha256 = dataset["sha256"]
    if not isinstance(dataset_sha256, str) or not SHA_RE.fullmatch(
        dataset_sha256
    ):
        raise SnapshotError("manifest dataset sha256 is invalid")
    for field in ("bytes", "file_count"):
        if type(dataset[field]) is not int or dataset[field] < 0:
            raise SnapshotError(
                f"manifest dataset {field} must be a non-negative integer"
            )
    if manifest["dataset_id"] != dataset_sha256:
        raise SnapshotError("manifest dataset_id does not match dataset sha256")

    archive = _require_exact_keys(
        manifest["archive"],
        {"bytes", "filename", "sha256"},
        "manifest archive",
    )
    if not isinstance(archive["sha256"], str) or not SHA_RE.fullmatch(
        archive["sha256"]
    ):
        raise SnapshotError("manifest archive sha256 is invalid")
    if type(archive["bytes"]) is not int or archive["bytes"] < 1:
        raise SnapshotError("manifest archive bytes must be a positive integer")
    expected_archive_name, expected_manifest_name = _manifest_names(
        dataset_sha256
    )
    if archive["filename"] != expected_archive_name:
        raise SnapshotError(
            "manifest archive filename does not match dataset sha256"
        )
    if path.name != expected_manifest_name:
        raise SnapshotError(
            "manifest sidecar filename does not match dataset sha256"
        )
    return manifest


def _validate_member_name(name: str) -> None:
    if not name or name.startswith("/") or "\\" in name:
        raise SnapshotError(f"unsafe archive path: {name!r}")
    parts = name.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise SnapshotError(f"unsafe archive path: {name!r}")
    normalized = PurePosixPath(name).as_posix()
    if normalized != name:
        raise SnapshotError(f"unsafe archive path: {name!r}")


def _validate_member_scope(
    member: tarfile.TarInfo,
    *,
    contract_version: int,
) -> None:
    name = member.name
    _validate_member_name(name)
    cache_names = {path.as_posix() for path in (*CACHE_FILES, *OPTIONAL_CACHE_FILES, *ARCHIVED_CACHE_FILES)}
    if name == "data":
        if not member.isdir():
            raise SnapshotError("archive data root must be a directory")
    elif name.startswith("data/"):
        if (
            contract_version != LEGACY_CONTRACT_VERSION
            and (member.isdir() or member.isfile())
            and not _is_allowed_v2_data_member(name, is_dir=member.isdir())
        ):
            raise SnapshotError(f"unexpected archive member: {name}")
    elif name == ".cache":
        if not member.isdir():
            raise SnapshotError("archive cache root must be a directory")
    elif name in cache_names:
        if not member.isfile():
            raise SnapshotError(f"cache archive member must be a file: {name}")
    elif (
        PurePosixPath(name).parent.as_posix() == ".cache"
        and PurePosixPath(name).name.startswith(
            PAIR_TRANSACTION_ARTIFACT_PREFIX
        )
    ):
        raise SnapshotError(
            f"security-master transaction artifact is not publishable: {name}"
        )
    elif (
        contract_version == LEGACY_CONTRACT_VERSION
        and PurePosixPath(name).parent.as_posix() == ".cache"
    ):
        if not member.isfile():
            raise SnapshotError(f"legacy cache member must be a file: {name}")
    else:
        raise SnapshotError(f"unexpected archive member: {name}")


def _copy_exact(
    source: BinaryIO,
    destination: Optional[BinaryIO],
    digest: "hashlib._Hash",
    size: int,
) -> None:
    remaining = size
    while remaining:
        block = source.read(min(1024 * 1024, remaining))
        if not block:
            raise SnapshotError("archive member is truncated")
        digest.update(block)
        if destination is not None:
            destination.write(block)
        remaining -= len(block)
    if source.read(1):
        raise SnapshotError("archive member exceeds its declared size")


def _verify_archive_contents(
    archive_path: Path,
    manifest: dict[str, Any],
    extract_root: Optional[Path],
) -> None:
    dataset = manifest["dataset"]
    digest = hashlib.sha256()
    names: list[str] = []
    seen_names: set[str] = set()
    previous_name: Optional[str] = None
    directories: set[str] = set()
    file_count = 0
    content_bytes = 0

    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            for member in archive:
                _validate_member_scope(
                    member,
                    contract_version=manifest["contract_version"],
                )
                if member.name in seen_names:
                    raise SnapshotError(
                        f"duplicate archive member: {member.name}"
                    )
                if previous_name is not None and member.name < previous_name:
                    raise SnapshotError("archive members are not deterministic")
                names.append(member.name)
                seen_names.add(member.name)
                previous_name = member.name
                if member.pax_headers:
                    raise SnapshotError(
                        f"archive member has unsupported PAX metadata: {member.name}"
                    )
                if member.type not in {tarfile.DIRTYPE, tarfile.REGTYPE}:
                    raise SnapshotError(
                        f"archive member has unsupported type: {member.name}"
                    )
                if member.size < 0 or (member.isdir() and member.size != 0):
                    raise SnapshotError(
                        f"archive member has invalid size: {member.name}"
                    )
                expected_mode = _archive_mode(
                    member.name,
                    is_dir=member.isdir(),
                )
                if (
                    member.uid != 0
                    or member.gid != 0
                    or member.uname != ""
                    or member.gname != ""
                    or member.mtime != 0
                    or member.mode != expected_mode
                ):
                    raise SnapshotError(
                        f"archive member metadata is not canonical: {member.name}"
                    )

                if member.isdir():
                    directories.add(member.name)
                    _digest_header(
                        digest,
                        name=member.name,
                        is_dir=True,
                        size=0,
                    )
                    if extract_root is not None:
                        destination = extract_root.joinpath(*member.name.split("/"))
                        destination.mkdir()
                        destination.chmod(expected_mode)
                    continue

                file_count += 1
                content_bytes += member.size
                if file_count > dataset["file_count"]:
                    raise SnapshotError("archive contains too many files")
                if content_bytes > dataset["bytes"]:
                    raise SnapshotError("archive content exceeds manifest bytes")
                _digest_header(
                    digest,
                    name=member.name,
                    is_dir=False,
                    size=member.size,
                )
                source = archive.extractfile(member)
                if source is None:
                    raise SnapshotError(
                        f"archive member cannot be read: {member.name}"
                    )
                extract_member = (
                    extract_root is not None
                    and (
                        not member.name.startswith(".cache/")
                        or member.name
                        in {
                            path.as_posix()
                            for path in (
                                (*CACHE_FILES, *OPTIONAL_CACHE_FILES)
                                if manifest["contract_version"] == CONTRACT_VERSION
                                else LEGACY_RESTORABLE_CACHE_FILES
                            )
                        }
                    )
                )
                if not extract_member:
                    with source:
                        _copy_exact(source, None, digest, member.size)
                else:
                    assert extract_root is not None
                    destination = extract_root.joinpath(*member.name.split("/"))
                    with source, destination.open("xb") as output:
                        _copy_exact(source, output, digest, member.size)
                    destination.chmod(expected_mode)
                    os.utime(destination, (0, 0))
    except SnapshotError:
        raise
    except (tarfile.TarError, EOFError, OSError) as error:
        raise SnapshotError(f"archive cannot be read: {error}") from error

    required_cache_files = (
        CACHE_FILES
        if manifest["contract_version"] == CONTRACT_VERSION
        else LEGACY_RESTORABLE_CACHE_FILES
    )
    required_names = {".cache", "data"} | {
        path.as_posix() for path in required_cache_files
    }
    missing = sorted(required_names - set(names))
    if missing:
        raise SnapshotError(f"archive is missing required members: {missing}")
    for name in names:
        parent = PurePosixPath(name).parent
        if str(parent) != "." and parent.as_posix() not in directories:
            raise SnapshotError(
                f"archive member is missing its parent directory: {name}"
            )
    if file_count != dataset["file_count"]:
        raise SnapshotError("archive file count does not match manifest")
    if content_bytes != dataset["bytes"]:
        raise SnapshotError("archive content bytes do not match manifest")
    if digest.hexdigest() != dataset["sha256"]:
        raise SnapshotError("archive content digest does not match manifest")


def verify_snapshot(
    *,
    archive_path: Path,
    manifest_path: Path,
    max_archive_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES,
    extract_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Verify a snapshot and optionally extract it to a new destination."""
    _validate_positive_limit(max_archive_bytes)
    archive_path = Path(archive_path)
    manifest_path = Path(manifest_path)
    archive_metadata = _regular_file(archive_path, "archive")
    manifest = _load_manifest(manifest_path)
    if archive_path.name != manifest["archive"]["filename"]:
        raise SnapshotError("archive filename does not match manifest")
    if archive_metadata.st_size > max_archive_bytes:
        raise SnapshotError(
            f"archive is too large: {archive_metadata.st_size} > "
            f"{max_archive_bytes} bytes"
        )
    if archive_metadata.st_size != manifest["archive"]["bytes"]:
        raise SnapshotError("archive byte count does not match manifest")
    if _sha256_file(archive_path) != manifest["archive"]["sha256"]:
        raise SnapshotError("archive checksum does not match manifest")

    extraction_staging: Optional[Path] = None
    destination: Optional[Path] = None
    if extract_root is not None:
        destination = Path(extract_root).absolute()
        if destination.exists() or destination.is_symlink():
            raise SnapshotError(
                f"extraction destination already exists: {destination}"
            )
        _regular_directory(destination.parent, "extraction destination parent")
        extraction_staging = Path(
            tempfile.mkdtemp(
                prefix=f".{destination.name}.snapshot-",
                dir=destination.parent,
            )
        )

    try:
        _verify_archive_contents(
            archive_path,
            manifest,
            extraction_staging,
        )
        if extraction_staging is not None and destination is not None:
            os.replace(extraction_staging, destination)
            extraction_staging = None
    finally:
        if extraction_staging is not None:
            shutil.rmtree(extraction_staging, ignore_errors=True)

    summary = _manifest_summary(manifest)
    summary["verified"] = True
    if destination is not None:
        summary["extract_root"] = str(destination)
    return summary


def _capture_locked_snapshot_source(
    root: Path,
    output_dir: Path,
) -> tuple[Path, str, int, int, str, str, Path, Path]:
    """Materialize one archive while the SEC cache pair is immutable."""

    master_path, source_state_path = (root / relative for relative in CACHE_FILES)
    try:
        with security_master_pair_lock(
            master_path=master_path,
            source_state_path=source_state_path,
        ):
            entries = _scan_source(root)
            dataset_sha256, file_count, content_bytes = _source_content_digest(
                entries
            )
            archive_name, manifest_name = _manifest_names(dataset_sha256)
            archive_path = output_dir / archive_name
            manifest_path = output_dir / manifest_name
            if archive_path.exists() or archive_path.is_symlink():
                raise SnapshotError(
                    f"snapshot archive already exists: {archive_path}"
                )
            if manifest_path.exists() or manifest_path.is_symlink():
                raise SnapshotError(
                    f"snapshot manifest already exists: {manifest_path}"
                )

            with tempfile.NamedTemporaryFile(
                prefix=".data-snapshot-",
                suffix=ARCHIVE_SUFFIX,
                dir=output_dir,
                delete=False,
            ) as temporary:
                temporary_archive = Path(temporary.name)
            temporary_archive.unlink()
            try:
                _write_archive(entries, temporary_archive)
            except BaseException:
                temporary_archive.unlink(missing_ok=True)
                raise
    except SecurityMasterError as error:
        raise SnapshotError(
            f"SEC security-master pair is invalid: {error}"
        ) from error

    return (
        temporary_archive,
        dataset_sha256,
        file_count,
        content_bytes,
        archive_name,
        manifest_name,
        archive_path,
        manifest_path,
    )


def pack_snapshot(
    *,
    root: Path,
    output_dir: Path,
    source_sha: str,
    max_archive_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES,
    created_at: Optional[str] = None,
) -> dict[str, Any]:
    """Build a deterministic archive and its strict sidecar manifest."""
    _validate_positive_limit(max_archive_bytes)
    if not SOURCE_SHA_RE.fullmatch(source_sha):
        raise SnapshotError(
            "source SHA must be exactly 40 lowercase hex characters"
        )
    root = Path(root).resolve()
    _regular_directory(root, "snapshot root")
    output_dir = Path(output_dir).absolute()
    data_root = root / "data"
    cache_root = root / ".cache"
    if output_dir in {data_root, cache_root} or any(
        parent in {data_root, cache_root} for parent in output_dir.parents
    ):
        raise SnapshotError("output directory cannot be inside snapshot data")
    if output_dir.exists():
        _regular_directory(output_dir, "output directory")
    else:
        output_dir.mkdir(parents=True)

    temporary_archive: Optional[Path] = None
    temporary_manifest: Optional[Path] = None
    try:
        (
            temporary_archive,
            dataset_sha256,
            file_count,
            content_bytes,
            archive_name,
            manifest_name,
            archive_path,
            manifest_path,
        ) = _capture_locked_snapshot_source(root, output_dir)
        dataset_id = dataset_sha256
        archive_bytes = temporary_archive.stat().st_size
        if archive_bytes > max_archive_bytes:
            raise SnapshotError(
                f"archive is too large: {archive_bytes} > "
                f"{max_archive_bytes} bytes"
            )
        archive_sha256 = _sha256_file(temporary_archive)
        manifest = {
            "archive": {
                "bytes": archive_bytes,
                "filename": archive_name,
                "sha256": archive_sha256,
            },
            "contract_version": CONTRACT_VERSION,
            "created_at": created_at
            or datetime.now(timezone.utc).replace(microsecond=0).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "dataset": {
                "bytes": content_bytes,
                "file_count": file_count,
                "sha256": dataset_sha256,
            },
            "dataset_id": dataset_id,
            "source_sha": source_sha,
        }
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=".data-snapshot-",
            suffix=MANIFEST_SUFFIX,
            dir=output_dir,
            delete=False,
        ) as temporary:
            temporary_manifest = Path(temporary.name)
            json.dump(manifest, temporary, sort_keys=True, separators=(",", ":"))
            temporary.write("\n")

        # Verify the materialized bytes before exposing either final asset.
        verification_archive = output_dir / archive_name
        verification_manifest = output_dir / manifest_name
        os.replace(temporary_archive, verification_archive)
        temporary_archive = None
        try:
            os.replace(temporary_manifest, verification_manifest)
            temporary_manifest = None
            verify_snapshot(
                archive_path=verification_archive,
                manifest_path=verification_manifest,
                max_archive_bytes=max_archive_bytes,
            )
        except Exception:
            verification_archive.unlink(missing_ok=True)
            verification_manifest.unlink(missing_ok=True)
            raise
    finally:
        if temporary_archive is not None:
            temporary_archive.unlink(missing_ok=True)
        if temporary_manifest is not None:
            temporary_manifest.unlink(missing_ok=True)

    summary = _manifest_summary(manifest)
    summary.update(
        {
            "archive_path": str(archive_path),
            "manifest_filename": manifest_name,
            "manifest_path": str(manifest_path),
        }
    )
    return summary


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Do not forward a GitHub token to a cross-host signed asset URL."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: BinaryIO,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Optional[urllib.request.Request]:
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None:
            old_host = urllib.parse.urlsplit(req.full_url).netloc
            new_host = urllib.parse.urlsplit(newurl).netloc
            if old_host.lower() != new_host.lower():
                redirected.remove_header("Authorization")
        return redirected


_URL_OPENER = urllib.request.build_opener(_SafeRedirectHandler())


def _authorized_request(url: str, token: str, accept: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={
            "Accept": accept,
            "Authorization": f"Bearer {token}",
            "User-Agent": "super-investor-seeker-data-snapshot/1",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )


def _github_json(url: str, token: str) -> Any:
    payload = b""
    for attempt in range(len(GITHUB_RETRY_DELAYS_SECONDS) + 1):
        request = _authorized_request(url, token, "application/vnd.github+json")
        try:
            with _URL_OPENER.open(request, timeout=60) as response:
                payload = response.read(MAX_API_RESPONSE_BYTES + 1)
            break
        except urllib.error.HTTPError as error:
            retryable = error.code in GITHUB_RETRY_STATUS_CODES
            if retryable and attempt < len(GITHUB_RETRY_DELAYS_SECONDS):
                time.sleep(GITHUB_RETRY_DELAYS_SECONDS[attempt])
                continue
            raise SnapshotError(f"GitHub API request failed: {error}") from error
        except GITHUB_RETRY_EXCEPTIONS as error:
            if attempt < len(GITHUB_RETRY_DELAYS_SECONDS):
                time.sleep(GITHUB_RETRY_DELAYS_SECONDS[attempt])
                continue
            raise SnapshotError(f"GitHub API request failed: {error}") from error
    if len(payload) > MAX_API_RESPONSE_BYTES:
        raise SnapshotError("GitHub API response is too large")
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SnapshotError("GitHub API returned invalid JSON") from error


def _download_url(
    *,
    url: str,
    destination: Path,
    token: str,
    max_bytes: int,
    expected_bytes: Optional[int] = None,
) -> None:
    total = 0
    for attempt in range(len(GITHUB_RETRY_DELAYS_SECONDS) + 1):
        request = _authorized_request(url, token, "application/octet-stream")
        total = 0
        try:
            with _URL_OPENER.open(request, timeout=120) as response:
                content_length = response.headers.get("Content-Length")
                if content_length is not None:
                    try:
                        declared_bytes = int(content_length)
                    except ValueError as error:
                        raise SnapshotError(
                            "download response has invalid Content-Length"
                        ) from error
                    if declared_bytes > max_bytes:
                        raise SnapshotError("download exceeds maximum allowed size")
                    if expected_bytes is not None and declared_bytes != expected_bytes:
                        raise SnapshotError(
                            "download Content-Length does not match release asset"
                        )
                with destination.open("xb") as output:
                    os.fchmod(output.fileno(), 0o600)
                    while True:
                        block = response.read(1024 * 1024)
                        if not block:
                            break
                        total += len(block)
                        if total > max_bytes:
                            raise SnapshotError(
                                "download exceeds maximum allowed size"
                            )
                        output.write(block)
            break
        except SnapshotError:
            destination.unlink(missing_ok=True)
            raise
        except urllib.error.HTTPError as error:
            destination.unlink(missing_ok=True)
            retryable = error.code in GITHUB_RETRY_STATUS_CODES
            if retryable and attempt < len(GITHUB_RETRY_DELAYS_SECONDS):
                time.sleep(GITHUB_RETRY_DELAYS_SECONDS[attempt])
                continue
            raise SnapshotError(f"release asset download failed: {error}") from error
        except GITHUB_RETRY_EXCEPTIONS as error:
            destination.unlink(missing_ok=True)
            if attempt < len(GITHUB_RETRY_DELAYS_SECONDS):
                time.sleep(GITHUB_RETRY_DELAYS_SECONDS[attempt])
                continue
            raise SnapshotError(f"release asset download failed: {error}") from error
        except OSError as error:
            destination.unlink(missing_ok=True)
            raise SnapshotError(f"release asset download failed: {error}") from error
    if expected_bytes is not None and total != expected_bytes:
        destination.unlink(missing_ok=True)
        raise SnapshotError("downloaded byte count does not match release asset")


def _validated_release(value: object, *, expected_tag: Optional[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SnapshotError("GitHub release response must be an object")
    tag = value.get("tag_name")
    if not isinstance(tag, str) or not tag.startswith("dataset-"):
        raise SnapshotError("release tag must start with dataset-")
    if expected_tag is not None and tag != expected_tag:
        raise SnapshotError("GitHub returned a different release tag")
    if (
        value.get("draft") is not False
        or value.get("prerelease") is not False
        or not value.get("published_at")
    ):
        raise SnapshotError(f"release is not a stable published release: {tag}")
    if not isinstance(value.get("assets"), list):
        raise SnapshotError(f"release assets are invalid: {tag}")
    return value


def _resolve_release(
    *,
    repository: str,
    release_tag: Optional[str],
    token: str,
) -> dict[str, Any]:
    quoted_repository = "/".join(
        urllib.parse.quote(part, safe="") for part in repository.split("/")
    )
    if release_tag is not None:
        if not release_tag.startswith("dataset-"):
            raise SnapshotError("release tag must start with dataset-")
        url = (
            f"{API_BASE}/repos/{quoted_repository}/releases/tags/"
            f"{urllib.parse.quote(release_tag, safe='')}"
        )
        return _validated_release(
            _github_json(url, token),
            expected_tag=release_tag,
        )

    url = f"{API_BASE}/repos/{quoted_repository}/releases?per_page=100"
    response = _github_json(url, token)
    if not isinstance(response, list):
        raise SnapshotError("GitHub releases response must be an array")
    candidates = [
        release
        for release in response
        if isinstance(release, dict)
        and isinstance(release.get("tag_name"), str)
        and release["tag_name"].startswith("dataset-")
        and release.get("draft") is False
        and release.get("prerelease") is False
        and isinstance(release.get("published_at"), str)
        and isinstance(release.get("assets"), list)
    ]
    if not candidates:
        raise SnapshotError("no published dataset-* release was found")
    selected = max(
        candidates,
        key=lambda release: (
            release["published_at"],
            release.get("id", 0) if type(release.get("id", 0)) is int else 0,
        ),
    )
    return _validated_release(selected, expected_tag=None)


def _find_asset(
    assets: list[object],
    *,
    name: Optional[str] = None,
    suffix: Optional[str] = None,
) -> dict[str, Any]:
    matches = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        asset_name = asset.get("name")
        if not isinstance(asset_name, str):
            continue
        if name is not None and asset_name != name:
            continue
        if suffix is not None and not (
            asset_name.startswith(ARCHIVE_PREFIX) and asset_name.endswith(suffix)
        ):
            continue
        matches.append(asset)
    if len(matches) != 1:
        description = name or f"one *{suffix} snapshot asset"
        raise SnapshotError(
            f"release must contain exactly {description}; found {len(matches)}"
        )
    asset = matches[0]
    if not isinstance(asset.get("url"), str):
        raise SnapshotError(f"release asset URL is invalid: {asset.get('name')}")
    if type(asset.get("size")) is not int or asset["size"] < 1:
        raise SnapshotError(f"release asset size is invalid: {asset.get('name')}")
    return asset


def _download_asset(
    *,
    asset: dict[str, Any],
    destination: Path,
    token: str,
    max_bytes: int,
) -> None:
    _download_url(
        url=asset["url"],
        destination=destination,
        token=token,
        max_bytes=max_bytes,
        expected_bytes=asset["size"],
    )


def _validate_restore_targets(
    root: Path,
    *,
    cache_files: tuple[Path, ...],
) -> None:
    data_target = root / "data"
    if data_target.exists() or data_target.is_symlink():
        _regular_directory(data_target, "existing data target")
    cache_root = root / ".cache"
    if cache_root.exists() or cache_root.is_symlink():
        _regular_directory(cache_root, "existing cache target")
    for relative in cache_files:
        target = root / relative
        if target.exists() or target.is_symlink():
            _regular_file(target, "existing cache target")


def _restore_cache_contract(
    contract_version: int,
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    """Return cache files to install and stale cache files to remove."""

    if contract_version == CONTRACT_VERSION:
        cache_files = CACHE_FILES
        removal_files = (*RETIRED_CACHE_FILES, *OPTIONAL_CACHE_FILES)
    elif contract_version == LEGACY_CONTRACT_VERSION:
        cache_files = LEGACY_RESTORABLE_CACHE_FILES
        # A legacy data tree cannot safely share newer SEC evidence. The next
        # migration rebuild must recreate both v2 files from that tree.
        removal_files = (*RETIRED_CACHE_FILES, *CACHE_FILES, *OPTIONAL_CACHE_FILES)
    else:
        raise SnapshotError(
            f"unsupported snapshot contract version: {contract_version}"
        )
    return cache_files, tuple(dict.fromkeys(removal_files))


def _replace_payload_transaction(
    root: Path,
    payload: Path,
    *,
    cache_files: tuple[Path, ...],
    removal_files: tuple[Path, ...],
    incoming_pair: tuple[dict[str, Any], dict[str, Any]] | None,
) -> None:
    """Apply the already-validated payload with rollback on every throwable."""

    # A v2 SEC pair is installed only by the pair transaction below. Keeping
    # the current targets in place until that final call lets the core helper
    # recover either the complete old or complete new generation after a kill.
    generic_cache_files = (
        tuple(relative for relative in cache_files if relative not in CACHE_FILES)
        if incoming_pair is not None
        else cache_files
    )
    generic_removal_files = (
        tuple(relative for relative in removal_files if relative not in CACHE_FILES)
        if incoming_pair is not None
        else removal_files
    )
    cache_targets = tuple(
        dict.fromkeys((*generic_cache_files, *generic_removal_files))
    )

    backup = payload.parent / "restore-backup"
    backup.mkdir()
    moved_old: list[tuple[Path, Path]] = []
    installed: list[Path] = []
    cache_root = root / ".cache"
    cache_root_created = False
    try:
        data_target = root / "data"
        if data_target.exists():
            old_data = backup / "data"
            os.replace(data_target, old_data)
            moved_old.append((old_data, data_target))
        for relative in cache_targets:
            target = root / relative
            if target.exists():
                old_target = backup / relative
                old_target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(target, old_target)
                moved_old.append((old_target, target))

        os.replace(payload / "data", data_target)
        installed.append(data_target)
        if not cache_root.exists():
            cache_root.mkdir()
            cache_root_created = True
        for relative in generic_cache_files:
            target = root / relative
            os.replace(payload / relative, target)
            installed.append(target)
        if incoming_pair is not None:
            incoming_master, incoming_state = incoming_pair
            master_path, source_state_path = (
                root / relative for relative in CACHE_FILES
            )
            # This is deliberately the last cache mutation. The pair helper
            # catches BaseException and restores the old pair before our outer
            # data/retired-cache rollback runs.
            save_security_master_pair(
                incoming_master,
                incoming_state,
                master_path=master_path,
                source_state_path=source_state_path,
            )
    except BaseException as error:
        rollback_errors = []
        for installed_path in reversed(installed):
            try:
                if installed_path.is_dir() and not installed_path.is_symlink():
                    shutil.rmtree(installed_path)
                else:
                    installed_path.unlink(missing_ok=True)
            except OSError as rollback_error:
                rollback_errors.append(str(rollback_error))
        for backup_path, target in reversed(moved_old):
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(backup_path, target)
            except OSError as rollback_error:
                rollback_errors.append(str(rollback_error))
        if cache_root_created:
            try:
                cache_root.rmdir()
            except OSError:
                pass
        details = f"; rollback errors: {rollback_errors}" if rollback_errors else ""
        if not isinstance(error, Exception):
            if details and hasattr(error, "add_note"):
                error.add_note(f"snapshot restore{details}")
            raise
        raise SnapshotError(f"snapshot restore failed: {error}{details}") from error


def _replace_payload(
    root: Path,
    payload: Path,
    *,
    contract_version: int,
) -> None:
    """Transactionally replace data, caches, and retired cache state."""

    cache_files, removal_files = _restore_cache_contract(contract_version)
    if contract_version == CONTRACT_VERSION:
        cache_files = (*cache_files, *(relative for relative in OPTIONAL_CACHE_FILES if (payload / relative).exists()))
    cache_targets = tuple(dict.fromkeys((*cache_files, *removal_files)))
    _validate_restore_targets(root, cache_files=cache_targets)
    from saved_price_migration import migrate_saved_prices

    _regular_directory(payload / "data", "extracted data directory")
    _regular_directory(payload / ".cache", "extracted cache directory")
    for relative in cache_files:
        _regular_file(payload / relative, "extracted cache file")
    try:
        migrate_saved_prices(payload)
    except (ValueError, OSError) as error:
        raise SnapshotError(f"saved price receipt migration failed: {error}") from error

    master_path, source_state_path = (root / relative for relative in CACHE_FILES)
    if contract_version == CONTRACT_VERSION:
        extracted_master_path, extracted_state_path = (
            payload / relative for relative in CACHE_FILES
        )
        try:
            incoming_pair = load_security_master_pair(
                master_path=extracted_master_path,
                source_state_path=extracted_state_path,
            )
        except (SecurityMasterError, OSError) as error:
            raise SnapshotError(
                f"extracted SEC security-master pair is invalid: {error}"
            ) from error
        _replace_payload_transaction(
            root,
            payload,
            cache_files=cache_files,
            removal_files=removal_files,
            incoming_pair=incoming_pair,
        )
        return

    # A legacy restore removes, rather than installs, both SEC targets. Hold
    # the same lock as pair writers so it cannot capture one target midway
    # through a concurrent generation change. Both targets may validly be
    # absent before the one-release migration restore.
    try:
        with security_master_pair_lock(
            master_path=master_path,
            source_state_path=source_state_path,
        ):
            _replace_payload_transaction(
                root,
                payload,
                cache_files=cache_files,
                removal_files=removal_files,
                incoming_pair=None,
            )
    except SecurityMasterError as error:
        raise SnapshotError(
            f"existing SEC security-master pair is invalid: {error}"
        ) from error


def pull_snapshot(
    *,
    repository: str,
    root: Path,
    replace: bool,
    release_tag: Optional[str] = None,
    max_archive_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES,
    token: Optional[str] = None,
) -> dict[str, Any]:
    """Download, verify, and transactionally restore a private release."""
    _validate_positive_limit(max_archive_bytes)
    if not replace:
        raise SnapshotError("pull requires --replace to modify local data")
    if not REPOSITORY_RE.fullmatch(repository):
        raise SnapshotError("repository must use OWNER/REPO format")
    root = Path(root).resolve()
    _regular_directory(root, "restore root")
    resolved_token = token if token is not None else os.environ.get(TOKEN_ENV)
    if not resolved_token:
        raise SnapshotError(f"{TOKEN_ENV} is not set")

    release = _resolve_release(
        repository=repository,
        release_tag=release_tag,
        token=resolved_token,
    )
    assets = release["assets"]
    manifest_asset = _find_asset(assets, suffix=MANIFEST_SUFFIX)
    with tempfile.TemporaryDirectory(
        prefix=".data-snapshot-pull-",
        dir=root.parent,
    ) as temporary_directory:
        temporary_root = Path(temporary_directory)
        manifest_path = temporary_root / manifest_asset["name"]
        _download_asset(
            asset=manifest_asset,
            destination=manifest_path,
            token=resolved_token,
            max_bytes=MAX_MANIFEST_BYTES,
        )
        manifest = _load_manifest(manifest_path)
        archive_asset = _find_asset(
            assets,
            name=manifest["archive"]["filename"],
        )
        asset_names = [
            asset.get("name") if isinstance(asset, dict) else None
            for asset in assets
        ]
        required_asset_names = {
            manifest_asset["name"],
            archive_asset["name"],
        }
        allowed_asset_names = required_asset_names | {"pages-deployment.json"}
        if (
            len(asset_names) != len(set(asset_names))
            or not required_asset_names.issubset(set(asset_names))
            or not set(asset_names).issubset(allowed_asset_names)
        ):
            raise SnapshotError(
                "release assets must be the snapshot archive and manifest, "
                "with only an optional pages-deployment.json marker"
            )
        if archive_asset["size"] != manifest["archive"]["bytes"]:
            raise SnapshotError(
                "release archive asset size does not match manifest"
            )
        archive_path = temporary_root / archive_asset["name"]
        _download_asset(
            asset=archive_asset,
            destination=archive_path,
            token=resolved_token,
            max_bytes=max_archive_bytes,
        )
        payload = temporary_root / "payload"
        summary = verify_snapshot(
            archive_path=archive_path,
            manifest_path=manifest_path,
            max_archive_bytes=max_archive_bytes,
            extract_root=payload,
        )
        _replace_payload(
            root,
            payload,
            contract_version=summary["contract_version"],
        )

    summary.pop("extract_root", None)
    summary.update(
        {
            "release_tag": release["tag_name"],
            "repository": repository,
            "restored_root": str(root),
        }
    )
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    pack = commands.add_parser("pack", help="create a deterministic snapshot")
    pack.add_argument("--root", type=Path, default=ROOT)
    pack.add_argument("--output-dir", type=Path, required=True)
    pack.add_argument("--source-sha", required=True)
    pack.add_argument(
        "--max-archive-bytes",
        type=int,
        default=DEFAULT_MAX_ARCHIVE_BYTES,
    )

    verify = commands.add_parser("verify", help="verify a snapshot")
    verify.add_argument("--archive", type=Path, required=True)
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--extract-root", type=Path)
    verify.add_argument(
        "--max-archive-bytes",
        type=int,
        default=DEFAULT_MAX_ARCHIVE_BYTES,
    )

    pull = commands.add_parser(
        "pull",
        help="restore a private GitHub Release snapshot",
    )
    pull.add_argument("--repository", required=True)
    pull.add_argument("--root", type=Path, default=ROOT)
    pull.add_argument("--release-tag")
    pull.add_argument("--replace", action="store_true", required=True)
    pull.add_argument(
        "--max-archive-bytes",
        type=int,
        default=DEFAULT_MAX_ARCHIVE_BYTES,
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "pack":
            result = pack_snapshot(
                root=args.root,
                output_dir=args.output_dir,
                source_sha=args.source_sha,
                max_archive_bytes=args.max_archive_bytes,
            )
        elif args.command == "verify":
            result = verify_snapshot(
                archive_path=args.archive,
                manifest_path=args.manifest,
                max_archive_bytes=args.max_archive_bytes,
                extract_root=args.extract_root,
            )
        else:
            result = pull_snapshot(
                repository=args.repository,
                root=args.root,
                release_tag=args.release_tag,
                replace=args.replace,
                max_archive_bytes=args.max_archive_bytes,
            )
    except (SnapshotError, OSError) as error:
        print(
            json.dumps(
                {"command": args.command, "error": str(error), "ok": False},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    result.update({"command": args.command, "ok": True})
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
