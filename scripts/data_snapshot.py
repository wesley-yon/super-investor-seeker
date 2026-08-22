#!/usr/bin/env python3
"""Create, verify, and restore private data snapshots.

The snapshot format is deliberately small and strict.  It contains the whole
``data/`` tree and only the durable cache files named in ``CACHE_FILES``.  The
sidecar manifest is the integrity boundary used by both local restores and
GitHub Actions.
"""

from __future__ import annotations

import argparse
import contextlib
try:
    import fcntl
except ImportError:  # pragma: no cover - pack_snapshot fails closed below.
    fcntl = None  # type: ignore[assignment]
import gzip
import hashlib
import http.client
import json
import os
import re
import secrets
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
from typing import Any, BinaryIO, Iterable, Iterator, Optional


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_VERSION = 1
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
    Path(".cache/company_tickers_mf.json"),
    Path(".cache/cusip_map.json"),
    Path(".cache/cusip_registry.json"),
    Path(".cache/openfigi_details.json"),
    Path(".cache/sec_fund_names.json"),
)
PRIVATE_INSIDER_PREFIX = "data/insiders/private"


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
            entries.append(
                SourceEntry(
                    name=relative,
                    path=path,
                    is_dir=False,
                    size=metadata.st_size,
                )
            )

    for relative in CACHE_FILES:
        path = root / relative
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
    with path.open("rb") as handle:
        return _sha256_handle(handle)


def _sha256_handle(handle: BinaryIO) -> str:
    digest = hashlib.sha256()
    handle.seek(0)
    for block in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(block)
    return digest.hexdigest()


def _archive_mode(name: str, *, is_dir: bool) -> int:
    is_private_insider = (
        name == PRIVATE_INSIDER_PREFIX
        or name.startswith(f"{PRIVATE_INSIDER_PREFIX}/")
    )
    if is_private_insider:
        return 0o700 if is_dir else 0o600
    return 0o755 if is_dir else 0o644


def _tar_info(entry: SourceEntry) -> tarfile.TarInfo:
    info = tarfile.TarInfo(entry.name)
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    info.mode = _archive_mode(entry.name, is_dir=entry.is_dir)
    if entry.is_dir:
        info.type = tarfile.DIRTYPE
        info.size = 0
    else:
        info.type = tarfile.REGTYPE
        info.size = entry.size
    return info


def _write_archive(entries: Iterable[SourceEntry], destination: Path | int) -> None:
    if isinstance(destination, int):
        raw_stream = os.fdopen(os.dup(destination), "wb")
    else:
        raw_stream = destination.open("xb")
    with raw_stream as raw_output:
        os.fchmod(raw_output.fileno(), 0o600)
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


def _fsync_regular_file(path: Path, label: str) -> None:
    """Flush a staged regular file before making its name visible."""
    _regular_file(path, label)
    try:
        with path.open("rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise SnapshotError(f"{label} must be a regular file: {path}")
            os.fsync(handle.fileno())
    except SnapshotError:
        raise
    except OSError as error:
        raise SnapshotError(f"could not fsync {label}: {path}: {error}") from error


def _fsync_directory(path: Path, label: str) -> None:
    """Flush directory entries after a transactional publication change."""
    _regular_directory(path, label)
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError as error:
        raise SnapshotError(f"could not open {label}: {path}: {error}") from error
    try:
        if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise SnapshotError(f"{label} must be a real directory: {path}")
        os.fsync(descriptor)
    except SnapshotError:
        raise
    except OSError as error:
        raise SnapshotError(f"could not fsync {label}: {path}: {error}") from error
    finally:
        os.close(descriptor)


def _fsync_containing_directory(path: Path, label: str) -> None:
    """Flush the real directory that contains a possibly aliased path."""

    try:
        parent = path.parent.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise SnapshotError(
            f"could not resolve {label}: {path.parent}: {error}"
        ) from error
    _fsync_directory(parent, label)


def _open_directory_at(
    parent_descriptor: int,
    name: str,
    label: str,
) -> int:
    """Open one child directory without following a replacement symlink."""

    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=parent_descriptor,
        )
    except OSError as error:
        raise SnapshotError(f"could not securely open {label}: {name}: {error}") from error
    try:
        _directory_descriptor_identity(descriptor, label)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _ensure_durable_directory(path: Path, label: str) -> None:
    """Create a canonical directory tree with descriptor-relative entries."""

    if not _directory_fd_capabilities_available():
        raise SnapshotError("secure directory-descriptor publication is unavailable")
    missing: list[Path] = []
    current = path
    while True:
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            if current.parent == current:
                raise SnapshotError(
                    f"{label} has no existing directory ancestor"
                ) from None
            missing.append(current)
            current = current.parent
            continue
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise SnapshotError(f"{label} must be a real directory: {current}")
        break

    # `path` is the canonical target captured before this helper runs. Freeze
    # the deepest existing ancestor and make every later name lookup relative
    # to its verified descriptor, never to a mutable caller alias.
    ancestor_identity = _directory_identity(current, f"{label} ancestor")
    try:
        parent_descriptor = os.open(
            current,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
    except OSError as error:
        raise SnapshotError(
            f"could not securely open {label} ancestor: {current}: {error}"
        ) from error
    try:
        if (
            _directory_descriptor_identity(
                parent_descriptor,
                f"{label} ancestor",
            )
            != ancestor_identity
        ):
            raise SnapshotError(f"{label} ancestor changed while preparing snapshot")
        if not missing:
            # Persist the existing output directory's entry before publication.
            _fsync_containing_directory(path, f"{label} parent")
            return

        for directory in reversed(missing):
            name = directory.name
            created = False
            try:
                os.mkdir(name, mode=0o700, dir_fd=parent_descriptor)
                created = True
            except FileExistsError:
                pass
            except OSError as error:
                raise SnapshotError(
                    f"could not create {label}: {directory}: {error}"
                ) from error

            child_descriptor = _open_directory_at(
                parent_descriptor,
                name,
                label,
            )
            try:
                if created:
                    try:
                        os.fchmod(child_descriptor, 0o700)
                    except OSError as error:
                        raise SnapshotError(
                            f"could not restrict {label}: {directory}: {error}"
                        ) from error
                child_metadata = os.fstat(child_descriptor)
                if (
                    not stat.S_ISDIR(child_metadata.st_mode)
                    or stat.S_IMODE(child_metadata.st_mode) != 0o700
                ):
                    raise SnapshotError(
                        f"{label} must be a private real directory: {directory}"
                    )
                # The child contents/mode are durable before its parent names it.
                _fsync_directory_at(child_descriptor, label)
                _fsync_directory_at(parent_descriptor, f"{label} parent")
            except BaseException:
                os.close(child_descriptor)
                raise
            os.close(parent_descriptor)
            parent_descriptor = child_descriptor
    finally:
        os.close(parent_descriptor)


def _path_is_at_or_below(candidate: Path, roots: tuple[Path, ...]) -> bool:
    """Compare paths lexically and by existing directory identity."""

    if candidate in roots or any(parent in roots for parent in candidate.parents):
        return True
    for ancestor in (candidate, *candidate.parents):
        for root in roots:
            try:
                if ancestor.samefile(root):
                    return True
            except FileNotFoundError:
                continue
            except OSError as error:
                raise SnapshotError(
                    f"could not validate output directory scope: {ancestor}: {error}"
                ) from error
    return False


def _existing_regular_file(path: Path, label: str) -> bool:
    """Return whether a final name is absent or a safe regular file."""
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SnapshotError(f"{label} must be a regular file: {path}")
    return True


def _directory_fd_capabilities_available() -> bool:
    """Return whether this host can perform the descriptor-anchored transaction."""

    return (
        fcntl is not None
        and hasattr(os, "O_DIRECTORY")
        and hasattr(os, "O_NOFOLLOW")
        and os.open in os.supports_dir_fd
        and os.mkdir in os.supports_dir_fd
        and os.stat in os.supports_dir_fd
        and os.unlink in os.supports_dir_fd
    )


def _directory_identity(path: Path, label: str) -> tuple[int, int]:
    """Return the identity of a real directory without following its final name."""

    try:
        metadata = os.stat(path, follow_symlinks=False)
    except OSError as error:
        raise SnapshotError(f"could not inspect {label}: {path}: {error}") from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise SnapshotError(f"{label} must be a real directory: {path}")
    return metadata.st_dev, metadata.st_ino


def _directory_descriptor_identity(descriptor: int, label: str) -> tuple[int, int]:
    """Return the identity of an opened real directory."""

    try:
        metadata = os.fstat(descriptor)
    except OSError as error:
        raise SnapshotError(f"could not inspect {label}: {error}") from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise SnapshotError(f"{label} must be a real directory")
    return metadata.st_dev, metadata.st_ino


def _open_verified_output_directory(path: Path, label: str) -> int:
    """Open the final output component without following a replacement symlink."""

    if not _directory_fd_capabilities_available():
        raise SnapshotError("secure directory-descriptor publication is unavailable")
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
    except OSError as error:
        raise SnapshotError(f"could not securely open {label}: {path}: {error}") from error
    try:
        _directory_descriptor_identity(descriptor, label)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


@contextlib.contextmanager
def _locked_output_directory(descriptor: int) -> Iterator[None]:
    """Serialize cooperating publishers without creating a lock-file artifact."""

    try:
        # flock on a directory fd is supported by the macOS and Linux hosts
        # that provide the descriptor primitives required above.
        fcntl.flock(descriptor, fcntl.LOCK_EX)  # type: ignore[union-attr]
    except (AttributeError, OSError) as error:
        raise SnapshotError(f"could not lock output directory: {error}") from error
    try:
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)  # type: ignore[union-attr]
        except (AttributeError, OSError):
            pass


def _revalidate_output_directory(path: Path, descriptor: int) -> None:
    """Reject a final path that no longer names the verified directory."""

    try:
        named = os.stat(path, follow_symlinks=False)
        opened = os.fstat(descriptor)
    except OSError as error:
        raise SnapshotError(f"could not revalidate output directory: {path}: {error}") from error
    if (
        not stat.S_ISDIR(named.st_mode)
        or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
    ):
        raise SnapshotError("output directory changed while preparing snapshot")


def _open_regular_file_at(
    directory_descriptor: int,
    name: str,
    label: str,
) -> tuple[int, os.stat_result]:
    """Open one regular directory member without following a symlink."""

    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=directory_descriptor,
        )
    except OSError as error:
        raise SnapshotError(f"{label} must be a regular file: {name}") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise SnapshotError(f"{label} must be a regular file: {name}")
        return descriptor, metadata
    except BaseException:
        os.close(descriptor)
        raise


def _existing_regular_file_at(
    directory_descriptor: int,
    name: str,
    label: str,
) -> bool:
    """Return whether a descriptor-relative final name is absent or regular."""

    try:
        metadata = os.stat(
            name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return False
    except OSError as error:
        raise SnapshotError(f"could not inspect {label}: {name}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SnapshotError(f"{label} must be a regular file: {name}")
    return True


def _create_staged_file_at(directory_descriptor: int, suffix: str) -> tuple[str, int]:
    """Create a private staging member by name relative to the verified fd."""

    for _ in range(100):
        name = f".data-snapshot-{secrets.token_hex(16)}{suffix}"
        try:
            descriptor = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory_descriptor,
            )
        except FileExistsError:
            continue
        except OSError as error:
            raise SnapshotError(f"could not create staged snapshot member: {error}") from error
        return name, descriptor
    raise SnapshotError("could not allocate a unique staged snapshot member")


def _fsync_regular_file_at(directory_descriptor: int, name: str, label: str) -> None:
    """Flush a no-follow staged regular file before publishing its name."""

    descriptor, _ = _open_regular_file_at(directory_descriptor, name, label)
    try:
        os.fsync(descriptor)
    except OSError as error:
        raise SnapshotError(f"could not fsync {label}: {name}: {error}") from error
    finally:
        os.close(descriptor)


def _fsync_directory_at(directory_descriptor: int, label: str) -> None:
    """Flush the verified output directory after a descriptor-relative change."""

    if not stat.S_ISDIR(os.fstat(directory_descriptor).st_mode):
        raise SnapshotError(f"{label} must be a real directory")
    try:
        os.fsync(directory_descriptor)
    except OSError as error:
        raise SnapshotError(f"could not fsync {label}: {error}") from error


def _replace_at(directory_descriptor: int, source_name: str, target_name: str) -> None:
    """Rename a staged member without resolving either name through a path."""

    try:
        os.replace(
            source_name,
            target_name,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
        )
    except (NotImplementedError, TypeError) as error:
        raise SnapshotError(
            "secure directory-descriptor publication is unavailable"
        ) from error
    except OSError as error:
        raise SnapshotError(f"could not publish snapshot member: {error}") from error


def _unlink_at(directory_descriptor: int, name: str) -> None:
    """Unlink only the named entry in the verified output directory."""

    try:
        os.unlink(name, dir_fd=directory_descriptor)
    except FileNotFoundError:
        return
    except OSError as error:
        raise SnapshotError(f"could not remove snapshot member: {name}: {error}") from error


def _manifest_names(dataset_sha256: str) -> tuple[str, str]:
    archive_name = f"{ARCHIVE_PREFIX}{dataset_sha256}{ARCHIVE_SUFFIX}"
    manifest_name = f"{ARCHIVE_PREFIX}{dataset_sha256}{MANIFEST_SUFFIX}"
    return archive_name, manifest_name


def _manifest_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    dataset = manifest["dataset"]
    archive = manifest["archive"]
    return {
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


def _parse_manifest(serialized: str, manifest_name: str) -> dict[str, Any]:
    try:
        parsed = json.loads(serialized)
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
    if manifest["contract_version"] != CONTRACT_VERSION:
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
    if manifest_name != expected_manifest_name:
        raise SnapshotError(
            "manifest sidecar filename does not match dataset sha256"
        )
    return manifest


def _load_manifest(path: Path) -> dict[str, Any]:
    metadata = _regular_file(path, "manifest")
    if metadata.st_size > MAX_MANIFEST_BYTES:
        raise SnapshotError("manifest is too large")
    try:
        serialized = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as error:
        raise SnapshotError(f"manifest is not valid UTF-8 JSON: {error}") from error
    return _parse_manifest(serialized, path.name)


def _load_manifest_at(directory_descriptor: int, name: str) -> dict[str, Any]:
    descriptor, metadata = _open_regular_file_at(
        directory_descriptor,
        name,
        "manifest",
    )
    try:
        if metadata.st_size > MAX_MANIFEST_BYTES:
            raise SnapshotError("manifest is too large")
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            serialized = handle.read(MAX_MANIFEST_BYTES + 1)
        if len(serialized.encode("utf-8")) > MAX_MANIFEST_BYTES:
            raise SnapshotError("manifest is too large")
    except UnicodeDecodeError as error:
        raise SnapshotError(f"manifest is not valid UTF-8 JSON: {error}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return _parse_manifest(serialized, name)


def _validate_member_name(name: str) -> None:
    if not name or name.startswith("/") or "\\" in name:
        raise SnapshotError(f"unsafe archive path: {name!r}")
    parts = name.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise SnapshotError(f"unsafe archive path: {name!r}")
    normalized = PurePosixPath(name).as_posix()
    if normalized != name:
        raise SnapshotError(f"unsafe archive path: {name!r}")


def _validate_member_scope(member: tarfile.TarInfo) -> None:
    name = member.name
    _validate_member_name(name)
    cache_names = {path.as_posix() for path in CACHE_FILES}
    if name == "data":
        if not member.isdir():
            raise SnapshotError("archive data root must be a directory")
    elif name.startswith("data/"):
        pass
    elif name == ".cache":
        if not member.isdir():
            raise SnapshotError("archive cache root must be a directory")
    elif name in cache_names:
        if not member.isfile():
            raise SnapshotError(f"cache archive member must be a file: {name}")
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
    archive_source: Path | BinaryIO,
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
        if isinstance(archive_source, Path):
            archive_context = tarfile.open(archive_source, mode="r:gz")
        else:
            archive_source.seek(0)
            archive_context = tarfile.open(fileobj=archive_source, mode="r:gz")
        with archive_context as archive:
            for member in archive:
                _validate_member_scope(member)
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
                if extract_root is None:
                    with source:
                        _copy_exact(source, None, digest, member.size)
                else:
                    destination = extract_root.joinpath(*member.name.split("/"))
                    with source, destination.open("xb") as output:
                        _copy_exact(source, output, digest, member.size)
                    destination.chmod(expected_mode)
                    os.utime(destination, (0, 0))
    except SnapshotError:
        raise
    except (tarfile.TarError, EOFError, OSError) as error:
        raise SnapshotError(f"archive cannot be read: {error}") from error

    required_names = {".cache", "data"} | {
        path.as_posix() for path in CACHE_FILES
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
    if stat.S_IMODE(archive_metadata.st_mode) != 0o600:
        raise SnapshotError("archive mode must be exactly 0600")
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


def _verify_snapshot_at(
    *,
    directory_descriptor: int,
    archive_name: str,
    manifest_name: str,
    max_archive_bytes: int,
) -> dict[str, Any]:
    """Verify a pair using only names resolved from the verified directory fd."""

    archive_descriptor, archive_metadata = _open_regular_file_at(
        directory_descriptor,
        archive_name,
        "archive",
    )
    try:
        if stat.S_IMODE(archive_metadata.st_mode) != 0o600:
            raise SnapshotError("archive mode must be exactly 0600")
        manifest = _load_manifest_at(directory_descriptor, manifest_name)
        if archive_name != manifest["archive"]["filename"]:
            raise SnapshotError("archive filename does not match manifest")
        if archive_metadata.st_size > max_archive_bytes:
            raise SnapshotError(
                f"archive is too large: {archive_metadata.st_size} > "
                f"{max_archive_bytes} bytes"
            )
        if archive_metadata.st_size != manifest["archive"]["bytes"]:
            raise SnapshotError("archive byte count does not match manifest")
        with os.fdopen(archive_descriptor, "rb") as archive:
            archive_descriptor = -1
            if _sha256_handle(archive) != manifest["archive"]["sha256"]:
                raise SnapshotError("archive checksum does not match manifest")
            _verify_archive_contents(archive, manifest, None)
        return manifest
    finally:
        if archive_descriptor >= 0:
            os.close(archive_descriptor)


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
    try:
        output_metadata = output_dir.lstat()
    except FileNotFoundError:
        pass
    except OSError as error:
        raise SnapshotError(
            f"could not inspect output directory: {output_dir}: {error}"
        ) from error
    else:
        if stat.S_ISLNK(output_metadata.st_mode):
            raise SnapshotError(f"output directory must be a real directory: {output_dir}")
    data_root = root / "data"
    cache_root = root / ".cache"
    try:
        # Canonicalize only the parent. The final component is intentionally
        # appended without resolution so a symlink inserted after the initial
        # lstat is rejected by descriptor-anchored no-follow traversal rather
        # than followed into an arbitrary target.
        resolved_output_parent = output_dir.parent.resolve(strict=False)
        resolved_output_dir = resolved_output_parent / output_dir.name
    except (OSError, RuntimeError) as error:
        raise SnapshotError(
            f"could not resolve output directory: {output_dir}: {error}"
        ) from error
    protected_roots = (data_root, cache_root)
    if any(
        _path_is_at_or_below(candidate, protected_roots)
        for candidate in (output_dir, resolved_output_dir)
    ):
        raise SnapshotError("output directory cannot be inside snapshot data")
    _ensure_durable_directory(resolved_output_dir, "output directory")
    try:
        # Resolve the mutable raw alias without requiring its final name to
        # exist: canonical creation may have intentionally left a retargeted
        # alias without that final component.
        prepared_output_dir = output_dir.resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise SnapshotError(
            f"could not resolve prepared output directory: {output_dir}: {error}"
        ) from error
    if prepared_output_dir != resolved_output_dir:
        if _path_is_at_or_below(prepared_output_dir, protected_roots):
            raise SnapshotError("output directory cannot be inside snapshot data")
        raise SnapshotError("output directory changed while preparing snapshot")
    prepared_output_identity = _directory_identity(
        prepared_output_dir,
        "prepared output directory",
    )

    entries = _scan_source(root)
    dataset_sha256, file_count, content_bytes = _source_content_digest(entries)
    insider_files = [
        entry
        for entry in entries
        if not entry.is_dir
        and entry.name.startswith(f"{PRIVATE_INSIDER_PREFIX}/")
    ]
    insider_file_count = len(insider_files)
    insider_content_bytes = sum(entry.size for entry in insider_files)
    dataset_id = dataset_sha256
    archive_name, manifest_name = _manifest_names(dataset_sha256)
    archive_path = output_dir / archive_name
    manifest_path = output_dir / manifest_name
    output_descriptor = _open_verified_output_directory(
        prepared_output_dir,
        "output directory",
    )
    try:
        if (
            _directory_descriptor_identity(output_descriptor, "output directory")
            != prepared_output_identity
        ):
            raise SnapshotError("output directory changed while preparing snapshot")
        with _locked_output_directory(output_descriptor):
            _revalidate_output_directory(output_dir, output_descriptor)
            archive_exists = _existing_regular_file_at(
                output_descriptor,
                archive_name,
                "snapshot archive",
            )
            manifest_exists = _existing_regular_file_at(
                output_descriptor,
                manifest_name,
                "snapshot manifest",
            )

            if archive_exists and manifest_exists:
                # Content-addressed names make a complete pair immutable:
                # adopt it only when it is the exact requested transaction.
                manifest = _verify_snapshot_at(
                    directory_descriptor=output_descriptor,
                    archive_name=archive_name,
                    manifest_name=manifest_name,
                    max_archive_bytes=max_archive_bytes,
                )
                if manifest["source_sha"] != source_sha or (
                    created_at is not None and manifest["created_at"] != created_at
                ):
                    raise SnapshotError("completed snapshot pair does not match request")
                _fsync_directory_at(output_descriptor, "output directory")
                _revalidate_output_directory(output_dir, output_descriptor)
            else:
                if archive_exists:
                    _unlink_at(output_descriptor, archive_name)
                if manifest_exists:
                    _unlink_at(output_descriptor, manifest_name)
                if archive_exists or manifest_exists:
                    # Persist the empty pair state before rebuilding it after a crash.
                    _fsync_directory_at(output_descriptor, "output directory")

                temporary_archive: Optional[str] = None
                temporary_manifest: Optional[str] = None
                published_archive = False
                published_manifest = False
                try:
                    temporary_archive, archive_descriptor = _create_staged_file_at(
                        output_descriptor,
                        ARCHIVE_SUFFIX,
                    )
                    try:
                        _write_archive(entries, archive_descriptor)
                    finally:
                        os.close(archive_descriptor)
                    _fsync_regular_file_at(
                        output_descriptor,
                        temporary_archive,
                        "staged archive",
                    )
                    staged_descriptor, staged_metadata = _open_regular_file_at(
                        output_descriptor,
                        temporary_archive,
                        "staged archive",
                    )
                    try:
                        archive_bytes = staged_metadata.st_size
                        with os.fdopen(staged_descriptor, "rb") as staged_archive:
                            staged_descriptor = -1
                            archive_sha256 = _sha256_handle(staged_archive)
                    finally:
                        if staged_descriptor >= 0:
                            os.close(staged_descriptor)
                    if archive_bytes > max_archive_bytes:
                        raise SnapshotError(
                            f"archive is too large: {archive_bytes} > "
                            f"{max_archive_bytes} bytes"
                        )
                    manifest = {
                        "archive": {
                            "bytes": archive_bytes,
                            "filename": archive_name,
                            "sha256": archive_sha256,
                        },
                        "contract_version": CONTRACT_VERSION,
                        "created_at": created_at
                        or datetime.now(timezone.utc)
                        .replace(microsecond=0)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "dataset": {
                            "bytes": content_bytes,
                            "file_count": file_count,
                            "sha256": dataset_sha256,
                        },
                        "dataset_id": dataset_id,
                        "source_sha": source_sha,
                    }
                    temporary_manifest, manifest_descriptor = _create_staged_file_at(
                        output_descriptor,
                        MANIFEST_SUFFIX,
                    )
                    with os.fdopen(
                        manifest_descriptor,
                        "w",
                        encoding="utf-8",
                    ) as temporary:
                        manifest_descriptor = -1
                        json.dump(
                            manifest,
                            temporary,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        temporary.write("\n")
                    _fsync_regular_file_at(
                        output_descriptor,
                        temporary_manifest,
                        "staged manifest",
                    )

                    # Each name becomes durable before publishing the next member.
                    _replace_at(output_descriptor, temporary_archive, archive_name)
                    temporary_archive = None
                    published_archive = True
                    _fsync_directory_at(output_descriptor, "output directory")
                    _replace_at(output_descriptor, temporary_manifest, manifest_name)
                    temporary_manifest = None
                    published_manifest = True
                    _fsync_directory_at(output_descriptor, "output directory")
                    manifest = _verify_snapshot_at(
                        directory_descriptor=output_descriptor,
                        archive_name=archive_name,
                        manifest_name=manifest_name,
                        max_archive_bytes=max_archive_bytes,
                    )
                    _revalidate_output_directory(output_dir, output_descriptor)
                except Exception:
                    # Normal failures remove only names relative to the verified
                    # descriptor. BaseException retains a recoverable partial
                    # pair for the next call, as before.
                    if published_archive:
                        _unlink_at(output_descriptor, archive_name)
                    if published_manifest:
                        _unlink_at(output_descriptor, manifest_name)
                    if published_archive or published_manifest:
                        _fsync_directory_at(output_descriptor, "output directory")
                    raise
                finally:
                    if temporary_archive is not None:
                        _unlink_at(output_descriptor, temporary_archive)
                    if temporary_manifest is not None:
                        _unlink_at(output_descriptor, temporary_manifest)
    finally:
        os.close(output_descriptor)

    summary = _manifest_summary(manifest)
    summary.update(
        {
            "archive_path": str(archive_path),
            "insider_content_bytes": insider_content_bytes,
            "insider_file_count": insider_file_count,
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


def _validate_restore_targets(root: Path) -> None:
    data_target = root / "data"
    if data_target.exists() or data_target.is_symlink():
        _regular_directory(data_target, "existing data target")
    cache_root = root / ".cache"
    if cache_root.exists() or cache_root.is_symlink():
        _regular_directory(cache_root, "existing cache target")
    for relative in CACHE_FILES:
        target = root / relative
        if target.exists() or target.is_symlink():
            _regular_file(target, "existing cache target")


def _replace_payload(root: Path, payload: Path) -> None:
    """Transactionally replace data and only the allowlisted cache files."""
    _validate_restore_targets(root)
    _regular_directory(payload / "data", "extracted data directory")
    _regular_directory(payload / ".cache", "extracted cache directory")
    for relative in CACHE_FILES:
        _regular_file(payload / relative, "extracted cache file")

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
        for relative in CACHE_FILES:
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
        for relative in CACHE_FILES:
            target = root / relative
            os.replace(payload / relative, target)
            installed.append(target)
    except Exception as error:
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
        raise SnapshotError(f"snapshot restore failed: {error}{details}") from error


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
        _replace_payload(root, payload)

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
