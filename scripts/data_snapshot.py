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
import stat
import sys
import tarfile
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
# Extraction ceilings are trusted local policy, never inferred from a manifest.
# They retain headroom for the known 4.62GB / 65,000-file production snapshot.
DEFAULT_MAX_CONTENT_BYTES = 8 * 1024 * 1024 * 1024
DEFAULT_MAX_FILE_COUNT = 100_000
DEFAULT_MAX_MEMBER_COUNT = 200_000
DEFAULT_MAX_PATH_COMPONENTS = 64
MAX_MANIFEST_BYTES = 1_000_000
MAX_API_RESPONSE_BYTES = 10_000_000
RELEASES_PER_PAGE = 100
MAX_RELEASE_LIST_PAGES = 100
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
ASSET_DIGEST_RE = re.compile(r"sha256:([0-9a-f]{64})")
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
RESTORE_TRANSACTION_NAME = ".data-snapshot-restore"
RESTORE_PREPARE_NAME = ".data-snapshot-restore.prepare"
RESTORE_CLEANUP_NAME = ".data-snapshot-restore.cleanup"
RESTORE_STATE_NAME = "state.json"
RESTORE_STATE_TEMP_NAME = ".state.json.tmp"
RESTORE_CONTRACT_VERSION = 1
MAX_RESTORE_STATE_BYTES = 16_384


class SnapshotError(ValueError):
    """Raised when a snapshot fails a closed validation rule."""


@dataclass(frozen=True)
class SourceIdentity:
    """The immutable scan-time identity required for a source component."""

    device: int
    inode: int
    file_type: int
    size: int
    mode: int


@dataclass(frozen=True)
class SourceComponent:
    name: str
    identity: SourceIdentity


@dataclass(frozen=True)
class SourceEntry:
    name: str
    path: Path
    is_dir: bool
    size: int
    root: Path
    root_identity: SourceIdentity
    components: tuple[SourceComponent, ...]


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


def _validate_extraction_limits(
    *,
    max_content_bytes: int,
    max_file_count: int,
    max_member_count: int,
    max_path_components: int,
) -> None:
    """Reject invalid trusted extraction policy before reading an archive."""

    for value, label in (
        (max_content_bytes, "maximum content bytes"),
        (max_file_count, "maximum file count"),
        (max_member_count, "maximum member count"),
        (max_path_components, "maximum path components"),
    ):
        if type(value) is not int or value < 1:
            raise SnapshotError(f"{label} must be a positive integer")


def _preflight_manifest_extraction_limits(
    manifest: dict[str, Any],
    *,
    max_content_bytes: int,
    max_file_count: int,
) -> None:
    """Fail closed on manifest expansion claims before gzip/tar traversal."""

    dataset = manifest["dataset"]
    if dataset["bytes"] > max_content_bytes:
        raise SnapshotError("manifest exceeds maximum content bytes")
    if dataset["file_count"] > max_file_count:
        raise SnapshotError("manifest exceeds maximum file count")


def _source_identity(metadata: os.stat_result) -> SourceIdentity:
    return SourceIdentity(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        file_type=stat.S_IFMT(metadata.st_mode),
        size=metadata.st_size,
        mode=stat.S_IMODE(metadata.st_mode),
    )


def _source_entry(
    *,
    root: Path,
    root_identity: SourceIdentity,
    path: Path,
    is_dir: bool,
    metadata: os.stat_result,
) -> SourceEntry:
    """Capture each no-follow component identity without retaining descriptors."""

    components: list[SourceComponent] = []
    current = root
    relative = path.relative_to(root)
    for index, component in enumerate(relative.parts):
        current /= component
        component_metadata = (
            metadata
            if index == len(relative.parts) - 1
            else os.stat(current, follow_symlinks=False)
        )
        components.append(
            SourceComponent(component, _source_identity(component_metadata))
        )
    return SourceEntry(
        name=relative.as_posix(),
        path=path,
        is_dir=is_dir,
        size=metadata.st_size if not is_dir else 0,
        root=root,
        root_identity=root_identity,
        components=tuple(components),
    )


def _scan_source(root: Path) -> list[SourceEntry]:
    root = root.resolve()
    data_root = root / "data"
    cache_root = root / ".cache"
    root_identity = _source_identity(_regular_directory(root, "snapshot root"))
    data_metadata = _regular_directory(data_root, "data directory")
    cache_metadata = _regular_directory(cache_root, "cache directory")

    entries = [
        _source_entry(
            root=root,
            root_identity=root_identity,
            path=cache_root,
            is_dir=True,
            metadata=cache_metadata,
        ),
        _source_entry(
            root=root,
            root_identity=root_identity,
            path=data_root,
            is_dir=True,
            metadata=data_metadata,
        ),
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
            entries.append(
                _source_entry(
                    root=root,
                    root_identity=root_identity,
                    path=path,
                    is_dir=True,
                    metadata=metadata,
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
            entries.append(
                _source_entry(
                    root=root,
                    root_identity=root_identity,
                    path=path,
                    is_dir=False,
                    metadata=metadata,
                )
            )

    for relative in CACHE_FILES:
        path = root / relative
        metadata = _regular_file(path, "required cache file")
        entries.append(
            _source_entry(
                root=root,
                root_identity=root_identity,
                path=path,
                is_dir=False,
                metadata=metadata,
            )
        )

    entries.sort(key=lambda entry: entry.name)
    names = [entry.name for entry in entries]
    if len(names) != len(set(names)):
        raise SnapshotError("source contains duplicate archive paths")
    return entries


def _open_verified_source_entry(entry: SourceEntry) -> int:
    """Open one scanned entry through checked no-follow component descriptors."""

    if not _directory_fd_capabilities_available():
        raise SnapshotError("secure source traversal is unavailable")
    try:
        descriptor = os.open(
            entry.root,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
    except OSError as error:
        raise SnapshotError(f"could not securely open snapshot root: {error}") from error
    try:
        if _source_identity(os.fstat(descriptor)) != entry.root_identity:
            raise SnapshotError("snapshot root changed after source scan")
        for index, component in enumerate(entry.components):
            is_final = index == len(entry.components) - 1
            flags = os.O_RDONLY | os.O_NOFOLLOW
            if not is_final or entry.is_dir:
                flags |= os.O_DIRECTORY
            child = os.open(component.name, flags, dir_fd=descriptor)
            try:
                if _source_identity(os.fstat(child)) != component.identity:
                    raise SnapshotError(
                        f"source changed after scan: {entry.name}"
                    )
            except BaseException:
                os.close(child)
                raise
            os.close(descriptor)
            descriptor = child
        return descriptor
    except SnapshotError:
        os.close(descriptor)
        raise
    except OSError as error:
        os.close(descriptor)
        raise SnapshotError(f"source changed after scan: {entry.name}") from error


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
        descriptor = _open_verified_source_entry(entry)
        os.close(descriptor)
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
        descriptor = _open_verified_source_entry(entry)
        with os.fdopen(descriptor, "rb") as handle:
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
                    descriptor = _open_verified_source_entry(entry)
                    try:
                        info = _tar_info(entry)
                        if entry.is_dir:
                            archive.addfile(info)
                        else:
                            with os.fdopen(descriptor, "rb") as source:
                                descriptor = -1
                                archive.addfile(info, source)
                    finally:
                        if descriptor >= 0:
                            os.close(descriptor)


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
        and os.rmdir in os.supports_dir_fd
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


def _create_private_directory_at(
    parent_descriptor: int,
    prefix: str,
    label: str,
) -> tuple[str, int]:
    """Create and retain a random owner-only sibling directory capability."""

    for _ in range(100):
        name = f"{prefix}{secrets.token_hex(16)}"
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_descriptor)
        except FileExistsError:
            continue
        except OSError as error:
            raise SnapshotError(f"could not create {label}: {error}") from error
        try:
            descriptor = _open_directory_at(parent_descriptor, name, label)
            os.fchmod(descriptor, 0o700)
            _fsync_directory_at(descriptor, label)
            _fsync_directory_at(parent_descriptor, f"{label} parent")
            return name, descriptor
        except BaseException:
            try:
                os.rmdir(name, dir_fd=parent_descriptor)
            except OSError:
                pass
            raise
    raise SnapshotError(f"could not allocate a unique {label}")


def _remove_tree_contents_at(directory_descriptor: int, label: str) -> None:
    """Remove bounded staging contents using only retained directory handles."""

    try:
        entries = list(os.scandir(directory_descriptor))
    except OSError as error:
        raise SnapshotError(f"could not enumerate {label}: {error}") from error
    for entry in entries:
        try:
            metadata = os.stat(
                entry.name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            if stat.S_ISDIR(metadata.st_mode):
                child = _open_directory_at(directory_descriptor, entry.name, label)
                try:
                    _remove_tree_contents_at(child, label)
                    _fsync_directory_at(child, label)
                finally:
                    os.close(child)
                os.rmdir(entry.name, dir_fd=directory_descriptor)
            elif stat.S_ISREG(metadata.st_mode):
                os.unlink(entry.name, dir_fd=directory_descriptor)
            else:
                raise SnapshotError(f"{label} contains an unsupported entry: {entry.name}")
        except SnapshotError:
            raise
        except OSError as error:
            raise SnapshotError(f"could not remove {label} entry {entry.name}: {error}") from error
    _fsync_directory_at(directory_descriptor, label)


def _remove_tree_at(parent_descriptor: int, name: str, label: str) -> None:
    """Remove one real directory entry relative to its retained parent fd."""

    try:
        child = _open_directory_at(parent_descriptor, name, label)
    except SnapshotError as error:
        try:
            os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            return
        raise error
    try:
        _remove_tree_contents_at(child, label)
    finally:
        os.close(child)
    try:
        os.rmdir(name, dir_fd=parent_descriptor)
        _fsync_directory_at(parent_descriptor, f"{label} parent")
    except OSError as error:
        raise SnapshotError(f"could not remove {label}: {error}") from error


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


def _validate_member_name(name: str, max_path_components: int) -> None:
    if not name or name.startswith("/") or "\\" in name:
        raise SnapshotError(f"unsafe archive path: {name!r}")
    parts = name.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise SnapshotError(f"unsafe archive path: {name!r}")
    if len(parts) > max_path_components:
        raise SnapshotError("archive member exceeds maximum path components")
    normalized = PurePosixPath(name).as_posix()
    if normalized != name:
        raise SnapshotError(f"unsafe archive path: {name!r}")


def _validate_member_scope(member: tarfile.TarInfo, max_path_components: int) -> None:
    name = member.name
    _validate_member_name(name, max_path_components)
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


def _open_archive_member_parent_at(
    root_descriptor: int,
    member_name: str,
    label: str,
) -> tuple[int, str]:
    """Resolve an already-validated archive parent through retained fds only."""

    descriptor = os.dup(root_descriptor)
    try:
        parts = member_name.split("/")
        for component in parts[:-1]:
            child = _open_directory_at(descriptor, component, label)
            os.close(descriptor)
            descriptor = child
        return descriptor, parts[-1]
    except BaseException:
        os.close(descriptor)
        raise


def _extract_archive_directory_at(
    root_descriptor: int,
    member_name: str,
    mode: int,
) -> None:
    parent, name = _open_archive_member_parent_at(
        root_descriptor,
        member_name,
        "archive extraction directory",
    )
    try:
        os.mkdir(name, mode=mode, dir_fd=parent)
        child = _open_directory_at(parent, name, "archive extraction directory")
        try:
            os.fchmod(child, mode)
            _fsync_directory_at(child, "archive extraction directory")
        finally:
            os.close(child)
        _fsync_directory_at(parent, "archive extraction parent")
    except FileExistsError as error:
        raise SnapshotError(f"duplicate archive directory: {member_name}") from error
    except OSError as error:
        raise SnapshotError(f"could not extract archive directory {member_name}: {error}") from error
    finally:
        os.close(parent)


def _extract_archive_file_at(
    root_descriptor: int,
    member_name: str,
    mode: int,
    source: BinaryIO,
    digest: "hashlib._Hash",
    size: int,
) -> None:
    parent, name = _open_archive_member_parent_at(
        root_descriptor,
        member_name,
        "archive extraction directory",
    )
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            mode,
            dir_fd=parent,
        )
        with os.fdopen(descriptor, "wb") as output:
            descriptor = -1
            _copy_exact(source, output, digest, size)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(name, mode, dir_fd=parent, follow_symlinks=False)
        os.utime(name, (0, 0), dir_fd=parent, follow_symlinks=False)
        _fsync_regular_file_at(parent, name, "extracted archive file")
        _fsync_directory_at(parent, "archive extraction parent")
    except FileExistsError as error:
        raise SnapshotError(f"duplicate archive file: {member_name}") from error
    except OSError as error:
        raise SnapshotError(f"could not extract archive file {member_name}: {error}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent)


def _verify_archive_contents(
    archive_source: Path | BinaryIO,
    manifest: dict[str, Any],
    extract_root: Optional[int],
    *,
    max_content_bytes: int,
    max_file_count: int,
    max_member_count: int,
    max_path_components: int,
) -> None:
    dataset = manifest["dataset"]
    digest = hashlib.sha256()
    names: list[str] = []
    seen_names: set[str] = set()
    previous_name: Optional[str] = None
    directories: set[str] = set()
    file_count = 0
    member_count = 0
    content_bytes = 0

    try:
        if isinstance(archive_source, Path):
            archive_context = tarfile.open(archive_source, mode="r:gz")
        else:
            archive_source.seek(0)
            archive_context = tarfile.open(fileobj=archive_source, mode="r:gz")
        with archive_context as archive:
            for member in archive:
                member_count += 1
                if member_count > max_member_count:
                    raise SnapshotError("archive exceeds maximum member count")
                _validate_member_scope(member, max_path_components)
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
                        _extract_archive_directory_at(
                            extract_root,
                            member.name,
                            expected_mode,
                        )
                    continue

                file_count += 1
                content_bytes += member.size
                if file_count > max_file_count:
                    raise SnapshotError("archive exceeds maximum file count")
                if content_bytes > max_content_bytes:
                    raise SnapshotError("archive exceeds maximum content bytes")
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
                    with source:
                        _extract_archive_file_at(
                            extract_root,
                            member.name,
                            expected_mode,
                            source,
                            digest,
                            member.size,
                        )
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


def _fsync_extracted_tree(root_descriptor: int) -> None:
    """Persist extracted child directories before their root can be renamed."""

    try:
        entries = list(os.scandir(root_descriptor))
    except OSError as error:
        raise SnapshotError(f"could not enumerate extracted archive directory: {error}") from error
    for entry in entries:
        metadata = os.stat(
            entry.name,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        if stat.S_ISDIR(metadata.st_mode):
            child = _open_directory_at(
                root_descriptor,
                entry.name,
                "extracted archive directory",
            )
            try:
                _fsync_extracted_tree(child)
            finally:
                os.close(child)
        elif not stat.S_ISREG(metadata.st_mode):
            raise SnapshotError(f"extracted archive contains unsupported entry: {entry.name}")
    _fsync_directory_at(root_descriptor, "extracted archive directory")


def verify_snapshot(
    *,
    archive_path: Path,
    manifest_path: Path,
    max_archive_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES,
    max_content_bytes: int = DEFAULT_MAX_CONTENT_BYTES,
    max_file_count: int = DEFAULT_MAX_FILE_COUNT,
    max_member_count: int = DEFAULT_MAX_MEMBER_COUNT,
    max_path_components: int = DEFAULT_MAX_PATH_COMPONENTS,
    extract_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Verify a snapshot and optionally extract it to a new destination."""
    _validate_positive_limit(max_archive_bytes)
    _validate_extraction_limits(
        max_content_bytes=max_content_bytes,
        max_file_count=max_file_count,
        max_member_count=max_member_count,
        max_path_components=max_path_components,
    )
    archive_path = Path(archive_path)
    manifest_path = Path(manifest_path)
    archive_metadata = _regular_file(archive_path, "archive")
    if stat.S_IMODE(archive_metadata.st_mode) != 0o600:
        raise SnapshotError("archive mode must be exactly 0600")
    manifest = _load_manifest(manifest_path)
    manifest_sha256 = _sha256_file(manifest_path)
    _preflight_manifest_extraction_limits(
        manifest,
        max_content_bytes=max_content_bytes,
        max_file_count=max_file_count,
    )
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

    extraction_staging_name: Optional[str] = None
    extraction_staging_descriptor = -1
    destination_parent_descriptor = -1
    destination: Optional[Path] = None
    if extract_root is not None:
        if not _directory_fd_capabilities_available():
            raise SnapshotError("secure directory-descriptor extraction is unavailable")
        destination = Path(extract_root).absolute()
        if destination.exists() or destination.is_symlink():
            raise SnapshotError(
                f"extraction destination already exists: {destination}"
            )
        parent_identity = _directory_identity(
            destination.parent,
            "extraction destination parent",
        )
        destination_parent_descriptor = _open_verified_output_directory(
            destination.parent,
            "extraction destination parent",
        )
        if (
            _directory_descriptor_identity(
                destination_parent_descriptor,
                "extraction destination parent",
            )
            != parent_identity
        ):
            os.close(destination_parent_descriptor)
            raise SnapshotError("extraction destination parent changed while opening it")
        try:
            (
                extraction_staging_name,
                extraction_staging_descriptor,
            ) = _create_private_directory_at(
                destination_parent_descriptor,
                f".{destination.name}.snapshot-",
                "extraction staging directory",
            )
        except BaseException:
            os.close(destination_parent_descriptor)
            raise

    try:
        _verify_archive_contents(
            archive_path,
            manifest,
            extraction_staging_descriptor if extraction_staging_name is not None else None,
            max_content_bytes=max_content_bytes,
            max_file_count=max_file_count,
            max_member_count=max_member_count,
            max_path_components=max_path_components,
        )
        if extraction_staging_name is not None and destination is not None:
            _fsync_extracted_tree(extraction_staging_descriptor)
            os.close(extraction_staging_descriptor)
            extraction_staging_descriptor = -1
            _revalidate_output_directory(
                destination.parent,
                destination_parent_descriptor,
            )
            try:
                os.replace(
                    extraction_staging_name,
                    destination.name,
                    src_dir_fd=destination_parent_descriptor,
                    dst_dir_fd=destination_parent_descriptor,
                )
            except (NotImplementedError, TypeError) as error:
                raise SnapshotError(
                    "secure directory-descriptor extraction is unavailable"
                ) from error
            except OSError as error:
                raise SnapshotError(f"could not publish extraction: {error}") from error
            _fsync_directory_at(
                destination_parent_descriptor,
                "extraction destination parent",
            )
            _revalidate_output_directory(
                destination.parent,
                destination_parent_descriptor,
            )
            extraction_staging_name = None
    finally:
        if extraction_staging_descriptor >= 0:
            os.close(extraction_staging_descriptor)
        if extraction_staging_name is not None and destination_parent_descriptor >= 0:
            _remove_tree_at(
                destination_parent_descriptor,
                extraction_staging_name,
                "extraction staging directory",
            )
        if destination_parent_descriptor >= 0:
            os.close(destination_parent_descriptor)

    summary = _manifest_summary(manifest)
    summary["manifest_sha256"] = manifest_sha256
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
    max_content_bytes: int = DEFAULT_MAX_CONTENT_BYTES,
    max_file_count: int = DEFAULT_MAX_FILE_COUNT,
    max_member_count: int = DEFAULT_MAX_MEMBER_COUNT,
    max_path_components: int = DEFAULT_MAX_PATH_COMPONENTS,
) -> dict[str, Any]:
    """Verify a pair using only names resolved from the verified directory fd."""

    _validate_extraction_limits(
        max_content_bytes=max_content_bytes,
        max_file_count=max_file_count,
        max_member_count=max_member_count,
        max_path_components=max_path_components,
    )
    archive_descriptor, archive_metadata = _open_regular_file_at(
        directory_descriptor,
        archive_name,
        "archive",
    )
    try:
        if stat.S_IMODE(archive_metadata.st_mode) != 0o600:
            raise SnapshotError("archive mode must be exactly 0600")
        manifest = _load_manifest_at(directory_descriptor, manifest_name)
        _preflight_manifest_extraction_limits(
            manifest,
            max_content_bytes=max_content_bytes,
            max_file_count=max_file_count,
        )
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
            _verify_archive_contents(
                archive,
                manifest,
                None,
                max_content_bytes=max_content_bytes,
                max_file_count=max_file_count,
                max_member_count=max_member_count,
                max_path_components=max_path_components,
            )
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
            "manifest_sha256": _sha256_file(manifest_path),
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


def _release_id(value: object) -> int:
    if type(value) is not int or value < 1:
        raise SnapshotError("release ID must be a positive integer")
    return value


def _validated_release(
    value: object,
    *,
    expected_tag: Optional[str],
    expected_release_id: Optional[int] = None,
) -> dict[str, Any]:
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
    release_id = _release_id(value.get("id"))
    if expected_release_id is not None and release_id != expected_release_id:
        raise SnapshotError("GitHub returned a different release ID")
    if not isinstance(value.get("assets"), list):
        raise SnapshotError(f"release assets are invalid: {tag}")
    return value


def _resolve_newest_release(*, repository: str, token: str) -> dict[str, Any]:
    quoted_repository = "/".join(
        urllib.parse.quote(part, safe="") for part in repository.split("/")
    )
    selected: dict[str, Any] | None = None
    for page in range(1, MAX_RELEASE_LIST_PAGES + 1):
        url = (
            f"{API_BASE}/repos/{quoted_repository}/releases"
            f"?per_page={RELEASES_PER_PAGE}&page={page}"
        )
        response = _github_json(url, token)
        if not isinstance(response, list):
            raise SnapshotError("GitHub releases response must be an array")
        if len(response) > RELEASES_PER_PAGE:
            raise SnapshotError(
                "GitHub releases response exceeded the requested page size"
            )
        for release in response:
            if not (
                isinstance(release, dict)
                and isinstance(release.get("tag_name"), str)
                and release["tag_name"].startswith("dataset-")
                and release.get("draft") is False
                and release.get("prerelease") is False
                and isinstance(release.get("published_at"), str)
                and isinstance(release.get("assets"), list)
            ):
                continue
            if selected is None or (
                release["published_at"],
                release.get("id", 0) if type(release.get("id", 0)) is int else 0,
            ) > (
                selected["published_at"],
                selected.get("id", 0) if type(selected.get("id", 0)) is int else 0,
            ):
                selected = release
        if len(response) < RELEASES_PER_PAGE:
            break
    else:
        raise SnapshotError(
            "GitHub release list exceeded bounded pagination; refusing a partial newest-release result"
        )
    if selected is None:
        raise SnapshotError("no published dataset-* release was found")
    return _validated_release(selected, expected_tag=None)


def _resolve_release(
    *,
    repository: str,
    release_tag: Optional[str],
    release_id: Optional[int] = None,
    token: str,
) -> dict[str, Any]:
    if release_tag is not None and release_id is not None:
        raise SnapshotError("release tag and release ID cannot both select a release")
    quoted_repository = "/".join(
        urllib.parse.quote(part, safe="") for part in repository.split("/")
    )
    if release_id is not None:
        release_id = _release_id(release_id)
        release = _validated_release(
            _github_json(
                f"{API_BASE}/repos/{quoted_repository}/releases/{release_id}", token
            ),
            expected_tag=None,
            expected_release_id=release_id,
        )
        newest = _resolve_newest_release(repository=repository, token=token)
        if _release_id(newest["id"]) != release_id:
            raise SnapshotError("selected release is no longer the newest valid private release")
        return release
    if release_tag is not None:
        if not release_tag.startswith("dataset-"):
            raise SnapshotError("release tag must start with dataset-")
        return _validated_release(
            _github_json(
                f"{API_BASE}/repos/{quoted_repository}/releases/tags/"
                f"{urllib.parse.quote(release_tag, safe='')}",
                token,
            ),
            expected_tag=release_tag,
        )
    return _resolve_newest_release(repository=repository, token=token)


def _canonical_release_identity(
    *, repository: str, release: dict[str, Any]
) -> dict[str, Any]:
    release_id = _release_id(release.get("id"))
    tag_name = release.get("tag_name")
    title = release.get("name")
    body = release.get("body")
    draft = release.get("draft")
    prerelease = release.get("prerelease")
    if not isinstance(tag_name, str) or not tag_name.startswith("dataset-"):
        raise SnapshotError("release tag must start with dataset-")
    if not isinstance(title, str) or not isinstance(body, str):
        raise SnapshotError("release title and body must be strings")
    if type(draft) is not bool or type(prerelease) is not bool:
        raise SnapshotError("release draft and prerelease state must be booleans")
    assets: list[dict[str, Any]] = []
    for asset in release["assets"]:
        if not isinstance(asset, dict):
            raise SnapshotError("release assets are invalid")
        asset_id = _release_id(asset.get("id"))
        name = asset.get("name")
        size = asset.get("size")
        state = asset.get("state")
        digest = asset.get("digest")
        if not isinstance(name, str) or type(size) is not int or size < 1:
            raise SnapshotError("release asset identity is invalid")
        if not isinstance(state, str):
            raise SnapshotError("release asset state is invalid")
        if not isinstance(digest, str) or ASSET_DIGEST_RE.fullmatch(digest) is None:
            raise SnapshotError("release asset digest is invalid")
        assets.append(
            {"digest": digest, "id": asset_id, "name": name, "size": size, "state": state}
        )
    return {
        "assets": sorted(assets, key=lambda asset: (asset["name"], asset["id"])),
        "body": body,
        "contract_version": 1,
        "draft": draft,
        "prerelease": prerelease,
        "release_id": release_id,
        "repository": repository,
        "tag_name": tag_name,
        "title": title,
    }


def release_identity_sha256(*, repository: str, release: dict[str, Any]) -> str:
    """Hash the complete bounded private-release identity without exposing notes."""
    canonical = _canonical_release_identity(repository=repository, release=release)
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def resolve_release_identity(
    *,
    repository: str,
    release_tag: Optional[str] = None,
    release_id: Optional[int] = None,
    token: Optional[str] = None,
) -> dict[str, Any]:
    """Resolve a private release and return only bounded identity metadata."""

    if not REPOSITORY_RE.fullmatch(repository):
        raise SnapshotError("repository must use OWNER/REPO format")
    resolved_token = token if token is not None else os.environ.get(TOKEN_ENV)
    if not resolved_token:
        raise SnapshotError(f"{TOKEN_ENV} is not set")
    release = _resolve_release(
        repository=repository,
        release_tag=release_tag,
        release_id=release_id,
        token=resolved_token,
    )
    dataset_id, archive_asset, manifest_asset = _snapshot_release_assets(release)
    result = {
        "archive_sha256": _release_asset_sha256(archive_asset),
        "dataset_id": dataset_id,
        "manifest_sha256": _release_asset_sha256(manifest_asset),
        "release_tag": release["tag_name"],
        "repository": repository,
    }
    if release_id is not None or (
        isinstance(release.get("name"), str)
        and isinstance(release.get("body"), str)
        and all(
            isinstance(asset, dict)
            and type(asset.get("id")) is int
            and isinstance(asset.get("state"), str)
            for asset in release["assets"]
        )
    ):
        result.update(
            {
                "release_id": _release_id(release["id"]),
                "release_identity_sha256": release_identity_sha256(
                    repository=repository,
                    release=release,
                ),
            }
        )
    return result


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


def _release_asset_sha256(asset: dict[str, Any]) -> str:
    digest = asset.get("digest")
    match = ASSET_DIGEST_RE.fullmatch(digest) if type(digest) is str else None
    if match is None:
        raise SnapshotError(f"release asset digest is invalid: {asset.get('name')}")
    return match.group(1)


def _snapshot_release_assets(
    release: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    assets = release["assets"]
    manifest_asset = _find_asset(assets, suffix=MANIFEST_SUFFIX)
    manifest_name = manifest_asset["name"]
    dataset_id = manifest_name[len(ARCHIVE_PREFIX) : -len(MANIFEST_SUFFIX)]
    if not SHA_RE.fullmatch(dataset_id):
        raise SnapshotError("release manifest name has an invalid dataset ID")
    archive_name, expected_manifest_name = _manifest_names(dataset_id)
    if manifest_name != expected_manifest_name:
        raise SnapshotError("release manifest name does not match its dataset ID")
    archive_asset = _find_asset(assets, name=archive_name)
    asset_names = [
        asset.get("name") if isinstance(asset, dict) else None for asset in assets
    ]
    required_asset_names = {manifest_name, archive_name}
    allowed_asset_names = required_asset_names | {"pages-deployment.json"}
    if (
        len(asset_names) != len(set(asset_names))
        or not required_asset_names.issubset(set(asset_names))
        or not set(asset_names).issubset(allowed_asset_names)
    ):
        raise SnapshotError(
            "release assets must be the snapshot archive and manifest, with only an optional pages-deployment.json marker"
        )
    return dataset_id, archive_asset, manifest_asset


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


def _download_asset_at(
    *,
    asset: dict[str, Any],
    directory_descriptor: int,
    name: str,
    token: str,
    max_bytes: int,
) -> None:
    """Download one release asset through a retained staging directory fd."""

    if "/" in name or name in {"", ".", ".."}:
        raise SnapshotError("release asset name is unsafe")
    total = 0
    for attempt in range(len(GITHUB_RETRY_DELAYS_SECONDS) + 1):
        descriptor = -1
        total = 0
        try:
            request = _authorized_request(asset["url"], token, "application/octet-stream")
            with _URL_OPENER.open(request, timeout=120) as response:
                content_length = response.headers.get("Content-Length")
                if content_length is not None and int(content_length) > max_bytes:
                    raise SnapshotError("download exceeds maximum allowed size")
                descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory_descriptor)
                with os.fdopen(descriptor, "wb") as output:
                    descriptor = -1
                    while True:
                        block = response.read(1024 * 1024)
                        if not block:
                            break
                        total += len(block)
                        if total > max_bytes:
                            raise SnapshotError("download exceeds maximum allowed size")
                        output.write(block)
                    output.flush()
                    os.fsync(output.fileno())
            if total != asset["size"]:
                raise SnapshotError("downloaded byte count does not match release asset")
            _fsync_directory_at(directory_descriptor, "pull staging directory")
            return
        except SnapshotError:
            _unlink_at(directory_descriptor, name)
            raise
        except urllib.error.HTTPError as error:
            _unlink_at(directory_descriptor, name)
            if error.code in GITHUB_RETRY_STATUS_CODES and attempt < len(GITHUB_RETRY_DELAYS_SECONDS):
                time.sleep(GITHUB_RETRY_DELAYS_SECONDS[attempt])
                continue
            raise SnapshotError(f"release asset download failed: {error}") from error
        except GITHUB_RETRY_EXCEPTIONS as error:
            _unlink_at(directory_descriptor, name)
            if attempt < len(GITHUB_RETRY_DELAYS_SECONDS):
                time.sleep(GITHUB_RETRY_DELAYS_SECONDS[attempt])
                continue
            raise SnapshotError(f"release asset download failed: {error}") from error
        except (OSError, ValueError) as error:
            _unlink_at(directory_descriptor, name)
            raise SnapshotError(f"release asset download failed: {error}") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)


def _restore_transaction_state(
    *,
    data_existed: bool,
    cache_root_existed: bool,
    cache_files_existed: list[str],
    phase: str,
) -> dict[str, Any]:
    """Return the canonical, durable restore journal payload."""

    return {
        "cache_files_existed": sorted(cache_files_existed),
        "cache_root_existed": cache_root_existed,
        "contract_version": RESTORE_CONTRACT_VERSION,
        "data_existed": data_existed,
        "phase": phase,
    }


def _at_metadata(directory_descriptor: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise SnapshotError(f"could not inspect restore entry {name}: {error}") from error


def _at_directory(directory_descriptor: int, name: str, label: str) -> bool:
    metadata = _at_metadata(directory_descriptor, name)
    if metadata is None:
        return False
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise SnapshotError(f"{label} must be a real directory: {name}")
    return True


def _at_regular_file(directory_descriptor: int, name: str, label: str) -> bool:
    metadata = _at_metadata(directory_descriptor, name)
    if metadata is None:
        return False
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SnapshotError(f"{label} must be a regular file: {name}")
    return True


def _replace_restore_at(
    source_descriptor: int,
    source_name: str,
    target_descriptor: int,
    target_name: str,
    label: str,
) -> None:
    try:
        os.replace(
            source_name,
            target_name,
            src_dir_fd=source_descriptor,
            dst_dir_fd=target_descriptor,
        )
    except (NotImplementedError, TypeError) as error:
        raise SnapshotError("secure directory-descriptor restore is unavailable") from error
    except OSError as error:
        raise SnapshotError(f"could not move {label}: {error}") from error
    _fsync_directory_at(source_descriptor, f"{label} source directory")
    if target_descriptor != source_descriptor:
        _fsync_directory_at(target_descriptor, f"{label} target directory")


def _restore_cache_names() -> tuple[str, ...]:
    return tuple(relative.name for relative in CACHE_FILES)


def _write_restore_state_at(transaction_descriptor: int, state: dict[str, Any]) -> None:
    temporary = RESTORE_STATE_TEMP_NAME
    if _at_regular_file(transaction_descriptor, temporary, "temporary restore journal"):
        _unlink_at(transaction_descriptor, temporary)
        _fsync_directory_at(transaction_descriptor, "restore transaction directory")
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=transaction_descriptor,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(state, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        _replace_restore_at(
            transaction_descriptor,
            temporary,
            transaction_descriptor,
            RESTORE_STATE_NAME,
            "restore journal",
        )
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _load_restore_state_at(transaction_descriptor: int) -> dict[str, Any]:
    descriptor, metadata = _open_regular_file_at(
        transaction_descriptor,
        RESTORE_STATE_NAME,
        "restore journal",
    )
    try:
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            raise SnapshotError("restore journal must have mode 0600")
        if metadata.st_size < 1 or metadata.st_size > MAX_RESTORE_STATE_BYTES:
            raise SnapshotError("restore journal size is invalid")
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            parsed = json.loads(handle.read(MAX_RESTORE_STATE_BYTES + 1))
    except (UnicodeDecodeError, json.JSONDecodeError, OSError) as error:
        raise SnapshotError(f"restore journal is invalid: {error}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    state = _require_exact_keys(
        parsed,
        {"cache_files_existed", "cache_root_existed", "contract_version", "data_existed", "phase"},
        "restore journal",
    )
    if state["contract_version"] != RESTORE_CONTRACT_VERSION or type(state["data_existed"]) is not bool or type(state["cache_root_existed"]) is not bool or state["phase"] not in {"prepared", "committed"}:
        raise SnapshotError("restore journal contract is invalid")
    cache_files = state["cache_files_existed"]
    allowed = {relative.as_posix() for relative in CACHE_FILES}
    if not isinstance(cache_files, list) or any(not isinstance(value, str) for value in cache_files) or cache_files != sorted(cache_files) or len(cache_files) != len(set(cache_files)) or not set(cache_files).issubset(allowed):
        raise SnapshotError("restore journal cache file inventory is invalid")
    if cache_files and not state["cache_root_existed"]:
        raise SnapshotError("restore journal cache root inventory is invalid")
    return state


def _remove_restore_directory_at(root_descriptor: int, name: str, label: str) -> None:
    metadata = _at_metadata(root_descriptor, name)
    if metadata is None:
        return
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise SnapshotError(f"{label} must be a real directory: {name}")
    _remove_tree_at(root_descriptor, name, label)


def _finish_restore_transaction_at(root_descriptor: int, transaction_name: str) -> None:
    _remove_restore_directory_at(root_descriptor, RESTORE_CLEANUP_NAME, "stale restore cleanup")
    _replace_restore_at(root_descriptor, transaction_name, root_descriptor, RESTORE_CLEANUP_NAME, "restore transaction")
    _remove_restore_directory_at(root_descriptor, RESTORE_CLEANUP_NAME, "restore cleanup")


def _validate_restore_targets_at(root_descriptor: int) -> None:
    _at_directory(root_descriptor, "data", "existing data target")
    cache_exists = _at_directory(root_descriptor, ".cache", "existing cache target")
    if cache_exists:
        cache_descriptor = _open_directory_at(root_descriptor, ".cache", "existing cache target")
        try:
            for name in _restore_cache_names():
                _at_regular_file(cache_descriptor, name, "existing cache target")
        finally:
            os.close(cache_descriptor)


def _open_payload_capability(payload: Path) -> int:
    descriptor = _open_verified_output_directory(payload, "extracted payload")
    try:
        if not _at_directory(descriptor, "data", "extracted data directory") or not _at_directory(descriptor, ".cache", "extracted cache directory"):
            raise SnapshotError("extracted payload is incomplete")
        cache_descriptor = _open_directory_at(descriptor, ".cache", "extracted cache directory")
        try:
            for name in _restore_cache_names():
                if not _at_regular_file(cache_descriptor, name, "extracted cache file"):
                    raise SnapshotError(f"extracted cache file is missing: {name}")
        finally:
            os.close(cache_descriptor)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _create_restore_transaction_at(root_descriptor: int, state: dict[str, Any]) -> tuple[str, int]:
    if _at_metadata(root_descriptor, RESTORE_TRANSACTION_NAME) is not None:
        raise SnapshotError("an unrecovered restore transaction already exists")
    _remove_restore_directory_at(root_descriptor, RESTORE_PREPARE_NAME, "stale restore preparation")
    try:
        os.mkdir(RESTORE_PREPARE_NAME, mode=0o700, dir_fd=root_descriptor)
        prepare = _open_directory_at(root_descriptor, RESTORE_PREPARE_NAME, "restore preparation")
        try:
            os.fchmod(prepare, 0o700)
            os.mkdir("backup", mode=0o700, dir_fd=prepare)
            backup = _open_directory_at(prepare, "backup", "restore backup directory")
            try:
                os.fchmod(backup, 0o700)
                os.mkdir(".cache", mode=0o700, dir_fd=backup)
                backup_cache = _open_directory_at(backup, ".cache", "restore cache backup directory")
                try:
                    os.fchmod(backup_cache, 0o700)
                    _fsync_directory_at(backup_cache, "restore cache backup directory")
                finally:
                    os.close(backup_cache)
                _fsync_directory_at(backup, "restore backup directory")
            finally:
                os.close(backup)
            _write_restore_state_at(prepare, state)
            _fsync_directory_at(prepare, "restore preparation directory")
        finally:
            os.close(prepare)
        _replace_restore_at(root_descriptor, RESTORE_PREPARE_NAME, root_descriptor, RESTORE_TRANSACTION_NAME, "restore preparation")
        return RESTORE_TRANSACTION_NAME, _open_directory_at(root_descriptor, RESTORE_TRANSACTION_NAME, "restore transaction")
    except BaseException:
        _remove_restore_directory_at(root_descriptor, RESTORE_PREPARE_NAME, "restore preparation")
        raise


def _remove_restore_data_at(root_descriptor: int) -> None:
    if _at_directory(root_descriptor, "data", "restore data target"):
        _remove_tree_at(root_descriptor, "data", "restore data target")


def _remove_restore_cache_file_at(cache_descriptor: int, name: str) -> None:
    if _at_regular_file(cache_descriptor, name, "restore cache target"):
        _unlink_at(cache_descriptor, name)
        _fsync_directory_at(cache_descriptor, "restore cache directory")


def _rollback_prepared_restore_at(root_descriptor: int, transaction_descriptor: int, state: dict[str, Any]) -> None:
    backup = _open_directory_at(transaction_descriptor, "backup", "restore backup directory")
    try:
        backup_cache = _open_directory_at(backup, ".cache", "restore cache backup directory")
        try:
            if state["data_existed"]:
                if _at_directory(backup, "data", "restore data backup"):
                    _remove_restore_data_at(root_descriptor)
                    _replace_restore_at(backup, "data", root_descriptor, "data", "restore data backup")
                elif not _at_directory(root_descriptor, "data", "existing restore data target"):
                    raise SnapshotError("restore data backup and target are both missing")
            else:
                if _at_metadata(backup, "data") is not None:
                    raise SnapshotError("unexpected restore data backup exists")
                _remove_restore_data_at(root_descriptor)
            existing = set(state["cache_files_existed"])
            cache_exists = _at_directory(root_descriptor, ".cache", "restore cache root")
            if (existing or state["cache_root_existed"]) and not cache_exists:
                os.mkdir(".cache", mode=0o700, dir_fd=root_descriptor)
                cache_exists = True
            cache = _open_directory_at(root_descriptor, ".cache", "restore cache root") if cache_exists else -1
            try:
                for relative, name in zip(CACHE_FILES, _restore_cache_names()):
                    if relative.as_posix() in existing:
                        if _at_regular_file(backup_cache, name, "restore cache backup"):
                            _remove_restore_cache_file_at(cache, name)
                            _replace_restore_at(backup_cache, name, cache, name, "restore cache backup")
                        elif not _at_regular_file(cache, name, "existing restore cache target"):
                            raise SnapshotError(f"restore cache backup and target are both missing: {relative}")
                    else:
                        if _at_metadata(backup_cache, name) is not None:
                            raise SnapshotError(f"unexpected restore cache backup exists: {relative}")
                        if cache >= 0:
                            _remove_restore_cache_file_at(cache, name)
                if cache >= 0 and not state["cache_root_existed"]:
                    try:
                        os.rmdir(".cache", dir_fd=root_descriptor)
                    except OSError:
                        pass
            finally:
                if cache >= 0:
                    os.close(cache)
        finally:
            os.close(backup_cache)
    finally:
        os.close(backup)


def _recover_interrupted_restore_locked(
    root_descriptor: int,
    named_root: Path | None = None,
) -> None:
    if named_root is not None:
        _revalidate_output_directory(named_root, root_descriptor)
    _remove_restore_directory_at(root_descriptor, RESTORE_CLEANUP_NAME, "stale restore cleanup")
    _remove_restore_directory_at(root_descriptor, RESTORE_PREPARE_NAME, "stale restore preparation")
    if not _at_directory(root_descriptor, RESTORE_TRANSACTION_NAME, "restore transaction"):
        return
    transaction = _open_directory_at(root_descriptor, RESTORE_TRANSACTION_NAME, "restore transaction")
    try:
        if stat.S_IMODE(os.fstat(transaction).st_mode) != 0o700:
            raise SnapshotError("restore transaction must have mode 0700")
        state = _load_restore_state_at(transaction)
        if state["phase"] == "prepared":
            _rollback_prepared_restore_at(root_descriptor, transaction, state)
        else:
            _validate_restore_targets_at(root_descriptor)
    finally:
        os.close(transaction)
    _finish_restore_transaction_at(root_descriptor, RESTORE_TRANSACTION_NAME)


def _replace_payload_locked(
    root_descriptor: int,
    payload_descriptor: int,
    named_root: Path | None = None,
) -> None:
    if named_root is not None:
        _revalidate_output_directory(named_root, root_descriptor)
    _recover_interrupted_restore_locked(root_descriptor)
    _validate_restore_targets_at(root_descriptor)
    data_existed = _at_directory(root_descriptor, "data", "existing data target")
    cache_existed = _at_directory(root_descriptor, ".cache", "existing cache target")
    cache = _open_directory_at(root_descriptor, ".cache", "existing cache target") if cache_existed else -1
    try:
        existing_cache = [
            relative.as_posix()
            for relative, name in zip(CACHE_FILES, _restore_cache_names())
            if cache >= 0 and _at_regular_file(cache, name, "existing cache target")
        ]
    finally:
        if cache >= 0:
            os.close(cache)
    state = _restore_transaction_state(data_existed=data_existed, cache_root_existed=cache_existed, cache_files_existed=existing_cache, phase="prepared")
    transaction_name: str | None = None
    transaction = -1
    try:
        transaction_name, transaction = _create_restore_transaction_at(root_descriptor, state)
        backup = _open_directory_at(transaction, "backup", "restore backup directory")
        try:
            backup_cache = _open_directory_at(backup, ".cache", "restore cache backup directory")
            try:
                if data_existed:
                    _replace_restore_at(root_descriptor, "data", backup, "data", "existing data")
                cache = _open_directory_at(root_descriptor, ".cache", "existing cache target") if cache_existed else -1
                try:
                    for relative, name in zip(CACHE_FILES, _restore_cache_names()):
                        if relative.as_posix() in existing_cache:
                            _replace_restore_at(cache, name, backup_cache, name, f"existing cache file {relative}")
                finally:
                    if cache >= 0:
                        os.close(cache)
                _replace_restore_at(payload_descriptor, "data", root_descriptor, "data", "new data")
                if not _at_directory(root_descriptor, ".cache", "restored cache directory"):
                    os.mkdir(".cache", mode=0o700, dir_fd=root_descriptor)
                    # Persist the new root entry before a journal can commit it.
                    _fsync_directory_at(root_descriptor, "restore root")
                cache = _open_directory_at(root_descriptor, ".cache", "restored cache directory")
                payload_cache = _open_directory_at(payload_descriptor, ".cache", "extracted cache directory")
                try:
                    for relative, name in zip(CACHE_FILES, _restore_cache_names()):
                        _replace_restore_at(payload_cache, name, cache, name, f"new cache file {relative}")
                        _fsync_regular_file_at(cache, name, "restored cache file")
                    _fsync_directory_at(cache, "restored cache directory")
                finally:
                    os.close(payload_cache)
                    os.close(cache)
            finally:
                os.close(backup_cache)
        finally:
            os.close(backup)
        _validate_restore_targets_at(root_descriptor)
        committed = dict(state)
        committed["phase"] = "committed"
        if named_root is not None:
            _revalidate_output_directory(named_root, root_descriptor)
        _write_restore_state_at(transaction, committed)
        os.close(transaction)
        transaction = -1
        _finish_restore_transaction_at(root_descriptor, transaction_name)
        if named_root is not None:
            _revalidate_output_directory(named_root, root_descriptor)
    except BaseException as error:
        if transaction >= 0:
            os.close(transaction)
        try:
            _recover_interrupted_restore_locked(root_descriptor)
        except BaseException as recovery_error:
            if hasattr(error, "add_note"):
                error.add_note(f"restore recovery also failed: {recovery_error}")
        if isinstance(error, Exception):
            raise SnapshotError(f"snapshot restore failed: {error}") from error
        raise


def _recover_interrupted_restore(root: Path) -> None:
    root = Path(root)
    expected_identity = _directory_identity(root, "restore root")
    descriptor = _open_verified_output_directory(root, "restore root")
    try:
        if _directory_descriptor_identity(descriptor, "restore root") != expected_identity:
            raise SnapshotError("restore root changed while opening it")
        with _locked_output_directory(descriptor):
            _recover_interrupted_restore_locked(descriptor, root)
            _revalidate_output_directory(root, descriptor)
    finally:
        os.close(descriptor)


def _replace_payload(root: Path, payload: Path) -> None:
    """Transactionally replace data and only the allowlisted cache files."""

    root = Path(root)
    expected_identity = _directory_identity(root, "restore root")
    descriptor = _open_verified_output_directory(root, "restore root")
    payload_descriptor = -1
    try:
        if _directory_descriptor_identity(descriptor, "restore root") != expected_identity:
            raise SnapshotError("restore root changed while opening it")
        with _locked_output_directory(descriptor):
            payload_descriptor = _open_payload_capability(Path(payload))
            _replace_payload_locked(descriptor, payload_descriptor, root)
            _revalidate_output_directory(root, descriptor)
    finally:
        if payload_descriptor >= 0:
            os.close(payload_descriptor)
        os.close(descriptor)


def pull_snapshot(
    *,
    repository: str,
    root: Path,
    replace: bool,
    release_tag: Optional[str] = None,
    release_id: Optional[int] = None,
    max_archive_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES,
    max_content_bytes: int = DEFAULT_MAX_CONTENT_BYTES,
    max_file_count: int = DEFAULT_MAX_FILE_COUNT,
    max_member_count: int = DEFAULT_MAX_MEMBER_COUNT,
    max_path_components: int = DEFAULT_MAX_PATH_COMPONENTS,
    token: Optional[str] = None,
) -> dict[str, Any]:
    """Download, verify, and transactionally restore a private release."""
    _validate_positive_limit(max_archive_bytes)
    _validate_extraction_limits(
        max_content_bytes=max_content_bytes,
        max_file_count=max_file_count,
        max_member_count=max_member_count,
        max_path_components=max_path_components,
    )
    if not replace:
        raise SnapshotError("pull requires --replace to modify local data")
    if not REPOSITORY_RE.fullmatch(repository):
        raise SnapshotError("repository must use OWNER/REPO format")
    root = Path(root).absolute()
    root_identity = _directory_identity(root, "restore root")
    staging_parent_descriptor = _open_verified_output_directory(
        root.parent,
        "pull staging parent",
    )
    root_descriptor = -1
    staging_descriptor = -1
    staging_name: str | None = None
    try:
        root_descriptor = _open_directory_at(
            staging_parent_descriptor,
            root.name,
            "restore root",
        )
        if _directory_descriptor_identity(root_descriptor, "restore root") != root_identity:
            raise SnapshotError("restore root changed while opening it")
    except BaseException:
        os.close(staging_parent_descriptor)
        raise
    try:
        resolved_token = token if token is not None else os.environ.get(TOKEN_ENV)
        if not resolved_token:
            raise SnapshotError(f"{TOKEN_ENV} is not set")
        release = _resolve_release(
            repository=repository,
            release_tag=release_tag,
            release_id=release_id,
            token=resolved_token,
        )
        release_dataset_id, archive_asset, manifest_asset = _snapshot_release_assets(
            release
        )
        staging_name, staging_descriptor = _create_private_directory_at(
            staging_parent_descriptor,
            ".data-snapshot-pull-",
            "pull staging directory",
        )
        _download_asset_at(asset=manifest_asset, directory_descriptor=staging_descriptor, name=manifest_asset["name"], token=resolved_token, max_bytes=MAX_MANIFEST_BYTES)
        manifest_descriptor, _ = _open_regular_file_at(
            staging_descriptor,
            manifest_asset["name"],
            "snapshot manifest",
        )
        with os.fdopen(manifest_descriptor, "rb") as manifest_handle:
            manifest_sha256 = _sha256_handle(manifest_handle)
        if manifest_sha256 != _release_asset_sha256(manifest_asset):
            raise SnapshotError(
                "release manifest digest does not match downloaded bytes"
            )
        manifest = _load_manifest_at(staging_descriptor, manifest_asset["name"])
        if manifest["dataset_id"] != release_dataset_id:
            raise SnapshotError("release assets do not match the manifest dataset ID")
        if archive_asset["name"] != manifest["archive"]["filename"]:
            raise SnapshotError("release archive name does not match manifest")
        if archive_asset["size"] != manifest["archive"]["bytes"]:
            raise SnapshotError("release archive asset size does not match manifest")
        if _release_asset_sha256(archive_asset) != manifest["archive"]["sha256"]:
            raise SnapshotError("release archive digest does not match manifest")
        _download_asset_at(asset=archive_asset, directory_descriptor=staging_descriptor, name=archive_asset["name"], token=resolved_token, max_bytes=max_archive_bytes)
        _verify_snapshot_at(
            directory_descriptor=staging_descriptor,
            archive_name=archive_asset["name"],
            manifest_name=manifest_asset["name"],
            max_archive_bytes=max_archive_bytes,
            max_content_bytes=max_content_bytes,
            max_file_count=max_file_count,
            max_member_count=max_member_count,
            max_path_components=max_path_components,
        )
        os.mkdir("payload", mode=0o700, dir_fd=staging_descriptor)
        payload_descriptor = _open_directory_at(staging_descriptor, "payload", "pull payload")
        try:
            archive_descriptor, _ = _open_regular_file_at(staging_descriptor, archive_asset["name"], "archive")
            try:
                with os.fdopen(archive_descriptor, "rb") as archive:
                    archive_descriptor = -1
                    _verify_archive_contents(
                        archive,
                        manifest,
                        payload_descriptor,
                        max_content_bytes=max_content_bytes,
                        max_file_count=max_file_count,
                        max_member_count=max_member_count,
                        max_path_components=max_path_components,
                    )
            finally:
                if archive_descriptor >= 0:
                    os.close(archive_descriptor)
            _fsync_extracted_tree(payload_descriptor)
            with _locked_output_directory(root_descriptor):
                _replace_payload_locked(root_descriptor, payload_descriptor, root)
                _revalidate_output_directory(root, root_descriptor)
        finally:
            os.close(payload_descriptor)
        summary = _manifest_summary(manifest)
        summary.pop("extract_root", None)
        summary.update(
            {
                "manifest_sha256": manifest_sha256,
                "release_tag": release["tag_name"],
                "repository": repository,
                "restored_root": str(root),
            }
        )
        if release_id is not None or (
            isinstance(release.get("name"), str)
            and isinstance(release.get("body"), str)
            and all(
                isinstance(asset, dict)
                and type(asset.get("id")) is int
                and isinstance(asset.get("state"), str)
                for asset in release["assets"]
            )
        ):
            summary.update(
                {
                    "release_id": _release_id(release["id"]),
                    "release_identity_sha256": release_identity_sha256(
                        repository=repository,
                        release=release,
                    ),
                }
            )
        return summary
    finally:
        if staging_descriptor >= 0:
            os.close(staging_descriptor)
        if staging_name is not None:
            _remove_tree_at(staging_parent_descriptor, staging_name, "pull staging directory")
        if root_descriptor >= 0:
            os.close(root_descriptor)
        os.close(staging_parent_descriptor)


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
    verify.add_argument("--max-content-bytes", type=int, default=DEFAULT_MAX_CONTENT_BYTES)
    verify.add_argument("--max-file-count", type=int, default=DEFAULT_MAX_FILE_COUNT)
    verify.add_argument("--max-member-count", type=int, default=DEFAULT_MAX_MEMBER_COUNT)
    verify.add_argument("--max-path-components", type=int, default=DEFAULT_MAX_PATH_COMPONENTS)

    resolve = commands.add_parser(
        "resolve",
        help="resolve bounded private GitHub Release identity metadata",
    )
    resolve.add_argument("--repository", required=True)
    resolve.add_argument("--release-tag")
    resolve.add_argument("--release-id", type=int)

    pull = commands.add_parser(
        "pull",
        help="restore a private GitHub Release snapshot",
    )
    pull.add_argument("--repository", required=True)
    pull.add_argument("--root", type=Path, default=ROOT)
    pull.add_argument("--release-tag")
    pull.add_argument("--release-id", type=int)
    pull.add_argument("--replace", action="store_true", required=True)
    pull.add_argument(
        "--max-archive-bytes",
        type=int,
        default=DEFAULT_MAX_ARCHIVE_BYTES,
    )
    pull.add_argument("--max-content-bytes", type=int, default=DEFAULT_MAX_CONTENT_BYTES)
    pull.add_argument("--max-file-count", type=int, default=DEFAULT_MAX_FILE_COUNT)
    pull.add_argument("--max-member-count", type=int, default=DEFAULT_MAX_MEMBER_COUNT)
    pull.add_argument("--max-path-components", type=int, default=DEFAULT_MAX_PATH_COMPONENTS)
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
                max_content_bytes=args.max_content_bytes,
                max_file_count=args.max_file_count,
                max_member_count=args.max_member_count,
                max_path_components=args.max_path_components,
                extract_root=args.extract_root,
            )
        elif args.command == "resolve":
            result = resolve_release_identity(
                repository=args.repository,
                release_tag=args.release_tag,
                release_id=args.release_id,
            )
        else:
            result = pull_snapshot(
                repository=args.repository,
                root=args.root,
                release_tag=args.release_tag,
                release_id=args.release_id,
                replace=args.replace,
                max_archive_bytes=args.max_archive_bytes,
                max_content_bytes=args.max_content_bytes,
                max_file_count=args.max_file_count,
                max_member_count=args.max_member_count,
                max_path_components=args.max_path_components,
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
