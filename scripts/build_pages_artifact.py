#!/usr/bin/env python3
"""Build the bounded, deterministic static artifact deployed to GitHub Pages."""

from __future__ import annotations

import argparse
import concurrent.futures
import fcntl
import gzip
import hashlib
import json
import os
import re
import secrets
import stat
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from insider_publication import (  # noqa: E402
    read_validated_insider_public_snapshot_fd,
)


DEFAULT_MAX_ARCHIVE_BYTES = 1_000_000_000
MAX_STATIC_FILE_BYTES = 25_000_000
MAX_DATA_JSON_BYTES = 50_000_000
MAX_TOTAL_SOURCE_BYTES = 5_000_000_000
MAX_DATA_FILES_PER_DIRECTORY = 50_000
MAX_ARTIFACT_FILES = 100_100
MAX_GZIP_COMPRESSION_RATIO = 500
MAX_COMPRESSION_WORKERS = 32
STATIC_FILES = (
    Path(".nojekyll"),
    Path("CNAME"),
    Path("index.html"),
    Path("site-data-loader.js"),
)
INDEX_FILES = (
    Path("data/funds-index.json"),
    Path("data/index.json"),
    Path("data/security_labels.json"),
)
COMPRESSED_DIRECTORIES = (
    Path("data/funds"),
    Path("data/stocks"),
)
INSIDER_PUBLIC_ROOT = Path("data/insiders/public")
INSIDER_PUBLIC_MANIFEST = INSIDER_PUBLIC_ROOT / "manifest.json"
INSIDER_COMPRESSED_DIRECTORIES = (
    INSIDER_PUBLIC_ROOT / "securities",
    INSIDER_PUBLIC_ROOT / "filings",
)
SHA_RE = re.compile(r"[0-9a-f]{40}")
DATASET_ID_RE = re.compile(r"[0-9a-f]{64}")
FUND_SOURCE_NAME_RE = re.compile(r"[0-9]{1,10}\.json")
STOCK_SOURCE_NAME_RE = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,159}\.json")
_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_FILE_READ_FLAGS = (
    os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)
_FILE_WRITE_FLAGS = (
    os.O_WRONLY
    | os.O_CREAT
    | os.O_EXCL
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)


class _DirectoryNameCollision(ValueError):
    """A randomly generated directory name already exists."""


@dataclass(frozen=True)
class CompressionResult:
    source_bytes: int
    compressed_bytes: int


@dataclass(frozen=True)
class FileSource:
    directory_fd: int
    name: str
    display_path: str
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True)
class FileDestination:
    directory_fd: int
    name: str
    display_path: str


@dataclass(frozen=True)
class FileSeal:
    relative: Path
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int
    sha256: str


@dataclass(frozen=True)
class BuildAnchors:
    source_path: Path
    source_fd: int
    source_directories: tuple[tuple[int, str, int, str], ...]
    absent_source_directories: tuple[tuple[int, str, str], ...]
    source_file_sets: tuple[tuple[int, str, tuple[FileSource, ...]], ...]
    output_parent_path: Path
    output_parent_fd: int


def _metadata_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _directory_identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def _directory_fd_is_within(ancestor_fd: int, candidate_fd: int) -> bool:
    """Return whether candidate is ancestor itself or one of its descendants."""

    try:
        ancestor_identity = _directory_identity(os.fstat(ancestor_fd))
        current_fd = os.dup(candidate_fd)
    except OSError as error:
        raise ValueError("could not verify output directory ancestry") from error
    try:
        for _ in range(256):
            try:
                current_identity = _directory_identity(os.fstat(current_fd))
            except OSError as error:
                raise ValueError(
                    "could not verify output directory ancestry"
                ) from error
            if current_identity == ancestor_identity:
                return True
            try:
                parent_fd = os.open("..", _DIRECTORY_FLAGS, dir_fd=current_fd)
            except OSError as error:
                raise ValueError(
                    "could not verify output directory ancestry"
                ) from error
            try:
                parent_identity = _directory_identity(os.fstat(parent_fd))
            except BaseException:
                os.close(parent_fd)
                raise
            if parent_identity == current_identity:
                os.close(parent_fd)
                return False
            os.close(current_fd)
            current_fd = parent_fd
        raise ValueError("output directory ancestry exceeds its depth limit")
    finally:
        os.close(current_fd)


def _single_component(name: str, label: str) -> None:
    if not name or name in {".", ".."} or "/" in name or "\0" in name:
        raise ValueError(f"{label} has an invalid name")


def _open_directory(path: Path, label: str) -> int:
    try:
        descriptor = os.open(os.fspath(path), _DIRECTORY_FLAGS)
    except OSError as error:
        raise ValueError(f"could not securely open {label}: {path}") from error
    try:
        if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise ValueError(f"{label} is not a directory: {path}")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_directory_at(parent_fd: int, name: str, label: str) -> int:
    _single_component(name, label)
    try:
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as error:
        raise ValueError(f"could not securely open {label}: {name}") from error
    try:
        if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise ValueError(f"{label} is not a directory: {name}")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_optional_directory_at(parent_fd: int, name: str, label: str) -> int | None:
    _single_component(name, label)
    try:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ValueError(f"could not inspect {label}: {name}") from error
    if not stat.S_ISDIR(before.st_mode):
        raise ValueError(f"{label} must be a regular directory: {name}")
    descriptor = _open_directory_at(parent_fd, name, label)
    try:
        if _directory_identity(before) != _directory_identity(os.fstat(descriptor)):
            raise ValueError(f"{label} changed during artifact build")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _fsync_directory(directory_fd: int, label: str) -> None:
    try:
        os.fsync(directory_fd)
    except OSError as error:
        raise ValueError(f"could not durably synchronize {label}") from error


def _require_path_identity(path: Path, directory_fd: int, label: str) -> None:
    try:
        named = os.stat(path, follow_symlinks=False)
        opened = os.fstat(directory_fd)
    except OSError as error:
        raise ValueError(f"{label} changed during artifact build") from error
    if (
        not stat.S_ISDIR(named.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or _directory_identity(named) != _directory_identity(opened)
    ):
        raise ValueError(f"{label} changed during artifact build")


def _require_named_directory_identity(
    parent_fd: int,
    name: str,
    directory_fd: int,
    label: str,
) -> None:
    try:
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        opened = os.fstat(directory_fd)
    except OSError as error:
        raise ValueError(f"{label} changed during artifact build") from error
    if (
        not stat.S_ISDIR(named.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or _directory_identity(named) != _directory_identity(opened)
    ):
        raise ValueError(f"{label} changed during artifact build")


def _require_named_directory_absent(parent_fd: int, name: str, label: str) -> None:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    except OSError as error:
        raise ValueError(f"could not inspect {label}") from error
    raise ValueError(f"{label} changed during artifact build")


def _verify_source_file_set(
    directory_fd: int,
    label: str,
    sources: tuple[FileSource, ...],
) -> None:
    expected = {source.name: source for source in sources}
    try:
        entries = sorted(os.scandir(directory_fd), key=lambda entry: entry.name)
    except OSError as error:
        raise ValueError(f"could not rescan {label}") from error
    if {entry.name for entry in entries} != set(expected):
        raise ValueError(f"{label} changed during artifact build")
    for entry in entries:
        source = expected[entry.name]
        try:
            metadata = os.stat(
                entry.name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError as error:
            raise ValueError(f"{label} changed during artifact build") from error
        expected_identity = (
            source.device,
            source.inode,
            source.size,
            source.modified_ns,
            source.changed_ns,
        )
        if (
            not stat.S_ISREG(metadata.st_mode)
            or _metadata_identity(metadata) != expected_identity
        ):
            raise ValueError(f"{label} changed during artifact build")


def _artifact_checkpoint(label: str, anchors: BuildAnchors) -> None:
    """Test seam invoked before anchor revalidation at publication checkpoints."""

    del label, anchors


def _validate_artifact_anchors(anchors: BuildAnchors) -> None:
    _require_path_identity(
        anchors.source_path,
        anchors.source_fd,
        "source repository root",
    )
    for parent_fd, name, directory_fd, label in anchors.source_directories:
        _require_named_directory_identity(parent_fd, name, directory_fd, label)
    for parent_fd, name, label in anchors.absent_source_directories:
        _require_named_directory_absent(parent_fd, name, label)
    for directory_fd, label, sources in anchors.source_file_sets:
        _verify_source_file_set(directory_fd, label, sources)
    _require_path_identity(
        anchors.output_parent_path,
        anchors.output_parent_fd,
        "artifact output parent",
    )


def _run_artifact_checkpoint(label: str, anchors: BuildAnchors) -> None:
    _artifact_checkpoint(label, anchors)
    _validate_artifact_anchors(anchors)


def _open_verified_regular_file_at(
    directory_fd: int,
    name: str,
    label: str,
) -> tuple[int, os.stat_result]:
    _single_component(name, label)
    try:
        before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        descriptor = os.open(name, _FILE_READ_FLAGS, dir_fd=directory_fd)
    except OSError as error:
        raise ValueError(f"{label} must be a regular file: {name}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _metadata_identity(before) != _metadata_identity(opened)
        ):
            raise ValueError(f"{label} changed during artifact build: {name}")
        return descriptor, opened
    except BaseException:
        os.close(descriptor)
        raise


def bounded_regular_bytes_at(
    directory_fd: int,
    name: str,
    *,
    maximum: int,
    label: str,
    display_path: str,
) -> bytes:
    descriptor, before = _open_verified_regular_file_at(directory_fd, name, label)
    try:
        if before.st_size > maximum:
            raise ValueError(f"{label} exceeds its byte limit: {display_path}")
        chunks: list[bytes] = []
        total = 0
        while True:
            block = os.read(descriptor, min(1024 * 1024, maximum + 1 - total))
            if not block:
                break
            total += len(block)
            if total > maximum:
                raise ValueError(f"{label} exceeds its byte limit: {display_path}")
            chunks.append(block)
        after = os.fstat(descriptor)
        if (
            _metadata_identity(before) != _metadata_identity(after)
            or total != before.st_size
        ):
            raise ValueError(f"{label} changed while being read: {display_path}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, payload: bytes, label: str) -> None:
    view = memoryview(payload)
    while view:
        try:
            written = os.write(descriptor, view)
        except OSError as error:
            raise ValueError(f"could not write {label}") from error
        if written < 1:
            raise ValueError(f"could not write {label}")
        view = view[written:]


def normalized_write(payload: bytes, destination: FileDestination) -> int:
    try:
        descriptor = os.open(
            destination.name,
            _FILE_WRITE_FLAGS,
            0o600,
            dir_fd=destination.directory_fd,
        )
    except OSError as error:
        raise ValueError(
            f"could not create artifact file: {destination.display_path}"
        ) from error
    try:
        _write_all(descriptor, payload, destination.display_path)
        os.fchmod(descriptor, 0o644)
        os.utime(descriptor, (0, 0))
        os.fsync(descriptor)
        return len(payload)
    finally:
        os.close(descriptor)


def _open_compression_destination(destination: FileDestination) -> int:
    try:
        return os.open(
            destination.name,
            _FILE_WRITE_FLAGS,
            0o600,
            dir_fd=destination.directory_fd,
        )
    except OSError as error:
        raise ValueError(
            f"could not create artifact file: {destination.display_path}"
        ) from error


def _duplicate_binary_writer(descriptor: int):
    duplicate = os.dup(descriptor)
    try:
        return os.fdopen(duplicate, "wb")
    except BaseException:
        os.close(duplicate)
        raise


def _finish_compression_destination(
    descriptor: int,
    destination: FileDestination,
) -> int:
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(
            f"compressed artifact is not regular: {destination.display_path}"
        )
    os.fchmod(descriptor, 0o644)
    os.utime(descriptor, (0, 0))
    os.fsync(descriptor)
    return metadata.st_size


def gzip_file(
    source: FileSource,
    destination: FileDestination,
    *,
    compresslevel: int,
) -> CompressionResult:
    source_fd, before = _open_verified_regular_file_at(
        source.directory_fd,
        source.name,
        "data JSON",
    )
    destination_fd: int | None = None
    try:
        expected_identity = (
            source.device,
            source.inode,
            source.size,
            source.modified_ns,
            source.changed_ns,
        )
        if _metadata_identity(before) != expected_identity:
            raise ValueError(
                f"data JSON changed before compression: {source.display_path}"
            )
        destination_fd = _open_compression_destination(destination)
        source_bytes = 0
        raw_output = _duplicate_binary_writer(destination_fd)
        with raw_output:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=raw_output,
                compresslevel=compresslevel,
                mtime=0,
            ) as gzip_output:
                while True:
                    block = os.read(source_fd, 1024 * 1024)
                    if not block:
                        break
                    source_bytes += len(block)
                    if source_bytes > MAX_DATA_JSON_BYTES:
                        raise ValueError(
                            f"data JSON exceeds its byte limit: {source.display_path}"
                        )
                    gzip_output.write(block)
            raw_output.flush()
        after = os.fstat(source_fd)
        if (
            _metadata_identity(after) != expected_identity
            or source_bytes != source.size
        ):
            raise ValueError(
                f"data JSON changed during compression: {source.display_path}"
            )
        compressed_bytes = _finish_compression_destination(
            destination_fd,
            destination,
        )
        if source_bytes > max(1, compressed_bytes) * MAX_GZIP_COMPRESSION_RATIO:
            raise ValueError(
                f"gzip compression ratio exceeds its limit: {source.display_path}"
            )
        return CompressionResult(
            source_bytes=source_bytes,
            compressed_bytes=compressed_bytes,
        )
    finally:
        if destination_fd is not None:
            os.close(destination_fd)
        os.close(source_fd)


def gzip_bytes(
    payload: bytes,
    destination: FileDestination,
    *,
    compresslevel: int,
) -> CompressionResult:
    if len(payload) > MAX_DATA_JSON_BYTES:
        raise ValueError(
            f"data JSON exceeds its byte limit: {destination.display_path}"
        )
    destination_fd = _open_compression_destination(destination)
    try:
        raw_output = _duplicate_binary_writer(destination_fd)
        with raw_output:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=raw_output,
                compresslevel=compresslevel,
                mtime=0,
            ) as gzip_output:
                gzip_output.write(payload)
            raw_output.flush()
        compressed_bytes = _finish_compression_destination(
            destination_fd,
            destination,
        )
        if len(payload) > max(1, compressed_bytes) * MAX_GZIP_COMPRESSION_RATIO:
            raise ValueError(
                f"gzip compression ratio exceeds its limit: {destination.display_path}"
            )
        return CompressionResult(
            source_bytes=len(payload),
            compressed_bytes=compressed_bytes,
        )
    finally:
        os.close(destination_fd)


def admit_data_directory(
    directory_fd: int,
    *,
    display_path: str,
    filename_pattern: re.Pattern[str],
    allow_empty: bool,
) -> list[FileSource]:
    admitted: list[FileSource] = []
    try:
        entries = sorted(os.scandir(directory_fd), key=lambda entry: entry.name)
    except OSError as error:
        raise ValueError(f"cannot scan data directory: {display_path}") from error
    for entry in entries:
        if len(admitted) >= MAX_DATA_FILES_PER_DIRECTORY:
            raise ValueError(f"data file count exceeds its limit: {display_path}")
        if (
            not filename_pattern.fullmatch(entry.name)
            or entry.is_symlink()
            or not entry.is_file(follow_symlinks=False)
        ):
            raise ValueError(f"unexpected entry in {display_path}: {entry.name}")
        try:
            metadata = os.stat(entry.name, dir_fd=directory_fd, follow_symlinks=False)
        except OSError as error:
            raise ValueError(
                f"cannot inspect data JSON: {display_path}/{entry.name}"
            ) from error
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"unexpected entry in {display_path}: {entry.name}")
        if metadata.st_size > MAX_DATA_JSON_BYTES:
            raise ValueError(
                f"data JSON exceeds its byte limit: {display_path}/{entry.name}"
            )
        admitted.append(
            FileSource(
                directory_fd=directory_fd,
                name=entry.name,
                display_path=f"{display_path}/{entry.name}",
                device=metadata.st_dev,
                inode=metadata.st_ino,
                size=metadata.st_size,
                modified_ns=metadata.st_mtime_ns,
                changed_ns=metadata.st_ctime_ns,
            )
        )
    if not admitted and not allow_empty:
        raise ValueError(f"no JSON payloads found in {display_path}")
    return admitted


def _create_directory_at(parent_fd: int, name: str, label: str) -> int:
    _single_component(name, label)
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
    except FileExistsError as error:
        raise _DirectoryNameCollision(f"could not create {label}: {name}") from error
    except OSError as error:
        raise ValueError(f"could not create {label}: {name}") from error
    descriptor: int | None = None
    try:
        descriptor = _open_directory_at(parent_fd, name, label)
        os.fchmod(descriptor, 0o700)
        _fsync_directory(descriptor, label)
        _fsync_directory(parent_fd, f"{label} parent")
        return descriptor
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        raise


def _create_random_directory_at(
    parent_fd: int,
    *,
    prefix: str,
    label: str,
) -> tuple[str, int]:
    for _ in range(100):
        name = f"{prefix}{secrets.token_hex(16)}"
        try:
            descriptor = _create_directory_at(parent_fd, name, label)
        except _DirectoryNameCollision:
            continue
        return name, descriptor
    raise ValueError(f"could not allocate {label}")


def _replace_directory_between(
    source_parent_fd: int,
    source: str,
    target_parent_fd: int,
    target: str,
    label: str,
) -> None:
    try:
        os.replace(
            source,
            target,
            src_dir_fd=source_parent_fd,
            dst_dir_fd=target_parent_fd,
        )
    except (NotImplementedError, TypeError) as error:
        raise ValueError(
            "secure descriptor-relative artifact publication is unavailable"
        ) from error
    except OSError as error:
        raise ValueError(f"could not replace {label}") from error
    _fsync_directory(source_parent_fd, f"{label} source parent")
    if target_parent_fd != source_parent_fd:
        _fsync_directory(target_parent_fd, f"{label} target parent")


def _replace_directory_at(parent_fd: int, source: str, target: str, label: str) -> None:
    _replace_directory_between(parent_fd, source, parent_fd, target, label)


def _create_artifact_directories(
    stage_fd: int,
    directory_paths: set[Path],
) -> dict[Path, int]:
    directories: dict[Path, int] = {Path("."): stage_fd}
    try:
        for relative in sorted(
            directory_paths,
            key=lambda item: (len(item.parts), item.as_posix()),
        ):
            parent_relative = (
                relative.parent if relative.parent != Path("") else Path(".")
            )
            parent_fd = directories[parent_relative]
            directories[relative] = _create_directory_at(
                parent_fd,
                relative.name,
                f"artifact directory {relative.as_posix()}",
            )
        return directories
    except BaseException:
        for relative, descriptor in sorted(
            directories.items(),
            key=lambda item: len(item[0].parts),
            reverse=True,
        ):
            if relative != Path("."):
                os.close(descriptor)
        raise


def _destination(
    directories: dict[Path, int],
    relative: Path,
) -> FileDestination:
    parent = relative.parent if relative.parent != Path("") else Path(".")
    return FileDestination(
        directory_fd=directories[parent],
        name=relative.name,
        display_path=relative.as_posix(),
    )


def _verify_artifact_topology(
    directories: dict[Path, int],
    expected_directories: set[Path],
    expected_files: set[Path],
) -> None:
    all_directories = {Path("."), *expected_directories}
    for relative, directory_fd in directories.items():
        expected_child_directories = {
            path.name
            for path in expected_directories
            if (path.parent if path.parent != Path("") else Path(".")) == relative
        }
        expected_child_files = {
            path.name
            for path in expected_files
            if (path.parent if path.parent != Path("") else Path(".")) == relative
        }
        try:
            entries = sorted(os.scandir(directory_fd), key=lambda entry: entry.name)
        except OSError as error:
            raise ValueError("could not scan staged Pages artifact") from error
        actual_names = {entry.name for entry in entries}
        if actual_names != expected_child_directories | expected_child_files:
            raise ValueError("artifact file topology changed during packaging")
        for entry in entries:
            metadata = os.stat(
                entry.name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            child = relative / entry.name if relative != Path(".") else Path(entry.name)
            if entry.name in expected_child_directories:
                child_fd = directories[child]
                if not stat.S_ISDIR(metadata.st_mode) or _directory_identity(
                    metadata
                ) != _directory_identity(os.fstat(child_fd)):
                    raise ValueError(
                        "artifact directory topology changed during packaging"
                    )
            elif not stat.S_ISREG(metadata.st_mode):
                raise ValueError("Pages artifact contains unsupported entry")
    if set(directories) != all_directories:
        raise ValueError("artifact directory topology changed during packaging")


def _seal_artifact_files(
    directories: dict[Path, int],
    relative_files: set[Path],
) -> tuple[list[FileSeal], str, int]:
    tree_digest = hashlib.sha256()
    seals: list[FileSeal] = []
    total_bytes = 0
    for relative in sorted(relative_files, key=lambda item: item.as_posix()):
        destination = _destination(directories, relative)
        descriptor, before = _open_verified_regular_file_at(
            destination.directory_fd,
            destination.name,
            "artifact file",
        )
        file_digest = hashlib.sha256()
        observed = 0
        tree_digest.update(relative.as_posix().encode("utf-8"))
        tree_digest.update(b"\0")
        try:
            while True:
                block = os.read(descriptor, 1024 * 1024)
                if not block:
                    break
                observed += len(block)
                file_digest.update(block)
                tree_digest.update(block)
            after = os.fstat(descriptor)
            if (
                _metadata_identity(before) != _metadata_identity(after)
                or observed != before.st_size
            ):
                raise ValueError("artifact file changed while being sealed")
        finally:
            os.close(descriptor)
        tree_digest.update(b"\0")
        total_bytes += observed
        seals.append(
            FileSeal(
                relative=relative,
                device=before.st_dev,
                inode=before.st_ino,
                size=before.st_size,
                modified_ns=before.st_mtime_ns,
                changed_ns=before.st_ctime_ns,
                sha256=file_digest.hexdigest(),
            )
        )
    return seals, tree_digest.hexdigest(), total_bytes


def _verify_file_seals(
    directories: dict[Path, int],
    seals: list[FileSeal],
) -> None:
    for seal in seals:
        destination = _destination(directories, seal.relative)
        descriptor, metadata = _open_verified_regular_file_at(
            destination.directory_fd,
            destination.name,
            "artifact file",
        )
        try:
            expected = (
                seal.device,
                seal.inode,
                seal.size,
                seal.modified_ns,
                seal.changed_ns,
            )
            if _metadata_identity(metadata) != expected:
                raise ValueError("artifact file changed after sealing")
        finally:
            os.close(descriptor)


def deterministic_tar_size(
    directories: dict[Path, int],
    expected_directories: set[Path],
    seals: list[FileSeal],
    *,
    maximum: int,
) -> int:
    """Materialize the upload tar from retained descriptors within its ceiling."""

    class BoundedArchiveWriter:
        def __init__(self, raw_file: object, limit: int) -> None:
            self.raw_file = raw_file
            self.limit = limit

        def write(self, payload: bytes) -> int:
            position = self.tell()
            if position + len(payload) >= self.limit:
                raise ValueError(
                    "Pages archive is too large: "
                    f"at least {position + len(payload):,} bytes >= {self.limit:,} bytes"
                )
            return self.raw_file.write(payload)  # type: ignore[attr-defined]

        def tell(self) -> int:
            return self.raw_file.tell()  # type: ignore[attr-defined,no-any-return]

        def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
            return self.raw_file.seek(offset, whence)  # type: ignore[attr-defined,no-any-return]

        def flush(self) -> None:
            self.raw_file.flush()  # type: ignore[attr-defined]

    seal_map = {seal.relative: seal for seal in seals}
    with tempfile.TemporaryFile(mode="w+b") as temporary:
        bounded = BoundedArchiveWriter(temporary, maximum)
        with tarfile.open(
            fileobj=bounded,  # pyright: ignore[reportArgumentType]
            mode="w",
            format=tarfile.USTAR_FORMAT,
        ) as archive:
            entries = sorted(
                [*expected_directories, *seal_map],
                key=lambda path: path.as_posix(),
            )
            for relative in entries:
                info = tarfile.TarInfo(relative.as_posix())
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                info.mtime = 0
                if relative in expected_directories:
                    info.type = tarfile.DIRTYPE
                    info.mode = 0o755
                    archive.addfile(info)
                    continue
                seal = seal_map[relative]
                destination = _destination(directories, relative)
                descriptor, metadata = _open_verified_regular_file_at(
                    destination.directory_fd,
                    destination.name,
                    "artifact file",
                )
                try:
                    expected = (
                        seal.device,
                        seal.inode,
                        seal.size,
                        seal.modified_ns,
                        seal.changed_ns,
                    )
                    if _metadata_identity(metadata) != expected:
                        raise ValueError(
                            "artifact file changed before archive measurement"
                        )
                    info.type = tarfile.REGTYPE
                    info.mode = 0o644
                    info.size = seal.size
                    with os.fdopen(os.dup(descriptor), "rb") as file_object:
                        archive.addfile(info, file_object)
                    if _metadata_identity(os.fstat(descriptor)) != expected:
                        raise ValueError(
                            "artifact file changed during archive measurement"
                        )
                finally:
                    os.close(descriptor)
        temporary.seek(0, os.SEEK_END)
        return temporary.tell()


def _normalize_artifact_directories(directories: dict[Path, int]) -> None:
    for relative in sorted(
        directories,
        key=lambda item: (len(item.parts), item.as_posix()),
        reverse=True,
    ):
        directory_fd = directories[relative]
        os.fchmod(directory_fd, 0o755)
        os.utime(directory_fd, (0, 0))
        _fsync_directory(directory_fd, f"artifact directory {relative.as_posix()}")


def _admit_output_target(
    output_parent_fd: int,
    output_name: str,
) -> tuple[int, bool]:
    try:
        metadata = os.stat(output_name, dir_fd=output_parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        try:
            descriptor = _create_directory_at(
                output_parent_fd,
                output_name,
                "reserved output directory",
            )
        except ValueError as error:
            raise ValueError("could not reserve output directory") from error
        return descriptor, True
    except OSError as error:
        raise ValueError("could not inspect output directory") from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("output directory must be absent or empty")
    descriptor = _open_directory_at(output_parent_fd, output_name, "output directory")
    try:
        if _directory_identity(metadata) != _directory_identity(os.fstat(descriptor)):
            raise ValueError("output directory changed during artifact admission")
        if any(os.scandir(descriptor)):
            raise ValueError("output directory must be absent or empty")
        return descriptor, False
    except BaseException:
        os.close(descriptor)
        raise


def _require_output_target_state(
    output_parent_fd: int,
    output_name: str,
    original_target_fd: int,
) -> None:
    _require_named_directory_identity(
        output_parent_fd,
        output_name,
        original_target_fd,
        "output directory",
    )
    if any(os.scandir(original_target_fd)):
        raise ValueError("output directory changed during artifact build")


def build_artifact(
    *,
    source_root: Path,
    output_root: Path,
    source_sha: str,
    dataset_id: str,
    workers: int,
    compresslevel: int,
    max_archive_bytes: int,
) -> dict[str, int | str]:
    source_lexical = Path(os.path.abspath(os.fspath(source_root)))
    output_lexical = Path(os.path.abspath(os.fspath(output_root)))
    output_name = output_lexical.name
    _single_component(output_name, "output directory")
    try:
        source_path = source_lexical.resolve(strict=True)
        output_parent_path = output_lexical.parent.resolve(strict=True)
    except OSError as error:
        raise ValueError("source and output parent directories must exist") from error
    output_path = output_parent_path / output_name
    if not SHA_RE.fullmatch(source_sha):
        raise ValueError("source SHA must be exactly 40 lowercase hex characters")
    if not DATASET_ID_RE.fullmatch(dataset_id):
        raise ValueError("dataset ID must be exactly 64 lowercase hex characters")
    if workers < 1:
        raise ValueError("workers must be at least 1")
    if workers > MAX_COMPRESSION_WORKERS:
        raise ValueError(f"workers must not exceed {MAX_COMPRESSION_WORKERS}")
    if not 1 <= compresslevel <= 9:
        raise ValueError("compress level must be between 1 and 9")
    if max_archive_bytes < 1:
        raise ValueError("maximum archive size must be positive")
    if output_path == source_path or source_path in output_path.parents:
        raise ValueError("output directory must be outside the source repository")

    source_fd = _open_directory(source_path, "source repository root")
    output_parent_fd: int | None = None
    data_fd: int | None = None
    funds_fd: int | None = None
    stocks_fd: int | None = None
    insiders_fd: int | None = None
    insiders_locked = False
    insider_public_fd: int | None = None
    original_target_fd: int | None = None
    original_target_created = False
    stage_fd: int | None = None
    stage_name: str | None = None
    artifact_published = False
    directory_fds: dict[Path, int] = {}
    summary: dict[str, int | str] | None = None
    try:
        output_parent_fd = _open_directory(output_parent_path, "artifact output parent")
        if _directory_fd_is_within(source_fd, output_parent_fd):
            raise ValueError("output directory must be outside the source repository")
        data_fd = _open_directory_at(source_fd, "data", "source data root")
        funds_fd = _open_directory_at(data_fd, "funds", "fund data root")
        stocks_fd = _open_directory_at(data_fd, "stocks", "stock data root")
        source_directories: list[tuple[int, str, int, str]] = [
            (source_fd, "data", data_fd, "source data root"),
            (data_fd, "funds", funds_fd, "fund data root"),
            (data_fd, "stocks", stocks_fd, "stock data root"),
        ]
        absent_source_directories: list[tuple[int, str, str]] = []
        insiders_fd = _open_optional_directory_at(
            data_fd,
            "insiders",
            "insider data root",
        )
        if insiders_fd is None:
            absent_source_directories.append((data_fd, "insiders", "insider data root"))
        else:
            source_directories.append(
                (data_fd, "insiders", insiders_fd, "insider data root")
            )
            try:
                fcntl.flock(insiders_fd, fcntl.LOCK_SH)
                insiders_locked = True
            except OSError as error:
                raise ValueError("public insider projection lock failed") from error
            insider_public_fd = _open_optional_directory_at(
                insiders_fd,
                "public",
                "public insider projection root",
            )
            if insider_public_fd is None:
                absent_source_directories.append(
                    (insiders_fd, "public", "public insider projection root")
                )
            else:
                source_directories.append(
                    (
                        insiders_fd,
                        "public",
                        insider_public_fd,
                        "public insider projection root",
                    )
                )
        anchors = BuildAnchors(
            source_path=source_path,
            source_fd=source_fd,
            source_directories=tuple(source_directories),
            absent_source_directories=tuple(absent_source_directories),
            source_file_sets=(),
            output_parent_path=output_parent_path,
            output_parent_fd=output_parent_fd,
        )
        _run_artifact_checkpoint("after_admission", anchors)
        original_target_fd, original_target_created = _admit_output_target(
            output_parent_fd,
            output_name,
        )

        static_payloads: dict[Path, bytes] = {}
        for relative in (*STATIC_FILES, *INDEX_FILES):
            if relative.parent == Path("."):
                parent_fd = source_fd
            elif relative.parent == Path("data"):
                parent_fd = data_fd
            else:
                raise ValueError(f"unsupported static artifact path: {relative}")
            static_payloads[relative] = bounded_regular_bytes_at(
                parent_fd,
                relative.name,
                maximum=MAX_STATIC_FILE_BYTES,
                label="static file",
                display_path=relative.as_posix(),
            )
        try:
            html = static_payloads[Path("index.html")].decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("index.html must be valid UTF-8") from error
        loader_tag = re.search(
            r"""<script\b[^>]*\bsrc=["']site-data-loader\.js["'][^>]*>""",
            html,
        )
        application_offset = html.find("const DATA_CONTRACT_VERSION")
        if loader_tag is None:
            raise ValueError(
                "index.html must load site-data-loader.js before Pages packaging"
            )
        if (
            application_offset < 0
            or loader_tag.start() > application_offset
            or re.search(r"\b(?:async|defer)\b", loader_tag.group(0), re.IGNORECASE)
        ):
            raise ValueError(
                "site-data-loader.js must load synchronously before the application"
            )

        include_insider_publication = insider_public_fd is not None
        insider_snapshot: dict[str, bytes] = {}
        if insider_public_fd is not None:
            insider_snapshot, insider_errors = (
                read_validated_insider_public_snapshot_fd(insider_public_fd)
            )
            if insider_errors:
                rendered = "; ".join(insider_errors[:10])
                if len(insider_errors) > 10:
                    rendered += f"; ... {len(insider_errors) - 10} more"
                raise ValueError(
                    "public insider projection failed validation: " + rendered
                )

        admitted_directories: dict[Path, list[FileSource]] = {
            Path("data/funds"): admit_data_directory(
                funds_fd,
                display_path="data/funds",
                filename_pattern=FUND_SOURCE_NAME_RE,
                allow_empty=False,
            ),
            Path("data/stocks"): admit_data_directory(
                stocks_fd,
                display_path="data/stocks",
                filename_pattern=STOCK_SOURCE_NAME_RE,
                allow_empty=False,
            ),
        }
        directory_counts = {
            "funds": len(admitted_directories[Path("data/funds")]),
            "stocks": len(admitted_directories[Path("data/stocks")]),
        }
        anchors = BuildAnchors(
            source_path=anchors.source_path,
            source_fd=anchors.source_fd,
            source_directories=anchors.source_directories,
            absent_source_directories=anchors.absent_source_directories,
            source_file_sets=(
                (
                    funds_fd,
                    "fund data root",
                    tuple(admitted_directories[Path("data/funds")]),
                ),
                (
                    stocks_fd,
                    "stock data root",
                    tuple(admitted_directories[Path("data/stocks")]),
                ),
            ),
            output_parent_path=anchors.output_parent_path,
            output_parent_fd=anchors.output_parent_fd,
        )
        insider_sources: dict[Path, list[tuple[str, bytes]]] = {}
        if include_insider_publication:
            for relative_directory in INSIDER_COMPRESSED_DIRECTORIES:
                snapshot_directory = relative_directory.relative_to(
                    INSIDER_PUBLIC_ROOT
                ).as_posix()
                sources = sorted(
                    (relative, payload)
                    for relative, payload in insider_snapshot.items()
                    if Path(relative).parent.as_posix() == snapshot_directory
                )
                if len(sources) > MAX_DATA_FILES_PER_DIRECTORY:
                    raise ValueError(
                        f"data file count exceeds its limit: {relative_directory}"
                    )
                for relative, payload in sources:
                    if len(payload) > MAX_DATA_JSON_BYTES:
                        raise ValueError(
                            "data JSON exceeds its byte limit: "
                            f"{relative_directory / Path(relative).name}"
                        )
                insider_sources[relative_directory] = sources
                directory_counts[f"insider_{relative_directory.name}"] = len(sources)

        static_source_bytes = sum(len(payload) for payload in static_payloads.values())
        if include_insider_publication:
            static_source_bytes += len(insider_snapshot["manifest.json"])
        compressed_source_bytes_expected = sum(
            source.size
            for sources in admitted_directories.values()
            for source in sources
        ) + sum(
            len(payload)
            for sources in insider_sources.values()
            for _, payload in sources
        )
        total_source_bytes = static_source_bytes + compressed_source_bytes_expected
        if total_source_bytes > MAX_TOTAL_SOURCE_BYTES:
            raise ValueError(
                "total source bytes exceed their limit: "
                f"{total_source_bytes:,} > {MAX_TOTAL_SOURCE_BYTES:,}"
            )
        compression_task_count = sum(
            len(sources) for sources in admitted_directories.values()
        ) + sum(len(sources) for sources in insider_sources.values())
        expected_artifact_files = (
            len(static_payloads)
            + compression_task_count
            + 1
            + (1 if include_insider_publication else 0)
        )
        if expected_artifact_files > MAX_ARTIFACT_FILES:
            raise ValueError(
                "artifact file count exceeds its limit: "
                f"{expected_artifact_files:,} > {MAX_ARTIFACT_FILES:,}"
            )
        _run_artifact_checkpoint("after_source_snapshot", anchors)

        stage_name, stage_fd = _create_random_directory_at(
            output_parent_fd,
            prefix=f".{output_name}.prepare-",
            label="artifact staging directory",
        )
        expected_directories: set[Path] = {
            Path("data"),
            Path("data/funds"),
            Path("data/stocks"),
        }
        if include_insider_publication:
            expected_directories.update(
                {
                    Path("data/insiders"),
                    Path("data/insiders/public"),
                    *INSIDER_COMPRESSED_DIRECTORIES,
                }
            )
        directory_fds = _create_artifact_directories(stage_fd, expected_directories)
        expected_files: set[Path] = set(static_payloads)
        for relative, payload in sorted(static_payloads.items()):
            normalized_write(payload, _destination(directory_fds, relative))
        if include_insider_publication:
            normalized_write(
                insider_snapshot["manifest.json"],
                _destination(directory_fds, INSIDER_PUBLIC_MANIFEST),
            )
            expected_files.add(INSIDER_PUBLIC_MANIFEST)

        compression_tasks: list[tuple[FileSource | bytes, FileDestination]] = []
        for relative_directory, sources in admitted_directories.items():
            for source in sources:
                relative = relative_directory / f"{source.name}.gz"
                expected_files.add(relative)
                compression_tasks.append(
                    (source, _destination(directory_fds, relative))
                )
        for relative_directory, sources in insider_sources.items():
            for relative_source, payload in sources:
                relative = relative_directory / f"{Path(relative_source).name}.gz"
                expected_files.add(relative)
                compression_tasks.append(
                    (payload, _destination(directory_fds, relative))
                )

        def compress(
            task: tuple[FileSource | bytes, FileDestination],
        ) -> CompressionResult:
            source, destination = task
            if isinstance(source, bytes):
                return gzip_bytes(
                    source,
                    destination,
                    compresslevel=compresslevel,
                )
            return gzip_file(
                source,
                destination,
                compresslevel=compresslevel,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(compress, compression_tasks))

        compressed_source_bytes = sum(result.source_bytes for result in results)
        compressed_payload_bytes = sum(result.compressed_bytes for result in results)
        if compressed_source_bytes != compressed_source_bytes_expected:
            raise ValueError("source bytes changed during artifact compression")
        _verify_artifact_topology(
            directory_fds,
            expected_directories,
            expected_files,
        )
        _, tree_sha256, _ = _seal_artifact_files(directory_fds, expected_files)
        manifest = {
            "artifact_contract_version": 2,
            "compressed_payload_bytes": compressed_payload_bytes,
            "dataset_id": dataset_id,
            "fund_payloads": directory_counts["funds"],
            "insider_filing_payloads": directory_counts.get(
                "insider_filings",
                0,
            ),
            "insider_security_payloads": directory_counts.get(
                "insider_securities",
                0,
            ),
            "max_artifact_files": MAX_ARTIFACT_FILES,
            "max_compression_workers": MAX_COMPRESSION_WORKERS,
            "max_data_files_per_directory": MAX_DATA_FILES_PER_DIRECTORY,
            "max_data_json_bytes": MAX_DATA_JSON_BYTES,
            "max_gzip_compression_ratio": MAX_GZIP_COMPRESSION_RATIO,
            "max_static_file_bytes": MAX_STATIC_FILE_BYTES,
            "max_total_source_bytes": MAX_TOTAL_SOURCE_BYTES,
            "source_bytes": total_source_bytes,
            "source_sha": source_sha,
            "stock_payloads": directory_counts["stocks"],
            "tree_sha256": tree_sha256,
            "uncompressed_payload_bytes": compressed_source_bytes,
        }
        manifest_relative = Path("deployment-manifest.json")
        normalized_write(
            (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
            _destination(directory_fds, manifest_relative),
        )
        expected_files.add(manifest_relative)
        if len(expected_files) != expected_artifact_files:
            raise ValueError("artifact file topology changed during packaging")
        _normalize_artifact_directories(directory_fds)
        _verify_artifact_topology(
            directory_fds,
            expected_directories,
            expected_files,
        )
        full_seals, _, artifact_bytes = _seal_artifact_files(
            directory_fds,
            expected_files,
        )
        if len(full_seals) > MAX_ARTIFACT_FILES:
            raise ValueError(
                "artifact file count exceeds its limit: "
                f"{len(full_seals):,} > {MAX_ARTIFACT_FILES:,}"
            )
        archive_bytes = deterministic_tar_size(
            directory_fds,
            expected_directories,
            full_seals,
            maximum=max_archive_bytes,
        )
        if archive_bytes >= max_archive_bytes:
            raise ValueError(
                "Pages archive is too large: "
                f"{archive_bytes:,} bytes >= {max_archive_bytes:,} bytes"
            )
        summary = {
            **manifest,
            "archive_bytes": archive_bytes,
            "artifact_bytes": artifact_bytes,
            "artifact_files": len(full_seals),
            "max_archive_bytes": max_archive_bytes,
        }

        _run_artifact_checkpoint("before_commit", anchors)
        for relative, expected_payload in static_payloads.items():
            if relative.parent == Path("."):
                parent_fd = source_fd
            elif relative.parent == Path("data"):
                parent_fd = data_fd
            else:
                raise ValueError(f"unsupported static artifact path: {relative}")
            observed_payload = bounded_regular_bytes_at(
                parent_fd,
                relative.name,
                maximum=MAX_STATIC_FILE_BYTES,
                label="static file",
                display_path=relative.as_posix(),
            )
            if observed_payload != expected_payload:
                raise ValueError(
                    f"static file changed during artifact build: {relative.as_posix()}"
                )
        if insider_public_fd is not None:
            observed_insider_snapshot, observed_insider_errors = (
                read_validated_insider_public_snapshot_fd(insider_public_fd)
            )
            if observed_insider_errors or observed_insider_snapshot != insider_snapshot:
                raise ValueError(
                    "public insider projection changed during artifact build"
                )
        if original_target_fd is None:
            raise ValueError("output directory admission was lost")
        _require_output_target_state(
            output_parent_fd,
            output_name,
            original_target_fd,
        )
        _verify_file_seals(directory_fds, full_seals)
        _require_named_directory_identity(
            output_parent_fd,
            stage_name,
            stage_fd,
            "artifact staging directory",
        )
        _replace_directory_at(
            output_parent_fd,
            stage_name,
            output_name,
            "Pages artifact",
        )
        artifact_published = True
        _require_named_directory_identity(
            output_parent_fd,
            output_name,
            stage_fd,
            "published Pages artifact",
        )
        _run_artifact_checkpoint("before_return", anchors)
        _require_named_directory_identity(
            output_parent_fd,
            output_name,
            stage_fd,
            "published Pages artifact",
        )
        _verify_file_seals(directory_fds, full_seals)
        return summary
    except BaseException as error:
        rollback_error: BaseException | None = None
        try:
            if (
                output_parent_fd is not None
                and artifact_published
                and stage_fd is not None
                and stage_name is not None
            ):
                _require_named_directory_identity(
                    output_parent_fd,
                    output_name,
                    stage_fd,
                    "published Pages artifact",
                )
                _replace_directory_at(
                    output_parent_fd,
                    output_name,
                    stage_name,
                    "artifact rollback staging",
                )
                artifact_published = False
                if not original_target_created:
                    restored_target_fd = _create_directory_at(
                        output_parent_fd,
                        output_name,
                        "restored empty output directory",
                    )
                    os.close(restored_target_fd)
        except BaseException as caught:
            rollback_error = caught
        if rollback_error is not None:
            raise ValueError(f"artifact rollback failed: {rollback_error}") from error
        raise
    finally:
        for relative, descriptor in sorted(
            directory_fds.items(),
            key=lambda item: len(item[0].parts),
            reverse=True,
        ):
            if relative != Path("."):
                os.close(descriptor)
        if stage_fd is not None:
            os.close(stage_fd)
        if original_target_fd is not None:
            os.close(original_target_fd)
        if insider_public_fd is not None:
            os.close(insider_public_fd)
        if insiders_locked and insiders_fd is not None:
            try:
                fcntl.flock(insiders_fd, fcntl.LOCK_UN)
            except OSError:
                pass
        if insiders_fd is not None:
            os.close(insiders_fd)
        if stocks_fd is not None:
            os.close(stocks_fd)
        if funds_fd is not None:
            os.close(funds_fd)
        if data_fd is not None:
            os.close(data_fd)
        if output_parent_fd is not None:
            os.close(output_parent_fd)
        os.close(source_fd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=ROOT,
        help="repository root (default: inferred from this script)",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument(
        "--workers",
        type=int,
        default=min(8, os.cpu_count() or 1),
    )
    parser.add_argument("--compress-level", type=int, default=6)
    parser.add_argument(
        "--max-archive-bytes",
        type=int,
        default=DEFAULT_MAX_ARCHIVE_BYTES,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_artifact(
        source_root=args.source_root,
        output_root=args.output,
        source_sha=args.source_sha,
        dataset_id=args.dataset_id,
        workers=args.workers,
        compresslevel=args.compress_level,
        max_archive_bytes=args.max_archive_bytes,
    )
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
