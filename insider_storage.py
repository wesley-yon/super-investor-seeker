"""Immutable private-file storage seam for normalized ownership filings."""

from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import os
import re
import secrets
import stat
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from insider_contract import (
    MAX_NORMALIZED_JSON_BYTES,
    MAX_RAW_XML_BYTES,
    canonical_insider_json_bytes,
)
from insider_parser import InsiderParseError, raw_ownership_document


PRIVATE_INSIDER_ROOT = Path("data/insiders/private")
_ACCESSION_RE = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_PARSER_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_LIBC = ctypes.CDLL(None, use_errno=True)


class InsiderStorageError(ValueError):
    """Raised when a private insider artifact cannot be stored safely."""


class ImmutableInsiderStorageConflict(InsiderStorageError):
    """Raised when an immutable storage key already has different bytes."""


@dataclass(frozen=True, slots=True)
class StoredArtifact:
    path: Path
    sha256: str
    byte_count: int
    created: bool


def _validate_accession(accession_number: object) -> str:
    if type(accession_number) is not str or not _ACCESSION_RE.fullmatch(
        accession_number
    ):
        raise InsiderStorageError("accession number is invalid")
    return accession_number


def _validate_parser_version(parser_version: object) -> str:
    if type(parser_version) is not str or not _PARSER_VERSION_RE.fullmatch(
        parser_version
    ):
        raise InsiderStorageError("parser version is invalid")
    return parser_version


_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_FILE_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


def _unsafe_storage_error(kind: str, error: OSError) -> InsiderStorageError:
    return InsiderStorageError(f"private storage {kind} is unsafe: {error.strerror}")


def _rename_noreplace(
    directory_descriptor: int,
    source_name: str,
    target_name: str,
) -> None:
    try:
        if sys.platform == "darwin":
            native_rename = _LIBC.renameatx_np
            flag = 0x00000004  # RENAME_EXCL
        elif sys.platform.startswith("linux"):
            native_rename = _LIBC.renameat2
            flag = 0x00000001  # RENAME_NOREPLACE
        else:
            raise AttributeError
    except AttributeError as error:
        raise InsiderStorageError(
            "private storage requires atomic no-replace publication support"
        ) from error

    native_rename.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    native_rename.restype = ctypes.c_int
    ctypes.set_errno(0)
    result = native_rename(
        directory_descriptor,
        os.fsencode(source_name),
        directory_descriptor,
        os.fsencode(target_name),
        flag,
    )
    if result == 0:
        return
    error_number = ctypes.get_errno() or errno.EIO
    if error_number == errno.EEXIST:
        raise FileExistsError(
            error_number,
            os.strerror(error_number),
            target_name,
        )
    error = OSError(error_number, os.strerror(error_number), target_name)
    raise _unsafe_storage_error("publication", error) from error


def _open_child_directory(
    parent_descriptor: int,
    name: str,
    *,
    create: bool,
    restricted: bool,
) -> int:
    created = False
    try:
        descriptor = os.open(
            name,
            _DIRECTORY_OPEN_FLAGS,
            dir_fd=parent_descriptor,
        )
    except FileNotFoundError:
        if not create:
            raise
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_descriptor)
            created = True
            os.fsync(parent_descriptor)
        except FileExistsError:
            pass
        try:
            descriptor = os.open(
                name,
                _DIRECTORY_OPEN_FLAGS,
                dir_fd=parent_descriptor,
            )
        except OSError as error:
            raise _unsafe_storage_error("parent", error) from error
    except OSError as error:
        raise _unsafe_storage_error("parent", error) from error

    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode) or (
        restricted and metadata.st_mode & 0o077
    ):
        os.close(descriptor)
        raise InsiderStorageError("private storage parent is unsafe")
    if created:
        os.fchmod(descriptor, 0o700)
        os.fsync(descriptor)
    return descriptor


def _remove_safe_stale_temporary_artifacts(
    directory_descriptor: int,
    target_name: str,
) -> None:
    temporary_name = re.compile(
        rf"\A\.{re.escape(target_name)}\.tmp-[0-9a-f]{{24}}\Z"
    )
    removed = False
    try:
        names = os.listdir(directory_descriptor)
    except OSError as error:
        raise _unsafe_storage_error("temporary artifact", error) from error

    for name in names:
        if not temporary_name.fullmatch(name):
            continue
        try:
            descriptor = os.open(
                name,
                _FILE_READ_FLAGS,
                dir_fd=directory_descriptor,
            )
        except OSError as error:
            raise _unsafe_storage_error("temporary artifact", error) from error
        try:
            metadata = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.geteuid()
        ):
            raise InsiderStorageError("private storage temporary artifact is unsafe")
        try:
            named_metadata = os.stat(
                name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
        except OSError as error:
            raise _unsafe_storage_error("temporary artifact", error) from error
        if (
            named_metadata.st_dev != metadata.st_dev
            or named_metadata.st_ino != metadata.st_ino
            or named_metadata.st_mode != metadata.st_mode
            or named_metadata.st_nlink != metadata.st_nlink
            or named_metadata.st_uid != metadata.st_uid
            or named_metadata.st_gid != metadata.st_gid
        ):
            raise InsiderStorageError("private storage temporary artifact is unsafe")
        try:
            os.unlink(name, dir_fd=directory_descriptor)
        except OSError as error:
            raise _unsafe_storage_error("temporary artifact", error) from error
        removed = True

    if removed:
        os.fsync(directory_descriptor)


def _read_regular_file(
    directory_descriptor: int,
    name: str,
    *,
    max_bytes: int,
) -> bytes:
    try:
        descriptor = os.open(
            name,
            _FILE_READ_FLAGS,
            dir_fd=directory_descriptor,
        )
    except FileNotFoundError:
        raise
    except OSError as error:
        raise _unsafe_storage_error("target", error) from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise InsiderStorageError("private storage target is unsafe")
        if metadata.st_nlink != 1:
            raise InsiderStorageError(
                "private storage target must not be hard-linked"
            )
        if metadata.st_mode & 0o077:
            raise InsiderStorageError(
                "private storage target permissions are unsafe"
            )
        if metadata.st_size > max_bytes:
            raise InsiderStorageError(
                "private storage target exceeds the storage size limit"
            )
        blocks: list[bytes] = []
        remaining = metadata.st_size
        while remaining:
            block = os.read(descriptor, min(1024 * 1024, remaining))
            if not block:
                raise InsiderStorageError(
                    "private storage target changed while being read"
                )
            blocks.append(block)
            remaining -= len(block)
        if os.read(descriptor, 1):
            raise InsiderStorageError(
                "private storage target changed while being read"
            )
        return b"".join(blocks)
    finally:
        os.close(descriptor)


@contextmanager
def _exclusive_directory_lock(directory_descriptor: int) -> Iterator[None]:
    try:
        fcntl.flock(directory_descriptor, fcntl.LOCK_EX)
    except OSError as error:
        raise _unsafe_storage_error("lock", error) from error
    try:
        yield
    finally:
        try:
            fcntl.flock(directory_descriptor, fcntl.LOCK_UN)
        except OSError as error:
            raise _unsafe_storage_error("lock", error) from error


class InsiderStorage:
    """Store immutable raw and parser-versioned records below ignored data/."""

    def __init__(self, repository_root: Path) -> None:
        root = Path(repository_root)
        if root.is_symlink() or not root.is_dir():
            raise InsiderStorageError(
                "repository root must be an existing real directory"
            )
        self.repository_root = root.resolve()
        self.private_root = self.repository_root / PRIVATE_INSIDER_ROOT

    def _accession_directory(self, accession_number: str) -> Path:
        accession = _validate_accession(accession_number)
        return self.private_root / "accessions" / accession

    @contextmanager
    def _open_accession_directory(
        self,
        accession_number: str,
        *,
        create: bool,
    ) -> Iterator[tuple[int, Path]]:
        target = self._accession_directory(accession_number)
        try:
            descriptor = os.open(self.repository_root, _DIRECTORY_OPEN_FLAGS)
        except OSError as error:
            raise _unsafe_storage_error("root", error) from error
        try:
            for index, part in enumerate(
                target.relative_to(self.repository_root).parts
            ):
                child = _open_child_directory(
                    descriptor,
                    part,
                    create=create,
                    restricted=index >= 2,
                )
                os.close(descriptor)
                descriptor = child
            yield descriptor, target
        finally:
            os.close(descriptor)

    def _existing_artifact(
        self,
        directory_descriptor: int,
        target_name: str,
        target: Path,
        payload: bytes,
        max_bytes: int,
    ) -> StoredArtifact:
        existing = _read_regular_file(
            directory_descriptor,
            target_name,
            max_bytes=max_bytes,
        )
        existing_sha256 = hashlib.sha256(existing).hexdigest()
        if existing != payload:
            raise ImmutableInsiderStorageConflict(
                "immutable insider artifact conflicts with existing bytes"
            )
        return StoredArtifact(
            path=target,
            sha256=existing_sha256,
            byte_count=len(existing),
            created=False,
        )

    def _store_immutable(
        self,
        directory_descriptor: int,
        target_name: str,
        target: Path,
        payload: bytes,
        max_bytes: int,
    ) -> StoredArtifact:
        if type(payload) is not bytes or not payload:
            raise InsiderStorageError("private artifact payload must be bytes")
        with _exclusive_directory_lock(directory_descriptor):
            _remove_safe_stale_temporary_artifacts(
                directory_descriptor,
                target_name,
            )
            return self._store_immutable_locked(
                directory_descriptor,
                target_name,
                target,
                payload,
                max_bytes,
            )

    def _store_immutable_locked(
        self,
        directory_descriptor: int,
        target_name: str,
        target: Path,
        payload: bytes,
        max_bytes: int,
    ) -> StoredArtifact:
        try:
            return self._existing_artifact(
                directory_descriptor,
                target_name,
                target,
                payload,
                max_bytes,
            )
        except FileNotFoundError:
            pass

        temporary_name = ""
        descriptor = -1
        for _ in range(10):
            temporary_name = f".{target_name}.tmp-{secrets.token_hex(12)}"
            try:
                descriptor = os.open(
                    temporary_name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=directory_descriptor,
                )
                break
            except FileExistsError:
                continue
        if descriptor < 0:
            raise InsiderStorageError(
                "could not allocate a private storage temporary file"
            )
        try:
            os.fchmod(descriptor, 0o600)
            remaining = memoryview(payload)
            while remaining:
                written = os.write(descriptor, remaining)
                if written < 1:
                    raise InsiderStorageError(
                        "private artifact write did not make progress"
                    )
                remaining = remaining[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            try:
                existing = self._existing_artifact(
                    directory_descriptor,
                    target_name,
                    target,
                    payload,
                    max_bytes,
                )
            except FileNotFoundError:
                pass
            else:
                os.unlink(temporary_name, dir_fd=directory_descriptor)
                temporary_name = ""
                os.fsync(directory_descriptor)
                return existing
            try:
                _rename_noreplace(
                    directory_descriptor,
                    temporary_name,
                    target_name,
                )
            except FileExistsError:
                os.unlink(temporary_name, dir_fd=directory_descriptor)
                temporary_name = ""
                os.fsync(directory_descriptor)
                return self._existing_artifact(
                    directory_descriptor,
                    target_name,
                    target,
                    payload,
                    max_bytes,
                )
            temporary_name = ""
            os.fsync(directory_descriptor)
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary_name:
                try:
                    os.unlink(temporary_name, dir_fd=directory_descriptor)
                except FileNotFoundError:
                    pass
                os.fsync(directory_descriptor)
            raise

        stored = _read_regular_file(
            directory_descriptor,
            target_name,
            max_bytes=max_bytes,
        )
        if stored != payload:
            raise ImmutableInsiderStorageConflict(
                "immutable insider artifact changed during creation"
            )

        return StoredArtifact(
            path=target,
            sha256=hashlib.sha256(payload).hexdigest(),
            byte_count=len(payload),
            created=True,
        )

    def store_raw(
        self,
        accession_number: str,
        xml_bytes: bytes,
    ) -> StoredArtifact:
        """Create one accession's raw XML, or verify an identical retry."""

        if type(xml_bytes) is not bytes or not xml_bytes:
            raise InsiderStorageError("private artifact payload must be bytes")
        if len(xml_bytes) > MAX_RAW_XML_BYTES:
            raise InsiderStorageError("raw ownership XML exceeds the storage size limit")
        with self._open_accession_directory(
            accession_number,
            create=True,
        ) as (directory_descriptor, directory):
            return self._store_immutable(
                directory_descriptor,
                "raw.xml",
                directory / "raw.xml",
                xml_bytes,
                MAX_RAW_XML_BYTES,
            )

    def store_normalized(
        self,
        accession_number: str,
        parser_version: str,
        payload: object,
    ) -> StoredArtifact:
        """Create one immutable normalized record for a parser version."""

        accession = _validate_accession(accession_number)
        version = _validate_parser_version(parser_version)
        if not isinstance(payload, dict):
            raise InsiderStorageError("normalized filing must be an object")
        if payload.get("accession_number") != accession:
            raise InsiderStorageError(
                "normalized filing accession does not match storage key"
            )
        if payload.get("parser_version") != version:
            raise InsiderStorageError(
                "normalized filing parser version does not match storage key"
            )
        rendered = canonical_insider_json_bytes(payload)
        if len(rendered) > MAX_NORMALIZED_JSON_BYTES:
            raise InsiderStorageError(
                "normalized ownership JSON exceeds the storage size limit"
            )
        try:
            with self._open_accession_directory(
                accession,
                create=False,
            ) as (accession_descriptor, accession_directory):
                raw_xml = _read_regular_file(
                    accession_descriptor,
                    "raw.xml",
                    max_bytes=MAX_RAW_XML_BYTES,
                )
                raw_sha256 = hashlib.sha256(raw_xml).hexdigest()
                if payload.get("raw_sha256") != raw_sha256:
                    raise InsiderStorageError(
                        "normalized filing raw SHA-256 does not match stored XML"
                    )
                try:
                    stored_raw_document = raw_ownership_document(raw_xml)
                except (InsiderParseError, TypeError) as error:
                    raise InsiderStorageError(
                        "stored raw ownership XML cannot be parsed safely"
                    ) from error
                if payload.get("raw_document") != stored_raw_document:
                    raise InsiderStorageError(
                        "normalized filing raw document does not match stored XML"
                    )
                normalized_descriptor = _open_child_directory(
                    accession_descriptor,
                    "normalized",
                    create=True,
                    restricted=True,
                )
                try:
                    normalized_directory = accession_directory / "normalized"
                    return self._store_immutable(
                        normalized_descriptor,
                        f"{version}.json",
                        normalized_directory / f"{version}.json",
                        rendered,
                        MAX_NORMALIZED_JSON_BYTES,
                    )
                finally:
                    os.close(normalized_descriptor)
        except FileNotFoundError as error:
            raise InsiderStorageError(
                "raw ownership XML must be stored before normalization"
            ) from error


__all__ = [
    "MAX_NORMALIZED_JSON_BYTES",
    "MAX_RAW_XML_BYTES",
    "PRIVATE_INSIDER_ROOT",
    "ImmutableInsiderStorageConflict",
    "InsiderStorage",
    "InsiderStorageError",
    "StoredArtifact",
]
