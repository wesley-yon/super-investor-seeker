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
import threading
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from typing import Callable, Iterator, TypeVar
from urllib.parse import urlsplit

import json
from insider_contract import (
    MAX_NORMALIZED_JSON_BYTES,
    MAX_RAW_XML_BYTES,
    canonical_insider_json_bytes,
)
from insider_source import (
    MAX_INDEX_HTML_BYTES,
    canonical_source_metadata_json_bytes,
    parse_insider_filing_index,
    validate_insider_source_metadata,
)
from insider_parser import InsiderParseError, parse_ownership_xml, raw_ownership_document
from security_identity import (
    normalize_section16_security_title,
    section16_owner_group_key,
    section16_security_class_key,
)


MAX_SOURCE_METADATA_JSON_BYTES = 1_000_000
_PublicationResult = TypeVar("_PublicationResult")


def _strict_json_bytes(rendered: bytes) -> object:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(
            rendered.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("nonfinite JSON")
            ),
        )
    except RecursionError as error:
        raise ValueError("JSON nesting is too deep") from error


PRIVATE_INSIDER_ROOT = Path("data/insiders/private")
PRIVATE_INSIDER_STATE_ROOT = PRIVATE_INSIDER_ROOT / "state"
MAX_INSIDER_STATE_BYTES = 1_000_000
MAX_INSIDER_STATE_COLLECTION = 1_000
MAX_INSIDER_STATE_STRING_CHARS = 4_096
MAX_INSIDER_STATE_INTEGER = 2_147_483_647
MAX_TELEMETRY_RECENT_RUNS = 100
MAX_TELEMETRY_ACCESSION_EXAMPLES = 25
INCREMENTAL_STATE_CONTRACT_VERSION = 1
BACKFILL_STATE_CONTRACT_VERSION = 1
REPARSE_STATE_CONTRACT_VERSION = 1
APPROVED_ISSUERS_STATE_CONTRACT_VERSION = 1
ISSUER_STATE_CONTRACT_VERSION = 1
QUARANTINE_STATE_CONTRACT_VERSION = 1
TELEMETRY_STATE_CONTRACT_VERSION = 1
_STATE_CONTRACT_VERSIONS = {
    "incremental": INCREMENTAL_STATE_CONTRACT_VERSION,
    "backfill": BACKFILL_STATE_CONTRACT_VERSION,
    "reparse": REPARSE_STATE_CONTRACT_VERSION,
    "approved": APPROVED_ISSUERS_STATE_CONTRACT_VERSION,
    "issuer": ISSUER_STATE_CONTRACT_VERSION,
    "accession_quarantine": QUARANTINE_STATE_CONTRACT_VERSION,
    "quarter_quarantine": QUARANTINE_STATE_CONTRACT_VERSION,
    "telemetry": TELEMETRY_STATE_CONTRACT_VERSION,
}
_ACCESSION_RE = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_PARSER_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_CIK_RE = re.compile(r"[0-9]{10}")
_QUARTER_RE = re.compile(r"[0-9]{4}Q[1-4]")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_SAFE_CODE_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,127}")
_STATE_STATUSES = frozenset({"incomplete", "pending", "running", "completed", "failed", "quarantined"})
_REPARSE_SCOPES = frozenset({"accession", "issuer", "all"})
_SECTION16_FORM_TYPE_RE = re.compile(r"[345](?:/A)?")
_INCREMENTAL_ENTITY_ROLES = frozenset({"issuer", "reporting_owner"})
_INCREMENTAL_REPORTING_FILENAME_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}\.html?"
)
_QUARANTINE_STAGES = frozenset({
    "discovery", "cache", "raw", "source", "archive", "backfill", "reparse", "issuer", "telemetry", "checkpoint",
})
_ERROR_CLASSES = frozenset({
    "InsiderIndexParseError", "InsiderContractError", "InsiderIssuerReductionError",
    "InsiderParseError", "InsiderStorageError", "TimeoutError", "ConnectionError",
    "HTTPError",
})
_REASON_CODES = frozenset({
    "discovery_invalid", "cache_invalid", "raw_fetch_failed", "raw_invalid", "source_invalid", "archive_invalid",
    "backfill_invalid", "reparse_invalid", "issuer_invalid", "telemetry_invalid", "checkpoint_invalid",
    "index_parse_invalid", "contract_invalid", "timeout", "connection_failed", "http_error",
})
_AMENDMENT_CONFIDENCES = frozenset({"high", "medium", "low", "unresolved"})
_AMENDMENT_REASON_CODES = frozenset({"explicit_reference", "single_candidate", "ambiguous_candidates", "no_candidate"})
_AMBIGUITY_REASON_CODES = frozenset({"ambiguous_candidates", "no_candidate"})
_QUARANTINE_REASON_CODES_BY_STAGE = {
    "discovery": frozenset({"discovery_invalid", "index_parse_invalid", "timeout", "connection_failed", "http_error"}),
    "cache": frozenset({"cache_invalid"}),
    "raw": frozenset({"raw_fetch_failed", "raw_invalid", "timeout", "connection_failed", "http_error"}),
    "source": frozenset({"source_invalid", "index_parse_invalid"}),
    "archive": frozenset({"archive_invalid"}),
    "backfill": frozenset({"backfill_invalid", "timeout", "connection_failed", "http_error"}),
    "reparse": frozenset({"reparse_invalid"}),
    "issuer": frozenset({"issuer_invalid"}),
    "telemetry": frozenset({"telemetry_invalid"}),
    "checkpoint": frozenset({"checkpoint_invalid"}),
}
_TRANSIENT_QUARANTINE_REASON_CODES = frozenset(
    {"raw_fetch_failed", "timeout", "connection_failed", "http_error"}
)
_TELEMETRY_COUNTERS = frozenset({
    "discovery_attempts", "discovery_entries", "discovered_accession_groups",
    "index_fetches", "index_cache_hits", "raw_fetches", "raw_cache_hits",
    "parse_attempts", "parse_successes", "parse_failures",
    "reporting_owner_rows", "non_derivative_rows", "derivative_rows",
    "non_derivative_transaction_rows", "non_derivative_holding_rows",
    "derivative_transaction_rows", "derivative_holding_rows", "footnote_rows",
    "owner_signature_rows", "unknown_codes", "unknown_elements", "parse_warnings",
    "unmapped_security_titles", "amendments", "amendments_resolved",
    "amendments_unresolved", "http_attempts", "http_status_2xx", "http_status_4xx",
    "http_status_5xx", "http_latency_ms", "limiter_wait_ms", "limiter_utilization",
    "backfill_source_quarters", "backfill_source_hashes", "backfill_tables",
    "backfill_table_evidence", "backfill_reconciliations", "checkpoint_writes",
    "checkpoint_failures", "reparse_attempts", "reparse_completed", "reparse_failures",
})
_BACKFILL_TABLES = frozenset({
    "SUBMISSION", "REPORTINGOWNER", "NONDERIV_TRANS", "NONDERIV_HOLDING",
    "DERIV_TRANS", "DERIV_HOLDING", "FOOTNOTES", "OWNER_SIGNATURE",
})
_REQUIRED_BACKFILL_TABLES = frozenset({"SUBMISSION"})
_BACKFILL_OPTIONAL_TABLES = _BACKFILL_TABLES - _REQUIRED_BACKFILL_TABLES
_RECONCILIATION_STATUSES = frozenset({"matched", "mismatch", "pending", "not_applicable"})
_RUN_STATUSES = frozenset({"running", "completed", "failed", "cancelled"})
_TELEMETRY_STAGES = frozenset({
    "discovery", "cache", "index", "raw", "parse", "source", "archive",
    "normalized", "issuer", "checkpoint", "backfill", "reparse", "telemetry",
})
_TELEMETRY_OUTCOMES = frozenset(
    {"created", "cache_hit", "checkpointed", "quarantined", "retry_later"}
)
_SECURITY_TITLE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9 .,&()/:+_-]{0,255}")
_UTC_TIMESTAMP_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?Z"
)
_LIBC = ctypes.CDLL(None, use_errno=True)


class InsiderStorageError(ValueError):
    """Raised when a private insider artifact cannot be stored safely."""


class InsiderApprovalScopeError(InsiderStorageError):
    """Raised when incremental state is outside the durable issuer authority."""


class InsiderStateRevisionError(InsiderStorageError):
    """Raised when a mutable-state compare-and-swap revision is no longer current."""


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
_FILE_READ_FLAGS = (
    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
)


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
) -> tuple[int, bool]:
    """Open one trusted child, making each create traversal durable before return."""
    descriptor = -1
    created = False
    success = False
    try:
        try:
            descriptor = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_descriptor)
        except FileNotFoundError:
            if not create:
                raise
            try:
                os.mkdir(name, mode=0o700, dir_fd=parent_descriptor)
                created = True
            except FileExistsError:
                pass
            descriptor = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_descriptor)

        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or (restricted and stat.S_IMODE(metadata.st_mode) != 0o700)
            or (not restricted and stat.S_IMODE(metadata.st_mode) & 0o022)
        ):
            raise InsiderStorageError("private storage parent is unsafe")
        if created:
            os.fchmod(descriptor, 0o700)
            os.fsync(descriptor)
        # A concurrent mkdir may have won.  Its namespace entry still needs this
        # traversal's durable parent validation before a create operation succeeds.
        if create:
            os.fsync(parent_descriptor)
        success = True
        return descriptor, created
    except FileNotFoundError as error:
        if not create:
            raise
        raise _unsafe_storage_error("parent", error) from error
    except OSError as error:
        raise _unsafe_storage_error("parent", error) from error
    finally:
        if not success and descriptor >= 0:
            os.close(descriptor)


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
        if stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.geteuid():
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
        lock: bool = False,
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
                child, _ = _open_child_directory(
                    descriptor,
                    part,
                    create=create,
                    restricted=index >= 2,
                )
                os.close(descriptor)
                descriptor = child
            if lock:
                with _exclusive_directory_lock(descriptor):
                    yield descriptor, target
            else:
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

    def _store_immutable_locked(
        self,
        directory_descriptor: int,
        target_name: str,
        target: Path,
        payload: bytes,
        max_bytes: int,
        *,
        pre_publish_verify: Callable[[], None] | None = None,
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
            if pre_publish_verify is not None:
                pre_publish_verify()
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

    def store_index_html(
        self, accession_number: str, index_html_bytes: bytes
    ) -> StoredArtifact:
        """Create one immutable official filing index, or verify a retry."""

        if type(index_html_bytes) is not bytes or not index_html_bytes:
            raise InsiderStorageError("private artifact payload must be bytes")
        if len(index_html_bytes) > MAX_INDEX_HTML_BYTES:
            raise InsiderStorageError("filing index HTML exceeds the storage size limit")
        with self._open_accession_directory(accession_number, create=True, lock=True) as (
            descriptor,
            directory,
        ):
            _remove_safe_stale_temporary_artifacts(descriptor, "index.html")
            return self._store_immutable_locked(
                descriptor, "index.html", directory / "index.html", index_html_bytes,
                MAX_INDEX_HTML_BYTES,
            )

    def read_index_html(self, accession_number: str) -> bytes:
        try:
            with self._open_accession_directory(accession_number, create=False) as (descriptor, _):
                return _read_regular_file(descriptor, "index.html", max_bytes=MAX_INDEX_HTML_BYTES)
        except FileNotFoundError as error:
            raise InsiderStorageError("filing index HTML must be stored before reading") from error

    def read_raw(self, accession_number: str) -> bytes:
        try:
            with self._open_accession_directory(accession_number, create=False) as (descriptor, _):
                return _read_regular_file(descriptor, "raw.xml", max_bytes=MAX_RAW_XML_BYTES)
        except FileNotFoundError as error:
            raise InsiderStorageError("raw ownership XML must be stored before reading") from error

    def _validate_source_metadata_bindings_locked(
        self, descriptor: int, accession: str, validated: dict[str, object]
    ) -> None:
        index_html = _read_regular_file(descriptor, "index.html", max_bytes=MAX_INDEX_HTML_BYTES)
        raw_xml = _read_regular_file(descriptor, "raw.xml", max_bytes=MAX_RAW_XML_BYTES)
        index = validated["index"]
        document = validated["document"]
        assert isinstance(index, dict) and isinstance(document, dict)
        if (index["sha256"], index["byte_count"]) != (hashlib.sha256(index_html).hexdigest(), len(index_html)) or (document["sha256"], document["byte_count"]) != (hashlib.sha256(raw_xml).hexdigest(), len(raw_xml)):
            raise InsiderStorageError("source metadata artifact hashes or sizes do not match stored bytes")
        try:
            parsed_index = parse_insider_filing_index(index_html, index_url=index["url"], accession_number=accession, issuer_cik=validated["issuer_cik"], reporting_owner_ciks=validated["reporting_owner_ciks"])
        except (TypeError, ValueError) as error:
            raise InsiderStorageError("stored filing index cannot be parsed safely") from error
        bindings = {"form_type": validated["form_type"], "filing_date": validated["filing_date"], "accepted_at": validated["accepted_at"], "issuer_cik": validated["issuer_cik"], "index_url": index["url"], "index_archive_cik": index["archive_cik"], "document_url": document["url"], "document_archive_cik": document["archive_cik"], "document_archive_cik_role": document["archive_cik_role"], "document_sequence": document["sequence"], "document_type": document["document_type"], "document_filename": document["filename"]}
        if any(parsed_index[key] != value for key, value in bindings.items()):
            raise InsiderStorageError("source metadata does not match stored filing index")
        try:
            normalized = parse_ownership_xml(raw_xml, accession_number=accession, filing_date=validated["filing_date"], accepted_at=validated["accepted_at"], source_index_url=index["url"], source_document_url=document["url"])
        except (InsiderParseError, TypeError) as error:
            raise InsiderStorageError("stored raw ownership XML cannot be parsed safely") from error
        if normalized["form_type"] != validated["form_type"] or normalized["issuer"]["cik"] != validated["issuer_cik"] or sorted(owner["cik"] for owner in normalized["owners"]) != validated["reporting_owner_ciks"]:
            raise InsiderStorageError("source metadata does not match stored raw ownership XML")

    def _read_source_metadata_locked(
        self, descriptor: int, accession: str
    ) -> dict[str, object]:
        try:
            rendered = _read_regular_file(
                descriptor,
                "source-metadata.json",
                max_bytes=MAX_SOURCE_METADATA_JSON_BYTES,
            )
        except FileNotFoundError as error:
            raise InsiderStorageError(
                "source metadata must be stored before normalization"
            ) from error
        try:
            metadata = validate_insider_source_metadata(_strict_json_bytes(rendered))
            if (
                canonical_source_metadata_json_bytes(metadata) != rendered
                or metadata["accession_number"] != accession
            ):
                raise ValueError
            return metadata
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
            raise InsiderStorageError("stored source metadata is invalid") from error

    def _validate_normalized_source_bindings_locked(
        self, descriptor: int, accession: str, payload: dict[str, object]
    ) -> None:
        raw_xml = _read_regular_file(
            descriptor, "raw.xml", max_bytes=MAX_RAW_XML_BYTES
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
        metadata = self._read_source_metadata_locked(descriptor, accession)
        self._validate_source_metadata_bindings_locked(descriptor, accession, metadata)
        source = payload.get("source")
        issuer = payload.get("issuer")
        owners = payload.get("owners")
        document = metadata["document"]
        index = metadata["index"]
        if (
            payload.get("form_type") != metadata["form_type"]
            or payload.get("filing_date") != metadata["filing_date"]
            or payload.get("accepted_at") != metadata["accepted_at"]
            or not isinstance(issuer, dict)
            or issuer.get("cik") != metadata["issuer_cik"]
            or not isinstance(owners, list)
            or sorted(owner.get("cik") for owner in owners if isinstance(owner, dict))
            != metadata["reporting_owner_ciks"]
            or not isinstance(source, dict)
            or not isinstance(index, dict)
            or not isinstance(document, dict)
            or source.get("index_url") != index["url"]
            or source.get("document_url") != document["url"]
            or payload.get("raw_sha256") != document["sha256"]
            or len(raw_xml) != document["byte_count"]
        ):
            raise InsiderStorageError(
                "normalized filing does not match stored source metadata"
            )

    def store_source_metadata(
        self, accession_number: str, metadata: object
    ) -> StoredArtifact:
        """Bind immutable index and raw bytes to deterministic source metadata."""

        accession = _validate_accession(accession_number)
        try:
            validated = validate_insider_source_metadata(metadata)
            rendered = canonical_source_metadata_json_bytes(validated)
        except (TypeError, ValueError) as error:
            raise InsiderStorageError("source metadata is invalid") from error
        if validated["accession_number"] != accession:
            raise InsiderStorageError("source metadata accession does not match storage key")
        if len(rendered) > MAX_SOURCE_METADATA_JSON_BYTES:
            raise InsiderStorageError("source metadata exceeds the storage size limit")
        try:
            with self._open_accession_directory(
                accession, create=False, lock=True
            ) as (descriptor, directory):
                self._validate_source_metadata_bindings_locked(descriptor, accession, validated)
                _remove_safe_stale_temporary_artifacts(descriptor, "source-metadata.json")
                return self._store_immutable_locked(
                    descriptor, "source-metadata.json", directory / "source-metadata.json",
                    rendered,
                    MAX_SOURCE_METADATA_JSON_BYTES,
                    pre_publish_verify=lambda: self._validate_source_metadata_bindings_locked(
                        descriptor, accession, validated
                    ),
                )
        except FileNotFoundError as error:
            raise InsiderStorageError("index HTML and raw ownership XML must be stored before source metadata") from error

    def read_source_metadata(self, accession_number: str) -> dict[str, object]:
        accession = _validate_accession(accession_number)
        try:
            with self._open_accession_directory(
                accession, create=False, lock=True
            ) as (descriptor, _):
                rendered = _read_regular_file(
                    descriptor,
                    "source-metadata.json",
                    max_bytes=MAX_SOURCE_METADATA_JSON_BYTES,
                )
                try:
                    parsed = _strict_json_bytes(rendered)
                    if canonical_source_metadata_json_bytes(parsed) != rendered:
                        raise ValueError
                    validated = validate_insider_source_metadata(parsed)
                    if validated["accession_number"] != accession:
                        raise ValueError
                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                    TypeError,
                    ValueError,
                ) as error:
                    raise InsiderStorageError("stored source metadata is invalid") from error
                self._validate_source_metadata_bindings_locked(
                    descriptor, accession, validated
                )
                return validated
        except FileNotFoundError as error:
            raise InsiderStorageError("source metadata must be stored before reading") from error

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
            accession_number, create=True, lock=True
        ) as (directory_descriptor, directory):
            _remove_safe_stale_temporary_artifacts(directory_descriptor, "raw.xml")
            return self._store_immutable_locked(
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
                lock=True,
            ) as (accession_descriptor, accession_directory):
                self._validate_normalized_source_bindings_locked(
                    accession_descriptor, accession, payload
                )
                normalized_descriptor, normalized_created = _open_child_directory(
                    accession_descriptor,
                    "normalized",
                    create=True,
                    restricted=True,
                )
                try:
                    normalized_directory = accession_directory / "normalized"
                    _remove_safe_stale_temporary_artifacts(normalized_descriptor, f"{version}.json")
                    result = self._store_immutable_locked(
                        normalized_descriptor,
                        f"{version}.json",
                        normalized_directory / f"{version}.json",
                        rendered,
                        MAX_NORMALIZED_JSON_BYTES,
                        pre_publish_verify=lambda: self._validate_normalized_source_bindings_locked(
                            accession_descriptor, accession, payload
                        ),
                    )
                except BaseException:
                    os.close(normalized_descriptor)
                    if normalized_created:
                        try:
                            os.rmdir("normalized", dir_fd=accession_descriptor)
                        except OSError:
                            pass
                        else:
                            os.fsync(accession_descriptor)
                    raise
                else:
                    os.close(normalized_descriptor)
                    return result
        except FileNotFoundError as error:
            raise InsiderStorageError(
                "raw ownership XML must be stored before normalization"
            ) from error

    def read_normalized(self, accession_number: str, parser_version: str) -> dict[str, object]:
        accession = _validate_accession(accession_number)
        version = _validate_parser_version(parser_version)
        try:
            with self._open_accession_directory(
                accession, create=False, lock=True
            ) as (descriptor, _):
                normalized_descriptor, _ = _open_child_directory(
                    descriptor, "normalized", create=False, restricted=True
                )
                try:
                    rendered = _read_regular_file(
                        normalized_descriptor,
                        f"{version}.json",
                        max_bytes=MAX_NORMALIZED_JSON_BYTES,
                    )
                finally:
                    os.close(normalized_descriptor)
                try:
                    parsed = _strict_json_bytes(rendered)
                    if (
                        not isinstance(parsed, dict)
                        or canonical_insider_json_bytes(parsed) != rendered
                        or parsed.get("accession_number") != accession
                        or parsed.get("parser_version") != version
                    ):
                        raise ValueError
                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                    TypeError,
                    ValueError,
                ) as error:
                    raise InsiderStorageError("stored normalized filing is invalid") from error
                try:
                    self._validate_normalized_source_bindings_locked(
                        descriptor, accession, parsed
                    )
                except (FileNotFoundError, InsiderStorageError) as error:
                    raise InsiderStorageError(
                        "normalized filing source bindings are invalid"
                    ) from error
                return parsed
        except FileNotFoundError as error:
            raise InsiderStorageError("normalized filing must be stored before reading") from error


def _state_error(message: str) -> InsiderStorageError:
    return InsiderStorageError(f"private insider state is invalid: {message}")


def _state_exact_keys(
    payload: object,
    required: set[str],
    *,
    contract: bool = True,
    version: int | None = None,
) -> dict[str, object]:
    if not isinstance(payload, dict) or set(payload) != required:
        raise _state_error("schema keys")
    if contract:
        if version is None:
            raise TypeError("contract version must be explicit")
        if (
            type(payload.get("contract_version")) is not int
            or payload["contract_version"] != version
        ):
            raise _state_error("contract version")
    return payload


def _state_string(value: object, label: str, *, pattern: re.Pattern[str] | None = None) -> str:
    if type(value) is not str or not value or len(value) > MAX_INSIDER_STATE_STRING_CHARS:
        raise _state_error(label)
    if pattern is not None and not pattern.fullmatch(value):
        raise _state_error(label)
    return value


def _state_cik(value: object, label: str) -> str:
    cik = _state_string(value, label, pattern=_CIK_RE)
    if cik == "0000000000":
        raise _state_error(label)
    return cik


def _state_quarter(value: object, label: str) -> str:
    quarter = _state_string(value, label, pattern=_QUARTER_RE)
    # Only the historical lower bound belongs to time-independent state validation.
    if int(quarter[:4]) < 2006:
        raise _state_error(label)
    return quarter


def _state_accession(value: object, label: str) -> str:
    try:
        return _validate_accession(value)
    except InsiderStorageError as error:
        raise _state_error(label) from error


def _state_timestamp(value: object, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    timestamp = _state_string(value, label, pattern=_UTC_TIMESTAMP_RE)
    try:
        _state_timestamp_instant(timestamp)
    except ValueError as error:
        raise _state_error(label) from error
    return timestamp


def _state_timestamp_instant(timestamp: str) -> datetime:
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))


def _state_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list) or len(value) > MAX_INSIDER_STATE_COLLECTION:
        raise _state_error(label)
    return value


def _state_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_INSIDER_STATE_INTEGER:
        raise _state_error(label)
    return value


def _state_code(value: object, label: str, allowed: frozenset[str]) -> str:
    code = _state_string(value, label, pattern=_SAFE_CODE_RE)
    if code not in allowed:
        raise _state_error(label)
    return code


def _state_security_title(value: object) -> str:
    title = _state_string(value, "security title")
    if any(
        unicodedata.category(character) in {"Cc", "Cf", "Cs"}
        or character in {"<", ">"}
        for character in title
    ):
        raise _state_error("security title")
    try:
        normalize_section16_security_title(title)
    except (TypeError, ValueError) as error:
        raise _state_error("security title") from error
    return title


def _state_sec_url(
    value: object,
    label: str,
    *,
    prefixes: tuple[str, ...],
    nullable: bool = False,
) -> str | None:
    if value is None and nullable:
        return None
    url = _state_string(value, label)
    if any(
        ord(character) < 32
        or ord(character) == 127
        or ord(character) > 127
        or character in {"\\", "%"}
        for character in url
    ):
        raise _state_error(label)
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except (TypeError, ValueError) as error:
        raise _state_error(label) from error
    if (
        parsed.scheme != "https"
        or parsed.netloc not in {"www.sec.gov", "www.sec.gov:443"}
        or parsed.hostname != "www.sec.gov"
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.query
        or parsed.fragment
        or not any(
            parsed.path == prefix.rstrip("/")
            or parsed.path.startswith(prefix if prefix.endswith("/") else prefix + "/")
            for prefix in prefixes
        )
    ):
        raise _state_error(label)
    components = parsed.path.split("/")[1:]
    if not components or any(not component or component in {".", ".."} for component in components):
        raise _state_error(label)
    return url


def _state_index_url(
    value: object,
    label: str,
    *,
    accession_number: str,
    issuer_cik: str,
) -> str:
    url = _state_sec_url(
        value,
        label,
        prefixes=("/Archives/edgar/data/",),
    )
    assert url is not None
    expected_stem = (
        f"/Archives/edgar/data/{int(issuer_cik)}/"
        f"{accession_number.replace('-', '')}/{accession_number}-index.htm"
    )
    if urlsplit(url).path not in {expected_stem, f"{expected_stem}l"}:
        raise _state_error(label)
    return url


def _state_incremental_entry_url(
    value: object,
    label: str,
    *,
    accession_number: str,
    entity_cik: str,
    entity_role: str,
) -> str:
    if entity_role == "issuer":
        return _state_index_url(
            value,
            label,
            accession_number=accession_number,
            issuer_cik=entity_cik,
        )
    if entity_role != "reporting_owner":
        raise _state_error(label)
    try:
        return _state_index_url(
            value,
            label,
            accession_number=accession_number,
            issuer_cik=entity_cik,
        )
    except InsiderStorageError:
        pass
    url = _state_sec_url(
        value,
        label,
        prefixes=("/Archives/edgar/data/",),
    )
    assert url is not None
    path = urlsplit(url).path
    expected_prefix = (
        f"/Archives/edgar/data/{int(entity_cik)}/"
        f"{accession_number.replace('-', '')}/"
    )
    if not path.startswith(expected_prefix):
        raise _state_error(label)
    filename = path[len(expected_prefix) :]
    if not _INCREMENTAL_REPORTING_FILENAME_RE.fullmatch(filename):
        raise _state_error(label)
    return url


def _state_catalog_url(value: object, label: str, *, nullable: bool = False) -> str | None:
    prefix = "/data-research/sec-markets-data/insider-transactions-data-sets"
    url = _state_sec_url(value, label, prefixes=(prefix,), nullable=nullable)
    if url is not None and urlsplit(url).path != prefix:
        raise _state_error(label)
    return url


def _state_zip_url(
    value: object,
    label: str,
    *,
    quarter: str,
    nullable: bool = False,
) -> str | None:
    url = _state_sec_url(value, label, prefixes=("/files/",), nullable=nullable)
    if url is None:
        return None
    path = urlsplit(url).path
    filenames = (
        f"{quarter.lower()}.zip",
        f"{quarter.lower()}_form345.zip",
    )
    allowed_paths = {
        f"/files/dera/data/insider-transactions-data-sets/{filename}"
        for filename in filenames
    } | {
        f"/files/structureddata/data/insider-transactions-data-sets/{filename}"
        for filename in filenames
    }
    if path not in allowed_paths:
        raise _state_error(label)
    return url


def _state_etag(value: object, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    etag = _state_string(value, label)
    if len(etag) > 1024 or not re.fullmatch(
        r'(?:W/)?"[\x20-\x21\x23-\x7e\x80-\xff]*"', etag
    ):
        raise _state_error(label)
    return etag


def _state_http_date(value: object, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    rendered = _state_string(value, label)
    try:
        parsed = parsedate_to_datetime(rendered)
        if parsed.tzinfo is None or format_datetime(parsed.astimezone(timezone.utc), usegmt=True) != rendered:
            raise ValueError("not canonical IMF-fixdate")
    except (TypeError, ValueError, IndexError, OverflowError) as error:
        raise _state_error(label) from error
    return rendered


def _validate_state_accession_list(value: object, label: str) -> list[str]:
    values = _state_list(value, label)
    normalized = [_state_accession(item, label) for item in values]
    if normalized != sorted(set(normalized)):
        raise _state_error(label)
    return normalized


def _validate_incremental(payload: object) -> dict[str, object]:
    result = _state_exact_keys(
        payload,
        {
            "contract_version", "status", "lookback_seconds", "first_observed_at",
            "last_observed_at", "queue", "completed_accessions", "source_entries",
        },
        version=INCREMENTAL_STATE_CONTRACT_VERSION,
    )
    _state_code(result["status"], "status", _STATE_STATUSES)
    _state_nonnegative_int(result["lookback_seconds"], "lookback seconds")
    first = _state_timestamp(result["first_observed_at"], "first observed", nullable=True)
    last = _state_timestamp(result["last_observed_at"], "last observed", nullable=True)
    if (
        first is not None
        and last is not None
        and _state_timestamp_instant(first) > _state_timestamp_instant(last)
    ):
        raise _state_error("observation timestamps")
    completed = [_state_accession(item, "completed accessions") for item in _state_list(result["completed_accessions"], "completed accessions")]
    if completed != sorted(set(completed)):
        raise _state_error("completed accessions")

    queue_entries: list[tuple[str, str, str, str, str, str]] = []
    for entry in _state_list(result["queue"], "queue"):
        item = _state_exact_keys(
            entry,
            {
                "accession_number", "issuer_cik", "form_type", "index_url",
                "accepted_at", "observed_at",
            },
            contract=False,
        )
        accession = _state_accession(item["accession_number"], "accession")
        cik = _state_cik(item["issuer_cik"], "issuer CIK")
        form_type = _state_string(
            item["form_type"], "form type", pattern=_SECTION16_FORM_TYPE_RE
        )
        index_url = _state_index_url(
            item["index_url"],
            "index URL",
            accession_number=accession,
            issuer_cik=cik,
        )
        accepted = _state_timestamp(item["accepted_at"], "accepted at")
        observed = _state_timestamp(item["observed_at"], "observed at")
        assert accepted is not None and observed is not None
        if _state_timestamp_instant(accepted) > _state_timestamp_instant(observed):
            raise _state_error("accepted and observed timestamps")
        queue_entries.append(
            (accession, cik, form_type, index_url, accepted, observed)
        )
    if queue_entries != sorted(
        queue_entries,
        key=lambda entry: (_state_timestamp_instant(entry[4]), entry[0]),
    ) or len({entry[0] for entry in queue_entries}) != len(queue_entries):
        raise _state_error("queue")

    source_entries: list[tuple[str, str, str, str, str, str, str]] = []
    for entry in _state_list(result["source_entries"], "source entries"):
        item = _state_exact_keys(
            entry,
            {
                "accession_number", "form_type", "entity_role", "entity_cik",
                "entry_url", "accepted_at", "observed_at",
            },
            contract=False,
        )
        accession = _state_accession(item["accession_number"], "accession")
        form_type = _state_string(
            item["form_type"], "form type", pattern=_SECTION16_FORM_TYPE_RE
        )
        role = _state_code(
            item["entity_role"], "entity role", _INCREMENTAL_ENTITY_ROLES
        )
        entity_cik = _state_cik(item["entity_cik"], "entity CIK")
        entry_url = _state_incremental_entry_url(
            item["entry_url"],
            "entry URL",
            accession_number=accession,
            entity_cik=entity_cik,
            entity_role=role,
        )
        accepted = _state_timestamp(item["accepted_at"], "accepted at")
        observed = _state_timestamp(item["observed_at"], "observed at")
        assert accepted is not None and observed is not None
        if _state_timestamp_instant(accepted) > _state_timestamp_instant(observed):
            raise _state_error("accepted and observed timestamps")
        source_entries.append(
            (
                accession, form_type, role, entity_cik, entry_url, accepted,
                observed,
            )
        )
    if source_entries != sorted(
        source_entries,
        key=lambda entry: (
            _state_timestamp_instant(entry[5]), entry[0], entry[2], entry[3],
            entry[4], _state_timestamp_instant(entry[6]),
        ),
    ) or len(set(source_entries)) != len(source_entries):
        raise _state_error("source entries")

    queue_by_accession = {entry[0]: entry for entry in queue_entries}
    sources_by_accession: dict[
        str, list[tuple[str, str, str, str, str, str, str]]
    ] = {}
    for entry in source_entries:
        sources_by_accession.setdefault(entry[0], []).append(entry)
    if set(sources_by_accession) != set(queue_by_accession):
        raise _state_error("incremental evidence bindings")
    for accession, queue_entry in queue_by_accession.items():
        sources = sources_by_accession.get(accession, [])
        issuer_sources = [entry for entry in sources if entry[2] == "issuer"]
        if len(issuer_sources) != 1:
            raise _state_error("incremental issuer evidence")
        issuer_source = issuer_sources[0]
        if (
            issuer_source[1] != queue_entry[2]
            or issuer_source[3] != queue_entry[1]
            or issuer_source[4] != queue_entry[3]
            or issuer_source[5] != queue_entry[4]
            or issuer_source[6] != queue_entry[5]
            or any(
                source[1] != queue_entry[2]
                or source[5] != queue_entry[4]
                for source in sources
            )
        ):
            raise _state_error("incremental evidence bindings")
    if not set(completed) <= set(queue_by_accession):
        raise _state_error("incremental evidence bindings")
    if result["status"] == "completed" and set(completed) != set(queue_by_accession):
        raise _state_error("incremental completion bindings")
    # State validation deliberately has no injected/current clock: Task7 owns future-quarter policy.
    if source_entries:
        first_source = min(
            source_entries,
            key=lambda entry: (_state_timestamp_instant(entry[6]), entry[0]),
        )[6]
        last_source = max(
            source_entries,
            key=lambda entry: (_state_timestamp_instant(entry[6]), entry[0]),
        )[6]
        if (first, last) != (first_source, last_source):
            raise _state_error("observation timestamps")
    elif first is not None or last is not None:
        raise _state_error("observation timestamps")
    return result


def validate_incremental_state_payload(payload: object) -> dict[str, object]:
    """Validate and return the canonical incremental mutable-state contract."""

    return _validate_incremental(payload)


def _validate_backfill(payload: object, quarter: str) -> dict[str, object]:
    result = _state_exact_keys(
        payload,
        {
            "contract_version", "quarter", "issuer_cik", "status", "catalog_url", "zip_url",
            "zip_sha256", "zip_byte_count", "etag", "last_modified", "table_evidence",
            "missing_optional_tables", "selected_accessions", "completed_accessions",
            "reconciliation",
        },
        version=BACKFILL_STATE_CONTRACT_VERSION,
    )
    if _state_quarter(result["quarter"], "quarter") != quarter:
        raise _state_error("quarter")
    _state_cik(result["issuer_cik"], "issuer CIK")
    _state_code(result["status"], "status", _STATE_STATUSES)
    _state_catalog_url(result["catalog_url"], "catalog URL", nullable=True)
    _state_zip_url(result["zip_url"], "ZIP URL", quarter=quarter, nullable=True)
    if result["zip_sha256"] is not None:
        _state_string(result["zip_sha256"], "ZIP SHA-256", pattern=_SHA256_RE)
    if result["zip_byte_count"] is not None:
        _state_nonnegative_int(result["zip_byte_count"], "ZIP bytes")
    _state_etag(result["etag"], "etag", nullable=True)
    _state_http_date(result["last_modified"], "last modified", nullable=True)
    table_names: list[str] = []
    for entry in _state_list(result["table_evidence"], "table evidence"):
        item = _state_exact_keys(
            entry, {"table_name", "headers", "row_count"}, contract=False
        )
        table_name = _state_code(item["table_name"], "table name", _BACKFILL_TABLES)
        table_names.append(table_name)
        headers = [
            _state_string(header, "header", pattern=_SAFE_CODE_RE)
            for header in _state_list(item["headers"], "headers")
        ]
        if not headers or headers != list(dict.fromkeys(headers)):
            raise _state_error("headers")
        _state_nonnegative_int(item["row_count"], "row count")
    if table_names != sorted(set(table_names)):
        raise _state_error("table evidence")
    missing_optional_tables = [
        _state_code(table, "missing optional table", _BACKFILL_OPTIONAL_TABLES)
        for table in _state_list(
            result["missing_optional_tables"], "missing optional tables"
        )
    ]
    if missing_optional_tables != sorted(set(missing_optional_tables)):
        raise _state_error("missing optional tables")
    present_tables = set(table_names)
    missing_tables = set(missing_optional_tables)
    if present_tables & missing_tables:
        raise _state_error("backfill table evidence bindings")
    if result["status"] == "completed":
        if (
            result["catalog_url"] is None
            or result["zip_url"] is None
            or type(result["zip_sha256"]) is not str
            or not _SHA256_RE.fullmatch(result["zip_sha256"])
            or type(result["zip_byte_count"]) is not int
            or result["zip_byte_count"] <= 0
            or not _REQUIRED_BACKFILL_TABLES <= present_tables
            or present_tables | missing_tables != _BACKFILL_TABLES
        ):
            raise _state_error("completed backfill evidence")
    selected_accessions = _validate_state_accession_list(
        result["selected_accessions"], "selected accessions"
    )
    completed_accessions = _validate_state_accession_list(
        result["completed_accessions"], "completed accessions"
    )
    if not set(completed_accessions) <= set(selected_accessions):
        raise _state_error("backfill completion bindings")
    if (
        result["status"] == "completed"
        and set(completed_accessions) != set(selected_accessions)
    ):
        raise _state_error("backfill completion bindings")
    reconciliation_names: list[str] = []
    for entry in _state_list(result["reconciliation"], "reconciliation"):
        item = _state_exact_keys(
            entry,
            {"name", "expected_count", "actual_count", "status"},
            contract=False,
        )
        name = _state_string(item["name"], "reconciliation name", pattern=_SAFE_CODE_RE)
        reconciliation_names.append(name)
        expected_count = _state_nonnegative_int(
            item["expected_count"], "expected count"
        )
        actual_count = _state_nonnegative_int(item["actual_count"], "actual count")
        reconciliation_status = _state_code(
            item["status"], "reconciliation status", _RECONCILIATION_STATUSES
        )
        if (
            (reconciliation_status == "matched" and expected_count != actual_count)
            or (reconciliation_status == "mismatch" and expected_count == actual_count)
            or (
                reconciliation_status == "not_applicable"
                and (expected_count != 0 or actual_count != 0)
            )
            or (result["status"] == "completed" and reconciliation_status == "pending")
        ):
            raise _state_error("reconciliation")
    if reconciliation_names != sorted(set(reconciliation_names)):
        raise _state_error("reconciliation")
    return result


def _validate_reparse(payload: object) -> dict[str, object]:
    result = _state_exact_keys(
        payload,
        {
            "contract_version", "status", "parser_version", "scope", "scope_identifier",
            "max_accessions", "queue", "completed_accessions",
        },
        version=REPARSE_STATE_CONTRACT_VERSION,
    )
    _state_code(result["status"], "status", _STATE_STATUSES)
    _validate_parser_version(result["parser_version"])
    scope = _state_code(result["scope"], "scope", _REPARSE_SCOPES)
    scope_identifier: str | None
    if scope == "accession":
        scope_identifier = _state_accession(
            result["scope_identifier"], "scope identifier"
        )
    elif scope == "issuer":
        scope_identifier = _state_cik(result["scope_identifier"], "scope identifier")
    else:
        if result["scope_identifier"] is not None:
            raise _state_error("scope identifier")
        scope_identifier = None
    maximum = _state_nonnegative_int(result["max_accessions"], "max accessions")
    if maximum == 0 or maximum > MAX_INSIDER_STATE_COLLECTION:
        raise _state_error("max accessions")
    queue_entries: list[tuple[str, str]] = []
    for entry in _state_list(result["queue"], "queue"):
        item = _state_exact_keys(
            entry, {"accession_number", "issuer_cik"}, contract=False
        )
        queue_entries.append(
            (
                _state_accession(item["accession_number"], "queue accession"),
                _state_cik(item["issuer_cik"], "queue issuer CIK"),
            )
        )
    if (
        queue_entries != sorted(set(queue_entries))
        or len({accession for accession, _ in queue_entries}) != len(queue_entries)
        or len(queue_entries) > maximum
    ):
        raise _state_error("queue")
    if scope == "accession" and (
        maximum != 1
        or [accession for accession, _ in queue_entries] != [scope_identifier]
    ):
        raise _state_error("accession scope bindings")
    if scope == "issuer" and any(
        issuer_cik != scope_identifier for _, issuer_cik in queue_entries
    ):
        raise _state_error("issuer scope bindings")
    completed_accessions = _validate_state_accession_list(
        result["completed_accessions"], "completed accessions"
    )
    queued_accessions = {accession for accession, _ in queue_entries}
    if not set(completed_accessions) <= queued_accessions:
        raise _state_error("reparse completion bindings")
    if result["status"] == "completed" and (
        set(completed_accessions) != queued_accessions
        or (scope == "accession" and queued_accessions != {scope_identifier})
    ):
        raise _state_error("reparse completion bindings")
    return result


def _reparse_authority_issuer_ciks(payload: dict[str, object]) -> frozenset[str]:
    queue = payload["queue"]
    assert isinstance(queue, list)
    issuer_ciks = {
        entry["issuer_cik"]
        for entry in queue
        if isinstance(entry, dict) and type(entry.get("issuer_cik")) is str
    }
    scope = payload["scope"]
    scope_identifier = payload["scope_identifier"]
    if scope == "issuer":
        assert isinstance(scope_identifier, str)
        issuer_ciks.add(scope_identifier)
    return frozenset(issuer_ciks)


def _validate_approved_issuers(payload: object) -> dict[str, object]:
    result = _state_exact_keys(
        payload, {"contract_version", "issuer_ciks"}, version=APPROVED_ISSUERS_STATE_CONTRACT_VERSION
    )
    issuers = [_state_cik(value, "issuer CIK") for value in _state_list(result["issuer_ciks"], "issuer CIKs")]
    if issuers != sorted(set(issuers)):
        raise _state_error("issuer CIKs")
    return result


_ISSUER_STATE_KEYS = {
    "contract_version", "issuer_cik", "accessions", "owner_groups",
    "security_classes", "amendments", "unresolved_ambiguities", "generation_digest",
}
_CANONICAL_AMENDMENT_KEYS = {
    "accession_number", "amends_accession", "confidence", "reason_code", "candidates",
}
_LEGACY_AMENDMENT_KEYS = {
    "accession_number", "effective_accession", "confidence", "reason_code", "candidates",
}


def _migrate_legacy_issuer_state(payload: object) -> object:
    """Safely read the pre-rename v1 amendment key without accepting mixed shapes.

    Issuer state remains contract version 1. New writes must validate the canonical
    ``amends_accession`` shape; only canonical on-disk v1 state whose every
    amendment uses the former exact ``effective_accession`` shape is migrated here.
    The generation digest is deterministically recomputed because the field rename
    changes its canonical serialization.
    """

    if (
        type(payload) is not dict
        or set(payload) != _ISSUER_STATE_KEYS
        or type(payload.get("contract_version")) is not int
        or payload["contract_version"] != ISSUER_STATE_CONTRACT_VERSION
        or type(payload.get("amendments")) is not list
    ):
        return payload
    amendments = payload["amendments"]
    if not amendments:
        return payload
    migrated_amendments: list[dict[str, object]] = []
    for amendment in amendments:
        if type(amendment) is not dict or set(amendment) != _LEGACY_AMENDMENT_KEYS:
            return payload
        migrated_amendments.append(
            {
                "accession_number": amendment["accession_number"],
                "amends_accession": amendment["effective_accession"],
                "confidence": amendment["confidence"],
                "reason_code": amendment["reason_code"],
                "candidates": amendment["candidates"],
            }
        )
    accessions = payload["accessions"]
    if type(accessions) is not list or any(type(accession) is not dict for accession in accessions):
        return payload
    try:
        legacy_resolutions = {
            amendment["accession_number"]: {
                "effective_accession": amendment["effective_accession"],
                "confidence": amendment["confidence"],
                "reason_code": amendment["reason_code"],
                "candidates": amendment["candidates"],
            }
            for amendment in amendments
        }
        legacy_digest = issuer_generation_digest(
            [
                {
                    "accession_number": accession.get("accession_number"),
                    "parser_version": accession.get("parser_version"),
                    "normalized_sha256": accession.get("normalized_sha256"),
                    "amendment_resolution": legacy_resolutions.get(
                        accession.get("accession_number")
                    ),
                }
                for accession in accessions
            ]
        )
    except (TypeError, ValueError):
        return payload
    if payload["generation_digest"] != legacy_digest:
        return payload
    resolutions = {
        amendment["accession_number"]: {
            "amends_accession": amendment["amends_accession"],
            "confidence": amendment["confidence"],
            "reason_code": amendment["reason_code"],
            "candidates": amendment["candidates"],
        }
        for amendment in migrated_amendments
    }
    return {
        **payload,
        "amendments": migrated_amendments,
        "generation_digest": issuer_generation_digest(
            [
                {
                    "accession_number": accession.get("accession_number"),
                    "parser_version": accession.get("parser_version"),
                    "normalized_sha256": accession.get("normalized_sha256"),
                    "amendment_resolution": resolutions.get(
                        accession.get("accession_number")
                    ),
                }
                for accession in accessions
            ]
        ),
    }


def _validate_issuer(payload: object, issuer_cik: str) -> dict[str, object]:
    result = _state_exact_keys(
        payload,
        _ISSUER_STATE_KEYS,
        version=ISSUER_STATE_CONTRACT_VERSION,
    )
    if _state_cik(result["issuer_cik"], "issuer CIK") != issuer_cik:
        raise _state_error("issuer CIK")
    accessions: list[tuple[str, str, str]] = []
    for entry in _state_list(result["accessions"], "accessions"):
        item = _state_exact_keys(
            entry,
            {"accession_number", "parser_version", "normalized_sha256"},
            contract=False,
        )
        accession = _state_accession(item["accession_number"], "accession")
        parser_version = _validate_parser_version(item["parser_version"])
        normalized_sha256 = _state_string(
            item["normalized_sha256"],
            "normalized SHA-256",
            pattern=_SHA256_RE,
        )
        accessions.append((accession, parser_version, normalized_sha256))
    if (
        accessions != sorted(set(accessions))
        or len({accession for accession, _, _ in accessions}) != len(accessions)
    ):
        raise _state_error("accessions")
    allowed_accessions = {accession for accession, _, _ in accessions}
    owner_groups: list[str] = []
    for entry in _state_list(result["owner_groups"], "owner groups"):
        item = _state_exact_keys(entry, {"owner_group_key", "owner_ciks"}, contract=False)
        owner_group_key = _state_string(item["owner_group_key"], "owner group key", pattern=_SHA256_RE)
        owner_ciks = [
            _state_cik(value, "owner CIK")
            for value in _state_list(item["owner_ciks"], "owner CIKs")
        ]
        if not owner_ciks or owner_ciks != sorted(set(owner_ciks)):
            raise _state_error("owner CIKs")
        if section16_owner_group_key(owner_ciks) != owner_group_key:
            raise _state_error("owner group key")
        owner_groups.append(owner_group_key)
    if owner_groups != sorted(set(owner_groups)):
        raise _state_error("owner groups")
    security_classes: list[str] = []
    for entry in _state_list(result["security_classes"], "security classes"):
        item = _state_exact_keys(
            entry, {"security_class_key", "derivative", "title"}, contract=False
        )
        key = _state_string(item["security_class_key"], "security class key", pattern=_SHA256_RE)
        if type(item["derivative"]) is not bool:
            raise _state_error("derivative")
        title = _state_security_title(item["title"])
        if section16_security_class_key(issuer_cik, title, is_derivative=item["derivative"]) != key:
            raise _state_error("security class key")
        security_classes.append(key)
    if security_classes != sorted(set(security_classes)):
        raise _state_error("security classes")
    amendments: list[str] = []
    amendment_resolutions: dict[str, dict[str, object]] = {}
    unresolved_amendments: dict[str, tuple[str, tuple[str, ...]]] = {}
    for entry in _state_list(result["amendments"], "amendments"):
        item = _state_exact_keys(
            entry,
            _CANONICAL_AMENDMENT_KEYS,
            contract=False,
        )
        accession = _state_accession(item["accession_number"], "accession")
        amendments.append(accession)
        candidates = [
            _state_accession(value, "amendment candidates")
            for value in _state_list(item["candidates"], "amendment candidates")
        ]
        if (
            candidates != sorted(set(candidates))
            or accession not in allowed_accessions
            or accession in candidates
            or not set(candidates) <= allowed_accessions
        ):
            raise _state_error("amendment accession bindings")
        confidence = _state_code(
            item["confidence"], "amendment confidence", _AMENDMENT_CONFIDENCES
        )
        reason = _state_code(
            item["reason_code"], "amendment reason", _AMENDMENT_REASON_CODES
        )
        amends_value = item["amends_accession"]
        amends_accession: str | None = None
        if confidence == "unresolved":
            if amends_value is not None or reason not in _AMBIGUITY_REASON_CODES:
                raise _state_error("unresolved amendment")
            if (reason == "no_candidate" and candidates) or (
                reason == "ambiguous_candidates" and len(candidates) < 2
            ):
                raise _state_error("amendment candidates")
            unresolved_amendments[accession] = (reason, tuple(candidates))
        else:
            if reason not in {"explicit_reference", "single_candidate"}:
                raise _state_error("resolved amendment")
            if amends_value is None:
                raise _state_error("amends accession")
            amends_accession = _state_accession(
                amends_value, "amends accession"
            )
            if (
                amends_accession not in candidates
                or amends_accession not in allowed_accessions
                or len(candidates) != 1
            ):
                raise _state_error("amends accession")
        amendment_resolutions[accession] = {
            "amends_accession": amends_accession,
            "confidence": confidence,
            "reason_code": reason,
            "candidates": candidates,
        }
    if amendments != sorted(set(amendments)):
        raise _state_error("amendments")
    ambiguities: list[str] = []
    ambiguity_summaries: dict[str, tuple[str, tuple[str, ...]]] = {}
    for entry in _state_list(result["unresolved_ambiguities"], "unresolved ambiguities"):
        item = _state_exact_keys(
            entry, {"accession_number", "reason_code", "candidates"}, contract=False
        )
        accession = _state_accession(item["accession_number"], "accession")
        ambiguities.append(accession)
        reason = _state_code(
            item["reason_code"], "ambiguity reason", _AMBIGUITY_REASON_CODES
        )
        candidates = [
            _state_accession(value, "ambiguity candidates")
            for value in _state_list(item["candidates"], "ambiguity candidates")
        ]
        if (
            candidates != sorted(set(candidates))
            or accession not in allowed_accessions
            or accession in candidates
            or not set(candidates) <= allowed_accessions
            or (reason == "no_candidate" and candidates)
            or (reason == "ambiguous_candidates" and len(candidates) < 2)
        ):
            raise _state_error("ambiguity accession bindings")
        ambiguity_summaries[accession] = (reason, tuple(candidates))
    if ambiguities != sorted(set(ambiguities)):
        raise _state_error("unresolved ambiguities")
    if ambiguity_summaries != unresolved_amendments:
        raise _state_error("unresolved ambiguity bindings")
    generation_material = [
        {
            "accession_number": accession,
            "parser_version": parser_version,
            "normalized_sha256": normalized_sha256,
            "amendment_resolution": amendment_resolutions.get(accession),
        }
        for accession, parser_version, normalized_sha256 in accessions
    ]
    generation_digest = _state_string(
        result["generation_digest"],
        "generation digest",
        pattern=_SHA256_RE,
    )
    if generation_digest != issuer_generation_digest(generation_material):
        raise _state_error("generation digest")
    return result


def _validate_quarantine(payload: object, *, accession: str | None, quarter: str | None) -> dict[str, object]:
    required = {
        "contract_version", "stage", "error_class", "reason_code", "retry_count",
        "next_retry_at", "parser_version", "source_hashes",
    }
    if accession is not None:
        required |= {
            "accession_number",
            "issuer_cik",
            "form_type",
            "index_url",
            "accepted_at",
            "reporting_owner_ciks",
        }
    else:
        required.add("quarter")
    result = _state_exact_keys(
        payload, required, version=QUARANTINE_STATE_CONTRACT_VERSION
    )
    stage = _state_code(result["stage"], "stage", _QUARANTINE_STAGES)
    _state_code(result["error_class"], "error class", _ERROR_CLASSES)
    reason = _state_code(result["reason_code"], "reason code", _REASON_CODES)
    if reason not in _QUARANTINE_REASON_CODES_BY_STAGE[stage]:
        raise _state_error("stage and reason code")
    _state_nonnegative_int(result["retry_count"], "retry count")
    next_retry = _state_timestamp(
        result["next_retry_at"], "next retry", nullable=True
    )
    if (reason in _TRANSIENT_QUARANTINE_REASON_CODES) != (
        next_retry is not None
    ):
        raise _state_error("retry schedule")
    if result["parser_version"] is not None:
        _validate_parser_version(result["parser_version"])
    source_hashes = [_state_string(digest, "source hash", pattern=_SHA256_RE) for digest in _state_list(result["source_hashes"], "source hashes")]
    if source_hashes != sorted(set(source_hashes)):
        raise _state_error("source hashes")
    if accession is not None:
        if _state_accession(result["accession_number"], "accession") != accession:
            raise _state_error("accession")
        issuer_cik = None
        if result["issuer_cik"] is not None:
            issuer_cik = _state_cik(result["issuer_cik"], "issuer CIK")
        if result["form_type"] is not None:
            _state_string(
                result["form_type"],
                "form type",
                pattern=_SECTION16_FORM_TYPE_RE,
            )
        index_url = result["index_url"]
        accepted_at = _state_timestamp(
            result["accepted_at"], "accepted at", nullable=True
        )
        reporting_owner_ciks = [
            _state_cik(value, "reporting owner CIK")
            for value in _state_list(
                result["reporting_owner_ciks"], "reporting owner CIKs"
            )
        ]
        if reporting_owner_ciks != sorted(set(reporting_owner_ciks)):
            raise _state_error("reporting owner CIKs")
        if index_url is None:
            if accepted_at is not None or reporting_owner_ciks:
                raise _state_error("quarantine filing identity")
        else:
            if issuer_cik is None or accepted_at is None:
                raise _state_error("quarantine filing identity")
            validated_index_url = _state_index_url(
                index_url,
                "index URL",
                accession_number=accession,
                issuer_cik=issuer_cik,
            )
            if urlsplit(validated_index_url).netloc != "www.sec.gov":
                raise _state_error("index URL")
    elif _state_quarter(result["quarter"], "quarter") != quarter:
        raise _state_error("quarter")
    return result


def _validate_telemetry_counters(value: object, label: str) -> None:
    if not isinstance(value, dict) or len(value) > len(_TELEMETRY_COUNTERS):
        raise _state_error(label)
    for key, counter in value.items():
        _state_code(key, "counter name", _TELEMETRY_COUNTERS)
        _state_nonnegative_int(counter, "counter")


def _validate_telemetry(payload: object) -> dict[str, object]:
    result = _state_exact_keys(
        payload, {"contract_version", "counters", "recent_runs"}, version=TELEMETRY_STATE_CONTRACT_VERSION
    )
    _validate_telemetry_counters(result["counters"], "counters")
    recent_runs = _state_list(result["recent_runs"], "recent runs")
    if len(recent_runs) > MAX_TELEMETRY_RECENT_RUNS:
        raise _state_error("recent runs")
    runs: list[tuple[datetime, str]] = []
    for entry in recent_runs:
        item = _state_exact_keys(
            entry,
            {
                "run_id", "status", "started_at", "finished_at", "counters",
                "accession_examples",
            },
            contract=False,
        )
        run_id = _state_string(item["run_id"], "run ID", pattern=_SAFE_CODE_RE)
        status = _state_code(item["status"], "run status", _RUN_STATUSES)
        started = _state_timestamp(item["started_at"], "run start")
        finished = _state_timestamp(item["finished_at"], "run finish", nullable=True)
        assert started is not None
        started_instant = _state_timestamp_instant(started)
        if (
            finished is not None
            and started_instant > _state_timestamp_instant(finished)
        ):
            raise _state_error("run timestamps")
        if (status == "running") != (finished is None):
            raise _state_error("run status and finish")
        _validate_telemetry_counters(item["counters"], "run counters")
        examples = _state_list(item["accession_examples"], "accession examples")
        if len(examples) > MAX_TELEMETRY_ACCESSION_EXAMPLES:
            raise _state_error("accession examples")
        example_accessions: list[str] = []
        for example in examples:
            summary = _state_exact_keys(
                example,
                {
                    "accession_number", "issuer_cik", "form_type", "parser_version",
                    "stage", "outcome", "error_class", "reason_code", "retry_count",
                    "next_retry_at",
                },
                contract=False,
            )
            accession = _state_accession(
                summary["accession_number"], "example accession"
            )
            example_accessions.append(accession)
            if summary["issuer_cik"] is not None:
                _state_cik(summary["issuer_cik"], "example issuer CIK")
            if summary["form_type"] is not None:
                _state_string(
                    summary["form_type"],
                    "example form type",
                    pattern=_SECTION16_FORM_TYPE_RE,
                )
            if summary["parser_version"] is not None:
                _validate_parser_version(summary["parser_version"])
            _state_code(summary["stage"], "example stage", _TELEMETRY_STAGES)
            outcome = _state_code(
                summary["outcome"], "example outcome", _TELEMETRY_OUTCOMES
            )
            error_class = None
            if summary["error_class"] is not None:
                error_class = _state_code(
                    summary["error_class"], "example error class", _ERROR_CLASSES
                )
            reason = None
            if summary["reason_code"] is not None:
                reason = _state_code(
                    summary["reason_code"], "example reason code", _REASON_CODES
                )
            retry_count = _state_nonnegative_int(
                summary["retry_count"], "example retry count"
            )
            next_retry = _state_timestamp(
                summary["next_retry_at"], "example next retry", nullable=True
            )
            error_outcome = outcome in {"quarantined", "retry_later"}
            if (
                (error_class is None) != (reason is None)
                or (error_class is not None) != error_outcome
                or (reason in _TRANSIENT_QUARANTINE_REASON_CODES)
                != (next_retry is not None)
                or (outcome == "retry_later")
                != (reason in _TRANSIENT_QUARANTINE_REASON_CODES)
                or (outcome == "retry_later" and retry_count == 0)
            ):
                raise _state_error("accession example outcome")
        if example_accessions != sorted(set(example_accessions)):
            raise _state_error("accession examples")
        runs.append((started_instant, run_id))
    if runs != sorted(set(runs)) or len({run_id for _, run_id in runs}) != len(runs):
        raise _state_error("recent runs")
    return result


def _state_key_details(key: object) -> tuple[tuple[str, ...], str, str | None]:
    if type(key) is not str or not key or len(key) > 256:
        raise _state_error("state key")
    exact = {
        "incremental-v1": ("incremental", "incremental-v1.json", None),
        "reparse-v1": ("reparse", "reparse-v1.json", None),
        "approved-issuers-v1": ("approved", "approved-issuers-v1.json", None),
        "telemetry-v1": ("telemetry", "telemetry-v1.json", None),
    }
    if key in exact:
        kind, filename, identifier = exact[key]
        return (filename,), kind, identifier
    match = re.fullmatch(r"backfill/([0-9]{4}Q[1-4])", key)
    if match and int(match.group(1)[:4]) >= 2006:
        return ("backfill", f"{match.group(1)}.json"), "backfill", match.group(1)
    match = re.fullmatch(r"issuers/([0-9]{10})", key)
    if match and match.group(1) != "0000000000":
        return ("issuers", f"{match.group(1)}.json"), "issuer", match.group(1)
    match = re.fullmatch(r"quarantine/accessions/([0-9]{10}-[0-9]{2}-[0-9]{6})", key)
    if match:
        return ("quarantine", "accessions", f"{match.group(1)}.json"), "accession_quarantine", match.group(1)
    match = re.fullmatch(r"quarantine/quarters/([0-9]{4}Q[1-4])", key)
    if match and int(match.group(1)[:4]) >= 2006:
        return ("quarantine", "quarters", f"{match.group(1)}.json"), "quarter_quarantine", match.group(1)
    raise _state_error("state key")


def _validate_state_payload(kind: str, identifier: str | None, payload: object) -> dict[str, object]:
    expected_version = _STATE_CONTRACT_VERSIONS[kind]
    if (
        not isinstance(payload, dict)
        or type(payload.get("contract_version")) is not int
        or payload["contract_version"] != expected_version
    ):
        raise _state_error("contract version")
    if kind == "incremental":
        return _validate_incremental(payload)
    if kind == "reparse":
        return _validate_reparse(payload)
    if kind == "approved":
        return _validate_approved_issuers(payload)
    if kind == "backfill":
        assert identifier is not None
        return _validate_backfill(payload, identifier)
    if kind == "issuer":
        assert identifier is not None
        return _validate_issuer(payload, identifier)
    if kind == "accession_quarantine":
        assert identifier is not None
        return _validate_quarantine(payload, accession=identifier, quarter=None)
    if kind == "quarter_quarantine":
        assert identifier is not None
        return _validate_quarantine(payload, accession=None, quarter=identifier)
    assert kind == "telemetry"
    return _validate_telemetry(payload)


def canonical_insider_state_json_bytes(payload: object) -> bytes:
    try:
        return json.dumps(
            payload, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8") + b"\n"
    except (TypeError, ValueError) as error:
        raise _state_error("JSON") from error


def issuer_generation_digest(generation_material: object) -> str:
    """Hash the canonical issuer inputs that determine amendment resolution."""

    return hashlib.sha256(
        b"section16-issuer-generation-v1\0"
        + canonical_insider_state_json_bytes(generation_material)
    ).hexdigest()


_STATE_DIRECTORY_OPERATION_LOCAL = threading.local()


@contextmanager
def _state_directory_operation(state_root: Path) -> Iterator[None]:
    """Reject same-thread re-entry before attempting a second state-root flock."""

    process_id = os.getpid()
    if getattr(_STATE_DIRECTORY_OPERATION_LOCAL, "process_id", None) != process_id:
        _STATE_DIRECTORY_OPERATION_LOCAL.process_id = process_id
        _STATE_DIRECTORY_OPERATION_LOCAL.roots = set()
    roots = _STATE_DIRECTORY_OPERATION_LOCAL.roots
    root_key = os.fspath(state_root)
    if root_key in roots:
        raise InsiderStorageError(
            "private insider state operation cannot re-enter the same state root"
        )
    roots.add(root_key)
    try:
        yield
    finally:
        roots.remove(root_key)
        if not roots:
            del _STATE_DIRECTORY_OPERATION_LOCAL.roots
            del _STATE_DIRECTORY_OPERATION_LOCAL.process_id


class InsiderStateStore:
    """Crash-safe, allowlisted mutable private state separate from 13F state."""

    def __init__(self, repository_root: Path) -> None:
        root = Path(repository_root)
        if root.is_symlink() or not root.is_dir():
            raise InsiderStorageError("repository root must be an existing real directory")
        self.repository_root = root.resolve()
        self.state_root = self.repository_root / PRIVATE_INSIDER_STATE_ROOT

    @contextmanager
    def _open_state_directory(self, *, create: bool) -> Iterator[tuple[int, Path]]:
        with _state_directory_operation(self.state_root):
            try:
                descriptor = os.open(self.repository_root, _DIRECTORY_OPEN_FLAGS)
            except OSError as error:
                raise _unsafe_storage_error("root", error) from error
            try:
                for index, part in enumerate(PRIVATE_INSIDER_STATE_ROOT.parts):
                    child, _ = _open_child_directory(
                        descriptor, part, create=create, restricted=index >= 2
                    )
                    os.close(descriptor)
                    descriptor = child
                with _exclusive_directory_lock(descriptor):
                    yield descriptor, self.state_root
            finally:
                os.close(descriptor)

    def _read_locked(
        self, descriptor: int, target_name: str, kind: str, identifier: str | None
    ) -> dict[str, object]:
        try:
            rendered = _read_regular_file(
                descriptor, target_name, max_bytes=MAX_INSIDER_STATE_BYTES
            )
            parsed = _strict_json_bytes(rendered)
            candidate = (
                _migrate_legacy_issuer_state(parsed)
                if kind == "issuer"
                else parsed
            )
            validated = _validate_state_payload(kind, identifier, candidate)
            if candidate is parsed:
                if canonical_insider_state_json_bytes(validated) != rendered:
                    raise _state_error("canonical JSON")
            elif canonical_insider_state_json_bytes(parsed) != rendered:
                raise _state_error("canonical JSON")
            return validated
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            RecursionError,
            TypeError,
            ValueError,
        ) as error:
            if isinstance(error, InsiderStorageError):
                raise
            raise _state_error("JSON") from error

    def _write_locked(
        self,
        descriptor: int,
        target_name: str,
        target: Path,
        kind: str,
        identifier: str | None,
        payload: object,
    ) -> StoredArtifact:
        validated = _validate_state_payload(kind, identifier, payload)
        rendered = canonical_insider_state_json_bytes(validated)
        if len(rendered) > MAX_INSIDER_STATE_BYTES:
            raise _state_error("size limit")
        _remove_safe_stale_temporary_artifacts(descriptor, target_name)
        try:
            self._read_locked(descriptor, target_name, kind, identifier)
        except FileNotFoundError:
            pass
        temporary_name = ""
        temporary_descriptor = -1
        try:
            for _ in range(10):
                temporary_name = f".{target_name}.tmp-{secrets.token_hex(12)}"
                try:
                    temporary_descriptor = os.open(
                        temporary_name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                        0o600,
                        dir_fd=descriptor,
                    )
                    break
                except FileExistsError:
                    continue
            if temporary_descriptor < 0:
                raise InsiderStorageError("could not allocate a private state temporary file")
            os.fchmod(temporary_descriptor, 0o600)
            remaining = memoryview(rendered)
            while remaining:
                written = os.write(temporary_descriptor, remaining)
                if written < 1:
                    raise InsiderStorageError("private state write did not make progress")
                remaining = remaining[written:]
            os.fsync(temporary_descriptor)
            os.close(temporary_descriptor)
            temporary_descriptor = -1
            os.replace(
                temporary_name,
                target_name,
                src_dir_fd=descriptor,
                dst_dir_fd=descriptor,
            )
            temporary_name = ""
            os.fsync(descriptor)
        except BaseException:
            if temporary_descriptor >= 0:
                os.close(temporary_descriptor)
            if temporary_name:
                try:
                    os.unlink(temporary_name, dir_fd=descriptor)
                except FileNotFoundError:
                    pass
                os.fsync(descriptor)
            raise
        stored = self._read_locked(descriptor, target_name, kind, identifier)
        if stored != validated:
            raise InsiderStorageError("private state changed during write")
        return StoredArtifact(
            path=target,
            sha256=hashlib.sha256(rendered).hexdigest(),
            byte_count=len(rendered),
            created=True,
        )

    def _compare_and_write_locked(
        self,
        descriptor: int,
        target_name: str,
        target: Path,
        kind: str,
        identifier: str | None,
        validated: dict[str, object],
        rendered: bytes,
        expected_sha256: str | None,
    ) -> StoredArtifact:
        _remove_safe_stale_temporary_artifacts(descriptor, target_name)
        try:
            existing = self._read_locked(
                descriptor, target_name, kind, identifier
            )
        except FileNotFoundError:
            if expected_sha256 is not None:
                raise InsiderStateRevisionError(
                    "private state revision is stale"
                ) from None
        else:
            existing_bytes = canonical_insider_state_json_bytes(existing)
            existing_sha256 = hashlib.sha256(existing_bytes).hexdigest()
            if (
                expected_sha256 is not None
                and expected_sha256 != existing_sha256
            ):
                raise InsiderStateRevisionError("private state revision is stale")
            if existing_bytes == rendered:
                return StoredArtifact(
                    path=target,
                    sha256=existing_sha256,
                    byte_count=len(existing_bytes),
                    created=False,
                )
            if expected_sha256 is None:
                raise InsiderStateRevisionError("private state revision is required")
        return self._write_locked(
            descriptor, target_name, target, kind, identifier, validated
        )

    def write(
        self, key: str, payload: object, *, expected_sha256: str | None = None
    ) -> StoredArtifact:
        """Create state, or atomically compare-and-swap a known revision.

        Supplying no revision is intentionally idempotent-only: a different existing
        state is rejected rather than silently overwritten.  Use ``update`` for a
        lock-held read/modify/write operation.
        """
        if expected_sha256 is not None and (
            type(expected_sha256) is not str or not _SHA256_RE.fullmatch(expected_sha256)
        ):
            raise _state_error("expected SHA-256")
        parts, kind, identifier = _state_key_details(key)
        if kind in {"backfill", "issuer", "reparse"}:
            raise InsiderApprovalScopeError(
                f"{kind} state requires approval-gated mutation"
            )
        target_name = parts[-1]
        validated = _validate_state_payload(kind, identifier, payload)
        rendered = canonical_insider_state_json_bytes(validated)
        if len(rendered) > MAX_INSIDER_STATE_BYTES:
            raise _state_error("size limit")
        with self._open_state_directory(create=True) as (descriptor, root):
            parent_descriptor = descriptor
            try:
                for part in parts[:-1]:
                    child, _ = _open_child_directory(
                        parent_descriptor, part, create=True, restricted=True
                    )
                    if parent_descriptor != descriptor:
                        os.close(parent_descriptor)
                    parent_descriptor = child
                target = root.joinpath(*parts)
                return self._compare_and_write_locked(
                    parent_descriptor,
                    target_name,
                    target,
                    kind,
                    identifier,
                    validated,
                    rendered,
                    expected_sha256,
                )
            finally:
                if parent_descriptor != descriptor:
                    os.close(parent_descriptor)

    def publish_if_issuer_approved(
        self,
        issuer_cik: str,
        publish: Callable[[], _PublicationResult],
    ) -> _PublicationResult:
        """Run one immutable publication under current durable issuer authority."""

        issuer = _state_cik(issuer_cik, "issuer CIK")
        if not callable(publish):
            raise TypeError("publication callback must be callable")
        with self._open_state_directory(create=False) as (descriptor, _):
            approved = self._read_locked(
                descriptor,
                "approved-issuers-v1.json",
                "approved",
                None,
            )
            approved_values = approved["issuer_ciks"]
            assert isinstance(approved_values, list)
            if issuer not in approved_values:
                raise InsiderApprovalScopeError(
                    "immutable publication is outside the approved issuer scope"
                )
            return publish()

    def write_accession_quarantine_if_issuer_approved(
        self,
        accession_number: str,
        issuer_cik: str,
        payload: object,
        *,
        expected_sha256: str | None = None,
    ) -> StoredArtifact:
        """Atomically publish one accession quarantine under issuer authority."""

        if expected_sha256 is not None and (
            type(expected_sha256) is not str
            or not _SHA256_RE.fullmatch(expected_sha256)
        ):
            raise _state_error("expected SHA-256")
        accession = _state_accession(accession_number, "accession number")
        issuer = _state_cik(issuer_cik, "issuer CIK")
        parts, kind, identifier = _state_key_details(
            f"quarantine/accessions/{accession}"
        )
        target_name = parts[-1]
        validated = _validate_state_payload(kind, identifier, payload)
        if validated.get("issuer_cik") != issuer:
            raise InsiderApprovalScopeError(
                "accession quarantine issuer does not match its authority"
            )
        rendered = canonical_insider_state_json_bytes(validated)
        if len(rendered) > MAX_INSIDER_STATE_BYTES:
            raise _state_error("size limit")

        with self._open_state_directory(create=False) as (descriptor, root):
            approved = self._read_locked(
                descriptor,
                "approved-issuers-v1.json",
                "approved",
                None,
            )
            approved_values = approved["issuer_ciks"]
            assert isinstance(approved_values, list)
            if issuer not in approved_values:
                raise InsiderApprovalScopeError(
                    "accession quarantine is outside the approved issuer scope"
                )

            parent_descriptor = descriptor
            try:
                for part in parts[:-1]:
                    child, _ = _open_child_directory(
                        parent_descriptor,
                        part,
                        create=True,
                        restricted=True,
                    )
                    if parent_descriptor != descriptor:
                        os.close(parent_descriptor)
                    parent_descriptor = child
                return self._compare_and_write_locked(
                    parent_descriptor,
                    target_name,
                    root.joinpath(*parts),
                    kind,
                    identifier,
                    validated,
                    rendered,
                    expected_sha256,
                )
            finally:
                if parent_descriptor != descriptor:
                    os.close(parent_descriptor)

    def write_incremental_if_issuers_approved(
        self,
        payload: object,
        *,
        expected_sha256: str | None = None,
    ) -> StoredArtifact:
        """Atomically bind an incremental checkpoint to durable issuer authority."""

        if expected_sha256 is not None and (
            type(expected_sha256) is not str
            or not _SHA256_RE.fullmatch(expected_sha256)
        ):
            raise _state_error("expected SHA-256")
        validated = _validate_state_payload("incremental", None, payload)
        rendered = canonical_insider_state_json_bytes(validated)
        if len(rendered) > MAX_INSIDER_STATE_BYTES:
            raise _state_error("size limit")
        queue = validated["queue"]
        assert isinstance(queue, list)
        issuer_ciks = {
            entry["issuer_cik"]
            for entry in queue
            if isinstance(entry, dict)
        }
        with self._open_state_directory(create=False) as (descriptor, root):
            approved = self._read_locked(
                descriptor,
                "approved-issuers-v1.json",
                "approved",
                None,
            )
            approved_values = approved["issuer_ciks"]
            assert isinstance(approved_values, list)
            if not issuer_ciks <= set(approved_values):
                raise InsiderApprovalScopeError(
                    "incremental state is outside the approved issuer scope"
                )
            return self._compare_and_write_locked(
                descriptor,
                "incremental-v1.json",
                root / "incremental-v1.json",
                "incremental",
                None,
                validated,
                rendered,
                expected_sha256,
            )

    def write_reparse_if_issuers_approved(
        self,
        payload: object,
        *,
        expected_sha256: str | None = None,
    ) -> StoredArtifact:
        """Atomically bind a reparse checkpoint to durable issuer authority."""

        if expected_sha256 is not None and (
            type(expected_sha256) is not str
            or not _SHA256_RE.fullmatch(expected_sha256)
        ):
            raise _state_error("expected SHA-256")
        validated = _validate_state_payload("reparse", None, payload)
        rendered = canonical_insider_state_json_bytes(validated)
        if len(rendered) > MAX_INSIDER_STATE_BYTES:
            raise _state_error("size limit")
        issuer_ciks = _reparse_authority_issuer_ciks(validated)
        with self._open_state_directory(create=False) as (descriptor, root):
            approved = self._read_locked(
                descriptor,
                "approved-issuers-v1.json",
                "approved",
                None,
            )
            approved_values = approved["issuer_ciks"]
            assert isinstance(approved_values, list)
            if not issuer_ciks <= set(approved_values):
                raise InsiderApprovalScopeError(
                    "reparse state is outside the approved issuer scope"
                )
            return self._compare_and_write_locked(
                descriptor,
                "reparse-v1.json",
                root / "reparse-v1.json",
                "reparse",
                None,
                validated,
                rendered,
                expected_sha256,
            )

    def write_backfill_if_issuer_approved(
        self,
        quarter: str,
        issuer_cik: str,
        payload: object,
        *,
        expected_sha256: str | None = None,
    ) -> StoredArtifact:
        """Atomically write one quarterly checkpoint under current issuer authority."""

        if expected_sha256 is not None and (
            type(expected_sha256) is not str
            or not _SHA256_RE.fullmatch(expected_sha256)
        ):
            raise _state_error("expected SHA-256")
        canonical_quarter = _state_quarter(quarter, "quarter")
        issuer = _state_cik(issuer_cik, "issuer CIK")
        parts, kind, identifier = _state_key_details(
            f"backfill/{canonical_quarter}"
        )
        target_name = parts[-1]
        validated = _validate_state_payload(kind, identifier, payload)
        if validated.get("issuer_cik") != issuer:
            raise InsiderApprovalScopeError(
                "backfill state does not match its issuer authority"
            )
        rendered = canonical_insider_state_json_bytes(validated)
        if len(rendered) > MAX_INSIDER_STATE_BYTES:
            raise _state_error("size limit")

        with self._open_state_directory(create=False) as (descriptor, root):
            approved = self._read_locked(
                descriptor,
                "approved-issuers-v1.json",
                "approved",
                None,
            )
            approved_values = approved["issuer_ciks"]
            assert isinstance(approved_values, list)
            if issuer not in approved_values:
                raise InsiderApprovalScopeError(
                    "backfill state is outside the approved issuer scope"
                )

            parent_descriptor = descriptor
            try:
                for part in parts[:-1]:
                    child, _ = _open_child_directory(
                        parent_descriptor,
                        part,
                        create=True,
                        restricted=True,
                    )
                    if parent_descriptor != descriptor:
                        os.close(parent_descriptor)
                    parent_descriptor = child
                return self._compare_and_write_locked(
                    parent_descriptor,
                    target_name,
                    root.joinpath(*parts),
                    kind,
                    identifier,
                    validated,
                    rendered,
                    expected_sha256,
                )
            finally:
                if parent_descriptor != descriptor:
                    os.close(parent_descriptor)

    def write_issuer_if_approved(
        self,
        issuer_cik: str,
        payload: object,
        *,
        expected_sha256: str | None = None,
    ) -> StoredArtifact:
        """Atomically write one derived issuer state under current authority."""

        if expected_sha256 is not None and (
            type(expected_sha256) is not str
            or not _SHA256_RE.fullmatch(expected_sha256)
        ):
            raise _state_error("expected SHA-256")
        issuer = _state_cik(issuer_cik, "issuer CIK")
        parts, kind, identifier = _state_key_details(f"issuers/{issuer}")
        target_name = parts[-1]
        validated = _validate_state_payload(kind, identifier, payload)
        if validated.get("issuer_cik") != issuer:
            raise InsiderApprovalScopeError(
                "issuer state does not match its authority"
            )
        rendered = canonical_insider_state_json_bytes(validated)
        if len(rendered) > MAX_INSIDER_STATE_BYTES:
            raise _state_error("size limit")

        with self._open_state_directory(create=False) as (descriptor, root):
            approved = self._read_locked(
                descriptor,
                "approved-issuers-v1.json",
                "approved",
                None,
            )
            approved_values = approved["issuer_ciks"]
            assert isinstance(approved_values, list)
            if issuer not in approved_values:
                raise InsiderApprovalScopeError(
                    "issuer state is outside the approved issuer scope"
                )

            parent_descriptor = descriptor
            try:
                for part in parts[:-1]:
                    child, _ = _open_child_directory(
                        parent_descriptor,
                        part,
                        create=True,
                        restricted=True,
                    )
                    if parent_descriptor != descriptor:
                        os.close(parent_descriptor)
                    parent_descriptor = child
                return self._compare_and_write_locked(
                    parent_descriptor,
                    target_name,
                    root.joinpath(*parts),
                    kind,
                    identifier,
                    validated,
                    rendered,
                    expected_sha256,
                )
            finally:
                if parent_descriptor != descriptor:
                    os.close(parent_descriptor)

    def update_incremental_if_issuers_approved(
        self,
        transform: Callable[[dict[str, object]], object],
    ) -> dict[str, object]:
        """Atomically update incremental state under current durable authority."""

        if not callable(transform):
            raise TypeError("state transform must be callable")
        with self._open_state_directory(create=False) as (descriptor, root):
            approved = self._read_locked(
                descriptor,
                "approved-issuers-v1.json",
                "approved",
                None,
            )
            approved_values = approved["issuer_ciks"]
            assert isinstance(approved_values, list)

            current = self._read_locked(
                descriptor,
                "incremental-v1.json",
                "incremental",
                None,
            )
            candidate = transform(dict(current))
            validated = _validate_state_payload("incremental", None, candidate)
            queue = validated["queue"]
            assert isinstance(queue, list)
            issuer_ciks = {
                entry["issuer_cik"]
                for entry in queue
                if isinstance(entry, dict)
            }
            if not issuer_ciks <= set(approved_values):
                raise InsiderApprovalScopeError(
                    "incremental state is outside the approved issuer scope"
                )
            self._write_locked(
                descriptor,
                "incremental-v1.json",
                root / "incremental-v1.json",
                "incremental",
                None,
                validated,
            )
            return validated

    def update_reparse_if_issuers_approved(
        self,
        transform: Callable[[dict[str, object]], object],
    ) -> dict[str, object]:
        """Atomically update reparse state under current durable authority."""

        if not callable(transform):
            raise TypeError("state transform must be callable")
        with self._open_state_directory(create=False) as (descriptor, root):
            approved = self._read_locked(
                descriptor,
                "approved-issuers-v1.json",
                "approved",
                None,
            )
            approved_values = approved["issuer_ciks"]
            assert isinstance(approved_values, list)

            current = self._read_locked(
                descriptor,
                "reparse-v1.json",
                "reparse",
                None,
            )
            candidate = transform(dict(current))
            validated = _validate_state_payload("reparse", None, candidate)
            issuer_ciks = _reparse_authority_issuer_ciks(validated)
            if not issuer_ciks <= set(approved_values):
                raise InsiderApprovalScopeError(
                    "reparse state is outside the approved issuer scope"
                )
            self._write_locked(
                descriptor,
                "reparse-v1.json",
                root / "reparse-v1.json",
                "reparse",
                None,
                validated,
            )
            return validated

    def update_backfill_if_issuer_approved(
        self,
        quarter: str,
        issuer_cik: str,
        transform: Callable[[dict[str, object]], object],
    ) -> dict[str, object]:
        """Atomically update one quarterly checkpoint under current authority."""

        if not callable(transform):
            raise TypeError("state transform must be callable")
        canonical_quarter = _state_quarter(quarter, "quarter")
        issuer = _state_cik(issuer_cik, "issuer CIK")
        parts, kind, identifier = _state_key_details(
            f"backfill/{canonical_quarter}"
        )
        with self._open_state_directory(create=False) as (descriptor, root):
            approved = self._read_locked(
                descriptor,
                "approved-issuers-v1.json",
                "approved",
                None,
            )
            approved_values = approved["issuer_ciks"]
            assert isinstance(approved_values, list)
            if issuer not in approved_values:
                raise InsiderApprovalScopeError(
                    "backfill state is outside the approved issuer scope"
                )

            parent_descriptor = descriptor
            try:
                for part in parts[:-1]:
                    child, _ = _open_child_directory(
                        parent_descriptor,
                        part,
                        create=False,
                        restricted=True,
                    )
                    if parent_descriptor != descriptor:
                        os.close(parent_descriptor)
                    parent_descriptor = child
                current = self._read_locked(
                    parent_descriptor,
                    parts[-1],
                    kind,
                    identifier,
                )
                candidate = transform(dict(current))
                validated = _validate_state_payload(kind, identifier, candidate)
                if validated.get("issuer_cik") != issuer:
                    raise InsiderApprovalScopeError(
                        "backfill state does not match its issuer authority"
                    )
                self._write_locked(
                    parent_descriptor,
                    parts[-1],
                    root.joinpath(*parts),
                    kind,
                    identifier,
                    validated,
                )
                return validated
            finally:
                if parent_descriptor != descriptor:
                    os.close(parent_descriptor)

    def read(self, key: str) -> dict[str, object]:
        parts, kind, identifier = _state_key_details(key)
        with self._open_state_directory(create=False) as (descriptor, _):
            parent_descriptor = descriptor
            try:
                for part in parts[:-1]:
                    child, _ = _open_child_directory(
                        parent_descriptor, part, create=False, restricted=True
                    )
                    if parent_descriptor != descriptor:
                        os.close(parent_descriptor)
                    parent_descriptor = child
                return self._read_locked(parent_descriptor, parts[-1], kind, identifier)
            finally:
                if parent_descriptor != descriptor:
                    os.close(parent_descriptor)

    def update(
        self, key: str, transform: Callable[[dict[str, object]], object]
    ) -> dict[str, object]:
        if not callable(transform):
            raise TypeError("state transform must be callable")
        parts, kind, identifier = _state_key_details(key)
        if kind in {"backfill", "issuer", "reparse"}:
            raise InsiderApprovalScopeError(
                f"{kind} state requires approval-gated mutation"
            )
        with self._open_state_directory(create=False) as (descriptor, root):
            parent_descriptor = descriptor
            try:
                for part in parts[:-1]:
                    child, _ = _open_child_directory(
                        parent_descriptor, part, create=False, restricted=True
                    )
                    if parent_descriptor != descriptor:
                        os.close(parent_descriptor)
                    parent_descriptor = child
                current = self._read_locked(parent_descriptor, parts[-1], kind, identifier)
                candidate = transform(dict(current))
                self._write_locked(
                    parent_descriptor,
                    parts[-1],
                    root.joinpath(*parts),
                    kind,
                    identifier,
                    candidate,
                )
                return _validate_state_payload(kind, identifier, candidate)
            finally:
                if parent_descriptor != descriptor:
                    os.close(parent_descriptor)


__all__ = [
    "APPROVED_ISSUERS_STATE_CONTRACT_VERSION",
    "BACKFILL_STATE_CONTRACT_VERSION",
    "INCREMENTAL_STATE_CONTRACT_VERSION",
    "ISSUER_STATE_CONTRACT_VERSION",
    "MAX_INSIDER_STATE_BYTES",
    "MAX_INSIDER_STATE_COLLECTION",
    "MAX_INSIDER_STATE_INTEGER",
    "MAX_INSIDER_STATE_STRING_CHARS",
    "MAX_TELEMETRY_ACCESSION_EXAMPLES",
    "MAX_TELEMETRY_RECENT_RUNS",
    "QUARANTINE_STATE_CONTRACT_VERSION",
    "REPARSE_STATE_CONTRACT_VERSION",
    "TELEMETRY_STATE_CONTRACT_VERSION",
    "MAX_NORMALIZED_JSON_BYTES",
    "MAX_RAW_XML_BYTES",
    "PRIVATE_INSIDER_ROOT",
    "PRIVATE_INSIDER_STATE_ROOT",
    "ImmutableInsiderStorageConflict",
    "InsiderApprovalScopeError",
    "InsiderStateRevisionError",
    "InsiderStateStore",
    "InsiderStorage",
    "InsiderStorageError",
    "StoredArtifact",
    "canonical_insider_state_json_bytes",
    "issuer_generation_digest",
    "validate_incremental_state_payload",
]
