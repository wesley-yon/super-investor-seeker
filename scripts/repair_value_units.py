#!/usr/bin/env python3
"""Audit or repair verified historical 1,000x value-unit errors.

This is an idempotent migration for data produced by the former unweighted
median-price heuristic. It uses peer consensus for broad discovery plus a
narrow SEC-backed manifest for cases peer prices cannot classify. By default it
reports candidates; pass ``--apply`` to write mechanically safe repairs and
recompute composition hashes. ``--migrate-policy`` performs the one-time
policy-v2 cutover only after a bounded peer and adjacent-quarter scan of every
retained quarter succeeds.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import secrets
import stat
import statistics
import sys
from collections import Counter, defaultdict
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import validate_data
from value_units import (
    VALUE_UNIT_POLICY_VERSION,
    is_unit_evidence_holding,
    peer_scale_evidence,
)


FUNDS_DIR = ROOT / "data" / "funds"
STATE_PATH = ROOT / "data" / "pipeline_state.json"
LEGACY_VALUE_UNIT_POLICY_VERSION = 1
MAX_SOURCE_ASSIGNMENT_COMPONENTS = 128
MAX_SOURCE_ASSIGNMENT_STATES = 100_000
REPAIR_TRANSACTION_NAME = ".value-unit-repair-transaction"
REPAIR_PREPARE_NAME = ".value-unit-repair-prepare"
REPAIR_CLEANUP_NAME = ".value-unit-repair-cleanup"
REPAIR_MARKER_NAME = "transaction.json"
REPAIR_MARKER_VERSION = 2

# Auditable manifest of the historical quarters repaired by this migration.
# The value is the correct multiplier for the SEC-reported source values.
KNOWN_REPAIRS: dict[int, dict[int, tuple[str, ...]]] = {
    1: {
        724683: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
        919185: ("2025-06-30", "2025-09-30", "2025-12-31"),
        920760: ("2025-12-31", "2026-03-31"),
        1042537: ("2025-06-30",),
        1259927: ("2025-06-30",),
        1301396: ("2026-03-31",),
        1346554: ("2025-06-30", "2025-09-30", "2026-03-31"),
        1395064: ("2025-06-30",),
        1453885: ("2025-06-30", "2025-12-31"),
        1524362: ("2025-09-30",),
        1533551: ("2026-03-31",),
        1600745: ("2025-06-30",),
        1674784: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
        1698093: ("2025-06-30",),
        1738654: ("2025-06-30",),
        1746382: ("2025-09-30",),
        1750183: ("2025-09-30", "2025-12-31"),
        1778719: ("2025-06-30", "2025-09-30"),
        1782866: ("2025-06-30",),
        1801577: ("2025-06-30",),
        1815467: ("2026-03-31",),
        1853431: ("2025-06-30",),
        1854423: ("2025-12-31", "2026-03-31"),
        1857418: ("2025-09-30",),
        1869028: ("2025-12-31", "2026-03-31"),
        1878495: ("2025-06-30", "2025-09-30"),
        1905106: ("2025-09-30", "2025-12-31", "2026-03-31"),
        1910592: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
        1950677: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
        1964236: ("2025-06-30", "2025-09-30"),
        1983408: ("2025-06-30",),
        2032489: ("2025-12-31",),
    },
    1000: {
        1624050: ("2025-09-30", "2025-12-31", "2026-03-31"),
        1912699: ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"),
    },
}

# SEC-verified repairs that cannot be recovered by the peer-price scan:
# non-equity filings, later quarters absent from the original migration, and
# one filing whose rows used mixed units. Exact totals and row signatures make
# this manifest fail closed if the historical inputs ever differ.
EXPLICIT_HISTORICAL_REPAIRS: dict[tuple[int, str], dict] = {
    (1629996, "2025-09-30"): {
        "accession": "0001629996-25-000006",
        "operation": "multiply_all",
        "factor": 1000,
        "bad_total": 61_071,
        "correct_total": 61_071_000,
        "holding_count": 36,
        "bad_signature": (
            "8856181ddc3c72cd455ae998c694fdaadc93ff1600834421505bc5189cf7c26f"
        ),
        "correct_signature": (
            "db3bafdebeee0b073b296bb057ed1b2da8e5d6fb629dbe9735348fc18ec7582e"
        ),
        "value_multiplier": 1000,
        "repair_status": "understated_1000x",
    },
    (1629996, "2025-12-31"): {
        "accession": "0001629996-26-000001",
        "operation": "multiply_all",
        "factor": 1000,
        "bad_total": 61_299,
        "correct_total": 61_299_000,
        "holding_count": 38,
        "bad_signature": (
            "e89e43256e4e400fcd8528ea4eea3b2ef1e2c677ebb214307d701257ad8ab5a6"
        ),
        "correct_signature": (
            "9e5f537c6ae4639a4ede036d4959fc6bd3a3ed255bba2035d979f06d2e2e4442"
        ),
        "value_multiplier": 1000,
        "repair_status": "understated_1000x",
    },
    (1631562, "2025-06-30"): {
        "accession": "0001631562-25-000005",
        "operation": "multiply_except_cusips",
        "factor": 1000,
        "unscaled_cusips": ("002824100",),
        "bad_total": 124_792_282,
        "correct_total": 6_102_754_336,
        "holding_count": 53,
        "bad_signature": (
            "bc3e032305c4b7ce5d04e5bf2f1e0448bdf0a1afa8c6e2f7e12951ca75cf2074"
        ),
        "correct_signature": (
            "04531b33766ffc529c200dc5fcb383347ad3d542e02b04e4941080f10fa05d16"
        ),
        "value_multiplier": None,
        "repair_status": "mixed_source_units",
    },
    (1738654, "2025-09-30"): {
        "accession": "0000919574-25-006977",
        "operation": "divide_all",
        "divisor": 1000,
        "bad_total": 1_600_206_697_000,
        "correct_total": 1_600_206_697,
        "holding_count": 44,
        "bad_signature": (
            "5c8296654d4edafb2303bf2d4f577d382e67239acebaf988db1864b3324bbe97"
        ),
        "correct_signature": (
            "202d78fe815f85a8847875c89f6a6d0837e975b723503fb3b2329ef30e96e69d"
        ),
        "value_multiplier": 1,
        "repair_status": "inflated_1000x",
    },
    (1738654, "2025-12-31"): {
        "accession": "0000919574-26-001163",
        "operation": "divide_all",
        "divisor": 1000,
        "bad_total": 1_371_559_130_000,
        "correct_total": 1_371_559_130,
        "holding_count": 43,
        "bad_signature": (
            "b7dff725ce4689abef84f690533f3d8529f1b2bcb3100cd5748ed2a0a18c3ab1"
        ),
        "correct_signature": (
            "f460c49f4b9320d81703323df9f9fadcdc265f3f3242eb86bdd44740e76e6cfb"
        ),
        "value_multiplier": 1,
        "repair_status": "inflated_1000x",
    },
    (1738654, "2026-03-31"): {
        "accession": "0000919574-26-003413",
        "operation": "divide_all",
        "divisor": 1000,
        "bad_total": 1_388_169_795_000,
        "correct_total": 1_388_169_795,
        "holding_count": 36,
        "bad_signature": (
            "6aa30e73aeb9f46a4b9824b8e2d0d93fed745f9b79dad8571921844d4a51caf1"
        ),
        "correct_signature": (
            "6bf6ea4c40d43820002575f32ac89cfcc76ca3e7e7c2a5d6e4f5cd4329e7cdcd"
        ),
        "value_multiplier": 1,
        "repair_status": "inflated_1000x",
    },
}


def load_fund(path: Path) -> dict:
    with open(path) as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} is not a fund object")
    return payload


def _directory_open_flags() -> int:
    """Return the required no-follow directory capability flags, or fail closed."""

    try:
        return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    except AttributeError as error:
        raise ValueError("secure directory descriptors are unavailable") from error


def _path_identity(metadata: os.stat_result) -> tuple[int, int]:
    return (metadata.st_dev, metadata.st_ino)


def _entry_identity(metadata: os.stat_result) -> tuple[int, int, int]:
    return (metadata.st_dev, metadata.st_ino, stat.S_IFMT(metadata.st_mode))


def _raw_parent_matches(
    raw_parent: Path,
    frozen_parent: Path,
    parent_fd: int,
) -> bool:
    """Verify that the caller's path still reaches the descriptor we retained."""

    try:
        if raw_parent.resolve(strict=False) != frozen_parent:
            return False
        metadata = raw_parent.stat()
    except (OSError, RuntimeError):
        return False
    descriptor_metadata = os.fstat(parent_fd)
    return (
        stat.S_ISDIR(metadata.st_mode)
        and stat.S_ISDIR(descriptor_metadata.st_mode)
        and _path_identity(metadata) == _path_identity(descriptor_metadata)
    )


def _assert_parent_continuity(
    raw_parent: Path,
    frozen_parent: Path,
    parent_fd: int,
) -> None:
    if not _raw_parent_matches(raw_parent, frozen_parent, parent_fd):
        raise ValueError("parent directory continuity changed during atomic write")


def _open_or_create_verified_directory(
    parent_fd: int,
    name: str,
    label: str,
) -> int:
    """Open one child without following it, creating it privately if absent."""

    created = False
    metadata = _entry_stat(parent_fd, name)
    if metadata is None:
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            # A concurrent creator is safe only if its entry is still a real dir.
            pass
        metadata = _entry_stat(parent_fd, name)
    if metadata is None or stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"{label} must be a real directory: {name}")
    if created:
        # umask can make a just-created directory inaccessible even to us;
        # normalize only this fresh entry before opening its descriptor.
        os.chmod(name, 0o700, dir_fd=parent_fd, follow_symlinks=False)
        _assert_entry_identity(parent_fd, name, metadata, label)
    descriptor = _open_repair_dir(parent_fd, name, label)
    try:
        if _path_identity(metadata) != _path_identity(os.fstat(descriptor)):
            raise ValueError(f"{label} changed while opening: {name}")
        if created:
            os.fchmod(descriptor, 0o700)
        os.fsync(descriptor)
        if created:
            os.fsync(parent_fd)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _prepare_verified_parent(
    path: Path,
    frozen_path: Path | None = None,
) -> tuple[int, Path, Path]:
    """Freeze a path, then create/open its parent through directory descriptors."""

    raw_path = Path(path)
    frozen = raw_path.resolve(strict=False) if frozen_path is None else frozen_path
    raw_parent = raw_path.parent
    frozen_parent = frozen.parent
    parts = frozen_parent.parts
    if not frozen_parent.is_absolute() or not parts or parts[0] != os.sep:
        raise ValueError(f"atomic write path must be absolute: {raw_path}")
    descriptor = os.open(os.sep, _directory_open_flags())
    try:
        for component in parts[1:]:
            child = _open_or_create_verified_directory(
                descriptor,
                component,
                "atomic write parent",
            )
            os.close(descriptor)
            descriptor = child
        _assert_parent_continuity(raw_parent, frozen_parent, descriptor)
        return descriptor, frozen, raw_parent
    except BaseException:
        os.close(descriptor)
        raise


def _prepare_verified_directory(
    path: Path,
    frozen_path: Path | None = None,
) -> tuple[int, Path]:
    """Create/open a final directory only through its pinned parent descriptor."""

    parent_fd, frozen, raw_parent = _prepare_verified_parent(path, frozen_path)
    try:
        _assert_parent_continuity(raw_parent, frozen.parent, parent_fd)
        descriptor = _open_or_create_verified_directory(
            parent_fd,
            frozen.name,
            "funds directory",
        )
        try:
            _assert_parent_continuity(raw_parent, frozen.parent, parent_fd)
            raw_metadata = Path(path).stat()
            if _path_identity(raw_metadata) != _path_identity(os.fstat(descriptor)):
                raise ValueError("funds directory identity changed during repair")
            return descriptor, frozen
        except BaseException:
            os.close(descriptor)
            raise
    finally:
        os.close(parent_fd)


def atomic_write_json(path: Path, payload: dict) -> None:
    """Publish JSON through a pinned parent descriptor and fail closed on retargets."""

    parent_fd, frozen_path, raw_parent = _prepare_verified_parent(Path(path))
    try:
        def check_continuity() -> None:
            _assert_parent_continuity(raw_parent, frozen_path.parent, parent_fd)

        check_continuity()
        _atomic_write_json_at(
            parent_fd,
            frozen_path.name,
            payload,
            check_continuity,
        )
        check_continuity()
    finally:
        os.close(parent_fd)


def _repair_marker(
    *,
    phase: str,
    targets: list[str],
    present: list[str],
    before_sha256: dict[str, str],
    after_sha256: dict[str, str],
) -> dict:
    return {
        "after_sha256": dict(sorted(after_sha256.items())),
        "before_sha256": dict(sorted(before_sha256.items())),
        "contract_version": REPAIR_MARKER_VERSION,
        "phase": phase,
        "present": sorted(present),
        "targets": sorted(targets),
    }


def _directory_identity(path: Path) -> tuple[int, int]:
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"funds directory must be a real directory: {path}")
    return (metadata.st_dev, metadata.st_ino)


def _assert_funds_directory_continuity(
    funds_fd: int,
    expected_identity: tuple[int, int],
    frozen_funds: Path | None = None,
) -> None:
    descriptor_metadata = os.fstat(funds_fd)
    try:
        raw_metadata = FUNDS_DIR.stat()
        raw_frozen = FUNDS_DIR.resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise ValueError("funds directory identity changed during repair") from error
    if (
        not stat.S_ISDIR(descriptor_metadata.st_mode)
        or not stat.S_ISDIR(raw_metadata.st_mode)
        or _path_identity(descriptor_metadata) != expected_identity
        or _path_identity(raw_metadata) != expected_identity
        or (frozen_funds is not None and raw_frozen != frozen_funds)
    ):
        raise ValueError("funds directory identity changed during repair")


def _entry_stat(parent_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def _assert_entry_identity(
    parent_fd: int,
    name: str,
    expected: os.stat_result,
    label: str,
) -> None:
    current = _entry_stat(parent_fd, name)
    if current is None or _entry_identity(current) != _entry_identity(expected):
        raise ValueError(f"{label} changed while opening: {name}")


def _open_repair_dir(parent_fd: int, name: str, label: str) -> int:
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=parent_fd,
        )
    except OSError as error:
        raise ValueError(f"{label} must be a real directory: {name}") from error
    if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise ValueError(f"{label} must be a real directory: {name}")
    return descriptor


def _repair_hash_at(parent_fd: int, name: str, label: str) -> str:
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=parent_fd,
        )
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"{label} must be a regular file: {name}")
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as error:
        raise ValueError(f"{label} is not a readable regular file: {name}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _atomic_write_json_at(
    parent_fd: int,
    name: str,
    payload: dict,
    check_continuity: Callable[[], None] | None = None,
) -> None:
    """Write one private JSON entry through an already-verified parent fd."""

    temporary = f".{name}.{secrets.token_hex(16)}.tmp"
    descriptor = -1
    owned_identity: tuple[int, int, int] | None = None
    try:
        if check_continuity is not None:
            check_continuity()
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent_fd,
        )
        owned_identity = _entry_identity(os.fstat(descriptor))
        if owned_identity[2] != stat.S_IFREG:
            raise ValueError("repair temporary file must be a regular file")
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        if check_continuity is not None:
            check_continuity()
        metadata = _entry_stat(parent_fd, temporary)
        if metadata is None or _entry_identity(metadata) != owned_identity:
            raise ValueError("repair temporary file changed before publication")
        os.replace(
            temporary,
            name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        os.fsync(parent_fd)
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        metadata = _entry_stat(parent_fd, temporary)
        if metadata is not None and _entry_identity(metadata) == owned_identity:
            os.unlink(temporary, dir_fd=parent_fd)
            os.fsync(parent_fd)
        raise


def _remove_repair_tree_at(parent_fd: int, name: str) -> None:
    metadata = _entry_stat(parent_fd, name)
    if metadata is None:
        return
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"repair transaction directory must be a real directory: {name}")
    descriptor = _open_repair_dir(parent_fd, name, "repair transaction directory")
    try:
        if _entry_identity(os.fstat(descriptor)) != _entry_identity(metadata):
            raise ValueError(f"repair transaction directory changed while opening: {name}")
        for child in os.listdir(descriptor):
            child_metadata = _entry_stat(descriptor, child)
            if child_metadata is None:
                continue
            if stat.S_ISDIR(child_metadata.st_mode):
                _remove_repair_tree_at(descriptor, child)
            elif stat.S_ISREG(child_metadata.st_mode):
                _assert_entry_identity(
                    descriptor,
                    child,
                    child_metadata,
                    "repair transaction entry",
                )
                os.unlink(child, dir_fd=descriptor)
                os.fsync(descriptor)
            else:
                raise ValueError("repair transaction contains an unsafe entry")
    finally:
        os.close(descriptor)
    _assert_entry_identity(parent_fd, name, metadata, "repair transaction directory")
    os.rmdir(name, dir_fd=parent_fd)
    os.fsync(parent_fd)


def _remove_repair_file_at(parent_fd: int, name: str) -> None:
    metadata = _entry_stat(parent_fd, name)
    if metadata is None:
        return
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"repair target must be a regular file: {name}")
    _assert_entry_identity(parent_fd, name, metadata, "repair target")
    os.unlink(name, dir_fd=parent_fd)
    os.fsync(parent_fd)


def _validate_repair_marker(marker: object) -> dict:
    if not isinstance(marker, dict) or set(marker) != {
        "after_sha256", "before_sha256", "contract_version", "phase", "present", "targets",
    }:
        raise ValueError("repair transaction marker fields are invalid")
    targets = marker["targets"]
    present = marker["present"]
    before_sha256 = marker["before_sha256"]
    after_sha256 = marker["after_sha256"]
    if (
        marker["contract_version"] != REPAIR_MARKER_VERSION
        or marker["phase"] not in {"prepared", "published"}
        or not isinstance(targets, list)
        or not targets
        or any(not isinstance(name, str) or not name.endswith(".json") or not name[:-5].isdigit() for name in targets)
        or targets != sorted(targets)
        or len(targets) != len(set(targets))
        or not isinstance(present, list)
        or any(not isinstance(name, str) for name in present)
        or present != sorted(present)
        or len(present) != len(set(present))
        or not set(present).issubset(set(targets))
        or not isinstance(before_sha256, dict)
        or set(before_sha256) != set(present)
        or not isinstance(after_sha256, dict)
        or set(after_sha256) != set(targets)
        or any(not isinstance(digest, str) or len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest) for digest in [*before_sha256.values(), *after_sha256.values()])
    ):
        raise ValueError("repair transaction marker state is invalid")
    return marker


def _load_repair_marker_at(transaction_fd: int) -> dict:
    metadata = _entry_stat(transaction_fd, REPAIR_MARKER_NAME)
    if metadata is None or stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ValueError("repair transaction marker must be a regular file")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise ValueError("repair transaction marker must have mode 0600")
    descriptor = -1
    try:
        descriptor = os.open(
            REPAIR_MARKER_NAME,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=transaction_fd,
        )
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            marker = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("repair transaction marker is invalid") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return _validate_repair_marker(marker)


def _finish_repair_transaction_at(funds_fd: int) -> None:
    _remove_repair_tree_at(funds_fd, REPAIR_CLEANUP_NAME)
    os.replace(
        REPAIR_TRANSACTION_NAME,
        REPAIR_CLEANUP_NAME,
        src_dir_fd=funds_fd,
        dst_dir_fd=funds_fd,
    )
    os.fsync(funds_fd)
    _remove_repair_tree_at(funds_fd, REPAIR_CLEANUP_NAME)


def _recover_fund_updates_at(funds_fd: int) -> None:
    _remove_repair_tree_at(funds_fd, REPAIR_CLEANUP_NAME)
    _remove_repair_tree_at(funds_fd, REPAIR_PREPARE_NAME)
    transaction_metadata = _entry_stat(funds_fd, REPAIR_TRANSACTION_NAME)
    if transaction_metadata is None:
        return
    if stat.S_ISLNK(transaction_metadata.st_mode) or not stat.S_ISDIR(transaction_metadata.st_mode):
        raise ValueError("repair transaction must be a real directory")
    if stat.S_IMODE(transaction_metadata.st_mode) != 0o700:
        raise ValueError("repair transaction must have mode 0700")
    transaction_fd = _open_repair_dir(funds_fd, REPAIR_TRANSACTION_NAME, "repair transaction")
    backup_fd = staged_fd = -1
    try:
        marker = _load_repair_marker_at(transaction_fd)
        backup_fd = _open_repair_dir(transaction_fd, "backup", "repair backup directory")
        staged_fd = _open_repair_dir(transaction_fd, "staged", "repair staging directory")
        if marker["phase"] == "published" and all(
            _repair_hash_at(funds_fd, name, "published repair target") == marker["after_sha256"][name]
            for name in marker["targets"]
        ):
            # The exact requested generation is already durable; skip rollback
            # and retire its marker after the descriptors close below.
            marker["targets"] = []
        for name in marker["targets"]:
            backup_metadata = _entry_stat(backup_fd, name)
            target_metadata = _entry_stat(funds_fd, name)
            if backup_metadata is not None:
                if _repair_hash_at(backup_fd, name, "repair backup") != marker["before_sha256"].get(name):
                    raise ValueError("repair backup hash does not match marker")
                if target_metadata is not None:
                    target_sha256 = _repair_hash_at(funds_fd, name, "interrupted repair target")
                    if target_sha256 not in {marker["before_sha256"].get(name), marker["after_sha256"][name]}:
                        raise ValueError("interrupted repair target hash does not match marker")
                    _remove_repair_file_at(funds_fd, name)
                os.replace(name, name, src_dir_fd=backup_fd, dst_dir_fd=funds_fd)
                os.fsync(backup_fd)
                os.fsync(funds_fd)
            elif name in marker["present"]:
                if _repair_hash_at(funds_fd, name, "existing repair target") != marker["before_sha256"][name]:
                    raise ValueError("existing repair target hash does not match marker")
            elif target_metadata is not None:
                if _repair_hash_at(funds_fd, name, "interrupted new repair target") != marker["after_sha256"][name]:
                    raise ValueError("interrupted new repair target hash does not match marker")
                _remove_repair_file_at(funds_fd, name)
    finally:
        if staged_fd >= 0:
            os.close(staged_fd)
        if backup_fd >= 0:
            os.close(backup_fd)
        os.close(transaction_fd)
    _finish_repair_transaction_at(funds_fd)


def _create_private_repair_directory(
    parent_fd: int,
    name: str,
    label: str,
) -> int:
    os.mkdir(name, mode=0o700, dir_fd=parent_fd)
    metadata = _entry_stat(parent_fd, name)
    if metadata is None or stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"{label} must be a real directory: {name}")
    os.chmod(name, 0o700, dir_fd=parent_fd, follow_symlinks=False)
    _assert_entry_identity(parent_fd, name, metadata, label)
    descriptor = _open_repair_dir(parent_fd, name, label)
    try:
        if _entry_identity(os.fstat(descriptor)) != _entry_identity(metadata):
            raise ValueError(f"{label} changed while opening: {name}")
        os.fchmod(descriptor, 0o700)
        os.fsync(descriptor)
        os.fsync(parent_fd)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _create_repair_transaction_at(funds_fd: int, updates: dict[str, dict]) -> None:
    if _entry_stat(funds_fd, REPAIR_TRANSACTION_NAME) is not None:
        raise ValueError("an unrecovered repair transaction already exists")
    _remove_repair_tree_at(funds_fd, REPAIR_PREPARE_NAME)
    prepare_fd = staged_fd = backup_fd = -1
    try:
        prepare_fd = _create_private_repair_directory(
            funds_fd, REPAIR_PREPARE_NAME, "repair prepare directory"
        )
        staged_fd = _create_private_repair_directory(
            prepare_fd, "staged", "repair staging directory"
        )
        backup_fd = _create_private_repair_directory(
            prepare_fd, "backup", "repair backup directory"
        )
        targets = sorted(updates)
        before_sha256 = {
            name: _repair_hash_at(funds_fd, name, "existing repair target")
            for name in targets
            if _entry_stat(funds_fd, name) is not None
        }
        after_sha256: dict[str, str] = {}
        for name in targets:
            _atomic_write_json_at(staged_fd, name, updates[name])
            after_sha256[name] = _repair_hash_at(staged_fd, name, "staged repair target")
        _atomic_write_json_at(
            prepare_fd,
            REPAIR_MARKER_NAME,
            _repair_marker(
                phase="prepared", targets=targets, present=sorted(before_sha256),
                before_sha256=before_sha256, after_sha256=after_sha256,
            ),
        )
        os.fsync(staged_fd)
        os.fsync(backup_fd)
        os.fsync(prepare_fd)
        os.fsync(funds_fd)
    except BaseException:
        if backup_fd >= 0:
            os.close(backup_fd)
            backup_fd = -1
        if staged_fd >= 0:
            os.close(staged_fd)
            staged_fd = -1
        if prepare_fd >= 0:
            os.close(prepare_fd)
            prepare_fd = -1
        try:
            _remove_repair_tree_at(funds_fd, REPAIR_PREPARE_NAME)
        except (OSError, ValueError):
            pass
        raise
    finally:
        if backup_fd >= 0:
            os.close(backup_fd)
        if staged_fd >= 0:
            os.close(staged_fd)
        if prepare_fd >= 0:
            os.close(prepare_fd)
    os.replace(REPAIR_PREPARE_NAME, REPAIR_TRANSACTION_NAME, src_dir_fd=funds_fd, dst_dir_fd=funds_fd)
    os.fsync(funds_fd)


def _publish_fund_updates(updates: dict[Path, dict]) -> None:
    """Publish and recover repairs exclusively through a locked funds fd."""

    frozen_funds = FUNDS_DIR.resolve(strict=False)
    try:
        _directory_identity(FUNDS_DIR)
    except FileNotFoundError:
        # _prepare_verified_directory creates only below the frozen canonical
        # parent, and rejects a raw-path retarget before its first mkdir.
        pass
    normalized: dict[str, dict] = {}
    for path, payload in updates.items():
        candidate = Path(path)
        if (
            candidate.parent != FUNDS_DIR
            or not candidate.name.endswith(".json")
            or not candidate.stem.isdigit()
            or not isinstance(payload, dict)
            or candidate.name in normalized
        ):
            raise ValueError(f"invalid fund repair target: {candidate}")
        normalized[candidate.name] = payload
    funds_fd, frozen_funds = _prepare_verified_directory(
        FUNDS_DIR,
        frozen_funds,
    )
    expected_identity = _path_identity(os.fstat(funds_fd))
    raw_fd = -1
    try:
        # Re-open the raw spelling only to prove it still names the capability;
        # every mutation below remains relative to funds_fd.
        raw_fd = os.open(FUNDS_DIR, _directory_open_flags())
        if _path_identity(os.fstat(raw_fd)) != expected_identity:
            raise ValueError("funds directory identity changed during repair")
    finally:
        if raw_fd >= 0:
            os.close(raw_fd)
    try:
        _assert_funds_directory_continuity(
            funds_fd,
            expected_identity,
            frozen_funds,
        )
        fcntl.flock(funds_fd, fcntl.LOCK_EX)
        _assert_funds_directory_continuity(
            funds_fd,
            expected_identity,
            frozen_funds,
        )
        _recover_fund_updates_at(funds_fd)
        if not normalized:
            _assert_funds_directory_continuity(
                funds_fd,
                expected_identity,
                frozen_funds,
            )
            return
        try:
            _create_repair_transaction_at(funds_fd, normalized)
            transaction_fd = _open_repair_dir(funds_fd, REPAIR_TRANSACTION_NAME, "repair transaction")
            backup_fd = staged_fd = -1
            try:
                marker = _load_repair_marker_at(transaction_fd)
                backup_fd = _open_repair_dir(transaction_fd, "backup", "repair backup directory")
                staged_fd = _open_repair_dir(transaction_fd, "staged", "repair staging directory")
                for name in marker["present"]:
                    if _repair_hash_at(funds_fd, name, "existing repair target") != marker["before_sha256"][name]:
                        raise ValueError("existing repair target changed during transaction setup")
                    os.replace(name, name, src_dir_fd=funds_fd, dst_dir_fd=backup_fd)
                    os.fsync(funds_fd)
                    os.fsync(backup_fd)
                for name in marker["targets"]:
                    if _repair_hash_at(staged_fd, name, "staged repair target") != marker["after_sha256"][name]:
                        raise ValueError("staged repair target hash does not match marker")
                    os.replace(name, name, src_dir_fd=staged_fd, dst_dir_fd=funds_fd)
                    os.fsync(staged_fd)
                    os.fsync(funds_fd)
                    if _repair_hash_at(funds_fd, name, "published repair target") != marker["after_sha256"][name]:
                        raise ValueError("published repair target hash does not match marker")
                _atomic_write_json_at(
                    transaction_fd,
                    REPAIR_MARKER_NAME,
                    _repair_marker(
                        phase="published", targets=marker["targets"], present=marker["present"],
                        before_sha256=marker["before_sha256"], after_sha256=marker["after_sha256"],
                    ),
                )
            finally:
                if staged_fd >= 0:
                    os.close(staged_fd)
                if backup_fd >= 0:
                    os.close(backup_fd)
                os.close(transaction_fd)
            _finish_repair_transaction_at(funds_fd)
            _assert_funds_directory_continuity(
                funds_fd,
                expected_identity,
                frozen_funds,
            )
        except BaseException as error:
            recovery_error: BaseException | None = None
            try:
                _recover_fund_updates_at(funds_fd)
            except BaseException as rollback_error:
                recovery_error = rollback_error
            if recovery_error is not None and hasattr(error, "add_note"):
                error.add_note(f"fund repair recovery also failed: {recovery_error}")
            raise
    finally:
        try:
            fcntl.flock(funds_fd, fcntl.LOCK_UN)
        finally:
            os.close(funds_fd)


def holding_value_signature(holdings: list[dict]) -> str:
    """Hash the SEC identity, amount, and value fields affected by a repair."""
    rows: list[list[str | int | float]] = []
    for holding in holdings:
        if not isinstance(holding, dict):
            raise ValueError("repair quarter contains a non-object holding")
        value = holding.get("value")
        shares = holding.get("shares")
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or isinstance(shares, bool)
            or not isinstance(shares, (int, float))
        ):
            raise ValueError("repair quarter contains non-numeric value or shares")
        rows.append([
            str(holding.get("cusip") or "").strip().upper(),
            shares,
            value,
        ])
    rows.sort(key=lambda row: (str(row[0]), str(row[1]), str(row[2])))
    canonical = json.dumps(rows, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def explicit_repair_metadata(spec: dict) -> dict:
    evidence = {
        "migration": True,
        "repair_status": spec["repair_status"],
        "sec_accession": spec["accession"],
        "pre_repair_total": spec["bad_total"],
        "normalized_value_total": spec["correct_total"],
    }
    if spec["operation"] == "multiply_except_cusips":
        evidence["row_value_multipliers"] = {
            "default": spec["factor"],
            **{cusip: 1 for cusip in spec["unscaled_cusips"]},
        }
    if spec["value_multiplier"] is None:
        # A mixed filing has no truthful filing-wide multiplier. Keep its
        # audit trail separate from the uniform convention fields consumed by
        # future-quarter inference.
        return {
            "value_unit_repair": {
                "method": "sec_verified_historical_migration",
                "confidence": "high",
                "evidence": evidence,
            },
        }
    return {
        "value_unit_policy_version": VALUE_UNIT_POLICY_VERSION,
        "value_unit_method": "sec_verified_historical_migration",
        "value_unit_confidence": "high",
        "value_unit_evidence": evidence,
    }


def repair_explicit_historical_quarter(quarter: dict, spec: dict) -> bool:
    """Apply one exact SEC-backed repair, or no-op on its repaired form."""
    holdings = quarter.get("holdings")
    if not isinstance(holdings, list):
        raise ValueError("explicit repair quarter holdings are not a list")
    if len(holdings) != spec["holding_count"]:
        raise ValueError(
            "explicit repair holding count changed: "
            f"expected {spec['holding_count']}, found {len(holdings)}"
        )
    total = quarter.get("total_value")
    if total != sum(holding.get("value", 0) or 0 for holding in holdings):
        raise ValueError("explicit repair quarter total does not match holdings")

    signature = holding_value_signature(holdings)
    value_changed = False
    if total == spec["bad_total"]:
        if signature != spec["bad_signature"]:
            raise ValueError("explicit repair source row signature changed")
        unscaled_cusips = set(spec.get("unscaled_cusips", ()))
        for holding in holdings:
            cusip = str(holding.get("cusip") or "").strip().upper()
            if (
                spec["operation"] == "multiply_except_cusips"
                and cusip in unscaled_cusips
            ):
                continue
            value = holding["value"]
            if spec["operation"] == "divide_all":
                divisor = spec["divisor"]
                if not isinstance(value, int) or value % divisor != 0:
                    raise ValueError(
                        "explicit downscale value is not an exact multiple of 1,000"
                    )
                holding["value"] = value // divisor
            elif spec["factor"] == 1000:
                holding["value"] = value * 1000
            else:
                raise ValueError("unsupported explicit repair factor")
        quarter["total_value"] = sum(
            holding.get("value", 0) or 0 for holding in holdings
        )
        if (
            quarter["total_value"] != spec["correct_total"]
            or holding_value_signature(holdings) != spec["correct_signature"]
        ):
            raise ValueError("explicit repair did not produce the verified result")
        value_changed = True
    elif (
        total != spec["correct_total"]
        or signature != spec["correct_signature"]
    ):
        raise ValueError(
            "explicit repair quarter matches neither verified source nor result"
        )

    metadata = explicit_repair_metadata(spec)
    metadata_changed = any(
        quarter.get(key) != value for key, value in metadata.items()
    )
    quarter.update(metadata)
    multiplier = spec["value_multiplier"]
    if multiplier is None:
        for key in (
            "value_unit_policy_version",
            "value_multiplier",
            "value_unit_method",
            "value_unit_confidence",
            "value_unit_evidence",
        ):
            if key in quarter:
                del quarter[key]
                metadata_changed = True
    elif quarter.get("value_multiplier") != multiplier:
        quarter["value_multiplier"] = multiplier
        metadata_changed = True

    if value_changed and quarter.get("composition_version") in {1, 2}:
        quarter["composition_hash"] = validate_data.calculate_composition_hash(
            quarter
        )
    return value_changed or metadata_changed


def apply_explicit_historical_repairs() -> int:
    """Write the narrow manifest only after every target validates in memory."""
    funds: dict[Path, dict] = {}
    changed_paths: set[Path] = set()
    changed_quarters = 0
    for (cik, report_date), spec in EXPLICIT_HISTORICAL_REPAIRS.items():
        path = FUNDS_DIR / f"{cik}.json"
        fund = funds.setdefault(path, load_fund(path))
        matches = [
            quarter
            for quarter in fund.get("quarters", []) or []
            if (
                isinstance(quarter, dict)
                and quarter.get("report_date") == report_date
            )
        ]
        if len(matches) != 1:
            raise ValueError(
                f"{path} does not contain exactly one {report_date} quarter"
            )
        if repair_explicit_historical_quarter(matches[0], spec):
            changed_paths.add(path)
            changed_quarters += 1

    _publish_fund_updates({path: funds[path] for path in changed_paths})
    return changed_quarters


def build_peer_references() -> dict[tuple[str, str], tuple[float, int]]:
    prices: dict[
        tuple[str, str], dict[str, list[float]]
    ] = defaultdict(lambda: defaultdict(list))
    for path in sorted(FUNDS_DIR.glob("*.json")):
        fund = load_fund(path)
        cik = str(fund.get("cik") or path.stem)
        for quarter in fund.get("quarters", []) or []:
            if not isinstance(quarter, dict):
                continue
            report_date = str(quarter.get("report_date") or "")
            for holding in quarter.get("holdings", []) or []:
                if not isinstance(holding, dict) or not is_unit_evidence_holding(
                    holding
                ):
                    continue
                value = holding.get("value")
                shares = holding.get("shares")
                if (
                    report_date
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and value > 0
                    and isinstance(shares, (int, float))
                    and not isinstance(shares, bool)
                    and shares > 0
                ):
                    cusip = str(holding.get("cusip") or "").strip().upper()
                    if cusip:
                        prices[(report_date, cusip)][cik].append(value / shares)
    return {
        key: (
            statistics.median(
                statistics.median(filer_values)
                for filer_values in by_filer.values()
            ),
            len(by_filer),
        )
        for key, by_filer in prices.items()
        if len(by_filer) >= 4
    }


def find_candidates(
    references: dict[tuple[str, str], tuple[float, int]],
) -> list[dict]:
    candidates: list[dict] = []
    for path in sorted(FUNDS_DIR.glob("*.json")):
        fund = load_fund(path)
        cik = int(fund.get("cik") or path.stem)
        for quarter_index, quarter in enumerate(fund.get("quarters", []) or []):
            if not isinstance(quarter, dict):
                continue
            report_date = str(quarter.get("report_date") or "")
            holdings = quarter.get("holdings")
            if not report_date or not isinstance(holdings, list):
                continue
            if (cik, report_date) in EXPLICIT_HISTORICAL_REPAIRS:
                continue
            peer_prices = {
                str(holding.get("cusip") or "").strip().upper(): reference
                for holding in holdings
                if isinstance(holding, dict)
                and (
                    reference := references.get(
                        (
                            report_date,
                            str(holding.get("cusip") or "").strip().upper(),
                        )
                    )
                )
            }
            evidence = peer_scale_evidence(
                holdings,
                peer_prices,
                min_scale_positions=1,
            )
            if evidence["status"] is not None:
                candidates.append({
                    "path": path,
                    "cik": cik,
                    "name": fund.get("name"),
                    "quarter_index": quarter_index,
                    "report_date": report_date,
                    "evidence": evidence,
                })
    return candidates


def infer_source_multipliers(
    quarter: dict,
) -> list[tuple[dict, int]] | None:
    applied = [
        source
        for source in quarter.get("source_filings", []) or []
        if isinstance(source, dict) and source.get("applied") is True
    ]
    raw_totals = [source.get("reported_value_total") for source in applied]
    total_value = quarter.get("total_value")
    if (
        not applied
        or len(applied) > MAX_SOURCE_ASSIGNMENT_COMPONENTS
        or isinstance(total_value, bool)
        or not isinstance(total_value, int)
        or total_value < 0
    ):
        return None
    totals: list[int] = []
    for total in raw_totals:
        if isinstance(total, bool) or not isinstance(total, int) or total <= 0:
            return None
        totals.append(total)

    baseline = sum(totals)
    difference = total_value - baseline
    if difference < 0 or difference % 999:
        return None
    required_scaled_total = difference // 999
    if required_scaled_total > baseline:
        return None
    if required_scaled_total == 0:
        return [(source, 1) for source in applied]

    # Choosing multiplier 1000 instead of 1 adds exactly 999 * total.
    # Solve the resulting subset-sum with a hard cap and retain at most two
    # witnesses per subtotal so ambiguity remains fail-closed.
    states: dict[int, tuple[int, int]] = {0: (1, 0)}
    for index, total in enumerate(totals):
        next_states = dict(states)
        for subtotal, (count, mask) in states.items():
            candidate = subtotal + total
            if candidate > required_scaled_total:
                continue
            candidate_mask = mask | (1 << index)
            existing_count, existing_mask = next_states.get(candidate, (0, 0))
            combined_count = min(2, existing_count + count)
            next_states[candidate] = (
                combined_count,
                candidate_mask if existing_count == 0 else existing_mask,
            )
        if len(next_states) > MAX_SOURCE_ASSIGNMENT_STATES:
            return None
        states = next_states

    match_count, selected_mask = states.get(required_scaled_total, (0, 0))
    if match_count != 1:
        return None
    return [
        (source, 1000 if selected_mask & (1 << index) else 1)
        for index, source in enumerate(applied)
    ]


def backfill_unit_provenance(quarter: dict) -> None:
    """Record arithmetic source attribution without treating it as proof."""
    assignment = infer_source_multipliers(quarter)
    if assignment is None:
        return
    for source, multiplier in assignment:
        source["value_unit_policy_version"] = VALUE_UNIT_POLICY_VERSION
        source["value_multiplier"] = multiplier
        source["normalized_value_total"] = (
            source["reported_value_total"] * multiplier
        )
        source["value_unit_method"] = "arithmetic_only_migration"
        source["value_unit_confidence"] = "low"
        source["value_unit_evidence"] = {
            "migration": True,
            "independent_unit_proof": False,
        }


def repair_mixed_composition(quarter: dict) -> None:
    assignment = infer_source_multipliers(quarter)
    if assignment is None:
        raise ValueError("mixed composition has no unique current unit assignment")

    corrected = 0
    for source, multiplier in assignment:
        if multiplier != 1000:
            continue
        if source.get("reported_entry_total") != 1:
            raise ValueError(
                "mixed composition scaled source is not a one-row supplement"
            )
        scaled_total = source["reported_value_total"] * 1000
        matches = [
            holding
            for holding in quarter.get("holdings", [])
            if holding.get("value") == scaled_total
        ]
        if len(matches) != 1:
            raise ValueError(
                "mixed composition scaled source does not match one holding"
            )
        matches[0]["value"] = source["reported_value_total"]
        corrected += 1
    if corrected == 0:
        raise ValueError("mixed composition had no scaled component to repair")


def repair_candidate(quarter: dict, status: str) -> None:
    holdings = quarter.get("holdings")
    if not isinstance(holdings, list):
        raise ValueError("quarter holdings are not a list")

    applied = [
        source
        for source in quarter.get("source_filings", []) or []
        if isinstance(source, dict) and source.get("applied") is True
    ]
    if status == "inflated_1000x":
        if quarter.get("composition_version") in {1, 2} and len(applied) > 1:
            repair_mixed_composition(quarter)
        else:
            invalid = [
                holding.get("value")
                for holding in holdings
                if (
                    isinstance(holding.get("value"), bool)
                    or not isinstance(holding.get("value"), int)
                    or holding["value"] % 1000 != 0
                )
            ]
            if invalid:
                raise ValueError(
                    "inflated quarter contains values that are not exact "
                    "multiples of 1,000"
                )
            for holding in holdings:
                holding["value"] //= 1000
    elif status == "understated_1000x":
        if quarter.get("composition_version") in {1, 2} and len(applied) > 1:
            raise ValueError(
                "refusing to scale a mixed composition without row provenance"
            )
        for holding in holdings:
            value = holding.get("value")
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError("understated quarter contains a non-numeric value")
            holding["value"] = value * 1000
    else:
        raise ValueError(f"unsupported repair status {status!r}")

    quarter["total_value"] = sum(
        holding.get("value", 0) or 0 for holding in holdings
    )
    backfill_unit_provenance(quarter)
    if quarter.get("composition_version") in {1, 2}:
        quarter["composition_hash"] = validate_data.calculate_composition_hash(
            quarter
        )


def apply_candidates(candidates: list[dict]) -> int:
    funds: dict[Path, dict] = {}
    for candidate in candidates:
        path = candidate["path"]
        fund = funds.setdefault(path, load_fund(path))
        quarter = fund["quarters"][candidate["quarter_index"]]
        if quarter.get("report_date") != candidate["report_date"]:
            raise ValueError(f"{path} changed while planning repairs")
        repair_candidate(quarter, candidate["evidence"]["status"])

    _publish_fund_updates(funds)
    return len(candidates)


def backfill_known_repair_provenance() -> int:
    """Fill missing legacy metadata without promoting it to current proof."""
    changed_quarters = 0
    loaded_funds: dict[Path, dict] = {}
    changed_paths: set[Path] = set()
    for multiplier, funds in KNOWN_REPAIRS.items():
        repair_status = (
            "inflated_1000x" if multiplier == 1 else "understated_1000x"
        )
        for cik, report_dates in funds.items():
            path = FUNDS_DIR / f"{cik}.json"
            if path not in loaded_funds:
                loaded_funds[path] = load_fund(path)
            fund = loaded_funds[path]
            quarters = {
                quarter.get("report_date"): quarter
                for quarter in fund.get("quarters", []) or []
                if isinstance(quarter, dict)
            }
            changed = False
            for report_date in report_dates:
                quarter = quarters.get(report_date)
                if quarter is None:
                    raise ValueError(
                        f"{path} is missing known repair quarter {report_date}"
                    )
                applied_sources = [
                    source
                    for source in quarter.get("source_filings", []) or []
                    if isinstance(source, dict) and source.get("applied") is True
                ]
                if applied_sources or quarter.get(
                    "value_unit_policy_version"
                ) is not None:
                    # Current composed quarters already carry stronger
                    # per-source proof. Existing legacy metadata is retained as
                    # history but is no longer trusted by policy v2.
                    continue
                metadata = {
                    "value_unit_policy_version": (
                        LEGACY_VALUE_UNIT_POLICY_VERSION
                    ),
                    "value_multiplier": multiplier,
                    "value_unit_method": "legacy_peer_consensus_migration",
                    "value_unit_confidence": "low",
                    "value_unit_evidence": {
                        "migration": True,
                        "repair_status": repair_status,
                        "independent_unit_proof": False,
                    },
                }
                if any(
                    quarter.get(key) != value
                    for key, value in metadata.items()
                ):
                    quarter.update(metadata)
                    changed = True
                    changed_quarters += 1
            if changed:
                changed_paths.add(path)
    _publish_fund_updates({path: loaded_funds[path] for path in changed_paths})
    return changed_quarters


def retained_policy_inventory() -> Counter:
    """Count the current proof status of every retained fund quarter."""
    inventory: Counter = Counter()
    for path in sorted(FUNDS_DIR.glob("*.json")):
        fund = load_fund(path)
        for quarter in fund.get("quarters", []) or []:
            if not isinstance(quarter, dict):
                continue
            inventory["quarters"] += 1
            if isinstance(quarter.get("value_unit_repair"), dict):
                inventory["explicit_mixed_repairs"] += 1

            source_filings = quarter.get("source_filings")
            applied_sources = []
            if isinstance(source_filings, list):
                applied_sources = [
                    source
                    for source in source_filings
                    if (
                        isinstance(source, dict)
                        and source.get("applied") is True
                    )
                ]
            proof_records = applied_sources or [quarter]
            versions = {
                record.get("value_unit_policy_version")
                for record in proof_records
                if record.get("value_unit_policy_version") is not None
            }
            if (
                proof_records
                and all(
                    record.get("value_unit_policy_version")
                    == VALUE_UNIT_POLICY_VERSION
                    and record.get("value_unit_confidence") == "high"
                    for record in proof_records
                )
            ):
                inventory["current_high_confidence"] += 1
            elif versions:
                inventory["legacy_or_low_confidence"] += 1
            else:
                inventory["without_unit_provenance"] += 1
    return inventory


def audit_retained_value_unit_policy() -> tuple[Counter, list[str]]:
    """Run bounded peer and exact-adjacent checks over every retained quarter."""
    references = build_peer_references()
    candidates = find_candidates(references)
    errors: list[str] = []
    for candidate in candidates:
        evidence = candidate["evidence"]
        errors.append(
            f"{candidate['cik']} {candidate['report_date']} "
            f"{evidence['status']} "
            f"(coverage={evidence['matched_value_coverage']:.3f})"
        )

    for path in sorted(FUNDS_DIR.glob("*.json")):
        fund = load_fund(path)
        validate_data.validate_adjacent_quarter_value_units(
            fund,
            f"fund file {path.name}",
            errors,
        )
    return retained_policy_inventory(), errors


def mark_value_unit_migration_complete() -> None:
    """Persist the v2 cutover only after the complete retained-corpus audit."""
    state = load_fund(STATE_PATH)
    state["value_unit_migration_version"] = VALUE_UNIT_POLICY_VERSION
    atomic_write_json(STATE_PATH, state)


def migrate_value_unit_policy() -> tuple[Counter, int]:
    """Apply exact manifests, audit all retained quarters, then cut over."""
    explicit = apply_explicit_historical_repairs()
    inventory, errors = audit_retained_value_unit_policy()
    if errors:
        examples = "\n".join(f"  - {error}" for error in errors[:20])
        raise ValueError(
            f"value-unit policy migration found {len(errors)} anomaly(s):\n"
            f"{examples}"
        )
    mark_value_unit_migration_complete()
    return inventory, explicit


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write exact verified repairs; default is audit-only",
    )
    parser.add_argument(
        "--migrate-policy",
        action="store_true",
        help="run the bounded retained-corpus audit and mark policy v2 complete",
    )
    args = parser.parse_args()

    if args.apply and args.migrate_policy:
        parser.error("--apply and --migrate-policy are mutually exclusive")
    if args.migrate_policy:
        try:
            inventory, explicit = migrate_value_unit_policy()
        except (OSError, ValueError) as exc:
            print(f"Value-unit policy migration failed: {exc}", file=sys.stderr)
            return 1
        print(
            "Value-unit policy migration complete: "
            f"{inventory['quarters']} retained quarters audited, "
            f"{inventory['current_high_confidence']} current high-confidence, "
            f"{inventory['legacy_or_low_confidence']} legacy/low-confidence, "
            f"{inventory['without_unit_provenance']} without provenance, "
            f"{inventory['explicit_mixed_repairs']} explicit mixed repairs"
        )
        print(f"Applied explicit repairs/provenance to {explicit} quarter(s)")
        return 0

    references = build_peer_references()
    candidates = find_candidates(references)
    for candidate in candidates:
        evidence = candidate["evidence"]
        if (
            evidence["status"] == "mixed_scale_clusters"
            and evidence["intrinsic_count_value_conflict"]
            and evidence["matched_count_coverage"] < 0.8
        ):
            support = (
                "intrinsic-low-price="
                f"{evidence['low_price_count_support']:.3f}/"
                f"{evidence['low_price_value_support']:.3f}, "
                f"peer-count={evidence['matched_count_coverage']:.3f}"
            )
        elif evidence["status"] == "mixed_scale_clusters":
            support = (
                "mixed="
                f"aligned {evidence['aligned_count_support']:.3f}/"
                f"{evidence['aligned_value_support']:.3f}, "
                f"inflated {evidence['inflated_count_support']:.3f}/"
                f"{evidence['inflated_value_support']:.3f}, "
                f"understated {evidence['understated_count_support']:.3f}/"
                f"{evidence['understated_value_support']:.3f}"
            )
        else:
            support = (
                f"inflated={evidence['inflated_value_support']:.3f}\t"
                f"understated={evidence['understated_value_support']:.3f}"
            )
        print(
            f"{candidate['cik']}\t{candidate['report_date']}\t"
            f"{evidence['status']}\t"
            f"coverage={evidence['matched_value_coverage']:.3f}\t"
            f"{support}\t"
            f"{candidate['name']}"
        )
    print(f"{len(candidates)} value-unit anomaly candidate quarter(s)")

    if not args.apply:
        return 0
    repaired = apply_candidates(candidates)
    explicit = apply_explicit_historical_repairs()
    backfilled = backfill_known_repair_provenance()
    print(f"Repaired {repaired} quarter(s)")
    print(f"Applied explicit repairs/provenance to {explicit} quarter(s)")
    print(f"Backfilled provenance for {backfilled} repaired quarter(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
