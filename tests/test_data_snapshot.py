from __future__ import annotations

import contextlib
import gzip
import hashlib
import io
import json
import os
import shutil
import stat
import tarfile
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from scripts import data_snapshot


SOURCE_SHA = "a" * 40
CREATED_AT = "2026-08-05T16:00:00Z"
INSIDER_ACCESSION = "0000000001-26-000001"
PRIVATE_INSIDER_FILES = {
    Path(
        f"data/insiders/private/accessions/{INSIDER_ACCESSION}/index.html"
    ): b"<html>PRIVATE_SENTINEL_INDEX</html>\n",
    Path(
        f"data/insiders/private/accessions/{INSIDER_ACCESSION}/source-metadata.json"
    ): b'{"private":"PRIVATE_SENTINEL_SOURCE_METADATA"}\n',
    Path(
        f"data/insiders/private/accessions/{INSIDER_ACCESSION}/raw.xml"
    ): b"<ownershipDocument>PRIVATE_SENTINEL_RAW_XML</ownershipDocument>\n",
    Path(
        f"data/insiders/private/accessions/{INSIDER_ACCESSION}/normalized/1.0.0.json"
    ): b'{"private":"PRIVATE_SENTINEL_NORMALIZED_V1"}\n',
    Path(
        f"data/insiders/private/accessions/{INSIDER_ACCESSION}/normalized/2.0.0.json"
    ): b'{"private":"PRIVATE_SENTINEL_NORMALIZED_V2"}\n',
    Path("data/insiders/private/state/incremental-v1.json"): b'{"private":"PRIVATE_SENTINEL_INCREMENTAL"}\n',
    Path("data/insiders/private/state/backfill/2026Q1.json"): b'{"private":"PRIVATE_SENTINEL_BACKFILL"}\n',
    Path("data/insiders/private/state/reparse-v1.json"): b'{"private":"PRIVATE_SENTINEL_REPARSE"}\n',
    Path("data/insiders/private/state/issuers/0000000001.json"): b'{"private":"PRIVATE_SENTINEL_ISSUER"}\n',
    Path(
        f"data/insiders/private/state/quarantine/accessions/{INSIDER_ACCESSION}.json"
    ): b'{"private":"PRIVATE_SENTINEL_ACCESSION_QUARANTINE"}\n',
    Path("data/insiders/private/state/quarantine/quarters/2026Q1.json"): b'{"private":"PRIVATE_SENTINEL_QUARTER_QUARANTINE"}\n',
    Path("data/insiders/private/state/telemetry-v1.json"): b'{"private":"PRIVATE_SENTINEL_TELEMETRY"}\n',
}
PRIVATE_INSIDER_SENTINELS = tuple(PRIVATE_INSIDER_FILES.values())
QUARTERLY_ZIP_CACHE = Path(".cache/insider-quarterly/2026q1.zip")


class DataSnapshotTests(unittest.TestCase):
    def make_source(self, parent: Path, name: str = "source") -> Path:
        source = parent / name
        (source / "data/funds").mkdir(parents=True)
        (source / "data/stocks").mkdir(parents=True)
        (source / "data/empty").mkdir(parents=True)
        (source / ".cache").mkdir()
        (source / "data/funds/1.json").write_text(
            '{"cik":1,"holdings":["ABC"]}\n',
            encoding="utf-8",
        )
        (source / "data/stocks/ABC.json").write_text(
            '{"ticker":"ABC","funds":[1]}\n',
            encoding="utf-8",
        )
        (source / "data/pipeline_state.json").write_text(
            '{"cursor":"current"}\n',
            encoding="utf-8",
        )
        for relative, payload in PRIVATE_INSIDER_FILES.items():
            target = source / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        for index, relative in enumerate(data_snapshot.CACHE_FILES):
            (source / relative).write_text(
                json.dumps({"cache": index}, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        (source / ".cache/local-only.txt").write_text(
            "must not be archived\n",
            encoding="utf-8",
        )
        quarterly_zip = source / QUARTERLY_ZIP_CACHE
        quarterly_zip.parent.mkdir(parents=True)
        quarterly_zip.write_bytes(b"PRIVATE_SENTINEL_QUARTERLY_ZIP")
        return source

    def pack(self, source: Path, output: Path) -> dict:
        return data_snapshot.pack_snapshot(
            root=source,
            output_dir=output,
            source_sha=SOURCE_SHA,
            max_archive_bytes=1_000_000,
            created_at=CREATED_AT,
        )

    def load_manifest(self, summary: dict) -> dict:
        return json.loads(Path(summary["manifest_path"]).read_text())

    def test_pack_is_deterministic_and_uses_raw_digest_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            first = self.pack(source, root / "first")
            second = self.pack(source, root / "second")

            self.assertRegex(first["dataset_id"], r"^[0-9a-f]{64}$")
            self.assertEqual(first["dataset_sha256"], first["dataset_id"])
            self.assertEqual(first["dataset_id"], second["dataset_id"])
            self.assertEqual(
                Path(first["archive_path"]).read_bytes(),
                Path(second["archive_path"]).read_bytes(),
            )
            self.assertEqual(
                Path(first["manifest_path"]).read_bytes(),
                Path(second["manifest_path"]).read_bytes(),
            )
            manifest = self.load_manifest(first)
            self.assertEqual(
                data_snapshot.CONTRACT_VERSION,
                manifest["contract_version"],
            )
            self.assertEqual(CREATED_AT, manifest["created_at"])
            self.assertEqual(SOURCE_SHA, manifest["source_sha"])
            self.assertEqual(first["archive_sha256"], manifest["archive"]["sha256"])
            self.assertEqual(first["archive_bytes"], manifest["archive"]["bytes"])
            self.assertEqual(first["file_count"], manifest["dataset"]["file_count"])

    def test_pack_reports_bounded_insider_growth_without_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)

            summary = self.pack(source, root / "output")

            self.assertEqual(
                len(PRIVATE_INSIDER_FILES),
                summary["insider_file_count"],
            )
            self.assertEqual(
                sum(len(payload) for payload in PRIVATE_INSIDER_FILES.values()),
                summary["insider_content_bytes"],
            )
            rendered = json.dumps(summary, sort_keys=True).encode("utf-8")
            for sentinel in PRIVATE_INSIDER_SENTINELS:
                self.assertNotIn(sentinel, rendered)

    def test_verify_round_trip_extracts_full_data_and_allowlisted_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            summary = self.pack(source, root / "output")
            extracted = root / "extracted"

            verified = data_snapshot.verify_snapshot(
                archive_path=Path(summary["archive_path"]),
                manifest_path=Path(summary["manifest_path"]),
                max_archive_bytes=1_000_000,
                extract_root=extracted,
            )

            self.assertTrue(verified["verified"])
            self.assertEqual(summary["dataset_id"], verified["dataset_id"])
            self.assertEqual(
                (source / "data/funds/1.json").read_bytes(),
                (extracted / "data/funds/1.json").read_bytes(),
            )
            self.assertTrue((extracted / "data/empty").is_dir())
            for relative, expected in PRIVATE_INSIDER_FILES.items():
                restored = (extracted / relative).read_bytes()
                self.assertEqual(expected, restored, relative.as_posix())
                self.assertEqual(
                    hashlib.sha256((source / relative).read_bytes()).hexdigest(),
                    hashlib.sha256(restored).hexdigest(),
                    relative.as_posix(),
                )
            for relative in data_snapshot.CACHE_FILES:
                self.assertEqual(
                    (source / relative).read_bytes(),
                    (extracted / relative).read_bytes(),
                )
            self.assertFalse((extracted / ".cache/local-only.txt").exists())
            self.assertTrue((source / QUARTERLY_ZIP_CACHE).is_file())
            self.assertFalse((extracted / QUARTERLY_ZIP_CACHE).exists())
            self.assertEqual([], list(extracted.rglob("*.zip")))

    def test_private_insider_snapshot_uses_restricted_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            summary = self.pack(source, root / "output")
            extracted = root / "extracted"

            self.assertEqual(
                0o600,
                Path(summary["archive_path"]).stat().st_mode & 0o777,
            )

            data_snapshot.verify_snapshot(
                archive_path=Path(summary["archive_path"]),
                manifest_path=Path(summary["manifest_path"]),
                max_archive_bytes=1_000_000,
                extract_root=extracted,
            )

            private_root = extracted / "data/insiders/private"
            self.assertEqual(0o700, private_root.stat().st_mode & 0o777)
            for path in private_root.rglob("*"):
                expected_mode = 0o700 if path.is_dir() else 0o600
                self.assertEqual(
                    expected_mode,
                    path.stat().st_mode & 0o777,
                    path.relative_to(extracted).as_posix(),
                )

    def test_verify_rejects_non_owner_only_private_insider_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            summary = self.pack(source, root / "output")
            archive = Path(summary["archive_path"])
            archive.chmod(0o644)
            destination = root / "must-not-exist"

            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "archive mode must be exactly 0600",
            ):
                data_snapshot.verify_snapshot(
                    archive_path=archive,
                    manifest_path=Path(summary["manifest_path"]),
                    max_archive_bytes=1_000_000,
                    extract_root=destination,
                )
            self.assertFalse(destination.exists())

    def test_pack_rejects_missing_cache_and_source_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            missing_source = self.make_source(root, "missing")
            (missing_source / data_snapshot.CACHE_FILES[0]).unlink()
            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "required cache file is missing",
            ):
                self.pack(missing_source, root / "missing-output")

            linked_source = self.make_source(root, "linked")
            (linked_source / "data/link").symlink_to(
                linked_source / "data/funds/1.json"
            )
            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "unsupported entry",
            ):
                self.pack(linked_source, root / "linked-output")

    def test_pack_and_verify_fail_closed_at_size_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            failed_output = root / "failed"
            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "archive is too large",
            ):
                data_snapshot.pack_snapshot(
                    root=source,
                    output_dir=failed_output,
                    source_sha=SOURCE_SHA,
                    max_archive_bytes=1,
                    created_at=CREATED_AT,
                )
            self.assertEqual([], list(failed_output.iterdir()))

            summary = self.pack(source, root / "valid")
            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "archive is too large",
            ):
                data_snapshot.verify_snapshot(
                    archive_path=Path(summary["archive_path"]),
                    manifest_path=Path(summary["manifest_path"]),
                    max_archive_bytes=summary["archive_bytes"] - 1,
                )

    def test_pack_retry_recovers_archive_only_interruption(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            real_replace = os.replace

            def interrupt_manifest_publish(source_path, target_path, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
                if Path(str(target_path)).suffix == ".json":
                    raise KeyboardInterrupt("simulated interruption")
                real_replace(source_path, target_path, *args, **kwargs)

            with mock.patch.object(
                data_snapshot.os,
                "replace",
                side_effect=interrupt_manifest_publish,
            ):
                with self.assertRaisesRegex(KeyboardInterrupt, "simulated interruption"):
                    self.pack(source, output)

            archive_name, manifest_name = data_snapshot._manifest_names(
                data_snapshot._source_content_digest(data_snapshot._scan_source(source))[0]
            )
            self.assertTrue((output / archive_name).is_file())
            self.assertFalse((output / manifest_name).exists())
            self.assertEqual([], list(output.glob(".data-snapshot-*")))

            summary = self.pack(source, output)

            self.assertEqual(archive_name, Path(summary["archive_path"]).name)
            self.assertEqual(manifest_name, Path(summary["manifest_path"]).name)
            self.assertTrue(
                data_snapshot.verify_snapshot(
                    archive_path=Path(summary["archive_path"]),
                    manifest_path=Path(summary["manifest_path"]),
                    max_archive_bytes=1_000_000,
                )["verified"]
            )
            self.assertEqual([], list(output.glob(".data-snapshot-*")))

    def test_pack_fsyncs_staged_files_and_parent_after_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            observed_modes: list[int] = []

            def record_fsync(file_descriptor: int) -> None:
                observed_modes.append(os.fstat(file_descriptor).st_mode)

            with mock.patch.object(data_snapshot.os, "fsync", side_effect=record_fsync):
                self.pack(source, root / "output")

            self.assertGreaterEqual(
                sum(stat.S_ISREG(mode) for mode in observed_modes),
                2,
            )
            self.assertGreaterEqual(
                sum(stat.S_ISDIR(mode) for mode in observed_modes),
                2,
            )

    def test_pack_durably_creates_nested_output_directory_before_publication(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "new-parent" / "output"
            events: list[tuple[str, object]] = []
            real_fsync_directory_at = data_snapshot._fsync_directory_at
            real_replace = os.replace

            def record_directory_fsync(descriptor: int, label: str) -> None:
                events.append(
                    (
                        "fsync",
                        (os.fstat(descriptor).st_dev, os.fstat(descriptor).st_ino),
                    )
                )
                real_fsync_directory_at(descriptor, label)

            def record_replace(source_path, target_path, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
                events.append(("replace", Path(target_path)))
                real_replace(source_path, target_path, *args, **kwargs)

            with mock.patch.object(
                data_snapshot,
                "_fsync_directory_at",
                side_effect=record_directory_fsync,
            ), mock.patch.object(
                data_snapshot.os,
                "replace",
                side_effect=record_replace,
            ):
                self.pack(source, output)

            first_replace = next(
                index for index, event in enumerate(events) if event[0] == "replace"
            )
            fsynced_before_publication = {
                identity for event, identity in events[:first_replace] if event == "fsync"
            }
            self.assertIn(
                (output.parent.stat().st_dev, output.parent.stat().st_ino),
                fsynced_before_publication,
            )
            self.assertIn(
                (output.parent.parent.stat().st_dev, output.parent.parent.stat().st_ino),
                fsynced_before_publication,
            )

    def test_pack_fsyncs_existing_output_parent_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            output.mkdir()
            events: list[tuple[str, Path]] = []
            real_fsync_directory = data_snapshot._fsync_directory
            real_replace = os.replace

            def record_directory_fsync(path: Path, label: str) -> None:
                events.append(("fsync", Path(path)))
                real_fsync_directory(path, label)

            def record_replace(source_path, target_path, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
                events.append(("replace", Path(target_path)))
                real_replace(source_path, target_path, *args, **kwargs)

            with mock.patch.object(
                data_snapshot,
                "_fsync_directory",
                side_effect=record_directory_fsync,
            ), mock.patch.object(
                data_snapshot.os,
                "replace",
                side_effect=record_replace,
            ):
                self.pack(source, output)

            first_replace = next(
                index for index, event in enumerate(events) if event[0] == "replace"
            )
            self.assertIn(("fsync", output.parent.resolve()), events[:first_replace])

    def test_pack_allows_real_directory_symlink_only_in_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            real_parent = root / "real-parent"
            real_parent.mkdir()
            alias_parent = root / "alias-parent"
            alias_parent.symlink_to(real_parent, target_is_directory=True)
            output = alias_parent / "output"

            summary = self.pack(source, output)

            self.assertEqual(output.absolute(), Path(summary["archive_path"]).parent)
            self.assertTrue(Path(summary["archive_path"]).is_file())
            self.assertTrue(alias_parent.is_symlink())

    def test_pack_rejects_symlink_ancestor_into_snapshot_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            alias_parent = root / "alias-data"
            alias_parent.symlink_to(source / "data", target_is_directory=True)
            output = alias_parent / "snapshot-output"

            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "output directory cannot be inside snapshot data",
            ):
                self.pack(source, output)

            self.assertFalse((source / "data/snapshot-output").exists())

    def _assert_missing_output_retarget_does_not_create_in(
        self,
        protected_relative: Path,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            protected_parent = source / protected_relative
            safe_parent = root / "safe-parent"
            safe_parent.mkdir()
            alias_parent = root / "alias-parent"
            alias_parent.symlink_to(safe_parent, target_is_directory=True)
            output = alias_parent / "snapshot-output"
            source_before = {
                path.relative_to(source): path.read_bytes()
                for path in source.rglob("*")
                if path.is_file()
            }
            original_mkdir = os.mkdir
            retargeted = False

            def retarget_immediately_before_creation(
                path,
                mode=0o777,
                *,
                dir_fd=None,
            ):  # type: ignore[no-untyped-def]
                nonlocal retargeted
                if not retargeted and (
                    (dir_fd is None and Path(path) == output)
                    or (dir_fd is not None and path == "snapshot-output")
                ):
                    retargeted = True
                    alias_parent.unlink()
                    alias_parent.symlink_to(protected_parent, target_is_directory=True)
                return original_mkdir(path, mode, dir_fd=dir_fd)

            with mock.patch.object(
                data_snapshot,
                "_directory_fd_capabilities_available",
                return_value=True,
            ), mock.patch.object(
                data_snapshot.os,
                "mkdir",
                side_effect=retarget_immediately_before_creation,
            ):
                with self.assertRaisesRegex(
                    data_snapshot.SnapshotError,
                    "output directory cannot be inside snapshot data",
                ):
                    self.pack(source, output)

            self.assertTrue(retargeted)
            self.assertFalse((protected_parent / "snapshot-output").exists())
            self.assertEqual(
                [],
                list(protected_parent.rglob(f"{data_snapshot.ARCHIVE_PREFIX}*")),
            )
            self.assertEqual([], list(protected_parent.rglob(".data-snapshot-*")))
            source_after = {
                path.relative_to(source): path.read_bytes()
                for path in source.rglob("*")
                if path.is_file()
            }
            self.assertEqual(source_before, source_after)

    def test_pack_rejects_missing_output_ancestor_retarget_to_data_before_creation(
        self,
    ) -> None:
        self._assert_missing_output_retarget_does_not_create_in(Path("data"))

    def test_pack_rejects_missing_output_ancestor_retarget_to_cache_before_creation(
        self,
    ) -> None:
        self._assert_missing_output_retarget_does_not_create_in(Path(".cache"))

    def test_pack_rejects_missing_output_ancestor_retarget_to_real_parent_before_creation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            intended_parent = root / "intended-parent"
            replacement_parent = root / "replacement-parent"
            intended_parent.mkdir()
            replacement_parent.mkdir()
            alias_parent = root / "alias-parent"
            alias_parent.symlink_to(intended_parent, target_is_directory=True)
            output = alias_parent / "snapshot-output"
            original_mkdir = os.mkdir
            retargeted = False

            def retarget_immediately_before_creation(
                path,
                mode=0o777,
                *,
                dir_fd=None,
            ):  # type: ignore[no-untyped-def]
                nonlocal retargeted
                if not retargeted and (
                    (dir_fd is None and Path(path) == output)
                    or (dir_fd is not None and path == "snapshot-output")
                ):
                    retargeted = True
                    alias_parent.unlink()
                    alias_parent.symlink_to(
                        replacement_parent,
                        target_is_directory=True,
                    )
                return original_mkdir(path, mode, dir_fd=dir_fd)

            with mock.patch.object(
                data_snapshot,
                "_directory_fd_capabilities_available",
                return_value=True,
            ), mock.patch.object(
                data_snapshot.os,
                "mkdir",
                side_effect=retarget_immediately_before_creation,
            ):
                with self.assertRaisesRegex(
                    data_snapshot.SnapshotError,
                    "output directory changed",
                ):
                    self.pack(source, output)

            self.assertTrue(retargeted)
            self.assertTrue((intended_parent / "snapshot-output").is_dir())
            self.assertFalse((replacement_parent / "snapshot-output").exists())
            for parent in (intended_parent, replacement_parent):
                self.assertEqual(
                    [],
                    list(parent.rglob(f"{data_snapshot.ARCHIVE_PREFIX}*")),
                )
                self.assertEqual([], list(parent.rglob(".data-snapshot-*")))

    def test_pack_rejects_final_output_symlink_inserted_before_resolution(
        self,
    ) -> None:
        """A newly inserted final symlink must not create its target directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            safe_parent = root / "safe-parent"
            replacement_parent = root / "replacement-parent"
            safe_parent.mkdir()
            replacement_parent.mkdir()
            output = safe_parent / "output"
            redirected_output = replacement_parent / "redirected-output"
            sentinel = b"replacement sentinel"
            (replacement_parent / "sentinel").write_bytes(sentinel)
            real_resolve = Path.resolve
            real_write_archive = data_snapshot._write_archive
            injected = False

            def insert_final_symlink_before_resolution(
                path: Path,
                strict: bool = False,
            ) -> Path:
                nonlocal injected
                if not injected and path in {output, safe_parent}:
                    injected = True
                    output.symlink_to(redirected_output, target_is_directory=True)
                return real_resolve(path, strict=strict)

            with mock.patch.object(
                Path,
                "resolve",
                new=insert_final_symlink_before_resolution,
            ), mock.patch.object(
                data_snapshot,
                "_write_archive",
                wraps=real_write_archive,
            ) as write_archive:
                with self.assertRaises(data_snapshot.SnapshotError):
                    self.pack(source, output)

            self.assertTrue(injected)
            self.assertTrue(output.is_symlink())
            write_archive.assert_not_called()
            self.assertFalse(redirected_output.exists())
            self.assertEqual(sentinel, (replacement_parent / "sentinel").read_bytes())
            self.assertEqual(
                [],
                list(replacement_parent.rglob(f"{data_snapshot.ARCHIVE_PREFIX}*")),
            )
            self.assertEqual([], list(replacement_parent.rglob(".data-snapshot-*")))

    def test_pack_rejects_symlink_at_final_output_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            real_output = root / "real-output"
            real_output.mkdir()
            output_alias = root / "output-alias"
            output_alias.symlink_to(real_output, target_is_directory=True)

            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "output directory must be a real directory",
            ):
                self.pack(source, output_alias)

            self.assertEqual([], list(real_output.iterdir()))
            self.assertTrue(output_alias.is_symlink())

    def test_pack_rejects_case_alias_inside_snapshot_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            case_alias = source / "DATA"
            if not case_alias.exists() or not os.path.samefile(
                case_alias,
                source / "data",
            ):
                self.skipTest("filesystem is case-sensitive")
            output = case_alias / "snapshot-output"

            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "output directory cannot be inside snapshot data",
            ):
                self.pack(source, output)

            self.assertFalse((source / "data/snapshot-output").exists())

    def test_pack_verification_failure_durably_removes_published_pair(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            events: list[tuple[str, Path]] = []
            real_fsync_directory = data_snapshot._fsync_directory_at

            def fail_verification(**_kwargs) -> None:  # type: ignore[no-untyped-def]
                events.append(("verify", output))
                raise data_snapshot.SnapshotError("simulated verification failure")

            def record_directory_fsync(_descriptor: int, label: str) -> None:
                events.append(("fsync", output))
                real_fsync_directory(_descriptor, label)

            with mock.patch.object(
                data_snapshot,
                "_verify_snapshot_at",
                side_effect=fail_verification,
            ), mock.patch.object(
                data_snapshot,
                "_fsync_directory_at",
                side_effect=record_directory_fsync,
            ):
                with self.assertRaisesRegex(
                    data_snapshot.SnapshotError,
                    "simulated verification failure",
                ):
                    self.pack(source, output)

            self.assertEqual([], list(output.iterdir()))
            verification = next(
                index for index, event in enumerate(events) if event[0] == "verify"
            )
            self.assertIn(("fsync", output), events[verification + 1 :])

    def test_pack_rebuilds_regular_manifest_only_pair(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            complete = self.pack(source, root / "complete")
            output = root / "output"
            output.mkdir()
            manifest = Path(complete["manifest_path"])
            incomplete_manifest = output / manifest.name
            shutil.copyfile(manifest, incomplete_manifest)

            rebuilt = self.pack(source, output)

            self.assertTrue(Path(rebuilt["archive_path"]).is_file())
            self.assertTrue(Path(rebuilt["manifest_path"]).is_file())
            self.assertEqual([], list(output.glob(".data-snapshot-*")))

    def test_pack_adopts_exact_completed_pair_on_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            first = self.pack(source, root / "output")

            second = self.pack(source, root / "output")

            self.assertEqual(first, second)

    def test_pack_rejects_real_output_directory_replacement_before_open(
        self,
    ) -> None:
        """The durable prepared directory must survive the scan-to-open window."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            output.mkdir()
            displaced_output = root / "displaced-output"
            replacement = root / "replacement"
            replacement.mkdir()
            original_sentinel = b"original output sentinel"
            replacement_sentinel = b"replacement output sentinel"
            (output / "sentinel").write_bytes(original_sentinel)
            (replacement / "sentinel").write_bytes(replacement_sentinel)
            prepared_output = output.resolve(strict=True)
            dataset_sha256 = data_snapshot._source_content_digest(
                data_snapshot._scan_source(source)
            )[0]
            archive_name, manifest_name = data_snapshot._manifest_names(dataset_sha256)
            real_open_output = data_snapshot._open_verified_output_directory
            real_write_archive = data_snapshot._write_archive
            injected = False
            opened_paths: list[Path] = []

            def replace_real_directory_before_open(path: Path, label: str) -> int:
                nonlocal injected
                self.assertEqual("output directory", label)
                self.assertFalse(injected)
                injected = True
                opened_paths.append(path)
                output.rename(displaced_output)
                replacement.rename(output)
                return real_open_output(path, label)

            with mock.patch.object(
                data_snapshot,
                "_open_verified_output_directory",
                side_effect=replace_real_directory_before_open,
            ), mock.patch.object(
                data_snapshot,
                "_write_archive",
                wraps=real_write_archive,
            ) as write_archive:
                with self.assertRaisesRegex(
                    data_snapshot.SnapshotError,
                    "output directory changed",
                ):
                    self.pack(source, output)

            self.assertTrue(injected)
            self.assertEqual([prepared_output], opened_paths)
            write_archive.assert_not_called()
            self.assertEqual(original_sentinel, (displaced_output / "sentinel").read_bytes())
            self.assertEqual(replacement_sentinel, (output / "sentinel").read_bytes())
            for directory in (displaced_output, output):
                self.assertFalse((directory / archive_name).exists())
                self.assertFalse((directory / manifest_name).exists())
                self.assertEqual([], list(directory.glob(".data-snapshot-*")))

    def test_pack_rejects_ancestor_alias_replacement_before_open(self) -> None:
        """A raw ancestor alias change must not redirect descriptor publication."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            original_parent = root / "original-parent"
            replacement_parent = root / "replacement-parent"
            original_output = original_parent / "output"
            replacement_output = replacement_parent / "output"
            original_output.mkdir(parents=True)
            replacement_output.mkdir(parents=True)
            alias_parent = root / "alias-parent"
            alias_parent.symlink_to(original_parent, target_is_directory=True)
            output = alias_parent / "output"
            original_sentinel = b"original alias output sentinel"
            replacement_sentinel = b"replacement alias output sentinel"
            (original_output / "sentinel").write_bytes(original_sentinel)
            (replacement_output / "sentinel").write_bytes(replacement_sentinel)
            dataset_sha256 = data_snapshot._source_content_digest(
                data_snapshot._scan_source(source)
            )[0]
            archive_name, manifest_name = data_snapshot._manifest_names(dataset_sha256)
            real_open_output = data_snapshot._open_verified_output_directory
            real_write_archive = data_snapshot._write_archive
            injected = False

            def replace_ancestor_alias_before_open(path: Path, label: str) -> int:
                nonlocal injected
                self.assertEqual("output directory", label)
                self.assertFalse(injected)
                injected = True
                alias_parent.unlink()
                alias_parent.symlink_to(replacement_parent, target_is_directory=True)
                return real_open_output(path, label)

            with mock.patch.object(
                data_snapshot,
                "_open_verified_output_directory",
                side_effect=replace_ancestor_alias_before_open,
            ), mock.patch.object(
                data_snapshot,
                "_write_archive",
                wraps=real_write_archive,
            ) as write_archive:
                with self.assertRaisesRegex(
                    data_snapshot.SnapshotError,
                    "output directory changed",
                ):
                    self.pack(source, output)

            self.assertTrue(injected)
            write_archive.assert_not_called()
            self.assertEqual(original_sentinel, (original_output / "sentinel").read_bytes())
            self.assertEqual(
                replacement_sentinel,
                (replacement_output / "sentinel").read_bytes(),
            )
            for directory in (original_output, replacement_output):
                self.assertFalse((directory / archive_name).exists())
                self.assertFalse((directory / manifest_name).exists())
                self.assertEqual([], list(directory.glob(".data-snapshot-*")))

    def test_pack_rejects_output_directory_symlink_swap_without_touching_victim(
        self,
    ) -> None:
        """Publication must remain in the verified directory after a path swap."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            output.mkdir()
            displaced_output = root / "displaced-output"
            victim = root / "victim"
            victim.mkdir()
            dataset_sha256 = data_snapshot._source_content_digest(
                data_snapshot._scan_source(source)
            )[0]
            archive_name, manifest_name = data_snapshot._manifest_names(dataset_sha256)
            archive_sentinel = b"victim archive sentinel"
            manifest_sentinel = b"victim manifest sentinel"
            (victim / archive_name).write_bytes(archive_sentinel)
            (victim / manifest_name).write_bytes(manifest_sentinel)
            real_write_archive = data_snapshot._write_archive
            swapped = False

            def swap_output_before_staged_archive_write(entries, destination):  # type: ignore[no-untyped-def]
                nonlocal swapped
                if not swapped:
                    swapped = True
                    output.rename(displaced_output)
                    output.symlink_to(victim, target_is_directory=True)
                real_write_archive(entries, destination)

            with mock.patch.object(
                data_snapshot,
                "_write_archive",
                side_effect=swap_output_before_staged_archive_write,
            ):
                with self.assertRaisesRegex(
                    data_snapshot.SnapshotError,
                    "output directory changed",
                ):
                    self.pack(source, output)

            self.assertEqual(archive_sentinel, (victim / archive_name).read_bytes())
            self.assertEqual(manifest_sentinel, (victim / manifest_name).read_bytes())
            self.assertEqual(
                [],
                list(victim.glob(".data-snapshot-*")),
            )
            self.assertTrue(output.is_symlink())

    def test_pack_serializes_conflicting_publishers_and_returns_disk_pair(
        self,
    ) -> None:
        """Concurrent content-addressed publishers cannot return split-brain metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            output.mkdir()
            requests = (
                ("b" * 40, "2026-08-05T16:00:00Z"),
                ("c" * 40, "2026-08-05T16:00:01Z"),
            )
            opened = threading.Barrier(2)
            real_open_output = data_snapshot._open_verified_output_directory
            successes: list[dict] = []
            failures: list[BaseException] = []

            def open_output_together(path: Path, label: str) -> int:
                descriptor = real_open_output(path, label)
                opened.wait(timeout=10)
                return descriptor

            def publish(source_sha: str, created_at: str) -> None:
                try:
                    successes.append(
                        data_snapshot.pack_snapshot(
                            root=source,
                            output_dir=output,
                            source_sha=source_sha,
                            max_archive_bytes=1_000_000,
                            created_at=created_at,
                        )
                    )
                except BaseException as error:
                    failures.append(error)

            with mock.patch.object(
                data_snapshot,
                "_open_verified_output_directory",
                side_effect=open_output_together,
            ):
                threads = [
                    threading.Thread(target=publish, args=request)
                    for request in requests
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=15)

            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(1, len(successes))
            self.assertEqual(1, len(failures))
            summary = successes[0]
            manifest = json.loads(Path(summary["manifest_path"]).read_text())
            self.assertEqual(summary["source_sha"], manifest["source_sha"])
            self.assertEqual(summary["created_at"], manifest["created_at"])
            self.assertEqual(
                summary["archive_filename"],
                manifest["archive"]["filename"],
            )
            self.assertEqual(
                summary["archive_sha256"],
                manifest["archive"]["sha256"],
            )

    def test_pack_fails_closed_without_directory_descriptor_capabilities(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)

            with mock.patch.object(
                data_snapshot,
                "_directory_fd_capabilities_available",
                return_value=False,
                create=True,
            ):
                with self.assertRaisesRegex(
                    data_snapshot.SnapshotError,
                    "secure directory-descriptor publication is unavailable",
                ):
                    self.pack(source, root / "output")

    def test_pack_rejects_corrupt_completed_pair_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            summary = self.pack(source, root / "output")
            archive = Path(summary["archive_path"])
            archive.write_bytes(b"not an archive")

            with self.assertRaisesRegex(data_snapshot.SnapshotError, "byte count"):
                self.pack(source, root / "output")

            self.assertEqual(b"not an archive", archive.read_bytes())

    def test_pack_rejects_nonregular_incomplete_pair_member(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            dataset_sha256 = data_snapshot._source_content_digest(
                data_snapshot._scan_source(source)
            )[0]
            archive_name, manifest_name = data_snapshot._manifest_names(dataset_sha256)

            for kind, name in (("symlink", archive_name), ("directory", manifest_name)):
                with self.subTest(kind=kind):
                    output = root / kind
                    output.mkdir()
                    member = output / name
                    if kind == "symlink":
                        member.symlink_to(root / "elsewhere")
                    else:
                        member.mkdir()

                    with self.assertRaisesRegex(
                        data_snapshot.SnapshotError,
                        "must be a regular file",
                    ):
                        self.pack(source, output)

                    self.assertTrue(member.is_symlink() or member.is_dir())

    def test_verify_rejects_corruption_without_creating_destination(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            summary = self.pack(source, root / "output")
            archive = Path(summary["archive_path"])
            payload = bytearray(archive.read_bytes())
            payload[len(payload) // 2] ^= 1
            archive.write_bytes(payload)
            destination = root / "must-not-exist"

            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "checksum",
            ):
                data_snapshot.verify_snapshot(
                    archive_path=archive,
                    manifest_path=Path(summary["manifest_path"]),
                    max_archive_bytes=1_000_000,
                    extract_root=destination,
                )
            self.assertFalse(destination.exists())

    def test_verify_rejects_manifest_contract_id_and_timestamp_mismatches(self) -> None:
        cases = (
            ("contract_version", 2, "contract version"),
            ("dataset_id", "dataset-" + "b" * 64, "dataset_id"),
            ("created_at", "not-a-time", "created_at"),
        )
        for field, value, message in cases:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                source = self.make_source(root)
                summary = self.pack(source, root / "output")
                manifest_path = Path(summary["manifest_path"])
                manifest = json.loads(manifest_path.read_text())
                manifest[field] = value
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

                with self.assertRaisesRegex(data_snapshot.SnapshotError, message):
                    data_snapshot.verify_snapshot(
                        archive_path=Path(summary["archive_path"]),
                        manifest_path=manifest_path,
                        max_archive_bytes=1_000_000,
                    )

    def write_malicious_snapshot(
        self,
        root: Path,
        bad_member: tarfile.TarInfo,
        payload: bytes = b"bad",
    ) -> tuple[Path, Path]:
        dataset_sha = "b" * 64
        archive_name, manifest_name = data_snapshot._manifest_names(dataset_sha)
        archive_path = root / archive_name
        base_members = []
        for name in [".cache", "data"]:
            info = tarfile.TarInfo(name)
            info.type = tarfile.DIRTYPE
            info.mode = 0o755
            info.mtime = 0
            base_members.append((info, None))
        for relative in data_snapshot.CACHE_FILES:
            info = tarfile.TarInfo(relative.as_posix())
            info.type = tarfile.REGTYPE
            info.mode = 0o644
            info.mtime = 0
            info.size = 1
            base_members.append((info, b"x"))
        base_members.append((bad_member, payload))
        base_members.sort(key=lambda item: item[0].name)
        with archive_path.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
                with tarfile.open(
                    fileobj=zipped,
                    mode="w",
                    format=tarfile.USTAR_FORMAT,
                ) as archive:
                    for info, content in base_members:
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        archive.addfile(
                            info,
                            None if content is None else io.BytesIO(content),
                        )
        archive_path.chmod(0o600)
        manifest = {
            "archive": {
                "bytes": archive_path.stat().st_size,
                "filename": archive_name,
                "sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
            },
            "contract_version": 1,
            "created_at": CREATED_AT,
            "dataset": {
                "bytes": len(payload) + len(data_snapshot.CACHE_FILES),
                "file_count": len(data_snapshot.CACHE_FILES) + int(bad_member.isfile()),
                "sha256": dataset_sha,
            },
            "dataset_id": dataset_sha,
            "source_sha": SOURCE_SHA,
        }
        manifest_path = root / manifest_name
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return archive_path, manifest_path

    def test_verify_rejects_unsafe_duplicate_and_noncanonical_members(self) -> None:
        cases = []
        traversal = tarfile.TarInfo("data/../outside")
        traversal.type = tarfile.REGTYPE
        traversal.mode = 0o644
        traversal.size = 3
        cases.append((traversal, "unsafe archive path"))
        symlink = tarfile.TarInfo("data/link")
        symlink.type = tarfile.SYMTYPE
        symlink.mode = 0o644
        symlink.linkname = "../../outside"
        cases.append((symlink, "unsupported type"))
        unexpected = tarfile.TarInfo(".cache/private-secret.json")
        unexpected.type = tarfile.REGTYPE
        unexpected.mode = 0o644
        unexpected.size = 3
        cases.append((unexpected, "unexpected archive member"))
        duplicate = tarfile.TarInfo("data")
        duplicate.type = tarfile.DIRTYPE
        duplicate.mode = 0o755
        cases.append((duplicate, "duplicate archive member"))
        public_private_file = tarfile.TarInfo(
            "data/insiders/private/raw.xml"
        )
        public_private_file.type = tarfile.REGTYPE
        public_private_file.mode = 0o644
        public_private_file.size = 3
        cases.append((public_private_file, "metadata is not canonical"))
        public_private_directory = tarfile.TarInfo(
            "data/insiders/private"
        )
        public_private_directory.type = tarfile.DIRTYPE
        public_private_directory.mode = 0o755
        cases.append((public_private_directory, "metadata is not canonical"))

        for index, (member, message) in enumerate(cases):
            with self.subTest(message=message), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                archive, manifest = self.write_malicious_snapshot(root, member)
                destination = root / "extract"
                with self.assertRaisesRegex(data_snapshot.SnapshotError, message):
                    data_snapshot.verify_snapshot(
                        archive_path=archive,
                        manifest_path=manifest,
                        max_archive_bytes=1_000_000,
                        extract_root=destination,
                    )
                self.assertFalse(destination.exists())
                self.assertFalse((root / "outside").exists())

    def test_pull_replaces_only_data_and_allowlisted_cache_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            publisher = self.make_source(root, "publisher")
            summary = self.pack(publisher, root / "assets")
            manifest_path = Path(summary["manifest_path"])
            archive_path = Path(summary["archive_path"])
            target = self.make_source(root, "target")
            (target / "data/old-only.json").write_text("old\n")
            (target / ".cache/local-only.txt").write_text("preserve me\n")
            (target / "user-file.txt").write_text("also preserve me\n")
            for relative in data_snapshot.CACHE_FILES:
                (target / relative).write_text("old cache\n")

            release = {
                "assets": [
                    {
                        "name": manifest_path.name,
                        "size": manifest_path.stat().st_size,
                        "url": "asset:manifest",
                    },
                    {
                        "name": archive_path.name,
                        "size": archive_path.stat().st_size,
                        "url": "asset:archive",
                    },
                    {
                        "name": "pages-deployment.json",
                        "size": 1,
                        "url": "asset:marker",
                    },
                ],
                "draft": False,
                "prerelease": False,
                "published_at": CREATED_AT,
                "tag_name": "dataset-20260805T160000Z-" + summary["dataset_id"][:16],
            }
            sources = {
                manifest_path.name: manifest_path,
                archive_path.name: archive_path,
            }

            def copy_asset(**kwargs: object) -> None:
                asset = kwargs["asset"]
                destination = kwargs["destination"]
                shutil.copyfile(sources[asset["name"]], destination)
                Path(str(destination)).chmod(0o600)

            with mock.patch.object(
                data_snapshot,
                "_resolve_release",
                return_value=release,
            ), mock.patch.object(
                data_snapshot,
                "_download_asset",
                side_effect=copy_asset,
            ):
                restored = data_snapshot.pull_snapshot(
                    repository="owner/private-data",
                    root=target,
                    replace=True,
                    token="secret",
                    max_archive_bytes=1_000_000,
                )

            self.assertEqual(release["tag_name"], restored["release_tag"])
            self.assertEqual(summary["dataset_id"], restored["dataset_id"])
            self.assertFalse((target / "data/old-only.json").exists())
            self.assertEqual(
                (publisher / "data/funds/1.json").read_bytes(),
                (target / "data/funds/1.json").read_bytes(),
            )
            for relative in data_snapshot.CACHE_FILES:
                self.assertEqual(
                    (publisher / relative).read_bytes(),
                    (target / relative).read_bytes(),
                )
            self.assertEqual(
                "preserve me\n",
                (target / ".cache/local-only.txt").read_text(),
            )
            self.assertEqual(
                "also preserve me\n",
                (target / "user-file.txt").read_text(),
            )

    def test_pull_rejects_extra_release_asset_before_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            publisher = self.make_source(root, "publisher")
            summary = self.pack(publisher, root / "assets")
            manifest_path = Path(summary["manifest_path"])
            archive_path = Path(summary["archive_path"])
            target = self.make_source(root, "target")
            before = (target / "data/funds/1.json").read_bytes()
            release = {
                "assets": [
                    {
                        "name": manifest_path.name,
                        "size": manifest_path.stat().st_size,
                        "url": "m",
                    },
                    {
                        "name": archive_path.name,
                        "size": archive_path.stat().st_size,
                        "url": "a",
                    },
                    {"name": "unexpected.zip", "size": 1, "url": "x"},
                ],
                "tag_name": "dataset-exact",
            }
            sources = {manifest_path.name: manifest_path}

            def copy_manifest(**kwargs: object) -> None:
                asset = kwargs["asset"]
                shutil.copyfile(sources[asset["name"]], kwargs["destination"])

            with mock.patch.object(
                data_snapshot,
                "_resolve_release",
                return_value=release,
            ), mock.patch.object(
                data_snapshot,
                "_download_asset",
                side_effect=copy_manifest,
            ):
                with self.assertRaisesRegex(
                    data_snapshot.SnapshotError,
                    "release assets",
                ):
                    data_snapshot.pull_snapshot(
                        repository="owner/private-data",
                        root=target,
                        replace=True,
                        token="secret",
                        max_archive_bytes=1_000_000,
                    )
            self.assertEqual(before, (target / "data/funds/1.json").read_bytes())

    def test_resolve_release_uses_newest_stable_published_dataset(self) -> None:
        releases = [
            {
                "tag_name": "dataset-old",
                "published_at": "2026-01-01T00:00:00Z",
                "draft": False,
                "prerelease": False,
                "assets": [],
                "id": 1,
            },
            {
                "tag_name": "dataset-new",
                "published_at": "2026-02-01T00:00:00Z",
                "draft": False,
                "prerelease": False,
                "assets": [],
                "id": 2,
            },
            {
                "tag_name": "dataset-draft",
                "published_at": "2026-03-01T00:00:00Z",
                "draft": True,
                "prerelease": False,
                "assets": [],
                "id": 3,
            },
            {
                "tag_name": "dataset-prerelease",
                "published_at": "2026-04-01T00:00:00Z",
                "draft": False,
                "prerelease": True,
                "assets": [],
                "id": 4,
            },
        ]
        with mock.patch.object(data_snapshot, "_github_json", return_value=releases):
            selected = data_snapshot._resolve_release(
                repository="owner/repo",
                release_tag=None,
                token="secret",
            )
        self.assertEqual("dataset-new", selected["tag_name"])

    def test_resolve_explicit_release_rejects_prerelease(self) -> None:
        release = {
            "tag_name": "dataset-prerelease",
            "published_at": "2026-04-01T00:00:00Z",
            "draft": False,
            "prerelease": True,
            "assets": [],
        }
        with mock.patch.object(data_snapshot, "_github_json", return_value=release):
            with self.assertRaisesRegex(
                data_snapshot.SnapshotError,
                "stable published release",
            ):
                data_snapshot._resolve_release(
                    repository="owner/repo",
                    release_tag="dataset-prerelease",
                    token="secret",
                )

    def test_cli_verify_emits_one_machine_readable_json_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            summary = self.pack(source, root / "output")
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                status = data_snapshot.main(
                    [
                        "verify",
                        "--archive",
                        summary["archive_path"],
                        "--manifest",
                        summary["manifest_path"],
                        "--max-archive-bytes",
                        "1000000",
                    ]
                )
            payload = json.loads(stdout.getvalue())
            self.assertEqual(0, status)
            self.assertTrue(payload["ok"])
            self.assertEqual("verify", payload["command"])
            self.assertEqual(summary["dataset_id"], payload["dataset_id"])


if __name__ == "__main__":
    unittest.main()
