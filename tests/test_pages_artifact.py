from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from insider_publication import (
    canonical_public_json_bytes,
    write_insider_publication,
)
from scripts import build_pages_artifact
from tests.test_insider_publication_validation import publication_fixture


SHA = "a" * 40
DATASET_ID = "b" * 64
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
    Path(
        "data/insiders/private/state/incremental-v1.json"
    ): b'{"private":"PRIVATE_SENTINEL_INCREMENTAL"}\n',
    Path(
        "data/insiders/private/state/backfill/2026Q1.json"
    ): b'{"private":"PRIVATE_SENTINEL_BACKFILL"}\n',
    Path(
        "data/insiders/private/state/reparse-v1.json"
    ): b'{"private":"PRIVATE_SENTINEL_REPARSE"}\n',
    Path(
        "data/insiders/private/state/issuers/0000000001.json"
    ): b'{"private":"PRIVATE_SENTINEL_ISSUER"}\n',
    Path(
        f"data/insiders/private/state/quarantine/accessions/{INSIDER_ACCESSION}.json"
    ): b'{"private":"PRIVATE_SENTINEL_ACCESSION_QUARANTINE"}\n',
    Path(
        "data/insiders/private/state/quarantine/quarters/2026Q1.json"
    ): b'{"private":"PRIVATE_SENTINEL_QUARTER_QUARANTINE"}\n',
    Path(
        "data/insiders/private/state/telemetry-v1.json"
    ): b'{"private":"PRIVATE_SENTINEL_TELEMETRY"}\n',
}
PRIVATE_INSIDER_SENTINELS = tuple(PRIVATE_INSIDER_FILES.values())


class PagesArtifactTests(unittest.TestCase):
    def make_source(self, root: Path) -> Path:
        source = root / "source"
        (source / "data/funds").mkdir(parents=True)
        (source / "data/stocks").mkdir(parents=True)
        (source / ".nojekyll").write_text("")
        (source / "CNAME").write_text("example.test\n")
        (source / "site-data-loader.js").write_text("window.fetch = fetch;\n")
        (source / "index.html").write_text(
            '<html><head><script src="site-data-loader.js"></script>'
            "</head><body><script>const DATA_CONTRACT_VERSION = 5;</script>"
            "</body></html>\n"
        )
        (source / "data/funds-index.json").write_text('{"funds":[]}\n')
        (source / "data/index.json").write_text('{"tickers":[]}\n')
        (source / "data/security_labels.json").write_text(
            '{"kinds":{},"labels":{},"product_names":{}}\n'
        )
        (source / "data/pipeline_state.json").write_text('{"private":true}\n')
        (source / "data/ticker_health.json").write_text('{"private":true}\n')
        (source / "data/cusip_registry.json").write_text('{"private":true}\n')
        (source / "data/cache").mkdir()
        (source / "data/cache/openfigi.json").write_text('{"private":true}\n')
        for relative, payload in PRIVATE_INSIDER_FILES.items():
            target = source / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        (source / ".cache").mkdir()
        (source / ".cache/cusip-map.json").write_text('{"private":true}\n')
        (source / "data/funds/1.json").write_text(
            json.dumps({"cik": "1", "holdings": ["A" * 4_000]})
        )
        (source / "data/stocks/ABC.json").write_text(
            json.dumps({"ticker": "ABC", "funds": ["B" * 4_000]})
        )
        return source

    def add_public_insiders(self, source: Path) -> Path:
        write_insider_publication(
            publication_fixture(),
            repository_root=source,
        )
        return source / "data/insiders/public"

    def build(self, source: Path, output: Path) -> dict[str, int | str]:
        return build_pages_artifact.build_artifact(
            source_root=source,
            output_root=output,
            source_sha=SHA,
            dataset_id=DATASET_ID,
            workers=2,
            compresslevel=6,
            max_archive_bytes=1_000_000,
        )

    def test_build_is_bounded_compressed_and_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            first = root / "first"
            second = root / "second"
            first.mkdir()

            first_summary = self.build(source, first)
            second_summary = self.build(source, second)

            self.assertFalse((first / "data/funds/1.json").exists())
            self.assertFalse((first / "data/stocks/ABC.json").exists())
            self.assertEqual(
                (source / "data/funds/1.json").read_bytes(),
                gzip.decompress((first / "data/funds/1.json.gz").read_bytes()),
            )
            self.assertEqual(
                (source / "data/stocks/ABC.json").read_bytes(),
                gzip.decompress((first / "data/stocks/ABC.json.gz").read_bytes()),
            )
            self.assertEqual(
                (first / "data/funds/1.json.gz").read_bytes(),
                (second / "data/funds/1.json.gz").read_bytes(),
            )
            self.assertEqual(
                json.loads((first / "deployment-manifest.json").read_text()),
                json.loads((second / "deployment-manifest.json").read_text()),
            )
            self.assertEqual(
                first_summary["archive_bytes"],
                second_summary["archive_bytes"],
            )
            self.assertLess(first_summary["archive_bytes"], 1_000_000)
            self.assertEqual("example.test\n", (first / "CNAME").read_text())
            self.assertTrue((first / ".nojekyll").is_file())
            manifest = json.loads((first / "deployment-manifest.json").read_text())
            self.assertEqual(SHA, manifest["source_sha"])
            self.assertEqual(DATASET_ID, manifest["dataset_id"])
            self.assertIn(
                "const DATA_CONTRACT_VERSION = 5;",
                (first / "index.html").read_text(),
            )
            self.assertFalse((first / "data/insiders").exists())
            for path in first.rglob("*"):
                if not path.is_file():
                    continue
                payload = path.read_bytes()
                for sentinel in PRIVATE_INSIDER_SENTINELS:
                    self.assertNotIn(sentinel, payload, path.as_posix())
            for relative in (
                *build_pages_artifact.STATIC_FILES,
                *build_pages_artifact.INDEX_FILES,
                *build_pages_artifact.COMPRESSED_DIRECTORIES,
            ):
                public_input = relative.as_posix()
                self.assertFalse(
                    public_input == "data/insiders"
                    or public_input.startswith("data/insiders/"),
                    public_input,
                )

            public_files = {
                path.relative_to(first).as_posix()
                for path in first.rglob("*")
                if path.is_file()
            }
            self.assertEqual(
                {
                    ".nojekyll",
                    "CNAME",
                    "deployment-manifest.json",
                    "index.html",
                    "site-data-loader.js",
                    "data/funds-index.json",
                    "data/index.json",
                    "data/security_labels.json",
                    "data/funds/1.json.gz",
                    "data/stocks/ABC.json.gz",
                },
                public_files,
            )

    def test_valid_public_insider_projection_is_validated_and_packaged(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            public_root = self.add_public_insiders(source)
            output = root / "output"

            summary = self.build(source, output)

            self.assertEqual(1, summary["insider_security_payloads"])
            self.assertEqual(2, summary["insider_filing_payloads"])
            self.assertEqual(
                (public_root / "manifest.json").read_bytes(),
                (output / "data/insiders/public/manifest.json").read_bytes(),
            )
            for relative in (
                Path("securities/03770N101.json"),
                Path("filings/0000000001-26-000001.json"),
                Path("filings/0000000001-26-000002.json"),
            ):
                self.assertFalse((output / "data/insiders/public" / relative).exists())
                self.assertEqual(
                    (public_root / relative).read_bytes(),
                    gzip.decompress(
                        (
                            output
                            / "data/insiders/public"
                            / f"{relative.as_posix()}.gz"
                        ).read_bytes()
                    ),
                )

            insider_files = {
                path.relative_to(output).as_posix()
                for path in (output / "data/insiders").rglob("*")
                if path.is_file()
            }
            self.assertEqual(
                {
                    "data/insiders/public/manifest.json",
                    "data/insiders/public/securities/03770N101.json.gz",
                    "data/insiders/public/filings/0000000001-26-000001.json.gz",
                    "data/insiders/public/filings/0000000001-26-000002.json.gz",
                },
                insider_files,
            )
            for path in output.rglob("*"):
                if not path.is_file():
                    continue
                for sentinel in PRIVATE_INSIDER_SENTINELS:
                    self.assertNotIn(sentinel, path.read_bytes(), path.as_posix())

    def test_packaging_holds_shared_insider_publication_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            self.add_public_insiders(source)
            real_flock = build_pages_artifact.fcntl.flock
            lock_operations: list[int] = []

            def record_lock(descriptor: int, operation: int) -> None:
                lock_operations.append(operation)
                real_flock(descriptor, operation)

            with mock.patch.object(
                build_pages_artifact.fcntl,
                "flock",
                side_effect=record_lock,
            ):
                self.build(source, root / "output")

            self.assertEqual(
                [
                    build_pages_artifact.fcntl.LOCK_SH,
                    build_pages_artifact.fcntl.LOCK_UN,
                ],
                lock_operations,
            )

    def test_packaging_fails_if_validated_insider_snapshot_changes_before_commit(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            public_root = self.add_public_insiders(source)
            source_page = public_root / "securities/03770N101.json"
            real_reader = build_pages_artifact.read_validated_insider_public_snapshot_fd
            read_count = 0

            def snapshot_then_mutate(public_root_fd: int):
                nonlocal read_count
                snapshot, errors = real_reader(public_root_fd)
                read_count += 1
                if read_count == 1:
                    source_page.write_bytes(b"POST_SNAPSHOT_MUTATION\n")
                return snapshot, errors

            output = root / "output"
            with (
                mock.patch.object(
                    build_pages_artifact,
                    "read_validated_insider_public_snapshot_fd",
                    side_effect=snapshot_then_mutate,
                ),
                self.assertRaisesRegex(
                    ValueError,
                    "public insider projection changed during artifact build",
                ),
            ):
                self.build(source, output)

            self.assertTrue(not output.exists() or not any(output.iterdir()))
            self.assertEqual(b"POST_SNAPSHOT_MUTATION\n", source_page.read_bytes())

    def test_invalid_public_insider_projection_fails_before_packaging(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            public_root = self.add_public_insiders(source)
            page_path = public_root / "securities/03770N101.json"
            page = json.loads(page_path.read_text())
            page["summary"]["purchases"]["value"] = "999999"
            page_path.write_bytes(canonical_public_json_bytes(page))

            manifest_path = public_root / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            entry = manifest["securityPayloads"][0]
            payload = page_path.read_bytes()
            entry["bytes"] = len(payload)
            entry["sha256"] = hashlib.sha256(payload).hexdigest()
            manifest_path.write_bytes(canonical_public_json_bytes(manifest))

            with self.assertRaisesRegex(
                ValueError,
                "public insider projection failed validation",
            ):
                self.build(source, root / "output")

    def test_dangling_public_insider_root_symlink_fails_before_packaging(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            public_root = source / "data/insiders/public"
            public_root.parent.mkdir(parents=True, exist_ok=True)
            public_root.symlink_to(
                source / "missing-public-tree",
                target_is_directory=True,
            )

            with self.assertRaisesRegex(
                ValueError,
                "public insider projection root must be a regular directory",
            ):
                self.build(source, root / "output")

    def test_output_parent_alias_cannot_publish_inside_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output_parent = source / "data/output-parent"
            output_parent.mkdir()
            alias = root / "source-alias"
            alias.symlink_to(source, target_is_directory=True)
            output = alias / "data/output-parent/artifact"

            with self.assertRaisesRegex(
                ValueError,
                "output directory must be outside the source repository",
            ):
                self.build(source, output)

            self.assertFalse((output_parent / "artifact").exists())

    def test_private_only_change_does_not_change_public_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            first = root / "first"
            second = root / "second"

            first_summary = self.build(source, first)
            telemetry = source / "data/insiders/private/state/telemetry-v1.json"
            telemetry.write_bytes(b'{"private":"PRIVATE_SENTINEL_CHANGED"}\n')
            second_summary = self.build(source, second)

            first_files = {
                path.relative_to(first).as_posix(): path.read_bytes()
                for path in first.rglob("*")
                if path.is_file()
            }
            second_files = {
                path.relative_to(second).as_posix(): path.read_bytes()
                for path in second.rglob("*")
                if path.is_file()
            }
            self.assertEqual(first_summary, second_summary)
            self.assertEqual(first_files, second_files)

    def test_build_rejects_missing_loader_integration(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            (source / "index.html").write_text("<html></html>\n")

            with self.assertRaisesRegex(
                ValueError,
                "must load site-data-loader.js",
            ):
                self.build(source, root / "output")

    def test_build_fails_closed_at_archive_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            with self.assertRaisesRegex(ValueError, "archive is too large"):
                build_pages_artifact.build_artifact(
                    source_root=source,
                    output_root=output,
                    source_sha=SHA,
                    dataset_id=DATASET_ID,
                    workers=1,
                    compresslevel=6,
                    max_archive_bytes=1,
                )
            self.assertTrue(not output.exists() or not any(output.iterdir()))

    def test_build_rejects_source_root_replacement_before_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            displaced = root / "source-displaced"
            output = root / "output"
            displaced_once = False

            def displace_before_commit(label: str, *args: object) -> None:
                nonlocal displaced_once
                if label == "before_commit" and not displaced_once:
                    displaced_once = True
                    source.rename(displaced)
                    source.mkdir()
                    (source / "FOREIGN_SENTINEL").write_text("FOREIGN\n")

            with mock.patch.object(
                build_pages_artifact,
                "_artifact_checkpoint",
                create=True,
                side_effect=displace_before_commit,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "source repository root changed during artifact build",
                ):
                    self.build(source, output)

            self.assertTrue(displaced_once)
            self.assertEqual("FOREIGN\n", (source / "FOREIGN_SENTINEL").read_text())
            self.assertTrue(not output.exists() or not any(output.iterdir()))

    def test_build_rejects_fund_directory_replacement_before_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            funds = source / "data/funds"
            displaced = source / "data/funds-displaced"
            output = root / "output"
            displaced_once = False

            def displace_before_commit(label: str, *args: object) -> None:
                nonlocal displaced_once
                if label == "before_commit" and not displaced_once:
                    displaced_once = True
                    funds.rename(displaced)
                    funds.mkdir()
                    (funds / "FOREIGN_SENTINEL").write_text("FOREIGN\n")

            with mock.patch.object(
                build_pages_artifact,
                "_artifact_checkpoint",
                side_effect=displace_before_commit,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "fund data root changed during artifact build",
                ):
                    self.build(source, output)

            self.assertTrue(displaced_once)
            self.assertEqual("FOREIGN\n", (funds / "FOREIGN_SENTINEL").read_text())
            self.assertTrue(not output.exists() or not any(output.iterdir()))

    def test_build_rejects_new_fund_file_before_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            added_once = False

            def add_before_commit(label: str, *args: object) -> None:
                nonlocal added_once
                if label == "before_commit" and not added_once:
                    added_once = True
                    (source / "data/funds/2.json").write_text('{"cik":"2"}\n')

            with mock.patch.object(
                build_pages_artifact,
                "_artifact_checkpoint",
                side_effect=add_before_commit,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "fund data root changed during artifact build",
                ):
                    self.build(source, output)

            self.assertTrue(added_once)
            self.assertTrue(not output.exists() or not any(output.iterdir()))

    def test_build_rejects_output_parent_replacement_before_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output_parent = root / "artifacts"
            output_parent.mkdir()
            output = output_parent / "output"
            output.mkdir()
            displaced = root / "artifacts-displaced"
            displaced_once = False

            def displace_before_commit(label: str, *args: object) -> None:
                nonlocal displaced_once
                if label == "before_commit" and not displaced_once:
                    displaced_once = True
                    output_parent.rename(displaced)
                    output_parent.mkdir()
                    (output_parent / "FOREIGN_SENTINEL").write_text("FOREIGN\n")

            with mock.patch.object(
                build_pages_artifact,
                "_artifact_checkpoint",
                create=True,
                side_effect=displace_before_commit,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "artifact output parent changed during artifact build",
                ):
                    self.build(source, output)

            self.assertTrue(displaced_once)
            self.assertEqual(
                "FOREIGN\n",
                (output_parent / "FOREIGN_SENTINEL").read_text(),
            )
            self.assertFalse((output_parent / "output").exists())

    def test_build_rejects_reserved_output_replacement_before_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            displaced = root / "output-displaced"
            displaced_once = False

            def displace_before_commit(label: str, *args: object) -> None:
                nonlocal displaced_once
                if label == "before_commit" and not displaced_once:
                    displaced_once = True
                    output.rename(displaced)
                    output.mkdir()
                    (output / "FOREIGN_SENTINEL").write_text("FOREIGN\n")

            with mock.patch.object(
                build_pages_artifact,
                "_artifact_checkpoint",
                side_effect=displace_before_commit,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "output directory changed during artifact build",
                ):
                    self.build(source, output)

            self.assertTrue(displaced_once)
            self.assertEqual("FOREIGN\n", (output / "FOREIGN_SENTINEL").read_text())

    def test_normalized_write_failure_preserves_foreign_name_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            directory_fd = os.open(
                root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            destination = build_pages_artifact.FileDestination(
                directory_fd=directory_fd,
                name="payload.json",
                display_path="payload.json",
            )

            def replace_then_fail(
                descriptor: int,
                payload: bytes,
                label: str,
            ) -> None:
                del descriptor, payload, label
                (root / "payload.json").rename(root / "owned-partial.json")
                (root / "payload.json").write_text("FOREIGN\n")
                raise ValueError("simulated write failure")

            try:
                with (
                    mock.patch.object(
                        build_pages_artifact,
                        "_write_all",
                        side_effect=replace_then_fail,
                    ),
                    self.assertRaisesRegex(ValueError, "simulated write failure"),
                ):
                    build_pages_artifact.normalized_write(b"payload", destination)
            finally:
                os.close(directory_fd)

            self.assertEqual("FOREIGN\n", (root / "payload.json").read_text())
            self.assertTrue((root / "owned-partial.json").is_file())

    def test_gzip_failure_preserves_foreign_name_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            directory_fd = os.open(
                root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            destination = build_pages_artifact.FileDestination(
                directory_fd=directory_fd,
                name="payload.json.gz",
                display_path="payload.json.gz",
            )

            def replace_then_fail(
                descriptor: int,
                admitted_destination: build_pages_artifact.FileDestination,
            ) -> int:
                del descriptor, admitted_destination
                (root / "payload.json.gz").rename(root / "owned-partial.json.gz")
                (root / "payload.json.gz").write_text("FOREIGN\n")
                raise ValueError("simulated compression failure")

            try:
                with (
                    mock.patch.object(
                        build_pages_artifact,
                        "_finish_compression_destination",
                        side_effect=replace_then_fail,
                    ),
                    self.assertRaisesRegex(ValueError, "simulated compression failure"),
                ):
                    build_pages_artifact.gzip_bytes(
                        b"payload",
                        destination,
                        compresslevel=6,
                    )
            finally:
                os.close(directory_fd)

            self.assertEqual("FOREIGN\n", (root / "payload.json.gz").read_text())
            self.assertTrue((root / "owned-partial.json.gz").is_file())

    def test_gzip_source_descriptor_closes_when_destination_admission_fails(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "source.json").write_bytes(b"{}\n")
            directory_fd = os.open(
                root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            metadata = os.stat(
                "source.json",
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            source = build_pages_artifact.FileSource(
                directory_fd=directory_fd,
                name="source.json",
                display_path="source.json",
                device=metadata.st_dev,
                inode=metadata.st_ino,
                size=metadata.st_size,
                modified_ns=metadata.st_mtime_ns,
                changed_ns=metadata.st_ctime_ns,
            )
            destination = build_pages_artifact.FileDestination(
                directory_fd=directory_fd,
                name="destination.json.gz",
                display_path="destination.json.gz",
            )
            real_open = build_pages_artifact._open_verified_regular_file_at
            opened_source_fds: list[int] = []

            def record_source_fd(
                source_directory_fd: int,
                source_name: str,
                source_label: str,
            ):
                opened = real_open(
                    source_directory_fd,
                    source_name,
                    source_label,
                )
                opened_source_fds.append(opened[0])
                return opened

            try:
                with (
                    mock.patch.object(
                        build_pages_artifact,
                        "_open_verified_regular_file_at",
                        side_effect=record_source_fd,
                    ),
                    mock.patch.object(
                        build_pages_artifact,
                        "_open_compression_destination",
                        side_effect=ValueError("destination unavailable"),
                    ),
                    self.assertRaisesRegex(ValueError, "destination unavailable"),
                ):
                    build_pages_artifact.gzip_file(
                        source,
                        destination,
                        compresslevel=6,
                    )
                self.assertEqual(1, len(opened_source_fds))
                with self.assertRaises(OSError):
                    os.fstat(opened_source_fds[0])
            finally:
                for descriptor in opened_source_fds:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                os.close(directory_fd)

    def test_gzip_bytes_closes_duplicate_if_stream_admission_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            directory_fd = os.open(root, build_pages_artifact._DIRECTORY_FLAGS)
            destination = build_pages_artifact.FileDestination(
                directory_fd=directory_fd,
                name="payload.json.gz",
                display_path="payload.json.gz",
            )
            real_dup = os.dup
            duplicate_fds: list[int] = []

            def record_duplicate(descriptor: int) -> int:
                duplicate = real_dup(descriptor)
                duplicate_fds.append(duplicate)
                return duplicate

            try:
                with (
                    mock.patch.object(
                        build_pages_artifact.os,
                        "dup",
                        side_effect=record_duplicate,
                    ),
                    mock.patch.object(
                        build_pages_artifact.os,
                        "fdopen",
                        side_effect=OSError("stream admission failed"),
                    ),
                    self.assertRaisesRegex(OSError, "stream admission failed"),
                ):
                    build_pages_artifact.gzip_bytes(
                        b"payload",
                        destination,
                        compresslevel=6,
                    )
                self.assertEqual(1, len(duplicate_fds))
                with self.assertRaises(OSError):
                    os.fstat(duplicate_fds[0])
            finally:
                for descriptor in duplicate_fds:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                os.close(directory_fd)

    def test_gzip_file_closes_duplicate_if_stream_admission_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "source.json").write_bytes(b"{}\n")
            directory_fd = os.open(root, build_pages_artifact._DIRECTORY_FLAGS)
            metadata = os.stat(
                "source.json",
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            source = build_pages_artifact.FileSource(
                directory_fd=directory_fd,
                name="source.json",
                display_path="source.json",
                device=metadata.st_dev,
                inode=metadata.st_ino,
                size=metadata.st_size,
                modified_ns=metadata.st_mtime_ns,
                changed_ns=metadata.st_ctime_ns,
            )
            destination = build_pages_artifact.FileDestination(
                directory_fd=directory_fd,
                name="destination.json.gz",
                display_path="destination.json.gz",
            )
            real_dup = os.dup
            duplicate_fds: list[int] = []

            def record_duplicate(descriptor: int) -> int:
                duplicate = real_dup(descriptor)
                duplicate_fds.append(duplicate)
                return duplicate

            try:
                with (
                    mock.patch.object(
                        build_pages_artifact.os,
                        "dup",
                        side_effect=record_duplicate,
                    ),
                    mock.patch.object(
                        build_pages_artifact.os,
                        "fdopen",
                        side_effect=OSError("stream admission failed"),
                    ),
                    self.assertRaisesRegex(OSError, "stream admission failed"),
                ):
                    build_pages_artifact.gzip_file(
                        source,
                        destination,
                        compresslevel=6,
                    )
                self.assertEqual(1, len(duplicate_fds))
                with self.assertRaises(OSError):
                    os.fstat(duplicate_fds[0])
            finally:
                for descriptor in duplicate_fds:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                os.close(directory_fd)

    def test_directory_descriptor_ancestry_is_identity_based(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            descendant = source / "nested/output"
            outside = root / "outside"
            descendant.mkdir(parents=True)
            outside.mkdir()
            source_fd = os.open(source, build_pages_artifact._DIRECTORY_FLAGS)
            descendant_fd = os.open(descendant, build_pages_artifact._DIRECTORY_FLAGS)
            outside_fd = os.open(outside, build_pages_artifact._DIRECTORY_FLAGS)
            try:
                self.assertTrue(
                    build_pages_artifact._directory_fd_is_within(
                        source_fd,
                        descendant_fd,
                    )
                )
                self.assertTrue(
                    build_pages_artifact._directory_fd_is_within(source_fd, source_fd)
                )
                self.assertFalse(
                    build_pages_artifact._directory_fd_is_within(source_fd, outside_fd)
                )
            finally:
                os.close(outside_fd)
                os.close(descendant_fd)
                os.close(source_fd)

    def test_failed_build_retains_uncertain_stage_instead_of_deleting_it(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            output = root / "output"
            observed_stages: list[Path] = []

            def replace_stage_file_then_fail(*args: object, **kwargs: object) -> int:
                del args, kwargs
                stages = list(root.glob(".output.prepare-*"))
                self.assertEqual(1, len(stages))
                stage = stages[0]
                (stage / "index.html").rename(stage / "owned-index.html")
                (stage / "index.html").write_text("FOREIGN\n")
                observed_stages.append(stage)
                raise ValueError("simulated archive failure")

            with (
                mock.patch.object(
                    build_pages_artifact,
                    "deterministic_tar_size",
                    side_effect=replace_stage_file_then_fail,
                ),
                self.assertRaisesRegex(ValueError, "simulated archive failure"),
            ):
                self.build(source, output)

            self.assertEqual(1, len(observed_stages))
            retained_stage = observed_stages[0]
            self.assertEqual("FOREIGN\n", (retained_stage / "index.html").read_text())
            self.assertTrue((retained_stage / "owned-index.html").is_file())
            self.assertTrue(not output.exists() or not any(output.iterdir()))

    def test_build_enforces_independent_source_resource_limits(self) -> None:
        cases = (
            ("MAX_STATIC_FILE_BYTES", 1, "static file exceeds"),
            ("MAX_DATA_JSON_BYTES", 1, "data JSON exceeds"),
            ("MAX_TOTAL_SOURCE_BYTES", 1, "total source bytes exceed"),
            ("MAX_DATA_FILES_PER_DIRECTORY", 0, "data file count exceeds"),
            ("MAX_ARTIFACT_FILES", 1, "artifact file count exceeds"),
        )
        for constant, limit, message in cases:
            with (
                self.subTest(constant=constant),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                root = Path(tmpdir)
                source = self.make_source(root)
                with mock.patch.object(
                    build_pages_artifact,
                    constant,
                    limit,
                    create=True,
                ):
                    with self.assertRaisesRegex(ValueError, message):
                        self.build(source, root / "output")

    def test_build_rejects_excessive_gzip_compression_ratio(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            (source / "data/funds/1.json").write_text(
                json.dumps({"holdings": ["A" * 100_000]})
            )
            with mock.patch.object(
                build_pages_artifact,
                "MAX_GZIP_COMPRESSION_RATIO",
                2,
                create=True,
            ):
                with self.assertRaisesRegex(ValueError, "compression ratio exceeds"):
                    self.build(source, root / "output")

    def test_build_rejects_invalid_dataset_id(self) -> None:
        invalid_ids = (
            "a" * 63,
            "a" * 65,
            "A" * 64,
            "g" * 64,
        )
        for dataset_id in invalid_ids:
            with self.subTest(dataset_id=dataset_id):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    source = self.make_source(root)
                    with self.assertRaisesRegex(
                        ValueError,
                        "dataset ID must be exactly 64 lowercase hex characters",
                    ):
                        build_pages_artifact.build_artifact(
                            source_root=source,
                            output_root=root / "output",
                            source_sha=SHA,
                            dataset_id=dataset_id,
                            workers=1,
                            compresslevel=6,
                            max_archive_bytes=1_000_000,
                        )

    def test_build_rejects_excessive_worker_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.make_source(root)
            with self.assertRaisesRegex(
                ValueError,
                f"workers must not exceed {build_pages_artifact.MAX_COMPRESSION_WORKERS}",
            ):
                build_pages_artifact.build_artifact(
                    source_root=source,
                    output_root=root / "output",
                    source_sha=SHA,
                    dataset_id=DATASET_ID,
                    workers=build_pages_artifact.MAX_COMPRESSION_WORKERS + 1,
                    compresslevel=6,
                    max_archive_bytes=1_000_000,
                )


if __name__ == "__main__":
    unittest.main()
