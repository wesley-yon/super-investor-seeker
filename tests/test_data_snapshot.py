from __future__ import annotations

import contextlib
import gzip
import hashlib
import io
import json
import shutil
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import data_snapshot


SOURCE_SHA = "a" * 40
CREATED_AT = "2026-08-05T16:00:00Z"


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
        for index, relative in enumerate(data_snapshot.CACHE_FILES):
            (source / relative).write_text(
                json.dumps({"cache": index}, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        (source / ".cache/local-only.txt").write_text(
            "must not be archived\n",
            encoding="utf-8",
        )
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
            for relative in data_snapshot.CACHE_FILES:
                self.assertEqual(
                    (source / relative).read_bytes(),
                    (extracted / relative).read_bytes(),
                )
            self.assertFalse((extracted / ".cache/local-only.txt").exists())

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

    def test_verify_rejects_traversal_symlink_and_unexpected_members(self) -> None:
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
