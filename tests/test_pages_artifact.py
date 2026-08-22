from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from scripts import build_pages_artifact


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


class PagesArtifactTests(unittest.TestCase):
    def make_source(self, root: Path) -> Path:
        source = root / "source"
        (source / "data/funds").mkdir(parents=True)
        (source / "data/stocks").mkdir(parents=True)
        (source / ".nojekyll").write_text("")
        (source / "CNAME").write_text("example.test\n")
        (source / "site-data-loader.js").write_text("window.fetch = fetch;\n")
        (source / "index.html").write_text(
            "<html><head><script src=\"site-data-loader.js\"></script>"
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
                gzip.decompress(
                    (first / "data/funds/1.json.gz").read_bytes()
                ),
            )
            self.assertEqual(
                (source / "data/stocks/ABC.json").read_bytes(),
                gzip.decompress(
                    (first / "data/stocks/ABC.json.gz").read_bytes()
                ),
            )
            self.assertEqual(
                (first / "data/funds/1.json.gz").read_bytes(),
                (second / "data/funds/1.json.gz").read_bytes(),
            )
            self.assertEqual(
                json.loads(
                    (first / "deployment-manifest.json").read_text()
                ),
                json.loads(
                    (second / "deployment-manifest.json").read_text()
                ),
            )
            self.assertEqual(
                first_summary["archive_bytes"],
                second_summary["archive_bytes"],
            )
            self.assertLess(first_summary["archive_bytes"], 1_000_000)
            self.assertEqual("example.test\n", (first / "CNAME").read_text())
            self.assertTrue((first / ".nojekyll").is_file())
            manifest = json.loads(
                (first / "deployment-manifest.json").read_text()
            )
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
            with self.assertRaisesRegex(ValueError, "archive is too large"):
                build_pages_artifact.build_artifact(
                    source_root=source,
                    output_root=root / "output",
                    source_sha=SHA,
                    dataset_id=DATASET_ID,
                    workers=1,
                    compresslevel=6,
                    max_archive_bytes=1,
                )

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


if __name__ == "__main__":
    unittest.main()
