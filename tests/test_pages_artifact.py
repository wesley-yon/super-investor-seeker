from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from scripts import build_pages_artifact


SHA = "a" * 40
DATASET_ID = "b" * 64


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
            "</head><body><script>const DATA_CONTRACT_VERSION = 3;</script>"
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
        (source / "data/cache/sec_evidence.json").write_text('{"private":true}\n')
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
