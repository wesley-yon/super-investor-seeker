#!/usr/bin/env python3
"""Build the bounded, deterministic static artifact deployed to GitHub Pages."""

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import hashlib
import json
import os
import re
import shutil
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MAX_ARCHIVE_BYTES = 1_000_000_000
STATIC_FILES = (
    Path(".nojekyll"),
    Path("CNAME"),
    Path("index.html"),
    Path("app.js"),
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
SHA_RE = re.compile(r"[0-9a-f]{40}")
DATASET_ID_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class CompressionResult:
    source_bytes: int
    compressed_bytes: int


def regular_file(path: Path) -> None:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"required regular file is missing: {path}")


def normalized_copy(source: Path, destination: Path) -> int:
    regular_file(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    destination.chmod(0o644)
    os.utime(destination, (0, 0))
    return source.stat().st_size


def gzip_file(
    source: Path,
    destination: Path,
    *,
    compresslevel: int,
) -> CompressionResult:
    regular_file(source)
    temporary = destination.with_name(f".{destination.name}.tmp")
    with source.open("rb") as source_handle, temporary.open("wb") as raw_output:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_output,
            compresslevel=compresslevel,
            mtime=0,
        ) as gzip_output:
            shutil.copyfileobj(source_handle, gzip_output, length=1024 * 1024)
    temporary.replace(destination)
    destination.chmod(0o644)
    os.utime(destination, (0, 0))
    return CompressionResult(
        source_bytes=source.stat().st_size,
        compressed_bytes=destination.stat().st_size,
    )


def iter_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    )


def content_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in iter_files(root):
        relative = path.relative_to(root).as_posix().encode()
        digest.update(relative)
        digest.update(b"\0")
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        digest.update(b"\0")
    return digest.hexdigest()


def deterministic_tar_size(root: Path) -> int:
    """Materialize the upload tar once so its actual size is gated."""
    with tempfile.NamedTemporaryFile(
        prefix="pages-artifact-",
        suffix=".tar",
        dir=root.parent,
        delete=False,
    ) as temporary:
        archive_path = Path(temporary.name)

    try:
        with tarfile.open(
            archive_path,
            mode="w",
            format=tarfile.USTAR_FORMAT,
        ) as archive:
            paths = sorted(
                (path for path in root.rglob("*") if not path.is_symlink()),
                key=lambda path: path.relative_to(root).as_posix(),
            )
            for path in paths:
                relative = path.relative_to(root).as_posix()
                info = archive.gettarinfo(str(path), arcname=relative)
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                info.mtime = 0
                info.mode = 0o755 if path.is_dir() else 0o644
                if path.is_file():
                    with path.open("rb") as handle:
                        archive.addfile(info, handle)
                elif path.is_dir():
                    archive.addfile(info)
                else:
                    raise ValueError(
                        f"Pages artifact contains unsupported entry: {path}"
                    )
        return archive_path.stat().st_size
    finally:
        archive_path.unlink(missing_ok=True)


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
    source_root = source_root.resolve()
    output_root = output_root.resolve()
    if not SHA_RE.fullmatch(source_sha):
        raise ValueError("source SHA must be exactly 40 lowercase hex characters")
    if not DATASET_ID_RE.fullmatch(dataset_id):
        raise ValueError("dataset ID must be exactly 64 lowercase hex characters")
    if workers < 1:
        raise ValueError("workers must be at least 1")
    if not 1 <= compresslevel <= 9:
        raise ValueError("compress level must be between 1 and 9")
    if max_archive_bytes < 1:
        raise ValueError("maximum archive size must be positive")
    if output_root == source_root or source_root in output_root.parents:
        raise ValueError("output directory must be outside the source repository")
    if output_root.exists():
        if not output_root.is_dir() or any(output_root.iterdir()):
            raise ValueError(
                f"output directory must be absent or empty: {output_root}"
            )

    html = (source_root / "index.html").read_text(encoding="utf-8")
    loader_tag = re.search(
        r"""<script\b[^>]*\bsrc=["']site-data-loader\.js["'][^>]*>""",
        html,
    )
    application_tag = re.search(
        r"""<script\b[^>]*\bsrc=["']app\.js["'][^>]*>""",
        html,
    )
    if loader_tag is None:
        raise ValueError(
            "index.html must load site-data-loader.js before Pages packaging"
        )
    if (
        application_tag is None
        or loader_tag.start() > application_tag.start()
        or re.search(r"\b(?:async|defer)\b", loader_tag.group(0), re.IGNORECASE)
        or re.search(r"\b(?:async|defer)\b", application_tag.group(0), re.IGNORECASE)
    ):
        raise ValueError(
            "site-data-loader.js must load synchronously before the application"
        )

    if "const DATA_CONTRACT_VERSION" not in (source_root / "app.js").read_text(encoding="utf-8"):
        raise ValueError("app.js must contain the application data-contract guard")

    output_root.mkdir(parents=True, exist_ok=True)
    static_source_bytes = 0
    for relative in (*STATIC_FILES, *INDEX_FILES):
        static_source_bytes += normalized_copy(
            source_root / relative,
            output_root / relative,
        )

    compression_tasks: list[tuple[Path, Path]] = []
    directory_counts: dict[str, int] = {}
    for relative_directory in COMPRESSED_DIRECTORIES:
        source_directory = source_root / relative_directory
        if not source_directory.is_dir() or source_directory.is_symlink():
            raise ValueError(
                f"required data directory is missing: {source_directory}"
            )
        sources = sorted(source_directory.glob("*.json"))
        if not sources:
            raise ValueError(f"no JSON payloads found in {source_directory}")
        unexpected = [
            path
            for path in source_directory.iterdir()
            if not path.is_file() or path.suffix != ".json" or path.is_symlink()
        ]
        if unexpected:
            raise ValueError(
                f"unexpected entry in {source_directory}: {unexpected[0]}"
            )
        destination_directory = output_root / relative_directory
        destination_directory.mkdir(parents=True)
        for source in sources:
            compression_tasks.append(
                (source, destination_directory / f"{source.name}.gz")
            )
        directory_counts[relative_directory.name] = len(sources)

    def compress(task: tuple[Path, Path]) -> CompressionResult:
        return gzip_file(
            task[0],
            task[1],
            compresslevel=compresslevel,
        )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers,
    ) as executor:
        results = list(executor.map(compress, compression_tasks))

    compressed_source_bytes = sum(
        result.source_bytes for result in results
    )
    compressed_payload_bytes = sum(
        result.compressed_bytes for result in results
    )
    manifest = {
        "artifact_contract_version": 1,
        "compressed_payload_bytes": compressed_payload_bytes,
        "dataset_id": dataset_id,
        "fund_payloads": directory_counts["funds"],
        "source_bytes": static_source_bytes + compressed_source_bytes,
        "source_sha": source_sha,
        "stock_payloads": directory_counts["stocks"],
        "tree_sha256": content_digest(output_root),
    }
    manifest_path = output_root / "deployment-manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest_path.chmod(0o644)
    os.utime(manifest_path, (0, 0))

    artifact_bytes = sum(path.stat().st_size for path in iter_files(output_root))
    archive_bytes = deterministic_tar_size(output_root)
    if archive_bytes >= max_archive_bytes:
        raise ValueError(
            "Pages archive is too large: "
            f"{archive_bytes:,} bytes >= {max_archive_bytes:,} bytes"
        )

    summary: dict[str, int | str] = {
        **manifest,
        "archive_bytes": archive_bytes,
        "artifact_bytes": artifact_bytes,
        "artifact_files": len(iter_files(output_root)),
        "max_archive_bytes": max_archive_bytes,
    }
    print(json.dumps(summary, sort_keys=True))
    return summary


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
    build_artifact(
        source_root=args.source_root,
        output_root=args.output,
        source_sha=args.source_sha,
        dataset_id=args.dataset_id,
        workers=args.workers,
        compresslevel=args.compress_level,
        max_archive_bytes=args.max_archive_bytes,
    )


if __name__ == "__main__":
    main()
