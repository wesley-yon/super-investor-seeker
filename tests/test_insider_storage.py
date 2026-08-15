from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

from insider_contract import InsiderContractError, canonical_insider_json_bytes
from insider_storage import (
    ImmutableInsiderStorageConflict,
    InsiderStorage,
    InsiderStorageError,
    StoredArtifact,
)
from insider_parser import MAX_RAW_XML_BYTES, parse_ownership_xml


ACCESSION = "0000000001-26-000001"
FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "insider_filings"
ORACLE = json.loads((FIXTURE_ROOT / "expectations.json").read_text())
SIMPLE_CASE = ORACLE["filings"]["form4_simple_purchase"]


def _multiprocess_store_worker(
    repository_root: str,
    mode: str,
    payload: object,
    parser_version: str | None,
    writer_name: str,
    pause_after_lock: bool,
    locked_event: Any,
    release_event: Any,
    lock_attempt_event: Any,
    done_event: Any,
    result_queue: Any,
) -> None:
    import insider_storage as storage_module

    storage = storage_module.InsiderStorage(Path(repository_root))

    def store() -> StoredArtifact:
        if mode == "raw":
            assert isinstance(payload, bytes)
            return storage.store_raw(ACCESSION, payload)
        if mode == "normalized":
            assert parser_version is not None
            return storage.store_normalized(ACCESSION, parser_version, payload)
        raise AssertionError(f"unsupported test storage mode: {mode}")

    def record_store() -> None:
        try:
            artifact = store()
            result_queue.put(
                ("stored", writer_name, artifact.created, artifact.sha256)
            )
        except ImmutableInsiderStorageConflict:
            result_queue.put(("conflict", writer_name, None, None))
        except BaseException as error:
            result_queue.put(
                ("error", writer_name, type(error).__name__, str(error))
            )
        finally:
            done_event.set()

    if not pause_after_lock:
        original_lock = storage_module._exclusive_directory_lock

        @contextmanager
        def observe_lock_attempt(directory_descriptor: int) -> Any:
            lock_attempt_event.set()
            with original_lock(directory_descriptor):
                yield

        with patch.object(
            storage_module,
            "_exclusive_directory_lock",
            side_effect=observe_lock_attempt,
        ):
            record_store()
        return

    original_cleanup = storage_module._remove_safe_stale_temporary_artifacts

    def pause_with_lock_held(
        directory_descriptor: int,
        target_name: str,
    ) -> None:
        locked_event.set()
        if not release_event.wait(10):
            raise TimeoutError("test did not release the storage directory lock")
        original_cleanup(directory_descriptor, target_name)

    with patch.object(
        storage_module,
        "_remove_safe_stale_temporary_artifacts",
        side_effect=pause_with_lock_held,
    ):
        record_store()


class InsiderStorageTests(unittest.TestCase):
    @staticmethod
    def normalized_payload(
        raw_xml: bytes,
        parser_version: str,
        case: dict | None = None,
    ) -> dict:
        case = SIMPLE_CASE if case is None else case
        payload = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )
        payload["parser_version"] = parser_version
        return payload

    def run_multiprocess_pair(
        self,
        repository_root: Path,
        *,
        mode: str,
        first_payload: object,
        second_payload: object,
        parser_version: str | None = None,
    ) -> list[tuple[object, ...]]:
        context = multiprocessing.get_context("spawn")
        locked_event = context.Event()
        release_event = context.Event()
        first_done = context.Event()
        second_done = context.Event()
        first_lock_attempt = context.Event()
        second_lock_attempt = context.Event()
        result_queue = context.Queue()
        common = (
            str(repository_root),
            mode,
            parser_version,
            locked_event,
            release_event,
            result_queue,
        )
        first = context.Process(
            target=_multiprocess_store_worker,
            args=(
                common[0],
                common[1],
                first_payload,
                common[2],
                "first",
                True,
                common[3],
                common[4],
                first_lock_attempt,
                first_done,
                common[5],
            ),
        )
        second = context.Process(
            target=_multiprocess_store_worker,
            args=(
                common[0],
                common[1],
                second_payload,
                common[2],
                "second",
                False,
                common[3],
                common[4],
                second_lock_attempt,
                second_done,
                common[5],
            ),
        )
        processes = [first]
        first.start()
        try:
            self.assertTrue(
                locked_event.wait(10),
                "first writer never reached the locked publication section",
            )
            self.assertFalse(first_done.is_set())
            second.start()
            processes.append(second)
            self.assertTrue(
                second_lock_attempt.wait(10),
                "second writer never attempted to acquire the directory lock",
            )
            self.assertFalse(
                second_done.wait(0.25),
                "second process completed while the first held the directory lock",
            )
        finally:
            release_event.set()
            for process in processes:
                process.join(10)
                if process.is_alive():
                    process.terminate()
                    process.join(5)

        for process in processes:
            self.assertEqual(0, process.exitcode)
        results = [result_queue.get(timeout=3) for _process in processes]
        result_queue.close()
        result_queue.join_thread()
        self.assertNotIn("error", {result[0] for result in results})
        return sorted(results, key=lambda result: str(result[1]))

    def test_raw_write_is_immutable_and_idempotent_by_accession(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            private_root = repository_root / "data/insiders/private"
            storage = InsiderStorage(repository_root)
            self.assertFalse(private_root.exists())
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()

            first = storage.store_raw(ACCESSION, raw_xml)
            second = storage.store_raw(ACCESSION, raw_xml)

            self.assertTrue(first.created)
            self.assertFalse(second.created)
            self.assertEqual(first.path, second.path)
            self.assertEqual(hashlib.sha256(raw_xml).hexdigest(), first.sha256)
            self.assertEqual(len(raw_xml), first.byte_count)
            self.assertEqual(raw_xml, first.path.read_bytes())
            self.assertEqual(0o600, first.path.stat().st_mode & 0o777)
            self.assertEqual(
                0o700,
                first.path.parent.stat().st_mode & 0o777,
            )
            with self.assertRaises(ImmutableInsiderStorageConflict):
                storage.store_raw(ACCESSION, raw_xml + b"CONFLICT")
            self.assertEqual(raw_xml, first.path.read_bytes())

    def test_raw_write_rejects_parser_oversize_before_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)

            with self.assertRaisesRegex(InsiderStorageError, "size limit"):
                storage.store_raw(ACCESSION, b"X" * (MAX_RAW_XML_BYTES + 1))

            self.assertFalse((repository_root / "data").exists())

    def test_raw_idempotency_rejects_oversized_existing_raw_before_reading_it(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            stored = storage.store_raw(ACCESSION, raw_xml)
            stored.path.write_bytes(raw_xml + b"X")

            with patch(
                "insider_storage.MAX_RAW_XML_BYTES",
                len(raw_xml),
            ), patch(
                "insider_storage.os.read",
                side_effect=AssertionError("oversized raw.xml was read"),
            ):
                with self.assertRaisesRegex(InsiderStorageError, "size limit"):
                    storage.store_raw(ACCESSION, raw_xml)

    def test_raw_write_removes_safe_stale_implementation_temp_before_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            payload = b"<ownershipDocument>SYNTHETIC</ownershipDocument>"
            initial = storage.store_raw(ACCESSION, payload)
            stale_temp = initial.path.parent / f".{initial.path.name}.tmp-{'a' * 24}"
            initial.path.unlink()
            stale_temp.write_bytes(b"incomplete synthetic write")
            stale_temp.chmod(0o600)

            created = storage.store_raw(ACCESSION, payload)
            retry = storage.store_raw(ACCESSION, payload)

            self.assertTrue(created.created)
            self.assertFalse(retry.created)
            self.assertEqual(payload, created.path.read_bytes())
            self.assertFalse(stale_temp.exists())

    def test_raw_write_rejects_unsafe_matching_stale_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            payload = b"<ownershipDocument>SYNTHETIC</ownershipDocument>"
            stored = storage.store_raw(ACCESSION, payload)
            outside = repository_root / "synthetic-outside.xml"
            outside.write_bytes(b"must remain outside private storage")
            stale_temp = stored.path.parent / f".{stored.path.name}.tmp-{'b' * 24}"
            stale_temp.symlink_to(outside)

            with self.assertRaisesRegex(
                InsiderStorageError,
                "temporary artifact is unsafe",
            ):
                storage.store_raw(ACCESSION, payload)

            self.assertEqual(payload, stored.path.read_bytes())
            self.assertEqual(b"must remain outside private storage", outside.read_bytes())
            self.assertTrue(stale_temp.is_symlink())

    def test_raw_write_rejects_matching_stale_temp_with_unsafe_permissions(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            payload = b"<ownershipDocument>SYNTHETIC</ownershipDocument>"
            stored = storage.store_raw(ACCESSION, payload)
            stale_temp = stored.path.parent / f".{stored.path.name}.tmp-{'e' * 24}"
            stale_temp.write_bytes(b"incomplete synthetic write")
            stale_temp.chmod(0o640)

            with self.assertRaisesRegex(
                InsiderStorageError,
                "temporary artifact is unsafe",
            ):
                storage.store_raw(ACCESSION, payload)

            self.assertEqual(0o640, stale_temp.stat().st_mode & 0o777)

    def test_raw_write_rejects_hardlinked_matching_stale_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            payload = b"<ownershipDocument>SYNTHETIC</ownershipDocument>"
            stored = storage.store_raw(ACCESSION, payload)
            source = stored.path.parent / "separate-synthetic-temp-source"
            source.write_bytes(b"incomplete synthetic write")
            source.chmod(0o600)
            stale_temp = stored.path.parent / f".{stored.path.name}.tmp-{'f' * 24}"
            os.link(source, stale_temp)

            with self.assertRaisesRegex(
                InsiderStorageError,
                "temporary artifact is unsafe",
            ):
                storage.store_raw(ACCESSION, payload)

            self.assertEqual(2, stale_temp.stat().st_nlink)
            self.assertEqual(b"incomplete synthetic write", source.read_bytes())

    def test_raw_write_preserves_unrelated_dotfiles_and_other_target_temps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            payload = b"<ownershipDocument>SYNTHETIC</ownershipDocument>"
            initial = storage.store_raw(ACCESSION, payload)
            initial.path.unlink()
            unrelated_dotfile = initial.path.parent / ".unrelated"
            unrelated_temp = initial.path.parent / f".other.xml.tmp-{'c' * 24}"
            unrelated_dotfile.write_bytes(b"unrelated dotfile")
            unrelated_temp.write_bytes(b"other target incomplete write")

            created = storage.store_raw(ACCESSION, payload)

            self.assertTrue(created.created)
            self.assertEqual(b"unrelated dotfile", unrelated_dotfile.read_bytes())
            self.assertEqual(
                b"other target incomplete write",
                unrelated_temp.read_bytes(),
            )

    def test_invalid_raw_payload_has_no_storage_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)

            with self.assertRaisesRegex(
                InsiderStorageError,
                "payload must be bytes",
            ):
                storage.store_raw(ACCESSION, b"")

            self.assertFalse((repository_root / "data").exists())

    def test_normalized_reparses_are_immutable_and_parser_versioned(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            raw = storage.store_raw(ACCESSION, raw_xml)
            version_one = self.normalized_payload(raw_xml, "1.0.0")

            first = storage.store_normalized(ACCESSION, "1.0.0", version_one)
            retry = storage.store_normalized(ACCESSION, "1.0.0", version_one)

            self.assertTrue(first.created)
            self.assertFalse(retry.created)
            self.assertEqual(first.path, retry.path)
            self.assertEqual(0o600, first.path.stat().st_mode & 0o777)
            self.assertEqual(0o700, first.path.parent.stat().st_mode & 0o777)
            self.assertEqual(
                version_one,
                json.loads(first.path.read_bytes()),
            )
            conflicting = json.loads(json.dumps(version_one))
            conflicting["accepted_at"] = "2026-01-16T16:31:00Z"
            conflicting_source = conflicting["source"]["field_sources"][
                "accepted_at"
            ]
            conflicting_source["raw_value"] = conflicting["accepted_at"]
            conflicting_source["normalized_value"] = conflicting["accepted_at"]
            with self.assertRaises(ImmutableInsiderStorageConflict):
                storage.store_normalized(ACCESSION, "1.0.0", conflicting)

            version_two = self.normalized_payload(raw_xml, "1.1.0")
            second_version = storage.store_normalized(
                ACCESSION,
                "1.1.0",
                version_two,
            )
            self.assertTrue(second_version.created)
            self.assertNotEqual(first.path, second_version.path)
            self.assertEqual(raw_xml, raw.path.read_bytes())
            self.assertEqual(
                {"1.0.0.json", "1.1.0.json"},
                {
                    path.name
                    for path in first.path.parent.iterdir()
                    if path.is_file()
                },
            )
            with self.assertRaisesRegex(
                InsiderStorageError,
                "parser version",
            ):
                storage.store_normalized(ACCESSION, "../escape", version_one)

    def test_normalized_write_rejects_self_consistent_malformed_timestamp(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            raw = storage.store_raw(ACCESSION, raw_xml)
            payload = self.normalized_payload(raw_xml, "1.0.0")
            payload["accepted_at"] = "2026-02-30T25:61:61Z"
            source = payload["source"]["field_sources"]["accepted_at"]
            source["raw_value"] = payload["accepted_at"]
            source["normalized_value"] = payload["accepted_at"]

            with self.assertRaisesRegex(
                InsiderContractError,
                "ISO timestamp",
            ):
                storage.store_normalized(ACCESSION, "1.0.0", payload)

            self.assertFalse((raw.path.parent / "normalized").exists())

    def test_normalized_write_rejects_oversize_before_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            raw = storage.store_raw(ACCESSION, raw_xml)
            payload = self.normalized_payload(raw_xml, "1.0.0")
            rendered = canonical_insider_json_bytes(payload)

            with patch(
                "insider_storage.MAX_NORMALIZED_JSON_BYTES",
                len(rendered) - 1,
                create=True,
            ):
                with self.assertRaisesRegex(InsiderStorageError, "size limit"):
                    storage.store_normalized(ACCESSION, "1.0.0", payload)

            self.assertFalse((raw.path.parent / "normalized").exists())

    def test_normalized_write_rejects_oversized_existing_raw_before_reading_it(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            stored = storage.store_raw(ACCESSION, raw_xml)
            stored.path.write_bytes(raw_xml + b"X")
            payload = self.normalized_payload(raw_xml, "1.0.0")

            with patch(
                "insider_storage.MAX_RAW_XML_BYTES",
                len(raw_xml),
            ), patch(
                "insider_storage.os.read",
                side_effect=AssertionError("oversized raw.xml was read"),
            ):
                with self.assertRaisesRegex(InsiderStorageError, "size limit"):
                    storage.store_normalized(ACCESSION, "1.0.0", payload)

    def test_normalized_idempotency_rejects_oversized_existing_json_before_reading_it(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            storage.store_raw(ACCESSION, raw_xml)
            payload = self.normalized_payload(raw_xml, "1.0.0")
            stored = storage.store_normalized(ACCESSION, "1.0.0", payload)
            rendered = stored.path.read_bytes()
            stored.path.write_bytes(rendered + b"X")
            real_read = os.read
            target_inode = stored.path.stat().st_ino

            def reject_normalized_read(descriptor: int, size: int) -> bytes:
                if os.fstat(descriptor).st_ino == target_inode:
                    raise AssertionError("oversized normalized artifact was read")
                return real_read(descriptor, size)

            with patch(
                "insider_storage.MAX_NORMALIZED_JSON_BYTES",
                len(rendered),
            ), patch("insider_storage.os.read", side_effect=reject_normalized_read):
                with self.assertRaisesRegex(InsiderStorageError, "size limit"):
                    storage.store_normalized(ACCESSION, "1.0.0", payload)

    def test_normalized_write_removes_safe_stale_implementation_temp_before_retry(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            storage.store_raw(ACCESSION, raw_xml)
            payload = self.normalized_payload(raw_xml, "1.0.0")
            rendered = b'{"synthetic":"normalized"}\n'
            with patch(
                "insider_storage.canonical_insider_json_bytes",
                return_value=rendered,
            ):
                initial = storage.store_normalized(ACCESSION, "1.0.0", payload)
                stale_temp = initial.path.parent / (
                    f".{initial.path.name}.tmp-{'d' * 24}"
                )
                initial.path.unlink()
                stale_temp.write_bytes(b"incomplete synthetic normalization")
                stale_temp.chmod(0o600)

                created = storage.store_normalized(ACCESSION, "1.0.0", payload)
                retry = storage.store_normalized(ACCESSION, "1.0.0", payload)

            self.assertTrue(created.created)
            self.assertFalse(retry.created)
            self.assertEqual(rendered, created.path.read_bytes())
            self.assertFalse(stale_temp.exists())

    def test_original_and_amendment_remain_separate_accessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            original_case = SIMPLE_CASE
            amendment_case = ORACLE["filings"]["form4_amendment"]
            stored = []

            for case in (original_case, amendment_case):
                raw_xml = (FIXTURE_ROOT / case["filename"]).read_bytes()
                raw = storage.store_raw(case["accession_number"], raw_xml)
                normalized = storage.store_normalized(
                    case["accession_number"],
                    "1.0.0",
                    self.normalized_payload(raw_xml, "1.0.0", case),
                )
                stored.append((raw, normalized))

            self.assertNotEqual(stored[0][0].path, stored[1][0].path)
            self.assertNotEqual(stored[0][1].path, stored[1][1].path)
            self.assertEqual(
                {original_case["accession_number"], amendment_case["accession_number"]},
                {artifact.path.parent.name for artifact, _ in stored},
            )
            self.assertEqual(
                ["4", "4/A"],
                [
                    json.loads(normalized.path.read_bytes())["form_type"]
                    for _, normalized in stored
                ],
            )

    def test_parallel_raw_writers_are_idempotent_or_fail_closed(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            barrier = threading.Barrier(8)

            def write_same() -> object:
                barrier.wait()
                return storage.store_raw(ACCESSION, raw_xml)

            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(lambda _index: write_same(), range(8)))

            self.assertEqual(1, sum(result.created for result in results))
            self.assertEqual(1, len({result.path for result in results}))

        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            barrier = threading.Barrier(2)

            def write_distinct(payload: bytes) -> str:
                barrier.wait()
                try:
                    storage.store_raw(ACCESSION, payload)
                except ImmutableInsiderStorageConflict:
                    return "conflict"
                return "created"

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = list(
                    executor.map(
                        write_distinct,
                        (raw_xml, raw_xml + b"SYNTHETIC CONFLICT"),
                    )
                )

            self.assertCountEqual(["created", "conflict"], outcomes)

    def test_multiprocess_writers_are_serialized_for_raw_and_normalized_data(
        self,
    ) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()

        with tempfile.TemporaryDirectory() as tmpdir:
            same_results = self.run_multiprocess_pair(
                Path(tmpdir),
                mode="raw",
                first_payload=raw_xml,
                second_payload=raw_xml,
            )
            self.assertCountEqual(
                [("stored", True), ("stored", False)],
                [(result[0], result[2]) for result in same_results],
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            conflict_results = self.run_multiprocess_pair(
                Path(tmpdir),
                mode="raw",
                first_payload=raw_xml,
                second_payload=raw_xml + b"SYNTHETIC CONFLICT",
            )
            self.assertEqual("stored", conflict_results[0][0])
            self.assertTrue(conflict_results[0][2])
            self.assertEqual("conflict", conflict_results[1][0])

        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            InsiderStorage(repository_root).store_raw(ACCESSION, raw_xml)
            normalized = self.normalized_payload(raw_xml, "1.0.0")
            normalized_results = self.run_multiprocess_pair(
                repository_root,
                mode="normalized",
                first_payload=normalized,
                second_payload=normalized,
                parser_version="1.0.0",
            )
            self.assertCountEqual(
                [("stored", True), ("stored", False)],
                [(result[0], result[2]) for result in normalized_results],
            )

    def test_publication_never_exposes_a_hardlinked_final_artifact(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()

        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            with patch("insider_storage.os.link") as hard_link:
                stored = storage.store_raw(ACCESSION, raw_xml)

            self.assertEqual(raw_xml, stored.path.read_bytes())
            hard_link.assert_not_called()

    def test_publication_does_not_overwrite_a_racing_target(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        competing_payload = b"SYNTHETIC COMPETING WRITER"

        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            real_existing_artifact = storage._existing_artifact
            existence_checks = 0

            def inject_competing_target(
                directory_descriptor: int,
                target_name: str,
                target: Path,
                payload: bytes,
                max_bytes: int,
            ) -> object:
                nonlocal existence_checks
                existence_checks += 1
                try:
                    return real_existing_artifact(
                        directory_descriptor,
                        target_name,
                        target,
                        payload,
                        max_bytes,
                    )
                except FileNotFoundError:
                    if existence_checks == 2:
                        descriptor = os.open(
                            target_name,
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                            0o600,
                            dir_fd=directory_descriptor,
                        )
                        try:
                            os.write(descriptor, competing_payload)
                            os.fsync(descriptor)
                        finally:
                            os.close(descriptor)
                        os.fsync(directory_descriptor)
                    raise

            with patch.object(
                storage,
                "_existing_artifact",
                side_effect=inject_competing_target,
            ):
                with self.assertRaises(ImmutableInsiderStorageConflict):
                    storage.store_raw(ACCESSION, raw_xml)

            target = (
                repository_root
                / "data/insiders/private/accessions"
                / ACCESSION
                / "raw.xml"
            )
            self.assertEqual(competing_payload, target.read_bytes())
            self.assertEqual(
                ["raw.xml"],
                sorted(path.name for path in target.parent.iterdir()),
            )

    def test_storage_rejects_traversal_missing_raw_and_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            payload = self.normalized_payload(raw_xml, "1.0.0")

            with self.assertRaisesRegex(
                InsiderStorageError,
                "accession number",
            ):
                storage.store_raw("../../escape", raw_xml)
            self.assertFalse((repository_root / "data").exists())
            with self.assertRaisesRegex(
                InsiderStorageError,
                "must be stored before normalization",
            ):
                storage.store_normalized(ACCESSION, "1.0.0", payload)

            storage.store_raw(ACCESSION, raw_xml)
            mismatched = dict(payload)
            mismatched["raw_sha256"] = "0" * 64
            with self.assertRaisesRegex(
                InsiderStorageError,
                "raw SHA-256",
            ):
                storage.store_normalized(ACCESSION, "1.0.0", mismatched)

    def test_normalized_payload_must_match_the_stored_raw_xml(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            altered_xml = raw_xml.replace(
                b"Class A Common Stock",
                b"Class B Common Stock",
                1,
            )
            self.assertNotEqual(raw_xml, altered_xml)
            storage.store_raw(ACCESSION, raw_xml)
            payload = self.normalized_payload(altered_xml, "1.0.0")
            payload["raw_sha256"] = hashlib.sha256(raw_xml).hexdigest()

            with self.assertRaisesRegex(
                InsiderStorageError,
                "raw document does not match stored XML",
            ):
                storage.store_normalized(ACCESSION, "1.0.0", payload)

    def test_storage_rejects_symlink_raw_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            accession_directory = (
                repository_root
                / "data/insiders/private/accessions"
                / ACCESSION
            )
            accession_directory.mkdir(parents=True)
            for directory in (
                repository_root / "data/insiders/private",
                repository_root / "data/insiders/private/accessions",
                accession_directory,
            ):
                directory.chmod(0o700)
            outside = repository_root / "synthetic-outside.xml"
            outside.write_bytes(b"SYNTHETIC OUTSIDE TEST ONLY")
            (accession_directory / "raw.xml").symlink_to(outside)

            with self.assertRaisesRegex(
                InsiderStorageError,
                "target is unsafe",
            ):
                storage.store_raw(
                    ACCESSION,
                    b"<ownershipDocument>SYNTHETIC</ownershipDocument>",
                )
            self.assertEqual(
                b"SYNTHETIC OUTSIDE TEST ONLY",
                outside.read_bytes(),
            )

    def test_storage_rejects_preexisting_hardlinked_raw_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            accession_directory = (
                repository_root
                / "data/insiders/private/accessions"
                / ACCESSION
            )
            accession_directory.mkdir(parents=True)
            for directory in (
                repository_root / "data/insiders/private",
                repository_root / "data/insiders/private/accessions",
                accession_directory,
            ):
                directory.chmod(0o700)
            raw_xml = b"<ownershipDocument>SYNTHETIC</ownershipDocument>"
            outside = repository_root / "synthetic-outside.xml"
            outside.write_bytes(raw_xml)
            os.link(outside, accession_directory / "raw.xml")

            with self.assertRaisesRegex(
                InsiderStorageError,
                "hard-linked",
            ):
                storage.store_raw(ACCESSION, raw_xml)

            self.assertEqual(raw_xml, outside.read_bytes())

    def test_storage_rejects_preexisting_hardlinked_normalized_target(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            storage.store_raw(ACCESSION, raw_xml)
            payload = self.normalized_payload(raw_xml, "1.0.0")
            normalized_directory = (
                repository_root
                / "data/insiders/private/accessions"
                / ACCESSION
                / "normalized"
            )
            normalized_directory.mkdir(mode=0o700)
            outside = repository_root / "synthetic-outside.json"
            outside.write_bytes(canonical_insider_json_bytes(payload))
            outside.chmod(0o600)
            os.link(outside, normalized_directory / "1.0.0.json")

            with self.assertRaisesRegex(
                InsiderStorageError,
                "hard-linked",
            ):
                storage.store_normalized(ACCESSION, "1.0.0", payload)

    def test_storage_rejects_group_or_world_readable_private_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            raw_xml = b"<ownershipDocument>SYNTHETIC</ownershipDocument>"
            stored = storage.store_raw(ACCESSION, raw_xml)
            stored.path.chmod(0o644)

            with self.assertRaisesRegex(
                InsiderStorageError,
                "permissions",
            ):
                storage.store_raw(ACCESSION, raw_xml)

    def test_normalized_write_rejects_intermediate_private_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            outside_private = repository_root / "outside-private"
            outside_accession = outside_private / "accessions" / ACCESSION
            outside_accession.mkdir(parents=True)
            (outside_accession / "raw.xml").write_bytes(raw_xml)
            (repository_root / "data/insiders").mkdir(parents=True)
            (repository_root / "data/insiders/private").symlink_to(
                outside_private,
                target_is_directory=True,
            )
            storage = InsiderStorage(repository_root)
            payload = self.normalized_payload(raw_xml, "1.0.0")

            with self.assertRaisesRegex(
                InsiderStorageError,
                "unsafe",
            ):
                storage.store_normalized(ACCESSION, "1.0.0", payload)

            self.assertFalse((outside_accession / "normalized").exists())


if __name__ == "__main__":
    unittest.main()
