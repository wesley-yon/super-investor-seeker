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
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any
from unittest.mock import patch

from insider_contract import InsiderContractError, canonical_insider_json_bytes
from insider_pipeline import (
    InsiderStateStore as PipelineInsiderStateStore,
    build_insider_source_metadata,
    canonical_source_metadata_json_bytes,
    parse_insider_filing_index,
)
from insider_storage import (
    ImmutableInsiderStorageConflict,
    InsiderApprovalScopeError,
    InsiderStateStore,
    InsiderStorage,
    InsiderStorageError,
    StoredArtifact,
    canonical_insider_state_json_bytes,
)
from insider_parser import MAX_RAW_XML_BYTES, parse_ownership_xml
from security_identity import section16_owner_group_key, section16_security_class_key


ACCESSION = "0000000001-26-000001"
FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "insider_filings"
ORACLE = json.loads((FIXTURE_ROOT / "expectations.json").read_text())
SIMPLE_CASE = ORACLE["filings"]["form4_simple_purchase"]


def filing_index_url(
    accession: str = ACCESSION,
    issuer_cik: str = "0000000001",
    *,
    explicit_port: bool = False,
) -> str:
    authority = "www.sec.gov:443" if explicit_port else "www.sec.gov"
    archive_cik = str(int(issuer_cik))
    compact_accession = accession.replace("-", "")
    return (
        f"https://{authority}/Archives/edgar/data/{archive_cik}/"
        f"{compact_accession}/{accession}-index.html"
    )


def accession_quarantine_identity(
    accession: str = ACCESSION,
    issuer_cik: str = "0000000001",
) -> dict[str, object]:
    return {
        "index_url": filing_index_url(accession, issuer_cik),
        "accepted_at": "2026-01-16T16:30:00Z",
        "reporting_owner_ciks": ["0000000002"],
    }


def incremental_queue_entry(entry: dict[str, object]) -> dict[str, object]:
    return {"form_type": "4", **entry}


def issuer_source_entry(entry: dict[str, object]) -> dict[str, object]:
    return {
        "accession_number": entry["accession_number"],
        "form_type": entry["form_type"],
        "entity_role": "issuer",
        "entity_cik": entry["issuer_cik"],
        "entry_url": entry["index_url"],
        "accepted_at": entry["accepted_at"],
        "observed_at": entry["observed_at"],
    }


def issuer_generation_digest(
    accessions: list[dict[str, object]],
    amendments: list[dict[str, object]],
) -> str:
    resolution_by_accession = {
        amendment["accession_number"]: {
            "effective_accession": amendment["effective_accession"],
            "confidence": amendment["confidence"],
            "reason_code": amendment["reason_code"],
            "candidates": amendment["candidates"],
        }
        for amendment in amendments
    }
    material = [
        {
            **accession,
            "amendment_resolution": resolution_by_accession.get(
                accession["accession_number"]
            ),
        }
        for accession in accessions
    ]
    return hashlib.sha256(
        b"section16-issuer-generation-v1\0"
        + canonical_insider_state_json_bytes(material)
    ).hexdigest()


def issuer_state_payload(
    issuer_cik: str = "0000000001",
    *,
    generation_digest: str | None = None,
) -> dict[str, object]:
    accessions: list[dict[str, object]] = []
    amendments: list[dict[str, object]] = []
    return {
        "contract_version": 1,
        "issuer_cik": issuer_cik,
        "accessions": accessions,
        "owner_groups": [],
        "security_classes": [],
        "amendments": amendments,
        "unresolved_ambiguities": [],
        "generation_digest": (
            issuer_generation_digest(accessions, amendments)
            if generation_digest is None
            else generation_digest
        ),
    }


def backfill_state_payload(
    quarter: str = "2026Q1",
    issuer_cik: str = "0000000001",
) -> dict[str, object]:
    return {
        "contract_version": 1,
        "quarter": quarter,
        "issuer_cik": issuer_cik,
        "status": "incomplete",
        "catalog_url": None,
        "zip_url": None,
        "zip_sha256": None,
        "zip_byte_count": None,
        "etag": None,
        "last_modified": None,
        "table_evidence": [],
        "missing_optional_tables": [],
        "selected_accessions": [],
        "completed_accessions": [],
        "reconciliation": [],
    }


def store_backfill_state_for_test(
    state: InsiderStateStore,
    payload: object,
    *,
    quarter: str = "2026Q1",
    issuer_cik: str = "0000000001",
    expected_sha256: str | None = None,
) -> StoredArtifact:
    state.write(
        "approved-issuers-v1",
        {"contract_version": 1, "issuer_ciks": [issuer_cik]},
    )
    return state.write_backfill_if_issuer_approved(
        quarter,
        issuer_cik,
        payload,
        expected_sha256=expected_sha256,
    )


def store_reparse_state_for_test(
    state: InsiderStateStore,
    payload: object,
    *,
    approved_issuer_ciks: tuple[str, ...] = ("0000000001",),
    expected_sha256: str | None = None,
) -> StoredArtifact:
    state.write(
        "approved-issuers-v1",
        {
            "contract_version": 1,
            "issuer_ciks": sorted(approved_issuer_ciks),
        },
    )
    return state.write_reparse_if_issuers_approved(
        payload,
        expected_sha256=expected_sha256,
    )


def with_valid_issuer_generation_digest(
    payload: dict[str, object],
) -> dict[str, object]:
    result = dict(payload)
    accessions = result["accessions"]
    amendments = result["amendments"]
    assert isinstance(accessions, list) and isinstance(amendments, list)
    result["generation_digest"] = issuer_generation_digest(accessions, amendments)
    return result


def telemetry_example(
    accession_number: str = ACCESSION,
    **overrides: object,
) -> dict[str, object]:
    result: dict[str, object] = {
        "accession_number": accession_number,
        "issuer_cik": "0000000001",
        "form_type": "4",
        "parser_version": "1.0.0",
        "stage": "normalized",
        "outcome": "created",
        "error_class": None,
        "reason_code": None,
        "retry_count": 0,
        "next_retry_at": None,
    }
    result.update(overrides)
    return result


def telemetry_run(
    run_id: str,
    *,
    status: str = "completed",
    started_at: str = "2026-01-01T00:00:00Z",
    finished_at: str | None = "2026-01-01T00:00:01Z",
    counters: dict[str, int] | None = None,
    accession_examples: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "counters": {} if counters is None else counters,
        "accession_examples": [] if accession_examples is None else accession_examples,
    }


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
        if mode == "index":
            assert isinstance(payload, bytes)
            return storage.store_index_html(ACCESSION, payload)
        if mode == "metadata":
            return storage.store_source_metadata(ACCESSION, payload)
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


def store_source_prerequisites(
    storage: InsiderStorage, accession: str, raw_xml: bytes, case: dict
) -> None:
    storage.store_raw(accession, raw_xml)
    parsed_raw = parse_ownership_xml(
        raw_xml,
        accession_number=accession,
        filing_date=case["filing_date"],
        accepted_at=case["accepted_at"],
        source_index_url=case["source_index_url"],
        source_document_url=case["source_document_url"],
    )
    form = parsed_raw["form_type"]
    filename = case["source_document_url"].rsplit("/", 1)[-1]
    accepted_eastern = datetime.fromisoformat(
        case["accepted_at"].replace("Z", "+00:00")
    ).astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
    index_html = (
        "<!doctype html><html><body>"
        f"<div id=\"formName\"><strong>Form {form}</strong></div>"
        f"<div class=\"infoHead\">Filing Date</div><div class=\"info\">{case['filing_date']}</div>"
        f"<div class=\"infoHead\">Accepted</div><div class=\"info\">{accepted_eastern}</div>"
        "<table class=\"tableFile\" summary=\"Document Format Files\"><tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>"
        f"<tr><td>1</td><td>Form {form}</td><td><a href=\"{case['source_document_url']}\">{filename}</a></td><td>{form}</td><td>{len(raw_xml)}</td></tr>"
        "</table></body></html>"
    ).encode()
    index_metadata = parse_insider_filing_index(
        index_html,
        index_url=case["source_index_url"],
        accession_number=accession,
        issuer_cik=parsed_raw["issuer"]["cik"],
        reporting_owner_ciks=tuple(owner["cik"] for owner in parsed_raw["owners"]),
    )
    storage.store_index_html(accession, index_html)
    storage.store_source_metadata(
        accession, build_insider_source_metadata(index_metadata, index_html, raw_xml)
    )


def _state_update_worker(
    repository_root: str,
    accession: str,
    hold_lock: bool,
    locked_event: Any,
    release_event: Any,
    attempted_event: Any,
    done_event: Any,
    result_queue: Any,
) -> None:
    import insider_storage as storage_module

    store = storage_module.InsiderStateStore(Path(repository_root))

    def transform(payload: dict[str, object]) -> dict[str, object]:
        if hold_lock:
            locked_event.set()
            if not release_event.wait(10):
                raise TimeoutError("test did not release state lock")
        completed = payload["completed_accessions"]
        assert isinstance(completed, list)
        result = dict(payload)
        entry = incremental_queue_entry(
            {
                "accession_number": accession,
                "issuer_cik": "0000000001",
                "index_url": filing_index_url(accession),
                "accepted_at": "2026-01-01T00:00:00Z",
                "observed_at": "2026-01-01T00:00:00Z",
            }
        )
        source_entry = issuer_source_entry(entry)
        result["queue"] = sorted(list(payload["queue"]) + [entry], key=lambda item: (item["accepted_at"], item["accession_number"]))
        result["source_entries"] = sorted(list(payload["source_entries"]) + [source_entry], key=lambda item: (item["accepted_at"], item["accession_number"], item["entity_role"], item["entity_cik"], item["entry_url"], item["observed_at"]))
        observed = [item["observed_at"] for item in result["source_entries"]]
        result["first_observed_at"] = min(observed)
        result["last_observed_at"] = max(observed)
        result["completed_accessions"] = sorted(completed + [accession])
        return result

    original_lock = storage_module._exclusive_directory_lock

    @contextmanager
    def signal_before_flock(directory_descriptor: int) -> Any:
        attempted_event.set()
        with original_lock(directory_descriptor):
            yield

    try:
        with patch.object(storage_module, "_exclusive_directory_lock", side_effect=signal_before_flock):
            store.update("incremental-v1", transform)
        result_queue.put("ok")
    except BaseException as error:
        result_queue.put(f"{type(error).__name__}:{error}")
    finally:
        done_event.set()


def _fifo_state_worker(repository_root: str, action: str, result_queue: Any) -> None:
    try:
        store = InsiderStateStore(Path(repository_root))
        if action == "read":
            store.read("incremental-v1")
        else:
            store.write("incremental-v1", {
                "contract_version": 1, "status": "incomplete", "lookback_seconds": 3600,
                "first_observed_at": None, "last_observed_at": None, "queue": [],
                "completed_accessions": [], "source_entries": [],
            })
    except BaseException as error:
        result_queue.put(type(error).__name__)
    else:
        result_queue.put("ok")


def _reentrant_state_worker(
    repository_root: str,
    action: str,
    result_queue: Any,
) -> None:
    payload = {
        "contract_version": 1,
        "status": "incomplete",
        "lookback_seconds": 3600,
        "first_observed_at": None,
        "last_observed_at": None,
        "queue": [],
        "completed_accessions": [],
        "source_entries": [],
    }
    try:
        store = InsiderStateStore(Path(repository_root))
        store.write(
            "approved-issuers-v1",
            {"contract_version": 1, "issuer_ciks": ["0000000001"]},
        )
        store.write("incremental-v1", payload)

        def transform(current: dict[str, object]) -> dict[str, object]:
            nested = InsiderStateStore(Path(repository_root))
            try:
                if action == "read":
                    nested.read("incremental-v1")
                elif action == "write":
                    nested.write("incremental-v1", current)
                elif action == "update":
                    nested.update("incremental-v1", lambda candidate: candidate)
                elif action == "write_incremental_if_issuers_approved":
                    nested.write_incremental_if_issuers_approved(current)
                elif action == "update_incremental_if_issuers_approved":
                    nested.update_incremental_if_issuers_approved(
                        lambda candidate: candidate
                    )
                elif action == "publish_if_issuer_approved":
                    nested.publish_if_issuer_approved(
                        "0000000001",
                        lambda: None,
                    )
                elif action == "write_accession_quarantine_if_issuer_approved":
                    nested.write_accession_quarantine_if_issuer_approved(
                        ACCESSION,
                        "0000000001",
                        {
                            "contract_version": 1,
                            "stage": "raw",
                            "error_class": "InsiderParseError",
                            "reason_code": "raw_invalid",
                            "retry_count": 0,
                            "next_retry_at": None,
                            "parser_version": "1.0.0",
                            "source_hashes": [],
                            "accession_number": ACCESSION,
                            "issuer_cik": "0000000001",
                            "form_type": "4",
                            **accession_quarantine_identity(),
                        },
                    )
                elif action == "write_issuer_if_approved":
                    nested.write_issuer_if_approved(
                        "0000000001",
                        issuer_state_payload(),
                    )
                else:
                    raise AssertionError(f"unsupported nested state action: {action}")
            except BaseException as error:
                result_queue.put(("nested_error", type(error).__name__, str(error)))
            else:
                result_queue.put(("nested_accepted", None, None))
            return current

        store.update("incremental-v1", transform)
        result_queue.put(("usable", store.read("incremental-v1") == payload))
    except BaseException as error:
        result_queue.put(("outer_error", type(error).__name__, str(error)))


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

    def test_nested_f345_document_path_round_trips_through_source_and_normalized_storage(
        self,
    ) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        nested_case = dict(SIMPLE_CASE)
        document_directory, document_filename = SIMPLE_CASE["source_document_url"].rsplit(
            "/", 1
        )
        nested_url = f"{document_directory}/xslF345X05/{document_filename}"
        nested_case["source_document_url"] = nested_url

        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            store_source_prerequisites(storage, ACCESSION, raw_xml, nested_case)
            payload = self.normalized_payload(raw_xml, "1.0.0", nested_case)
            stored = storage.store_normalized(ACCESSION, "1.0.0", payload)

            self.assertTrue(stored.created)
            metadata = storage.read_source_metadata(ACCESSION)
            document = metadata["document"]
            assert isinstance(document, dict)
            self.assertEqual(nested_url, document["url"])
            normalized = storage.read_normalized(ACCESSION, "1.0.0")
            source = normalized["source"]
            assert isinstance(source, dict)
            self.assertEqual(nested_url, source["document_url"])

    def run_multiprocess_pair(
        self: unittest.TestCase,
        repository_root: Path,
        *,
        mode: str,
        first_payload: object,
        second_payload: object,
        parser_version: str | None = None,
        allow_error: bool = False,
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
        if not allow_error:
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

    def test_normalization_rederives_source_bindings_after_canonical_metadata_replacement(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            storage.store_raw(ACCESSION, raw_xml)
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
            metadata_path = storage._accession_directory(ACCESSION) / "source-metadata.json"
            metadata = json.loads(metadata_path.read_bytes())
            metadata["accepted_at"] = "2026-01-16T16:31:00Z"
            metadata_path.write_bytes(canonical_source_metadata_json_bytes(metadata))
            payload = self.normalized_payload(raw_xml, "1.0.0")
            payload["accepted_at"] = metadata["accepted_at"]
            accepted_source = payload["source"]["field_sources"]["accepted_at"]
            accepted_source["raw_value"] = metadata["accepted_at"]
            accepted_source["normalized_value"] = metadata["accepted_at"]
            with self.assertRaisesRegex(InsiderStorageError, "source metadata"):
                storage.store_normalized(ACCESSION, "1.0.0", payload)
            self.assertFalse((metadata_path.parent / "normalized").exists())

    def test_normalization_freshly_reparses_raw_metadata_binding_before_publication(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            storage.store_raw(ACCESSION, raw_xml)
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
            payload = self.normalized_payload(raw_xml, "1.0.0")
            with patch("insider_storage.parse_ownership_xml", wraps=parse_ownership_xml) as parse_raw:
                stored = storage.store_normalized(ACCESSION, "1.0.0", payload)

            self.assertTrue(stored.created)
            self.assertEqual(2, parse_raw.call_count)
            self.assertTrue(all(call.args[0] == raw_xml for call in parse_raw.call_args_list))

    def test_read_normalized_binds_internal_keys_to_its_path(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            storage.store_raw(ACCESSION, raw_xml)
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
            payload = self.normalized_payload(raw_xml, "1.0.0")
            stored = storage.store_normalized(ACCESSION, "1.0.0", payload)
            tampered = json.loads(stored.path.read_bytes())
            tampered["parser_version"] = "1.0.1"
            stored.path.write_bytes(canonical_insider_json_bytes(tampered))
            with self.assertRaisesRegex(InsiderStorageError, "normalized filing"):
                storage.read_normalized(ACCESSION, "1.0.0")

    def test_normalized_reparses_are_immutable_and_parser_versioned(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
            raw = storage.store_raw(ACCESSION, raw_xml)
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
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
            with self.assertRaises(InsiderStorageError):
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
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
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
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
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
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
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
                store_source_prerequisites(storage, case["accession_number"], raw_xml, case)
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
            storage = InsiderStorage(repository_root)
            storage.store_raw(ACCESSION, raw_xml)
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
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
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
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


class InsiderSourceMetadataStorageTests(unittest.TestCase):
    index_fixture = (
        Path(__file__).parent / "fixtures" / "insider_ingestion" / "form4_index.html"
    )

    def metadata(self, raw_xml: bytes) -> dict[str, object]:
        index_bytes = self.index_fixture.read_bytes()
        parsed = parse_insider_filing_index(
            index_bytes,
            index_url=SIMPLE_CASE["source_index_url"],
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            reporting_owner_ciks=("0000000002",),
        )
        return build_insider_source_metadata(parsed, index_bytes, raw_xml)

    def test_index_and_canonical_metadata_are_immutable_and_readable(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        index_bytes = self.index_fixture.read_bytes()
        metadata = self.metadata(raw_xml)
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            first_index = storage.store_index_html(ACCESSION, index_bytes)
            self.assertTrue(first_index.created)
            self.assertEqual(index_bytes, storage.read_index_html(ACCESSION))
            storage.store_raw(ACCESSION, raw_xml)
            first = storage.store_source_metadata(ACCESSION, metadata)
            retry = storage.store_source_metadata(ACCESSION, dict(metadata))
            self.assertTrue(first.created)
            self.assertFalse(retry.created)
            self.assertEqual(
                canonical_source_metadata_json_bytes(metadata),
                first.path.read_bytes(),
            )
            self.assertEqual(metadata, storage.read_source_metadata(ACCESSION))
            self.assertEqual(raw_xml, storage.read_raw(ACCESSION))
            with self.assertRaises(InsiderStorageError):
                storage.store_source_metadata(
                    ACCESSION, {**metadata, "accepted_at": "2026-01-16T16:30:01Z"}
                )

    def test_read_source_metadata_binds_internal_accession_to_its_path(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            storage.store_index_html(ACCESSION, self.index_fixture.read_bytes())
            storage.store_raw(ACCESSION, raw_xml)
            stored = storage.store_source_metadata(ACCESSION, self.metadata(raw_xml))
            tampered = json.loads(stored.path.read_bytes())
            tampered["accession_number"] = "0000000001-26-000002"
            for artifact in (tampered["index"], tampered["document"]):
                artifact["url"] = artifact["url"].replace(ACCESSION, tampered["accession_number"]).replace(
                    ACCESSION.replace("-", ""), tampered["accession_number"].replace("-", "")
                )
            stored.path.write_bytes(canonical_source_metadata_json_bytes(tampered))
            with self.assertRaisesRegex(InsiderStorageError, "source metadata"):
                storage.read_source_metadata(ACCESSION)

    def test_multiprocess_index_and_metadata_writers_serialize_then_remain_live(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        index_bytes = self.index_fixture.read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            matching = InsiderStorageTests.run_multiprocess_pair(
                self, Path(tmpdir), mode="index", first_payload=index_bytes, second_payload=index_bytes
            )
            self.assertCountEqual(
                [("stored", True), ("stored", False)],
                [(result[0], result[2]) for result in matching],
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            conflict = InsiderStorageTests.run_multiprocess_pair(
                self,
                Path(tmpdir),
                mode="index",
                first_payload=index_bytes,
                second_payload=index_bytes + b"conflicting index",
            )
            self.assertEqual(("stored", "first", True), conflict[0][:3])
            self.assertEqual("conflict", conflict[1][0])

        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            storage.store_index_html(ACCESSION, index_bytes)
            storage.store_raw(ACCESSION, raw_xml)
            metadata = self.metadata(raw_xml)
            matching = InsiderStorageTests.run_multiprocess_pair(
                self, repository_root, mode="metadata", first_payload=metadata, second_payload=metadata
            )
            self.assertCountEqual(
                [("stored", True), ("stored", False)],
                [(result[0], result[2]) for result in matching],
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            repository_root = Path(tmpdir)
            storage = InsiderStorage(repository_root)
            storage.store_index_html(ACCESSION, index_bytes)
            storage.store_raw(ACCESSION, raw_xml)
            metadata = self.metadata(raw_xml)
            conflicting = json.loads(json.dumps(metadata))
            conflicting["accepted_at"] = "2026-01-16T16:30:01Z"
            conflict = InsiderStorageTests.run_multiprocess_pair(
                self,
                repository_root,
                mode="metadata",
                first_payload=metadata,
                second_payload=conflicting,
                allow_error=True,
            )
            self.assertEqual(("stored", "first", True), conflict[0][:3])
            self.assertEqual("error", conflict[1][0])
            self.assertEqual("InsiderStorageError", conflict[1][2])
            self.assertEqual(metadata, storage.read_source_metadata(ACCESSION))

    def test_index_and_metadata_targets_and_temps_fail_closed(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        index_bytes = self.index_fixture.read_bytes()

        for artifact in ("index", "metadata"):
            with self.subTest(artifact=artifact, attack="targets"):
                for attack in ("symlink", "hardlink", "directory", "mode"):
                    with self.subTest(attack=attack), tempfile.TemporaryDirectory() as tmpdir:
                        root = Path(tmpdir)
                        storage = InsiderStorage(root)
                        if artifact == "metadata":
                            storage.store_index_html(ACCESSION, index_bytes)
                            storage.store_raw(ACCESSION, raw_xml)
                            target = storage._accession_directory(ACCESSION) / "source-metadata.json"
                            write = lambda: storage.store_source_metadata(ACCESSION, self.metadata(raw_xml))
                        else:
                            storage.store_raw(ACCESSION, raw_xml)
                            target = storage._accession_directory(ACCESSION) / "index.html"
                            write = lambda: storage.store_index_html(ACCESSION, index_bytes)
                        outside = root / "outside"
                        outside.write_bytes(b"outside")
                        outside.chmod(0o600)
                        if attack == "symlink":
                            target.symlink_to(outside)
                        elif attack == "hardlink":
                            os.link(outside, target)
                        elif attack == "directory":
                            target.mkdir()
                        else:
                            target.write_bytes(b"widened")
                            target.chmod(0o644)
                        with self.assertRaises(InsiderStorageError):
                            write()
                        self.assertTrue(target.exists() or target.is_symlink())
                        self.assertEqual(b"outside", outside.read_bytes())

            with self.subTest(artifact=artifact, attack="temporary-artifacts"):
                for attack in ("safe", "symlink", "hardlink", "mode"):
                    with self.subTest(attack=attack), tempfile.TemporaryDirectory() as tmpdir:
                        root = Path(tmpdir)
                        storage = InsiderStorage(root)
                        if artifact == "metadata":
                            storage.store_index_html(ACCESSION, index_bytes)
                            storage.store_raw(ACCESSION, raw_xml)
                            directory = storage._accession_directory(ACCESSION)
                            target_name = "source-metadata.json"
                            write = lambda: storage.store_source_metadata(ACCESSION, self.metadata(raw_xml))
                        else:
                            storage.store_raw(ACCESSION, raw_xml)
                            directory = storage._accession_directory(ACCESSION)
                            target_name = "index.html"
                            write = lambda: storage.store_index_html(ACCESSION, index_bytes)
                        temporary = directory / f".{target_name}.tmp-{'a' * 24}"
                        unrelated = directory / ".unrelated"
                        other_target = directory / f".other.tmp-{'b' * 24}"
                        unrelated.write_bytes(b"keep")
                        other_target.write_bytes(b"keep-other")
                        if attack == "safe":
                            temporary.write_bytes(b"incomplete")
                            temporary.chmod(0o600)
                            self.assertTrue(write().created)
                            self.assertFalse(temporary.exists())
                        else:
                            outside = root / "outside-temp"
                            outside.write_bytes(b"outside-temp")
                            outside.chmod(0o600)
                            if attack == "symlink":
                                temporary.symlink_to(outside)
                            elif attack == "hardlink":
                                os.link(outside, temporary)
                            else:
                                temporary.write_bytes(b"incomplete")
                                temporary.chmod(0o644)
                            with self.assertRaisesRegex(InsiderStorageError, "temporary artifact"):
                                write()
                            self.assertTrue(temporary.exists() or temporary.is_symlink())
                        self.assertEqual(b"keep", unrelated.read_bytes())
                        self.assertEqual(b"keep-other", other_target.read_bytes())

    def test_index_and_metadata_baseexception_cleanup_is_retryable(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        index_bytes = self.index_fixture.read_bytes()
        for artifact in ("index", "metadata"):
            for seam in ("write", "fsync", "rename"):
                with self.subTest(artifact=artifact, seam=seam), tempfile.TemporaryDirectory() as tmpdir:
                    storage = InsiderStorage(Path(tmpdir))
                    if artifact == "metadata":
                        storage.store_index_html(ACCESSION, index_bytes)
                        storage.store_raw(ACCESSION, raw_xml)
                        target = storage._accession_directory(ACCESSION) / "source-metadata.json"
                        write = lambda: storage.store_source_metadata(ACCESSION, self.metadata(raw_xml))
                    else:
                        storage.store_raw(ACCESSION, raw_xml)
                        target = storage._accession_directory(ACCESSION) / "index.html"
                        write = lambda: storage.store_index_html(ACCESSION, index_bytes)
                    if seam == "write":
                        context = patch("insider_storage.os.write", side_effect=KeyboardInterrupt())
                    elif seam == "rename":
                        context = patch("insider_storage._rename_noreplace", side_effect=KeyboardInterrupt())
                    else:
                        real_fsync = os.fsync
                        first = True

                        def fail_once(descriptor: int) -> None:
                            nonlocal first
                            if first:
                                first = False
                                raise KeyboardInterrupt()
                            real_fsync(descriptor)

                        context = patch("insider_storage.os.fsync", side_effect=fail_once)
                    with context, self.assertRaises(KeyboardInterrupt):
                        write()
                    self.assertFalse(target.exists())
                    self.assertFalse(list(target.parent.glob(f".{target.name}.tmp-*")))
                    self.assertTrue(target.parent.is_dir())
                    self.assertTrue(write().created)

    def test_partial_prerequisites_reject_without_normalized_then_replay_idempotently(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        index_bytes = self.index_fixture.read_bytes()
        payload = InsiderStorageTests.normalized_payload(raw_xml, "1.0.0")
        for state in ("index-only", "raw-only", "index-raw", "invalid-metadata"):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as tmpdir:
                storage = InsiderStorage(Path(tmpdir))
                if state in {"index-only", "index-raw", "invalid-metadata"}:
                    storage.store_index_html(ACCESSION, index_bytes)
                if state in {"raw-only", "index-raw", "invalid-metadata"}:
                    storage.store_raw(ACCESSION, raw_xml)
                if state == "invalid-metadata":
                    metadata_path = storage._accession_directory(ACCESSION) / "source-metadata.json"
                    metadata_path.write_bytes(b'{"accession_number":"x","accession_number":"x"}')
                    metadata_path.chmod(0o600)
                with self.assertRaises(InsiderStorageError):
                    storage.store_normalized(ACCESSION, "1.0.0", payload)
                self.assertFalse((storage._accession_directory(ACCESSION) / "normalized").exists())

        for malformed in (
            b"not-json",
            b'{"accession_number":"x","accession_number":"x"}',
            b'{"source":NaN}',
            canonical_source_metadata_json_bytes(self.metadata(raw_xml)) + b" ",
        ):
            with self.subTest(malformed=malformed), tempfile.TemporaryDirectory() as tmpdir:
                storage = InsiderStorage(Path(tmpdir))
                storage.store_index_html(ACCESSION, index_bytes)
                storage.store_raw(ACCESSION, raw_xml)
                metadata_path = storage._accession_directory(ACCESSION) / "source-metadata.json"
                metadata_path.write_bytes(malformed)
                metadata_path.chmod(0o600)
                with self.assertRaises(InsiderStorageError):
                    storage.store_normalized(ACCESSION, "1.0.0", payload)
                self.assertFalse((metadata_path.parent / "normalized").exists())

        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            self.assertTrue(storage.store_index_html(ACCESSION, index_bytes).created)
            with self.assertRaises(InsiderStorageError):
                storage.store_source_metadata(ACCESSION, self.metadata(raw_xml))
            self.assertTrue(storage.store_raw(ACCESSION, raw_xml).created)
            (storage._accession_directory(ACCESSION) / "index.html").unlink()
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
            metadata = storage.read_source_metadata(ACCESSION)
            self.assertTrue(storage.store_normalized(ACCESSION, "1.0.0", payload).created)
            self.assertFalse(storage.store_index_html(ACCESSION, storage.read_index_html(ACCESSION)).created)
            self.assertFalse(storage.store_raw(ACCESSION, raw_xml).created)
            self.assertFalse(storage.store_source_metadata(ACCESSION, metadata).created)
            self.assertFalse(storage.store_normalized(ACCESSION, "1.0.0", payload).created)

    def test_normalization_rejects_post_publication_source_tampering(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        index_bytes = self.index_fixture.read_bytes()
        payload = InsiderStorageTests.normalized_payload(raw_xml, "1.0.0")
        for target_name in ("index.html", "raw.xml", "source-metadata.json"):
            with self.subTest(target=target_name), tempfile.TemporaryDirectory() as tmpdir:
                storage = InsiderStorage(Path(tmpdir))
                storage.store_index_html(ACCESSION, index_bytes)
                storage.store_raw(ACCESSION, raw_xml)
                storage.store_source_metadata(ACCESSION, self.metadata(raw_xml))
                target = storage._accession_directory(ACCESSION) / target_name
                if target_name == "index.html":
                    target.write_bytes(index_bytes + b"tampered")
                elif target_name == "raw.xml":
                    target.write_bytes(raw_xml + b"tampered")
                else:
                    tampered = self.metadata(raw_xml)
                    tampered["accepted_at"] = "2026-01-16T16:30:01Z"
                    target.write_bytes(canonical_source_metadata_json_bytes(tampered))
                target.chmod(0o600)
                self.assertEqual(0o600, target.stat().st_mode & 0o777)
                with self.assertRaises(InsiderStorageError):
                    storage.store_normalized(ACCESSION, "1.0.0", payload)
                self.assertFalse((target.parent / "normalized").exists())

    def test_strict_metadata_and_normalized_reads_reject_noncanonical_json(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        index_bytes = self.index_fixture.read_bytes()
        payload = InsiderStorageTests.normalized_payload(raw_xml, "1.0.0")
        for malformed in (
            b"not-json",
            b'{"accession_number":"x","accession_number":"x"}',
            b'{"source":NaN}',
            b"[" * 1_100 + b"]" * 1_100,
            canonical_source_metadata_json_bytes(self.metadata(raw_xml)) + b" ",
        ):
            with self.subTest(kind="metadata", malformed=malformed), tempfile.TemporaryDirectory() as tmpdir:
                storage = InsiderStorage(Path(tmpdir))
                storage.store_index_html(ACCESSION, index_bytes)
                storage.store_raw(ACCESSION, raw_xml)
                stored = storage.store_source_metadata(ACCESSION, self.metadata(raw_xml))
                stored.path.write_bytes(malformed)
                stored.path.chmod(0o600)
                with self.assertRaisesRegex(InsiderStorageError, "source metadata"):
                    storage.read_source_metadata(ACCESSION)
        for malformed in (
            b"not-json",
            b'{"accession_number":"x","accession_number":"x"}',
            b'{"source":NaN}',
            b"[" * 1_100 + b"]" * 1_100,
            canonical_insider_json_bytes(payload) + b" ",
        ):
            with self.subTest(kind="normalized", malformed=malformed), tempfile.TemporaryDirectory() as tmpdir:
                storage = InsiderStorage(Path(tmpdir))
                store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
                stored = storage.store_normalized(ACCESSION, "1.0.0", payload)
                stored.path.write_bytes(malformed)
                stored.path.chmod(0o600)
                with self.assertRaisesRegex(InsiderStorageError, "normalized filing"):
                    storage.read_normalized(ACCESSION, "1.0.0")

    def test_all_artifacts_have_exact_private_modes_owner_and_single_link(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
            index = StoredArtifact(
                storage._accession_directory(ACCESSION) / "index.html", "", 0, False
            )
            raw = StoredArtifact(
                storage._accession_directory(ACCESSION) / "raw.xml", "", 0, False
            )
            metadata = StoredArtifact(
                storage._accession_directory(ACCESSION) / "source-metadata.json", "", 0, False
            )
            normalized = storage.store_normalized(
                ACCESSION, "1.0.0", InsiderStorageTests.normalized_payload(raw_xml, "1.0.0")
            )
            for artifact in (index, raw, metadata, normalized):
                info = artifact.path.stat()
                self.assertEqual(0o600, info.st_mode & 0o777)
                self.assertEqual(os.geteuid(), info.st_uid)
                self.assertEqual(1, info.st_nlink)
            for directory in (
                storage.private_root,
                storage.private_root / "accessions",
                storage._accession_directory(ACCESSION),
                normalized.path.parent,
            ):
                info = directory.stat()
                self.assertEqual(0o700, info.st_mode & 0o777)
                self.assertEqual(os.geteuid(), info.st_uid)

    def test_metadata_and_normalization_hold_one_accession_lock_through_publish(self) -> None:
        import insider_storage as storage_module

        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        for operation in ("metadata", "normalized"):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as tmpdir:
                storage = InsiderStorage(Path(tmpdir))
                if operation == "metadata":
                    storage.store_index_html(ACCESSION, self.index_fixture.read_bytes())
                    storage.store_raw(ACCESSION, raw_xml)
                else:
                    store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
                original_lock = storage_module._exclusive_directory_lock
                original_validate = storage._validate_source_metadata_bindings_locked
                original_store = storage._store_immutable_locked
                held = 0
                events: list[str] = []

                @contextmanager
                def observe_lock(descriptor: int) -> Any:
                    nonlocal held
                    with original_lock(descriptor):
                        held += 1
                        events.append("lock")
                        try:
                            yield
                        finally:
                            events.append("unlock")
                            held -= 1

                def observe_validate(*args: Any, **kwargs: Any) -> None:
                    self.assertEqual(1, held)
                    events.append("validate")
                    original_validate(*args, **kwargs)

                def observe_publish(*args: Any, **kwargs: Any) -> StoredArtifact:
                    self.assertEqual(1, held)
                    events.append("publish")
                    return original_store(*args, **kwargs)

                with patch.object(storage_module, "_exclusive_directory_lock", side_effect=observe_lock), patch.object(
                    storage, "_validate_source_metadata_bindings_locked", side_effect=observe_validate
                ), patch.object(storage, "_store_immutable_locked", side_effect=observe_publish):
                    if operation == "metadata":
                        storage.store_source_metadata(ACCESSION, self.metadata(raw_xml))
                    else:
                        storage.store_normalized(
                            ACCESSION, "1.0.0", InsiderStorageTests.normalized_payload(raw_xml, "1.0.0")
                        )
                self.assertEqual(
                    ["lock", "validate", "publish", "validate", "unlock"], events
                )

    def test_source_metadata_publication_rechecks_raw_after_temp_fsync(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            storage.store_index_html(ACCESSION, self.index_fixture.read_bytes())
            storage.store_raw(ACCESSION, raw_xml)
            raw_path = storage._accession_directory(ACCESSION) / "raw.xml"
            original_validate = storage._validate_source_metadata_bindings_locked
            calls = 0

            def replace_raw_after_initial_validation(*args: Any, **kwargs: Any) -> None:
                nonlocal calls
                calls += 1
                original_validate(*args, **kwargs)
                if calls == 1:
                    raw_path.unlink()
                    raw_path.write_bytes(raw_xml + b"tampered")
                    raw_path.chmod(0o600)

            with patch.object(
                storage,
                "_validate_source_metadata_bindings_locked",
                side_effect=replace_raw_after_initial_validation,
            ), self.assertRaisesRegex(InsiderStorageError, "source metadata"):
                storage.store_source_metadata(ACCESSION, self.metadata(raw_xml))
            self.assertEqual(2, calls)
            self.assertFalse((raw_path.parent / "source-metadata.json").exists())
            self.assertFalse((raw_path.parent / "normalized").exists())

    def test_normalized_publication_rechecks_source_metadata_after_temp_fsync(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
            metadata_path = storage._accession_directory(ACCESSION) / "source-metadata.json"
            tampered_metadata = self.metadata(raw_xml)
            tampered_metadata["accepted_at"] = "2026-01-16T16:30:01Z"
            original_validate = storage._validate_source_metadata_bindings_locked
            calls = 0

            def replace_metadata_after_initial_validation(*args: Any, **kwargs: Any) -> None:
                nonlocal calls
                calls += 1
                original_validate(*args, **kwargs)
                if calls == 1:
                    metadata_path.unlink()
                    metadata_path.write_bytes(
                        canonical_source_metadata_json_bytes(tampered_metadata)
                    )
                    metadata_path.chmod(0o600)

            with patch.object(
                storage,
                "_validate_source_metadata_bindings_locked",
                side_effect=replace_metadata_after_initial_validation,
            ), self.assertRaisesRegex(InsiderStorageError, "source metadata"):
                storage.store_normalized(
                    ACCESSION,
                    "1.0.0",
                    InsiderStorageTests.normalized_payload(raw_xml, "1.0.0"),
                )
            self.assertEqual(2, calls)
            self.assertFalse(
                (metadata_path.parent / "normalized" / "1.0.0.json").exists()
            )
            self.assertFalse((metadata_path.parent / "normalized").exists())

    def test_reads_reject_post_publication_prerequisite_tampering(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            store_source_prerequisites(storage, ACCESSION, raw_xml, SIMPLE_CASE)
            storage.store_normalized(
                ACCESSION,
                "1.0.0",
                InsiderStorageTests.normalized_payload(raw_xml, "1.0.0"),
            )
            raw_path = storage._accession_directory(ACCESSION) / "raw.xml"
            raw_path.unlink()
            raw_path.write_bytes(raw_xml + b"tampered")
            raw_path.chmod(0o600)
            with self.assertRaisesRegex(InsiderStorageError, "source metadata"):
                storage.read_source_metadata(ACCESSION)
            with self.assertRaisesRegex(InsiderStorageError, "normalized filing"):
                storage.read_normalized(ACCESSION, "1.0.0")

    def test_metadata_requires_matching_stored_index_and_raw(self) -> None:
        raw_xml = (FIXTURE_ROOT / SIMPLE_CASE["filename"]).read_bytes()
        metadata = self.metadata(raw_xml)
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = InsiderStorage(Path(tmpdir))
            storage.store_index_html(ACCESSION, self.index_fixture.read_bytes())
            with self.assertRaisesRegex(InsiderStorageError, "raw ownership XML"):
                storage.store_source_metadata(ACCESSION, metadata)
            storage.store_raw(ACCESSION, raw_xml)
            bad = json.loads(json.dumps(metadata))
            bad["index"]["sha256"] = "0" * 64
            with self.assertRaisesRegex(InsiderStorageError, "metadata"):
                storage.store_source_metadata(ACCESSION, bad)


class InsiderPrivateStateTests(unittest.TestCase):
    def test_child_directory_retry_refsyncs_ancestor_after_failed_creation_sync(self) -> None:
        import insider_storage as storage_module

        with tempfile.TemporaryDirectory() as tmpdir:
            parent_descriptor = os.open(tmpdir, storage_module._DIRECTORY_OPEN_FLAGS)
            real_fsync = storage_module.os.fsync
            calls: list[int] = []

            failed_parent = False

            def fail_once_then_record(descriptor: int) -> None:
                nonlocal failed_parent
                if descriptor == parent_descriptor:
                    calls.append(descriptor)
                    if not failed_parent:
                        failed_parent = True
                        raise OSError("injected parent fsync failure")
                real_fsync(descriptor)

            try:
                with patch.object(storage_module.os, "fsync", side_effect=fail_once_then_record):
                    with self.assertRaises(InsiderStorageError):
                        storage_module._open_child_directory(
                            parent_descriptor, "child", create=True, restricted=True
                        )
                    child_descriptor, created = storage_module._open_child_directory(
                        parent_descriptor, "child", create=True, restricted=True
                    )
                try:
                    self.assertFalse(created)
                    self.assertEqual([parent_descriptor, parent_descriptor], calls)
                finally:
                    os.close(child_descriptor)
            finally:
                os.close(parent_descriptor)

    def test_state_namespace_ancestors_require_owner_and_safe_modes(self) -> None:
        import insider_storage as storage_module

        for relative, mode in (("data", 0o777), ("data/insiders", 0o777)):
            with self.subTest(relative=relative):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    state = InsiderStateStore(root)
                    state.write("incremental-v1", self.incremental())
                    ancestor = root / relative
                    ancestor.chmod(mode)
                    with self.assertRaises(InsiderStorageError):
                        state.read("incremental-v1")
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("incremental-v1", self.incremental())
            with patch.object(storage_module.os, "geteuid", return_value=os.geteuid() + 1):
                with self.assertRaises(InsiderStorageError):
                    state.read("incremental-v1")

    def test_fifo_state_targets_and_matching_temps_reject_without_blocking(self) -> None:
        context = multiprocessing.get_context("spawn")
        for action in ("read", "write"):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as tmpdir:
                state = InsiderStateStore(Path(tmpdir))
                stored = state.write("incremental-v1", self.incremental())
                if action == "read":
                    stored.path.unlink()
                    fifo = stored.path
                else:
                    fifo = stored.path.parent / ".incremental-v1.json.tmp-aaaaaaaaaaaaaaaaaaaaaaaa"
                os.mkfifo(fifo, 0o600)
                results = context.Queue()
                worker = context.Process(target=_fifo_state_worker, args=(tmpdir, action, results))
                worker.start()
                worker.join(3)
                if worker.is_alive():
                    worker.terminate()
                    worker.join(3)
                    self.fail("FIFO state target blocked instead of failing closed")
                self.assertEqual(0, worker.exitcode)
                self.assertEqual("InsiderStorageError", results.get(timeout=2))

    def test_state_versions_and_backfill_evidence_urls_fail_closed(self) -> None:
        valid = {
            "contract_version": 1, "quarter": "2026Q1", "issuer_cik": "0000000001",
            "status": "incomplete",
            "catalog_url": "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
            "zip_url": "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2026q1.zip",
            "zip_sha256": "0" * 64, "zip_byte_count": 1,
            "etag": 'W/"a stable etag"',
            "last_modified": "Sun, 06 Nov 1994 08:49:37 GMT",
            "table_evidence": [], "missing_optional_tables": [],
            "selected_accessions": [], "completed_accessions": [], "reconciliation": [],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            for version in (True, 1.0, 2):
                with self.subTest(version=version), self.assertRaises(InsiderStorageError):
                    store_backfill_state_for_test(
                        state, {**valid, "contract_version": version}
                    )
            stored = store_backfill_state_for_test(state, valid)
            stored.path.write_bytes(canonical_insider_state_json_bytes({**valid, "contract_version": True}))
            stored.path.chmod(0o600)
            with self.assertRaises(InsiderStorageError):
                state.read("backfill/2026Q1")
            for url in (
                "https://www.sec.gov:bad/files/dera/data/insider-transactions-data-sets/x.zip",
                "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/x.zip?x=1",
                "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/%2e%2e/x.zip",
                "https://www.sec.gov.evil/files/dera/data/insider-transactions-data-sets/x.zip",
            ):
                with self.subTest(url=url), self.assertRaises(InsiderStorageError):
                    store_backfill_state_for_test(state, {**valid, "zip_url": url})
            stored.path.write_bytes(canonical_insider_state_json_bytes(valid))
            stored.path.chmod(0o600)
            with self.assertRaises(InsiderStorageError):
                store_backfill_state_for_test(
                    state,
                    {
                        **valid,
                        "zip_url": "https://sec.gov:443/files/structureddata/data/insider-transactions-data-sets/x.zip",
                    },
                    expected_sha256=hashlib.sha256(
                        canonical_insider_state_json_bytes(valid)
                    ).hexdigest(),
                )

        form345 = {
            **valid,
            "zip_url": (
                "https://www.sec.gov/files/structureddata/data/"
                "insider-transactions-data-sets/2026q1_form345.zip"
            ),
            "etag": '"ordinary-etag"',
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            store_backfill_state_for_test(state, form345)
            self.assertEqual(form345, state.read("backfill/2026Q1"))

        unexpected_path = {
            **valid,
            "zip_url": (
                "https://www.sec.gov/files/future/location/"
                "insider-transactions-data-sets/2026q1.zip"
            ),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(InsiderStorageError):
                store_backfill_state_for_test(
                    InsiderStateStore(Path(tmpdir)),
                    unexpected_path,
                )

        missing_issuer = dict(valid)
        missing_issuer.pop("issuer_cik")
        for invalid in (
            missing_issuer,
            {**valid, "issuer_cik": "0000000000"},
            {**valid, "issuer_cik": True},
        ):
            with self.subTest(issuer=invalid.get("issuer_cik")), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    store_backfill_state_for_test(
                        InsiderStateStore(Path(tmpdir)), invalid
                    )

    def test_backfill_reconciliation_status_matches_counts_and_completion(self) -> None:
        import insider_storage as storage_module

        base = {
            "contract_version": 1,
            "quarter": "2026Q1",
            "issuer_cik": "0000000001",
            "status": "incomplete",
            "catalog_url": None,
            "zip_url": None,
            "zip_sha256": None,
            "zip_byte_count": None,
            "etag": None,
            "last_modified": None,
            "table_evidence": [],
            "missing_optional_tables": [],
            "selected_accessions": [],
            "completed_accessions": [],
            "reconciliation": [],
        }

        valid_reconciliations = (
            {"name": "rows", "expected_count": 1, "actual_count": 1, "status": "matched"},
            {"name": "rows", "expected_count": 1, "actual_count": 2, "status": "mismatch"},
            {"name": "rows", "expected_count": 1, "actual_count": 0, "status": "pending"},
            {"name": "rows", "expected_count": 0, "actual_count": 0, "status": "not_applicable"},
        )
        for reconciliation in valid_reconciliations:
            with self.subTest(valid=reconciliation), tempfile.TemporaryDirectory() as tmpdir:
                store_backfill_state_for_test(
                    InsiderStateStore(Path(tmpdir)),
                    {**base, "reconciliation": [reconciliation]},
                )

        invalid_reconciliations = (
            {"name": "rows", "expected_count": 1, "actual_count": 2, "status": "matched"},
            {"name": "rows", "expected_count": 1, "actual_count": 1, "status": "mismatch"},
            {"name": "rows", "expected_count": 1, "actual_count": 0, "status": "not_applicable"},
        )
        for reconciliation in invalid_reconciliations:
            with self.subTest(invalid=reconciliation), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    store_backfill_state_for_test(
                        InsiderStateStore(Path(tmpdir)),
                        {**base, "reconciliation": [reconciliation]},
                    )

        completed_with_pending = {
            **base,
            "status": "completed",
            "catalog_url": "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
            "zip_url": "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2026q1.zip",
            "zip_sha256": "0" * 64,
            "zip_byte_count": 1,
            "table_evidence": [
                {"table_name": "SUBMISSION", "headers": ["ACCESSION_NUMBER"], "row_count": 0}
            ],
            "missing_optional_tables": sorted(storage_module._BACKFILL_OPTIONAL_TABLES),
            "reconciliation": [
                {"name": "rows", "expected_count": 1, "actual_count": 0, "status": "pending"}
            ],
        }
        with tempfile.TemporaryDirectory() as tmpdir, self.assertRaises(InsiderStorageError):
            store_backfill_state_for_test(
                InsiderStateStore(Path(tmpdir)), completed_with_pending
            )

    def test_state_cross_field_ordering_keys_titles_and_scalars_fail_closed(self) -> None:
        entry = {
            "accession_number": ACCESSION, "issuer_cik": "0000000001",
            "index_url": filing_index_url(), "accepted_at": "2026-01-01T00:00:00Z",
            "observed_at": "2026-01-01T00:00:00Z",
        }
        incremental = self.incremental([entry])
        incremental["completed_accessions"] = [ACCESSION]
        owner_ciks = ["0000000001"]
        title = "Class A $5.00 [Series] – Owner's %"
        issuer = with_valid_issuer_generation_digest({
            "contract_version": 1, "issuer_cik": "0000000001", "accessions": [],
            "owner_groups": [{"owner_group_key": section16_owner_group_key(owner_ciks), "owner_ciks": owner_ciks}],
            "security_classes": [{"security_class_key": section16_security_class_key("0000000001", title, is_derivative=False), "derivative": False, "title": title}],
            "amendments": [], "unresolved_ambiguities": [],
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("incremental-v1", incremental)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            state.write_issuer_if_approved("0000000001", issuer)
            invalids = (
                ("incremental-v1", {**incremental, "completed_accessions": ["0000000001-26-000002"]}),
                ("incremental-v1", {**incremental, "status": "unknown"}),
                ("incremental-v1", {**incremental, "lookback_seconds": 2_147_483_648}),
                ("incremental-v1", {**incremental, "first_observed_at": "2026-01-02T00:00:00Z", "last_observed_at": "2026-01-01T00:00:00Z"}),
                ("issuers/0000000001", {**issuer, "owner_groups": [{"owner_group_key": "0" * 64, "owner_ciks": owner_ciks}]}),
                ("issuers/0000000001", {**issuer, "security_classes": [{"security_class_key": "0" * 64, "derivative": False, "title": title}]}),
                ("issuers/0000000001", {**issuer, "security_classes": [{"security_class_key": section16_security_class_key("0000000001", title, is_derivative=False), "derivative": False, "title": "<script>bad</script>"}]}),
            )
            for key, payload in invalids:
                with self.subTest(key=key, payload=payload), self.assertRaises(InsiderStorageError):
                    if key.startswith("issuers/"):
                        state.write_issuer_if_approved("0000000001", payload)
                    else:
                        state.write(key, payload)
            with self.assertRaises(InsiderStorageError):
                store_backfill_state_for_test(
                    state,
                    {"contract_version": 1, "quarter": "0000Q1", "issuer_cik": "0000000001", "status": "incomplete", "catalog_url": None, "zip_url": None, "zip_sha256": None, "zip_byte_count": None, "etag": None, "last_modified": None, "table_evidence": [], "missing_optional_tables": [], "selected_accessions": [], "completed_accessions": [], "reconciliation": []},
                    quarter="0000Q1",
                )

    def incremental(self, queue: list[dict[str, object]] | None = None) -> dict[str, object]:
        entries = [
            incremental_queue_entry(entry)
            for entry in ([] if queue is None else queue)
        ]
        source_entries = [issuer_source_entry(entry) for entry in entries]
        observed = [entry["observed_at"] for entry in source_entries]
        return {
            "contract_version": 1,
            "status": "incomplete",
            "lookback_seconds": 3600,
            "first_observed_at": min(observed) if observed else None,
            "last_observed_at": max(observed) if observed else None,
            "queue": entries,
            "completed_accessions": [],
            "source_entries": source_entries,
        }

    def test_atomic_immutable_publication_requires_current_durable_approval(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000009"]},
            )
            calls: list[str] = []
            with self.assertRaises(InsiderApprovalScopeError):
                state.publish_if_issuer_approved(
                    "0000000001",
                    lambda: calls.append("unapproved"),
                )
            self.assertEqual([], calls)

            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000001"],
                },
            )
            entered = threading.Event()
            release = threading.Event()
            publication_done = threading.Event()
            revocation_done = threading.Event()
            published: list[str] = []

            def publish() -> None:
                def callback() -> str:
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError("publication serialization test timed out")
                    calls.append("published")
                    return "published"

                published.append(
                    state.publish_if_issuer_approved("0000000001", callback)
                )
                publication_done.set()

            def revoke() -> None:
                state.update(
                    "approved-issuers-v1",
                    lambda current: {
                        **current,
                        "issuer_ciks": ["0000000009"],
                    },
                )
                revocation_done.set()

            publisher = threading.Thread(target=publish)
            publisher.start()
            self.assertTrue(entered.wait(5))
            revoker = threading.Thread(target=revoke)
            revoker.start()
            self.assertFalse(revocation_done.wait(0.2))
            release.set()
            publisher.join(5)
            revoker.join(5)

            self.assertFalse(publisher.is_alive())
            self.assertFalse(revoker.is_alive())
            self.assertTrue(publication_done.is_set())
            self.assertTrue(revocation_done.is_set())
            self.assertEqual(["published"], published)
            self.assertEqual(["published"], calls)
            with self.assertRaises(InsiderApprovalScopeError):
                state.publish_if_issuer_approved(
                    "0000000001",
                    lambda: calls.append("revoked"),
                )
            self.assertEqual(["published"], calls)

            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000001"],
                },
            )
            with self.assertRaisesRegex(InsiderStorageError, "re-enter"):
                state.publish_if_issuer_approved(
                    "0000000001",
                    lambda: state.read("approved-issuers-v1"),
                )
            with self.assertRaisesRegex(RuntimeError, "synthetic publication failure"):
                state.publish_if_issuer_approved(
                    "0000000001",
                    lambda: (_ for _ in ()).throw(
                        RuntimeError("synthetic publication failure")
                    ),
                )
            self.assertEqual(
                ["0000000001"],
                state.read("approved-issuers-v1")["issuer_ciks"],
            )

    def test_atomic_accession_quarantine_write_requires_current_approval(
        self,
    ) -> None:
        payload = {
            "contract_version": 1,
            "stage": "raw",
            "error_class": "InsiderParseError",
            "reason_code": "raw_invalid",
            "retry_count": 0,
            "next_retry_at": None,
            "parser_version": "1.0.0",
            "source_hashes": [],
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
            "form_type": "4",
            **accession_quarantine_identity(),
        }
        key = f"quarantine/accessions/{ACCESSION}"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)

            with self.assertRaises(FileNotFoundError):
                state.write_accession_quarantine_if_issuer_approved(
                    ACCESSION,
                    "0000000001",
                    payload,
                )
            self.assertFalse((root / "data").exists())

            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000009"]},
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_accession_quarantine_if_issuer_approved(
                    ACCESSION,
                    "0000000001",
                    payload,
                )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_accession_quarantine_if_issuer_approved(
                    ACCESSION,
                    "0000000009",
                    payload,
                )
            with self.assertRaises(FileNotFoundError):
                state.read(key)

            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000001"],
                },
            )
            first = state.write_accession_quarantine_if_issuer_approved(
                ACCESSION,
                "0000000001",
                payload,
            )
            retry = state.write_accession_quarantine_if_issuer_approved(
                ACCESSION,
                "0000000001",
                payload,
            )
            self.assertTrue(first.created)
            self.assertFalse(retry.created)
            self.assertEqual(first.sha256, retry.sha256)
            with self.assertRaisesRegex(InsiderStorageError, "revision is stale"):
                state.write_accession_quarantine_if_issuer_approved(
                    ACCESSION,
                    "0000000001",
                    payload,
                    expected_sha256="0" * 64,
                )

            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000009"],
                },
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_accession_quarantine_if_issuer_approved(
                    ACCESSION,
                    "0000000001",
                    {**payload, "error_class": "InsiderStorageError"},
                    expected_sha256=first.sha256,
                )
            self.assertEqual(payload, state.read(key))

    def test_atomic_issuer_state_write_requires_current_durable_approval(
        self,
    ) -> None:
        payload = issuer_state_payload()
        key = "issuers/0000000001"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)

            with self.assertRaises(FileNotFoundError):
                state.write_issuer_if_approved("0000000001", payload)
            self.assertFalse((root / "data").exists())

            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000009"]},
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_issuer_if_approved("0000000001", payload)
            with self.assertRaises(FileNotFoundError):
                state.read(key)

            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000001"],
                },
            )
            first = state.write_issuer_if_approved("0000000001", payload)
            retry = state.write_issuer_if_approved("0000000001", payload)
            self.assertTrue(first.created)
            self.assertFalse(retry.created)
            self.assertEqual(first.sha256, retry.sha256)

            owner_ciks = ["0000000002"]
            changed = {
                **payload,
                "owner_groups": [
                    {
                        "owner_group_key": section16_owner_group_key(owner_ciks),
                        "owner_ciks": owner_ciks,
                    }
                ],
            }
            with self.assertRaisesRegex(InsiderStorageError, "revision is stale"):
                state.write_issuer_if_approved(
                    "0000000001",
                    changed,
                    expected_sha256="f" * 64,
                )
            updated = state.write_issuer_if_approved(
                "0000000001",
                changed,
                expected_sha256=first.sha256,
            )
            self.assertTrue(updated.created)

            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000009"],
                },
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_issuer_if_approved(
                    "0000000001",
                    payload,
                    expected_sha256=updated.sha256,
                )
            self.assertEqual(changed, state.read(key))

    def test_backfill_state_write_and_update_require_current_durable_approval(
        self,
    ) -> None:
        payload = backfill_state_payload()
        key = "backfill/2026Q1"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)

            with self.assertRaises(FileNotFoundError):
                state.write_backfill_if_issuer_approved(
                    "2026Q1",
                    "0000000001",
                    payload,
                )
            self.assertFalse((root / "data").exists())

            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000009"]},
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_backfill_if_issuer_approved(
                    "2026Q1",
                    "0000000001",
                    payload,
                )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_backfill_if_issuer_approved(
                    "2026Q1",
                    "0000000009",
                    payload,
                )
            with self.assertRaises(FileNotFoundError):
                state.read(key)

            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000001"],
                },
            )
            first = state.write_backfill_if_issuer_approved(
                "2026Q1",
                "0000000001",
                payload,
            )
            retry = state.write_backfill_if_issuer_approved(
                "2026Q1",
                "0000000001",
                payload,
            )
            self.assertTrue(first.created)
            self.assertFalse(retry.created)
            self.assertEqual(first.sha256, retry.sha256)

            updated = state.update_backfill_if_issuer_approved(
                "2026Q1",
                "0000000001",
                lambda current: {
                    **current,
                    "selected_accessions": [ACCESSION],
                },
            )
            self.assertEqual([ACCESSION], updated["selected_accessions"])
            self.assertEqual(updated, state.read(key))

            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000009"],
                },
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.update_backfill_if_issuer_approved(
                    "2026Q1",
                    "0000000001",
                    lambda current: current,
                )
            self.assertEqual(updated, state.read(key))

    def test_generic_backfill_mutation_requires_approval_gated_api(self) -> None:
        payload = backfill_state_payload()
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write("backfill/2026Q1", payload)

            state.write_backfill_if_issuer_approved(
                "2026Q1",
                "0000000001",
                payload,
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.update("backfill/2026Q1", lambda current: current)

    def test_generic_reparse_mutation_and_revoked_updates_require_approval_gate(
        self,
    ) -> None:
        payload = {
            "contract_version": 1,
            "status": "running",
            "parser_version": "1.0.0",
            "scope": "accession",
            "scope_identifier": ACCESSION,
            "max_accessions": 1,
            "queue": [
                {
                    "accession_number": ACCESSION,
                    "issuer_cik": "0000000001",
                }
            ],
            "completed_accessions": [],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)

            with self.assertRaises(FileNotFoundError):
                state.write_reparse_if_issuers_approved(payload)
            self.assertFalse((root / "data").exists())

            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000009"]},
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_reparse_if_issuers_approved(payload)
            with self.assertRaises(FileNotFoundError):
                state.read("reparse-v1")

            state.update(
                "approved-issuers-v1",
                lambda current: {**current, "issuer_ciks": ["0000000001"]},
            )
            state.write_reparse_if_issuers_approved(payload)
            with self.assertRaises(InsiderApprovalScopeError):
                state.write("reparse-v1", payload)
            with self.assertRaises(InsiderApprovalScopeError):
                state.update("reparse-v1", lambda current: current)

            state.update(
                "approved-issuers-v1",
                lambda current: {**current, "issuer_ciks": ["0000000009"]},
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.update_reparse_if_issuers_approved(
                    lambda current: {**current, "status": "quarantined"}
                )
            self.assertEqual(payload, state.read("reparse-v1"))

    def test_empty_issuer_reparse_state_remains_bound_to_current_approval(
        self,
    ) -> None:
        payload = {
            "contract_version": 1,
            "status": "completed",
            "parser_version": "1.0.0",
            "scope": "issuer",
            "scope_identifier": "0000000001",
            "max_accessions": 1,
            "queue": [],
            "completed_accessions": [],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000009"]},
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_reparse_if_issuers_approved(payload)
            with self.assertRaises(FileNotFoundError):
                state.read("reparse-v1")

        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            state.write_reparse_if_issuers_approved(payload)
            state.update(
                "approved-issuers-v1",
                lambda current: {**current, "issuer_ciks": ["0000000009"]},
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.update_reparse_if_issuers_approved(lambda current: current)
            self.assertEqual(payload, state.read("reparse-v1"))

    def test_issuer_state_rejects_forged_generation_digest_and_generic_mutation(
        self,
    ) -> None:
        payload = issuer_state_payload()
        forged = {**payload, "generation_digest": "0" * 64}
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )

            with self.assertRaisesRegex(InsiderStorageError, "generation digest"):
                state.write_issuer_if_approved("0000000001", forged)
            with self.assertRaises(InsiderApprovalScopeError):
                state.write("issuers/0000000001", payload)

            state.write_issuer_if_approved("0000000001", payload)
            with self.assertRaises(InsiderApprovalScopeError):
                state.update(
                    "issuers/0000000001",
                    lambda current: current,
                )

    def test_issuer_state_write_serializes_concurrent_approval_revocation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            entered = threading.Event()
            release = threading.Event()
            revocation_done = threading.Event()
            publications: list[StoredArtifact] = []
            original_compare = state._compare_and_write_locked

            def paused_compare(*args, **kwargs):
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("issuer publication serialization timed out")
                return original_compare(*args, **kwargs)

            def publish() -> None:
                publications.append(
                    state.write_issuer_if_approved(
                        "0000000001",
                        issuer_state_payload(),
                    )
                )

            def revoke() -> None:
                state.update(
                    "approved-issuers-v1",
                    lambda current: {
                        **current,
                        "issuer_ciks": ["0000000009"],
                    },
                )
                revocation_done.set()

            with patch.object(
                state,
                "_compare_and_write_locked",
                side_effect=paused_compare,
            ):
                publisher = threading.Thread(target=publish)
                publisher.start()
                self.assertTrue(entered.wait(5))
                revoker = threading.Thread(target=revoke)
                revoker.start()
                self.assertFalse(revocation_done.wait(0.2))
                release.set()
                publisher.join(5)
                revoker.join(5)

            self.assertFalse(publisher.is_alive())
            self.assertFalse(revoker.is_alive())
            self.assertTrue(revocation_done.is_set())
            self.assertEqual(1, len(publications))
            self.assertTrue(publications[0].created)
            self.assertEqual(
                issuer_state_payload(),
                state.read("issuers/0000000001"),
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_issuer_if_approved(
                    "0000000001",
                    {
                        **issuer_state_payload(),
                        "owner_groups": [
                            {
                                "owner_group_key": section16_owner_group_key(
                                    ["0000000002"]
                                ),
                                "owner_ciks": ["0000000002"],
                            }
                        ],
                    },
                    expected_sha256=publications[0].sha256,
                )

    def test_atomic_incremental_write_requires_current_durable_issuer_approval(
        self,
    ) -> None:
        entry: dict[str, object] = {
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
            "index_url": filing_index_url(),
            "accepted_at": "2026-01-01T00:00:00Z",
            "observed_at": "2026-01-01T00:00:00Z",
        }
        candidate = self.incremental([entry])
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)

            with self.assertRaises(FileNotFoundError):
                state.write_incremental_if_issuers_approved(candidate)
            self.assertFalse((root / "data").exists())

            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000009"]},
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.write_incremental_if_issuers_approved(candidate)
            with self.assertRaises(FileNotFoundError):
                state.read("incremental-v1")

            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000001"],
                },
            )
            first = state.write_incremental_if_issuers_approved(candidate)
            retry = state.write_incremental_if_issuers_approved(candidate)
            self.assertTrue(first.created)
            self.assertFalse(retry.created)
            self.assertEqual(first.sha256, retry.sha256)

            with self.assertRaisesRegex(InsiderStorageError, "revision is stale"):
                state.write_incremental_if_issuers_approved(
                    {**candidate, "status": "pending"},
                    expected_sha256="0" * 64,
                )
            self.assertEqual(candidate, state.read("incremental-v1"))

            completed = state.update_incremental_if_issuers_approved(
                lambda current: {
                    **current,
                    "status": "completed",
                    "completed_accessions": [ACCESSION],
                }
            )
            self.assertEqual("completed", completed["status"])
            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000009"],
                },
            )
            before = state.read("incremental-v1")
            with self.assertRaises(InsiderApprovalScopeError):
                state.update_incremental_if_issuers_approved(
                    lambda current: {**current, "status": "running"}
                )
            self.assertEqual(before, state.read("incremental-v1"))

    def test_update_reentrant_state_operations_fail_closed_without_deadlock(
        self,
    ) -> None:
        context = multiprocessing.get_context("spawn")
        actions = (
            "read",
            "write",
            "update",
            "write_incremental_if_issuers_approved",
            "update_incremental_if_issuers_approved",
            "publish_if_issuer_approved",
            "write_accession_quarantine_if_issuer_approved",
            "write_issuer_if_approved",
        )
        for action in actions:
            with self.subTest(action=action), tempfile.TemporaryDirectory() as tmpdir:
                results = context.Queue()
                worker = context.Process(
                    target=_reentrant_state_worker,
                    args=(tmpdir, action, results),
                )
                worker.start()
                worker.join(3)
                if worker.is_alive():
                    worker.terminate()
                    worker.join(3)
                    self.fail(
                        f"nested state {action} deadlocked instead of failing closed"
                    )

                self.assertEqual(0, worker.exitcode)
                nested_result = results.get(timeout=2)
                self.assertEqual(
                    ("nested_error", "InsiderStorageError"),
                    nested_result[:2],
                )
                self.assertIn("re-enter", nested_result[2])
                self.assertEqual(("usable", True), results.get(timeout=2))
                results.close()

    def test_only_allowlisted_keys_and_explicit_contract_versions_round_trip(self) -> None:
        self.assertIs(InsiderStateStore, PipelineInsiderStateStore)
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            payload = self.incremental()
            stored = state.write("incremental-v1", payload)
            self.assertEqual(payload, state.read("incremental-v1"))
            self.assertEqual(
                Path(tmpdir).resolve() / "data/insiders/private/state/incremental-v1.json",
                stored.path,
            )
            for key in (
                "../x", "/tmp/x", "pipeline_state.json", "incremental-v1.json",
                "issuers/0000000000", "", "backfill//2026Q1", "backfill/2026Q1.json",
                "telemetry-v1/extra", "telemetry-v1\u2603",
            ):
                with self.subTest(key=key), self.assertRaises(InsiderStorageError):
                    state.write(key, payload)
            for payload_with_bad_version in (
                {**payload, "contract_version": 2},
                {**payload, "unknown": 1},
            ):
                with self.assertRaises(InsiderStorageError):
                    state.write("incremental-v1", payload_with_bad_version)

    def test_all_state_kinds_validate_canonical_safe_schemas(self) -> None:
        examples = {
            "incremental-v1": self.incremental(),
            "reparse-v1": {"contract_version": 1, "status": "incomplete", "parser_version": "1.0.0", "scope": "all", "scope_identifier": None, "max_accessions": 1, "queue": [], "completed_accessions": []},
            "approved-issuers-v1": {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            "backfill/2026Q1": {"contract_version": 1, "quarter": "2026Q1", "issuer_cik": "0000000001", "status": "incomplete", "catalog_url": None, "zip_url": None, "zip_sha256": None, "zip_byte_count": None, "etag": None, "last_modified": None, "table_evidence": [], "missing_optional_tables": [], "selected_accessions": [], "completed_accessions": [], "reconciliation": []},
            "issuers/0000000001": issuer_state_payload(),
            "quarantine/accessions/0000000001-26-000001": {
                "contract_version": 1,
                "stage": "raw",
                "error_class": "InsiderParseError",
                "reason_code": "raw_invalid",
                "retry_count": 0,
                "next_retry_at": None,
                "parser_version": "1.0.0",
                "accession_number": "0000000001-26-000001",
                "issuer_cik": "0000000001",
                "form_type": "4",
                "source_hashes": [],
                **accession_quarantine_identity(),
            },
            "quarantine/quarters/2026Q1": {"contract_version": 1, "quarter": "2026Q1", "stage": "backfill", "error_class": "HTTPError", "reason_code": "backfill_invalid", "retry_count": 0, "next_retry_at": None, "parser_version": None, "source_hashes": []},
            "telemetry-v1": {"contract_version": 1, "counters": {"index_cache_hits": 0}, "recent_runs": []},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            for key, payload in examples.items():
                with self.subTest(key=key):
                    if key.startswith("issuers/"):
                        state.write_issuer_if_approved("0000000001", payload)
                    elif key.startswith("backfill/"):
                        store_backfill_state_for_test(state, payload)
                    elif key == "reparse-v1":
                        store_reparse_state_for_test(state, payload)
                    else:
                        state.write(key, payload)
                    self.assertEqual(payload, state.read(key))
            target = Path(tmpdir) / "data/insiders/private/state/incremental-v1.json"
            for unsafe in (
                b'{"contract_version":1,"contract_version":1}',
                b'{"contract_version":NaN}',
                b'{"completed_accessions":[],"contract_version":1,"first_observed_at":null,"last_observed_at":null,"lookback_seconds":3600,"queue":[],"source_entries":[],"status":"incomplete"} ',
            ):
                target.write_bytes(unsafe)
                target.chmod(0o600)
                with self.assertRaises(InsiderStorageError):
                    state.read("incremental-v1")

    def test_modes_owner_links_and_stale_temps_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            stored = state.write("incremental-v1", self.incremental())
            self.assertEqual(0o600, stored.path.stat().st_mode & 0o777)
            self.assertEqual(os.geteuid(), stored.path.stat().st_uid)
            self.assertEqual(0o700, stored.path.parent.stat().st_mode & 0o777)
            for attack in ("symlink", "hardlink", "directory", "mode"):
                with self.subTest(attack=attack):
                    stored.path.unlink()
                    outside = root / "outside"
                    outside.write_bytes(b"outside")
                    outside.chmod(0o600)
                    if attack == "symlink":
                        stored.path.symlink_to(outside)
                    elif attack == "hardlink":
                        os.link(outside, stored.path)
                    elif attack == "directory":
                        stored.path.mkdir()
                    else:
                        stored.path.write_bytes(b"x")
                        stored.path.chmod(0o644)
                    with self.assertRaises(InsiderStorageError):
                        state.read("incremental-v1")
                    if stored.path.is_dir() or stored.path.is_symlink():
                        if stored.path.is_dir():
                            stored.path.rmdir()
                        else:
                            stored.path.unlink()
                    else:
                        stored.path.unlink()
                    outside.unlink()
                    state.write("incremental-v1", self.incremental())
            safe_temp = stored.path.parent / ".incremental-v1.json.tmp-aaaaaaaaaaaaaaaaaaaaaaaa"
            safe_temp.write_bytes(b"partial")
            safe_temp.chmod(0o600)
            state.write("incremental-v1", self.incremental())
            self.assertFalse(safe_temp.exists())
            unsafe_temp = stored.path.parent / ".incremental-v1.json.tmp-bbbbbbbbbbbbbbbbbbbbbbbb"
            unsafe_temp.write_bytes(b"partial")
            unsafe_temp.chmod(0o644)
            with self.assertRaises(InsiderStorageError):
                state.write("incremental-v1", self.incremental())

    def test_write_orders_file_fsync_replace_parent_fsync_and_interruption_is_readable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            old = self.incremental()
            initial = state.write("incremental-v1", old)
            new = self.incremental([{"accession_number": ACCESSION, "issuer_cik": "0000000001", "index_url": filing_index_url(), "accepted_at": "2026-01-01T00:00:00Z", "observed_at": "2026-01-01T00:00:00Z"}])
            events: list[str] = []
            import insider_storage as storage_module
            real_fsync = storage_module.os.fsync
            real_replace = storage_module.os.replace

            def record_fsync(fd: int) -> None:
                events.append("fsync")
                real_fsync(fd)

            def record_replace(*args: object, **kwargs: object) -> None:
                events.append("replace")
                real_replace(*args, **kwargs)

            with patch.object(storage_module.os, "fsync", side_effect=record_fsync), patch.object(storage_module.os, "replace", side_effect=record_replace):
                updated = state.write(
                    "incremental-v1", new, expected_sha256=initial.sha256
                )
            self.assertEqual(["fsync", "replace", "fsync"], events[-3:])
            with patch.object(storage_module.os, "replace", side_effect=KeyboardInterrupt()):
                with self.assertRaises(KeyboardInterrupt):
                    state.write("incremental-v1", old, expected_sha256=updated.sha256)
            self.assertEqual(new, state.read("incremental-v1"))
            original_read = state.read
            with patch.object(storage_module.os, "fsync", side_effect=[real_fsync] * 5 + [KeyboardInterrupt()]):
                with self.assertRaises(KeyboardInterrupt):
                    state.write("incremental-v1", old, expected_sha256=updated.sha256)
            self.assertEqual(old, original_read("incremental-v1"))

    def test_update_serializes_threaded_writers_without_lost_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("incremental-v1", self.incremental())

            def append(index: int) -> dict[str, object]:
                def transform(payload: dict[str, object]) -> dict[str, object]:
                    result = dict(payload)
                    accession = f"0000000001-26-{index:06d}"
                    entry = incremental_queue_entry(
                        {
                            "accession_number": accession,
                            "issuer_cik": "0000000001",
                            "index_url": filing_index_url(accession),
                            "accepted_at": "2026-01-01T00:00:00Z",
                            "observed_at": "2026-01-01T00:00:00Z",
                        }
                    )
                    source_entry = issuer_source_entry(entry)
                    result["queue"] = sorted(list(payload["queue"]) + [entry], key=lambda item: (item["accepted_at"], item["accession_number"]))
                    result["source_entries"] = sorted(list(payload["source_entries"]) + [source_entry], key=lambda item: (item["accepted_at"], item["accession_number"], item["entity_role"], item["entity_cik"], item["entry_url"], item["observed_at"]))
                    observed = [item["observed_at"] for item in result["source_entries"]]
                    result["first_observed_at"] = min(observed)
                    result["last_observed_at"] = max(observed)
                    result["completed_accessions"] = sorted(list(payload["completed_accessions"]) + [accession])
                    return result
                return state.update("incremental-v1", transform)

            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(append, range(1, 17)))
            final = state.read("incremental-v1")
            self.assertEqual(16, len(final["completed_accessions"]))
            self.assertEqual(16, len(set(final["completed_accessions"])))

    def test_update_blocks_a_second_process_then_preserves_both_updates(self) -> None:
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("incremental-v1", self.incremental())
            locked = context.Event()
            release = context.Event()
            first_attempt = context.Event()
            second_attempt = context.Event()
            first_done = context.Event()
            second_done = context.Event()
            results = context.Queue()
            first = context.Process(
                target=_state_update_worker,
                args=(tmpdir, "0000000001-26-000001", True, locked, release, first_attempt, first_done, results),
            )
            second = context.Process(
                target=_state_update_worker,
                args=(tmpdir, "0000000001-26-000002", False, locked, release, second_attempt, second_done, results),
            )
            first.start()
            self.assertTrue(locked.wait(10))
            second.start()
            self.assertTrue(second_attempt.wait(10))
            self.assertFalse(second_done.wait(0.2))
            release.set()
            first.join(10)
            second.join(10)
            self.assertEqual(0, first.exitcode)
            self.assertEqual(0, second.exitcode)
            self.assertEqual(["ok", "ok"], sorted([results.get(timeout=2), results.get(timeout=2)]))
            self.assertEqual(
                ["0000000001-26-000001", "0000000001-26-000002"],
                state.read("incremental-v1")["completed_accessions"],
            )

    def test_existing_corrupt_state_blocks_cas_write_and_unsafe_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            stored = state.write("incremental-v1", self.incremental())
            stored.path.write_bytes(b'{"contract_version":2}')
            stored.path.chmod(0o600)
            with self.assertRaises(InsiderStorageError):
                state.write("incremental-v1", self.incremental(), expected_sha256=stored.sha256)
            self.assertEqual(b'{"contract_version":2}', stored.path.read_bytes())
            unsafe = {
                "contract_version": 1,
                "quarter": "2026Q1",
                "issuer_cik": "0000000001",
                "status": "incomplete",
                "catalog_url": None,
                "zip_url": None,
                "zip_sha256": None,
                "zip_byte_count": None,
                "etag": "owner address traceback raw xml",
                "last_modified": None,
                "table_evidence": [],
                "missing_optional_tables": [],
                "selected_accessions": [],
                "completed_accessions": [],
                "reconciliation": [],
            }
            with self.assertRaises(InsiderStorageError):
                store_backfill_state_for_test(state, unsafe)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            with self.assertRaises(InsiderStorageError):
                state.write_issuer_if_approved(
                    "0000000001",
                    {
                        **issuer_state_payload(),
                        "owner_groups": [
                            {
                                "owner_group_key": section16_owner_group_key(
                                    ["0000000001"]
                                ),
                                "owner_ciks": ["0000000001"],
                                "remarks": "<ownershipDocument>private</ownershipDocument>",
                            }
                        ],
                    },
                )

    def test_limits_and_unsafe_text_are_rejected_before_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            payload = self.incremental()
            for invalid in (
                self.incremental([{"accession_number": ACCESSION, "issuer_cik": "0000000001", "index_url": filing_index_url(), "accepted_at": "2026-01-01T00:00:00Z", "observed_at": "2026-01-01T00:00:00Z"}] * 1001),
                {**payload, "status": "x" * 4097},
                {**payload, "first_observed_at": "2026-01-01T00:00:00+00:00"},
            ):
                with self.assertRaises(InsiderStorageError):
                    state.write("incremental-v1", invalid)
            self.assertFalse((Path(tmpdir) / "data/insiders/private/state/incremental-v1.json").exists())

    def test_owner_filed_reparse_state_uses_explicit_issuer_authority(self) -> None:
        accession_number = "0000000002-26-000001"
        payload = {
            "contract_version": 1,
            "status": "incomplete",
            "parser_version": "1.0.0",
            "scope": "accession",
            "scope_identifier": accession_number,
            "max_accessions": 1,
            "queue": [
                {
                    "accession_number": accession_number,
                    "issuer_cik": "0000000001",
                }
            ],
            "completed_accessions": [],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            store_reparse_state_for_test(state, payload)

            self.assertEqual(payload, state.read("reparse-v1"))
            approved_payload = {
                "contract_version": 1,
                "issuer_ciks": ["0000000001"],
            }
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": []},
                expected_sha256=hashlib.sha256(
                    canonical_insider_state_json_bytes(approved_payload)
                ).hexdigest(),
            )
            with self.assertRaises(InsiderApprovalScopeError):
                state.update_reparse_if_issuers_approved(
                    lambda current: current,
                )

    def test_reparse_scopes_bind_identifiers_and_limit_accessions(self) -> None:
        cases = (
            (
                "accession",
                ACCESSION,
                [{"accession_number": ACCESSION, "issuer_cik": "0000000001"}],
            ),
            ("issuer", "0000000001", []),
            ("all", None, []),
        )
        for scope, identifier, queue in cases:
            payload = {
                "contract_version": 1, "status": "incomplete", "parser_version": "1.0.0",
                "scope": scope, "scope_identifier": identifier, "max_accessions": 1,
                "queue": queue, "completed_accessions": [],
            }
            with self.subTest(scope=scope), tempfile.TemporaryDirectory() as tmpdir:
                store_reparse_state_for_test(
                    InsiderStateStore(Path(tmpdir)), payload
                )
        invalid_payloads = (
            {
                "contract_version": 1, "status": "incomplete", "parser_version": "1.0.0",
                "scope": "accession", "scope_identifier": "0000000001", "max_accessions": 0,
                "queue": [], "completed_accessions": [],
            },
            {
                "contract_version": 1, "status": "incomplete", "parser_version": "1.0.0",
                "scope": "accession", "scope_identifier": ACCESSION, "max_accessions": 1,
                "queue": [], "completed_accessions": [],
            },
            {
                "contract_version": 1, "status": "incomplete", "parser_version": "1.0.0",
                "scope": "accession", "scope_identifier": ACCESSION, "max_accessions": 1,
                "queue": [
                    {"accession_number": ACCESSION, "issuer_cik": "0000000002"}
                ],
                "completed_accessions": [],
            },
        )
        for invalid in invalid_payloads:
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as tmpdir, self.assertRaises(InsiderStorageError):
                store_reparse_state_for_test(InsiderStateStore(Path(tmpdir)), invalid)

    def test_state_contract_hardening_invariants(self) -> None:
        import insider_storage as storage_module

        entry_early = {"accession_number": "0000000001-26-000002", "issuer_cik": "0000000001", "index_url": filing_index_url("0000000001-26-000002"), "accepted_at": "2026-01-01T00:00:00Z", "observed_at": "2026-01-01T00:00:01Z"}
        entry_late = {"accession_number": ACCESSION, "issuer_cik": "0000000001", "index_url": filing_index_url(), "accepted_at": "2026-01-02T00:00:00Z", "observed_at": "2026-01-02T00:00:01Z"}
        valid_incremental = self.incremental([entry_early, entry_late])
        completed_backfill = {"contract_version": 1, "quarter": "2026Q1", "issuer_cik": "0000000001", "status": "completed", "catalog_url": "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets", "zip_url": "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2026q1.zip", "zip_sha256": "0" * 64, "zip_byte_count": 1, "etag": None, "last_modified": None, "table_evidence": [{"table_name": "SUBMISSION", "headers": ["ACCESSION_NUMBER"], "row_count": 0}], "missing_optional_tables": sorted(storage_module._BACKFILL_OPTIONAL_TABLES), "selected_accessions": [], "completed_accessions": [], "reconciliation": []}
        issuer = issuer_state_payload()
        run = telemetry_run("z")
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("incremental-v1", valid_incremental)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            state.write_backfill_if_issuer_approved(
                "2026Q1", "0000000001", completed_backfill
            )
            state.write("telemetry-v1", {"contract_version": 1, "counters": {"discovery_attempts": 1, "checkpoint_writes": 1}, "recent_runs": [run]})
            state.write_issuer_if_approved("0000000001", {**issuer, "security_classes": [{"security_class_key": section16_security_class_key("0000000001", "AT&T Common Stock", is_derivative=False), "derivative": False, "title": "AT&T Common Stock"}]})
        for url in ("https://sec.gov/Archives/x", "https://data.sec.gov/Archives/x", "https://www.sec.gov/insider-transactions-data-sets-evil", "https://www.sec.gov/Archives/é"):
            with self.subTest(url=url), self.assertRaises(InsiderStorageError):
                storage_module._state_sec_url(url, "URL", prefixes=("/Archives/",))
        explicit_port_url = filing_index_url(explicit_port=True)
        self.assertEqual(
            explicit_port_url,
            storage_module._state_index_url(
                explicit_port_url,
                "URL",
                accession_number=ACCESSION,
                issuer_cik="0000000001",
            ),
        )
        queue_entries = valid_incremental["queue"]
        source_entries = valid_incremental["source_entries"]
        assert isinstance(queue_entries, list) and isinstance(source_entries, list)
        invalid_incrementals = (
            {**valid_incremental, "queue": list(reversed(queue_entries))},
            {
                **valid_incremental,
                "source_entries": [
                    {**source_entries[0], "entity_cik": "0000000002"},
                    source_entries[1],
                ],
            },
            {**valid_incremental, "first_observed_at": entry_late["observed_at"]},
        )
        for payload in invalid_incrementals:
            with self.subTest(incremental=payload), tempfile.TemporaryDirectory() as tmpdir, self.assertRaises(InsiderStorageError):
                InsiderStateStore(Path(tmpdir)).write("incremental-v1", payload)
        duplicate_headers = {**completed_backfill, "status": "incomplete", "table_evidence": [{"table_name": "SUBMISSION", "headers": ["A", "A"], "row_count": 0}]}
        with tempfile.TemporaryDirectory() as tmpdir, self.assertRaises(InsiderStorageError):
            store_backfill_state_for_test(
                InsiderStateStore(Path(tmpdir)), duplicate_headers
            )
        amendment = {"accession_number": ACCESSION, "effective_accession": "0000000001-26-000002", "confidence": "high", "reason_code": "single_candidate", "candidates": ["0000000001-26-000003"]}
        invalid_issuer = with_valid_issuer_generation_digest(
            {**issuer, "amendments": [amendment]}
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            with self.assertRaises(InsiderStorageError):
                state.write_issuer_if_approved("0000000001", invalid_issuer)
        with tempfile.TemporaryDirectory() as tmpdir, self.assertRaises(InsiderStorageError):
            InsiderStateStore(Path(tmpdir)).write("telemetry-v1", {"contract_version": 1, "counters": {"arbitrary_counter": 1}, "recent_runs": [run]})
        with tempfile.TemporaryDirectory() as tmpdir, self.assertRaises(InsiderStorageError):
            InsiderStateStore(Path(tmpdir)).write("quarantine/quarters/0000Q1", {})
        with patch.object(storage_module, "APPROVED_ISSUERS_STATE_CONTRACT_VERSION", 7), patch.dict(storage_module._STATE_CONTRACT_VERSIONS, {"approved": 7}):
            with tempfile.TemporaryDirectory() as tmpdir:
                state = InsiderStateStore(Path(tmpdir))
                state.write("approved-issuers-v1", {"contract_version": 7, "issuer_ciks": []})
                state.write("incremental-v1", self.incremental())

    def test_quarantine_allowlists_and_telemetry_ring_ordering(self) -> None:
        quarantine = {"contract_version": 1, "quarter": "2026Q1", "stage": "checkpoint", "error_class": "TimeoutError", "reason_code": "checkpoint_invalid", "retry_count": 0, "next_retry_at": None, "parser_version": None, "source_hashes": []}
        runs = [
            telemetry_run("z"),
            telemetry_run(
                "a",
                status="running",
                started_at="2026-01-02T00:00:00Z",
                finished_at=None,
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("quarantine/quarters/2026Q1", quarantine)
            state.write("telemetry-v1", {"contract_version": 1, "counters": {"http_attempts": 1, "reparse_completed": 1}, "recent_runs": runs})
        for invalid in ({**quarantine, "stage": "unknown"}, {**quarantine, "error_class": "OSError"}, {**quarantine, "reason_code": "unknown"}):
            with self.subTest(quarantine=invalid), tempfile.TemporaryDirectory() as tmpdir, self.assertRaises(InsiderStorageError):
                InsiderStateStore(Path(tmpdir)).write("quarantine/quarters/2026Q1", invalid)
        unordered = [runs[1], runs[0]]
        duplicate_id = [runs[0], {**runs[1], "run_id": "z"}]
        for invalid in (unordered, duplicate_id):
            with self.subTest(runs=invalid), tempfile.TemporaryDirectory() as tmpdir, self.assertRaises(InsiderStorageError):
                InsiderStateStore(Path(tmpdir)).write("telemetry-v1", {"contract_version": 1, "counters": {}, "recent_runs": invalid})

    def test_etag_accepts_valid_opaque_tags_and_rejects_malformed_values(self) -> None:
        import insider_storage as storage_module

        for etag in ('"abc123"', 'W/"x7f-visible!#[]"'):
            with self.subTest(etag=etag):
                self.assertEqual(etag, storage_module._state_etag(etag, "etag"))
        for etag in (
            "abc123", "W/abc123", 'w/"abc"', '"a"b"',
            '"abc' + chr(0) + '"', '"abc' + chr(127) + '"',
            '"snowman-☃"',
        ):
            with self.subTest(etag=etag):
                with self.assertRaises(InsiderStorageError):
                    storage_module._state_etag(etag, "etag")

    @staticmethod
    def filing_index_url(
        accession: str = ACCESSION,
        issuer_cik: str = "0000000001",
        *,
        explicit_port: bool = False,
    ) -> str:
        authority = "www.sec.gov:443" if explicit_port else "www.sec.gov"
        archive_cik = str(int(issuer_cik))
        compact_accession = accession.replace("-", "")
        return (
            f"https://{authority}/Archives/edgar/data/{archive_cik}/"
            f"{compact_accession}/{accession}-index.html"
        )

    def test_state_contract_helpers_require_explicit_versions(self) -> None:
        import insider_storage as storage_module

        with self.assertRaises(TypeError):
            storage_module._state_exact_keys(
                {"contract_version": 1}, {"contract_version"}
            )

    def test_incremental_and_telemetry_compare_timestamps_as_instants(self) -> None:
        entry = {
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
            "index_url": self.filing_index_url(),
            "accepted_at": "2026-01-01T00:00:00.1Z",
            "observed_at": "2026-01-01T00:00:00Z",
        }
        invalid_incremental = self.incremental([entry])
        invalid_incremental["first_observed_at"] = entry["observed_at"]
        invalid_incremental["last_observed_at"] = entry["observed_at"]
        invalid_telemetry = {
            "contract_version": 1,
            "counters": {},
            "recent_runs": [
                telemetry_run(
                    "fractional-order",
                    started_at="2026-01-01T00:00:00.1Z",
                    finished_at="2026-01-01T00:00:00Z",
                )
            ],
        }
        for key, payload in (
            ("incremental-v1", invalid_incremental),
            ("telemetry-v1", invalid_telemetry),
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    InsiderStateStore(Path(tmpdir)).write(key, payload)

    def test_fractional_timestamps_sort_chronologically(self) -> None:
        whole_second = {
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
            "index_url": self.filing_index_url(),
            "accepted_at": "2026-01-01T00:00:00Z",
            "observed_at": "2026-01-01T00:00:00Z",
        }
        fractional_second = {
            "accession_number": "0000000001-26-000002",
            "issuer_cik": "0000000001",
            "index_url": self.filing_index_url("0000000001-26-000002"),
            "accepted_at": "2026-01-01T00:00:00.1Z",
            "observed_at": "2026-01-01T00:00:00.1Z",
        }
        incremental = self.incremental([whole_second, fractional_second])
        incremental.update(
            {
                "first_observed_at": whole_second["observed_at"],
                "last_observed_at": fractional_second["observed_at"],
            }
        )
        telemetry = {
            "contract_version": 1,
            "counters": {},
            "recent_runs": [
                telemetry_run(
                    "whole-second",
                    finished_at="2026-01-01T00:00:00Z",
                ),
                telemetry_run(
                    "fractional-second",
                    started_at="2026-01-01T00:00:00.1Z",
                    finished_at="2026-01-01T00:00:00.1Z",
                ),
            ],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("incremental-v1", incremental)
            state.write("telemetry-v1", telemetry)

    def test_incremental_index_urls_are_exact_and_bound_to_entry(self) -> None:
        valid_entry = {
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
            "index_url": self.filing_index_url(explicit_port=True),
            "accepted_at": "2026-01-01T00:00:00Z",
            "observed_at": "2026-01-01T00:00:00Z",
        }
        for index_url in (
            valid_entry["index_url"],
            self.filing_index_url().replace("-index.html", "-index.htm"),
        ):
            valid = self.incremental([{**valid_entry, "index_url": index_url}])
            with self.subTest(valid_index_url=index_url), tempfile.TemporaryDirectory() as tmpdir:
                InsiderStateStore(Path(tmpdir)).write("incremental-v1", valid)

        wrong_accession = "0000000001-26-000002"
        invalid_urls = (
            "https://www.sec.gov/Archives/not-an-index",
            self.filing_index_url(issuer_cik="0000000002"),
            self.filing_index_url(accession=wrong_accession),
            self.filing_index_url().replace("www.sec.gov/", "www.sec.gov:/"),
            self.filing_index_url().replace("-index.html", "-index.ht"),
            self.filing_index_url().replace("-index.html", "-index.htmlx"),
        )
        for index_url in invalid_urls:
            entry = {**valid_entry, "index_url": index_url}
            payload = self.incremental([entry])
            with self.subTest(index_url=index_url), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    InsiderStateStore(Path(tmpdir)).write("incremental-v1", payload)

    def test_backfill_completed_evidence_tracks_optional_absence(self) -> None:
        import insider_storage as storage_module

        completed = {
            "contract_version": 1,
            "quarter": "2006Q1",
            "issuer_cik": "0000000001",
            "status": "completed",
            "catalog_url": "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
            "zip_url": "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2006q1.zip",
            "zip_sha256": "0" * 64,
            "zip_byte_count": 1,
            "etag": None,
            "last_modified": None,
            "table_evidence": [
                {
                    "table_name": "SUBMISSION",
                    "headers": ["ACCESSION_NUMBER"],
                    "row_count": 1,
                }
            ],
            "missing_optional_tables": sorted(
                storage_module._BACKFILL_OPTIONAL_TABLES
            ),
            "selected_accessions": [],
            "completed_accessions": [],
            "reconciliation": [],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            store_backfill_state_for_test(state, completed, quarter="2006Q1")
            self.assertEqual(completed, state.read("backfill/2006Q1"))

    def test_backfill_evidence_rejects_empty_unknown_and_unbound_tables(self) -> None:
        import insider_storage as storage_module

        base = {
            "contract_version": 1,
            "quarter": "2026Q1",
            "issuer_cik": "0000000001",
            "status": "completed",
            "catalog_url": "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
            "zip_url": "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2026q1.zip",
            "zip_sha256": "0" * 64,
            "zip_byte_count": 1,
            "etag": None,
            "last_modified": None,
            "table_evidence": [
                {
                    "table_name": name,
                    "headers": [],
                    "row_count": 0,
                }
                for name in sorted(storage_module._REQUIRED_BACKFILL_TABLES)
            ],
            "missing_optional_tables": sorted(
                storage_module._BACKFILL_OPTIONAL_TABLES
            ),
            "selected_accessions": [],
            "completed_accessions": [],
            "reconciliation": [],
        }
        invalids = (
            base,
            {
                **base,
                "status": "incomplete",
                "table_evidence": [
                    {"table_name": "UNDECLARED", "headers": ["A"], "row_count": 0}
                ],
            },
            {
                **base,
                "status": "incomplete",
                "table_evidence": [],
                "zip_url": "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2025q4.zip",
            },
        )
        for payload in invalids:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    store_backfill_state_for_test(
                        InsiderStateStore(Path(tmpdir)), payload
                    )

    def test_reparse_queue_is_bounded_and_self_validating(self) -> None:
        accession_two = "0000000001-26-000002"
        queue_entry = {
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
        }
        valid_payloads = (
            {
                "contract_version": 1,
                "status": "incomplete",
                "parser_version": "1.0.0",
                "scope": "accession",
                "scope_identifier": ACCESSION,
                "max_accessions": 1,
                "queue": [queue_entry],
                "completed_accessions": [],
            },
            {
                "contract_version": 1,
                "status": "incomplete",
                "parser_version": "1.0.0",
                "scope": "issuer",
                "scope_identifier": "0000000001",
                "max_accessions": 1,
                "queue": [queue_entry],
                "completed_accessions": [ACCESSION],
            },
            {
                "contract_version": 1,
                "status": "incomplete",
                "parser_version": "1.0.0",
                "scope": "all",
                "scope_identifier": None,
                "max_accessions": 1,
                "queue": [queue_entry],
                "completed_accessions": [],
            },
        )
        for payload in valid_payloads:
            with self.subTest(scope=payload["scope"]), tempfile.TemporaryDirectory() as tmpdir:
                state = InsiderStateStore(Path(tmpdir))
                store_reparse_state_for_test(state, payload)
                self.assertEqual(payload, state.read("reparse-v1"))

        legacy_invalids = (
            {
                **valid_payloads[0],
                "queue": [accession_two],
            },
            {
                **valid_payloads[2],
                "queue": [ACCESSION, accession_two],
            },
        )
        for payload in legacy_invalids:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    store_reparse_state_for_test(
                        InsiderStateStore(Path(tmpdir)), payload
                    )

        invalid_entries = (
            {
                **valid_payloads[0],
                "queue": [{**queue_entry, "accession_number": accession_two}],
            },
            {
                **valid_payloads[0],
                "status": "completed",
                "queue": [],
                "completed_accessions": [],
            },
            {
                **valid_payloads[1],
                "queue": [{**queue_entry, "issuer_cik": "0000000002"}],
            },
            {
                **valid_payloads[2],
                "completed_accessions": [accession_two],
            },
        )
        for payload in invalid_entries:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    store_reparse_state_for_test(
                        InsiderStateStore(Path(tmpdir)), payload
                    )

    def test_issuer_amendment_references_are_closed_and_combinations_valid(self) -> None:
        accession_two = "0000000001-26-000002"
        accession_three = "0000000001-26-000003"

        def accession_entry(accession: str) -> dict[str, object]:
            return {
                "accession_number": accession,
                "parser_version": "1.0.0",
                "normalized_sha256": "0" * 64,
            }

        issuer = with_valid_issuer_generation_digest({
            "contract_version": 1,
            "issuer_cik": "0000000001",
            "accessions": [accession_entry(ACCESSION), accession_entry(accession_two)],
            "owner_groups": [],
            "security_classes": [],
            "amendments": [
                {
                    "accession_number": ACCESSION,
                    "effective_accession": accession_two,
                    "confidence": "high",
                    "reason_code": "single_candidate",
                    "candidates": [accession_two],
                }
            ],
            "unresolved_ambiguities": [],
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            state.write_issuer_if_approved("0000000001", issuer)

        unresolved_amendment = {
            "accession_number": ACCESSION,
            "effective_accession": None,
            "confidence": "unresolved",
            "reason_code": "ambiguous_candidates",
            "candidates": [accession_two, accession_three],
        }
        ambiguity_summary = {
            "accession_number": ACCESSION,
            "reason_code": "ambiguous_candidates",
            "candidates": [accession_two, accession_three],
        }
        issuer_with_ambiguity = with_valid_issuer_generation_digest({
            **issuer,
            "accessions": [
                accession_entry(ACCESSION),
                accession_entry(accession_two),
                accession_entry(accession_three),
            ],
            "amendments": [unresolved_amendment],
            "unresolved_ambiguities": [ambiguity_summary],
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            state.write_issuer_if_approved("0000000001", issuer_with_ambiguity)

        invalids = (
            {**issuer, "accessions": []},
            {
                **issuer,
                "accessions": [
                    accession_entry(ACCESSION),
                    {
                        **accession_entry(ACCESSION),
                        "parser_version": "2.0.0",
                    },
                ],
                "amendments": [],
            },
            {
                **issuer,
                "accessions": [accession_entry(ACCESSION)],
                "amendments": [
                    {
                        "accession_number": ACCESSION,
                        "effective_accession": ACCESSION,
                        "confidence": "high",
                        "reason_code": "single_candidate",
                        "candidates": [ACCESSION],
                    }
                ],
            },
            {**issuer_with_ambiguity, "unresolved_ambiguities": []},
            {**issuer_with_ambiguity, "amendments": []},
            {
                **issuer_with_ambiguity,
                "unresolved_ambiguities": [
                    {
                        **ambiguity_summary,
                        "reason_code": "no_candidate",
                        "candidates": [],
                    }
                ],
            },
            {
                **issuer,
                "amendments": [
                    {
                        "accession_number": ACCESSION,
                        "effective_accession": None,
                        "confidence": "unresolved",
                        "reason_code": "single_candidate",
                        "candidates": [accession_two],
                    }
                ],
            },
            {
                **issuer,
                "unresolved_ambiguities": [
                    {
                        "accession_number": ACCESSION,
                        "reason_code": "ambiguous_candidates",
                        "candidates": [accession_two, accession_three],
                    }
                ],
            },
        )
        for payload in invalids:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as tmpdir:
                state = InsiderStateStore(Path(tmpdir))
                state.write(
                    "approved-issuers-v1",
                    {"contract_version": 1, "issuer_ciks": ["0000000001"]},
                )
                with self.assertRaises(InsiderStorageError):
                    state.write_issuer_if_approved(
                        "0000000001",
                        with_valid_issuer_generation_digest(payload),
                    )

    def test_stale_cas_is_rejected_even_when_payload_is_identical(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            payload = self.incremental()
            state.write("incremental-v1", payload)
            with self.assertRaisesRegex(InsiderStorageError, "revision"):
                state.write(
                    "incremental-v1",
                    payload,
                    expected_sha256="0" * 64,
                )

    def test_deep_json_is_normalized_to_storage_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            stored = state.write("incremental-v1", self.incremental())
            stored.path.write_text("[" * 1_100 + "0" + "]" * 1_100)
            stored.path.chmod(0o600)
            with self.assertRaisesRegex(InsiderStorageError, "JSON"):
                state.read("incremental-v1")

    def test_telemetry_allowlist_covers_planned_outcomes(self) -> None:
        planned_counters = {
            "discovered_accession_groups",
            "parse_successes",
            "reporting_owner_rows",
            "non_derivative_transaction_rows",
            "non_derivative_holding_rows",
            "derivative_transaction_rows",
            "derivative_holding_rows",
            "footnote_rows",
            "owner_signature_rows",
            "unknown_elements",
            "amendments_resolved",
            "amendments_unresolved",
            "limiter_utilization",
            "backfill_source_quarters",
            "backfill_source_hashes",
            "backfill_tables",
            "checkpoint_failures",
            "reparse_failures",
        }
        payload = {
            "contract_version": 1,
            "counters": {name: 0 for name in sorted(planned_counters)},
            "recent_runs": [],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("telemetry-v1", payload)
            self.assertEqual(payload, state.read("telemetry-v1"))

    def test_quarantine_reason_codes_are_bound_to_stages(self) -> None:
        invalid = {
            "contract_version": 1,
            "quarter": "2026Q1",
            "stage": "checkpoint",
            "error_class": "InsiderStorageError",
            "reason_code": "raw_invalid",
            "retry_count": 0,
            "next_retry_at": None,
            "parser_version": None,
            "source_hashes": [],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(InsiderStorageError):
                InsiderStateStore(Path(tmpdir)).write(
                    "quarantine/quarters/2026Q1", invalid
                )

    def test_incremental_state_retains_issuer_and_reporting_atom_evidence(self) -> None:
        accepted_at = "2026-01-01T00:00:00Z"
        observed_at = "2026-01-01T00:00:01Z"
        queue_entry = {
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
            "form_type": "4",
            "index_url": self.filing_index_url(),
            "accepted_at": accepted_at,
            "observed_at": observed_at,
        }
        issuer_evidence = {
            "accession_number": ACCESSION,
            "form_type": "4",
            "entity_role": "issuer",
            "entity_cik": "0000000001",
            "entry_url": self.filing_index_url(),
            "accepted_at": accepted_at,
            "observed_at": observed_at,
        }
        reporting_evidence = {
            "accession_number": ACCESSION,
            "form_type": "4",
            "entity_role": "reporting_owner",
            "entity_cik": "0000000002",
            "entry_url": self.filing_index_url(issuer_cik="0000000002"),
            "accepted_at": accepted_at,
            "observed_at": observed_at,
        }
        payload = {
            "contract_version": 1,
            "status": "incomplete",
            "lookback_seconds": 3600,
            "first_observed_at": observed_at,
            "last_observed_at": observed_at,
            "queue": [queue_entry],
            "completed_accessions": [],
            "source_entries": [issuer_evidence, reporting_evidence],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("incremental-v1", payload)
            self.assertEqual(payload, state.read("incremental-v1"))

        invalid_payloads = (
            {
                **payload,
                "source_entries": [reporting_evidence],
            },
            {
                **payload,
                "source_entries": [
                    issuer_evidence,
                    {**issuer_evidence, "entity_cik": "0000000002"},
                ],
            },
            {
                **payload,
                "source_entries": [
                    issuer_evidence,
                    {**reporting_evidence, "form_type": "4/A"},
                ],
            },
        )
        for invalid in invalid_payloads:
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    InsiderStateStore(Path(tmpdir)).write("incremental-v1", invalid)

    def test_incremental_state_rejects_orphan_source_evidence(self) -> None:
        queue_entry: dict[str, object] = {
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
            "form_type": "4",
            "index_url": self.filing_index_url(),
            "accepted_at": "2026-01-01T00:00:00Z",
            "observed_at": "2026-01-01T00:00:01Z",
        }
        issuer_evidence = issuer_source_entry(queue_entry)
        reporting_evidence = {
            **issuer_evidence,
            "entity_role": "reporting_owner",
            "entity_cik": "0000000002",
            "entry_url": self.filing_index_url(issuer_cik="0000000002"),
        }
        for source_entry in (issuer_evidence, reporting_evidence):
            payload = {
                "contract_version": 1,
                "status": "incomplete",
                "lookback_seconds": 3600,
                "first_observed_at": source_entry["observed_at"],
                "last_observed_at": source_entry["observed_at"],
                "queue": [],
                "completed_accessions": [],
                "source_entries": [source_entry],
            }
            with self.subTest(source_entry=source_entry), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    InsiderStateStore(Path(tmpdir)).write("incremental-v1", payload)

    def test_completed_resumable_states_require_every_selected_accession(self) -> None:
        import insider_storage as storage_module

        queue_entry: dict[str, object] = {
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
            "index_url": self.filing_index_url(),
            "accepted_at": "2026-01-01T00:00:00Z",
            "observed_at": "2026-01-01T00:00:01Z",
        }
        incremental = {
            **self.incremental([queue_entry]),
            "status": "completed",
        }
        reparse = {
            "contract_version": 1,
            "status": "completed",
            "parser_version": "1.0.0",
            "scope": "all",
            "scope_identifier": None,
            "max_accessions": 1,
            "queue": [
                {"accession_number": ACCESSION, "issuer_cik": "0000000001"}
            ],
            "completed_accessions": [],
        }
        backfill = {
            "contract_version": 1,
            "quarter": "2026Q1",
            "issuer_cik": "0000000001",
            "status": "completed",
            "catalog_url": "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
            "zip_url": "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2026q1.zip",
            "zip_sha256": "0" * 64,
            "zip_byte_count": 1,
            "etag": None,
            "last_modified": None,
            "table_evidence": [
                {
                    "table_name": "SUBMISSION",
                    "headers": ["ACCESSION_NUMBER"],
                    "row_count": 1,
                }
            ],
            "missing_optional_tables": sorted(storage_module._BACKFILL_OPTIONAL_TABLES),
            "selected_accessions": [ACCESSION],
            "completed_accessions": [],
            "reconciliation": [],
        }
        cases = (
            ("incremental-v1", incremental),
            ("reparse-v1", reparse),
            ("backfill/2026Q1", backfill),
        )

        def write_case(repository_root: Path, key: str, payload: object) -> None:
            state = InsiderStateStore(repository_root)
            if key.startswith("backfill/"):
                store_backfill_state_for_test(state, payload)
            elif key == "reparse-v1":
                store_reparse_state_for_test(state, payload)
            else:
                state.write(key, payload)

        for key, payload in cases:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    write_case(Path(tmpdir), key, payload)

            completed = {**payload, "completed_accessions": [ACCESSION]}
            with self.subTest(key=key, status="completed"), tempfile.TemporaryDirectory() as tmpdir:
                write_case(Path(tmpdir), key, completed)

            for status in ("incomplete", "pending", "running", "failed", "quarantined"):
                resumable = {**payload, "status": status}
                with self.subTest(key=key, status=status), tempfile.TemporaryDirectory() as tmpdir:
                    write_case(Path(tmpdir), key, resumable)

    def test_telemetry_retains_bounded_allowlisted_run_summaries(self) -> None:
        retry_example = telemetry_example(
            "0000000001-26-000002",
            stage="raw",
            outcome="retry_later",
            error_class="TimeoutError",
            reason_code="timeout",
            retry_count=1,
            next_retry_at="2026-01-02T00:00:00Z",
        )
        run = telemetry_run(
            "run-001",
            counters={"discovered_accession_groups": 2, "raw_fetches": 1},
            accession_examples=[telemetry_example(), retry_example],
        )
        payload = {
            "contract_version": 1,
            "counters": {"discovery_attempts": 1},
            "recent_runs": [run],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write("telemetry-v1", payload)
            self.assertEqual(payload, state.read("telemetry-v1"))

        invalid_runs = (
            {**run, "counters": {"unknown_counter": 1}},
            {
                **run,
                "accession_examples": [
                    {**telemetry_example(), "private_details": "raw XML owner address"}
                ],
            },
            {
                **run,
                "accession_examples": [
                    telemetry_example(outcome="retry_later")
                ],
            },
            telemetry_run(
                "run-001",
                status="running",
                finished_at="2026-01-01T00:00:01Z",
            ),
            telemetry_run("run-001", status="completed", finished_at=None),
        )
        for invalid_run in invalid_runs:
            invalid = {**payload, "recent_runs": [invalid_run]}
            with self.subTest(invalid_run=invalid_run), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    InsiderStateStore(Path(tmpdir)).write("telemetry-v1", invalid)

        too_many_runs = [
            telemetry_run(f"run-{index:03d}") for index in range(101)
        ]
        too_many_examples = [
            telemetry_example(f"0000000001-26-{index:06d}")
            for index in range(1, 27)
        ]
        for recent_runs in (
            too_many_runs,
            [telemetry_run("run-001", accession_examples=too_many_examples)],
        ):
            invalid = {**payload, "recent_runs": recent_runs}
            with self.subTest(count=len(recent_runs)), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    InsiderStateStore(Path(tmpdir)).write("telemetry-v1", invalid)

    def test_accession_quarantine_identity_is_exact_and_canonical(self) -> None:
        valid = {
            "contract_version": 1,
            "accession_number": ACCESSION,
            "stage": "raw",
            "error_class": "InsiderParseError",
            "reason_code": "raw_invalid",
            "retry_count": 0,
            "next_retry_at": None,
            "parser_version": "1.0.0",
            "issuer_cik": "0000000001",
            "form_type": "4",
            "source_hashes": [],
            **accession_quarantine_identity(),
        }
        missing_index_url = dict(valid)
        del missing_index_url["index_url"]
        invalid_cases = (
            missing_index_url,
            {**valid, "index_url": filing_index_url(explicit_port=True)},
            {
                **valid,
                "index_url": filing_index_url("0000000001-26-000002"),
            },
            {**valid, "index_url": filing_index_url(issuer_cik="0000000009")},
            {**valid, "accepted_at": "2026-01-16T16:30:00+00:00"},
            {**valid, "reporting_owner_ciks": ["0000000003", "0000000002"]},
            {**valid, "reporting_owner_ciks": ["0000000002", "0000000002"]},
            {**valid, "reporting_owner_ciks": ["2"]},
            {**valid, "index_url": None},
            {**valid, "accepted_at": None},
            {
                **valid,
                "index_url": None,
                "accepted_at": None,
                "reporting_owner_ciks": ["0000000002"],
            },
        )
        key = f"quarantine/accessions/{ACCESSION}"
        for invalid in invalid_cases:
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    InsiderStateStore(Path(tmpdir)).write(key, invalid)

        generic = {
            **valid,
            "issuer_cik": None,
            "form_type": None,
            "index_url": None,
            "accepted_at": None,
            "reporting_owner_ciks": [],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(key, generic)
            self.assertEqual(generic, state.read(key))

    def test_quarantine_retry_schedule_matches_transient_semantics(self) -> None:
        transient = {
            "contract_version": 1,
            "accession_number": ACCESSION,
            "stage": "discovery",
            "error_class": "TimeoutError",
            "reason_code": "timeout",
            "retry_count": 1,
            "next_retry_at": None,
            "parser_version": None,
            "issuer_cik": "0000000001",
            "form_type": "4",
            "source_hashes": [],
            **accession_quarantine_identity(),
        }
        deterministic_with_schedule = {
            **transient,
            "error_class": "InsiderIndexParseError",
            "reason_code": "discovery_invalid",
            "next_retry_at": "2026-01-02T00:00:00Z",
        }
        valid_transient = {
            **transient,
            "next_retry_at": "2026-01-02T00:00:00Z",
        }
        for invalid in (transient, deterministic_with_schedule):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(InsiderStorageError):
                    InsiderStateStore(Path(tmpdir)).write(
                        f"quarantine/accessions/{ACCESSION}", invalid
                    )
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(f"quarantine/accessions/{ACCESSION}", valid_transient)
            self.assertEqual(
                valid_transient,
                state.read(f"quarantine/accessions/{ACCESSION}"),
            )


if __name__ == "__main__":
    unittest.main()
