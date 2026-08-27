from __future__ import annotations

import asyncio
import concurrent.futures
from contextlib import ExitStack, contextmanager
from dataclasses import fields as dataclass_fields, replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from typing import cast
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

import requests

from scripts import refresh_recent_insider_filings as refresh_script
import insider_pipeline
from insider_contract import InsiderContractError, canonical_insider_json_bytes
from insider_parser import INSIDER_PARSER_VERSION
from insider_storage import (
    InsiderApprovalScopeError,
    InsiderStateStore,
    InsiderStorage,
    InsiderStorageError,
)
from security_identity import section16_owner_group_key, section16_security_class_key

from insider_pipeline import (
    CURRENT_FILINGS_URL,
    MAX_RECENT_INSIDER_ATOM_BYTES,
    SECTION16_CURRENT_FORMS,
    InsiderDiscoveryError,
    InsiderIndexParseError,
    build_insider_source_metadata,
    build_recent_insider_feed_url,
    canonical_source_metadata_json_bytes,
    discover_recent_insider_accessions,
    group_recent_insider_entries,
    parse_recent_insider_atom,
    parse_insider_filing_index,
    persist_incremental_discovery_queue,
    validate_insider_source_metadata,
)


ACCESSION = "0000000001-26-000001"
INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/"
    "0000000001-26-000001-index.html"
)
FIXTURE = Path(__file__).parent / "fixtures" / "insider_ingestion" / "form4_index.html"
RAW_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "insider_filings"
    / "form4_simple_purchase.xml"
)
JOINT_RAW_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "insider_filings"
    / "form4_joint_sale_derivative.xml"
)
AMENDMENT_RAW_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "insider_filings"
    / "form4_amendment.xml"
)
DOCUMENT_URL = (
    "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/"
    "form4-synthetic.xml"
)
OWNER_ENTRY_URL = (
    "https://www.sec.gov/Archives/edgar/data/2/000000000126000001/"
    "owner-entry.html"
)


class _HostileHTTPFailure(BaseException):
    """Synthetic non-control-flow BaseException from an alternate HTTP client."""


class _AtomResponse:
    def __init__(
        self,
        content: bytes,
        *,
        url: str,
        status_code: int = 200,
        content_length: int | None = None,
    ) -> None:
        self.content = content
        self.url = url
        self.status_code = status_code
        self.headers = (
            {} if content_length is None else {"Content-Length": str(content_length)}
        )
        self.closed = False

    def iter_content(self, chunk_size: int = 8192):
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset : offset + chunk_size]

    def close(self) -> None:
        self.closed = True


class _AtomHTTP:
    def __init__(self, responder) -> None:
        self.responder = responder
        self.calls: list[str] = []
        self.request_kwargs: list[dict[str, object]] = []
        self.responses: list[_AtomResponse] = []

    def get(self, url: str, **kwargs: object) -> _AtomResponse:
        self.calls.append(url)
        self.request_kwargs.append(dict(kwargs))
        response = self.responder(url)
        self.responses.append(response)
        return response


class _ProcessorHTTP:
    def __init__(
        self,
        responses: dict[str, bytes | BaseException],
        events: list[str] | None = None,
    ) -> None:
        self._responses = responses
        self._events = events
        self.calls: list[str] = []
        self.request_kwargs: list[dict[str, object]] = []
        self.responses: list[_AtomResponse] = []

    def get(self, url: str, **kwargs: object) -> _AtomResponse:
        self.calls.append(url)
        self.request_kwargs.append(dict(kwargs))
        if self._events is not None:
            self._events.append("http_index" if url == INDEX_URL else "http_document")
        if url not in self._responses:
            raise AssertionError(f"unexpected processor HTTP request: {url}")
        value = self._responses[url]
        if isinstance(value, BaseException):
            raise value
        response = _AtomResponse(value, url=url)
        self.responses.append(response)
        return response


class _RecordingInsiderStorage(InsiderStorage):
    def __init__(self, repository_root: Path, events: list[str]) -> None:
        super().__init__(repository_root)
        self.events = events

    def read_index_html(self, accession_number: str) -> bytes:
        self.events.append("read_index_html")
        return super().read_index_html(accession_number)

    def store_index_html(
        self, accession_number: str, index_html_bytes: bytes
    ):
        self.events.append("store_index_html")
        return super().store_index_html(accession_number, index_html_bytes)

    def read_raw(self, accession_number: str) -> bytes:
        self.events.append("read_raw")
        return super().read_raw(accession_number)

    def store_raw(self, accession_number: str, xml_bytes: bytes):
        self.events.append("store_raw")
        return super().store_raw(accession_number, xml_bytes)

    def store_source_metadata(self, accession_number: str, metadata: object):
        self.events.append("store_source_metadata")
        return super().store_source_metadata(accession_number, metadata)

    def read_source_metadata(self, accession_number: str) -> dict[str, object]:
        self.events.append("read_source_metadata")
        return super().read_source_metadata(accession_number)

    def store_normalized(
        self,
        accession_number: str,
        parser_version: str,
        payload: object,
    ):
        self.events.append("store_normalized")
        return super().store_normalized(
            accession_number,
            parser_version,
            payload,
        )

    def read_normalized(
        self, accession_number: str, parser_version: str
    ) -> dict[str, object]:
        self.events.append("read_normalized")
        return super().read_normalized(accession_number, parser_version)


class _PausingFinalReadStorage(_RecordingInsiderStorage):
    def __init__(
        self,
        repository_root: Path,
        events: list[str],
        verified: threading.Event,
        resume: threading.Event,
    ) -> None:
        super().__init__(repository_root, events)
        self.verified = verified
        self.resume = resume

    def read_normalized(
        self, accession_number: str, parser_version: str
    ) -> dict[str, object]:
        normalized = super().read_normalized(accession_number, parser_version)
        self.verified.set()
        if not self.resume.wait(5):
            raise RuntimeError("authorization race test timed out")
        return normalized


class _PausingSourceReadStorage(_RecordingInsiderStorage):
    def __init__(
        self,
        repository_root: Path,
        events: list[str],
        source_read: threading.Event,
        resume: threading.Event,
    ) -> None:
        super().__init__(repository_root, events)
        self.source_read = source_read
        self.resume = resume

    def read_source_metadata(self, accession_number: str) -> dict[str, object]:
        source = super().read_source_metadata(accession_number)
        self.source_read.set()
        if not self.resume.wait(5):
            raise RuntimeError("source-read authorization race test timed out")
        return source


class _ConflictingRawPublishStorage(_RecordingInsiderStorage):
    conflicting_raw = (
        b"<ownershipDocument><documentType>4</documentType></ownershipDocument>"
    )

    def store_raw(self, accession_number: str, xml_bytes: bytes):
        InsiderStorage.store_raw(self, accession_number, self.conflicting_raw)
        return super().store_raw(accession_number, xml_bytes)


class _RecordingInsiderStateStore(InsiderStateStore):
    def __init__(self, repository_root: Path, events: list[str]) -> None:
        super().__init__(repository_root)
        self.events = events

    def update(self, key: str, transform):
        self.events.append("state_update")
        return super().update(key, transform)

    def update_incremental_if_issuers_approved(self, transform):
        self.events.append("state_update")
        return super().update_incremental_if_issuers_approved(transform)

    def write_issuer_if_approved(
        self,
        issuer_cik: str,
        payload: object,
        *,
        expected_sha256: str | None = None,
    ):
        self.events.append("write_issuer")
        return super().write_issuer_if_approved(
            issuer_cik,
            payload,
            expected_sha256=expected_sha256,
        )


@contextmanager
def _record_processor_functions(events: list[str]):
    parse_index = insider_pipeline.parse_insider_filing_index
    parse_raw = insider_pipeline.parse_ownership_xml
    build_source = insider_pipeline.build_insider_source_metadata

    def recorded_parse_index(*args, **kwargs):
        events.append("parse_index")
        return parse_index(*args, **kwargs)

    def recorded_parse_raw(*args, **kwargs):
        events.append("parse_raw")
        return parse_raw(*args, **kwargs)

    def recorded_build_source(*args, **kwargs):
        events.append("build_source_metadata")
        return build_source(*args, **kwargs)

    with (
        patch.object(
            insider_pipeline,
            "parse_insider_filing_index",
            side_effect=recorded_parse_index,
        ),
        patch.object(
            insider_pipeline,
            "parse_ownership_xml",
            side_effect=recorded_parse_raw,
        ),
        patch.object(
            insider_pipeline,
            "build_insider_source_metadata",
            side_effect=recorded_build_source,
        ),
    ):
        yield


class InsiderIndexMetadataTests(unittest.TestCase):
    def parse(self, html: bytes | None = None, **overrides: object) -> dict[str, object]:
        return parse_insider_filing_index(
            FIXTURE.read_bytes() if html is None else html,
            index_url=overrides.pop("index_url", INDEX_URL),
            accession_number=overrides.pop("accession_number", ACCESSION),
            issuer_cik=overrides.pop("issuer_cik", "0000000001"),
            reporting_owner_ciks=overrides.pop("reporting_owner_ciks", ("0000000002",)),
            **overrides,
        )

    def test_parses_official_style_index_into_deterministic_metadata(self) -> None:
        metadata = self.parse()
        self.assertEqual(
            {
                "accession_number": ACCESSION,
                "form_type": "4",
                "filing_date": "2026-01-16",
                "accepted_at": "2026-01-16T16:30:00Z",
                "issuer_cik": "0000000001",
                "reporting_owner_ciks": ["0000000002"],
                "index_url": INDEX_URL,
                "index_archive_cik": "0000000001",
                "document_url": INDEX_URL.rsplit("/", 1)[0] + "/form4-synthetic.xml",
                "document_archive_cik": "0000000001",
                "document_archive_cik_role": "issuer",
                "document_sequence": "1",
                "document_type": "4",
                "document_filename": "form4-synthetic.xml",
            },
            metadata,
        )

    def test_accepts_current_sec_index_htm_url(self) -> None:
        index_url = INDEX_URL.removesuffix("l")

        metadata = self.parse(index_url=index_url)

        self.assertEqual(index_url, metadata["index_url"])
        self.assertEqual(
            index_url.rsplit("/", 1)[0] + "/form4-synthetic.xml",
            metadata["document_url"],
        )

    def test_preserves_amended_form_type(self) -> None:
        html = FIXTURE.read_bytes().replace(b"Form 4", b"Form 4/A").replace(
            b">4</td><td>1234", b">4/A</td><td>1234"
        )
        result = self.parse(html)
        self.assertEqual("4/A", result["form_type"])
        self.assertEqual("4/A", result["document_type"])

    def test_accepts_owner_archive_href_and_empty_owners_for_issuer_document(self) -> None:
        owner_href = (
            b"/Archives/edgar/data/2/000000000126000001/owner.xml"
        )
        html = FIXTURE.read_bytes().replace(
            b'form4-synthetic.xml">form4-synthetic.xml', owner_href + b'">owner.xml', 1
        )
        result = self.parse(html)
        self.assertEqual("reporting_owner", result["document_archive_cik_role"])
        self.assertEqual("0000000002", result["document_archive_cik"])
        self.assertEqual("owner.xml", result["document_filename"])
        issuer_result = self.parse(reporting_owner_ciks=())
        self.assertEqual([], issuer_result["reporting_owner_ciks"])

    def test_accepts_multi_class_info_headers_and_values(self) -> None:
        html = FIXTURE.read_bytes().replace(b'class="infoHead"', b'class="box infoHead"').replace(
            b'class="info"', b'class="box info"'
        )
        self.assertEqual("2026-01-16", self.parse(html)["filing_date"])
        self.assertEqual(
            "4",
            self.parse(FIXTURE.read_bytes().replace(b'class="tableFile"', b'class="x tableFile y"'))["form_type"],
        )

    def test_rejects_conflicting_form_declarations(self) -> None:
        for label in (b"Form 4 - Form 8-K", b"Not a Form 4 filing"):
            with self.subTest(label=label):
                html = FIXTURE.read_bytes().replace(b"Form 4</strong>", label + b"</strong>", 1)
                with self.assertRaisesRegex(InsiderIndexParseError, "form type"):
                    self.parse(html)
        described = FIXTURE.read_bytes().replace(
            b"Form 4</strong>",
            b"Form 4 - Statement of changes in beneficial ownership</strong>",
            1,
        )
        self.assertEqual("4", self.parse(described)["form_type"])

    def test_converts_edt_sec_acceptance_time_to_utc(self) -> None:
        html = FIXTURE.read_bytes().replace(
            b"2026-01-16 11:30:00", b"2026-06-02 11:30:00"
        ).replace(b"2026-01-16</strong>", b"2026-06-02</strong>")
        self.assertEqual("2026-06-02T15:30:00Z", self.parse(html)["accepted_at"])

    def test_accepts_exact_sec_legacy_html_doctype(self) -> None:
        legacy_doctype = (
            b'<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" '
            b'"http://www.w3.org/TR/html4/loose.dtd">\n'
        )
        html = legacy_doctype + FIXTURE.read_bytes().split(b"\n", 1)[1]

        self.assertEqual("4", self.parse(html)["form_type"])

    def test_rejects_unsafe_doctypes_and_dst_wall_times(self) -> None:
        for prefix in (
            b'<!DOCTYPE html SYSTEM "x">', b'<!DOCTYPE html PUBLIC "x">',
            b'<!DOCTYPE html [<!ENTITY x "y">]>', b'<!doctype html><!doctype html>',
            b'<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" '
            b'"https://www.w3.org/TR/html4/loose.dtd">',
        ):
            with self.subTest(prefix=prefix):
                with self.assertRaises(InsiderIndexParseError):
                    self.parse(prefix + FIXTURE.read_bytes().split(b"\n", 1)[1])
        for value in (b"2026-03-08 02:30:00", b"2026-11-01 01:30:00"):
            with self.subTest(value=value):
                with self.assertRaises(InsiderIndexParseError):
                    self.parse(FIXTURE.read_bytes().replace(b"2026-01-16 11:30:00", value))

    def test_ignores_large_unrelated_page_text_and_requires_sequence_one(self) -> None:
        html = FIXTURE.read_bytes().replace(b"</body>", b"<p>" + b"x" * 5000 + b"</p></body>")
        self.assertEqual("1", self.parse(html)["document_sequence"])
        html = FIXTURE.read_bytes().replace(b"<td>1</td><td>FORM 4", b"<td>2</td><td>FORM 4")
        with self.assertRaises(InsiderIndexParseError):
            self.parse(html)

    def test_rejects_non_bytes_and_oversized_input_before_parse(self) -> None:
        with self.assertRaises(TypeError):
            self.parse("not bytes")  # type: ignore[arg-type]
        with self.assertRaisesRegex(InsiderIndexParseError, "size limit"):
            self.parse(b"x" * 1_000_001)

    def test_rejects_duplicate_normalized_document_table_headers(self) -> None:
        html = FIXTURE.read_bytes().replace(b"<th>Size</th>", b"<th> type </th>", 1)
        with self.assertRaisesRegex(InsiderIndexParseError, "headers"):
            self.parse(html)

    def test_requires_exact_official_document_table_header_sequence(self) -> None:
        header = b"<th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th>"
        for replacement in (
            b"<th>Seq</th><th>Untrusted Label</th><th>Document</th><th>Type</th><th>Size</th>",
            b"<th>Seq</th><th>Document</th><th>Type</th><th>Size</th>",
            b"<th>Description</th><th>Seq</th><th>Document</th><th>Type</th><th>Size</th>",
            header + b"<th>Extra</th>",
            header.replace(b"<th>", b"<td>").replace(b"</th>", b"</td>"),
        ):
            with self.subTest(replacement=replacement):
                html = FIXTURE.read_bytes().replace(header, replacement, 1)
                with self.assertRaisesRegex(InsiderIndexParseError, "headers"):
                    self.parse(html)

    def test_rejects_unsafe_or_conflicting_document_rows(self) -> None:
        for replacement in (
            b'<a href="https://evil.example/form4-synthetic.xml">',
            b'<a href="form4-synthetic.xml?query=1">',
            b'<a href="../../form4-synthetic.xml">',
            b'<a href="form4-synthetic.xml">form4-synthetic.xml</a></td><td>4</td><td>1234</td></tr><tr><td>3</td><td>duplicate</td><td><a href="other.xml">other.xml</a></td><td>4</td><td>4</td></tr><tr><td>1</td><td>FORM 4</td><td>',
        ):
            html = FIXTURE.read_bytes().replace(
                b'<a href="form4-synthetic.xml">', replacement, 1
            )
            with self.subTest(replacement=replacement[:30]):
                with self.assertRaises(InsiderIndexParseError):
                    self.parse(html)

    def test_rejects_noncanonical_document_absolute_host(self) -> None:
        html = FIXTURE.read_bytes().replace(
            b'form4-synthetic.xml">form4-synthetic.xml',
            b'https://sec.gov/Archives/edgar/data/1/000000000126000001/form4-synthetic.xml">form4-synthetic.xml',
            1,
        )
        with self.assertRaisesRegex(InsiderIndexParseError, "URL"):
            self.parse(html)

    def test_element_limit_aborts_before_full_dom_parse(self) -> None:
        html = b"<html><body><div></div></body></html>"
        with patch("insider_source.MAX_INDEX_HTML_ELEMENTS", 2), patch(
            "insider_source.etree.fromstring",
            side_effect=AssertionError("full DOM parser was reached"),
        ):
            with self.assertRaisesRegex(InsiderIndexParseError, "elements"):
                self.parse(html)

    def test_official_row_and_each_relevant_field_limit_abort_before_full_dom_parse(self) -> None:
        too_many_rows = FIXTURE.read_bytes().replace(
            b"</table>",
            b"<tr><td>3</td><td>x</td><td>x</td><td></td><td>1</td></tr></table>",
            1,
        )
        with patch("insider_source.MAX_INDEX_TABLE_ROWS", 1), patch(
            "insider_source.etree.fromstring",
            side_effect=AssertionError("full DOM parser was reached"),
        ):
            with self.assertRaisesRegex(InsiderIndexParseError, "row"):
                self.parse(too_many_rows)
        for html in (
            FIXTURE.read_bytes().replace(b"Form 4", b"Form 44444"),
            FIXTURE.read_bytes().replace(b"Filing Date", b"Filing Date 44444"),
            FIXTURE.read_bytes().replace(b"2026-01-16</div>", b"2026-01-1644444</div>", 1),
            FIXTURE.read_bytes().replace(b"<th>Seq</th>", b"<th>Sequence 44444</th>"),
        ):
            with self.subTest(html=html[:80]), patch("insider_source.MAX_INDEX_FIELD_CHARS", 8), patch(
                "insider_source.etree.fromstring",
                side_effect=AssertionError("full DOM parser was reached"),
            ):
                with self.assertRaisesRegex(InsiderIndexParseError, "field"):
                    self.parse(html)

    def test_relevant_nested_document_field_limit_aborts_before_full_dom_parse(self) -> None:
        html = FIXTURE.read_bytes().replace(
            b'form4-synthetic.xml">form4-synthetic.xml',
            b'form4-synthetic.xml">a<span>bc</span>d',
            1,
        )
        with patch("insider_source.MAX_INDEX_FIELD_CHARS", 3), patch(
            "insider_source.etree.fromstring",
            side_effect=AssertionError("full DOM parser was reached"),
        ):
            with self.assertRaisesRegex(InsiderIndexParseError, "field"):
                self.parse(html)

    def test_metadata_rejects_non_domain_sha_types_and_noncanonical_sequence(self) -> None:
        parsed = self.parse()
        metadata = build_insider_source_metadata(parsed, FIXTURE.read_bytes(), b"synthetic raw")
        for value in (None, 1, True, [], {}):
            with self.subTest(sha=value):
                invalid = {**metadata, "index": {**metadata["index"], "sha256": value}}
                with self.assertRaises(InsiderIndexParseError):
                    validate_insider_source_metadata(invalid)
        for sequence in ("0", "2", "01"):
            with self.subTest(sequence=sequence):
                invalid = {**metadata, "document": {**metadata["document"], "sequence": sequence}}
                with self.assertRaises(InsiderIndexParseError):
                    validate_insider_source_metadata(invalid)

    def test_rejects_whitespace_obfuscated_declaration_before_full_dom_parse(self) -> None:
        for prefix in (b"<! DOCTYPE html>", b"<!\tDOCTYPE html>", b"<!\nENTITY x 'y'>"):
            with self.subTest(prefix=prefix), patch(
                "insider_source.etree.fromstring",
                side_effect=AssertionError("full DOM parser was reached"),
            ):
                with self.assertRaisesRegex(InsiderIndexParseError, "DTD"):
                    self.parse(prefix + FIXTURE.read_bytes())

    def test_declaration_preflight_rejects_malformed_markup_before_parsers_and_keeps_valid_controls(self) -> None:
        cases = (
            ("unknown declaration", b"<!BOGUS>" + FIXTURE.read_bytes(), True),
            ("CDATA declaration", b"<![CDATA[x]]>" + FIXTURE.read_bytes(), True),
            ("bare declaration opener", b"<!" + FIXTURE.read_bytes(), True),
            ("unclosed unknown declaration", b"<!BOGUS" + FIXTURE.read_bytes(), True),
            ("malformed comment", b"<!-->" + FIXTURE.read_bytes(), True),
            ("unclosed comment", b"<!-- unclosed" + FIXTURE.read_bytes(), True),
            ("ordinary comment", b"<!-- ordinary comment -->" + FIXTURE.read_bytes(), False),
            ("plain HTML5 doctype", FIXTURE.read_bytes(), False),
        )
        for label, html, should_reject in cases:
            with self.subTest(label=label):
                if should_reject:
                    with patch(
                        "insider_source.etree.HTMLPullParser",
                        side_effect=AssertionError("bounded parser was reached"),
                    ), patch(
                        "insider_source.etree.fromstring",
                        side_effect=AssertionError("full DOM parser was reached"),
                    ):
                        with self.assertRaises(InsiderIndexParseError):
                            self.parse(html)
                else:
                    self.assertEqual("4", self.parse(html)["form_type"])

    def test_rejects_incomplete_doctype_marker(self) -> None:
        for prefix in (b"<!DOCTYPE", b"<!DOCTYPE html"):
            with self.subTest(prefix=prefix):
                with self.assertRaisesRegex(InsiderIndexParseError, "DTD"):
                    self.parse(prefix + FIXTURE.read_bytes())

    def test_rejects_document_rows_outside_the_one_official_table(self) -> None:
        for replacement in (
            b'<table class="tableFile" summary="Data Files">',
            b'<table class="other" summary="Document Format Files">',
        ):
            with self.subTest(replacement=replacement):
                html = FIXTURE.read_bytes().replace(
                    b'<table class="tableFile" summary="Document Format Files">', replacement, 1
                )
                with self.assertRaisesRegex(InsiderIndexParseError, "official"):
                    self.parse(html)

    def test_rejects_noncanonical_filing_date_lexemes(self) -> None:
        for value in (b"20260116", b"2026-W03-5"):
            with self.subTest(value=value):
                html = FIXTURE.read_bytes().replace(b"2026-01-16</div>", value + b"</div>", 1)
                with self.assertRaisesRegex(InsiderIndexParseError, "Filing Date"):
                    self.parse(html)

    def test_rejects_invalid_index_url_and_wrong_form_or_date(self) -> None:
        with self.assertRaises(InsiderIndexParseError):
            self.parse(index_url=INDEX_URL.replace("www.sec.gov", "www.sec.gov.evil"))
        with self.assertRaises(InsiderIndexParseError):
            self.parse(FIXTURE.read_bytes().replace(b"Form 4</strong>", b"Form 8-K</strong>"))
        with self.assertRaises(InsiderIndexParseError):
            self.parse(FIXTURE.read_bytes().replace(b"2026-01-16</div>", b"2026-99-99</div>", 1))
    def test_all_six_forms_and_document_url_forms_are_exact(self) -> None:
        for form_type in ("3", "3/A", "4", "4/A", "5", "5/A"):
            with self.subTest(form_type=form_type):
                html = FIXTURE.read_bytes().replace(b"Form 4", f"Form {form_type}".encode()).replace(
                    b">4</td><td>1234", f">{form_type}</td><td>1234".encode()
                )
                parsed = self.parse(html)
                self.assertEqual(form_type, parsed["form_type"])
                self.assertEqual(form_type, parsed["document_type"])
        archive = b"/Archives/edgar/data/1/000000000126000001/form4-synthetic.xml"
        for href in (b"form4-synthetic.xml", archive, b"https://www.sec.gov" + archive):
            with self.subTest(href=href):
                html = FIXTURE.read_bytes().replace(
                    b'form4-synthetic.xml">form4-synthetic.xml', href + b'">form4-synthetic.xml', 1
                )
                self.assertEqual(
                    "https://www.sec.gov" + archive.decode(), self.parse(html)["document_url"]
                )

    def test_accepts_official_nested_f345_document_paths(self) -> None:
        nested = b"xslF345X05/form4-synthetic.xml"
        archive = b"/Archives/edgar/data/1/000000000126000001/" + nested
        for href in (nested, archive, b"https://www.sec.gov" + archive):
            with self.subTest(href=href):
                html = FIXTURE.read_bytes().replace(
                    b'form4-synthetic.xml\">form4-synthetic.xml',
                    href + b'\">form4-synthetic.xml',
                    1,
                )
                parsed = self.parse(html)
                expected_url = "https://www.sec.gov" + archive.decode()
                self.assertEqual(expected_url, parsed["document_url"])
                self.assertEqual("form4-synthetic.xml", parsed["document_filename"])
                metadata = build_insider_source_metadata(parsed, html, b"synthetic raw")
                document = validate_insider_source_metadata(metadata)["document"]
                assert isinstance(document, dict)
                self.assertEqual(expected_url, document["url"])

    def test_accepts_official_rendered_f345_row_before_raw_xml(self) -> None:
        header = (
            b"<tr><th>Seq</th><th>Description</th><th>Document</th>"
            b"<th>Type</th><th>Size</th></tr>"
        )
        rendered_template = (
            b'<tr><td>1</td><td>{description}</td><td><a href="/Archives/edgar/data/1/'
            b'000000000126000001/xslF345X05/form4-synthetic.xml">'
            b"form4-synthetic.html</a></td><td>4</td><td>&nbsp;</td></tr>"
        )
        for description in (b"4", b"FORM 4", b"PRIMARY DOCUMENT"):
            with self.subTest(description=description):
                rendered = rendered_template.replace(b"{description}", description)
                html = FIXTURE.read_bytes().replace(header, header + rendered, 1)

                parsed = self.parse(html)

                self.assertEqual(
                    INDEX_URL.rsplit("/", 1)[0] + "/form4-synthetic.xml",
                    parsed["document_url"],
                )
                self.assertEqual("form4-synthetic.xml", parsed["document_filename"])

    def test_rejects_near_match_rendered_f345_rows(self) -> None:
        header = (
            b"<tr><th>Seq</th><th>Description</th><th>Document</th>"
            b"<th>Type</th><th>Size</th></tr>"
        )
        rendered = (
            b'<tr><td>1</td><td>4</td><td><a href="/Archives/edgar/data/1/'
            b'000000000126000001/xslF345X05/form4-synthetic.xml">'
            b"form4-synthetic.html</a></td><td>4</td><td>&nbsp;</td></tr>"
        )
        for near_match in (
            rendered.replace(b"xslF345X05", b"xslF345X5"),
            rendered.replace(b"form4-synthetic.html", b"other.html"),
            rendered.replace(b"&nbsp;", b"1234"),
            rendered.replace(b"/data/1/", b"/data/3/"),
            rendered.replace(b"26000001/", b"26000002/"),
            rendered.replace(
                b"<td>4</td><td><a",
                b"<td>primary document</td><td><a",
                1,
            ),
        ):
            with self.subTest(near_match=near_match):
                html = FIXTURE.read_bytes().replace(header, header + near_match, 1)
                with self.assertRaisesRegex(InsiderIndexParseError, "href"):
                    self.parse(html)

    def test_requires_case_sensitive_official_table_class_token(self) -> None:
        original = b'class="tableFile" summary="Document Format Files"'
        for class_name in (b"TABLEFILE", b"TableFile", b"tablefile"):
            with self.subTest(class_name=class_name):
                replacement = b'class="' + class_name + b'" summary="Document Format Files"'
                with self.assertRaisesRegex(InsiderIndexParseError, "official"):
                    self.parse(FIXTURE.read_bytes().replace(original, replacement, 1))

    def test_rejects_index_and_document_url_adversarial_matrix_as_domain_errors(self) -> None:
        for url in (
            INDEX_URL.replace("www.sec.gov", "user:pass@www.sec.gov"),
            INDEX_URL.replace("www.sec.gov", "www.sec.gov:444"),
            INDEX_URL + "?x=1",
            INDEX_URL + "#x",
            INDEX_URL.replace("26000001", "26000002"),
            INDEX_URL.replace("/data/1/", "/data/2/"),
            INDEX_URL.replace("www.sec.gov", "www.sec.gov.evil"),
            INDEX_URL.replace("-index.html", "-index.ht"),
            INDEX_URL.replace("-index.html", "-index.htmlx"),
        ):
            with self.subTest(index_url=url):
                with self.assertRaises(InsiderIndexParseError):
                    self.parse(index_url=url)
        for href in (
            b"https://user:pass@www.sec.gov/Archives/edgar/data/1/000000000126000001/form4-synthetic.xml",
            b"https://www.sec.gov:444/Archives/edgar/data/1/000000000126000001/form4-synthetic.xml",
            b"form4-synthetic.xml?x=1", b"form4-synthetic.xml#x",
            b"https://www.sec.gov.evil/Archives/edgar/data/1/000000000126000001/form4-synthetic.xml",
            b"https://evil.sec.gov/Archives/edgar/data/1/000000000126000001/form4-synthetic.xml",
            b"/Archives/edgar/data/1/000000000126000002/form4-synthetic.xml",
            b"/Archives/edgar/data/3/000000000126000001/form4-synthetic.xml",
            b"..%2Fform4-synthetic.xml", b"file:///tmp/form4-synthetic.xml",
            b"nested/../form4-synthetic.xml", b"nested//form4-synthetic.xml",
            b"nested/%2e%2e/form4-synthetic.xml", b"bad name.xml", b"https://[::1",
        ):
            with self.subTest(href=href):
                html = FIXTURE.read_bytes().replace(
                    b'form4-synthetic.xml">form4-synthetic.xml', href + b'">form4-synthetic.xml', 1
                )
                with self.assertRaises(InsiderIndexParseError):
                    self.parse(html)
        html = FIXTURE.read_bytes().replace(b">form4-synthetic.xml</a>", b">other.xml</a>", 1)
        with self.assertRaises(InsiderIndexParseError):
            self.parse(html)

    def test_requires_one_official_table_and_preserves_comment_text(self) -> None:
        original = b'<table class="tableFile" summary="Document Format Files">'
        for replacement in (b'<table class="tableFile" summary="Data Files">', b'<table class="other" summary="Document Format Files">'):
            with self.subTest(replacement=replacement):
                with self.assertRaises(InsiderIndexParseError):
                    self.parse(FIXTURE.read_bytes().replace(original, replacement, 1))
        duplicate = FIXTURE.read_bytes().replace(original, original + original, 1)
        with self.assertRaises(InsiderIndexParseError):
            self.parse(duplicate)
        self.assertEqual("4", self.parse(b"<!-- harmless <!DOCTYPE html> text -->" + FIXTURE.read_bytes())["form_type"])

    def test_source_metadata_contract_boundaries_and_canonical_json(self) -> None:
        owner_html = FIXTURE.read_bytes().replace(
            b'form4-synthetic.xml">form4-synthetic.xml',
            b'/Archives/edgar/data/2/000000000126000001/owner.xml">owner.xml',
            1,
        )
        parsed = self.parse(owner_html, reporting_owner_ciks=("0000000002", "0000000003"))
        metadata = build_insider_source_metadata(parsed, FIXTURE.read_bytes(), b"raw")
        self.assertEqual(["0000000002", "0000000003"], validate_insider_source_metadata(metadata)["reporting_owner_ciks"])
        encoded = canonical_source_metadata_json_bytes(metadata)
        self.assertEqual(encoded, (json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode())
        for invalid in (
            {**metadata, "filing_date": "2026-1-16"},
            {**metadata, "extra": None},
            {**metadata, "index": {key: value for key, value in metadata["index"].items() if key != "url"}},
            {**metadata, "document": {**metadata["document"], "sequence": 1}},
            {**metadata, "document": {**metadata["document"], "sequence": "01"}},
            {**metadata, "document": {**metadata["document"], "document_type": 4}},
            {**metadata, "index": {**metadata["index"], "sha256": "A" * 64}},
            {**metadata, "index": {**metadata["index"], "sha256": "a" * 63}},
            {**metadata, "index": {**metadata["index"], "byte_count": True}},
            {**metadata, "index": {**metadata["index"], "byte_count": 0}},
            {**metadata, "index": {**metadata["index"], "byte_count": -1}},
            {**metadata, "index": {**metadata["index"], "byte_count": 1_000_001}},
            {**metadata, "document": {**metadata["document"], "byte_count": 0}},
            {**metadata, "document": {**metadata["document"], "byte_count": 10_000_001}},
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(InsiderIndexParseError):
                    validate_insider_source_metadata(invalid)
        boundary = {**metadata, "index": {**metadata["index"], "byte_count": 1_000_000}, "document": {**metadata["document"], "byte_count": 10_000_000}}
        self.assertEqual(boundary, validate_insider_source_metadata(boundary))


class InsiderIncrementalDiscoveryTests(unittest.TestCase):
    observed_at = "2026-01-17T00:00:00Z"
    fixture_names = {
        "3": "latest_form3.atom",
        "3/A": "latest_form3a.atom",
        "4": "latest_form4.atom",
        "4/A": "latest_form4a.atom",
        "5": "latest_form5.atom",
        "5/A": "latest_form5a.atom",
    }

    def fixture(self, form_type: str) -> bytes:
        return (FIXTURE.parent / self.fixture_names[form_type]).read_bytes()

    def entries(self, form_type: str = "4"):
        return parse_recent_insider_atom(
            self.fixture(form_type),
            requested_form_type=form_type,
            observed_at=self.observed_at,
        )

    def grouped(self, *form_types: str):
        requested = form_types or SECTION16_CURRENT_FORMS
        return group_recent_insider_entries(
            (
                entry
                for form_type in requested
                for entry in self.entries(form_type)
            ),
            approved_issuer_ciks=("0000000001",),
            max_accessions=10,
        )

    @staticmethod
    def empty_feed() -> bytes:
        return b'<feed xmlns="http://www.w3.org/2005/Atom"></feed>'

    @staticmethod
    def approved_state_store(
        root: Path,
        issuer_ciks: tuple[str, ...] = ("0000000001",),
    ) -> InsiderStateStore:
        state = InsiderStateStore(root)
        state.write(
            "approved-issuers-v1",
            {"contract_version": 1, "issuer_ciks": list(issuer_ciks)},
        )
        return state

    def fixture_http(self) -> _AtomHTTP:
        def respond(url: str) -> _AtomResponse:
            query = parse_qs(urlsplit(url).query)
            body = (
                self.fixture(query["type"][0])
                if query["start"] == ["0"]
                else self.empty_feed()
            )
            return _AtomResponse(body, url=url)

        return _AtomHTTP(respond)

    def test_parses_six_exact_forms_and_retains_reporting_evidence(self) -> None:
        self.assertEqual(("3", "3/A", "4", "4/A", "5", "5/A"), SECTION16_CURRENT_FORMS)
        parsed = []
        for form_type in SECTION16_CURRENT_FORMS:
            with self.subTest(form_type=form_type):
                entries = self.entries(form_type)
                self.assertTrue(entries)
                self.assertEqual({form_type}, {entry.form_type for entry in entries})
                parsed.extend(entries)

        form4 = [entry for entry in parsed if entry.accession_number == ACCESSION]
        self.assertEqual(["issuer", "reporting_owner", "reporting_owner"], [entry.entity_role for entry in form4])
        self.assertEqual(["0000000001", "0000000002", "0000000003"], [entry.entity_cik for entry in form4])
        self.assertTrue(form4[0].entry_url.endswith(f"/{ACCESSION}-index.html"))
        self.assertTrue(form4[1].entry_url.endswith("/owner-entry.html"))

        grouped = group_recent_insider_entries(
            parsed,
            approved_issuer_ciks=("1",),
            max_accessions=10,
        )
        self.assertEqual(
            [
                "0000000001-26-000003",
                "0000000001-26-000004",
                ACCESSION,
                "0000000001-26-000005",
                "0000000001-26-000006",
                "0000000001-26-000007",
            ],
            [entry.accession_number for entry in grouped.accessions],
        )
        queued_form4 = next(entry for entry in grouped.accessions if entry.accession_number == ACCESSION)
        self.assertEqual("0000000001", queued_form4.issuer_cik)
        self.assertEqual(2, queued_form4.reporting_entry_count)
        self.assertEqual(3, len(queued_form4.source_entries))
        self.assertEqual((), grouped.quarantined_accessions)

    def test_base_form_query_accepts_exact_amended_variant(self) -> None:
        entries = parse_recent_insider_atom(
            self.fixture("3/A"),
            requested_form_type="3",
            observed_at=self.observed_at,
        )

        self.assertEqual(1, len(entries))
        self.assertEqual("3/A", entries[0].form_type)
        with self.assertRaisesRegex(InsiderDiscoveryError, "form type"):
            parse_recent_insider_atom(
                self.fixture("3"),
                requested_form_type="3/A",
                observed_at=self.observed_at,
            )

    def test_deduplicates_exact_sources_repeated_across_form_queries(self) -> None:
        entries = self.entries("3/A")

        result = group_recent_insider_entries(
            (*entries, *entries),
            approved_issuer_ciks=("0000000001",),
            max_accessions=10,
        )

        self.assertEqual(1, len(result.accessions))
        self.assertEqual(entries, result.accessions[0].source_entries)
        self.assertEqual((), result.quarantined_accessions)

    def test_discovery_deduplicates_amendment_returned_by_both_queries(self) -> None:
        def respond(url: str) -> _AtomResponse:
            form_type = parse_qs(urlsplit(url).query)["type"][0]
            body = self.fixture("3/A") if form_type in {"3", "3/A"} else self.empty_feed()
            return _AtomResponse(body, url=url)

        result = discover_recent_insider_accessions(
            approved_issuer_ciks=("0000000001",),
            lookback_seconds=86_400,
            max_pages=1,
            page_size=100,
            max_accessions=10,
            deadline_seconds=60,
            now=datetime(2026, 1, 17, tzinfo=timezone.utc),
            http=_AtomHTTP(respond),
            monotonic=lambda: 100.0,
        )

        self.assertEqual(6, result.pages_fetched)
        self.assertEqual(1, len(result.accessions))
        self.assertEqual("3/A", result.accessions[0].form_type)
        self.assertEqual(1, len(result.accessions[0].source_entries))
        self.assertEqual((), result.quarantined_accessions)

    def test_accepts_current_sec_index_htm_links(self) -> None:
        atom = self.fixture("4").replace(b".html", b".htm")

        entries = parse_recent_insider_atom(
            atom,
            requested_form_type="4",
            observed_at=self.observed_at,
        )

        self.assertEqual(3, len(entries))
        self.assertTrue(all(entry.entry_url.endswith(".htm") for entry in entries))

    def test_persists_current_sec_index_htm_links(self) -> None:
        atom = self.fixture("4").replace(b".html", b".htm")
        result = group_recent_insider_entries(
            parse_recent_insider_atom(
                atom,
                requested_form_type="4",
                observed_at=self.observed_at,
            ),
            approved_issuer_ciks=("0000000001",),
            max_accessions=10,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            state = self.approved_state_store(Path(tmpdir))

            persisted = persist_incremental_discovery_queue(
                state,
                result=result,
                lookback_seconds=86_400,
            )

        queue = persisted["queue"]
        sources = persisted["source_entries"]
        assert isinstance(queue, list) and isinstance(sources, list)
        self.assertTrue(all(entry["index_url"].endswith(".htm") for entry in queue))
        self.assertTrue(all(entry["entry_url"].endswith(".htm") for entry in sources))

    def test_quarantines_missing_duplicate_or_conflicting_issuer_groups(self) -> None:
        issuer, owner, co_owner = self.entries()
        cases = {
            "missing issuer": (owner, co_owner),
            "conflicting issuer URLs": (
                issuer,
                replace(issuer, entry_url=issuer.entry_url.removesuffix("l")),
                owner,
            ),
            "conflicting accepted time": (
                issuer,
                replace(owner, accepted_at="2026-01-16T16:31:00Z"),
            ),
            "conflicting form": (issuer, replace(owner, form_type="4/A")),
        }
        for label, entries in cases.items():
            with self.subTest(label=label):
                result = group_recent_insider_entries(
                    entries,
                    approved_issuer_ciks=("0000000001",),
                    max_accessions=10,
                )
                self.assertEqual((), result.accessions)
                self.assertEqual((ACCESSION,), result.quarantined_accessions)

    def test_owner_archive_path_never_becomes_issuer_and_allowlist_is_explicit(self) -> None:
        entries = self.entries()
        grouped = group_recent_insider_entries(
            entries,
            approved_issuer_ciks=("0000000001",),
            max_accessions=10,
        )
        self.assertEqual("0000000001", grouped.accessions[0].issuer_cik)
        self.assertEqual("0000000002", grouped.accessions[0].source_entries[1].entity_cik)

        unapproved = group_recent_insider_entries(
            entries,
            approved_issuer_ciks=("0000000009",),
            max_accessions=10,
        )
        self.assertEqual((), unapproved.accessions)
        self.assertEqual((), unapproved.quarantined_accessions)
        for invalid in ((), ("bad",), (True,)):
            with self.subTest(invalid=invalid), self.assertRaises(InsiderDiscoveryError):
                group_recent_insider_entries(
                    entries,
                    approved_issuer_ciks=invalid,
                    max_accessions=10,
                )

    def test_rejects_malformed_atom_unsafe_declarations_fields_and_urls(self) -> None:
        valid = self.fixture("4")
        invalid_documents = (
            b"<not-feed />",
            b"<feed xmlns='http://www.w3.org/2005/Atom'><entry></feed>",
            valid.replace(b"<title>4 - Synthetic Issuer", b"<title>3 - Synthetic Issuer", 1),
            valid.replace(b"<updated>2026-01-16T16:30:00Z</updated>", b"", 1),
            valid.replace(b"<link rel=", b"<missing-link rel=", 1),
            b'<!DOCTYPE feed [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>' + valid,
            b"<!ENTITY x 'unsafe'>" + valid,
        )
        for document in invalid_documents:
            with self.subTest(document=document[:60]), self.assertRaises(InsiderDiscoveryError):
                parse_recent_insider_atom(
                    document,
                    requested_form_type="4",
                    observed_at=self.observed_at,
                )
        with self.assertRaises(TypeError):
            parse_recent_insider_atom(
                "not bytes",  # type: ignore[arg-type]
                requested_form_type="4",
                observed_at=self.observed_at,
            )
        with self.assertRaisesRegex(InsiderDiscoveryError, "size"):
            parse_recent_insider_atom(
                b"x" * (MAX_RECENT_INSIDER_ATOM_BYTES + 1),
                requested_form_type="4",
                observed_at=self.observed_at,
            )

        issuer_url = b"https://www.sec.gov/Archives/edgar/data/1/000000000126000001/0000000001-26-000001-index.html"
        for unsafe in (
            issuer_url.replace(b"https://", b"http://"),
            issuer_url.replace(b"www.sec.gov", b"www.sec.gov.evil"),
            issuer_url.replace(b"www.sec.gov", b"WWW.SEC.GOV"),
            issuer_url.replace(b"www.sec.gov", b"www.sec.gov:0443"),
            issuer_url.replace(b"www.sec.gov", b"user:pass@www.sec.gov"),
            issuer_url.replace(b"www.sec.gov", b"www.sec.gov:444"),
            issuer_url + b"?x=1",
            issuer_url + b"#x",
            issuer_url.replace(b"/data/1/", b"/data/2/"),
            issuer_url.replace(b"000000000126000001", b"000000000126000002"),
            issuer_url.replace(b"-index.html", b".xml"),
            issuer_url.replace(b"-index.html", b"-index.ht"),
            issuer_url.replace(b"-index.html", b"-index.htmlx"),
            issuer_url.replace(b"/Archives/", b"/Archives/%2e%2e/"),
        ):
            with self.subTest(unsafe=unsafe), self.assertRaises(InsiderDiscoveryError):
                parse_recent_insider_atom(
                    valid.replace(issuer_url, unsafe, 1),
                    requested_form_type="4",
                    observed_at=self.observed_at,
                )

    def test_issues_six_exact_owner_include_queries_with_bounded_pagination(self) -> None:
        http = self.fixture_http()
        result = discover_recent_insider_accessions(
            approved_issuer_ciks=("0000000001",),
            lookback_seconds=86_400,
            max_pages=2,
            page_size=40,
            max_accessions=10,
            deadline_seconds=60,
            now=datetime(2026, 1, 17, tzinfo=timezone.utc),
            http=http,
            monotonic=lambda: 100.0,
        )
        self.assertEqual(12, result.pages_fetched)
        self.assertEqual(6, len(result.accessions))
        self.assertFalse(result.deadline_reached)
        self.assertTrue(all(response.closed for response in http.responses))
        self.assertEqual(
            [{"stream": True, "deadline_monotonic": 160.0}] * len(http.calls),
            http.request_kwargs,
        )
        seen: list[tuple[str, int]] = []
        for url in http.calls:
            parsed = urlsplit(url)
            self.assertEqual(
                CURRENT_FILINGS_URL,
                f"{parsed.scheme}://{parsed.netloc}{parsed.path}",
            )
            query = parse_qs(parsed.query)
            self.assertEqual(["getcurrent"], query["action"])
            self.assertEqual(["include"], query["owner"])
            self.assertEqual(["atom"], query["output"])
            self.assertEqual(["40"], query["count"])
            seen.append((query["type"][0], int(query["start"][0])))
        self.assertEqual(
            [
                (form_type, start)
                for form_type in SECTION16_CURRENT_FORMS
                for start in (0, 40)
            ],
            seen,
        )
        self.assertIn(
            "type=4%2FA",
            build_recent_insider_feed_url("4/A", start=40, page_size=40),
        )

    def test_rejects_unbound_or_non_success_atom_response_before_reading_body(self) -> None:
        class UnreadableAtomResponse(_AtomResponse):
            def __init__(self, *, url: str, status_code: int = 200) -> None:
                super().__init__(b"", url=url, status_code=status_code)
                self.body_read = False

            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                self.body_read = True
                raise AssertionError("unbound Atom response body was consumed")

        cases = (
            (
                "wrong SEC path",
                "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=include&start=0&count=40&output=atom",
                200,
            ),
            (
                "wrong SEC query",
                "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=include&start=1&count=40&output=atom",
                200,
            ),
            (
                "non-SEC authority",
                "https://evil.example/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=include&start=0&count=40&output=atom",
                200,
            ),
            ("non-success status", build_recent_insider_feed_url("3", start=0, page_size=40), 503),
        )
        for label, response_url, status_code in cases:
            with self.subTest(label=label):
                response = UnreadableAtomResponse(
                    url=response_url,
                    status_code=status_code,
                )
                with self.assertRaisesRegex(InsiderDiscoveryError, "Atom response") as raised:
                    discover_recent_insider_accessions(
                        approved_issuer_ciks=("0000000001",),
                        lookback_seconds=86_400,
                        max_pages=1,
                        page_size=40,
                        max_accessions=10,
                        deadline_seconds=60,
                        now=datetime(2026, 1, 17, tzinfo=timezone.utc),
                        http=_AtomHTTP(lambda _url: response),
                    )
                self.assertNotIn("evil.example", str(raised.exception))
                self.assertFalse(response.body_read)
                self.assertTrue(response.closed)

    def test_discovery_uses_supplied_absolute_deadline_without_restarting_it(self) -> None:
        http = self.fixture_http()
        result = discover_recent_insider_accessions(
            approved_issuer_ciks=("0000000001",),
            lookback_seconds=86_400,
            max_pages=1,
            page_size=40,
            max_accessions=10,
            deadline_seconds=60,
            deadline_monotonic=160.0,
            now=datetime(2026, 1, 17, tzinfo=timezone.utc),
            http=http,
            monotonic=lambda: 130.0,
        )

        self.assertFalse(result.deadline_reached)
        self.assertEqual(
            [{"stream": True, "deadline_monotonic": 160.0}] * len(http.calls),
            http.request_kwargs,
        )

    def test_discovery_interrupts_blocking_body_as_cooperative_deadline(self) -> None:
        class BlockingAtomResponse(_AtomResponse):
            def __init__(self, *, url: str) -> None:
                super().__init__(b"", url=url)
                self.released = threading.Event()

            def close(self) -> None:
                super().close()
                self.released.set()

            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                self.released.wait(timeout=1.0)
                if not self.closed:
                    yield self.content

        responses: list[BlockingAtomResponse] = []

        def respond(url: str) -> BlockingAtomResponse:
            response = BlockingAtomResponse(url=url)
            responses.append(response)
            return response

        http = _AtomHTTP(respond)
        started = time.monotonic()
        result = discover_recent_insider_accessions(
            approved_issuer_ciks=("0000000001",),
            lookback_seconds=86_400,
            max_pages=1,
            page_size=40,
            max_accessions=10,
            deadline_seconds=60,
            deadline_monotonic=started + 0.05,
            now=datetime(2026, 1, 17, tzinfo=timezone.utc),
            http=http,
            monotonic=time.monotonic,
        )
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.5)
        self.assertTrue(result.deadline_reached)
        self.assertEqual(0, result.pages_fetched)
        self.assertEqual(1, len(http.calls))
        self.assertTrue(responses[0].closed)
        self.assertTrue(responses[0].released.is_set())

    def test_alternate_discovery_http_base_exceptions_are_sanitized(
        self,
    ) -> None:
        secret = "TASK4_ALTERNATE_HTTP_BASE_EXCEPTION_SECRET"

        class ExplodingGetAttribute:
            @property
            def get(self):
                raise _HostileHTTPFailure(secret)

        class ExplodingGetMethod:
            def get(self, _url: str, **_kwargs: object):
                raise _HostileHTTPFailure(secret)

        for http, expected_label in (
            (ExplodingGetAttribute(), "HTTP client"),
            (ExplodingGetMethod(), "Atom response"),
        ):
            with self.subTest(http=type(http).__name__):
                with self.assertRaises(InsiderDiscoveryError) as raised:
                    discover_recent_insider_accessions(
                        approved_issuer_ciks=("0000000001",),
                        lookback_seconds=86_400,
                        max_pages=1,
                        page_size=40,
                        max_accessions=10,
                        deadline_seconds=60,
                        now=datetime(2026, 1, 17, tzinfo=timezone.utc),
                        http=http,
                    )
                self.assertIn(expected_label, str(raised.exception))
                self.assertNotIn(secret, str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)
                self.assertIsNone(raised.exception.__context__)

    def test_alternate_discovery_http_control_flow_is_preserved(self) -> None:
        controls = (
            KeyboardInterrupt(),
            SystemExit(),
            GeneratorExit(),
            asyncio.CancelledError(),
            concurrent.futures.CancelledError(),
        )

        class ExplodingHTTP:
            def __init__(self, error: BaseException) -> None:
                self.error = error

            def get(self, _url: str, **_kwargs: object):
                raise self.error

        for control in controls:
            with self.subTest(control=type(control).__name__), self.assertRaises(
                type(control)
            ):
                discover_recent_insider_accessions(
                    approved_issuer_ciks=("0000000001",),
                    lookback_seconds=86_400,
                    max_pages=1,
                    page_size=40,
                    max_accessions=10,
                    deadline_seconds=60,
                    now=datetime(2026, 1, 17, tzinfo=timezone.utc),
                    http=ExplodingHTTP(control),
                )

    def test_discovery_bounds_filter_lookback_and_cap_deterministically(self) -> None:
        invalid_cases = (
            {"lookback_seconds": 0},
            {"lookback_seconds": True},
            {"max_pages": 0},
            {"page_size": 0},
            {"page_size": 101},
            {"max_accessions": 0},
            {"max_accessions": 1001},
            {"deadline_seconds": 0},
        )
        for overrides in invalid_cases:
            http = self.fixture_http()
            arguments = {
                "approved_issuer_ciks": ("0000000001",),
                "lookback_seconds": 86_400,
                "max_pages": 2,
                "page_size": 40,
                "max_accessions": 10,
                "deadline_seconds": 60,
                "now": datetime(2026, 1, 17, tzinfo=timezone.utc),
                "http": http,
                **overrides,
            }
            with self.subTest(overrides=overrides), self.assertRaises(
                InsiderDiscoveryError
            ):
                discover_recent_insider_accessions(**arguments)
            self.assertEqual([], http.calls)

        capped = discover_recent_insider_accessions(
            approved_issuer_ciks=("0000000001",),
            lookback_seconds=86_400,
            max_pages=1,
            page_size=40,
            max_accessions=3,
            deadline_seconds=60,
            now=datetime(2026, 1, 17, tzinfo=timezone.utc),
            http=self.fixture_http(),
        )
        self.assertEqual(
            ["0000000001-26-000003", "0000000001-26-000004", ACCESSION],
            [entry.accession_number for entry in capped.accessions],
        )
        old = discover_recent_insider_accessions(
            approved_issuer_ciks=("0000000001",),
            lookback_seconds=86_400,
            max_pages=2,
            page_size=40,
            max_accessions=10,
            deadline_seconds=60,
            now=datetime(2026, 1, 20, tzinfo=timezone.utc),
            http=self.fixture_http(),
        )
        self.assertEqual((), old.accessions)

        for invalid_clock in (float("nan"), float("inf"), -float("inf")):
            http = self.fixture_http()
            with self.subTest(clock=invalid_clock), self.assertRaises(
                InsiderDiscoveryError
            ):
                discover_recent_insider_accessions(
                    approved_issuer_ciks=("0000000001",),
                    lookback_seconds=86_400,
                    max_pages=1,
                    page_size=40,
                    max_accessions=10,
                    deadline_seconds=60,
                    now=datetime(2026, 1, 17, tzinfo=timezone.utc),
                    http=http,
                    monotonic=lambda: invalid_clock,
                )
            self.assertEqual([], http.calls)

    def test_entry_element_discovery_and_group_bounds_fail_closed(self) -> None:
        with patch("insider_pipeline.MAX_RECENT_INSIDER_ATOM_ENTRIES", 0):
            with self.assertRaisesRegex(InsiderDiscoveryError, "entry limit"):
                self.entries("3")
        with patch("insider_pipeline.MAX_RECENT_INSIDER_ATOM_ELEMENTS", 1):
            with self.assertRaisesRegex(InsiderDiscoveryError, "element limit"):
                self.entries("3")
        with patch("insider_pipeline.MAX_RECENT_INSIDER_ATOM_FIELD_CHARS", 8):
            with self.assertRaises(InsiderDiscoveryError):
                self.entries("3")
        with patch("insider_pipeline.MAX_RECENT_INSIDER_GROUPS", 1):
            with self.assertRaisesRegex(InsiderDiscoveryError, "group limit"):
                self.grouped("3", "3/A")
        with patch("insider_pipeline.MAX_RECENT_INSIDER_DISCOVERY_ENTRIES", 1):
            with self.assertRaisesRegex(InsiderDiscoveryError, "entry limit"):
                group_recent_insider_entries(
                    self.entries("4")[:2],
                    approved_issuer_ciks=("0000000001",),
                    max_accessions=1,
                )
        with patch("insider_pipeline.MAX_INSIDER_STATE_COLLECTION", 2):
            with self.assertRaisesRegex(InsiderDiscoveryError, "evidence limit"):
                group_recent_insider_entries(
                    self.entries("4"),
                    approved_issuer_ciks=("0000000001",),
                    max_accessions=1,
                )
        with patch("insider_pipeline.MAX_RECENT_INSIDER_DISCOVERY_ENTRIES", 1):
            with self.assertRaisesRegex(InsiderDiscoveryError, "entry limit"):
                discover_recent_insider_accessions(
                    approved_issuer_ciks=("0000000001",),
                    lookback_seconds=86_400,
                    max_pages=1,
                    page_size=40,
                    max_accessions=10,
                    deadline_seconds=60,
                    now=datetime(2026, 1, 17, tzinfo=timezone.utc),
                    http=self.fixture_http(),
                )

    def test_public_collection_bounds_stop_at_the_first_excess_item(self) -> None:
        def overlong_values(*, include_accession: bool = False):
            if include_accession:
                yield ACCESSION
            for value in range(1, 5):
                if value == 4:
                    raise AssertionError("collection was consumed past its bound")
                yield str(value)

        with patch("insider_pipeline.MAX_INSIDER_STATE_COLLECTION", 2):
            with self.assertRaisesRegex(InsiderDiscoveryError, "approved issuer"):
                group_recent_insider_entries(
                    self.entries("3")[:1],
                    approved_issuer_ciks=overlong_values(),
                    max_accessions=1,
                )

        valid = self.grouped("4")
        with patch("insider_pipeline.MAX_INSIDER_STATE_COLLECTION", 3):
            with self.assertRaisesRegex(InsiderDiscoveryError, "completed accessions"):
                insider_pipeline._incremental_state_payload(
                    valid,
                    lookback_seconds=86_400,
                    completed_accessions=overlong_values(include_accession=True),
                )

    def test_oversized_constructed_source_evidence_is_rejected_before_validation(self) -> None:
        valid = self.grouped("4")
        with (
            patch("insider_pipeline.MAX_INSIDER_STATE_COLLECTION", 2),
            patch(
                "insider_pipeline._validate_recent_feed_entry",
                side_effect=AssertionError("oversized evidence was traversed"),
            ),
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            with self.assertRaisesRegex(InsiderDiscoveryError, "evidence limit"):
                persist_incremental_discovery_queue(
                    InsiderStateStore(Path(tmpdir)),
                    result=valid,
                    lookback_seconds=86_400,
                )

    def test_grouping_revalidates_constructed_entries_before_queueing(self) -> None:
        issuer = self.entries("3")[0]
        invalids = (
            replace(issuer, accession_number="not-an-accession"),
            replace(issuer, entity_cik="1"),
            replace(issuer, entry_url=issuer.entry_url.replace("www.sec.gov", "evil.example")),
            replace(issuer, accepted_at="not-a-timestamp"),
            replace(issuer, observed_at="2026-01-01T00:00:00Z"),
        )
        for invalid in invalids:
            with self.subTest(invalid=invalid), self.assertRaises(
                InsiderDiscoveryError
            ):
                group_recent_insider_entries(
                    (invalid,),
                    approved_issuer_ciks=("0000000001",),
                    max_accessions=10,
                )

    def test_group_sort_is_accepted_time_then_accession_not_arrival_order(self) -> None:
        first = replace(
            self.entries("3")[0],
            accepted_at="2026-01-16T16:10:00Z",
        )
        second = replace(
            self.entries("3/A")[0],
            accepted_at="2026-01-16T16:10:00Z",
        )
        grouped = group_recent_insider_entries(
            (second, first),
            approved_issuer_ciks=("0000000001",),
            max_accessions=10,
        )
        self.assertEqual(
            sorted((first.accession_number, second.accession_number)),
            [entry.accession_number for entry in grouped.accessions],
        )

    def test_rejects_page_loops_and_declared_or_streamed_response_overflow(self) -> None:
        repeated = self.fixture("3")
        loop_http = _AtomHTTP(lambda url: _AtomResponse(repeated, url=url))
        with self.assertRaisesRegex(InsiderDiscoveryError, "loop"):
            discover_recent_insider_accessions(
                approved_issuer_ciks=("0000000001",),
                lookback_seconds=86_400,
                max_pages=2,
                page_size=40,
                max_accessions=10,
                deadline_seconds=60,
                now=datetime(2026, 1, 17, tzinfo=timezone.utc),
                http=loop_http,
            )
        self.assertTrue(all(response.closed for response in loop_http.responses))

        for declared in (True, False):
            def overflow(url: str, declared: bool = declared) -> _AtomResponse:
                body = b"x" * (MAX_RECENT_INSIDER_ATOM_BYTES + 1)
                return _AtomResponse(
                    body,
                    url=url,
                    content_length=len(body) if declared else None,
                )

            http = _AtomHTTP(overflow)
            with self.subTest(declared=declared), self.assertRaisesRegex(
                InsiderDiscoveryError, "response"
            ):
                discover_recent_insider_accessions(
                    approved_issuer_ciks=("0000000001",),
                    lookback_seconds=86_400,
                    max_pages=1,
                    page_size=40,
                    max_accessions=10,
                    deadline_seconds=60,
                    now=datetime(2026, 1, 17, tzinfo=timezone.utc),
                    http=http,
                )
            self.assertTrue(http.responses[0].closed)

    def test_persists_the_validated_queue_and_reporting_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = self.approved_state_store(Path(tmpdir))
            persisted = persist_incremental_discovery_queue(
                state,
                result=self.grouped(),
                lookback_seconds=86_400,
            )

            self.assertEqual(persisted, state.read("incremental-v1"))
            self.assertEqual("incomplete", persisted["status"])
            self.assertEqual(6, len(persisted["queue"]))
            self.assertEqual([], persisted["completed_accessions"])
            self.assertEqual(self.observed_at, persisted["first_observed_at"])
            self.assertEqual(self.observed_at, persisted["last_observed_at"])
            source_entries = persisted["source_entries"]
            assert isinstance(source_entries, list)
            form4_sources = [
                source for source in source_entries
                if source["accession_number"] == ACCESSION
            ]
            self.assertEqual(
                ["issuer", "reporting_owner", "reporting_owner"],
                [source["entity_role"] for source in form4_sources],
            )
            self.assertTrue(form4_sources[1]["entry_url"].endswith("owner-entry.html"))

    def test_persistence_requires_durable_approved_issuer_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            with self.assertRaisesRegex(InsiderDiscoveryError, "approved issuer state"):
                persist_incremental_discovery_queue(
                    state,
                    result=self.grouped("4"),
                    lookback_seconds=86_400,
                )
            with self.assertRaises(FileNotFoundError):
                state.read("incremental-v1")

    def test_persistence_rejects_constructed_result_outside_durable_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = self.approved_state_store(Path(tmpdir))
            valid = self.grouped("4")
            discovered = valid.accessions[0]
            unapproved_index_url = discovered.index_url.replace(
                "/data/1/", "/data/9/"
            )
            unapproved_sources = tuple(
                replace(
                    source,
                    entity_cik="0000000009",
                    entry_url=unapproved_index_url,
                )
                if source.entity_role == "issuer"
                else source
                for source in discovered.source_entries
            )
            unapproved = replace(
                valid,
                accessions=(
                    replace(
                        discovered,
                        issuer_cik="0000000009",
                        index_url=unapproved_index_url,
                        source_entries=unapproved_sources,
                    ),
                ),
            )

            with self.assertRaisesRegex(InsiderDiscoveryError, "approved issuer"):
                persist_incremental_discovery_queue(
                    state,
                    result=unapproved,
                    lookback_seconds=86_400,
                )
            with self.assertRaises(FileNotFoundError):
                state.read("incremental-v1")

            pending = persist_incremental_discovery_queue(
                state,
                result=valid,
                lookback_seconds=86_400,
            )
            state.update(
                "approved-issuers-v1",
                lambda current: {
                    **current,
                    "issuer_ciks": ["0000000009"],
                },
            )
            with self.assertRaisesRegex(InsiderDiscoveryError, "approved issuer"):
                persist_incremental_discovery_queue(
                    state,
                    result=unapproved,
                    lookback_seconds=86_400,
                )
            self.assertEqual(pending, state.read("incremental-v1"))

    def test_persistence_rechecks_approval_atomically_before_queue_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = self.approved_state_store(Path(tmpdir))
            result = self.grouped("4")
            approved_read = threading.Event()
            resume_writer = threading.Event()
            outcome: list[BaseException | None] = []
            original = insider_pipeline._durable_approved_issuer_ciks

            def pause_after_approved_read(
                state_store: InsiderStateStore,
            ) -> frozenset[str]:
                approved = original(state_store)
                approved_read.set()
                if not resume_writer.wait(5):
                    raise RuntimeError("approval race test timed out")
                return approved

            def persist() -> None:
                try:
                    persist_incremental_discovery_queue(
                        state,
                        result=result,
                        lookback_seconds=86_400,
                    )
                except BaseException as error:
                    outcome.append(error)
                else:
                    outcome.append(None)

            with patch.object(
                insider_pipeline,
                "_durable_approved_issuer_ciks",
                pause_after_approved_read,
            ):
                writer = threading.Thread(target=persist)
                writer.start()
                self.assertTrue(approved_read.wait(5))
                state.update(
                    "approved-issuers-v1",
                    lambda current: {
                        **current,
                        "issuer_ciks": ["0000000009"],
                    },
                )
                resume_writer.set()
                writer.join(5)

            self.assertFalse(writer.is_alive())
            self.assertEqual(1, len(outcome))
            self.assertIsInstance(outcome[0], InsiderDiscoveryError)
            with self.assertRaises(FileNotFoundError):
                state.read("incremental-v1")

    def test_persistence_revalidates_result_envelope_and_reporting_count(self) -> None:
        valid = self.grouped("4")
        accession = valid.accessions[0]
        invalids = (
            replace(valid, deadline_reached="false"),
            replace(valid, pages_fetched=True),
            replace(valid, quarantined_accessions=("not-an-accession",)),
            replace(
                valid,
                accessions=(replace(accession, reporting_entry_count=99),),
            ),
        )
        for invalid in invalids:
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as tmpdir:
                state = self.approved_state_store(Path(tmpdir))
                with self.assertRaises(InsiderDiscoveryError):
                    persist_incremental_discovery_queue(
                        state,
                        result=invalid,
                        lookback_seconds=86_400,
                    )

    def test_resumes_pending_batch_and_uses_verified_artifacts_for_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = self.approved_state_store(Path(tmpdir))
            form4_result = self.grouped("4")
            pending = persist_incremental_discovery_queue(
                state,
                result=form4_result,
                lookback_seconds=86_400,
            )
            resumed = persist_incremental_discovery_queue(
                state,
                result=self.grouped("3"),
                lookback_seconds=3_600,
            )
            self.assertEqual(pending, resumed)

            for resumable_status in ("pending", "running", "failed", "quarantined"):
                state.update(
                    "incremental-v1",
                    lambda current, status=resumable_status: {
                        **current,
                        "status": status,
                    },
                )
                preserved = persist_incremental_discovery_queue(
                    state,
                    result=self.grouped("3"),
                    lookback_seconds=3_600,
                )
                self.assertEqual(resumable_status, preserved["status"])
                self.assertEqual(pending["queue"], preserved["queue"])

            def mark_completed(current: dict[str, object]) -> dict[str, object]:
                queue = current["queue"]
                assert isinstance(queue, list)
                return {
                    **current,
                    "status": "completed",
                    "completed_accessions": [
                        entry["accession_number"] for entry in queue
                    ],
                }

            state.update("incremental-v1", mark_completed)
            reopened = persist_incremental_discovery_queue(
                state,
                result=form4_result,
                lookback_seconds=86_400,
                completed_artifact_verifier=lambda _accession: False,
            )
            self.assertEqual("incomplete", reopened["status"])
            self.assertEqual([], reopened["completed_accessions"])

            state.update("incremental-v1", mark_completed)
            verified: list[str] = []

            def verify(accession) -> bool:
                verified.append(accession.accession_number)
                return True

            completed = persist_incremental_discovery_queue(
                state,
                result=form4_result,
                lookback_seconds=86_400,
                completed_artifact_verifier=verify,
            )
            self.assertEqual([ACCESSION], verified)
            self.assertEqual("completed", completed["status"])
            self.assertEqual([ACCESSION], completed["completed_accessions"])

    def test_incremental_resume_action_requires_exact_pending_scope_and_bound(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = self.approved_state_store(
                Path(tmpdir),
                ("0000000001", "0000000002"),
            )
            pending = persist_incremental_discovery_queue(
                state,
                result=self.grouped(),
                lookback_seconds=86_400,
            )
            pending_queue = pending["queue"]
            assert isinstance(pending_queue, list)

            self.assertEqual(
                "resume",
                insider_pipeline.resolve_incremental_checkpoint_action(
                    pending,
                    issuer_ciks=("0000000001",),
                    max_accessions=6,
                ),
            )
            with self.assertRaisesRegex(
                InsiderDiscoveryError,
                "incremental checkpoint scope",
            ):
                insider_pipeline.resolve_incremental_checkpoint_action(
                    pending,
                    issuer_ciks=("0000000002",),
                    max_accessions=6,
                )
            with self.assertRaisesRegex(
                InsiderDiscoveryError,
                "incremental checkpoint scope",
            ):
                insider_pipeline.resolve_incremental_checkpoint_action(
                    pending,
                    issuer_ciks=("0000000001",),
                    max_accessions=5,
                )

            completed = {
                **pending,
                "status": "completed",
                "completed_accessions": sorted(
                    entry["accession_number"] for entry in pending_queue
                ),
            }
            self.assertEqual(
                "new",
                insider_pipeline.resolve_incremental_checkpoint_action(
                    completed,
                    issuer_ciks=("0000000002",),
                    max_accessions=1,
                ),
            )
            with self.assertRaisesRegex(
                InsiderDiscoveryError,
                "incremental checkpoint scope",
            ):
                insider_pipeline.validate_incremental_checkpoint_scope(
                    completed,
                    issuer_ciks=("0000000002",),
                    max_accessions=6,
                )

    def test_completed_rediscovery_cannot_rebind_validated_filing_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = self.approved_state_store(
                Path(tmpdir),
                ("0000000001", "0000000009"),
            )
            original = self.grouped("4")
            persist_incremental_discovery_queue(
                state,
                result=original,
                lookback_seconds=86_400,
            )

            def mark_completed(current: dict[str, object]) -> dict[str, object]:
                queue = current["queue"]
                assert isinstance(queue, list)
                return {
                    **current,
                    "status": "completed",
                    "completed_accessions": [
                        entry["accession_number"] for entry in queue
                    ],
                }

            state.update("incremental-v1", mark_completed)
            before = state.read("incremental-v1")
            discovered = original.accessions[0]
            conflicting_index_url = discovered.index_url.replace("/data/1/", "/data/9/")
            issuer_source = next(
                source
                for source in discovered.source_entries
                if source.entity_role == "issuer"
            )
            conflicting_issuer_source = replace(
                issuer_source,
                entity_cik="0000000009",
                entry_url=conflicting_index_url,
            )
            conflicting = replace(
                discovered,
                issuer_cik="0000000009",
                index_url=conflicting_index_url,
                source_entries=tuple(
                    conflicting_issuer_source if source is issuer_source else source
                    for source in discovered.source_entries
                ),
            )

            with self.assertRaisesRegex(InsiderDiscoveryError, "rediscovery binding"):
                persist_incremental_discovery_queue(
                    state,
                    result=replace(original, accessions=(conflicting,)),
                    lookback_seconds=86_400,
                    completed_artifact_verifier=lambda _candidate: True,
                )
            self.assertEqual(before, state.read("incremental-v1"))

    def test_state_revalidates_reporting_entry_urls_before_persistence(self) -> None:
        valid = self.grouped("4")
        accession = valid.accessions[0]
        reporting = accession.source_entries[1]
        for unsafe in (
            reporting.entry_url.replace("/data/2/", "/data/3/"),
            reporting.entry_url.replace("000000000126000001", "000000000126000002"),
            reporting.entry_url.replace("owner-entry.html", "%2e%2e.html"),
        ):
            with self.subTest(unsafe=unsafe), tempfile.TemporaryDirectory() as tmpdir:
                state = self.approved_state_store(Path(tmpdir))
                bad_reporting = replace(reporting, entry_url=unsafe)
                bad_accession = replace(
                    accession,
                    source_entries=(
                        accession.source_entries[0],
                        bad_reporting,
                        *accession.source_entries[2:],
                    ),
                )
                with self.assertRaises(ValueError):
                    persist_incremental_discovery_queue(
                        state,
                        result=replace(valid, accessions=(bad_accession,)),
                        lookback_seconds=86_400,
                    )

    def test_cooperative_deadline_stops_before_the_next_page_and_checkpoints(self) -> None:
        http = self.fixture_http()
        result = discover_recent_insider_accessions(
            approved_issuer_ciks=("0000000001",),
            lookback_seconds=86_400,
            max_pages=2,
            page_size=40,
            max_accessions=10,
            deadline_seconds=60,
            now=datetime(2026, 1, 17, tzinfo=timezone.utc),
            http=http,
            monotonic=lambda: (
                60.0 if http.responses and http.responses[0].closed else 0.0
            ),
        )
        self.assertEqual(1, result.pages_fetched)
        self.assertTrue(result.deadline_reached)
        self.assertEqual(1, len(http.calls))
        with tempfile.TemporaryDirectory() as tmpdir:
            state = self.approved_state_store(Path(tmpdir))
            persisted = persist_incremental_discovery_queue(
                state,
                result=result,
                lookback_seconds=86_400,
            )
        self.assertEqual("incomplete", persisted["status"])


class InsiderAccessionProcessorTests(unittest.TestCase):
    observed_at = "2026-01-17T00:00:00Z"

    @classmethod
    def candidate(cls):
        issuer = insider_pipeline.RecentInsiderFeedEntry(
            accession_number=ACCESSION,
            form_type="4",
            entity_role="issuer",
            entity_cik="0000000001",
            entry_url=INDEX_URL,
            accepted_at="2026-01-16T16:30:00Z",
            observed_at=cls.observed_at,
        )
        owner = insider_pipeline.RecentInsiderFeedEntry(
            accession_number=ACCESSION,
            form_type="4",
            entity_role="reporting_owner",
            entity_cik="0000000002",
            entry_url=OWNER_ENTRY_URL,
            accepted_at="2026-01-16T16:30:00Z",
            observed_at=cls.observed_at,
        )
        return insider_pipeline.DiscoveredInsiderAccession(
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            form_type="4",
            index_url=INDEX_URL,
            accepted_at="2026-01-16T16:30:00Z",
            observed_at=cls.observed_at,
            reporting_entry_count=1,
            source_entries=(issuer, owner),
        )

    @classmethod
    def discovery_result(cls):
        return insider_pipeline.IncrementalDiscoveryResult(
            accessions=(cls.candidate(),),
            quarantined_accessions=(),
            pages_fetched=1,
            deadline_reached=False,
        )

    @classmethod
    def prepared_stores(
        cls,
        root: Path,
        events: list[str],
        candidate=None,
    ):
        selected = cls.candidate() if candidate is None else candidate
        state = _RecordingInsiderStateStore(root, events)
        state.write(
            "approved-issuers-v1",
            {"contract_version": 1, "issuer_ciks": ["0000000001"]},
        )
        persist_incremental_discovery_queue(
            state,
            result=insider_pipeline.IncrementalDiscoveryResult(
                accessions=(selected,),
                quarantined_accessions=(),
                pages_fetched=1,
                deadline_reached=False,
            ),
            lookback_seconds=86_400,
        )
        events.clear()
        return _RecordingInsiderStorage(root, events), state

    @classmethod
    def process(
        cls,
        *,
        storage: InsiderStorage,
        state: InsiderStateStore,
        http: object,
        events: list[str],
        candidate=None,
        deadline=None,
        monotonic=lambda: 0.0,
    ):
        selected = cls.candidate() if candidate is None else candidate
        selected_deadline = (
            insider_pipeline.CooperativeDeadline(
                started_monotonic=0.0,
                deadline_seconds=60,
            )
            if deadline is None
            else deadline
        )
        with _record_processor_functions(events):
            return insider_pipeline.process_insider_accession(
                selected,
                storage=storage,
                state_store=state,
                approved_issuer_ciks=("0000000001",),
                parser_version=INSIDER_PARSER_VERSION,
                http=http,
                deadline=selected_deadline,
                monotonic=monotonic,
            )

    @staticmethod
    def http(events: list[str] | None = None) -> _ProcessorHTTP:
        return _ProcessorHTTP(
            {
                INDEX_URL: FIXTURE.read_bytes(),
                DOCUMENT_URL: RAW_FIXTURE.read_bytes(),
            },
            events,
        )

    def test_owner_filed_accession_prefix_is_independent_from_issuer(self) -> None:
        accession_number = "0000000002-26-000001"
        compact_accession = accession_number.replace("-", "")
        identity = insider_pipeline.InsiderAccessionIdentity(
            accession_number=accession_number,
            issuer_cik="0000000001",
            form_type="4",
            index_url=(
                "https://www.sec.gov/Archives/edgar/data/1/"
                f"{compact_accession}/{accession_number}-index.html"
            ),
            accepted_at="2026-01-16T16:30:00Z",
            reporting_owner_ciks=("0000000002",),
        )

        self.assertEqual(accession_number, identity.accession_number)
        self.assertEqual("0000000001", identity.issuer_cik)
        self.assertEqual(("0000000002",), identity.reporting_owner_ciks)

    def test_public_process_result_contract_rejects_malformed_values(self) -> None:
        baseline = {
            "accession_number": ACCESSION,
            "issuer_cik": "0000000001",
            "form_type": "4",
            "parser_version": INSIDER_PARSER_VERSION,
            "outcome": insider_pipeline.InsiderAccessionOutcome.CREATED,
            "stage": "checkpoint",
            "error_class": None,
            "reason_code": None,
        }
        valid_cases = (
            baseline,
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.CACHE_HIT,
                "stage": "cache",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
                "stage": "normalized",
                "reason_code": "deadline",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
                "error_class": "InsiderStorageError",
                "reason_code": "checkpoint_failed",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
                "stage": "raw",
                "error_class": "InsiderParseError",
                "reason_code": "raw_parse_invalid",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
                "stage": "index",
                "error_class": "OSError",
                "reason_code": "fetch_failed",
            },
        )
        for fields in valid_cases:
            with self.subTest(valid=fields):
                insider_pipeline.InsiderAccessionProcessResult(**fields)

        invalid_cases = (
            {**baseline, "accession_number": "bad"},
            {**baseline, "issuer_cik": "1"},
            {**baseline, "form_type": "6"},
            {**baseline, "parser_version": "future-parser"},
            {**baseline, "outcome": "created"},
            {**baseline, "stage": "arbitrary"},
            {**baseline, "error_class": "InsiderStorageError"},
            {**baseline, "reason_code": "deadline"},
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.CACHE_HIT,
                "stage": "cache",
                "reason_code": "deadline",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
                "stage": "source",
                "error_class": "InsiderStorageError",
                "reason_code": "deadline",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
                "reason_code": "checkpoint_failed",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
                "stage": "source",
                "error_class": "InsiderParseError",
                "reason_code": "raw_invalid",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
                "stage": "raw",
                "reason_code": "raw_invalid",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
                "stage": "source",
                "error_class": "OSError",
                "reason_code": "fetch_failed",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
                "stage": "raw",
                "reason_code": "fetch_failed",
            },
            {
                **baseline,
                "outcome": insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
                "stage": "raw",
                "error_class": "ArbitraryException",
                "reason_code": "raw_invalid",
            },
        )
        for fields in invalid_cases:
            with self.subTest(invalid=fields), self.assertRaises(
                InsiderDiscoveryError
            ):
                insider_pipeline.InsiderAccessionProcessResult(**fields)

    def test_http_error_retry_result_preserves_specific_public_error_class(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as tmpdir:
            storage, state = self.prepared_stores(Path(tmpdir), events)
            result = self.process(
                storage=storage,
                state=state,
                http=_ProcessorHTTP(
                    {INDEX_URL: requests.HTTPError("synthetic SEC status")},
                    events,
                ),
                events=events,
            )

        self.assertEqual(
            insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
            result.outcome,
        )
        self.assertEqual("index", result.stage)
        self.assertEqual("HTTPError", result.error_class)
        self.assertEqual("fetch_failed", result.reason_code)

    def test_shared_client_http_error_response_is_closed_exactly_once(self) -> None:
        response = Mock(spec=requests.Response)
        response.url = INDEX_URL
        response.status_code = 404
        response.headers = {}
        session = Mock()
        session.headers = {}
        session.get.return_value = response
        http = insider_pipeline.pipeline.RateLimitedSession(session=session, rate=8)

        with patch.object(http, "_claim_slot", return_value=0.0), self.assertRaises(
            requests.HTTPError
        ):
            insider_pipeline._fetch_bounded_processor_artifact(
                http,
                INDEX_URL,
                max_bytes=MAX_RECENT_INSIDER_ATOM_BYTES,
            )

        response.close.assert_called_once_with()

    def test_processor_stream_enforces_the_absolute_deadline(self) -> None:
        clock = [0.0]

        class LateProcessorResponse(_AtomResponse):
            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                clock[0] = 2.0
                yield self.content

        response = LateProcessorResponse(b"x", url=INDEX_URL)
        http = Mock()
        http.get.return_value = response

        with self.assertRaisesRegex(RuntimeError, "^SEC request deadline reached$"):
            insider_pipeline._fetch_bounded_processor_artifact(
                http,
                INDEX_URL,
                max_bytes=MAX_RECENT_INSIDER_ATOM_BYTES,
                deadline_monotonic=1.0,
                monotonic=lambda: clock[0],
            )

        self.assertTrue(response.closed)
        http.get.assert_called_once_with(
            INDEX_URL,
            stream=True,
            deadline_monotonic=1.0,
        )

    def test_processor_classifies_inflight_deadline_as_cooperative_checkpoint(
        self,
    ) -> None:
        class DeadlineAfterGetHTTP(_ProcessorHTTP):
            deadline_reached = False

            def get(self, url: str, **kwargs: object) -> _AtomResponse:
                response = super().get(url, **kwargs)
                self.deadline_reached = True
                return response

        with tempfile.TemporaryDirectory() as tmpdir:
            events: list[str] = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            http = DeadlineAfterGetHTTP({INDEX_URL: FIXTURE.read_bytes()}, events)

            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
                monotonic=lambda: 60.0 if http.deadline_reached else 0.0,
            )

        self.assertEqual(
            insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
            result.outcome,
        )
        self.assertEqual("index", result.stage)
        self.assertEqual("deadline", result.reason_code)
        self.assertIsNone(result.error_class)

    def test_processor_preserves_request_level_deadline_classification(self) -> None:
        class RequestDeadlineHTTP(_ProcessorHTTP):
            def get(self, url: str, **kwargs: object) -> _AtomResponse:
                del url, kwargs
                insider_pipeline.pipeline.sec_deadline_remaining(
                    0.0,
                    monotonic=lambda: 0.0,
                )
                raise AssertionError("deadline helper must raise")

        with tempfile.TemporaryDirectory() as tmpdir:
            events: list[str] = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            http = RequestDeadlineHTTP({}, events)

            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
            )

        self.assertEqual(
            insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
            result.outcome,
        )
        self.assertEqual("index", result.stage)
        self.assertEqual("deadline", result.reason_code)
        self.assertIsNone(result.error_class)

    def test_alternate_client_non_success_response_is_rejected_and_closed(self) -> None:
        for status_code in (404, 503):
            with self.subTest(status_code=status_code):
                response = _AtomResponse(
                    b"synthetic SEC error body",
                    url=INDEX_URL,
                    status_code=status_code,
                )
                http = Mock()
                http.get.return_value = response

                with self.assertRaises(requests.HTTPError) as raised:
                    insider_pipeline._fetch_bounded_processor_artifact(
                        http,
                        INDEX_URL,
                        max_bytes=MAX_RECENT_INSIDER_ATOM_BYTES,
                    )

                self.assertEqual("processor HTTP response failed", str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)
                self.assertIsNone(raised.exception.__context__)
                self.assertTrue(response.closed)
                http.get.assert_called_once_with(INDEX_URL, stream=True)

    def test_alternate_client_hostile_status_is_sanitized_and_closed(self) -> None:
        secret = "TASK5_ALTERNATE_STATUS_SECRET"

        class HostileStatusResponse:
            def __init__(self) -> None:
                self.url = INDEX_URL
                self.headers: dict[str, str] = {}
                self.closed = False

            @property
            def status_code(self) -> int:
                raise _HostileHTTPFailure(secret)

            def iter_content(self, chunk_size: int = 8192):
                yield b"synthetic body"

            def close(self) -> None:
                self.closed = True

        response = HostileStatusResponse()
        http = Mock()
        http.get.return_value = response

        with self.assertRaises(ValueError) as raised:
            insider_pipeline._fetch_bounded_processor_artifact(
                http,
                INDEX_URL,
                max_bytes=MAX_RECENT_INSIDER_ATOM_BYTES,
            )

        self.assertEqual("SEC response status is invalid", str(raised.exception))
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        self.assertTrue(response.closed)

    def test_deadline_rejects_unrepresentable_integer_clocks(self) -> None:
        with self.assertRaises(InsiderDiscoveryError):
            insider_pipeline.CooperativeDeadline(
                started_monotonic=10**400,
                deadline_seconds=60,
            )
        deadline = insider_pipeline.CooperativeDeadline(
            started_monotonic=0.0,
            deadline_seconds=60,
        )
        with self.assertRaises(InsiderDiscoveryError):
            deadline.reached(lambda: 10**400)

    def test_deadline_stops_before_cache_verification(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            events: list[str] = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            http = _ProcessorHTTP({}, events)

            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
                deadline=insider_pipeline.CooperativeDeadline(
                    started_monotonic=0.0,
                    deadline_seconds=60,
                ),
                monotonic=lambda: 60.0,
            )

            self.assertEqual([], state.read("incremental-v1")["completed_accessions"])

        self.assertEqual("checkpointed", result.outcome.value)
        self.assertEqual("cache", result.stage)
        self.assertEqual("deadline", result.reason_code)
        self.assertEqual([], http.calls)
        self.assertEqual([], events)

    def test_processor_orders_cache_index_raw_parse_source_normalized_then_checkpoint(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            events: list[str] = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            http = self.http(events)
            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
            )

        self.assertEqual("created", result.outcome.value)
        self.assertEqual("checkpoint", result.stage)
        self.assertEqual(
            [
                {"stream": True, "deadline_monotonic": 60.0},
                {"stream": True, "deadline_monotonic": 60.0},
            ],
            http.request_kwargs,
        )
        self.assertEqual(
            [
                "read_normalized",
                "read_index_html",
                "http_index",
                "store_index_html",
                "parse_index",
                "read_raw",
                "http_document",
                "store_raw",
                "parse_raw",
                "read_source_metadata",
                "build_source_metadata",
                "store_source_metadata",
                "store_normalized",
                "read_normalized",
                "write_issuer",
                "state_update",
            ],
            events,
        )

    def test_pending_candidate_reconstruction_rejects_malformed_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            events: list[str] = []
            _, state = self.prepared_stores(Path(tmpdir), events)
            valid = state.read("incremental-v1")

        unknown_queue_key = json.loads(json.dumps(valid))
        unknown_queue_key["queue"][0]["unknown"] = "x"
        unknown_source_key = json.loads(json.dumps(valid))
        unknown_source_key["source_entries"][0]["unknown"] = "x"
        impossible_completion = {
            **valid,
            "status": "completed",
            "completed_accessions": [],
        }
        for malformed in (
            unknown_queue_key,
            unknown_source_key,
            impossible_completion,
        ):
            with self.subTest(malformed=malformed), self.assertRaises(
                InsiderDiscoveryError
            ):
                insider_pipeline.pending_incremental_candidates(malformed)

    def test_processor_creates_all_immutable_artifacts_and_marks_only_verified_completion(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            result = self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
            )
            verified = InsiderStorage(root)
            self.assertEqual(FIXTURE.read_bytes(), verified.read_index_html(ACCESSION))
            self.assertEqual(RAW_FIXTURE.read_bytes(), verified.read_raw(ACCESSION))
            source = verified.read_source_metadata(ACCESSION)
            normalized = verified.read_normalized(ACCESSION, INSIDER_PARSER_VERSION)
            issuer = state.read("issuers/0000000001")
            checkpoint = state.read("incremental-v1")

        self.assertEqual("created", result.outcome.value)
        self.assertEqual(ACCESSION, source["accession_number"])
        self.assertEqual(ACCESSION, normalized["accession_number"])
        self.assertEqual(
            [ACCESSION],
            [entry["accession_number"] for entry in issuer["accessions"]],
        )
        self.assertEqual([ACCESSION], checkpoint["completed_accessions"])
        self.assertEqual("completed", checkpoint["status"])
        self.assertLess(events.index("read_normalized", 1), events.index("state_update"))

    def test_processor_accepts_current_sec_index_htm_url(self) -> None:
        index_url = INDEX_URL.removesuffix("l")
        candidate = self.candidate()
        sources = tuple(
            replace(source, entry_url=index_url)
            if source.entity_role == "issuer"
            else source
            for source in candidate.source_entries
        )
        candidate = replace(candidate, index_url=index_url, source_entries=sources)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events, candidate=candidate)
            result = self.process(
                storage=storage,
                state=state,
                http=_ProcessorHTTP(
                    {
                        index_url: FIXTURE.read_bytes(),
                        DOCUMENT_URL: RAW_FIXTURE.read_bytes(),
                    },
                    events,
                ),
                events=events,
                candidate=candidate,
            )
            source = InsiderStorage(root).read_source_metadata(ACCESSION)

        index = source["index"]
        assert isinstance(index, dict)
        self.assertEqual("created", result.outcome.value)
        self.assertEqual(index_url, index["url"])

    def test_processor_verified_cache_hit_makes_no_http_request_or_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
            )
            verifier = InsiderStorage(root)
            before = (
                verifier.read_index_html(ACCESSION),
                verifier.read_raw(ACCESSION),
                verifier.read_source_metadata(ACCESSION),
                verifier.read_normalized(ACCESSION, INSIDER_PARSER_VERSION),
                state.read("incremental-v1"),
            )
            events.clear()
            http = _ProcessorHTTP({}, events)

            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
            )
            after = (
                verifier.read_index_html(ACCESSION),
                verifier.read_raw(ACCESSION),
                verifier.read_source_metadata(ACCESSION),
                verifier.read_normalized(ACCESSION, INSIDER_PARSER_VERSION),
                state.read("incremental-v1"),
            )

        self.assertEqual("cache_hit", result.outcome.value)
        self.assertEqual("cache", result.stage)
        self.assertEqual([], http.calls)
        self.assertNotIn("state_update", events)
        self.assertEqual(before, after)

    def test_verified_cache_rebuilds_missing_issuer_state_without_http_then_is_idempotent(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
            )
            issuer_path = (
                root
                / insider_pipeline.PRIVATE_INSIDER_STATE_ROOT
                / "issuers"
                / "0000000001.json"
            )
            issuer_path.unlink()
            events.clear()
            rebuild_http = _ProcessorHTTP({}, events)

            rebuilt = self.process(
                storage=storage,
                state=state,
                http=rebuild_http,
                events=events,
            )
            rebuilt_state = state.read("issuers/0000000001")
            rebuilt_accessions = rebuilt_state["accessions"]
            assert isinstance(rebuilt_accessions, list)
            rebuild_events = tuple(events)
            events.clear()
            replay_http = _ProcessorHTTP({}, events)

            replay = self.process(
                storage=storage,
                state=state,
                http=replay_http,
                events=events,
            )
            replayed_state = state.read("issuers/0000000001")

        self.assertEqual("cache_hit", rebuilt.outcome.value)
        self.assertEqual([], rebuild_http.calls)
        self.assertIn("write_issuer", rebuild_events)
        self.assertEqual(
            [ACCESSION],
            [entry["accession_number"] for entry in rebuilt_accessions],
        )
        self.assertEqual("cache_hit", replay.outcome.value)
        self.assertEqual([], replay_http.calls)
        self.assertNotIn("write_issuer", events)
        self.assertNotIn("state_update", events)
        self.assertEqual(rebuilt_state, replayed_state)

    def test_amendment_before_original_rebuilds_issuer_state_when_original_arrives(
        self,
    ) -> None:
        amendment_accession = "0000000001-26-000005"
        amendment_compact = amendment_accession.replace("-", "")
        amendment_index_url = (
            "https://www.sec.gov/Archives/edgar/data/1/"
            f"{amendment_compact}/{amendment_accession}-index.html"
        )
        amendment_document_url = (
            "https://www.sec.gov/Archives/edgar/data/1/"
            f"{amendment_compact}/form4a-test-only.xml"
        )
        amendment_owner_url = (
            "https://www.sec.gov/Archives/edgar/data/2/"
            f"{amendment_compact}/owner-entry.html"
        )
        amendment_accepted_at = "2026-01-20T20:03:04Z"
        amendment_observed_at = "2026-01-21T00:00:00Z"
        amendment_candidate = insider_pipeline.DiscoveredInsiderAccession(
            accession_number=amendment_accession,
            issuer_cik="0000000001",
            form_type="4/A",
            index_url=amendment_index_url,
            accepted_at=amendment_accepted_at,
            observed_at=amendment_observed_at,
            reporting_entry_count=1,
            source_entries=(
                insider_pipeline.RecentInsiderFeedEntry(
                    accession_number=amendment_accession,
                    form_type="4/A",
                    entity_role="issuer",
                    entity_cik="0000000001",
                    entry_url=amendment_index_url,
                    accepted_at=amendment_accepted_at,
                    observed_at=amendment_observed_at,
                ),
                insider_pipeline.RecentInsiderFeedEntry(
                    accession_number=amendment_accession,
                    form_type="4/A",
                    entity_role="reporting_owner",
                    entity_cik="0000000002",
                    entry_url=amendment_owner_url,
                    accepted_at=amendment_accepted_at,
                    observed_at=amendment_observed_at,
                ),
            ),
        )
        amendment_index = (
            "<!doctype html><html><body>"
            '<div id="formName"><strong>Form 4/A</strong></div>'
            '<div class="infoHead">Filing Date</div>'
            '<div class="info">2026-01-20</div>'
            '<div class="infoHead">Accepted</div>'
            '<div class="info">2026-01-20 15:03:04</div>'
            '<table class="tableFile" summary="Document Format Files">'
            "<tr><th>Seq</th><th>Description</th><th>Document</th>"
            "<th>Type</th><th>Size</th></tr>"
            '<tr><td>1</td><td>FORM 4/A</td><td><a href="form4a-test-only.xml">'
            "form4a-test-only.xml</a></td><td>4/A</td><td>2518</td></tr>"
            "</table></body></html>"
        ).encode()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            state = _RecordingInsiderStateStore(root, events)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            persist_incremental_discovery_queue(
                state,
                result=insider_pipeline.IncrementalDiscoveryResult(
                    accessions=(self.candidate(), amendment_candidate),
                    quarantined_accessions=(),
                    pages_fetched=1,
                    deadline_reached=False,
                ),
                lookback_seconds=86_400,
            )
            storage = _RecordingInsiderStorage(root, events)
            events.clear()

            amendment_result = self.process(
                storage=storage,
                state=state,
                http=_ProcessorHTTP(
                    {
                        amendment_index_url: amendment_index,
                        amendment_document_url: AMENDMENT_RAW_FIXTURE.read_bytes(),
                    },
                    events,
                ),
                events=events,
                candidate=amendment_candidate,
            )
            unresolved = state.read("issuers/0000000001")
            events.clear()

            original_result = self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
            )
            resolved = state.read("issuers/0000000001")
            resolved_accessions = resolved["accessions"]
            assert isinstance(resolved_accessions, list)
            completed = state.read("incremental-v1")

        self.assertEqual("created", amendment_result.outcome.value)
        self.assertEqual(
            [{
                "accession_number": amendment_accession,
                "amends_accession": None,
                "confidence": "unresolved",
                "reason_code": "no_candidate",
                "candidates": [],
            }],
            unresolved["amendments"],
        )
        self.assertEqual("created", original_result.outcome.value)
        self.assertEqual(
            [ACCESSION, amendment_accession],
            [entry["accession_number"] for entry in resolved_accessions],
        )
        self.assertEqual(
            [{
                "accession_number": amendment_accession,
                "amends_accession": ACCESSION,
                "confidence": "high",
                "reason_code": "single_candidate",
                "candidates": [ACCESSION],
            }],
            resolved["amendments"],
        )
        self.assertEqual([], resolved["unresolved_ambiguities"])
        self.assertEqual(
            [ACCESSION, amendment_accession],
            completed["completed_accessions"],
        )

    def test_multi_owner_binding_is_order_independent_but_preserves_xml_order(
        self,
    ) -> None:
        issuer = insider_pipeline.RecentInsiderFeedEntry(
            accession_number=ACCESSION,
            form_type="4",
            entity_role="issuer",
            entity_cik="0000000001",
            entry_url=INDEX_URL,
            accepted_at="2026-01-16T16:30:00Z",
            observed_at=self.observed_at,
        )
        reporting_owners = tuple(
            insider_pipeline.RecentInsiderFeedEntry(
                accession_number=ACCESSION,
                form_type="4",
                entity_role="reporting_owner",
                entity_cik=cik,
                entry_url=OWNER_ENTRY_URL.replace(
                    "/data/2/",
                    f"/data/{int(cik)}/",
                ),
                accepted_at="2026-01-16T16:30:00Z",
                observed_at=self.observed_at,
            )
            for cik in ("0000000003", "0000000004")
        )
        candidate = insider_pipeline.DiscoveredInsiderAccession(
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            form_type="4",
            index_url=INDEX_URL,
            accepted_at="2026-01-16T16:30:00Z",
            observed_at=self.observed_at,
            reporting_entry_count=2,
            source_entries=(issuer, *reporting_owners),
        )
        reversed_raw = (
            JOINT_RAW_FIXTURE.read_bytes()
            .replace(
                b"<rptOwnerCik>3</rptOwnerCik>",
                b"<rptOwnerCik>9999999999</rptOwnerCik>",
            )
            .replace(
                b"<rptOwnerCik>0000000004</rptOwnerCik>",
                b"<rptOwnerCik>3</rptOwnerCik>",
            )
            .replace(
                b"<rptOwnerCik>9999999999</rptOwnerCik>",
                b"<rptOwnerCik>0000000004</rptOwnerCik>",
            )
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events, candidate)
            first_http = _ProcessorHTTP(
                {
                    INDEX_URL: FIXTURE.read_bytes(),
                    DOCUMENT_URL: reversed_raw,
                },
                events,
            )
            first = self.process(
                storage=storage,
                state=state,
                http=first_http,
                events=events,
                candidate=candidate,
            )
            verified = InsiderStorage(root)
            source = verified.read_source_metadata(ACCESSION)
            normalized = verified.read_normalized(ACCESSION, INSIDER_PARSER_VERSION)

            events.clear()
            replay_http = _ProcessorHTTP({}, events)
            replay = self.process(
                storage=storage,
                state=state,
                http=replay_http,
                events=events,
                candidate=candidate,
            )

        self.assertEqual("created", first.outcome.value)
        self.assertEqual(
            ["0000000003", "0000000004"],
            source["reporting_owner_ciks"],
        )
        self.assertEqual(
            ["0000000004", "0000000003"],
            [owner["cik"] for owner in normalized["owners"]],
        )
        self.assertEqual("cache_hit", replay.outcome.value)
        self.assertEqual([], replay_http.calls)

    def test_processor_resumes_from_verified_index_only_without_refetching_index(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            storage.store_index_html(ACCESSION, FIXTURE.read_bytes())
            events.clear()
            http = self.http(events)

            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
            )

            self.assertEqual([DOCUMENT_URL], http.calls)
            self.assertNotIn("store_index_html", events)
            self.assertEqual(
                [ACCESSION], state.read("incremental-v1")["completed_accessions"]
            )
            self.assertEqual(
                ACCESSION,
                InsiderStorage(root).read_normalized(
                    ACCESSION,
                    INSIDER_PARSER_VERSION,
                )["accession_number"],
            )

        self.assertEqual("created", result.outcome.value)

    def test_processor_resumes_from_verified_raw_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            storage.store_index_html(ACCESSION, FIXTURE.read_bytes())
            storage.store_raw(ACCESSION, RAW_FIXTURE.read_bytes())
            events.clear()
            http = _ProcessorHTTP({}, events)

            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
            )

            self.assertEqual([], http.calls)
            self.assertNotIn("store_index_html", events)
            self.assertNotIn("store_raw", events)
            self.assertIn("store_source_metadata", events)
            self.assertIn("store_normalized", events)
            self.assertEqual(
                [ACCESSION], state.read("incremental-v1")["completed_accessions"]
            )

        self.assertEqual("created", result.outcome.value)

    def test_processor_resumes_from_verified_source_metadata_at_normalization(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            index_html = FIXTURE.read_bytes()
            raw_xml = RAW_FIXTURE.read_bytes()
            storage.store_index_html(ACCESSION, index_html)
            storage.store_raw(ACCESSION, raw_xml)
            index_metadata = parse_insider_filing_index(
                index_html,
                index_url=INDEX_URL,
                accession_number=ACCESSION,
                issuer_cik="0000000001",
                reporting_owner_ciks=("0000000002",),
            )
            storage.store_source_metadata(
                ACCESSION,
                build_insider_source_metadata(
                    index_metadata,
                    index_html,
                    raw_xml,
                ),
            )
            events.clear()
            http = _ProcessorHTTP({}, events)

            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
            )

            self.assertEqual([], http.calls)
            self.assertNotIn("build_source_metadata", events)
            self.assertNotIn("store_source_metadata", events)
            self.assertIn("store_normalized", events)
            self.assertEqual(
                [ACCESSION], state.read("incremental-v1")["completed_accessions"]
            )

        self.assertEqual("created", result.outcome.value)

    def test_processor_never_publishes_source_or_normalized_before_raw(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            events: list[str] = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
            )
            self.assertLess(events.index("store_raw"), events.index("store_source_metadata"))
            self.assertLess(
                events.index("store_source_metadata"),
                events.index("store_normalized"),
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            events = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            http = _ProcessorHTTP(
                {
                    INDEX_URL: FIXTURE.read_bytes(),
                    DOCUMENT_URL: RuntimeError("temporary raw fetch failure"),
                },
                events,
            )
            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
            )
            checkpoint = state.read("incremental-v1")

        self.assertEqual("retry_later", result.outcome.value)
        self.assertEqual("raw", result.stage)
        self.assertNotIn("store_raw", events)
        self.assertNotIn("store_source_metadata", events)
        self.assertNotIn("store_normalized", events)
        self.assertNotIn("state_update", events)
        self.assertEqual([], checkpoint["completed_accessions"])

    def test_verified_cache_with_pending_checkpoint_resumes_without_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
            )
            state.update(
                "incremental-v1",
                lambda current: {
                    **current,
                    "status": "incomplete",
                    "completed_accessions": [],
                },
            )
            events.clear()
            http = _ProcessorHTTP({}, events)

            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
            )

            self.assertEqual([], http.calls)
            self.assertEqual(
                [ACCESSION], state.read("incremental-v1")["completed_accessions"]
            )

        self.assertEqual("checkpointed", result.outcome.value)
        self.assertEqual("checkpoint", result.stage)

    def test_explicit_default_port_converges_on_identical_immutable_artifacts(
        self,
    ) -> None:
        port_index_url = INDEX_URL.replace("www.sec.gov", "www.sec.gov:443")
        candidate = self.candidate()
        port_candidate = replace(
            candidate,
            index_url=port_index_url,
            source_entries=(
                replace(candidate.source_entries[0], entry_url=port_index_url),
                *candidate.source_entries[1:],
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events, candidate)
            self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
                candidate=candidate,
            )
            verifier = InsiderStorage(root)
            before = (
                verifier.read_index_html(ACCESSION),
                verifier.read_raw(ACCESSION),
                verifier.read_source_metadata(ACCESSION),
                verifier.read_normalized(ACCESSION, INSIDER_PARSER_VERSION),
            )
            events.clear()

            result = self.process(
                storage=storage,
                state=state,
                http=_ProcessorHTTP({}, events),
                events=events,
                candidate=port_candidate,
            )
            after = (
                verifier.read_index_html(ACCESSION),
                verifier.read_raw(ACCESSION),
                verifier.read_source_metadata(ACCESSION),
                verifier.read_normalized(ACCESSION, INSIDER_PARSER_VERSION),
            )

        self.assertEqual("cache_hit", result.outcome.value)
        self.assertEqual(before, after)
        self.assertEqual(INDEX_URL, before[2]["index"]["url"])

    def test_incremental_and_backfill_discovery_evidence_converges(self) -> None:
        incremental = self.candidate()
        backfill_observed_at = "2026-04-01T00:00:00Z"
        backfill = replace(
            incremental,
            observed_at=backfill_observed_at,
            source_entries=tuple(
                replace(source, observed_at=backfill_observed_at)
                for source in incremental.source_entries
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events, incremental)
            created = self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
                candidate=incremental,
            )
            verifier = InsiderStorage(root)
            before = (
                verifier.read_index_html(ACCESSION),
                verifier.read_raw(ACCESSION),
                verifier.read_source_metadata(ACCESSION),
                verifier.read_normalized(ACCESSION, INSIDER_PARSER_VERSION),
            )
            events.clear()
            http = _ProcessorHTTP({}, events)

            replay = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
                candidate=backfill,
            )
            after = (
                verifier.read_index_html(ACCESSION),
                verifier.read_raw(ACCESSION),
                verifier.read_source_metadata(ACCESSION),
                verifier.read_normalized(ACCESSION, INSIDER_PARSER_VERSION),
            )

        self.assertEqual("created", created.outcome.value)
        self.assertEqual("cache_hit", replay.outcome.value)
        self.assertEqual([], http.calls)
        self.assertEqual(before, after)
        self.assertNotIn(backfill_observed_at, json.dumps(after[2], sort_keys=True))

    def test_wrong_form_or_owner_relationship_is_deterministically_quarantined(
        self,
    ) -> None:
        candidate = self.candidate()
        wrong_form = replace(
            candidate,
            form_type="4/A",
            source_entries=tuple(
                replace(source, form_type="4/A")
                for source in candidate.source_entries
            ),
        )
        wrong_owner_url = OWNER_ENTRY_URL.replace("/data/2/", "/data/3/")
        wrong_owner = replace(
            candidate,
            source_entries=(
                candidate.source_entries[0],
                replace(
                    candidate.source_entries[1],
                    entity_cik="0000000003",
                    entry_url=wrong_owner_url,
                ),
            ),
        )
        for selected, expected_stage, quarantine_stage in (
            (wrong_form, "index", "discovery"),
            (wrong_owner, "raw", "raw"),
        ):
            with self.subTest(candidate=selected), tempfile.TemporaryDirectory() as tmpdir:
                events: list[str] = []
                storage, state = self.prepared_stores(
                    Path(tmpdir),
                    events,
                    selected,
                )
                result = self.process(
                    storage=storage,
                    state=state,
                    http=self.http(events),
                    events=events,
                    candidate=selected,
                )
                self.assertEqual("quarantined", result.outcome.value)
                self.assertEqual(expected_stage, result.stage)
                self.assertNotEqual("fetch_failed", result.reason_code)
                self.assertEqual(
                    [], state.read("incremental-v1")["completed_accessions"]
                )
                quarantine = state.read(f"quarantine/accessions/{ACCESSION}")
                self.assertEqual(quarantine_stage, quarantine["stage"])

                events.clear()
                replay_http = _ProcessorHTTP({}, events)
                replay = self.process(
                    storage=storage,
                    state=state,
                    http=replay_http,
                    events=events,
                    candidate=selected,
                )
                self.assertEqual("quarantined", replay.outcome.value)
                self.assertEqual(quarantine_stage, replay.stage)
                self.assertEqual([], replay_http.calls)
                self.assertNotIn("parse_index", events)
                self.assertNotIn("parse_raw", events)

    def test_invalid_final_response_metadata_and_cleanup_stays_quarantined(
        self,
    ) -> None:
        secret = "TASK5_PROCESSOR_RESPONSE_SECRET"

        class HostileResponse:
            headers: dict[str, str] = {}

            def __init__(self, *, exploding_url: bool) -> None:
                self.exploding_url = exploding_url
                self.close_calls = 0

            @property
            def url(self) -> str:
                if self.exploding_url:
                    raise RuntimeError(secret)
                return "https://example.invalid/Archives/a"

            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                raise AssertionError("invalid response must not be read")
                yield b""  # pragma: no cover - preserve generator shape

            def close(self) -> None:
                self.close_calls += 1
                raise RuntimeError(secret)

        class HostileHTTP:
            def __init__(self, response: HostileResponse) -> None:
                self.response = response
                self.calls: list[str] = []

            def get(self, url: str, **_kwargs: object) -> HostileResponse:
                self.calls.append(url)
                return self.response

        cases = (
            ("index", False, "discovery", "discovery_invalid", "index_invalid"),
            ("raw", True, "raw", "raw_invalid", "raw_invalid"),
        )
        for stage, exploding_url, durable_stage, durable_reason, result_reason in cases:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as tmpdir:
                events: list[str] = []
                storage, state = self.prepared_stores(Path(tmpdir), events)
                if stage == "raw":
                    storage.store_index_html(ACCESSION, FIXTURE.read_bytes())
                    events.clear()
                response = HostileResponse(exploding_url=exploding_url)
                http = HostileHTTP(response)

                result = self.process(
                    storage=storage,
                    state=state,
                    http=http,
                    events=events,
                )
                quarantine = state.read(f"quarantine/accessions/{ACCESSION}")

                self.assertEqual("quarantined", result.outcome.value)
                self.assertEqual(stage, result.stage)
                self.assertEqual(result_reason, result.reason_code)
                self.assertEqual(durable_stage, quarantine["stage"])
                self.assertEqual(durable_reason, quarantine["reason_code"])
                self.assertEqual(1, response.close_calls)
                self.assertNotIn(secret, repr(result))
                self.assertNotIn(secret, json.dumps(quarantine, sort_keys=True))
                self.assertEqual(
                    [], state.read("incremental-v1")["completed_accessions"]
                )

    def test_alternate_processor_http_base_exceptions_are_sanitized(self) -> None:
        secret = "TASK5_ALTERNATE_HTTP_BASE_EXCEPTION_SECRET"

        class ExplodingGetAttribute:
            @property
            def get(self):
                raise _HostileHTTPFailure(secret)

        class ExplodingGetMethod:
            def get(self, _url: str, **_kwargs: object):
                raise _HostileHTTPFailure(secret)

        cases = (
            (ExplodingGetAttribute(), "quarantined", "index_invalid"),
            (ExplodingGetMethod(), "retry_later", "fetch_failed"),
        )
        for http, outcome, reason_code in cases:
            with (
                self.subTest(http=type(http).__name__),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                events: list[str] = []
                storage, state = self.prepared_stores(Path(tmpdir), events)
                result = self.process(
                    storage=storage,
                    state=state,
                    http=http,
                    events=events,
                )

                self.assertEqual(outcome, result.outcome.value)
                self.assertEqual(reason_code, result.reason_code)
                self.assertNotIn(secret, repr(result))
                self.assertEqual(
                    [], state.read("incremental-v1")["completed_accessions"]
                )
                if outcome == "quarantined":
                    quarantine = state.read(f"quarantine/accessions/{ACCESSION}")
                    self.assertNotIn(secret, json.dumps(quarantine, sort_keys=True))

    def test_alternate_processor_http_control_flow_is_preserved(self) -> None:
        controls = (
            KeyboardInterrupt(),
            SystemExit(),
            GeneratorExit(),
            asyncio.CancelledError(),
            concurrent.futures.CancelledError(),
        )

        class ExplodingHTTP:
            def __init__(self, error: BaseException) -> None:
                self.error = error

            def get(self, _url: str, **_kwargs: object):
                raise self.error

        for control in controls:
            with (
                self.subTest(control=type(control).__name__),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                events: list[str] = []
                storage, state = self.prepared_stores(Path(tmpdir), events)
                with self.assertRaises(type(control)):
                    self.process(
                        storage=storage,
                        state=state,
                        http=ExplodingHTTP(control),
                        events=events,
                    )
                self.assertEqual(
                    [], state.read("incremental-v1")["completed_accessions"]
                )

    def test_raw_bounds_secure_parser_and_immutable_conflict_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "insider_pipeline.MAX_RAW_XML_BYTES",
            32,
        ):
            events: list[str] = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            http = _ProcessorHTTP(
                {
                    INDEX_URL: FIXTURE.read_bytes(),
                    DOCUMENT_URL: b"x" * 33,
                },
                events,
            )
            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
            )
            self.assertEqual("quarantined", result.outcome.value)
            self.assertEqual("raw", result.stage)
            self.assertTrue(http.responses[-1].closed)
            self.assertNotIn("store_raw", events)

        unsafe_raw = b'<!DOCTYPE x [<!ENTITY y SYSTEM "file:///etc/passwd">]><x>&y;</x>'
        with tempfile.TemporaryDirectory() as tmpdir:
            events = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            storage.store_raw(ACCESSION, unsafe_raw)
            events.clear()
            result = self.process(
                storage=storage,
                state=state,
                http=_ProcessorHTTP({INDEX_URL: FIXTURE.read_bytes()}, events),
                events=events,
            )
            self.assertEqual("quarantined", result.outcome.value)
            self.assertEqual("raw_parse_invalid", result.reason_code)
            self.assertEqual(unsafe_raw, storage.read_raw(ACCESSION))
            self.assertNotIn("store_source_metadata", events)
            self.assertNotIn("store_normalized", events)

    def test_deterministic_bounded_response_failures_are_not_retried_blindly(
        self,
    ) -> None:
        cases = (
            (
                "index",
                "MAX_INDEX_HTML_BYTES",
                {INDEX_URL: FIXTURE.read_bytes()},
                "discovery",
                "discovery_invalid",
            ),
            (
                "raw",
                "MAX_RAW_XML_BYTES",
                {
                    INDEX_URL: FIXTURE.read_bytes(),
                    DOCUMENT_URL: b"x" * 33,
                },
                "raw",
                "raw_invalid",
            ),
        )
        for stage, limit_name, bodies, quarantine_stage, reason_code in cases:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                events: list[str] = []
                storage, state = self.prepared_stores(root, events)
                with patch.object(insider_pipeline, limit_name, 32):
                    typed_bodies: dict[str, bytes | BaseException] = dict(bodies)
                    first_http = _ProcessorHTTP(typed_bodies, events)
                    first = self.process(
                        storage=storage,
                        state=state,
                        http=first_http,
                        events=events,
                    )
                    quarantine = state.read(
                        f"quarantine/accessions/{ACCESSION}"
                    )
                    events.clear()
                    replay_http = _ProcessorHTTP({}, events)
                    replay = self.process(
                        storage=storage,
                        state=state,
                        http=replay_http,
                        events=events,
                    )

                self.assertEqual("quarantined", first.outcome.value)
                self.assertEqual(stage, first.stage)
                self.assertTrue(all(response.closed for response in first_http.responses))
                self.assertEqual(quarantine_stage, quarantine["stage"])
                self.assertEqual(reason_code, quarantine["reason_code"])
                self.assertEqual("quarantined", replay.outcome.value)
                self.assertEqual(quarantine_stage, replay.stage)
                self.assertEqual(reason_code, replay.reason_code)
                self.assertEqual([], replay_http.calls)
                self.assertNotIn("parse_index", events)
                self.assertNotIn("parse_raw", events)
                self.assertEqual(
                    [], state.read("incremental-v1")["completed_accessions"]
                )

    def test_deterministic_raw_quarantine_is_durable_and_not_retried_blindly(
        self,
    ) -> None:
        unsafe_raw = (
            b'<!DOCTYPE x [<!ENTITY y SYSTEM "file:///etc/passwd">]><x>&y;</x>'
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            storage.store_raw(ACCESSION, unsafe_raw)
            events.clear()

            first_http = _ProcessorHTTP({INDEX_URL: FIXTURE.read_bytes()}, events)
            first = self.process(
                storage=storage,
                state=state,
                http=first_http,
                events=events,
            )
            quarantine = state.read(f"quarantine/accessions/{ACCESSION}")
            before = json.loads(json.dumps(quarantine))

            events.clear()
            second_http = _ProcessorHTTP({}, events)
            second = self.process(
                storage=storage,
                state=state,
                http=second_http,
                events=events,
            )
            after = state.read(f"quarantine/accessions/{ACCESSION}")
            second_events = tuple(events)

            state.update(
                f"quarantine/accessions/{ACCESSION}",
                lambda current: {**current, "parser_version": "older-parser"},
            )
            events.clear()
            third = self.process(
                storage=storage,
                state=state,
                http=_ProcessorHTTP({}, events),
                events=events,
            )
            refreshed = state.read(f"quarantine/accessions/{ACCESSION}")

        self.assertEqual("quarantined", first.outcome.value)
        self.assertEqual("raw_parse_invalid", first.reason_code)
        self.assertEqual("quarantined", second.outcome.value)
        self.assertEqual("raw", second.stage)
        self.assertEqual("raw_invalid", second.reason_code)
        self.assertEqual([], second_http.calls)
        self.assertNotIn("parse_raw", second_events)
        self.assertEqual("quarantined", third.outcome.value)
        self.assertIn("parse_raw", events)
        self.assertEqual(INSIDER_PARSER_VERSION, refreshed["parser_version"])
        self.assertEqual(before, after)
        self.assertEqual(
            {
                "contract_version": 1,
                "stage": "raw",
                "error_class": "InsiderParseError",
                "reason_code": "raw_invalid",
                "retry_count": 0,
                "next_retry_at": None,
                "parser_version": INSIDER_PARSER_VERSION,
                "source_hashes": sorted(
                    {
                        hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),
                        hashlib.sha256(unsafe_raw).hexdigest(),
                    }
                ),
                "accession_number": ACCESSION,
                "issuer_cik": "0000000001",
                "form_type": "4",
                "index_url": INDEX_URL,
                "accepted_at": "2026-01-16T16:30:00Z",
                "reporting_owner_ciks": ["0000000002"],
            },
            quarantine,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events = []
            _, state = self.prepared_stores(root, events)
            storage = _ConflictingRawPublishStorage(root, events)
            result = self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
            )
            quarantine = state.read(f"quarantine/accessions/{ACCESSION}")
            events.clear()
            replay_http = _ProcessorHTTP({}, events)
            replay = self.process(
                storage=storage,
                state=state,
                http=replay_http,
                events=events,
            )
            self.assertEqual("quarantined", result.outcome.value)
            self.assertEqual("raw_invalid", result.reason_code)
            self.assertEqual("raw", quarantine["stage"])
            self.assertEqual("raw_invalid", quarantine["reason_code"])
            self.assertEqual("quarantined", replay.outcome.value)
            self.assertEqual("raw", replay.stage)
            self.assertEqual("raw_invalid", replay.reason_code)
            self.assertEqual([], replay_http.calls)
            self.assertNotIn("parse_index", events)
            self.assertNotIn("parse_raw", events)
            self.assertEqual(storage.conflicting_raw, storage.read_raw(ACCESSION))
            self.assertNotIn("store_source_metadata", events)
            self.assertNotIn("store_normalized", events)
            self.assertEqual(
                [], state.read("incremental-v1")["completed_accessions"]
            )

    def test_existing_quarantine_degrades_to_cache_quarantine_after_corruption(
        self,
    ) -> None:
        unsafe_raw = (
            b'<!DOCTYPE x [<!ENTITY y SYSTEM "file:///etc/passwd">]><x>&y;</x>'
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            storage.store_raw(ACCESSION, unsafe_raw)
            first = self.process(
                storage=storage,
                state=state,
                http=_ProcessorHTTP({INDEX_URL: FIXTURE.read_bytes()}, events),
                events=events,
            )
            original_quarantine = state.read(f"quarantine/accessions/{ACCESSION}")

            index_path = storage.private_root / "accessions" / ACCESSION / "index.html"
            index_path.chmod(0o644)
            events.clear()
            second_http = _ProcessorHTTP({}, events)
            second = self.process(
                storage=storage,
                state=state,
                http=second_http,
                events=events,
            )
            cache_quarantine = state.read(f"quarantine/accessions/{ACCESSION}")

            events.clear()
            third_http = _ProcessorHTTP({}, events)
            third = self.process(
                storage=storage,
                state=state,
                http=third_http,
                events=events,
            )

        self.assertEqual("quarantined", first.outcome.value)
        self.assertEqual("raw", original_quarantine["stage"])
        self.assertEqual("quarantined", second.outcome.value)
        self.assertEqual("cache", second.stage)
        self.assertEqual("cache_invalid", second.reason_code)
        self.assertEqual("cache", cache_quarantine["stage"])
        self.assertEqual("cache_invalid", cache_quarantine["reason_code"])
        self.assertEqual([], cache_quarantine["source_hashes"])
        self.assertEqual([], second_http.calls)
        self.assertEqual("quarantined", third.outcome.value)
        self.assertEqual("cache", third.stage)
        self.assertEqual("cache_invalid", third.reason_code)
        self.assertEqual([], third_http.calls)

    def test_deterministic_source_quarantine_is_durable_and_not_retried_blindly(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            with patch.object(
                insider_pipeline,
                "build_insider_source_metadata",
                side_effect=InsiderStorageError("synthetic deterministic source failure"),
            ):
                first = self.process(
                    storage=storage,
                    state=state,
                    http=self.http(events),
                    events=events,
                )
            quarantine = state.read(f"quarantine/accessions/{ACCESSION}")
            before = json.loads(json.dumps(quarantine))

            events.clear()
            replay_http = _ProcessorHTTP({}, events)
            second = self.process(
                storage=storage,
                state=state,
                http=replay_http,
                events=events,
            )
            after = state.read(f"quarantine/accessions/{ACCESSION}")

        self.assertEqual("quarantined", first.outcome.value)
        self.assertEqual("source", first.stage)
        self.assertEqual("source_invalid", first.reason_code)
        self.assertEqual("source", quarantine["stage"])
        self.assertEqual("InsiderStorageError", quarantine["error_class"])
        self.assertEqual("source_invalid", quarantine["reason_code"])
        self.assertEqual(
            sorted(
                {
                    hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),
                    hashlib.sha256(RAW_FIXTURE.read_bytes()).hexdigest(),
                }
            ),
            quarantine["source_hashes"],
        )
        self.assertEqual("quarantined", second.outcome.value)
        self.assertEqual("source", second.stage)
        self.assertEqual("source_invalid", second.reason_code)
        self.assertEqual([], replay_http.calls)
        self.assertNotIn("parse_index", events)
        self.assertNotIn("parse_raw", events)
        self.assertNotIn("build_source_metadata", events)
        self.assertEqual(before, after)

    def test_contract_errors_use_bounded_public_and_durable_error_class(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            with patch.object(
                insider_pipeline,
                "build_insider_source_metadata",
                side_effect=InsiderContractError("synthetic contract failure"),
            ):
                result = self.process(
                    storage=storage,
                    state=state,
                    http=self.http(events),
                    events=events,
                )
            quarantine = state.read(f"quarantine/accessions/{ACCESSION}")

        self.assertEqual("quarantined", result.outcome.value)
        self.assertEqual("source", result.stage)
        self.assertEqual("InsiderContractError", result.error_class)
        self.assertEqual("source_invalid", result.reason_code)
        self.assertEqual("InsiderContractError", quarantine["error_class"])
        self.assertEqual("source_invalid", quarantine["reason_code"])

    def test_foreign_quarantine_stage_does_not_suppress_task5_processing(self) -> None:
        candidate = self.candidate()
        with tempfile.TemporaryDirectory() as tmpdir:
            events: list[str] = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            state.write(
                f"quarantine/accessions/{ACCESSION}",
                {
                    "contract_version": 1,
                    "stage": "archive",
                    "error_class": "InsiderStorageError",
                    "reason_code": "archive_invalid",
                    "retry_count": 0,
                    "next_retry_at": None,
                    "parser_version": INSIDER_PARSER_VERSION,
                    "source_hashes": [],
                    "accession_number": ACCESSION,
                    "issuer_cik": candidate.issuer_cik,
                    "form_type": candidate.form_type,
                    "index_url": INDEX_URL,
                    "accepted_at": candidate.accepted_at,
                    "reporting_owner_ciks": ["0000000002"],
                },
            )
            events.clear()

            result = self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
            )

        self.assertEqual("created", result.outcome.value)
        self.assertEqual("checkpoint", result.stage)
        self.assertIn("store_normalized", events)

    def test_corrupt_cached_artifacts_are_durably_quarantined_without_http_replay(
        self,
    ) -> None:
        for artifact in ("index", "raw", "normalized"):
            with self.subTest(artifact=artifact), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                events: list[str] = []
                storage, state = self.prepared_stores(root, events)
                accession_root = storage.private_root / "accessions" / ACCESSION

                if artifact == "index":
                    storage.store_index_html(ACCESSION, FIXTURE.read_bytes())
                    target = accession_root / "index.html"
                elif artifact == "raw":
                    storage.store_index_html(ACCESSION, FIXTURE.read_bytes())
                    storage.store_raw(ACCESSION, RAW_FIXTURE.read_bytes())
                    target = accession_root / "raw.xml"
                else:
                    created = self.process(
                        storage=storage,
                        state=state,
                        http=self.http(events),
                        events=events,
                    )
                    self.assertEqual("created", created.outcome.value)
                    target = (
                        accession_root
                        / "normalized"
                        / f"{INSIDER_PARSER_VERSION}.json"
                    )

                target.chmod(0o644)
                events.clear()
                first_http = _ProcessorHTTP({}, events)
                first = self.process(
                    storage=storage,
                    state=state,
                    http=first_http,
                    events=events,
                )
                quarantine = state.read(f"quarantine/accessions/{ACCESSION}")

                events.clear()
                second_http = _ProcessorHTTP({}, events)
                second = self.process(
                    storage=storage,
                    state=state,
                    http=second_http,
                    events=events,
                )

                self.assertEqual("quarantined", first.outcome.value)
                self.assertEqual("cache", first.stage)
                self.assertEqual("cache_invalid", first.reason_code)
                self.assertEqual("cache", quarantine["stage"])
                self.assertEqual("cache_invalid", quarantine["reason_code"])
                self.assertEqual([], quarantine["source_hashes"])
                self.assertEqual("quarantined", second.outcome.value)
                self.assertEqual("cache", second.stage)
                self.assertEqual("cache_invalid", second.reason_code)
                self.assertEqual([], first_http.calls)
                self.assertEqual([], second_http.calls)

    def test_deterministic_quarantine_replay_requires_exact_discovered_identity(
        self,
    ) -> None:
        unsafe_raw = (
            b'<!DOCTYPE x [<!ENTITY y SYSTEM "file:///etc/passwd">]><x>&y;</x>'
        )
        baseline = self.candidate()
        changed_accepted_at = "2026-01-16T16:31:00Z"
        changed_owner_url = (
            "https://www.sec.gov/Archives/edgar/data/3/000000000126000001/"
            "owner-entry.html"
        )
        cases = {
            "accepted_at": (
                replace(
                    baseline,
                    accepted_at=changed_accepted_at,
                    source_entries=tuple(
                        replace(entry, accepted_at=changed_accepted_at)
                        for entry in baseline.source_entries
                    ),
                ),
                "index_parse_invalid",
            ),
            "reporting_owner_ciks": (
                replace(
                    baseline,
                    source_entries=(
                        baseline.source_entries[0],
                        replace(
                            baseline.source_entries[1],
                            entity_cik="0000000003",
                            entry_url=changed_owner_url,
                        ),
                    ),
                ),
                "raw_parse_invalid",
            ),
        }
        for label, (changed_candidate, expected_reason) in cases.items():
            with self.subTest(identity_field=label), tempfile.TemporaryDirectory() as tmpdir:
                events: list[str] = []
                storage, state = self.prepared_stores(Path(tmpdir), events)
                storage.store_raw(ACCESSION, unsafe_raw)
                events.clear()
                first = self.process(
                    storage=storage,
                    state=state,
                    http=_ProcessorHTTP({INDEX_URL: FIXTURE.read_bytes()}, events),
                    events=events,
                )
                before = state.read(f"quarantine/accessions/{ACCESSION}")

                self.assertEqual("quarantined", first.outcome.value)
                self.assertEqual(INDEX_URL, before["index_url"])
                self.assertEqual(
                    baseline.accepted_at,
                    before["accepted_at"],
                )
                self.assertEqual(
                    ["0000000002"],
                    before["reporting_owner_ciks"],
                )

                events.clear()
                replay = self.process(
                    storage=storage,
                    state=state,
                    http=_ProcessorHTTP({}, events),
                    events=events,
                    candidate=changed_candidate,
                )
                after = state.read(f"quarantine/accessions/{ACCESSION}")

                self.assertEqual("quarantined", replay.outcome.value)
                self.assertEqual(expected_reason, replay.reason_code)
                self.assertIn("parse_index", events)
                self.assertNotEqual(before, after)
                self.assertEqual(INDEX_URL, after["index_url"])
                self.assertEqual(changed_candidate.accepted_at, after["accepted_at"])
                self.assertEqual(
                    sorted(
                        entry.entity_cik
                        for entry in changed_candidate.source_entries
                        if entry.entity_role == "reporting_owner"
                    ),
                    after["reporting_owner_ciks"],
                )

    def test_deadline_stops_after_durable_index_before_raw_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            events: list[str] = []
            storage, state = self.prepared_stores(Path(tmpdir), events)
            http = self.http(events)
            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
                deadline=insider_pipeline.CooperativeDeadline(
                    started_monotonic=0.0,
                    deadline_seconds=60,
                ),
                monotonic=lambda: 60.0 if "store_index_html" in events else 0.0,
            )

            self.assertEqual([INDEX_URL], http.calls)
            self.assertEqual(FIXTURE.read_bytes(), storage.read_index_html(ACCESSION))
            with self.assertRaises(InsiderStorageError):
                storage.read_raw(ACCESSION)
            self.assertEqual(
                [], state.read("incremental-v1")["completed_accessions"]
            )

        self.assertEqual("checkpointed", result.outcome.value)
        self.assertEqual("raw", result.stage)
        self.assertEqual("deadline", result.reason_code)

    def test_deadline_stops_after_durable_raw_before_parse_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            http = self.http(events)

            result = self.process(
                storage=storage,
                state=state,
                http=http,
                events=events,
                deadline=insider_pipeline.CooperativeDeadline(
                    started_monotonic=0.0,
                    deadline_seconds=60,
                ),
                monotonic=lambda: 60.0 if "store_raw" in events else 0.0,
            )

            self.assertEqual([INDEX_URL, DOCUMENT_URL], http.calls)
            self.assertEqual(RAW_FIXTURE.read_bytes(), storage.read_raw(ACCESSION))
            self.assertIn("store_raw", events)
            self.assertNotIn("parse_raw", events)
            self.assertNotIn("store_source_metadata", events)
            self.assertNotIn("store_normalized", events)
            self.assertEqual(
                [], state.read("incremental-v1")["completed_accessions"]
            )

        self.assertEqual("checkpointed", result.outcome.value)
        self.assertEqual("parse", result.stage)
        self.assertEqual("deadline", result.reason_code)

    def test_deadline_stops_after_source_metadata_before_normalized_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)

            result = self.process(
                storage=storage,
                state=state,
                http=self.http(events),
                events=events,
                deadline=insider_pipeline.CooperativeDeadline(
                    started_monotonic=0.0,
                    deadline_seconds=60,
                ),
                monotonic=lambda: (
                    60.0 if "store_source_metadata" in events else 0.0
                ),
            )

            source = storage.read_source_metadata(ACCESSION)
            self.assertEqual(ACCESSION, source["accession_number"])
            with self.assertRaises(InsiderStorageError):
                storage.read_normalized(ACCESSION, INSIDER_PARSER_VERSION)
            self.assertIn("store_source_metadata", events)
            self.assertNotIn("store_normalized", events)
            self.assertEqual(
                [], state.read("incremental-v1")["completed_accessions"]
            )

        self.assertEqual("checkpointed", result.outcome.value)
        self.assertEqual("normalized", result.stage)
        self.assertEqual("deadline", result.reason_code)

    def test_each_immutable_publication_is_atomically_gated_by_current_approval(
        self,
    ) -> None:
        for stage in ("index", "raw", "source", "normalized"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                events: list[str] = []
                storage, state = self.prepared_stores(root, events)
                index_html = FIXTURE.read_bytes()
                raw_xml = RAW_FIXTURE.read_bytes()
                if stage != "index":
                    storage.store_index_html(ACCESSION, index_html)
                if stage in {"source", "normalized"}:
                    storage.store_raw(ACCESSION, raw_xml)
                if stage == "normalized":
                    index_metadata = parse_insider_filing_index(
                        index_html,
                        index_url=INDEX_URL,
                        accession_number=ACCESSION,
                        issuer_cik="0000000001",
                        reporting_owner_ciks=("0000000002",),
                    )
                    storage.store_source_metadata(
                        ACCESSION,
                        build_insider_source_metadata(
                            index_metadata,
                            index_html,
                            raw_xml,
                        ),
                    )

                blocked = threading.Event()
                resume = threading.Event()
                if stage == "normalized":
                    storage = _PausingSourceReadStorage(
                        root,
                        events,
                        blocked,
                        resume,
                    )
                events.clear()
                outcomes: list[object] = []
                failures: list[BaseException] = []

                with ExitStack() as stack:
                    if stage in {"index", "raw"}:
                        blocked_url = INDEX_URL if stage == "index" else DOCUMENT_URL
                        real_reader = insider_pipeline.pipeline.read_bounded_sec_response

                        def blocking_reader(response, *args, **kwargs):
                            body = real_reader(response, *args, **kwargs)
                            if response.url == blocked_url:
                                blocked.set()
                                if not resume.wait(5):
                                    raise RuntimeError(
                                        "HTTP authorization race test timed out"
                                    )
                            return body

                        stack.enter_context(
                            patch.object(
                                insider_pipeline.pipeline,
                                "read_bounded_sec_response",
                                side_effect=blocking_reader,
                            )
                        )
                    elif stage == "source":
                        real_builder = insider_pipeline.build_insider_source_metadata

                        def blocking_builder(*args, **kwargs):
                            blocked.set()
                            if not resume.wait(5):
                                raise RuntimeError(
                                    "source authorization race test timed out"
                                )
                            return real_builder(*args, **kwargs)

                        stack.enter_context(
                            patch.object(
                                insider_pipeline,
                                "build_insider_source_metadata",
                                side_effect=blocking_builder,
                            )
                        )

                    def run() -> None:
                        try:
                            outcomes.append(
                                self.process(
                                    storage=storage,
                                    state=state,
                                    http=self.http(events),
                                    events=events,
                                )
                            )
                        except BaseException as error:
                            failures.append(error)

                    worker = threading.Thread(target=run)
                    worker.start()
                    self.assertTrue(blocked.wait(5))
                    state.update(
                        "approved-issuers-v1",
                        lambda current: {
                            **current,
                            "issuer_ciks": ["0000000009"],
                        },
                    )
                    resume.set()
                    worker.join(5)

                self.assertFalse(worker.is_alive())
                self.assertEqual([], outcomes)
                self.assertEqual(1, len(failures))
                self.assertIsInstance(failures[0], InsiderApprovalScopeError)
                verifier = InsiderStorage(root)
                with self.assertRaises(InsiderStorageError):
                    if stage == "index":
                        verifier.read_index_html(ACCESSION)
                    elif stage == "raw":
                        verifier.read_raw(ACCESSION)
                    elif stage == "source":
                        verifier.read_source_metadata(ACCESSION)
                    else:
                        verifier.read_normalized(ACCESSION, INSIDER_PARSER_VERSION)
                self.assertEqual(
                    [], state.read("incremental-v1")["completed_accessions"]
                )
                with self.assertRaises(FileNotFoundError):
                    state.read(f"quarantine/accessions/{ACCESSION}")

    def test_deterministic_quarantine_publication_rechecks_current_approval(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            _, state = self.prepared_stores(root, events)

            def revoke_then_reject_index(*args, **kwargs):
                state.update(
                    "approved-issuers-v1",
                    lambda current: {
                        **current,
                        "issuer_ciks": ["0000000009"],
                    },
                )
                raise InsiderIndexParseError("synthetic deterministic index rejection")

            with patch.object(
                insider_pipeline,
                "parse_insider_filing_index",
                side_effect=revoke_then_reject_index,
            ), self.assertRaises(InsiderApprovalScopeError):
                self.process(
                    storage=InsiderStorage(root),
                    state=state,
                    http=self.http(events),
                    events=events,
                )

            self.assertEqual(
                ["0000000009"], state.read("approved-issuers-v1")["issuer_ciks"]
            )
            self.assertEqual(
                [], state.read("incremental-v1")["completed_accessions"]
            )
            with self.assertRaises(FileNotFoundError):
                state.read(f"quarantine/accessions/{ACCESSION}")

    def test_checkpoint_atomically_rejects_issuer_revoked_after_artifact_verification(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = self.prepared_stores(root, events)
            verified = threading.Event()
            resume = threading.Event()
            outcomes: list[object] = []
            original_checkpoint = state.update_incremental_if_issuers_approved

            def pause_before_checkpoint(transform):
                verified.set()
                if not resume.wait(5):
                    raise RuntimeError("authorization race test timed out")
                return original_checkpoint(transform)

            def run() -> None:
                outcomes.append(
                    self.process(
                        storage=storage,
                        state=state,
                        http=self.http(events),
                        events=events,
                    )
                )

            with patch.object(
                state,
                "update_incremental_if_issuers_approved",
                side_effect=pause_before_checkpoint,
            ):
                worker = threading.Thread(target=run)
                worker.start()
                self.assertTrue(verified.wait(5))
                InsiderStateStore(root).update(
                    "approved-issuers-v1",
                    lambda current: {
                        **current,
                        "issuer_ciks": ["0000000009"],
                    },
                )
                resume.set()
                worker.join(5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(1, len(outcomes))
            result = outcomes[0]
            self.assertEqual("checkpointed", result.outcome.value)
            self.assertEqual("checkpoint_failed", result.reason_code)
            self.assertEqual(
                [], state.read("incremental-v1")["completed_accessions"]
            )
            issuer_state = state.read("issuers/0000000001")
            issuer_accessions = issuer_state["accessions"]
            assert isinstance(issuer_accessions, list)
            self.assertEqual(
                ACCESSION,
                issuer_accessions[0]["accession_number"],
            )


class InsiderNormalizedIssuerRecordTests(unittest.TestCase):
    @staticmethod
    def joint_normalized(*, accepted_at: str = "2026-02-11T17:45:12Z"):
        return insider_pipeline.parse_ownership_xml(
            JOINT_RAW_FIXTURE.read_bytes(),
            accession_number="0000000001-26-000002",
            filing_date="2026-02-11",
            accepted_at=accepted_at,
            source_index_url=(
                "https://www.sec.gov/Archives/edgar/data/1/"
                "000000000126000002/0000000001-26-000002-index.html"
            ),
            source_document_url=(
                "https://www.sec.gov/Archives/edgar/data/1/"
                "000000000126000002/form4-joint-test-only.xml"
            ),
        )

    def test_verified_joint_normalized_filing_projects_exact_reducer_evidence(
        self,
    ) -> None:
        normalized = self.joint_normalized(
            accepted_at="2026-02-11T17:45:12.123456Z"
        )

        record = insider_pipeline.issuer_record_from_normalized(
            normalized,
            parser_version=INSIDER_PARSER_VERSION,
        )

        self.assertEqual("0000000001-26-000002", record.accession_number)
        self.assertEqual("0000000001", record.issuer_cik)
        self.assertEqual(("0000000003", "0000000004"), record.owner_ciks)
        self.assertEqual(
            section16_owner_group_key(record.owner_ciks),
            record.owner_group_key,
        )
        self.assertEqual(
            hashlib.sha256(canonical_insider_json_bytes(normalized)).hexdigest(),
            record.normalized_sha256,
        )
        expected_signature = tuple(sorted(
            (
                row["source_table"],
                row["source_row_index"],
                row["security_class_key"],
                row["transaction_date"],
                row["transaction_code"],
            )
            for row in normalized["transactions"]
        ))
        self.assertEqual(expected_signature, record.transaction_signature)

        expected_classes: set[tuple[str, bool, str]] = set()
        for collection_name in ("transactions", "holdings"):
            for row in normalized[collection_name]:
                expected_classes.add((
                    row["security_class_key"],
                    row["source_table"] == "derivative",
                    row["security_title_as_filed"],
                ))
                if row["underlying_security_class_key"] is not None:
                    expected_classes.add((
                        row["underlying_security_class_key"],
                        False,
                        row["underlying_security_title"],
                    ))
        self.assertEqual(tuple(sorted(expected_classes)), record.security_classes)
        self.assertEqual("2026-02-11T17:45:12.123456Z", record.accepted_at)

    def test_normalized_projection_rejects_parser_path_mismatch(self) -> None:
        normalized = self.joint_normalized()

        with self.assertRaises(insider_pipeline.InsiderIssuerReductionError):
            insider_pipeline.issuer_record_from_normalized(
                normalized,
                parser_version="9.9.9",
            )

    def test_normalized_projection_is_bound_to_the_canonical_hashed_snapshot(
        self,
    ) -> None:
        normalized = self.joint_normalized()
        expected_bytes = canonical_insider_json_bytes(normalized)
        real_canonical = insider_pipeline.canonical_insider_json_bytes

        def render_then_mutate(payload: object) -> bytes:
            rendered = real_canonical(payload)
            assert isinstance(payload, dict)
            issuer = payload["issuer"]
            assert isinstance(issuer, dict)
            issuer["cik"] = "0000000009"
            return rendered

        with patch.object(
            insider_pipeline,
            "canonical_insider_json_bytes",
            side_effect=render_then_mutate,
        ):
            record = insider_pipeline.issuer_record_from_normalized(
                normalized,
                parser_version=INSIDER_PARSER_VERSION,
            )

        self.assertEqual("0000000001", record.issuer_cik)
        self.assertEqual(
            hashlib.sha256(expected_bytes).hexdigest(),
            record.normalized_sha256,
        )

    def test_verified_original_and_amendment_rebuild_persists_valid_issuer_state(
        self,
    ) -> None:
        original = insider_pipeline.parse_ownership_xml(
            RAW_FIXTURE.read_bytes(),
            accession_number=ACCESSION,
            filing_date="2026-01-16",
            accepted_at="2026-01-16T16:30:00Z",
            source_index_url=INDEX_URL,
            source_document_url=DOCUMENT_URL,
        )
        amendment = insider_pipeline.parse_ownership_xml(
            AMENDMENT_RAW_FIXTURE.read_bytes(),
            accession_number="0000000001-26-000005",
            filing_date="2026-01-20",
            accepted_at="2026-01-20T20:03:04Z",
            source_index_url=(
                "https://www.sec.gov/Archives/edgar/data/1/"
                "000000000126000005/0000000001-26-000005-index.html"
            ),
            source_document_url=(
                "https://www.sec.gov/Archives/edgar/data/1/"
                "000000000126000005/form4a-test-only.xml"
            ),
        )
        records = tuple(
            insider_pipeline.issuer_record_from_normalized(
                filing,
                parser_version=INSIDER_PARSER_VERSION,
            )
            for filing in (amendment, original)
        )
        reduction = insider_pipeline.reduce_issuer_state(
            issuer_cik="0000000001",
            records=records,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            stored = state.write_issuer_if_approved(
                "0000000001",
                reduction.issuer_state,
            )

            self.assertTrue(stored.created)
            self.assertEqual(
                reduction.issuer_state,
                state.read("issuers/0000000001"),
            )
            self.assertEqual(2, reduction.accession_count)
            self.assertEqual(1, reduction.amendments_resolved)
            amendments = reduction.issuer_state["amendments"]
            self.assertIsInstance(amendments, list)
            assert isinstance(amendments, list)
            self.assertIsInstance(amendments[0], dict)
            assert isinstance(amendments[0], dict)
            self.assertEqual(
                ACCESSION,
                amendments[0]["amends_accession"],
            )


class _InsiderIssuerRecordFixtures:
    issuer_cik = "0000000001"
    original_accession = "0000000001-26-000001"
    amendment_accession = "0000000001-26-000002"
    second_original_accession = "0000000001-26-000003"
    owner_ciks = ("0000000002", "0000000003")
    security_title = "Class A Common Stock"

    @classmethod
    def record(
        cls,
        *,
        accession_number: str | None = None,
        parser_version: str = INSIDER_PARSER_VERSION,
        normalized_sha256: str | None = None,
        is_amendment: bool = False,
        filing_date: str = "2026-01-16",
        accepted_at: str | None = "2026-01-16T12:00:00Z",
        original_submission_date: str | None = None,
        period_of_report: str | None = "2026-01-15",
        owner_ciks: tuple[str, ...] | None = None,
        transaction_signature: tuple[
            tuple[str, int, str, str | None, str | None], ...
        ]
        | None = None,
        security_classes: tuple[tuple[str, bool, str], ...] | None = None,
    ):
        accession = accession_number or cls.original_accession
        owners = owner_ciks or cls.owner_ciks
        class_key = section16_security_class_key(
            cls.issuer_cik,
            cls.security_title,
            is_derivative=False,
        )
        if transaction_signature is None:
            transaction_signature = (
                ("non_derivative", 0, class_key, "2026-01-15", "P"),
            )
        if security_classes is None:
            security_classes = ((class_key, False, cls.security_title),)
        digest = normalized_sha256 or hashlib.sha256(
            f"{accession}:{parser_version}".encode()
        ).hexdigest()
        return insider_pipeline.NormalizedIssuerRecord(
            accession_number=accession,
            parser_version=parser_version,
            normalized_sha256=digest,
            issuer_cik=cls.issuer_cik,
            base_form_type="4",
            is_amendment=is_amendment,
            filing_date=filing_date,
            accepted_at=accepted_at,
            original_submission_date=original_submission_date,
            period_of_report=period_of_report,
            owner_group_key=section16_owner_group_key(owners),
            owner_ciks=owners,
            transaction_signature=transaction_signature,
            security_classes=security_classes,
        )

    @classmethod
    def original(cls, **overrides: object):
        return cls.record(**overrides)

    @classmethod
    def amendment(cls, **overrides: object):
        values: dict[str, object] = {
            "accession_number": cls.amendment_accession,
            "is_amendment": True,
            "filing_date": "2026-01-20",
            "accepted_at": "2026-01-20T12:00:00Z",
            "original_submission_date": "2026-01-16",
        }
        values.update(overrides)
        return cls.record(**values)

    def reduce(self, *records):
        return insider_pipeline.reduce_issuer_state(
            issuer_cik=self.issuer_cik,
            records=records,
        )


class InsiderIssuerStateTests(_InsiderIssuerRecordFixtures, unittest.TestCase):
    def test_owner_group_key_is_order_independent_and_joint_filing_has_one_accession_reference(
        self,
    ) -> None:
        original = self.original()
        reordered = replace(
            original,
            owner_ciks=tuple(reversed(original.owner_ciks)),
        )

        reduction = self.reduce(reordered, original)

        self.assertEqual(1, reduction.accession_count)
        self.assertEqual(
            [
                {
                    "accession_number": original.accession_number,
                    "parser_version": original.parser_version,
                    "normalized_sha256": original.normalized_sha256,
                }
            ],
            reduction.issuer_state["accessions"],
        )
        self.assertEqual(
            [
                {
                    "owner_group_key": section16_owner_group_key(self.owner_ciks),
                    "owner_ciks": list(self.owner_ciks),
                }
            ],
            reduction.issuer_state["owner_groups"],
        )

    def test_security_inventory_preserves_derivative_dimension_and_distinct_as_filed_titles(
        self,
    ) -> None:
        values = (
            (
                section16_security_class_key(
                    self.issuer_cik,
                    "Class A Common Stock",
                    is_derivative=False,
                ),
                False,
                "Class A Common Stock",
            ),
            (
                section16_security_class_key(
                    self.issuer_cik,
                    "Class A Common Stock",
                    is_derivative=True,
                ),
                True,
                "Class A Common Stock",
            ),
            (
                section16_security_class_key(
                    self.issuer_cik,
                    "Class B Common Stock",
                    is_derivative=False,
                ),
                False,
                "Class B Common Stock",
            ),
        )

        reduction = self.reduce(self.original(security_classes=tuple(reversed(values))))

        self.assertEqual(3, reduction.security_class_count)
        expected = [
            {
                "security_class_key": key,
                "derivative": derivative,
                "title": title,
            }
            for key, derivative, title in sorted(values)
        ]
        self.assertEqual(expected, reduction.issuer_state["security_classes"])
        self.assertTrue(
            all("ticker" not in entry for entry in reduction.issuer_state["security_classes"])
        )

    def test_aggregate_security_inventory_fails_closed_at_state_limit(self) -> None:
        limit = insider_pipeline.MAX_INSIDER_STATE_COLLECTION
        classes = tuple(
            (
                section16_security_class_key(
                    self.issuer_cik,
                    title,
                    is_derivative=False,
                ),
                False,
                title,
            )
            for title in (
                f"Synthetic Security Class {index:04d}"
                for index in range(limit + 1)
            )
        )
        split = (limit // 2) + 1
        first = self.original(
            security_classes=classes[:split],
            transaction_signature=(),
        )
        second = self.original(
            accession_number=self.second_original_accession,
            security_classes=classes[split:],
            transaction_signature=(),
        )

        with self.assertRaises(insider_pipeline.InsiderIssuerReductionError):
            self.reduce(first, second)

    def test_reducer_rejects_a_structurally_bounded_but_oversized_issuer_state(
        self,
    ) -> None:
        records = tuple(
            self.original(
                accession_number=f"0000000001-26-{index + 1:06d}",
                owner_ciks=tuple(
                    f"{index * 1000 + owner + 1:010d}"
                    for owner in range(insider_pipeline.MAX_INSIDER_STATE_COLLECTION)
                ),
                transaction_signature=(),
                security_classes=(),
            )
            for index in range(90)
        )

        with self.assertRaises(insider_pipeline.InsiderIssuerReductionError):
            self.reduce(*records)

    def test_transaction_signature_security_classes_must_exist_in_inventory(
        self,
    ) -> None:
        original = self.original()
        foreign_class_key = "f" * 64
        self.assertNotIn(
            foreign_class_key,
            {key for key, _, _ in original.security_classes},
        )

        with self.assertRaises(insider_pipeline.InsiderIssuerReductionError):
            replace(
                original,
                transaction_signature=(
                    (
                        "non_derivative",
                        0,
                        foreign_class_key,
                        "2026-01-15",
                        "P",
                    ),
                ),
            )

    def test_transaction_signature_table_must_match_security_class_dimension(
        self,
    ) -> None:
        original = self.original()
        non_derivative_key = original.security_classes[0][0]

        with self.assertRaises(insider_pipeline.InsiderIssuerReductionError):
            replace(
                original,
                transaction_signature=(
                    (
                        "derivative",
                        0,
                        non_derivative_key,
                        "2026-01-15",
                        "P",
                    ),
                ),
            )

    def test_transaction_signature_rejects_conflicting_source_coordinates(
        self,
    ) -> None:
        original = self.original()
        first_key, _, first_title = original.security_classes[0]
        second_title = "Class B Common Stock"
        second_key = section16_security_class_key(
            self.issuer_cik,
            second_title,
            is_derivative=False,
        )

        with self.assertRaises(insider_pipeline.InsiderIssuerReductionError):
            replace(
                original,
                transaction_signature=(
                    ("non_derivative", 0, first_key, "2026-01-15", "P"),
                    ("non_derivative", 0, second_key, "2026-01-15", "S"),
                ),
                security_classes=(
                    (first_key, False, first_title),
                    (second_key, False, second_title),
                ),
            )

    def test_transaction_signature_rejects_non_string_source_table(self) -> None:
        original = self.original()
        class_key = original.security_classes[0][0]

        with self.assertRaises(insider_pipeline.InsiderIssuerReductionError):
            replace(
                original,
                transaction_signature=(
                    ([], 0, class_key, "2026-01-15", "P"),
                ),
            )

    def test_record_rejects_coercion_objects_without_invoking_them(self) -> None:
        class HostileText:
            def __str__(self) -> str:
                raise RuntimeError("TASK6_COERCION_SECRET")

        original = self.original()
        class_key = original.security_classes[0][0]
        cases = (
            {"issuer_cik": HostileText()},
            {"owner_ciks": (HostileText(),)},
            {
                "security_classes": (
                    (class_key, False, HostileText()),
                )
            },
        )

        for changes in cases:
            with self.subTest(field=next(iter(changes))):
                with self.assertRaises(
                    insider_pipeline.InsiderIssuerReductionError
                ) as caught:
                    replace(original, **changes)
                self.assertNotIn("TASK6_COERCION_SECRET", str(caught.exception))
                self.assertIsNone(caught.exception.__cause__)
                self.assertIsNone(caught.exception.__context__)

    def test_replaying_identical_accession_reference_is_byte_stable_and_does_not_duplicate_inventory(
        self,
    ) -> None:
        original = self.original()
        amendment = self.amendment()

        first = self.reduce(original, amendment)
        replay = self.reduce(amendment, original, amendment, original)

        self.assertEqual(
            insider_pipeline.canonical_insider_state_json_bytes(first.issuer_state),
            insider_pipeline.canonical_insider_state_json_bytes(replay.issuer_state),
        )
        self.assertEqual(first.issuer_state["generation_digest"], replay.issuer_state["generation_digest"])
        self.assertEqual(2, replay.accession_count)
        self.assertEqual(1, replay.owner_group_count)
        self.assertEqual(1, replay.security_class_count)

    def test_duplicate_accession_parser_reference_with_different_sha_fails_closed(
        self,
    ) -> None:
        original = self.original()
        conflicting = replace(original, normalized_sha256="f" * 64)

        with self.assertRaises(insider_pipeline.InsiderIssuerReductionError):
            self.reduce(original, conflicting)

    def test_reducer_rejects_record_subclasses(self) -> None:
        class DerivedRecord(insider_pipeline.NormalizedIssuerRecord):
            pass

        original = self.original()
        derived = DerivedRecord(**{
            field.name: getattr(original, field.name)
            for field in dataclass_fields(original)
        })

        with self.assertRaises(insider_pipeline.InsiderIssuerReductionError):
            self.reduce(derived)

    def test_reducer_normalizes_hostile_iterator_failures_without_leaking_text(
        self,
    ) -> None:
        class OpaqueIterationError(BaseException):
            pass

        class HostileRecords:
            def __init__(self, error: BaseException) -> None:
                self.error = error

            def __iter__(self):
                return self

            def __next__(self):
                raise self.error

        for error in (
            RuntimeError("TASK6_ITERATOR_SECRET"),
            OpaqueIterationError("TASK6_ITERATOR_SECRET"),
        ):
            with self.subTest(error_type=type(error).__name__):
                with self.assertRaises(
                    insider_pipeline.InsiderIssuerReductionError
                ) as caught:
                    insider_pipeline.reduce_issuer_state(
                        issuer_cik=self.issuer_cik,
                        records=HostileRecords(error),
                    )
                self.assertNotIn("TASK6_ITERATOR_SECRET", str(caught.exception))
                self.assertIsNone(caught.exception.__cause__)
                self.assertIsNone(caught.exception.__context__)

    def test_reducer_rejects_coercion_issuer_without_invoking_it(self) -> None:
        class HostileIssuer:
            def __str__(self) -> str:
                raise RuntimeError("TASK6_ISSUER_SECRET")

        with self.assertRaises(
            insider_pipeline.InsiderIssuerReductionError
        ) as caught:
            insider_pipeline.reduce_issuer_state(
                issuer_cik=cast(str, HostileIssuer()),
                records=(),
            )

        self.assertNotIn("TASK6_ISSUER_SECRET", str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)
        self.assertIsNone(caught.exception.__context__)

    def test_reducer_preserves_iterator_cancellation(self) -> None:
        class CancelledRecords:
            def __iter__(self):
                return self

            def __next__(self):
                raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            insider_pipeline.reduce_issuer_state(
                issuer_cik=self.issuer_cik,
                records=CancelledRecords(),
            )

    def test_new_parser_version_changes_generation_and_selected_reference(self) -> None:
        original = self.original()
        reparsed = replace(
            original,
            parser_version="1.1.0",
            normalized_sha256="e" * 64,
        )

        first = self.reduce(original)
        second = self.reduce(reparsed)

        self.assertNotEqual(
            first.issuer_state["generation_digest"],
            second.issuer_state["generation_digest"],
        )
        self.assertEqual("1.0.0", first.issuer_state["accessions"][0]["parser_version"])
        self.assertEqual("1.1.0", second.issuer_state["accessions"][0]["parser_version"])


class InsiderAmendmentResolutionTests(
    _InsiderIssuerRecordFixtures,
    unittest.TestCase,
):
    def amendment_entry(self, *records):
        reduction = self.reduce(*records)
        self.assertEqual(1, len(reduction.issuer_state["amendments"]))
        return reduction, reduction.issuer_state["amendments"][0]

    def test_unique_original_submission_date_match_resolves_high_confidence(self) -> None:
        original = self.original()
        reduction, amendment = self.amendment_entry(self.amendment(), original)

        self.assertEqual(
            {
                "accession_number": self.amendment_accession,
                "amends_accession": self.original_accession,
                "confidence": "high",
                "reason_code": "single_candidate",
                "candidates": [self.original_accession],
            },
            amendment,
        )
        self.assertEqual([], reduction.issuer_state["unresolved_ambiguities"])

    def test_future_dated_original_is_not_a_candidate_without_acceptance_times(
        self,
    ) -> None:
        original = self.original(
            filing_date="2026-02-01",
            accepted_at=None,
        )
        amendment_record = self.amendment(
            filing_date="2026-01-20",
            accepted_at=None,
            original_submission_date="2026-02-01",
        )

        reduction, amendment = self.amendment_entry(
            amendment_record,
            original,
        )

        self.assertEqual(
            {
                "accession_number": self.amendment_accession,
                "amends_accession": None,
                "confidence": "unresolved",
                "reason_code": "no_candidate",
                "candidates": [],
            },
            amendment,
        )
        self.assertEqual(
            [
                {
                    "accession_number": self.amendment_accession,
                    "reason_code": "no_candidate",
                    "candidates": [],
                }
            ],
            reduction.issuer_state["unresolved_ambiguities"],
        )

    def test_period_of_report_is_only_a_conservative_second_stage_tiebreaker(
        self,
    ) -> None:
        matching = self.original()
        other = self.original(
            accession_number=self.second_original_accession,
            period_of_report="2026-01-14",
        )

        _, amendment = self.amendment_entry(other, self.amendment(), matching)

        self.assertEqual("medium", amendment["confidence"])
        self.assertEqual(self.original_accession, amendment["amends_accession"])
        self.assertEqual([self.original_accession], amendment["candidates"])

    def test_transaction_coordinate_signature_breaks_remaining_period_tie_only_on_exact_match(
        self,
    ) -> None:
        matching = self.original()
        derivative_class_key = section16_security_class_key(
            self.issuer_cik,
            self.security_title,
            is_derivative=True,
        )
        other = self.original(
            accession_number=self.second_original_accession,
            transaction_signature=(
                ("derivative", 1, derivative_class_key, "2026-01-15", "S"),
            ),
            security_classes=(
                (derivative_class_key, True, self.security_title),
            ),
        )

        _, amendment = self.amendment_entry(other, matching, self.amendment())

        self.assertEqual("low", amendment["confidence"])
        self.assertEqual(self.original_accession, amendment["amends_accession"])
        self.assertEqual([self.original_accession], amendment["candidates"])

    def test_empty_transaction_signature_is_not_tiebreak_evidence(self) -> None:
        empty = self.original(transaction_signature=())
        nonempty = self.original(
            accession_number=self.second_original_accession,
        )
        amendment_record = self.amendment(transaction_signature=())

        reduction, amendment = self.amendment_entry(
            amendment_record,
            nonempty,
            empty,
        )

        candidates = [self.original_accession, self.second_original_accession]
        self.assertEqual("unresolved", amendment["confidence"])
        self.assertEqual("ambiguous_candidates", amendment["reason_code"])
        self.assertEqual(candidates, amendment["candidates"])
        self.assertEqual(1, reduction.amendments_unresolved)

    def test_multiple_candidates_after_all_tiebreakers_remain_unresolved_and_do_not_suppress_originals(
        self,
    ) -> None:
        first = self.original()
        second = self.original(accession_number=self.second_original_accession)

        reduction, amendment = self.amendment_entry(first, self.amendment(), second)

        candidates = [self.original_accession, self.second_original_accession]
        self.assertEqual(
            {
                "accession_number": self.amendment_accession,
                "amends_accession": None,
                "confidence": "unresolved",
                "reason_code": "ambiguous_candidates",
                "candidates": candidates,
            },
            amendment,
        )
        self.assertEqual(
            [
                {
                    "accession_number": self.amendment_accession,
                    "reason_code": "ambiguous_candidates",
                    "candidates": candidates,
                }
            ],
            reduction.issuer_state["unresolved_ambiguities"],
        )
        self.assertEqual(
            [
                self.original_accession,
                self.amendment_accession,
                self.second_original_accession,
            ],
            sorted(
                entry["accession_number"]
                for entry in reduction.issuer_state["accessions"]
            ),
        )

    def test_nonmatching_optional_tiebreakers_preserve_hard_candidates(self) -> None:
        first = self.original(period_of_report="2026-01-13")
        derivative_class_key = section16_security_class_key(
            self.issuer_cik,
            self.security_title,
            is_derivative=True,
        )
        second = self.original(
            accession_number=self.second_original_accession,
            period_of_report="2026-01-14",
            transaction_signature=(
                (
                    "derivative",
                    1,
                    derivative_class_key,
                    "2026-01-14",
                    "S",
                ),
            ),
            security_classes=(
                (derivative_class_key, True, self.security_title),
            ),
        )
        amendment = self.amendment(
            period_of_report="2026-01-15",
            transaction_signature=(
                (
                    "non_derivative",
                    2,
                    first.transaction_signature[0][2],
                    "2026-01-15",
                    "A",
                ),
            ),
        )

        reduction, resolved = self.amendment_entry(first, amendment, second)

        candidates = [self.original_accession, self.second_original_accession]
        self.assertEqual("unresolved", resolved["confidence"])
        self.assertEqual("ambiguous_candidates", resolved["reason_code"])
        self.assertEqual(candidates, resolved["candidates"])
        self.assertEqual(
            [
                {
                    "accession_number": self.amendment_accession,
                    "reason_code": "ambiguous_candidates",
                    "candidates": candidates,
                }
            ],
            reduction.issuer_state["unresolved_ambiguities"],
        )

    def test_amendment_before_original_is_unresolved_then_resolves_when_original_arrives(
        self,
    ) -> None:
        amendment_record = self.amendment()
        original_amendment_record = amendment_record

        first, unresolved = self.amendment_entry(amendment_record)
        second, resolved = self.amendment_entry(amendment_record, self.original())

        self.assertEqual("unresolved", unresolved["confidence"])
        self.assertEqual("no_candidate", unresolved["reason_code"])
        self.assertEqual([], unresolved["candidates"])
        self.assertEqual(1, first.amendments_unresolved)
        self.assertEqual("high", resolved["confidence"])
        self.assertEqual(self.original_accession, resolved["amends_accession"])
        self.assertEqual(1, second.amendments_resolved)
        self.assertIs(original_amendment_record, amendment_record)


class InsiderIncrementalScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.environment = patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    @staticmethod
    def fixture_http() -> _AtomHTTP:
        names = InsiderIncrementalDiscoveryTests.fixture_names

        def respond(url: str) -> _AtomResponse:
            query = parse_qs(urlsplit(url).query)
            body = (
                (FIXTURE.parent / names[query["type"][0]]).read_bytes()
                if query["start"] == ["0"]
                else InsiderIncrementalDiscoveryTests.empty_feed()
            )
            return _AtomResponse(body, url=url)

        return _AtomHTTP(respond)

    @staticmethod
    def valid_arguments() -> list[str]:
        return [
            "--issuer-cik", "1",
            "--lookback-seconds", "86400",
            "--max-pages", "2",
            "--page-size", "40",
            "--max-accessions", "10",
            "--deadline-seconds", "60",
        ]

    def test_script_uses_lock_ua_shared_http_and_persists_the_queue(self) -> None:
        self.assertTrue(hasattr(refresh_script.main, "__wrapped__"))
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            http = self.fixture_http()

            discovery_deadlines: list[object] = []

            def discover_at_fixture_time(**arguments):
                discovery_deadlines.append(arguments.get("deadline_monotonic"))
                return discover_recent_insider_accessions(
                    now=datetime(2026, 1, 17, tzinfo=timezone.utc),
                    **arguments,
                )

            processed: list[str] = []
            deadlines: list[object] = []

            def process_after_publication(candidate, **arguments):
                durable = state.read("incremental-v1")
                queue = durable["queue"]
                assert isinstance(queue, list)
                self.assertIn(
                    candidate.accession_number,
                    [entry["accession_number"] for entry in queue],
                )
                self.assertIs(http, arguments["http"])
                self.assertIsInstance(arguments["storage"], InsiderStorage)
                self.assertIs(state.__class__, arguments["state_store"].__class__)
                processed.append(candidate.accession_number)
                deadlines.append(arguments["deadline"])
                return insider_pipeline.InsiderAccessionProcessResult(
                    accession_number=candidate.accession_number,
                    issuer_cik=candidate.issuer_cik,
                    form_type=candidate.form_type,
                    parser_version=INSIDER_PARSER_VERSION,
                    outcome=insider_pipeline.InsiderAccessionOutcome.CACHE_HIT,
                    stage="cache",
                )

            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(refresh_script.pipeline, "HTTP", http), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                return_value="Synthetic Agent test@example.com",
            ) as require_ua, patch.object(
                refresh_script,
                "discover_recent_insider_accessions",
                side_effect=discover_at_fixture_time,
            ), patch.object(
                refresh_script,
                "process_insider_accession",
                side_effect=process_after_publication,
            ):
                self.assertEqual(75, refresh_script.main(self.valid_arguments()))

            require_ua.assert_called_once_with()
            self.assertEqual(12, len(http.calls))
            self.assertTrue(all(response.closed for response in http.responses))
            persisted = state.read("incremental-v1")
            self.assertEqual("incomplete", persisted["status"])
            queue = persisted["queue"]
            assert isinstance(queue, list)
            self.assertEqual(6, len(queue))
            self.assertEqual(
                [entry["accession_number"] for entry in queue],
                processed,
            )
            self.assertEqual(1, len({id(deadline) for deadline in deadlines}))
            shared_deadline = cast(insider_pipeline.CooperativeDeadline, deadlines[0])
            self.assertEqual(
                [shared_deadline.deadline_monotonic],
                discovery_deadlines,
            )
            self.assertTrue((root / ".pipeline-maintenance.lock").is_file())
            telemetry = state.read("telemetry-v1")
            telemetry_counters = telemetry["counters"]
            recent_runs = telemetry["recent_runs"]
            assert isinstance(telemetry_counters, dict)
            assert isinstance(recent_runs, list)
            self.assertEqual(1, telemetry_counters["discovery_attempts"])
            self.assertEqual(6, telemetry_counters["discovered_accession_groups"])
            run = recent_runs[0]
            assert isinstance(run, dict)
            self.assertEqual("completed", run["status"])
            run_id = run["run_id"]
            assert isinstance(run_id, str)
            self.assertTrue(run_id.startswith("incremental-"))

    def test_script_rejects_cross_issuer_pending_queue_before_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {
                    "contract_version": 1,
                    "issuer_ciks": ["0000000001", "0000000002"],
                },
            )
            persist_incremental_discovery_queue(
                state,
                result=InsiderIncrementalDiscoveryTests().grouped("4"),
                lookback_seconds=86_400,
            )
            arguments = [
                "--issuer-cik", "2",
                "--lookback-seconds", "86400",
                "--max-pages", "2",
                "--page-size", "40",
                "--max-accessions", "10",
                "--deadline-seconds", "60",
            ]

            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                return_value="Synthetic Agent test@example.com",
            ) as require_ua, patch.object(
                refresh_script,
                "discover_recent_insider_accessions",
            ) as discover, patch.object(
                refresh_script,
                "process_insider_accession",
            ) as process:
                self.assertEqual(2, refresh_script.main(arguments))

            require_ua.assert_not_called()
            discover.assert_not_called()
            process.assert_not_called()
            persisted = state.read("incremental-v1")
            persisted_queue = persisted["queue"]
            assert isinstance(persisted_queue, list)
            self.assertEqual(
                {"0000000001"},
                {entry["issuer_cik"] for entry in persisted_queue},
            )

    def test_script_resumes_matching_pending_queue_without_rediscovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            persist_incremental_discovery_queue(
                state,
                result=InsiderIncrementalDiscoveryTests().grouped("4"),
                lookback_seconds=86_400,
            )

            def cache_hit(candidate, **_arguments):
                return insider_pipeline.InsiderAccessionProcessResult(
                    accession_number=candidate.accession_number,
                    issuer_cik=candidate.issuer_cik,
                    form_type=candidate.form_type,
                    parser_version=INSIDER_PARSER_VERSION,
                    outcome=insider_pipeline.InsiderAccessionOutcome.CACHE_HIT,
                    stage="cache",
                )

            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                return_value="Synthetic Agent test@example.com",
            ) as require_ua, patch.object(
                refresh_script,
                "discover_recent_insider_accessions",
            ) as discover, patch.object(
                refresh_script,
                "process_insider_accession",
                side_effect=cache_hit,
            ) as process:
                self.assertEqual(75, refresh_script.main(self.valid_arguments()))

            require_ua.assert_called_once_with()
            discover.assert_not_called()
            self.assertEqual(1, process.call_count)

    def test_script_preserves_verified_completed_rediscovery_without_processing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            discovery = InsiderAccessionProcessorTests.discovery_result()
            first_http = InsiderAccessionProcessorTests.http()
            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(
                refresh_script.pipeline, "HTTP", first_http
            ), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                return_value="Synthetic Agent test@example.com",
            ), patch.object(
                refresh_script,
                "discover_recent_insider_accessions",
                return_value=discovery,
            ):
                self.assertEqual(0, refresh_script.main(self.valid_arguments()))
            self.assertEqual([INDEX_URL, DOCUMENT_URL], first_http.calls)
            storage = InsiderStorage(root)
            self.assertEqual(FIXTURE.read_bytes(), storage.read_index_html(ACCESSION))
            self.assertEqual(RAW_FIXTURE.read_bytes(), storage.read_raw(ACCESSION))
            before = state.read("incremental-v1")
            completed_before_processing: list[list[str]] = []
            processor_outcomes: list[str] = []
            http = _ProcessorHTTP({})

            def process_after_rediscovery(candidate, **arguments):
                durable = state.read("incremental-v1")
                completed = durable["completed_accessions"]
                assert isinstance(completed, list)
                completed_before_processing.append(list(completed))
                processed = insider_pipeline.process_insider_accession(
                    candidate,
                    **arguments,
                )
                processor_outcomes.append(processed.outcome.value)
                return processed

            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(refresh_script.pipeline, "HTTP", http), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                return_value="Synthetic Agent test@example.com",
            ), patch.object(
                refresh_script,
                "discover_recent_insider_accessions",
                return_value=discovery,
            ), patch.object(
                refresh_script,
                "process_insider_accession",
                side_effect=process_after_rediscovery,
            ):
                self.assertEqual(0, refresh_script.main(self.valid_arguments()))

            self.assertEqual([], completed_before_processing)
            self.assertEqual([], processor_outcomes)
            self.assertEqual([], http.calls)
            self.assertEqual(before, state.read("incremental-v1"))

    def test_script_returns_cooperative_status_for_retryable_checkpoint(self) -> None:
        discovery = InsiderAccessionProcessorTests.discovery_result()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )

            def retry_later(candidate, **_arguments):
                return insider_pipeline.InsiderAccessionProcessResult(
                    accession_number=candidate.accession_number,
                    issuer_cik=candidate.issuer_cik,
                    form_type=candidate.form_type,
                    parser_version=INSIDER_PARSER_VERSION,
                    outcome=insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
                    stage="index",
                    error_class="OSError",
                    reason_code="fetch_failed",
                )

            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                return_value="Synthetic Agent test@example.com",
            ), patch.object(
                refresh_script,
                "discover_recent_insider_accessions",
                return_value=discovery,
            ), patch.object(
                refresh_script,
                "process_insider_accession",
                side_effect=retry_later,
            ):
                self.assertEqual(75, refresh_script.main(self.valid_arguments()))

            checkpoint = state.read("incremental-v1")
            self.assertEqual("incomplete", checkpoint["status"])
            self.assertEqual([], checkpoint["completed_accessions"])

    def test_script_stops_before_next_accession_after_deadline_checkpoint(self) -> None:
        first = InsiderAccessionProcessorTests.candidate()
        second_accession = "0000000001-26-000002"
        second_index_url = (
            "https://www.sec.gov/Archives/edgar/data/1/000000000126000002/"
            f"{second_accession}-index.html"
        )
        second_owner_url = (
            "https://www.sec.gov/Archives/edgar/data/2/000000000126000002/"
            "owner-entry.html"
        )
        second = replace(
            first,
            accession_number=second_accession,
            index_url=second_index_url,
            source_entries=(
                replace(
                    first.source_entries[0],
                    accession_number=second_accession,
                    entry_url=second_index_url,
                ),
                replace(
                    first.source_entries[1],
                    accession_number=second_accession,
                    entry_url=second_owner_url,
                ),
            ),
        )
        discovery = insider_pipeline.IncrementalDiscoveryResult(
            accessions=(first, second),
            quarantined_accessions=(),
            pages_fetched=1,
            deadline_reached=False,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            processed: list[str] = []
            http = _ProcessorHTTP({})

            def process_until_deadline(candidate, **arguments):
                processed.append(candidate.accession_number)
                self.assertIs(http, arguments["http"])
                return insider_pipeline.InsiderAccessionProcessResult(
                    accession_number=candidate.accession_number,
                    issuer_cik=candidate.issuer_cik,
                    form_type=candidate.form_type,
                    parser_version=INSIDER_PARSER_VERSION,
                    outcome=insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
                    stage="index",
                    reason_code="deadline",
                )

            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(refresh_script.pipeline, "HTTP", http), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                return_value="Synthetic Agent test@example.com",
            ), patch.object(
                refresh_script,
                "discover_recent_insider_accessions",
                return_value=discovery,
            ), patch.object(
                refresh_script,
                "process_insider_accession",
                side_effect=process_until_deadline,
            ):
                self.assertEqual(75, refresh_script.main(self.valid_arguments()))

            self.assertEqual([ACCESSION], processed)
            self.assertEqual([], http.calls)
            persisted = state.read("incremental-v1")
            self.assertEqual("incomplete", persisted["status"])
            self.assertEqual([], persisted["completed_accessions"])

    def test_script_does_not_start_another_accession_after_deadline_expires(
        self,
    ) -> None:
        first = InsiderAccessionProcessorTests.candidate()
        second_accession = "0000000001-26-000002"
        second_index_url = (
            "https://www.sec.gov/Archives/edgar/data/1/000000000126000002/"
            f"{second_accession}-index.html"
        )
        second_owner_url = (
            "https://www.sec.gov/Archives/edgar/data/2/000000000126000002/"
            "owner-entry.html"
        )
        second = replace(
            first,
            accession_number=second_accession,
            index_url=second_index_url,
            source_entries=(
                replace(
                    first.source_entries[0],
                    accession_number=second_accession,
                    entry_url=second_index_url,
                ),
                replace(
                    first.source_entries[1],
                    accession_number=second_accession,
                    entry_url=second_owner_url,
                ),
            ),
        )
        discovery = insider_pipeline.IncrementalDiscoveryResult(
            accessions=(first, second),
            quarantined_accessions=(),
            pages_fetched=1,
            deadline_reached=False,
        )

        class BetweenAccessionsDeadline:
            def __init__(self) -> None:
                self.checks = 0
                self.deadline_monotonic = 60.0

            def reached(self, monotonic) -> bool:
                self.checks += 1
                return self.checks >= 2

        deadline = BetweenAccessionsDeadline()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            processed: list[str] = []
            http = _ProcessorHTTP({})

            def process_created(candidate, **arguments):
                processed.append(candidate.accession_number)
                self.assertIs(deadline, arguments["deadline"])
                return insider_pipeline.InsiderAccessionProcessResult(
                    accession_number=candidate.accession_number,
                    issuer_cik=candidate.issuer_cik,
                    form_type=candidate.form_type,
                    parser_version=INSIDER_PARSER_VERSION,
                    outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
                    stage="checkpoint",
                )

            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(refresh_script.pipeline, "HTTP", http), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                return_value="Synthetic Agent test@example.com",
            ), patch.object(
                refresh_script,
                "CooperativeDeadline",
                return_value=deadline,
            ), patch.object(
                refresh_script,
                "discover_recent_insider_accessions",
                return_value=discovery,
            ), patch.object(
                refresh_script,
                "process_insider_accession",
                side_effect=process_created,
            ):
                self.assertEqual(75, refresh_script.main(self.valid_arguments()))

            self.assertEqual([ACCESSION], processed)
            self.assertEqual(2, deadline.checks)
            self.assertEqual([], http.calls)
            persisted = state.read("incremental-v1")
            self.assertEqual("incomplete", persisted["status"])
            self.assertEqual([], persisted["completed_accessions"])
            queue = persisted["queue"]
            assert isinstance(queue, list)
            self.assertEqual(
                [ACCESSION, second_accession],
                [entry["accession_number"] for entry in queue],
            )

    def test_script_logs_only_bounded_counts_for_processor_failures(self) -> None:
        discovery = InsiderAccessionProcessorTests.discovery_result()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            http = _ProcessorHTTP({})

            def quarantine(candidate, **arguments):
                self.assertIs(http, arguments["http"])
                return insider_pipeline.InsiderAccessionProcessResult(
                    accession_number=candidate.accession_number,
                    issuer_cik=candidate.issuer_cik,
                    form_type=candidate.form_type,
                    parser_version=INSIDER_PARSER_VERSION,
                    outcome=insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
                    stage="raw",
                    error_class="InsiderParseError",
                    reason_code="raw_parse_invalid",
                )

            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(refresh_script.pipeline, "HTTP", http), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                return_value="Synthetic Agent test@example.com",
            ), patch.object(
                refresh_script,
                "discover_recent_insider_accessions",
                return_value=discovery,
            ), patch.object(
                refresh_script,
                "process_insider_accession",
                side_effect=quarantine,
            ), patch.object(
                refresh_script.pipeline.log,
                "error",
            ) as error_log, patch.object(
                refresh_script.pipeline.log,
                "info",
            ) as info_log:
                self.assertEqual(1, refresh_script.main(self.valid_arguments()))

            rendered_logs = repr(error_log.call_args_list + info_log.call_args_list)
            for sensitive in (
                ACCESSION,
                OWNER_ENTRY_URL,
                "InsiderParseError",
                "raw_parse_invalid",
            ):
                self.assertNotIn(sensitive, rendered_logs)
            error_log.assert_called_once_with(
                "recent insider processing completed with %s failed accession(s)",
                1,
            )
            self.assertEqual([], http.calls)

    def test_cli_env_allowlist_and_placeholder_ua_fail_before_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            cases = (
                [*self.valid_arguments(), "--lookback-seconds", "0"],
                [*self.valid_arguments(), "--max-pages", "0"],
                [*self.valid_arguments(), "--page-size", "101"],
                [*self.valid_arguments(), "--max-accessions", "1001"],
                [*self.valid_arguments(), "--deadline-seconds", "0"],
                [
                    "--issuer-cik", "2",
                    *self.valid_arguments()[2:],
                ],
                self.valid_arguments()[2:],
            )
            for arguments in cases:
                http = _AtomHTTP(
                    lambda url: (_ for _ in ()).throw(
                        AssertionError(f"unexpected HTTP request: {url}")
                    )
                )
                with self.subTest(arguments=arguments), patch.object(
                    refresh_script, "ROOT", root
                ), patch.object(
                    refresh_script.pipeline, "DATA_DIR", root / "data"
                ), patch.object(refresh_script.pipeline, "HTTP", http), patch.object(
                    refresh_script.pipeline,
                    "require_declared_sec_user_agent",
                    return_value="Synthetic Agent test@example.com",
                ):
                    self.assertEqual(2, refresh_script.main(arguments))
                self.assertEqual([], http.calls)

            http = self.fixture_http()
            with patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(refresh_script.pipeline, "HTTP", http), patch.object(
                refresh_script.pipeline,
                "require_declared_sec_user_agent",
                side_effect=ValueError("placeholder"),
            ):
                self.assertEqual(2, refresh_script.main(self.valid_arguments()))
            self.assertEqual([], http.calls)

            with patch.dict(
                os.environ,
                {
                    "RECENT_INSIDER_ISSUER_CIKS": "1",
                    "RECENT_INSIDER_MAX_PAGES": "not-an-integer",
                },
                clear=True,
            ), patch.object(refresh_script, "ROOT", root), patch.object(
                refresh_script.pipeline, "DATA_DIR", root / "data"
            ), patch.object(refresh_script.pipeline, "HTTP", http):
                self.assertEqual(2, refresh_script.main([]))
            self.assertEqual([], http.calls)


class InsiderBackfillProcessorBridgeTests(unittest.TestCase):
    def test_ambiguous_bulk_index_returns_deterministic_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage = InsiderStorage(root)
            state_store = InsiderStateStore(root)
            state_store.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            evidence = insider_pipeline.InsiderBulkAccessionEvidence(
                accession_number=ACCESSION,
                issuer_cik="0000000001",
                form_type="4",
                filing_date="2026-01-16",
                reporting_owner_ciks=(),
                table_row_counts=(("SUBMISSION", 1),),
            )
            http = _ProcessorHTTP({INDEX_URL: FIXTURE.read_bytes()})

            with (
                patch.object(
                    insider_pipeline,
                    "parse_insider_filing_index",
                    side_effect=insider_pipeline.InsiderIndexParseError(
                        "synthetic ambiguous filing index"
                    ),
                ),
                patch.object(
                    insider_pipeline,
                    "_process_insider_accession_identity",
                ) as shared,
            ):
                result = insider_pipeline.process_insider_backfill_accession(
                    evidence,
                    storage=storage,
                    state_store=state_store,
                    approved_issuer_ciks=("1",),
                    deadline=insider_pipeline.CooperativeDeadline(
                        started_monotonic=0.0,
                        deadline_seconds=60,
                    ),
                    http=http,
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(
                insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
                result.outcome,
            )
            self.assertEqual("index", result.stage)
            self.assertEqual("InsiderIndexParseError", result.error_class)
            self.assertEqual("index_parse_invalid", result.reason_code)
            shared.assert_not_called()

    def test_corrupt_cached_bulk_index_returns_quarantine_before_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage = InsiderStorage(root)
            state_store = InsiderStateStore(root)
            state_store.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            evidence = insider_pipeline.InsiderBulkAccessionEvidence(
                accession_number=ACCESSION,
                issuer_cik="0000000001",
                form_type="4",
                filing_date="2026-01-16",
                reporting_owner_ciks=(),
                table_row_counts=(("SUBMISSION", 1),),
            )
            http = _ProcessorHTTP({INDEX_URL: FIXTURE.read_bytes()})

            with patch.object(
                storage,
                "read_index_html",
                side_effect=InsiderStorageError("synthetic corrupt cached index"),
            ):
                result = insider_pipeline.process_insider_backfill_accession(
                    evidence,
                    storage=storage,
                    state_store=state_store,
                    approved_issuer_ciks=("1",),
                    deadline=insider_pipeline.CooperativeDeadline(
                        started_monotonic=0.0,
                        deadline_seconds=60,
                    ),
                    http=http,
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(
                insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
                result.outcome,
            )
            self.assertEqual("cache", result.stage)
            self.assertEqual("InsiderStorageError", result.error_class)
            self.assertEqual("cache_invalid", result.reason_code)
            self.assertEqual([], http.calls)

    def test_bulk_evidence_uses_index_identity_and_shared_processor_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage = InsiderStorage(root)
            state_store = InsiderStateStore(root)
            state_store.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            evidence = insider_pipeline.InsiderBulkAccessionEvidence(
                accession_number=ACCESSION,
                issuer_cik="0000000001",
                form_type="4",
                filing_date="2026-01-16",
                reporting_owner_ciks=(),
                table_row_counts=(("SUBMISSION", 1),),
            )
            index_html = FIXTURE.read_bytes()
            http = _ProcessorHTTP({INDEX_URL: index_html})
            captured: dict[str, object] = {}

            def shared_processor(identity, **kwargs):
                captured["identity"] = identity
                captured.update(kwargs)
                return insider_pipeline.InsiderAccessionProcessResult(
                    accession_number=ACCESSION,
                    issuer_cik="0000000001",
                    form_type="4",
                    parser_version=INSIDER_PARSER_VERSION,
                    outcome=insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
                    stage="raw",
                    error_class="ConnectionError",
                    reason_code="fetch_failed",
                )

            with patch.object(
                insider_pipeline,
                "_process_insider_accession_identity",
                side_effect=shared_processor,
                create=True,
            ) as shared:
                result = insider_pipeline.process_insider_backfill_accession(
                    evidence,
                    storage=storage,
                    state_store=state_store,
                    approved_issuer_ciks=("1",),
                    deadline=insider_pipeline.CooperativeDeadline(
                        started_monotonic=0.0,
                        deadline_seconds=60,
                    ),
                    http=http,
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(insider_pipeline.InsiderAccessionOutcome.RETRY_LATER, result.outcome)
            shared.assert_called_once()
            identity = cast(
                insider_pipeline.InsiderAccessionIdentity,
                captured["identity"],
            )
            self.assertIsInstance(identity, insider_pipeline.InsiderAccessionIdentity)
            self.assertEqual(ACCESSION, identity.accession_number)
            self.assertEqual("0000000001", identity.issuer_cik)
            self.assertEqual("4", identity.form_type)
            self.assertEqual(INDEX_URL, identity.index_url)
            self.assertEqual("2026-01-16T16:30:00Z", identity.accepted_at)
            self.assertEqual((), identity.reporting_owner_ciks)
            self.assertEqual(index_html, captured["prepared_index_html"])
            prepared_metadata = cast(
                dict[str, object],
                captured["prepared_index_metadata"],
            )
            self.assertEqual(INDEX_URL, prepared_metadata["index_url"])
            self.assertEqual([INDEX_URL], http.calls)
            self.assertEqual(index_html, storage.read_index_html(ACCESSION))
            self.assertTrue(all(response.closed for response in http.responses))


class InsiderReparseTests(unittest.TestCase):
    @staticmethod
    def _accession(ordinal: int) -> str:
        return f"0000000001-26-{ordinal:06d}"

    @classmethod
    def _prepare_artifacts(
        cls,
        root: Path,
        *,
        count: int = 1,
        old_parser_version: str = "0.9.0",
        accession_prefix: str = "0000000001",
    ) -> tuple[InsiderStorage, InsiderStateStore, tuple[str, ...]]:
        storage = InsiderStorage(root)
        state = InsiderStateStore(root)
        state.write(
            "approved-issuers-v1",
            {"contract_version": 1, "issuer_ciks": ["0000000001"]},
        )
        records = []
        accessions = []
        for ordinal in range(1, count + 1):
            accession = f"{accession_prefix}-26-{ordinal:06d}"
            compact = accession.replace("-", "")
            index_url = (
                "https://www.sec.gov/Archives/edgar/data/1/"
                f"{compact}/{accession}-index.html"
            )
            index_html = (
                FIXTURE.read_bytes()
                .replace(b"000000000126000001", compact.encode("ascii"))
                .replace(b"0000000001-26-000001", accession.encode("ascii"))
            )
            raw_xml = RAW_FIXTURE.read_bytes()
            metadata = parse_insider_filing_index(
                index_html,
                index_url=index_url,
                accession_number=accession,
                issuer_cik="0000000001",
                reporting_owner_ciks=("0000000002",),
            )
            storage.store_index_html(accession, index_html)
            storage.store_raw(accession, raw_xml)
            storage.store_source_metadata(
                accession,
                build_insider_source_metadata(metadata, index_html, raw_xml),
            )
            normalized = insider_pipeline.parse_ownership_xml(
                raw_xml,
                accession_number=accession,
                filing_date=metadata["filing_date"],
                accepted_at=metadata["accepted_at"],
                source_index_url=metadata["index_url"],
                source_document_url=metadata["document_url"],
            )
            old_normalized = {
                **normalized,
                "parser_version": old_parser_version,
            }
            storage.store_normalized(
                accession,
                old_parser_version,
                old_normalized,
            )
            records.append(
                insider_pipeline.issuer_record_from_normalized(
                    old_normalized,
                    parser_version=old_parser_version,
                )
            )
            accessions.append(accession)
        reduction = insider_pipeline.reduce_issuer_state(
            issuer_cik="0000000001",
            records=records,
        )
        state.write_issuer_if_approved(
            "0000000001",
            reduction.issuer_state,
        )
        return storage, state, tuple(accessions)

    @staticmethod
    def _deadline(seconds: int = 60) -> insider_pipeline.CooperativeDeadline:
        return insider_pipeline.CooperativeDeadline(
            started_monotonic=0.0,
            deadline_seconds=seconds,
        )

    def test_owner_filed_reparse_result_binds_explicit_issuer_not_prefix(self) -> None:
        result = insider_pipeline.InsiderReparseAccessionResult(
            accession_number="0000000002-26-000001",
            issuer_cik="0000000001",
            form_type="4",
            parser_version=INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
            stage="checkpoint",
        )

        self.assertEqual("0000000001", result.issuer_cik)
        self.assertEqual("0000000002-26-000001", result.accession_number)

    def test_owner_filed_issuer_reparse_run_uses_explicit_result_issuer(self) -> None:
        accession_number = "0000000002-26-000001"
        accession_result = insider_pipeline.InsiderReparseAccessionResult(
            accession_number=accession_number,
            issuer_cik="0000000001",
            form_type="4",
            parser_version=INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
            stage="checkpoint",
        )
        run = insider_pipeline.InsiderReparseRunResult(
            outcome=insider_pipeline.InsiderReparseOutcome.COMPLETED,
            parser_version=INSIDER_PARSER_VERSION,
            scope="issuer",
            scope_identifier="0000000001",
            queued_accessions=(accession_number,),
            completed_accessions=(accession_number,),
            accession_results=(accession_result,),
        )

        self.assertEqual((accession_number,), run.completed_accessions)
        self.assertEqual("0000000001", run.accession_results[0].issuer_cik)

    def test_owner_filed_accession_scope_resolves_explicit_issuer_offline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage, state, accessions = self._prepare_artifacts(
                root,
                accession_prefix="0000000002",
            )
            with patch.object(
                insider_pipeline.pipeline.HTTP,
                "get",
                side_effect=AssertionError("offline reparse attempted HTTP"),
            ) as http_get:
                result = insider_pipeline.run_insider_reparse(
                    scope="accession",
                    scope_identifier=accessions[0],
                    max_accessions=None,
                    storage=storage,
                    state_store=state,
                    deadline=self._deadline(),
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(
                insider_pipeline.InsiderReparseOutcome.COMPLETED,
                result.outcome,
            )
            self.assertEqual(accessions, result.completed_accessions)
            self.assertEqual("0000000001", result.accession_results[0].issuer_cik)
            self.assertEqual(
                [
                    {
                        "accession_number": accessions[0],
                        "issuer_cik": "0000000001",
                    }
                ],
                state.read("reparse-v1")["queue"],
            )
            http_get.assert_not_called()

    def test_default_reparse_is_offline_and_preserves_old_normalized_version(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage, state, accessions = self._prepare_artifacts(root)
            with patch.object(
                insider_pipeline.pipeline.HTTP,
                "get",
                side_effect=AssertionError("offline reparse attempted HTTP"),
            ) as http_get:
                result = insider_pipeline.run_insider_reparse(
                    scope="accession",
                    scope_identifier=accessions[0],
                    max_accessions=None,
                    storage=storage,
                    state_store=state,
                    deadline=self._deadline(),
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(
                insider_pipeline.InsiderReparseOutcome.COMPLETED,
                result.outcome,
            )
            self.assertEqual(accessions, result.queued_accessions)
            self.assertEqual(accessions, result.completed_accessions)
            self.assertEqual([], http_get.call_args_list)
            self.assertEqual(
                "0.9.0",
                storage.read_normalized(accessions[0], "0.9.0")["parser_version"],
            )
            self.assertEqual(
                INSIDER_PARSER_VERSION,
                storage.read_normalized(
                    accessions[0], INSIDER_PARSER_VERSION
                )["parser_version"],
            )
            issuer = state.read("issuers/0000000001")
            self.assertEqual(
                [INSIDER_PARSER_VERSION],
                [entry["parser_version"] for entry in issuer["accessions"]],
            )

    def test_existing_different_current_parser_artifact_is_not_overwritten(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage, state, accessions = self._prepare_artifacts(root)
            source = storage.read_source_metadata(accessions[0])
            raw_xml = storage.read_raw(accessions[0])
            current = insider_pipeline.parse_ownership_xml(
                raw_xml,
                accession_number=accessions[0],
                filing_date=source["filing_date"],
                accepted_at=source["accepted_at"],
                source_index_url=source["index"]["url"],
                source_document_url=source["document"]["url"],
            )
            transactions = cast(list[dict[str, object]], current["transactions"])
            divergent = {
                **current,
                "transactions": [
                    {
                        **transactions[0],
                        "normalized_security_id": "ABC",
                    },
                    *transactions[1:],
                ],
            }
            storage.store_normalized(
                accessions[0],
                INSIDER_PARSER_VERSION,
                divergent,
            )

            result = insider_pipeline.run_insider_reparse(
                scope="accession",
                scope_identifier=accessions[0],
                max_accessions=None,
                storage=storage,
                state_store=state,
                deadline=self._deadline(),
                monotonic=lambda: 0.0,
            )

            self.assertEqual(
                insider_pipeline.InsiderReparseOutcome.QUARANTINED,
                result.outcome,
            )
            self.assertEqual(
                divergent,
                storage.read_normalized(
                    accessions[0], INSIDER_PARSER_VERSION
                ),
            )
            self.assertEqual(
                "quarantined",
                state.read("reparse-v1")["status"],
            )
            self.assertEqual((), result.completed_accessions)

    def test_corrupt_stored_source_fails_closed_after_initial_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage, state, accessions = self._prepare_artifacts(root)
            raw_path = (
                root
                / "data/insiders/private/accessions"
                / accessions[0]
                / "raw.xml"
            )
            raw_path.write_bytes(b"<ownershipDocument/>")

            result = insider_pipeline.run_insider_reparse(
                scope="issuer",
                scope_identifier="0000000001",
                max_accessions=None,
                storage=storage,
                state_store=state,
                deadline=self._deadline(),
                monotonic=lambda: 0.0,
            )

            self.assertEqual(
                insider_pipeline.InsiderReparseOutcome.QUARANTINED,
                result.outcome,
            )
            checkpoint = state.read("reparse-v1")
            self.assertEqual("quarantined", checkpoint["status"])
            self.assertEqual(list(accessions), [e["accession_number"] for e in checkpoint["queue"]])
            self.assertEqual([], checkpoint["completed_accessions"])
            self.assertIsNone(result.accession_results[0].form_type)
            self.assertEqual("source", result.accession_results[0].stage)

    def test_transient_source_read_stops_cleanly_with_retry_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage, state, accessions = self._prepare_artifacts(Path(tmpdir))
            with patch.object(
                storage,
                "read_source_metadata",
                side_effect=OSError("synthetic temporary read failure"),
            ):
                result = insider_pipeline.run_insider_reparse(
                    scope="accession",
                    scope_identifier=accessions[0],
                    max_accessions=None,
                    storage=storage,
                    state_store=state,
                    deadline=self._deadline(),
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(
                insider_pipeline.InsiderReparseOutcome.CHECKPOINTED,
                result.outcome,
            )
            telemetry = result.accession_results[0]
            self.assertIsNone(telemetry.form_type)
            self.assertEqual("source", telemetry.stage)
            self.assertEqual("OSError", telemetry.error_class)
            self.assertTrue(telemetry.retry)
            self.assertEqual("running", state.read("reparse-v1")["status"])

    def test_new_parser_run_supersedes_incomplete_older_parser_checkpoint(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage, state, accessions = self._prepare_artifacts(Path(tmpdir))
            state.write_reparse_if_issuers_approved(
                {
                    "contract_version": 1,
                    "status": "running",
                    "parser_version": "0.9.0",
                    "scope": "accession",
                    "scope_identifier": accessions[0],
                    "max_accessions": 1,
                    "queue": [
                        {
                            "accession_number": accessions[0],
                            "issuer_cik": "0000000001",
                        }
                    ],
                    "completed_accessions": [],
                }
            )

            result = insider_pipeline.run_insider_reparse(
                scope="accession",
                scope_identifier=accessions[0],
                max_accessions=None,
                storage=storage,
                state_store=state,
                deadline=self._deadline(),
                monotonic=lambda: 0.0,
            )

            self.assertEqual(
                insider_pipeline.InsiderReparseOutcome.COMPLETED,
                result.outcome,
            )
            checkpoint = state.read("reparse-v1")
            self.assertEqual(INSIDER_PARSER_VERSION, checkpoint["parser_version"])
            self.assertEqual("completed", checkpoint["status"])

    def test_each_accession_is_checkpointed_and_resume_skips_completed_work(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage, state, accessions = self._prepare_artifacts(root, count=2)
            parse = insider_pipeline.parse_ownership_xml
            calls = 0

            def interrupt_second(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise KeyboardInterrupt
                return parse(*args, **kwargs)

            with patch.object(
                insider_pipeline,
                "parse_ownership_xml",
                side_effect=interrupt_second,
            ), self.assertRaises(KeyboardInterrupt):
                insider_pipeline.run_insider_reparse(
                    scope="issuer",
                    scope_identifier="0000000001",
                    max_accessions=None,
                    storage=storage,
                    state_store=state,
                    deadline=self._deadline(),
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(
                [accessions[0]],
                state.read("reparse-v1")["completed_accessions"],
            )
            with patch.object(
                insider_pipeline,
                "parse_ownership_xml",
                wraps=parse,
            ) as resumed_parse:
                result = insider_pipeline.run_insider_reparse(
                    scope="issuer",
                    scope_identifier="0000000001",
                    max_accessions=None,
                    storage=storage,
                    state_store=state,
                    deadline=self._deadline(),
                    monotonic=lambda: 0.0,
                    resume=True,
                )
            self.assertEqual(
                insider_pipeline.InsiderReparseOutcome.COMPLETED,
                result.outcome,
            )
            self.assertEqual(accessions, result.completed_accessions)
            self.assertEqual(1, resumed_parse.call_count)

    def test_all_scope_requires_and_obeys_a_deterministic_bound(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage, state, accessions = self._prepare_artifacts(root, count=2)
            with self.assertRaises(insider_pipeline.InsiderReparseError):
                insider_pipeline.run_insider_reparse(
                    scope="all",
                    scope_identifier=None,
                    max_accessions=None,
                    storage=storage,
                    state_store=state,
                    deadline=self._deadline(),
                    monotonic=lambda: 0.0,
                )

            result = insider_pipeline.run_insider_reparse(
                scope="all",
                scope_identifier=None,
                max_accessions=1,
                storage=storage,
                state_store=state,
                deadline=self._deadline(),
                monotonic=lambda: 0.0,
            )
            self.assertEqual((accessions[0],), result.queued_accessions)
            self.assertEqual(1, state.read("reparse-v1")["max_accessions"])

    def test_all_scope_retains_only_the_bounded_selection_while_scanning(self) -> None:
        live_entries = 0
        peak_entries = 0
        entries_per_issuer = 10
        issuers = tuple(f"{value:010d}" for value in range(1, 11))

        class TrackedEntry(dict[str, str]):
            def __init__(self, accession_number: str, issuer_cik: str) -> None:
                nonlocal live_entries, peak_entries
                super().__init__(
                    accession_number=accession_number,
                    issuer_cik=issuer_cik,
                )
                live_entries += 1
                peak_entries = max(peak_entries, live_entries)

            def __del__(self) -> None:
                nonlocal live_entries
                live_entries -= 1

        def issuer_entries(_state_store, issuer_cik: str):
            return tuple(
                TrackedEntry(
                    f"{issuer_cik}-26-{ordinal:06d}",
                    issuer_cik,
                )
                for ordinal in range(1, entries_per_issuer + 1)
            )

        with patch.object(
            insider_pipeline,
            "_reparse_issuer_accessions",
            side_effect=issuer_entries,
        ):
            queue, maximum = insider_pipeline._initial_reparse_queue(
                scope="all",
                scope_identifier=None,
                max_accessions=2,
                state_store=cast(InsiderStateStore, object()),
                approved_issuer_ciks=frozenset(issuers),
            )

        self.assertEqual(2, maximum)
        self.assertEqual(
            ("0000000001-26-000001", "0000000001-26-000002"),
            tuple(entry["accession_number"] for entry in queue),
        )
        self.assertLessEqual(peak_entries, entries_per_issuer + maximum)

    def test_empty_issuer_scope_is_durably_completed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage, state, accessions = self._prepare_artifacts(
                Path(tmpdir),
                count=0,
            )

            result = insider_pipeline.run_insider_reparse(
                scope="issuer",
                scope_identifier="0000000001",
                max_accessions=None,
                storage=storage,
                state_store=state,
                deadline=self._deadline(),
                monotonic=lambda: 0.0,
            )

            self.assertEqual((), accessions)
            self.assertEqual(
                insider_pipeline.InsiderReparseOutcome.COMPLETED,
                result.outcome,
            )
            self.assertEqual((), result.queued_accessions)
            self.assertEqual("completed", state.read("reparse-v1")["status"])

    def test_reparse_result_telemetry_is_bounded_and_contains_no_source_values(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage, state, accessions = self._prepare_artifacts(root)
            result = insider_pipeline.run_insider_reparse(
                scope="accession",
                scope_identifier=accessions[0],
                max_accessions=None,
                storage=storage,
                state_store=state,
                deadline=self._deadline(),
                monotonic=lambda: 0.0,
            )
            telemetry = result.accession_results[0]
            self.assertEqual(accessions[0], telemetry.accession_number)
            self.assertEqual("0000000001", telemetry.issuer_cik)
            self.assertEqual("4", telemetry.form_type)
            self.assertEqual(INSIDER_PARSER_VERSION, telemetry.parser_version)
            self.assertEqual("checkpoint", telemetry.stage)
            self.assertIsNone(telemetry.error_class)
            self.assertFalse(telemetry.retry)
            rendered = repr(telemetry)
            self.assertNotIn("SYNTHETIC ISSUER", rendered)
            self.assertNotIn("restricted_address", rendered)
            self.assertNotIn("form4-synthetic.xml", rendered)

    def test_reparse_public_results_reject_unbound_or_nondeterministic_shapes(
        self,
    ) -> None:
        first_accession = self._accession(1)
        second_accession = self._accession(2)

        with self.assertRaises(insider_pipeline.InsiderReparseError):
            insider_pipeline.InsiderReparseAccessionResult(
                accession_number=first_accession,
                issuer_cik="0000000001",
                form_type=None,
                parser_version=INSIDER_PARSER_VERSION,
                outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
                stage="checkpoint",
            )

        first_result = insider_pipeline.InsiderReparseAccessionResult(
            accession_number=first_accession,
            issuer_cik="0000000001",
            form_type="4",
            parser_version=INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
            stage="checkpoint",
        )
        second_result = insider_pipeline.InsiderReparseAccessionResult(
            accession_number=second_accession,
            issuer_cik="0000000001",
            form_type="4",
            parser_version=INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.CACHE_HIT,
            stage="checkpoint",
        )
        checkpointed_result = insider_pipeline.InsiderReparseAccessionResult(
            accession_number=first_accession,
            issuer_cik="0000000001",
            form_type=None,
            parser_version=INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
            stage="source",
            error_class="InsiderStorageError",
            retry=True,
        )

        invalid_runs = (
            {
                "outcome": insider_pipeline.InsiderReparseOutcome.COMPLETED,
                "queued_accessions": ("not-an-accession",),
                "completed_accessions": ("not-an-accession",),
                "accession_results": (),
            },
            {
                "outcome": insider_pipeline.InsiderReparseOutcome.COMPLETED,
                "queued_accessions": (first_accession, second_accession),
                "completed_accessions": (first_accession, second_accession),
                "accession_results": (second_result, first_result),
            },
            {
                "outcome": insider_pipeline.InsiderReparseOutcome.CHECKPOINTED,
                "queued_accessions": (first_accession,),
                "completed_accessions": (),
                "accession_results": (first_result,),
            },
            {
                "outcome": insider_pipeline.InsiderReparseOutcome.CHECKPOINTED,
                "queued_accessions": (first_accession,),
                "completed_accessions": (first_accession,),
                "accession_results": (checkpointed_result,),
            },
        )
        for invalid in invalid_runs:
            with self.subTest(invalid=invalid), self.assertRaises(
                insider_pipeline.InsiderReparseError
            ):
                insider_pipeline.InsiderReparseRunResult(
                    parser_version=INSIDER_PARSER_VERSION,
                    scope="issuer",
                    scope_identifier="0000000001",
                    **invalid,
                )

    def test_reparse_cli_distinguishes_cooperative_checkpoint_from_failure(
        self,
    ) -> None:
        from scripts import reparse_insider_filings as reparse_script

        common = {
            "parser_version": INSIDER_PARSER_VERSION,
            "scope": "issuer",
            "scope_identifier": "0000000001",
        }
        completed = insider_pipeline.InsiderReparseRunResult(
            outcome=insider_pipeline.InsiderReparseOutcome.COMPLETED,
            queued_accessions=(),
            completed_accessions=(),
            accession_results=(),
            **common,
        )
        cooperative = insider_pipeline.InsiderReparseRunResult(
            outcome=insider_pipeline.InsiderReparseOutcome.CHECKPOINTED,
            queued_accessions=(ACCESSION,),
            completed_accessions=(),
            accession_results=(),
            **common,
        )
        retry_result = insider_pipeline.InsiderReparseAccessionResult(
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            form_type=None,
            parser_version=INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.CHECKPOINTED,
            stage="source",
            error_class="OSError",
            retry=True,
        )
        retry = insider_pipeline.InsiderReparseRunResult(
            outcome=insider_pipeline.InsiderReparseOutcome.CHECKPOINTED,
            queued_accessions=(ACCESSION,),
            completed_accessions=(),
            accession_results=(retry_result,),
            **common,
        )
        quarantine_result = insider_pipeline.InsiderReparseAccessionResult(
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            form_type=None,
            parser_version=INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
            stage="source",
            error_class="InsiderStorageError",
            retry=False,
        )
        quarantined = insider_pipeline.InsiderReparseRunResult(
            outcome=insider_pipeline.InsiderReparseOutcome.QUARANTINED,
            queued_accessions=(ACCESSION,),
            completed_accessions=(),
            accession_results=(quarantine_result,),
            **common,
        )

        self.assertEqual(0, reparse_script._result_exit_code(completed))
        self.assertEqual(75, reparse_script._result_exit_code(cooperative))
        self.assertEqual(1, reparse_script._result_exit_code(retry))
        self.assertEqual(1, reparse_script._result_exit_code(quarantined))

    def test_reparse_cli_scopes_are_exact_and_refetch_is_not_public(self) -> None:
        from scripts import reparse_insider_filings as reparse_script

        accession = reparse_script._configuration(
            ["--accession", ACCESSION, "--deadline-seconds", "60"]
        )
        self.assertEqual("accession", accession.scope)
        issuer = reparse_script._configuration(
            ["--issuer-cik", "1", "--deadline-seconds", "60"]
        )
        self.assertEqual("issuer", issuer.scope)
        all_scope = reparse_script._configuration(
            [
                "--all",
                "--max-accessions",
                "2",
                "--deadline-seconds",
                "60",
            ]
        )
        self.assertEqual("all", all_scope.scope)
        for arguments in (
            ["--all", "--deadline-seconds", "60"],
            ["--accession", ACCESSION, "--max-accessions", "2", "--deadline-seconds", "60"],
            ["--issuer-cik", "1", "--refetch", "--deadline-seconds", "60"],
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                reparse_script._configuration(arguments)


class InsiderObservabilityTests(unittest.TestCase):
    @staticmethod
    def _identity(ordinal: int) -> insider_pipeline.InsiderAccessionIdentity:
        accession = f"0000000001-26-{ordinal:06d}"
        compact = accession.replace("-", "")
        return insider_pipeline.InsiderAccessionIdentity(
            accession_number=accession,
            issuer_cik="0000000001",
            form_type="4",
            index_url=(
                "https://www.sec.gov/Archives/edgar/data/1/"
                f"{compact}/{accession}-index.html"
            ),
            accepted_at="2026-01-17T00:00:00Z",
            reporting_owner_ciks=(),
        )

    def test_reparse_run_persists_safe_counters_and_scoped_http_metrics(self) -> None:
        started = datetime(2026, 1, 20, 12, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 1, 20, 12, 5, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage, state, accessions = InsiderReparseTests._prepare_artifacts(root)
            response = Mock(
                status_code=200,
                url="https://www.sec.gov/Archives/a",
                headers={},
            )
            response.close = Mock()
            session = Mock()
            session.headers = {}
            session.get.return_value = response
            client = insider_pipeline.pipeline.RateLimitedSession(
                session=session,
                monotonic=iter((1.0, 1.25)).__next__,
                rate=8,
            )

            with insider_pipeline.insider_telemetry_run(
                state,
                run_id="reparse-observability",
                started_at=started,
                now=lambda: finished,
            ):
                with patch.object(client, "_claim_slot", return_value=0.125):
                    client.get("https://www.sec.gov/Archives/a")
                result = insider_pipeline.run_insider_reparse(
                    scope="accession",
                    scope_identifier=accessions[0],
                    max_accessions=None,
                    storage=storage,
                    state_store=state,
                    deadline=InsiderReparseTests._deadline(),
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(
                insider_pipeline.InsiderReparseOutcome.COMPLETED,
                result.outcome,
            )
            telemetry = state.read("telemetry-v1")
            counters = telemetry["counters"]
            recent_runs = telemetry["recent_runs"]
            assert isinstance(counters, dict)
            assert isinstance(recent_runs, list)
            self.assertEqual(1, counters["http_attempts"])
            self.assertEqual(1, counters["http_status_2xx"])
            self.assertEqual(250, counters["http_latency_ms"])
            self.assertEqual(125, counters["limiter_wait_ms"])
            self.assertEqual(1, counters["limiter_utilization"])
            self.assertEqual(1, counters["reparse_attempts"])
            self.assertEqual(1, counters["reparse_completed"])
            self.assertEqual(1, counters["checkpoint_writes"])
            self.assertEqual(1, counters["parse_attempts"])
            self.assertEqual(1, counters["parse_successes"])
            self.assertGreater(counters["reporting_owner_rows"], 0)
            self.assertGreater(counters["non_derivative_transaction_rows"], 0)
            run = recent_runs[0]
            assert isinstance(run, dict)
            self.assertEqual("completed", run["status"])
            self.assertEqual("2026-01-20T12:00:00Z", run["started_at"])
            self.assertEqual("2026-01-20T12:05:00Z", run["finished_at"])
            rendered = json.dumps(telemetry, sort_keys=True)
            for sentinel in (
                "<ownershipDocument",
                "SYNTHETIC ISSUER",
                "form4_simple_purchase.xml",
                str(root),
                "restricted-address@example.invalid",
            ):
                self.assertNotIn(sentinel, rendered)

    def test_examples_bound_retry_times_and_sanitize_failure_details(self) -> None:
        started = datetime(2026, 1, 20, 12, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 1, 20, 12, 1, tzinfo=timezone.utc)
        secret = "credential=SENTINEL-CREDENTIAL /private/local/path stack-content"
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            with insider_pipeline.insider_telemetry_run(
                state,
                run_id="bounded-examples",
                started_at=started,
                now=lambda: finished,
            ):
                for ordinal in range(1, 29):
                    insider_pipeline._processor_result(
                        self._identity(ordinal),
                        INSIDER_PARSER_VERSION,
                        insider_pipeline.InsiderAccessionOutcome.CREATED,
                        "checkpoint",
                    )
                insider_pipeline._processor_result(
                    self._identity(29),
                    INSIDER_PARSER_VERSION,
                    insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
                    "raw",
                    error=ConnectionError(secret),
                    reason_code="fetch_failed",
                )
                insider_pipeline._processor_result(
                    self._identity(30),
                    INSIDER_PARSER_VERSION,
                    insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
                    "raw",
                    error=insider_pipeline.InsiderParseError(secret),
                    reason_code="raw_parse_invalid",
                )

            telemetry = state.read("telemetry-v1")
            counters = telemetry["counters"]
            assert isinstance(counters, dict)
            self.assertNotIn("checkpoint_writes", counters)
            self.assertNotIn("checkpoint_failures", counters)
            examples = telemetry["recent_runs"][0]["accession_examples"]
            self.assertEqual(insider_pipeline.MAX_TELEMETRY_ACCESSION_EXAMPLES, len(examples))
            self.assertEqual(
                sorted({example["accession_number"] for example in examples}),
                [example["accession_number"] for example in examples],
            )
            by_accession = {example["accession_number"]: example for example in examples}
            retry = by_accession["0000000001-26-000029"]
            self.assertEqual("retry_later", retry["outcome"])
            self.assertEqual("ConnectionError", retry["error_class"])
            self.assertEqual("connection_failed", retry["reason_code"])
            self.assertEqual(1, retry["retry_count"])
            self.assertEqual("2026-01-20T12:01:30Z", retry["next_retry_at"])
            quarantined = by_accession["0000000001-26-000030"]
            self.assertEqual("quarantined", quarantined["outcome"])
            self.assertEqual("InsiderParseError", quarantined["error_class"])
            self.assertEqual("raw_invalid", quarantined["reason_code"])
            self.assertEqual(0, quarantined["retry_count"])
            self.assertIsNone(quarantined["next_retry_at"])
            self.assertNotIn(secret, json.dumps(telemetry, sort_keys=True))

    def test_processor_counts_fetch_cache_parse_reduction_and_real_checkpoint(self) -> None:
        started = datetime(2026, 1, 20, 12, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 1, 20, 12, 1, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events: list[str] = []
            storage, state = InsiderAccessionProcessorTests.prepared_stores(
                root,
                events,
            )
            with insider_pipeline.insider_telemetry_run(
                state,
                run_id="processor-observability",
                started_at=started,
                now=lambda: finished,
            ):
                first = InsiderAccessionProcessorTests.process(
                    storage=storage,
                    state=state,
                    http=InsiderAccessionProcessorTests.http(events),
                    events=events,
                )
                second = InsiderAccessionProcessorTests.process(
                    storage=storage,
                    state=state,
                    http=_ProcessorHTTP({}, events),
                    events=events,
                )

            self.assertEqual(insider_pipeline.InsiderAccessionOutcome.CREATED, first.outcome)
            self.assertEqual(insider_pipeline.InsiderAccessionOutcome.CACHE_HIT, second.outcome)
            telemetry = state.read("telemetry-v1")
            counters = telemetry["counters"]
            assert isinstance(counters, dict)
            self.assertEqual(1, counters["index_fetches"])
            self.assertEqual(1, counters["raw_fetches"])
            self.assertEqual(1, counters["index_cache_hits"])
            self.assertEqual(1, counters["raw_cache_hits"])
            self.assertEqual(1, counters["parse_attempts"])
            self.assertEqual(1, counters["parse_successes"])
            self.assertEqual(1, counters["checkpoint_writes"])
            self.assertNotIn("checkpoint_failures", counters)
            self.assertIn("amendments_resolved", counters)
            self.assertIn("amendments_unresolved", counters)

    def test_recent_run_ring_is_bounded_and_unknown_version_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = InsiderStateStore(Path(tmpdir))
            base = datetime(2025, 1, 1, tzinfo=timezone.utc)
            seeded_runs = []
            for ordinal in range(insider_pipeline.MAX_TELEMETRY_RECENT_RUNS):
                timestamp = (base.replace(microsecond=0) + insider_pipeline.timedelta(seconds=ordinal))
                rendered = timestamp.isoformat().replace("+00:00", "Z")
                seeded_runs.append(
                    {
                        "run_id": f"seed-{ordinal:03d}",
                        "status": "completed",
                        "started_at": rendered,
                        "finished_at": rendered,
                        "counters": {},
                        "accession_examples": [],
                    }
                )
            state.write(
                "telemetry-v1",
                {
                    "contract_version": insider_pipeline.TELEMETRY_STATE_CONTRACT_VERSION,
                    "counters": {},
                    "recent_runs": seeded_runs,
                },
            )
            newest = datetime(2026, 1, 1, tzinfo=timezone.utc)
            with insider_pipeline.insider_telemetry_run(
                state,
                run_id="newest-run",
                started_at=newest,
                now=lambda: newest,
            ):
                pass

            runs = state.read("telemetry-v1")["recent_runs"]
            self.assertEqual(insider_pipeline.MAX_TELEMETRY_RECENT_RUNS, len(runs))
            self.assertNotIn("seed-000", {run["run_id"] for run in runs})
            self.assertIn("newest-run", {run["run_id"] for run in runs})
            with self.assertRaises(InsiderStorageError):
                state.write(
                    "telemetry-v1",
                    {"contract_version": 999, "counters": {}, "recent_runs": []},
                )

    def test_reparse_cli_wraps_the_production_run_in_private_telemetry(self) -> None:
        from scripts import reparse_insider_filings as reparse_script

        completed = insider_pipeline.InsiderReparseRunResult(
            outcome=insider_pipeline.InsiderReparseOutcome.COMPLETED,
            parser_version=INSIDER_PARSER_VERSION,
            scope="issuer",
            scope_identifier="0000000001",
            queued_accessions=(),
            completed_accessions=(),
            accession_results=(),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with (
                patch.object(reparse_script, "ROOT", root),
                patch.object(
                    reparse_script,
                    "run_insider_reparse",
                    return_value=completed,
                ) as runner,
            ):
                self.assertEqual(
                    0,
                    reparse_script.main(
                        ["--issuer-cik", "1", "--deadline-seconds", "60"]
                    ),
                )

            runner.assert_called_once()
            telemetry = InsiderStateStore(root).read("telemetry-v1")
            telemetry_counters = telemetry["counters"]
            recent_runs = telemetry["recent_runs"]
            assert isinstance(telemetry_counters, dict)
            assert isinstance(recent_runs, list)
            self.assertEqual({}, telemetry_counters)
            self.assertEqual(1, len(recent_runs))
            run = recent_runs[0]
            assert isinstance(run, dict)
            self.assertEqual("completed", run["status"])
            run_id = run["run_id"]
            assert isinstance(run_id, str)
            self.assertTrue(run_id.startswith("reparse-"))


if __name__ == "__main__":
    unittest.main()
