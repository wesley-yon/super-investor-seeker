from __future__ import annotations

from dataclasses import fields as dataclass_fields
from datetime import datetime, timezone
import io
import inspect
import os
from pathlib import Path
import stat
import struct
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import warnings
import zipfile

import insider_pipeline
import insider_storage


CATALOG_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "insider_backfill"
    / "catalog_sanitized.html"
)
CATALOG_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "insider-transactions-data-sets"
)
OLD_ZIP_URL = (
    "https://www.sec.gov/files/dera/data/"
    "insider-transactions-data-sets/2006q1.zip"
)
NEW_ZIP_URL = (
    "https://www.sec.gov/files/structureddata/data/"
    "insider-transactions-data-sets/2025q4.zip"
)
ACCESSION = "0000000001-25-000001"
OTHER_ACCESSION = "0000000002-25-000002"
CARRYOVER_ACCESSION = "0000000001-24-000003"
ALL_TABLES = (
    "SUBMISSION",
    "REPORTINGOWNER",
    "NONDERIV_TRANS",
    "NONDERIV_HOLDING",
    "DERIV_TRANS",
    "DERIV_HOLDING",
    "FOOTNOTES",
    "OWNER_SIGNATURE",
)


def _tsv(headers: tuple[str, ...], rows: tuple[tuple[str, ...], ...]) -> bytes:
    rendered = ["\t".join(headers)]
    rendered.extend("\t".join(row) for row in rows)
    return ("\n".join(rendered) + "\n").encode()


def _submission(
    *,
    extra_header: bool = False,
    accession: str = ACCESSION,
    filing_date: str = "15-DEC-2025",
) -> bytes:
    headers = ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK")
    approved = (accession, filing_date, "4", "1")
    unapproved = (OTHER_ACCESSION, "16-DEC-2025", "4/A", "2")
    if extra_header:
        headers += ("ADDITIVE_COLUMN",)
        approved += ("must-not-be-normalized",)
        unapproved += ("also-not-normalized",)
    return _tsv(headers, (approved, unapproved))


def _reporting_owner(*, accession: str = ACCESSION) -> bytes:
    return _tsv(
        ("ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME"),
        (
            (accession, "4", "Second owner"),
            (accession, "3", "First owner"),
            (OTHER_ACCESSION, "5", "Unapproved issuer owner"),
        ),
    )


def _optional_table() -> bytes:
    return _tsv(
        ("ACCESSION_NUMBER", "SOURCE_VALUE"),
        (
            (ACCESSION, "raw XML remains authoritative"),
            (OTHER_ACCESSION, "unapproved issuer row"),
        ),
    )


def _valid_tables(
    *,
    extra_submission_header: bool = False,
) -> list[tuple[str | zipfile.ZipInfo, bytes]]:
    tables: list[tuple[str | zipfile.ZipInfo, bytes]] = [
        ("SUBMISSION.tsv", _submission(extra_header=extra_submission_header))
    ]
    tables.append(("REPORTINGOWNER.tsv", _reporting_owner()))
    tables.extend((f"{name}.tsv", _optional_table()) for name in ALL_TABLES[2:])
    return tables


def _zip_bytes(
    members: list[tuple[str | zipfile.ZipInfo, bytes]],
    *,
    compression: int = zipfile.ZIP_DEFLATED,
) -> bytes:
    output = io.BytesIO()
    with warnings.catch_warnings(), zipfile.ZipFile(output, "w") as archive:
        warnings.simplefilter("ignore", UserWarning)
        for name, body in members:
            archive.writestr(name, body, compress_type=compression)
    return output.getvalue()


def _mark_first_member_encrypted(payload: bytes) -> bytes:
    mutated = bytearray(payload)
    local = mutated.find(b"PK\x03\x04")
    central = mutated.find(b"PK\x01\x02")
    if local < 0 or central < 0:
        raise AssertionError("synthetic ZIP headers missing")
    local_flags = struct.unpack_from("<H", mutated, local + 6)[0] | 1
    central_flags = struct.unpack_from("<H", mutated, central + 8)[0] | 1
    struct.pack_into("<H", mutated, local + 6, local_flags)
    struct.pack_into("<H", mutated, central + 8, central_flags)
    return bytes(mutated)


def _nul_terminate_first_member_name(payload: bytes) -> bytes:
    mutated = bytearray(payload)
    for signature, filename_offset, filename_size_offset in (
        (b"PK\x03\x04", 30, 26),
        (b"PK\x01\x02", 46, 28),
    ):
        header = mutated.find(signature)
        if header < 0:
            raise AssertionError("synthetic ZIP header missing")
        filename_size = struct.unpack_from(
            "<H", mutated, header + filename_size_offset
        )[0]
        start = header + filename_offset
        filename = bytes(mutated[start : start + filename_size])
        marker = filename.find(b"xevil")
        if marker < 0:
            raise AssertionError("synthetic ZIP filename marker missing")
        mutated[start + marker] = 0
    return bytes(mutated)


def _corrupt_first_stored_member(payload: bytes) -> bytes:
    mutated = bytearray(payload)
    local = mutated.find(b"PK\x03\x04")
    if local < 0:
        raise AssertionError("synthetic ZIP local header missing")
    filename_size = struct.unpack_from("<H", mutated, local + 26)[0]
    extra_size = struct.unpack_from("<H", mutated, local + 28)[0]
    data_offset = local + 30 + filename_size + extra_size
    mutated[data_offset] ^= 1
    return bytes(mutated)


class _BulkResponse:
    def __init__(
        self,
        content: bytes,
        *,
        url: str = NEW_ZIP_URL,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        temp_directory: Path | None = None,
        stream_error: BaseException | None = None,
    ) -> None:
        self.content = content
        self.url = url
        self.status_code = status_code
        self.headers = (
            {"Content-Length": str(len(content))} if headers is None else headers
        )
        self.temp_directory = temp_directory
        self.stream_error = stream_error
        self.close_calls = 0
        self.observed_temp_modes: list[int] = []

    def iter_content(self, chunk_size: int = 8192):
        if self.temp_directory is not None:
            for path in self.temp_directory.iterdir():
                self.observed_temp_modes.append(stat.S_IMODE(path.stat().st_mode))
        if self.stream_error is not None:
            raise self.stream_error
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset : offset + chunk_size]

    def close(self) -> None:
        self.close_calls += 1


class _BulkHTTP:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, url: str, **kwargs: object) -> object:
        self.calls.append((url, dict(kwargs)))
        return self.response


class _HostileMetadataResponse:
    def __init__(self, attribute: str, secret: str) -> None:
        self.attribute = attribute
        self.secret = secret
        self.close_calls = 0
        self.headers = {"Content-Length": "1"}

    def __getattribute__(self, name: str):
        if name in {"url", "status_code", "headers"} and name == object.__getattribute__(
            self, "attribute"
        ):
            raise RuntimeError(object.__getattribute__(self, "secret"))
        if name == "url":
            return NEW_ZIP_URL
        if name == "status_code":
            return 200
        return object.__getattribute__(self, name)

    def iter_content(self, chunk_size: int = 8192):
        del chunk_size
        yield b"x"

    def close(self) -> None:
        self.close_calls += 1


class _BoundedReadGuard:
    def __init__(self, wrapped, read_sizes: list[int]) -> None:
        self.wrapped = wrapped
        self.read_sizes = read_sizes

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.wrapped.__exit__(exc_type, exc_value, traceback)

    def __iter__(self):
        return self

    def __next__(self):
        raise AssertionError("TSV text was consumed through an unbounded iterator read")

    def readline(self, size: int = -1) -> str:
        if type(size) is not int or size <= 0:
            raise AssertionError("TSV text readline was not bounded")
        self.read_sizes.append(size)
        return self.wrapped.readline(size)


class InsiderCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = CATALOG_FIXTURE.read_bytes()
        self.as_of = datetime(2026, 2, 1, tzinfo=timezone.utc)

    def test_exact_quarter_links_accept_both_observed_sec_prefixes(self) -> None:
        old = insider_pipeline.parse_insider_bulk_catalog(
            self.catalog,
            quarter="2006Q1",
            as_of=self.as_of,
        )
        new = insider_pipeline.parse_insider_bulk_catalog(
            self.catalog,
            quarter="2025Q4",
            as_of=self.as_of,
        )

        self.assertEqual(("2006Q1", CATALOG_URL, OLD_ZIP_URL), (
            old.source_quarter,
            old.catalog_url,
            old.zip_url,
        ))
        self.assertEqual(NEW_ZIP_URL, new.zip_url)

    def test_exact_quarter_link_accepts_observed_form345_filename(self) -> None:
        expected = (
            "https://www.sec.gov/files/structureddata/data/"
            "insider-transactions-data-sets/2026q1_form345.zip"
        )
        catalog = (
            b'<html><body><a href="/files/structureddata/data/'
            b'insider-transactions-data-sets/2026q1_form345.zip">2026 Q1</a>'
            b"</body></html>"
        )

        parsed = insider_pipeline.parse_insider_bulk_catalog(
            catalog,
            quarter="2026Q1",
            as_of=self.as_of,
        )

        self.assertEqual(expected, parsed.zip_url)

    def test_catalog_accepts_benign_unbalanced_chrome_with_one_exact_link(self) -> None:
        catalog = b"""<!doctype html>
        <html><body>
          <header><div>official page chrome</header>
          <main>
            <a href="/files/structureddata/data/insider-transactions-data-sets/2025q4.zip">
              2025 Q4
            </a>
          </main>
        </body></html>
        """

        parsed = insider_pipeline.parse_insider_bulk_catalog(
            catalog,
            quarter="2025Q4",
            as_of=self.as_of,
        )

        self.assertEqual(NEW_ZIP_URL, parsed.zip_url)

    def test_catalog_ignores_invalid_unrelated_chrome_links(self) -> None:
        catalog = b"""<!doctype html>
        <html><body>
          <nav>
            <a href="/newsroom/whats-new?type=news%2Clink">News</a>
            <a href="">Decorative navigation anchor</a>
          </nav>
          <main>
            <a href="/files/structureddata/data/insider-transactions-data-sets/2025q4.zip">
              2025 Q4
            </a>
          </main>
        </body></html>
        """

        parsed = insider_pipeline.parse_insider_bulk_catalog(
            catalog,
            quarter="2025Q4",
            as_of=self.as_of,
        )

        self.assertEqual(NEW_ZIP_URL, parsed.zip_url)

    def test_catalog_link_authority_path_and_quarter_bindings_fail_closed(self) -> None:
        invalid_links = (
            "http://www.sec.gov/files/x/2025q4.zip",
            (
                "https:///files/structureddata/data/"
                "insider-transactions-data-sets/2025q4.zip"
            ),
            (
                "///files/structureddata/data/"
                "insider-transactions-data-sets/2025q4.zip"
            ),
            (
                "//www.sec.gov/files/structureddata/data/"
                "insider-transactions-data-sets/2025q4.zip"
            ),
            (
                "https://www.sec.gov/files/unexpected/private/"
                "insider-transactions-data-sets/2025q4.zip"
            ),
            "https://evil.example/files/x/2025q4.zip",
            "https://user@www.sec.gov/files/x/2025q4.zip",
            "https://www.sec.gov:444/files/x/2025q4.zip",
            "https://www.sec.gov/files/x/2025q4.zip?download=1",
            "https://www.sec.gov/files/x/2025q4.zip#fragment",
            "https://www.sec.gov/files/x/2025q3.zip",
            "https://www.sec.gov/files/x/2025q4.exe",
            "https://www.sec.gov/files/other-dataset/2025q4.zip",
            "https://www.sec.gov/files/x/%32%30%32%35q4.zip",
            "https://www.sec.gov/files/future/\u202e/insider-transactions-data-sets/2025q4.zip",
            "https://www.sec.gov/files/future/\x7f/insider-transactions-data-sets/2025q4.zip",
            "/files/structureddata/data/../data/insider-transactions-data-sets/2025q4.zip",
            "/files/structureddata/é/../data/insider-transactions-data-sets/2025q4.zip",
            "/files/structureddata/%ff/../data/insider-transactions-data-sets/2025q4.zip",
        )
        for link in invalid_links:
            html = f'<html><body><a href="{link}">quarter</a></body></html>'.encode()
            with self.subTest(link=link), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ):
                insider_pipeline.parse_insider_bulk_catalog(
                    html,
                    quarter="2025Q4",
                    as_of=self.as_of,
                )

    def test_catalog_missing_and_conflicting_quarter_links_fail_closed(self) -> None:
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline.parse_insider_bulk_catalog(
                b"<html><body><a href='/files/x/2025q3.zip'>other</a></body></html>",
                quarter="2025Q4",
                as_of=self.as_of,
            )

        conflict = b"""<html><body>
          <a href='/files/dera/data/insider-transactions-data-sets/2025q4.zip'>a</a>
          <a href='/files/structureddata/data/insider-transactions-data-sets/2025q4.zip'>b</a>
        </body></html>"""
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline.parse_insider_bulk_catalog(
                conflict,
                quarter="2025Q4",
                as_of=self.as_of,
            )

        duplicate = self.catalog.replace(b"</main>", (
            b'<a href="https://www.sec.gov/files/structureddata/data/'
            b'insider-transactions-data-sets/2025q4.zip">duplicate</a></main>'
        ))
        parsed = insider_pipeline.parse_insider_bulk_catalog(
            duplicate,
            quarter="2025Q4",
            as_of=self.as_of,
        )
        self.assertEqual(NEW_ZIP_URL, parsed.zip_url)

    def test_quarter_bounds_reject_pre_2006_invalid_and_future_values(self) -> None:
        for quarter in (
            "2005Q4",
            "2026Q2",
            "2025Q0",
            "2025Q5",
            "2025q4",
            "025Q4",
            True,
            None,
        ):
            with self.subTest(quarter=quarter), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ):
                insider_pipeline.parse_insider_bulk_catalog(
                    self.catalog,
                    quarter=quarter,
                    as_of=self.as_of,
                )

    def test_catalog_body_link_and_element_limits_are_fail_closed(self) -> None:
        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_CATALOG_BYTES", 8):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                insider_pipeline.parse_insider_bulk_catalog(
                    self.catalog,
                    quarter="2025Q4",
                    as_of=self.as_of,
                )

        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_CATALOG_LINKS", 1):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                insider_pipeline.parse_insider_bulk_catalog(
                    self.catalog,
                    quarter="2025Q4",
                    as_of=self.as_of,
                )

        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_CATALOG_ELEMENTS", 2):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                insider_pipeline.parse_insider_bulk_catalog(
                    self.catalog,
                    quarter="2025Q4",
                    as_of=self.as_of,
                )

    def test_catalog_structural_limits_abort_before_full_dom_materialization(self) -> None:
        cases = (
            (
                "element",
                "MAX_INSIDER_BULK_CATALOG_ELEMENTS",
                2,
                b"<html><body><div></div></body></html>",
            ),
            (
                "link",
                "MAX_INSIDER_BULK_CATALOG_LINKS",
                1,
                b"<html><body><a href='/a'></a><a href='/b'></a></body></html>",
            ),
        )
        for label, limit_name, limit, catalog in cases:
            with self.subTest(label=label), patch.object(
                insider_pipeline,
                limit_name,
                limit,
            ), patch(
                "insider_pipeline.etree.fromstring",
                side_effect=AssertionError("full DOM materialization must not run"),
            ) as fromstring:
                with self.assertRaisesRegex(
                    insider_pipeline.InsiderBackfillError,
                    f"catalog {label} limit",
                ):
                    insider_pipeline.parse_insider_bulk_catalog(
                        catalog,
                        quarter="2025Q4",
                        as_of=self.as_of,
                    )
                fromstring.assert_not_called()

    def test_catalog_fetch_validates_before_http_and_accepts_default_port(self) -> None:
        body = CATALOG_FIXTURE.read_bytes()
        invalid_response = _BulkResponse(body, url=CATALOG_URL)
        invalid_http = _BulkHTTP(invalid_response)
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline.fetch_insider_bulk_catalog(
                quarter="2026Q2",
                as_of=self.as_of,
                http=invalid_http,
            )
        self.assertEqual([], invalid_http.calls)
        self.assertEqual(0, invalid_response.close_calls)

        default_port_url = CATALOG_URL.replace("www.sec.gov", "www.sec.gov:443")
        response = _BulkResponse(body, url=default_port_url)
        http = _BulkHTTP(response)
        result = insider_pipeline.fetch_insider_bulk_catalog(
            quarter="2025Q4",
            as_of=self.as_of,
            http=http,
        )
        self.assertEqual(NEW_ZIP_URL, result.zip_url)
        self.assertEqual(1, response.close_calls)

    def test_catalog_stream_enforces_the_absolute_deadline(self) -> None:
        clock = [0.0]

        class LateCatalogResponse(_BulkResponse):
            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                clock[0] = 2.0
                yield self.content

        response = LateCatalogResponse(
            CATALOG_FIXTURE.read_bytes(),
            url=CATALOG_URL,
        )
        with self.assertRaisesRegex(
            insider_pipeline.InsiderBackfillError,
            "deadline",
        ):
            insider_pipeline.fetch_insider_bulk_catalog(
                quarter="2025Q4",
                as_of=self.as_of,
                http=_BulkHTTP(response),
                deadline_monotonic=1.0,
                monotonic=lambda: clock[0],
            )

        self.assertEqual(1, response.close_calls)

    def test_catalog_http_failures_are_sanitized_and_closed(self) -> None:
        secret = "TASK7_CATALOG_STREAM_SECRET"
        response = _BulkResponse(
            CATALOG_FIXTURE.read_bytes(),
            url=CATALOG_URL,
            stream_error=RuntimeError(secret),
        )
        with self.assertRaises(insider_pipeline.InsiderBackfillError) as raised:
            insider_pipeline.fetch_insider_bulk_catalog(
                quarter="2025Q4",
                as_of=self.as_of,
                http=_BulkHTTP(response),
            )
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        self.assertEqual(1, response.close_calls)

        secret = "TASK7_CATALOG_URL_SECRET"
        hostile = _HostileMetadataResponse("url", secret)
        with self.assertRaises(insider_pipeline.InsiderBackfillError) as raised:
            insider_pipeline.fetch_insider_bulk_catalog(
                quarter="2025Q4",
                as_of=self.as_of,
                http=_BulkHTTP(hostile),
            )
        self.assertNotIn(secret, " ".join((
            str(raised.exception),
            str(raised.exception.__cause__),
            str(raised.exception.__context__),
        )))
        self.assertEqual(1, hostile.close_calls)

    def test_catalog_requires_one_complete_200_response(self) -> None:
        for status_code in (204, 206):
            response = _BulkResponse(
                CATALOG_FIXTURE.read_bytes(),
                url=CATALOG_URL,
                status_code=status_code,
            )
            with self.subTest(status_code=status_code), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ):
                insider_pipeline.fetch_insider_bulk_catalog(
                    quarter="2025Q4",
                    as_of=self.as_of,
                    http=_BulkHTTP(response),
                )
            self.assertEqual(1, response.close_calls)

    def test_catalog_declared_length_must_match_selected_bytes(self) -> None:
        body = CATALOG_FIXTURE.read_bytes()
        response = _BulkResponse(
            body,
            url=CATALOG_URL,
            headers={"Content-Length": str(len(body) + 1)},
        )
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline.fetch_insider_bulk_catalog(
                quarter="2025Q4",
                as_of=self.as_of,
                http=_BulkHTTP(response),
            )
        self.assertEqual(1, response.close_calls)

    def test_catalog_rejects_non_identity_content_encoding(self) -> None:
        body = CATALOG_FIXTURE.read_bytes()
        response = _BulkResponse(
            body,
            url=CATALOG_URL,
            headers={
                "Content-Length": str(len(body)),
                "Content-Encoding": "gzip",
            },
        )
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline.fetch_insider_bulk_catalog(
                quarter="2025Q4",
                as_of=self.as_of,
                http=_BulkHTTP(response),
            )
        self.assertEqual(1, response.close_calls)

    def test_catalog_rejects_unsafe_declarations_and_dot_segments(self) -> None:
        cases = (
            b"<!DOCTYPE html SYSTEM 'https://evil.example/x'><html></html>",
            b"<!ENTITY secret SYSTEM 'file:///etc/passwd'><html></html>",
            (
                b"<html><a href='https://www.sec.gov/files/../"
                b"insider-transactions-data-sets/2025q4.zip'>quarter</a></html>"
            ),
        )
        for catalog in cases:
            with self.subTest(catalog=catalog), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ):
                insider_pipeline.parse_insider_bulk_catalog(
                    catalog,
                    quarter="2025Q4",
                    as_of=self.as_of,
                )


class InsiderBulkArchiveSafetyTests(unittest.TestCase):
    def catalog(self) -> insider_pipeline.InsiderBulkCatalogEntry:
        return insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )

    def fetch(
        self,
        payload: bytes,
        *,
        response_url: str = NEW_ZIP_URL,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        approved: tuple[object, ...] = ("1",),
        expected_source: insider_pipeline.InsiderBulkSourceIdentity | None = None,
        stream_error: BaseException | None = None,
    ) -> tuple[
        insider_pipeline.InsiderBulkArchiveResult,
        _BulkResponse,
        _BulkHTTP,
        Path,
    ]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        response = _BulkResponse(
            payload,
            url=response_url,
            status_code=status_code,
            headers=headers,
            temp_directory=root,
            stream_error=stream_error,
        )
        http = _BulkHTTP(response)
        result = insider_pipeline.fetch_insider_bulk_archive(
            self.catalog(),
            approved_issuer_ciks=approved,
            http=http,
            temp_directory=root,
            expected_source=expected_source,
        )
        return result, response, http, root

    def test_valid_archive_records_bounded_evidence_and_only_safe_discovery_values(self) -> None:
        payload = _zip_bytes(_valid_tables(extra_submission_header=True)[::-1])
        headers = {
            "Content-Length": str(len(payload)),
            "ETag": 'W/"synthetic-etag"',
            "Last-Modified": "Sun, 06 Nov 1994 08:49:37 GMT",
        }
        result, response, http, root = self.fetch(payload, headers=headers)

        self.assertEqual("2025Q4", result.source_quarter)
        self.assertEqual(CATALOG_URL, result.catalog_url)
        self.assertEqual(NEW_ZIP_URL, result.zip_url)
        self.assertEqual(len(payload), result.zip_byte_count)
        self.assertRegex(result.zip_sha256, r"^[0-9a-f]{64}$")
        self.assertEqual('W/"synthetic-etag"', result.etag)
        self.assertEqual("Sun, 06 Nov 1994 08:49:37 GMT", result.last_modified)
        self.assertEqual((), result.missing_optional_tables)
        self.assertEqual((ACCESSION,), tuple(
            item.accession_number for item in result.selected_accessions
        ))
        self.assertEqual("0000000001", result.selected_accessions[0].issuer_cik)
        self.assertEqual("4", result.selected_accessions[0].form_type)
        self.assertEqual("2025-12-15", result.selected_accessions[0].filing_date)
        self.assertEqual(
            ("0000000003", "0000000004"),
            result.selected_accessions[0].reporting_owner_ciks,
        )
        evidence = {item.table_name: item for item in result.table_evidence}
        self.assertEqual(set(ALL_TABLES), set(evidence))
        self.assertIn("ADDITIVE_COLUMN", evidence["SUBMISSION"].headers)
        self.assertEqual(2, evidence["SUBMISSION"].row_count)
        self.assertEqual(1, evidence["SUBMISSION"].selected_row_count)
        self.assertEqual(2, evidence["REPORTINGOWNER"].selected_row_count)
        self.assertEqual(1, evidence["FOOTNOTES"].selected_row_count)
        self.assertNotIn("raw XML remains authoritative", repr(result))
        self.assertFalse(hasattr(result, "rows"))
        self.assertFalse(hasattr(result, "values"))
        self.assertEqual(
            {
                "source_quarter",
                "catalog_url",
                "zip_url",
                "zip_sha256",
                "zip_byte_count",
                "etag",
                "last_modified",
                "table_evidence",
                "missing_optional_tables",
                "selected_accessions",
            },
            {field.name for field in dataclass_fields(type(result))},
        )
        self.assertEqual(
            [(
                NEW_ZIP_URL,
                {
                    "stream": True,
                    "headers": {"Accept-Encoding": "identity"},
                },
            )],
            http.calls,
        )
        self.assertEqual(1, response.close_calls)
        self.assertEqual([0o600], sorted(set(response.observed_temp_modes)))
        self.assertEqual([], list(root.iterdir()))

    def test_missing_optional_tables_are_explicit_telemetry(self) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        result, _, _, _ = self.fetch(payload)
        self.assertEqual(tuple(sorted(ALL_TABLES[1:])), result.missing_optional_tables)
        self.assertEqual(("SUBMISSION",), tuple(
            evidence.table_name for evidence in result.table_evidence
        ))

    def test_exact_official_auxiliary_members_are_bounded_and_ignored(self) -> None:
        payload = _zip_bytes([
            ("SUBMISSION.tsv", _submission()),
            ("FORM_345_metadata.json", b'{"official":"metadata"}\n'),
            ("FORM_345_readme.htm", b"<html><body>Official readme</body></html>\n"),
        ])

        result, _, _, _ = self.fetch(payload)

        self.assertEqual(("SUBMISSION",), tuple(
            evidence.table_name for evidence in result.table_evidence
        ))
        self.assertEqual(tuple(sorted(ALL_TABLES[1:])), result.missing_optional_tables)

    def test_member_paths_duplicates_types_encryption_and_compression_fail_closed(self) -> None:
        symlink = zipfile.ZipInfo("SUBMISSION.tsv")
        symlink.create_system = 3
        symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
        non_unix_symlink = zipfile.ZipInfo("SUBMISSION.tsv")
        non_unix_symlink.create_system = 0
        non_unix_symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
        dos_directory = zipfile.ZipInfo("SUBMISSION.tsv")
        dos_directory.create_system = 0
        dos_directory.external_attr = 0x10
        unix_dos_directory = zipfile.ZipInfo("SUBMISSION.tsv")
        unix_dos_directory.create_system = 3
        unix_dos_directory.external_attr = (
            (stat.S_IFREG | 0o600) << 16
        ) | 0x10
        cases: list[tuple[str, bytes]] = [
            ("traversal", _zip_bytes([("../SUBMISSION.tsv", _submission())])),
            ("absolute", _zip_bytes([("/SUBMISSION.tsv", _submission())])),
            ("nested", _zip_bytes([("nested/SUBMISSION.tsv", _submission())])),
            ("windows", _zip_bytes([("C:\\SUBMISSION.tsv", _submission())])),
            (
                "nul-truncated",
                _nul_terminate_first_member_name(
                    _zip_bytes([("SUBMISSION.tsvxevil", _submission())])
                ),
            ),
            (
                "duplicate",
                _zip_bytes([
                    ("SUBMISSION.tsv", _submission()),
                    ("SUBMISSION.tsv", _submission()),
                ]),
            ),
            ("symlink", _zip_bytes([(symlink, _submission())])),
            (
                "non-unix-symlink-metadata",
                _zip_bytes([(non_unix_symlink, _submission())]),
            ),
            ("dos-directory", _zip_bytes([(dos_directory, _submission())])),
            (
                "unix-dos-directory",
                _zip_bytes([(unix_dos_directory, _submission())]),
            ),
            (
                "encrypted",
                _mark_first_member_encrypted(
                    _zip_bytes([("SUBMISSION.tsv", _submission())])
                ),
            ),
            (
                "unsupported-compression",
                _zip_bytes(
                    [("SUBMISSION.tsv", _submission())],
                    compression=zipfile.ZIP_BZIP2,
                ),
            ),
            ("unknown-member", _zip_bytes([("UNKNOWN.tsv", b"A\n1\n")])),
        ]
        for label, payload in cases:
            with self.subTest(label=label), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ):
                self.fetch(payload)

    def test_archive_requires_one_complete_200_response(self) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        for status_code in (204, 206):
            with self.subTest(status_code=status_code), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                response = _BulkResponse(
                    payload,
                    status_code=status_code,
                    temp_directory=root,
                )
                with self.assertRaises(insider_pipeline.InsiderBackfillError):
                    insider_pipeline.fetch_insider_bulk_archive(
                        self.catalog(),
                        approved_issuer_ciks=("1",),
                        http=_BulkHTTP(response),
                        temp_directory=root,
                    )
                self.assertEqual(1, response.close_calls)
                self.assertEqual([], list(root.iterdir()))

    def test_archive_stream_enforces_deadline_and_removes_partial_file(self) -> None:
        clock = [0.0]
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])

        class LateArchiveResponse(_BulkResponse):
            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                clock[0] = 2.0
                yield self.content

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            response = LateArchiveResponse(payload, temp_directory=root)
            with self.assertRaisesRegex(
                insider_pipeline.InsiderBackfillError,
                "deadline",
            ):
                insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=_BulkHTTP(response),
                    temp_directory=root,
                    deadline_monotonic=1.0,
                    monotonic=lambda: clock[0],
                )

            self.assertEqual(1, response.close_calls)
            self.assertEqual([], list(root.iterdir()))

    def test_archive_stream_interrupts_blocking_read_and_cleans_partial_file(
        self,
    ) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])

        class BlockingArchiveResponse(_BulkResponse):
            def __init__(self, *, temp_directory: Path) -> None:
                super().__init__(payload, temp_directory=temp_directory)
                self.released = threading.Event()

            def close(self) -> None:
                super().close()
                self.released.set()

            def iter_content(self, chunk_size: int = 8192):
                del chunk_size
                self.released.wait(timeout=1.0)
                if self.close_calls == 0:
                    yield self.content

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            response = BlockingArchiveResponse(temp_directory=root)
            started = time.monotonic()
            with self.assertRaisesRegex(
                insider_pipeline.InsiderBackfillError,
                "deadline",
            ):
                insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=_BulkHTTP(response),
                    temp_directory=root,
                    deadline_monotonic=started + 0.05,
                    monotonic=time.monotonic,
                )
            elapsed = time.monotonic() - started

            self.assertLess(elapsed, 0.5)
            self.assertEqual(1, response.close_calls)
            self.assertTrue(response.released.is_set())
            self.assertEqual([], list(root.iterdir()))

    def test_archive_rejects_non_identity_content_encoding(self) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            response = _BulkResponse(
                payload,
                headers={
                    "Content-Length": str(len(payload)),
                    "Content-Encoding": "gzip",
                },
                temp_directory=root,
            )
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=_BulkHTTP(response),
                    temp_directory=root,
                )
            self.assertEqual(1, response.close_calls)
            self.assertEqual([], list(root.iterdir()))

    def test_archive_temp_directory_and_parsed_bytes_remain_private_and_hash_bound(self) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            root.chmod(0o777)
            response = _BulkResponse(payload, temp_directory=root)
            http = _BulkHTTP(response)
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=http,
                    temp_directory=root,
                )
            self.assertEqual([], http.calls)
            self.assertEqual(0, response.close_calls)

        replacement = _zip_bytes([
            ("SUBMISSION.tsv", _submission(accession=OTHER_ACCESSION))
        ])
        original_parser = insider_pipeline._parse_insider_bulk_archive_file

        def replace_before_parse(path: Path, **kwargs):
            replacement_path = path.with_name(path.name + ".replacement")
            replacement_path.write_bytes(replacement)
            replacement_path.chmod(0o600)
            replacement_path.replace(path)
            return original_parser(path, **kwargs)

        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            insider_pipeline,
            "_parse_insider_bulk_archive_file",
            side_effect=replace_before_parse,
        ):
            root = Path(tmpdir)
            response = _BulkResponse(payload, temp_directory=root)
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=_BulkHTTP(response),
                    temp_directory=root,
                )
            self.assertEqual(1, response.close_calls)
            self.assertEqual([], list(root.iterdir()))

    def test_archive_hash_binds_an_immutable_parse_snapshot(self) -> None:
        payload = _zip_bytes(
            [("SUBMISSION.tsv", _submission())],
            compression=zipfile.ZIP_STORED,
        )
        replacement = _zip_bytes(
            [("SUBMISSION.tsv", _submission(accession=OTHER_ACCESSION))],
            compression=zipfile.ZIP_STORED,
        )
        self.assertEqual(len(payload), len(replacement))

        original_read = os.read
        mutated = False
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            def mutate_source_after_hash_read(descriptor: int, size: int) -> bytes:
                nonlocal mutated
                chunk = original_read(descriptor, size)
                if chunk or mutated:
                    return chunk
                archives = tuple(root.iterdir())
                self.assertEqual(1, len(archives))
                archives[0].write_bytes(replacement)
                mutated = True
                return chunk

            response = _BulkResponse(payload, temp_directory=root)
            with patch.object(
                insider_pipeline.os,
                "read",
                side_effect=mutate_source_after_hash_read,
            ):
                result = insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=_BulkHTTP(response),
                    temp_directory=root,
                )

            self.assertTrue(mutated)
            self.assertEqual(
                (ACCESSION,),
                tuple(item.accession_number for item in result.selected_accessions),
            )
            self.assertEqual(1, response.close_calls)
            self.assertEqual([], list(root.iterdir()))

    def test_member_count_size_and_compression_ratio_limits_fail_closed(self) -> None:
        valid = _zip_bytes([("SUBMISSION.tsv", _submission())])
        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_ZIP_MEMBERS", 0):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(valid)

        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_COMPRESSED_BYTES", 1):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(valid)

        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_UNCOMPRESSED_BYTES", 1):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(valid)

        ratio_payload = _zip_bytes([
            ("SUBMISSION.tsv", _submission() + b"#" * 10_000),
        ])
        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_COMPRESSION_RATIO", 2):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(ratio_payload)

    def test_tsv_encoding_shape_header_column_field_and_row_limits_fail_closed(self) -> None:
        malformed_cases = (
            (
                "nul",
                b"ACCESSION_NUMBER\tFILING_DATE\tDOCUMENT_TYPE\tISSUERCIK\n"
                + ACCESSION.encode()
                + b"\t15-DEC-2025\t4\t1\x00\n",
            ),
            (
                "utf8",
                b"ACCESSION_NUMBER\tFILING_DATE\tDOCUMENT_TYPE\tISSUERCIK\n"
                b"\xff\t15-DEC-2025\t4\t1\n",
            ),
            (
                "missing-header",
                b"ACCESSION_NUMBER\tFILING_DATE\tDOCUMENT_TYPE\n"
                + ACCESSION.encode()
                + b"\t15-DEC-2025\t4\n",
            ),
            (
                "duplicate-header",
                b"ACCESSION_NUMBER\tFILING_DATE\tISSUERCIK\tISSUERCIK\tDOCUMENT_TYPE\n"
                + ACCESSION.encode()
                + b"\t15-DEC-2025\t1\t1\t4\n",
            ),
            (
                "empty-header",
                b"ACCESSION_NUMBER\tFILING_DATE\tDOCUMENT_TYPE\t\n"
                + ACCESSION.encode()
                + b"\t15-DEC-2025\t4\t1\n",
            ),
            (
                "missing-field",
                b"ACCESSION_NUMBER\tFILING_DATE\tDOCUMENT_TYPE\tISSUERCIK\n"
                + ACCESSION.encode()
                + b"\t15-DEC-2025\t4\n",
            ),
            (
                "extra-field",
                b"ACCESSION_NUMBER\tFILING_DATE\tDOCUMENT_TYPE\tISSUERCIK\n"
                + ACCESSION.encode()
                + b"\t15-DEC-2025\t4\t1\textra\n",
            ),
        )
        for label, submission in malformed_cases:
            with self.subTest(label=label), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ):
                self.fetch(_zip_bytes([("SUBMISSION.tsv", submission)]))

        too_many_columns = _tsv(
            ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK", "EXTRA"),
            ((ACCESSION, "15-DEC-2025", "4", "1", "x"),),
        )
        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_TSV_COLUMNS", 4):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(_zip_bytes([("SUBMISSION.tsv", too_many_columns)]))

        oversized_record = _tsv(
            ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK", "EXTRA"),
            ((ACCESSION, "15-DEC-2025", "4", "1", "x" * 100),),
        )
        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_TSV_RECORD_CHARS", 80):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(_zip_bytes([("SUBMISSION.tsv", oversized_record)]))

        long_field = _tsv(
            ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK", "EXTRA"),
            ((ACCESSION, "15-DEC-2025", "4", "1", "abcdef"),),
        )
        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_TSV_FIELD_CHARS", 5):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(_zip_bytes([("SUBMISSION.tsv", long_field)]))

        two_rows = _submission()
        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_TSV_ROWS", 1):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(_zip_bytes([("SUBMISSION.tsv", two_rows)]))

    def test_submission_is_required_and_accession_bindings_fail_closed(self) -> None:
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            self.fetch(_zip_bytes([("FOOTNOTES.tsv", _optional_table())]))

        invalid_accession = _tsv(
            ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK"),
            (("../bad", "15-DEC-2025", "4", "1"),),
        )
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            self.fetch(_zip_bytes([("SUBMISSION.tsv", invalid_accession)]))

        conflicting = _tsv(
            ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK"),
            (
                (ACCESSION, "15-DEC-2025", "4", "1"),
                (ACCESSION, "15-DEC-2025", "4/A", "1"),
            ),
        )
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            self.fetch(_zip_bytes([("SUBMISSION.tsv", conflicting)]))

        for filing_date in (
            "2025-12-15",
            "31-FEB-2025",
            "1-JAN-2025",
            "31-dec-2025",
        ):
            submission = _tsv(
                ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK"),
                ((ACCESSION, filing_date, "4", "1"),),
            )
            with self.subTest(filing_date=filing_date), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ):
                self.fetch(_zip_bytes([("SUBMISSION.tsv", submission)]))

        wrong_optional_accession = _tsv(
            ("ACCESSION_NUMBER", "SOURCE_VALUE"),
            (("not-an-accession", "x"),),
        )
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            self.fetch(_zip_bytes([
                ("SUBMISSION.tsv", _submission()),
                ("FOOTNOTES.tsv", wrong_optional_accession),
            ]))

    def test_official_submission_schema_and_next_quarter_carryover_are_supported(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q1",
            catalog_url=CATALOG_URL,
            zip_url=(
                "https://www.sec.gov/files/structureddata/data/"
                "insider-transactions-data-sets/2025q1.zip"
            ),
        )
        payload = _zip_bytes([
            (
                "SUBMISSION.tsv",
                _submission(
                    accession=CARRYOVER_ACCESSION,
                    filing_date="31-DEC-2024",
                ),
            ),
            (
                "REPORTINGOWNER.tsv",
                _reporting_owner(accession=CARRYOVER_ACCESSION),
            ),
        ])
        with tempfile.TemporaryDirectory() as tmpdir:
            response = _BulkResponse(payload, url=entry.zip_url)
            result = insider_pipeline.fetch_insider_bulk_archive(
                entry,
                approved_issuer_ciks=("1",),
                http=_BulkHTTP(response),
                temp_directory=Path(tmpdir),
                as_of=datetime(2025, 4, 1, tzinfo=timezone.utc),
            )
        self.assertEqual((CARRYOVER_ACCESSION,), tuple(
            item.accession_number for item in result.selected_accessions
        ))
        self.assertEqual("2024-12-31", result.selected_accessions[0].filing_date)
        self.assertEqual(
            ("0000000003", "0000000004"),
            result.selected_accessions[0].reporting_owner_ciks,
        )

    def test_reporting_owner_header_cik_and_uniqueness_fail_closed(self) -> None:
        cases = (
            _tsv(
                ("ACCESSION_NUMBER", "RPTOWNERNAME"),
                ((ACCESSION, "Missing CIK"),),
            ),
            _tsv(
                ("ACCESSION_NUMBER", "RPTOWNERCIK"),
                ((ACCESSION, "not-a-cik"),),
            ),
            _tsv(
                ("ACCESSION_NUMBER", "RPTOWNERCIK"),
                ((ACCESSION, "3"), (ACCESSION, "0000000003")),
            ),
        )
        for reporting_owner in cases:
            with self.subTest(reporting_owner=reporting_owner), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ):
                self.fetch(_zip_bytes([
                    ("SUBMISSION.tsv", _submission()),
                    ("REPORTINGOWNER.tsv", reporting_owner),
                ]))

    def test_download_bounds_final_url_status_streaming_close_and_cleanup(self) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        cases = (
            ("status", NEW_ZIP_URL, 404, {"Content-Length": str(len(payload))}),
            ("url", OLD_ZIP_URL, 200, {"Content-Length": str(len(payload))}),
            ("declared", NEW_ZIP_URL, 200, {"Content-Length": str(len(payload) + 1)}),
            ("negative", NEW_ZIP_URL, 200, {"Content-Length": "-1"}),
        )
        for label, url, status, headers in cases:
            with self.subTest(label=label):
                temporary = tempfile.TemporaryDirectory()
                self.addCleanup(temporary.cleanup)
                root = Path(temporary.name)
                response = _BulkResponse(
                    payload,
                    url=url,
                    status_code=status,
                    headers=headers,
                    temp_directory=root,
                )
                with self.assertRaises(insider_pipeline.InsiderBackfillError):
                    insider_pipeline.fetch_insider_bulk_archive(
                        self.catalog(),
                        approved_issuer_ciks=("1",),
                        http=_BulkHTTP(response),
                        temp_directory=root,
                    )
                self.assertEqual(1, response.close_calls)
                self.assertEqual([], list(root.iterdir()))

        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_ARCHIVE_BYTES", 8):
            temporary = tempfile.TemporaryDirectory()
            self.addCleanup(temporary.cleanup)
            root = Path(temporary.name)
            response = _BulkResponse(
                payload,
                url=NEW_ZIP_URL,
                headers={},
                temp_directory=root,
            )
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=_BulkHTTP(response),
                    temp_directory=root,
                )
            self.assertEqual(1, response.close_calls)
            self.assertEqual([], list(root.iterdir()))

    def test_hostile_response_metadata_and_stream_failures_are_sanitized(self) -> None:
        for attribute in ("url", "status_code", "headers"):
            secret = f"TASK7_{attribute.upper()}_SECRET"
            response = _HostileMetadataResponse(attribute, secret)
            http = _BulkHTTP(response)
            with self.subTest(attribute=attribute), tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(insider_pipeline.InsiderBackfillError) as raised:
                    insider_pipeline.fetch_insider_bulk_archive(
                        self.catalog(),
                        approved_issuer_ciks=("1",),
                        http=http,
                        temp_directory=Path(tmpdir),
                    )
                chain = (
                    str(raised.exception),
                    str(raised.exception.__cause__),
                    str(raised.exception.__context__),
                )
                self.assertNotIn(secret, " ".join(chain))
                self.assertEqual(1, response.close_calls)
                self.assertEqual([], list(Path(tmpdir).iterdir()))

        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        secret = "TASK7_STREAM_SECRET"
        with self.assertRaises(insider_pipeline.InsiderBackfillError) as raised:
            self.fetch(payload, stream_error=RuntimeError(secret))
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_crc_corruption_fails_closed_and_removes_temporary_archive(self) -> None:
        payload = _corrupt_first_stored_member(
            _zip_bytes(
                [("SUBMISSION.tsv", _submission())],
                compression=zipfile.ZIP_STORED,
            )
        )
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        response = _BulkResponse(payload, temp_directory=root)
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline.fetch_insider_bulk_archive(
                self.catalog(),
                approved_issuer_ciks=("1",),
                http=_BulkHTTP(response),
                temp_directory=root,
            )
        self.assertEqual(1, response.close_calls)
        self.assertEqual([], list(root.iterdir()))

    def test_archive_io_and_cleanup_failures_are_normalized(self) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        response = _BulkResponse(payload, temp_directory=root)
        with patch.object(insider_pipeline.os, "fsync", side_effect=OSError("secret")):
            with self.assertRaises(insider_pipeline.InsiderBackfillError) as raised:
                insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=_BulkHTTP(response),
                    temp_directory=root,
                )
        self.assertNotIn("secret", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)
        self.assertEqual([], list(root.iterdir()))

        response = _BulkResponse(payload, temp_directory=root)
        with patch.object(Path, "unlink", side_effect=OSError("cleanup secret")):
            with self.assertRaises(insider_pipeline.InsiderBackfillError) as raised:
                insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=_BulkHTTP(response),
                    temp_directory=root,
                )
        self.assertNotIn("cleanup secret", str(raised.exception))

        invalid = _zip_bytes([("FOOTNOTES.tsv", _optional_table())])
        response = _BulkResponse(invalid, temp_directory=root)
        with patch.object(Path, "unlink", side_effect=OSError("cleanup secret")):
            with self.assertRaises(insider_pipeline.InsiderBackfillError) as raised:
                insider_pipeline.fetch_insider_bulk_archive(
                    self.catalog(),
                    approved_issuer_ciks=("1",),
                    http=_BulkHTTP(response),
                    temp_directory=root,
                )
        self.assertIn("temporary archive cleanup", str(raised.exception))
        self.assertNotIn("cleanup secret", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_source_hash_revision_removes_temporary_archive(self) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        response = _BulkResponse(payload, temp_directory=root)
        expected = insider_pipeline.InsiderBulkSourceIdentity(
            source_quarter="2025Q4",
            zip_url=NEW_ZIP_URL,
            zip_sha256="0" * 64,
        )
        with self.assertRaises(insider_pipeline.InsiderBulkSourceRevisionError):
            insider_pipeline.fetch_insider_bulk_archive(
                self.catalog(),
                approved_issuer_ciks=("1",),
                http=_BulkHTTP(response),
                temp_directory=root,
                expected_source=expected,
            )
        self.assertEqual([], list(root.iterdir()))

    def test_completed_source_identity_must_remain_exact(self) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        first, _, _, _ = self.fetch(payload)
        expected = insider_pipeline.InsiderBulkSourceIdentity(
            source_quarter=first.source_quarter,
            zip_url=first.zip_url,
            zip_sha256=first.zip_sha256,
        )
        replay, _, _, _ = self.fetch(payload, expected_source=expected)
        self.assertEqual(first.zip_sha256, replay.zip_sha256)

        default_port = insider_pipeline.InsiderBulkSourceIdentity(
            source_quarter=first.source_quarter,
            zip_url=first.zip_url.replace("www.sec.gov/", "www.sec.gov:443/"),
            zip_sha256=first.zip_sha256,
        )
        replay, _, _, _ = self.fetch(payload, expected_source=default_port)
        self.assertEqual(first.zip_sha256, replay.zip_sha256)

        wrong_hash = insider_pipeline.InsiderBulkSourceIdentity(
            source_quarter="2025Q4",
            zip_url=NEW_ZIP_URL,
            zip_sha256="0" * 64,
        )
        with self.assertRaises(insider_pipeline.InsiderBulkSourceRevisionError):
            self.fetch(payload, expected_source=wrong_hash)

        wrong_url = insider_pipeline.InsiderBulkSourceIdentity(
            source_quarter="2025Q4",
            zip_url=(
                "https://www.sec.gov/files/dera/data/"
                "insider-transactions-data-sets/2025q4.zip"
            ),
            zip_sha256=first.zip_sha256,
        )
        with self.assertRaises(insider_pipeline.InsiderBulkSourceRevisionError):
            self.fetch(payload, expected_source=wrong_url)

    def test_catalog_fetch_uses_shared_streaming_http_and_closes_response(self) -> None:
        body = CATALOG_FIXTURE.read_bytes()
        response = _BulkResponse(body, url=CATALOG_URL)
        http = _BulkHTTP(response)
        result = insider_pipeline.fetch_insider_bulk_catalog(
            quarter="2025Q4",
            as_of=datetime(2026, 2, 1, tzinfo=timezone.utc),
            http=http,
        )
        self.assertEqual(NEW_ZIP_URL, result.zip_url)
        self.assertEqual(
            [(
                CATALOG_URL,
                {
                    "stream": True,
                    "headers": {"Accept-Encoding": "identity"},
                },
            )],
            http.calls,
        )
        self.assertEqual(1, response.close_calls)

    def test_submission_is_opened_first_and_no_extract_api_is_used(self) -> None:
        payload = _zip_bytes(_valid_tables()[::-1])
        opened: list[str] = []
        original_open = zipfile.ZipFile.open

        def recording_open(archive, member, *args, **kwargs):
            opened.append(member.filename if isinstance(member, zipfile.ZipInfo) else member)
            return original_open(archive, member, *args, **kwargs)

        with patch.object(zipfile.ZipFile, "open", recording_open):
            self.fetch(payload)
        self.assertEqual("SUBMISSION.tsv", opened[0])
        implementation = inspect.getsource(
            insider_pipeline._parse_insider_bulk_archive_file
        )
        self.assertNotIn(".extract(", implementation)
        self.assertNotIn(".extractall(", implementation)

    def test_tsv_records_are_bounded_before_csv_materialization(self) -> None:
        payload = _zip_bytes(_valid_tables())
        original_text_wrapper = io.TextIOWrapper
        read_sizes: list[int] = []

        def guarded_text_wrapper(*args, **kwargs):
            return _BoundedReadGuard(
                original_text_wrapper(*args, **kwargs),
                read_sizes,
            )

        with patch.object(insider_pipeline.io, "TextIOWrapper", guarded_text_wrapper):
            result, _, _, _ = self.fetch(payload)

        self.assertEqual((ACCESSION,), tuple(
            item.accession_number for item in result.selected_accessions
        ))
        self.assertTrue(read_sizes)
        self.assertTrue(all(size > 0 for size in read_sizes))

    def test_multiline_tsv_records_parse_and_share_one_cumulative_bound(self) -> None:
        multiline_footnote = (
            b"ACCESSION_NUMBER\tSOURCE_VALUE\n"
            + ACCESSION.encode()
            + b'\t"'
            + b"a" * 40
            + b"\n"
            + b"b" * 40
            + b'"\n'
        )
        payload = _zip_bytes([
            ("SUBMISSION.tsv", _submission()),
            ("FOOTNOTES.tsv", multiline_footnote),
        ])

        result, _, _, _ = self.fetch(payload)
        evidence = {item.table_name: item for item in result.table_evidence}
        self.assertEqual(1, evidence["FOOTNOTES"].selected_row_count)

        with patch.object(insider_pipeline, "MAX_INSIDER_BULK_TSV_RECORD_CHARS", 80):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(payload)

    def test_public_records_and_selected_accession_limit_fail_closed(self) -> None:
        for factory in (
            lambda: insider_pipeline.InsiderBulkCatalogEntry(
                source_quarter="2005Q4",
                catalog_url=CATALOG_URL,
                zip_url=OLD_ZIP_URL,
            ),
            lambda: insider_pipeline.InsiderBulkSourceIdentity(
                source_quarter="2025Q4",
                zip_url=NEW_ZIP_URL,
                zip_sha256="F" * 64,
            ),
            lambda: insider_pipeline.InsiderBulkTableEvidence(
                table_name="SUBMISSION",
                headers=("ACCESSION_NUMBER", "ACCESSION_NUMBER"),
                row_count=1,
                selected_row_count=1,
            ),
            lambda: insider_pipeline.InsiderBulkAccessionEvidence(
                accession_number=ACCESSION,
                issuer_cik="1",
                form_type="4",
                filing_date="2025-12-15",
                reporting_owner_ciks=(),
                table_row_counts=(("SUBMISSION", 1),),
            ),
            lambda: insider_pipeline.InsiderBulkAccessionEvidence(
                accession_number=ACCESSION,
                issuer_cik="0000000001",
                form_type="4",
                filing_date="2025-02-30",
                reporting_owner_ciks=(),
                table_row_counts=(("SUBMISSION", 1),),
            ),
            lambda: insider_pipeline.InsiderBulkAccessionEvidence(
                accession_number=ACCESSION,
                issuer_cik="0000000001",
                form_type="4",
                filing_date="2025-12-15",
                reporting_owner_ciks=("0000000004", "0000000003"),
                table_row_counts=(("REPORTINGOWNER", 2), ("SUBMISSION", 1)),
            ),
            lambda: insider_pipeline.InsiderBulkAccessionEvidence(
                accession_number=ACCESSION,
                issuer_cik="0000000001",
                form_type="4",
                filing_date="2025-12-15",
                reporting_owner_ciks=("0000000003",),
                table_row_counts=(("REPORTINGOWNER", 2), ("SUBMISSION", 1)),
            ),
        ):
            with self.subTest(factory=factory), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ):
                factory()

        selected_rows = tuple(
            (f"0000000001-25-{index:06d}", "15-DEC-2025", "4", "1")
            for index in range(1, 4)
        )
        submission = _tsv(
            ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK"),
            selected_rows,
        )
        with patch.object(
            insider_pipeline,
            "MAX_INSIDER_BULK_SELECTED_ACCESSIONS",
            2,
        ):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                self.fetch(_zip_bytes([("SUBMISSION.tsv", submission)]))

    def test_public_record_constructor_failures_drop_hostile_exception_chains(self) -> None:
        secret = "TASK7_CONSTRUCTOR_SECRET"
        factories = (
            lambda: insider_pipeline.InsiderBulkSourceIdentity(
                source_quarter="2025Q4",
                zip_url=(
                    f"https://www.sec.gov:{secret}/files/x/"
                    "insider-transactions-data-sets/2025q4.zip"
                ),
                zip_sha256="0" * 64,
            ),
            lambda: insider_pipeline.InsiderBulkAccessionEvidence(
                accession_number=ACCESSION,
                issuer_cik=secret,
                form_type="4",
                filing_date="2025-12-15",
                reporting_owner_ciks=(),
                table_row_counts=(("SUBMISSION", 1),),
            ),
        )
        for factory in factories:
            with self.subTest(factory=factory), self.assertRaises(
                insider_pipeline.InsiderBackfillError
            ) as raised:
                factory()
            chain = [
                str(item)
                for item in (
                    raised.exception,
                    raised.exception.__cause__,
                    raised.exception.__context__,
                )
                if item is not None
            ]
            self.assertNotIn(secret, " ".join(chain))
            self.assertIsNone(raised.exception.__cause__)
            self.assertIsNone(raised.exception.__context__)

    def test_tampered_expected_source_is_revalidated_before_http(self) -> None:
        expected = insider_pipeline.InsiderBulkSourceIdentity(
            source_quarter="2025Q4",
            zip_url=NEW_ZIP_URL,
            zip_sha256="0" * 64,
        )
        object.__setattr__(expected, "zip_sha256", "F" * 64)
        response = _BulkResponse(_zip_bytes([("SUBMISSION.tsv", _submission())]))
        http = _BulkHTTP(response)

        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline.fetch_insider_bulk_archive(
                self.catalog(),
                approved_issuer_ciks=("1",),
                http=http,
                expected_source=expected,
            )

        self.assertEqual([], http.calls)
        self.assertEqual(0, response.close_calls)

    def test_tampered_nested_archive_evidence_is_revalidated(self) -> None:
        result, _, _, _ = self.fetch(_zip_bytes(_valid_tables()))
        object.__setattr__(
            result.table_evidence[0],
            "headers",
            ("ACCESSION_NUMBER", "ACCESSION_NUMBER"),
        )
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline._validate_bulk_result(result)

        result, _, _, _ = self.fetch(_zip_bytes(_valid_tables()))
        object.__setattr__(result.selected_accessions[0], "form_type", "X")
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline._validate_bulk_result(result)

    def test_approved_issuer_input_is_bounded_before_http(self) -> None:
        consumed = 0

        def issuers():
            nonlocal consumed
            for index in range(insider_pipeline.MAX_INSIDER_STATE_COLLECTION + 2):
                consumed += 1
                yield index + 1

        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        response = _BulkResponse(payload)
        http = _BulkHTTP(response)
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline.fetch_insider_bulk_archive(
                self.catalog(),
                approved_issuer_ciks=issuers(),
                http=http,
            )
        self.assertEqual(insider_pipeline.MAX_INSIDER_STATE_COLLECTION + 1, consumed)
        self.assertEqual([], http.calls)

    def test_future_quarter_is_rejected_before_archive_http(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2026Q2",
            catalog_url=CATALOG_URL,
            zip_url=(
                "https://www.sec.gov/files/structureddata/data/"
                "insider-transactions-data-sets/2026q2.zip"
            ),
        )
        response = _BulkResponse(b"not requested", url=entry.zip_url)
        http = _BulkHTTP(response)
        with self.assertRaises(insider_pipeline.InsiderBackfillError):
            insider_pipeline.fetch_insider_bulk_archive(
                entry,
                approved_issuer_ciks=("1",),
                http=http,
                as_of=datetime(2026, 2, 1, tzinfo=timezone.utc),
            )
        self.assertEqual([], http.calls)
        self.assertEqual(0, response.close_calls)

    def test_control_flow_interrupt_propagates_but_still_closes_and_removes_temp(self) -> None:
        payload = _zip_bytes([("SUBMISSION.tsv", _submission())])
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        response = _BulkResponse(
            payload,
            temp_directory=root,
            stream_error=KeyboardInterrupt(),
        )
        with self.assertRaises(KeyboardInterrupt):
            insider_pipeline.fetch_insider_bulk_archive(
                self.catalog(),
                approved_issuer_ciks=("1",),
                http=_BulkHTTP(response),
                temp_directory=root,
            )
        self.assertEqual(1, response.close_calls)
        self.assertEqual([], list(root.iterdir()))


def _backfill_archive_result(
    accessions: tuple[str, ...] = (ACCESSION,),
    *,
    zip_sha256: str = "a" * 64,
) -> insider_pipeline.InsiderBulkArchiveResult:
    selected = tuple(
        insider_pipeline.InsiderBulkAccessionEvidence(
            accession_number=accession,
            issuer_cik="0000000001",
            form_type="4",
            filing_date="2025-12-15",
            reporting_owner_ciks=(),
            table_row_counts=(("SUBMISSION", 1),),
        )
        for accession in sorted(accessions)
    )
    return insider_pipeline.InsiderBulkArchiveResult(
        source_quarter="2025Q4",
        catalog_url=CATALOG_URL,
        zip_url=NEW_ZIP_URL,
        zip_sha256=zip_sha256,
        zip_byte_count=123,
        etag='"synthetic-etag"',
        last_modified="Sun, 06 Nov 1994 08:49:37 GMT",
        table_evidence=(
            insider_pipeline.InsiderBulkTableEvidence(
                table_name="SUBMISSION",
                headers=(
                    "ACCESSION_NUMBER",
                    "FILING_DATE",
                    "DOCUMENT_TYPE",
                    "ISSUERCIK",
                ),
                row_count=len(selected),
                selected_row_count=len(selected),
            ),
        ),
        missing_optional_tables=tuple(sorted(ALL_TABLES[1:])),
        selected_accessions=selected,
    )


class InsiderBackfillOrchestrationTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.storage = insider_storage.InsiderStorage(self.root)
        self.state_store = insider_storage.InsiderStateStore(self.root)
        self.state_store.write(
            "approved-issuers-v1",
            {"contract_version": 1, "issuer_ciks": ["0000000001"]},
        )
        self.deadline = insider_pipeline.CooperativeDeadline(
            started_monotonic=0.0,
            deadline_seconds=30,
        )

    def test_plan_only_discovers_bounded_evidence_without_mutation(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result((ACCESSION, CARRYOVER_ACCESSION))
        http = object()
        monotonic = lambda: 0.0

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ) as catalog_fetch,
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ) as archive_fetch,
            patch.object(insider_pipeline, "process_insider_accession") as processor,
        ):
            result = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=1,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                plan_only=True,
                resume=False,
                http=http,
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=monotonic,
            )

        self.assertEqual("planned", result.outcome.value)
        self.assertEqual("2025Q4", result.quarter)
        self.assertEqual("0000000001", result.issuer_cik)
        self.assertEqual((CARRYOVER_ACCESSION,), result.selected_accessions)
        self.assertEqual((), result.completed_accessions)
        catalog_fetch.assert_called_once()
        archive_fetch.assert_called_once()
        self.assertEqual(
            30.0,
            catalog_fetch.call_args.kwargs["deadline_monotonic"],
        )
        self.assertEqual(
            30.0,
            archive_fetch.call_args.kwargs["deadline_monotonic"],
        )
        self.assertIs(
            monotonic,
            catalog_fetch.call_args.kwargs["monotonic"],
        )
        self.assertIs(
            monotonic,
            archive_fetch.call_args.kwargs["monotonic"],
        )
        self.assertEqual(
            ("0000000001",),
            archive_fetch.call_args.kwargs["approved_issuer_ciks"],
        )
        processor.assert_not_called()
        with self.assertRaises(FileNotFoundError):
            self.state_store.read("backfill/2025Q4")

    def test_plan_only_rejects_deadline_reached_during_archive_fetch(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result()
        readings = iter((0.0, 0.0, 30.0))

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ),
        ):
            with self.assertRaisesRegex(
                insider_pipeline.InsiderBackfillError,
                "deadline",
            ):
                insider_pipeline.run_insider_backfill(
                    issuer_cik="1",
                    quarter="2025Q4",
                    max_accessions=1,
                    deadline=self.deadline,
                    storage=self.storage,
                    state_store=self.state_store,
                    plan_only=True,
                    http=object(),
                    as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                    monotonic=lambda: next(readings),
                )

        with self.assertRaises(FileNotFoundError):
            self.state_store.read("backfill/2025Q4")

    def test_unapproved_issuer_is_rejected_before_source_fetch(self) -> None:
        with (
            patch.object(insider_pipeline, "fetch_insider_bulk_catalog") as catalog_fetch,
            patch.object(insider_pipeline, "fetch_insider_bulk_archive") as archive_fetch,
        ):
            with self.assertRaises(insider_pipeline.InsiderBackfillError):
                insider_pipeline.run_insider_backfill(
                    issuer_cik="2",
                    quarter="2025Q4",
                    max_accessions=1,
                    deadline=self.deadline,
                    storage=self.storage,
                    state_store=self.state_store,
                    plan_only=True,
                    http=object(),
                    as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                    monotonic=lambda: 0.0,
                )

        catalog_fetch.assert_not_called()
        archive_fetch.assert_not_called()

    def test_source_evidence_is_checkpointed_before_filing_processing(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result()
        observed_states: list[dict[str, object]] = []

        def process(
            evidence: insider_pipeline.InsiderBulkAccessionEvidence,
            **_: object,
        ) -> insider_pipeline.InsiderAccessionProcessResult:
            self.assertEqual(ACCESSION, evidence.accession_number)
            observed_states.append(self.state_store.read("backfill/2025Q4"))
            return insider_pipeline.InsiderAccessionProcessResult(
                accession_number=ACCESSION,
                issuer_cik="0000000001",
                form_type="4",
                parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
                outcome=insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
                stage="index",
                error_class="ConnectionError",
                reason_code="fetch_failed",
            )

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ),
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                side_effect=process,
                create=True,
            ) as processor,
        ):
            result = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=1,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )

        processor.assert_called_once()
        self.assertEqual(1, len(observed_states))
        checkpoint = observed_states[0]
        self.assertEqual("running", checkpoint["status"])
        self.assertEqual(CATALOG_URL, checkpoint["catalog_url"])
        self.assertEqual(NEW_ZIP_URL, checkpoint["zip_url"])
        self.assertEqual("a" * 64, checkpoint["zip_sha256"])
        self.assertEqual(123, checkpoint["zip_byte_count"])
        self.assertEqual([ACCESSION], checkpoint["selected_accessions"])
        self.assertEqual([], checkpoint["completed_accessions"])
        self.assertEqual("checkpointed", result.outcome.value)
        persisted = self.state_store.read("backfill/2025Q4")
        self.assertEqual("incomplete", persisted["status"])
        self.assertEqual([], persisted["completed_accessions"])

    def test_success_checkpoints_each_accession_and_completes_reconciliation(
        self,
    ) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result((ACCESSION, CARRYOVER_ACCESSION))
        expected_order = tuple(
            evidence.accession_number for evidence in archive.selected_accessions
        )
        observed_completed: list[tuple[str, ...]] = []

        def process(
            evidence: insider_pipeline.InsiderBulkAccessionEvidence,
            **_: object,
        ) -> insider_pipeline.InsiderAccessionProcessResult:
            checkpoint = self.state_store.read("backfill/2025Q4")
            completed = checkpoint["completed_accessions"]
            if not isinstance(completed, list):
                self.fail("completed accession checkpoint must be a list")
            observed_completed.append(tuple(completed))
            return insider_pipeline.InsiderAccessionProcessResult(
                accession_number=evidence.accession_number,
                issuer_cik=evidence.issuer_cik,
                form_type=evidence.form_type,
                parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
                outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
                stage="checkpoint",
            )

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ),
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                side_effect=process,
            ),
        ):
            result = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=2,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )

        self.assertEqual(((), (expected_order[0],)), tuple(observed_completed))
        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.COMPLETED, result.outcome)
        self.assertEqual(expected_order, result.selected_accessions)
        self.assertEqual(expected_order, result.completed_accessions)
        persisted = self.state_store.read("backfill/2025Q4")
        self.assertEqual("completed", persisted["status"])
        self.assertEqual(list(expected_order), persisted["completed_accessions"])
        self.assertEqual(
            [
                {
                    "name": "SUBMISSION",
                    "expected_count": 2,
                    "actual_count": 2,
                    "status": "matched",
                }
            ],
            persisted["reconciliation"],
        )

    def test_resume_skips_completed_and_reuses_exact_source_identity(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result((ACCESSION, CARRYOVER_ACCESSION))
        expected_order = tuple(
            evidence.accession_number for evidence in archive.selected_accessions
        )
        processor_calls: list[str] = []
        expected_sources: list[insider_pipeline.InsiderBulkSourceIdentity | None] = []

        def archive_fetch(
            _: insider_pipeline.InsiderBulkCatalogEntry,
            **kwargs: object,
        ) -> insider_pipeline.InsiderBulkArchiveResult:
            expected = kwargs.get("expected_source")
            if expected is not None and not isinstance(
                expected, insider_pipeline.InsiderBulkSourceIdentity
            ):
                self.fail("expected source must use the public source identity")
            expected_sources.append(expected)
            return archive

        def process(
            evidence: insider_pipeline.InsiderBulkAccessionEvidence,
            **_: object,
        ) -> insider_pipeline.InsiderAccessionProcessResult:
            processor_calls.append(evidence.accession_number)
            if len(processor_calls) == 1:
                outcome = insider_pipeline.InsiderAccessionOutcome.CREATED
                stage = "checkpoint"
                error_class = None
                reason_code = None
            elif len(processor_calls) == 2:
                outcome = insider_pipeline.InsiderAccessionOutcome.RETRY_LATER
                stage = "index"
                error_class = "ConnectionError"
                reason_code = "fetch_failed"
            else:
                outcome = insider_pipeline.InsiderAccessionOutcome.CACHE_HIT
                stage = "cache"
                error_class = None
                reason_code = None
            return insider_pipeline.InsiderAccessionProcessResult(
                accession_number=evidence.accession_number,
                issuer_cik=evidence.issuer_cik,
                form_type=evidence.form_type,
                parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
                outcome=outcome,
                stage=stage,
                error_class=error_class,
                reason_code=reason_code,
            )

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                side_effect=archive_fetch,
            ),
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                side_effect=process,
            ),
        ):
            first = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=2,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )
            first_state = self.state_store.read("backfill/2025Q4")
            second = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=2,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                resume=True,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )

        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.CHECKPOINTED, first.outcome)
        self.assertEqual([expected_order[0]], first_state["completed_accessions"])
        self.assertEqual(
            [expected_order[0], expected_order[1], expected_order[1]],
            processor_calls,
        )
        self.assertIsNone(expected_sources[0])
        self.assertEqual(
            insider_pipeline.InsiderBulkSourceIdentity(
                source_quarter="2025Q4",
                zip_url=NEW_ZIP_URL,
                zip_sha256="a" * 64,
            ),
            expected_sources[1],
        )
        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.COMPLETED, second.outcome)
        self.assertEqual(expected_order, second.completed_accessions)
        self.assertEqual(
            list(expected_order),
            self.state_store.read("backfill/2025Q4")["completed_accessions"],
        )

    def test_resume_preserves_initial_selection_when_bound_increases(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result((ACCESSION, CARRYOVER_ACCESSION))
        originally_selected = archive.selected_accessions[0].accession_number
        processor_calls: list[str] = []

        def process(
            evidence: insider_pipeline.InsiderBulkAccessionEvidence,
            **_: object,
        ) -> insider_pipeline.InsiderAccessionProcessResult:
            processor_calls.append(evidence.accession_number)
            if len(processor_calls) == 1:
                return insider_pipeline.InsiderAccessionProcessResult(
                    accession_number=evidence.accession_number,
                    issuer_cik=evidence.issuer_cik,
                    form_type=evidence.form_type,
                    parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
                    outcome=insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
                    stage="index",
                    error_class="ConnectionError",
                    reason_code="fetch_failed",
                )
            return insider_pipeline.InsiderAccessionProcessResult(
                accession_number=evidence.accession_number,
                issuer_cik=evidence.issuer_cik,
                form_type=evidence.form_type,
                parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
                outcome=insider_pipeline.InsiderAccessionOutcome.CACHE_HIT,
                stage="cache",
            )

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ),
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                side_effect=process,
            ),
        ):
            first = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=1,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )
            second = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=2,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                resume=True,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )

        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.CHECKPOINTED, first.outcome)
        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.COMPLETED, second.outcome)
        self.assertEqual((originally_selected,), second.selected_accessions)
        self.assertEqual((originally_selected,), second.completed_accessions)
        self.assertEqual([originally_selected, originally_selected], processor_calls)
        persisted = self.state_store.read("backfill/2025Q4")
        self.assertEqual("completed", persisted["status"])
        self.assertEqual([originally_selected], persisted["selected_accessions"])
        self.assertEqual([originally_selected], persisted["completed_accessions"])

    def test_resume_smaller_bound_rejects_without_fetch_or_checkpoint_mutation(
        self,
    ) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result((ACCESSION, CARRYOVER_ACCESSION))

        def retry(
            evidence: insider_pipeline.InsiderBulkAccessionEvidence,
            **_: object,
        ) -> insider_pipeline.InsiderAccessionProcessResult:
            return insider_pipeline.InsiderAccessionProcessResult(
                accession_number=evidence.accession_number,
                issuer_cik=evidence.issuer_cik,
                form_type=evidence.form_type,
                parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
                outcome=insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
                stage="index",
                error_class="ConnectionError",
                reason_code="fetch_failed",
            )

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ),
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                side_effect=retry,
            ),
        ):
            first = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=2,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )

        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.CHECKPOINTED, first.outcome)
        before = self.state_store.read("backfill/2025Q4")
        with (
            patch.object(insider_pipeline, "fetch_insider_bulk_catalog") as catalog_fetch,
            patch.object(insider_pipeline, "fetch_insider_bulk_archive") as archive_fetch,
        ):
            with self.assertRaisesRegex(
                insider_pipeline.InsiderBackfillError,
                "accession bound",
            ):
                insider_pipeline.run_insider_backfill(
                    issuer_cik="1",
                    quarter="2025Q4",
                    max_accessions=1,
                    deadline=self.deadline,
                    storage=self.storage,
                    state_store=self.state_store,
                    resume=True,
                    http=object(),
                    as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                    monotonic=lambda: 0.0,
                )

        catalog_fetch.assert_not_called()
        archive_fetch.assert_not_called()
        self.assertEqual(before, self.state_store.read("backfill/2025Q4"))

    def test_resume_rejects_changed_source_without_reprocessing(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        original = _backfill_archive_result()
        revised = _backfill_archive_result(zip_sha256="b" * 64)
        retry = insider_pipeline.InsiderAccessionProcessResult(
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            form_type="4",
            parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.RETRY_LATER,
            stage="index",
            error_class="ConnectionError",
            reason_code="fetch_failed",
        )

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                side_effect=(original, revised),
            ) as archive_fetch,
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                return_value=retry,
            ) as processor,
        ):
            first = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=1,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )
            with self.assertRaises(insider_pipeline.InsiderBulkSourceRevisionError):
                insider_pipeline.run_insider_backfill(
                    issuer_cik="1",
                    quarter="2025Q4",
                    max_accessions=1,
                    deadline=self.deadline,
                    storage=self.storage,
                    state_store=self.state_store,
                    resume=True,
                    http=object(),
                    as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                    monotonic=lambda: 0.0,
                )

        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.CHECKPOINTED, first.outcome)
        self.assertEqual(1, processor.call_count)
        expected_source = archive_fetch.call_args_list[1].kwargs["expected_source"]
        self.assertEqual("a" * 64, expected_source.zip_sha256)
        persisted = self.state_store.read("backfill/2025Q4")
        self.assertEqual("a" * 64, persisted["zip_sha256"])
        self.assertEqual("quarantined", persisted["status"])

    def test_deterministic_processor_quarantine_marks_quarter_without_completion(
        self,
    ) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result()
        quarantined = insider_pipeline.InsiderAccessionProcessResult(
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            form_type="4",
            parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.QUARANTINED,
            stage="index",
            error_class="InsiderIndexParseError",
            reason_code="index_parse_invalid",
        )

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ),
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                return_value=quarantined,
            ),
        ):
            result = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=1,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )

        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.QUARANTINED, result.outcome)
        self.assertEqual((), result.completed_accessions)
        persisted = self.state_store.read("backfill/2025Q4")
        self.assertEqual("quarantined", persisted["status"])
        self.assertEqual([], persisted["completed_accessions"])

    def test_completed_replay_is_idempotent_and_does_not_reprocess(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result()
        created = insider_pipeline.InsiderAccessionProcessResult(
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            form_type="4",
            parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
            stage="checkpoint",
        )
        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ),
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                return_value=created,
            ) as processor,
        ):
            first = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=1,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )
            before = self.state_store.read("backfill/2025Q4")
            replay = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=1,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                resume=True,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )
            after = self.state_store.read("backfill/2025Q4")

        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.COMPLETED, first.outcome)
        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.COMPLETED, replay.outcome)
        self.assertEqual(before, after)
        processor.assert_called_once()

    def test_interruption_preserves_completed_work_for_resume(self) -> None:
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        archive = _backfill_archive_result((ACCESSION, CARRYOVER_ACCESSION))
        expected_order = tuple(
            evidence.accession_number for evidence in archive.selected_accessions
        )
        processor_calls: list[str] = []

        def process(
            evidence: insider_pipeline.InsiderBulkAccessionEvidence,
            **_: object,
        ) -> insider_pipeline.InsiderAccessionProcessResult:
            processor_calls.append(evidence.accession_number)
            if len(processor_calls) == 2:
                raise KeyboardInterrupt
            return insider_pipeline.InsiderAccessionProcessResult(
                accession_number=evidence.accession_number,
                issuer_cik=evidence.issuer_cik,
                form_type=evidence.form_type,
                parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
                outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
                stage="checkpoint",
            )

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ),
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                side_effect=process,
            ),
        ):
            with self.assertRaises(KeyboardInterrupt):
                insider_pipeline.run_insider_backfill(
                    issuer_cik="1",
                    quarter="2025Q4",
                    max_accessions=2,
                    deadline=self.deadline,
                    storage=self.storage,
                    state_store=self.state_store,
                    http=object(),
                    as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                    monotonic=lambda: 0.0,
                )
            interrupted = self.state_store.read("backfill/2025Q4")
            resumed = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=2,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                resume=True,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )

        self.assertEqual("running", interrupted["status"])
        self.assertEqual([expected_order[0]], interrupted["completed_accessions"])
        self.assertEqual(
            [expected_order[0], expected_order[1], expected_order[1]],
            processor_calls,
        )
        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.COMPLETED, resumed.outcome)
        self.assertEqual(expected_order, resumed.completed_accessions)

    def test_parser_counts_are_reconciliation_telemetry_not_value_authority(
        self,
    ) -> None:
        raw_fixture = (
            Path(__file__).parent
            / "fixtures"
            / "insider_filings"
            / "form4_simple_purchase.xml"
        )
        index_url = (
            "https://www.sec.gov/Archives/edgar/data/1/000000000125000001/"
            "0000000001-25-000001-index.html"
        )
        document_url = (
            "https://www.sec.gov/Archives/edgar/data/1/000000000125000001/"
            "form4-synthetic.xml"
        )
        normalized = insider_pipeline.parse_ownership_xml(
            raw_fixture.read_bytes(),
            accession_number=ACCESSION,
            filing_date="2025-12-15",
            accepted_at="2025-12-15T16:30:00Z",
            source_index_url=index_url,
            source_document_url=document_url,
        )
        transactions = normalized["transactions"]
        if not isinstance(transactions, list):
            self.fail("normalized transactions must be a list")
        actual_transactions = sum(
            1
            for transaction in transactions
            if isinstance(transaction, dict)
            and transaction.get("source_table") == "non_derivative"
        )
        self.assertGreater(actual_transactions, 0)
        source_transactions = actual_transactions + 1
        selected = insider_pipeline.InsiderBulkAccessionEvidence(
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            form_type="4",
            filing_date="2025-12-15",
            reporting_owner_ciks=(),
            table_row_counts=(
                ("NONDERIV_TRANS", source_transactions),
                ("SUBMISSION", 1),
            ),
        )
        archive = insider_pipeline.InsiderBulkArchiveResult(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
            zip_sha256="a" * 64,
            zip_byte_count=123,
            etag=None,
            last_modified=None,
            table_evidence=(
                insider_pipeline.InsiderBulkTableEvidence(
                    table_name="NONDERIV_TRANS",
                    headers=("ACCESSION_NUMBER", "SYNTHETIC_VALUE"),
                    row_count=source_transactions,
                    selected_row_count=source_transactions,
                ),
                insider_pipeline.InsiderBulkTableEvidence(
                    table_name="SUBMISSION",
                    headers=(
                        "ACCESSION_NUMBER",
                        "FILING_DATE",
                        "DOCUMENT_TYPE",
                        "ISSUERCIK",
                    ),
                    row_count=1,
                    selected_row_count=1,
                ),
            ),
            missing_optional_tables=tuple(
                sorted(set(ALL_TABLES) - {"SUBMISSION", "NONDERIV_TRANS"})
            ),
            selected_accessions=(selected,),
        )
        entry = insider_pipeline.InsiderBulkCatalogEntry(
            source_quarter="2025Q4",
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
        )
        created = insider_pipeline.InsiderAccessionProcessResult(
            accession_number=ACCESSION,
            issuer_cik="0000000001",
            form_type="4",
            parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
            outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
            stage="checkpoint",
        )

        with (
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_catalog",
                return_value=entry,
            ),
            patch.object(
                insider_pipeline,
                "fetch_insider_bulk_archive",
                return_value=archive,
            ),
            patch.object(
                insider_pipeline,
                "process_insider_backfill_accession",
                return_value=created,
            ),
            patch.object(
                self.storage,
                "read_normalized",
                return_value=normalized,
            ) as read_normalized,
        ):
            result = insider_pipeline.run_insider_backfill(
                issuer_cik="1",
                quarter="2025Q4",
                max_accessions=1,
                deadline=self.deadline,
                storage=self.storage,
                state_store=self.state_store,
                http=object(),
                as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                monotonic=lambda: 0.0,
            )

        self.assertEqual(insider_pipeline.InsiderBackfillOutcome.COMPLETED, result.outcome)
        read_normalized.assert_called_once_with(
            ACCESSION,
            insider_pipeline.INSIDER_PARSER_VERSION,
        )
        self.assertEqual(
            [
                {
                    "name": "NONDERIV_TRANS",
                    "expected_count": source_transactions,
                    "actual_count": actual_transactions,
                    "status": "mismatch",
                },
                {
                    "name": "SUBMISSION",
                    "expected_count": 1,
                    "actual_count": 1,
                    "status": "matched",
                },
            ],
            self.state_store.read("backfill/2025Q4")["reconciliation"],
        )


class InsiderBackfillScriptTests(unittest.TestCase):
    @staticmethod
    def valid_arguments() -> list[str]:
        return [
            "--issuer-cik",
            "1",
            "--quarter",
            "2025Q4",
            "--max-accessions",
            "3",
            "--deadline-seconds",
            "60",
            "--plan-only",
        ]

    def test_cli_uses_lock_user_agent_shared_http_and_explicit_scope(self) -> None:
        from scripts import backfill_insider_transactions as backfill_script

        self.assertTrue(hasattr(backfill_script.main, "__wrapped__"))
        planned = insider_pipeline.InsiderBackfillRunResult(
            quarter="2025Q4",
            issuer_cik="0000000001",
            outcome=insider_pipeline.InsiderBackfillOutcome.PLANNED,
            selected_accessions=(),
            completed_accessions=(),
            catalog_url=CATALOG_URL,
            zip_url=NEW_ZIP_URL,
            zip_sha256="a" * 64,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            http = object()
            with (
                patch.object(backfill_script, "ROOT", root),
                patch.object(backfill_script.pipeline, "DATA_DIR", root / "data"),
                patch.object(backfill_script.pipeline, "HTTP", http),
                patch.object(
                    backfill_script.pipeline,
                    "require_declared_sec_user_agent",
                    return_value="Synthetic Agent test@example.com",
                ) as require_ua,
                patch.object(
                    backfill_script,
                    "run_insider_backfill",
                    return_value=planned,
                ) as runner,
            ):
                self.assertEqual(0, backfill_script.main(self.valid_arguments()))
                telemetry = insider_storage.InsiderStateStore(root).read("telemetry-v1")
                recent_runs = telemetry["recent_runs"]
                assert isinstance(recent_runs, list)
                self.assertEqual(1, len(recent_runs))
                run = recent_runs[0]
                assert isinstance(run, dict)
                self.assertEqual("completed", run["status"])
                run_id = run["run_id"]
                assert isinstance(run_id, str)
                self.assertTrue(run_id.startswith("backfill-"))

        require_ua.assert_called_once_with()
        runner.assert_called_once()
        invocation = runner.call_args.kwargs
        self.assertEqual("0000000001", invocation["issuer_cik"])
        self.assertEqual("2025Q4", invocation["quarter"])
        self.assertEqual(3, invocation["max_accessions"])
        self.assertTrue(invocation["plan_only"])
        self.assertFalse(invocation["resume"])
        self.assertIs(http, invocation["http"])
        self.assertIsInstance(invocation["deadline"], insider_pipeline.CooperativeDeadline)

    def test_cli_rejects_missing_or_unbounded_scope_before_live_setup(self) -> None:
        from scripts import backfill_insider_transactions as backfill_script

        cases = (
            [
                "--issuer-cik",
                "1",
                "--max-accessions",
                "3",
                "--deadline-seconds",
                "60",
            ],
            [*self.valid_arguments(), "--resume"],
            [*self.valid_arguments(), "--all-history"],
        )
        with (
            patch.object(
                backfill_script.pipeline,
                "require_declared_sec_user_agent",
            ) as require_ua,
            patch.object(backfill_script, "run_insider_backfill") as runner,
        ):
            for arguments in cases:
                with self.subTest(arguments=arguments):
                    self.assertEqual(2, backfill_script.main(arguments))

        require_ua.assert_not_called()
        runner.assert_not_called()


class InsiderBackfillTelemetryTests(unittest.TestCase):
    def test_plan_summary_counts_source_evidence_without_storing_source_values(self) -> None:
        started = datetime(2026, 1, 20, 12, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 1, 20, 12, 1, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage = insider_storage.InsiderStorage(root)
            state = insider_storage.InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            deadline = insider_pipeline.CooperativeDeadline(
                started_monotonic=0.0,
                deadline_seconds=30,
            )
            catalog = insider_pipeline.InsiderBulkCatalogEntry(
                source_quarter="2025Q4",
                catalog_url=CATALOG_URL,
                zip_url=NEW_ZIP_URL,
            )
            archive = _backfill_archive_result((ACCESSION,))
            with (
                patch.object(
                    insider_pipeline,
                    "fetch_insider_bulk_catalog",
                    return_value=catalog,
                ),
                patch.object(
                    insider_pipeline,
                    "fetch_insider_bulk_archive",
                    return_value=archive,
                ),
                insider_pipeline.insider_telemetry_run(
                    state,
                    run_id="backfill-plan",
                    started_at=started,
                    now=lambda: finished,
                ),
            ):
                result = insider_pipeline.run_insider_backfill(
                    issuer_cik="1",
                    quarter="2025Q4",
                    max_accessions=1,
                    deadline=deadline,
                    storage=storage,
                    state_store=state,
                    plan_only=True,
                    http=object(),
                    as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(insider_pipeline.InsiderBackfillOutcome.PLANNED, result.outcome)
            telemetry = state.read("telemetry-v1")
            counters = telemetry["counters"]
            assert isinstance(counters, dict)
            self.assertEqual(1, counters["backfill_source_quarters"])
            self.assertEqual(1, counters["backfill_source_hashes"])
            self.assertEqual(1, counters["backfill_tables"])
            self.assertEqual(1, counters["backfill_table_evidence"])
            self.assertNotIn("checkpoint_writes", counters)
            self.assertNotIn("checkpoint_failures", counters)
            rendered = repr(telemetry)
            for sentinel in (
                CATALOG_URL,
                NEW_ZIP_URL,
                archive.zip_sha256,
                archive.etag,
                "ACCESSION_NUMBER",
                "SOURCE_VALUE",
                str(root),
            ):
                self.assertNotIn(str(sentinel), rendered)

    def test_completed_run_counts_reconciliation_and_durable_accession_checkpoint(
        self,
    ) -> None:
        started = datetime(2026, 1, 20, 12, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 1, 20, 12, 1, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            storage = insider_storage.InsiderStorage(root)
            state = insider_storage.InsiderStateStore(root)
            state.write(
                "approved-issuers-v1",
                {"contract_version": 1, "issuer_ciks": ["0000000001"]},
            )
            deadline = insider_pipeline.CooperativeDeadline(
                started_monotonic=0.0,
                deadline_seconds=30,
            )
            catalog = insider_pipeline.InsiderBulkCatalogEntry(
                source_quarter="2025Q4",
                catalog_url=CATALOG_URL,
                zip_url=NEW_ZIP_URL,
            )
            archive = _backfill_archive_result((ACCESSION,))
            processed = insider_pipeline.InsiderAccessionProcessResult(
                accession_number=ACCESSION,
                issuer_cik="0000000001",
                form_type="4",
                parser_version=insider_pipeline.INSIDER_PARSER_VERSION,
                outcome=insider_pipeline.InsiderAccessionOutcome.CREATED,
                stage="checkpoint",
            )
            with (
                patch.object(
                    insider_pipeline,
                    "fetch_insider_bulk_catalog",
                    return_value=catalog,
                ),
                patch.object(
                    insider_pipeline,
                    "fetch_insider_bulk_archive",
                    return_value=archive,
                ),
                patch.object(
                    insider_pipeline,
                    "process_insider_backfill_accession",
                    return_value=processed,
                ),
                insider_pipeline.insider_telemetry_run(
                    state,
                    run_id="backfill-completed",
                    started_at=started,
                    now=lambda: finished,
                ),
            ):
                result = insider_pipeline.run_insider_backfill(
                    issuer_cik="1",
                    quarter="2025Q4",
                    max_accessions=1,
                    deadline=deadline,
                    storage=storage,
                    state_store=state,
                    http=object(),
                    as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
                    monotonic=lambda: 0.0,
                )

            self.assertEqual(
                insider_pipeline.InsiderBackfillOutcome.COMPLETED,
                result.outcome,
            )
            telemetry = state.read("telemetry-v1")
            counters = telemetry["counters"]
            assert isinstance(counters, dict)
            self.assertEqual(1, counters["checkpoint_writes"])
            self.assertEqual(1, counters["backfill_reconciliations"])
            self.assertNotIn("checkpoint_failures", counters)


if __name__ == "__main__":
    unittest.main()
