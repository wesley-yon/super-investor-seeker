from __future__ import annotations

import hashlib
import json
import unittest
from datetime import date, datetime
from unittest import mock

import sec_13f_accession_discovery as discovery


CIK = "0001393818"
MAIN_URL = f"https://data.sec.gov/submissions/CIK{CIK}.json"
SHARD_NAME = f"CIK{CIK}-submissions-001.json"
SHARD_URL = f"https://data.sec.gov/submissions/{SHARD_NAME}"
REPORT_DATE = "2025-12-31"
ORIGINAL = "0001193125-26-054623"
AMENDMENT = "0001193125-26-226614"
OLD_ORIGINAL = "0001393818-20-000001"


def table(*rows: tuple[str, str, str, str]) -> dict[str, list[str]]:
    return {
        "form": [row[0] for row in rows],
        "accessionNumber": [row[1] for row in rows],
        "filingDate": [row[2] for row in rows],
        "reportDate": [row[3] for row in rows],
    }


def encoded(document: object) -> bytes:
    return json.dumps(document, sort_keys=True).encode()


def main_fixture(
    *,
    recent: dict | None = None,
    files: list[dict] | None = None,
    cik: object = int(CIK),
) -> bytes:
    return encoded(
        {
            "cik": cik,
            "name": "Example Manager",
            "filings": {
                "recent": recent
                if recent is not None
                else table(
                    ("13F-HR", ORIGINAL, "2026-02-17", REPORT_DATE),
                    ("13F-HR/A", AMENDMENT, "2026-05-15", REPORT_DATE),
                    ("10-K", "0001393818-26-000002", "2026-03-01", "2025-12-31"),
                    ("13F-HR", "0001393818-25-000003", "2025-11-15", "2025-09-30"),
                ),
                "files": [] if files is None else files,
            },
        }
    )


def archive_descriptor(
    *,
    name: str = SHARD_NAME,
    count: int = 1,
    filing_from: str = "2020-02-14",
    filing_to: str = "2020-02-14",
) -> dict:
    return {
        "name": name,
        "filingCount": count,
        "filingFrom": filing_from,
        "filingTo": filing_to,
    }


class FakeResponse:
    def __init__(
        self,
        url: str,
        *,
        content: bytes = b"{}",
        status_code: int = 200,
    ) -> None:
        self.url = url
        self.content = content
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise discovery.requests.HTTPError(
                f"HTTP {self.status_code}",
                response=self,
            )


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


class PureValidationTests(unittest.TestCase):
    def test_normalizers_and_url_builder(self) -> None:
        self.assertEqual(CIK, discovery.normalize_cik(1393818))
        self.assertEqual(CIK, discovery.normalize_cik("1393818"))
        self.assertEqual(REPORT_DATE, discovery.normalize_report_date(REPORT_DATE))
        self.assertEqual(
            REPORT_DATE,
            discovery.normalize_report_date(date(2025, 12, 31)),
        )
        self.assertEqual(MAIN_URL, discovery.build_sec_submissions_url(CIK))
        self.assertEqual(
            SHARD_URL,
            discovery.normalize_sec_submissions_url(SHARD_URL),
        )

    def test_invalid_identifiers_and_dates_fail_closed(self) -> None:
        for value in (True, 0, "", "abc", "12345678901"):
            with self.subTest(cik=value), self.assertRaises(
                discovery.Sec13FAccessionDiscoveryError
            ):
                discovery.normalize_cik(value)
        for value in (
            "2025-02-29",
            "12/31/2025",
            " 2025-12-31",
            datetime(2025, 12, 31),
            None,
        ):
            with self.subTest(report_date=value), self.assertRaises(
                discovery.Sec13FAccessionDiscoveryError
            ):
                discovery.normalize_report_date(value)

    def test_only_exact_sec_submissions_urls_are_accepted(self) -> None:
        unsafe = (
            "http://data.sec.gov/submissions/CIK0001393818.json",
            "https://example.com/submissions/CIK0001393818.json",
            "https://data.sec.gov@evil.test/submissions/CIK0001393818.json",
            "https://data.sec.gov/submissions/../CIK0001393818.json",
            "https://data.sec.gov/submissions/CIK0001393818.json?x=1",
            "https://data.sec.gov/submissions/CIK0001393818.json#fragment",
            "https://data.sec.gov/submissions/random.json",
            "https://www.sec.gov/submissions/CIK0001393818.json",
        )
        for url in unsafe:
            with self.subTest(url=url), self.assertRaises(
                discovery.NonSECSubmissionsURL
            ):
                discovery.normalize_sec_submissions_url(url)


class DiscoveryTests(unittest.TestCase):
    def test_recent_returns_every_target_original_and_amendment(self) -> None:
        result = discovery.discover_form13f_accessions(
            1393818,
            [REPORT_DATE, REPORT_DATE],
            fetcher={MAIN_URL: main_fixture()}.__getitem__,
        )

        self.assertEqual((ORIGINAL, AMENDMENT), result.accessions)
        self.assertEqual((ORIGINAL, AMENDMENT), result.accessions_for(REPORT_DATE))
        self.assertEqual((), result.missing_report_dates)
        self.assertEqual((REPORT_DATE,), result.report_dates)
        self.assertEqual({"13F-HR", "13F-HR/A"}, {
            filing.form_type for filing in result.filings
        })
        self.assertEqual(1, len(result.sources))
        self.assertEqual(4, result.sources[0].row_count)
        self.assertEqual(2, result.sources[0].matched_row_count)
        self.assertEqual(
            hashlib.sha256(main_fixture()).hexdigest(),
            result.sources[0].evidence.sha256,
        )

    def test_archives_are_complete_and_duplicate_evidence_is_merged(self) -> None:
        recent = table(
            ("13F-HR", ORIGINAL, "2026-02-17", REPORT_DATE),
        )
        archive = table(
            # A boundary duplicate may legitimately appear in both documents.
            ("13F-HR", ORIGINAL, "2026-02-17", REPORT_DATE),
            # A late amendment proves why report dates cannot filter shards by
            # their filing-date metadata ranges.
            ("13F-HR/A", AMENDMENT, "2026-05-15", REPORT_DATE),
        )
        payloads = {
            MAIN_URL: main_fixture(
                recent=recent,
                files=[
                    archive_descriptor(
                        count=2,
                        filing_from="2026-02-17",
                        filing_to="2026-05-15",
                    )
                ],
            ),
            SHARD_URL: encoded(archive),
        }

        result = discovery.discover_form13f_accessions(
            CIK,
            REPORT_DATE,
            fetcher=payloads.__getitem__,
        )

        self.assertEqual((ORIGINAL, AMENDMENT), result.accessions)
        self.assertEqual(2, len(result.sources))
        self.assertEqual(2, len(result.filings[0].evidence))
        self.assertEqual(
            {"recent", "archive_shard"},
            {item.kind for item in result.filings[0].evidence},
        )
        self.assertEqual(1, len(result.filings[1].evidence))

    def test_archive_loading_is_explicitly_optional(self) -> None:
        payload = main_fixture(
            recent=table(),
            files=[archive_descriptor()],
        )
        calls: list[str] = []

        def fetch(url: str) -> bytes:
            calls.append(url)
            return payload

        result = discovery.discover_form13f_accessions(
            CIK,
            "2019-12-31",
            include_archive_shards=False,
            fetcher=fetch,
        )

        self.assertEqual([MAIN_URL], calls)
        self.assertEqual((), result.filings)
        self.assertEqual(("2019-12-31",), result.missing_report_dates)

    def test_archive_finds_historical_period(self) -> None:
        archive = table(
            ("13F-HR", OLD_ORIGINAL, "2020-02-14", "2019-12-31"),
        )
        payloads = {
            MAIN_URL: main_fixture(
                recent=table(),
                files=[archive_descriptor()],
            ),
            SHARD_URL: encoded(archive),
        }

        result = discovery.discover_form13f_accessions(
            CIK,
            "2019-12-31",
            fetcher=payloads.__getitem__,
        )

        self.assertEqual((OLD_ORIGINAL,), result.accessions)
        self.assertEqual("archive_shard", result.filings[0].evidence[0].kind)

    def test_expected_checksums_are_verified(self) -> None:
        payload = main_fixture()
        digest = hashlib.sha256(payload).hexdigest()
        result = discovery.discover_form13f_accessions(
            CIK,
            REPORT_DATE,
            fetcher={MAIN_URL: payload}.__getitem__,
            expected_sha256_by_url={MAIN_URL: digest.upper()},
        )
        self.assertEqual(digest, result.sources[0].evidence.sha256)

        with self.assertRaises(discovery.SubmissionsChecksumError):
            discovery.discover_form13f_accessions(
                CIK,
                REPORT_DATE,
                fetcher={MAIN_URL: payload}.__getitem__,
                expected_sha256_by_url={MAIN_URL: "0" * 64},
            )

    def test_result_is_deterministic_and_contains_no_user_agent(self) -> None:
        payload = main_fixture()
        first = discovery.discover_form13f_accessions(
            CIK,
            REPORT_DATE,
            fetcher={MAIN_URL: payload}.__getitem__,
            user_agent="Private Person private@example.test",
        )
        second = discovery.discover_form13f_accessions(
            CIK,
            REPORT_DATE,
            fetcher={MAIN_URL: payload}.__getitem__,
        )
        self.assertEqual(first, second)
        serialized = json.dumps(first.to_dict(), sort_keys=True)
        self.assertNotIn("Private Person", serialized)
        self.assertNotIn("private@example.test", serialized)

    def test_accessions_for_rejects_unrequested_period(self) -> None:
        result = discovery.discover_form13f_accessions(
            CIK,
            REPORT_DATE,
            fetcher={MAIN_URL: main_fixture()}.__getitem__,
        )
        with self.assertRaises(discovery.Sec13FAccessionDiscoveryError):
            result.accessions_for("2025-09-30")

    def test_main_cik_mismatch_and_bad_parallel_arrays_fail_closed(self) -> None:
        cases = (
            main_fixture(cik=1),
            main_fixture(
                recent={
                    "form": ["13F-HR"],
                    "accessionNumber": [ORIGINAL],
                    "filingDate": ["2026-02-17"],
                    "reportDate": [],
                }
            ),
        )
        for payload in cases:
            with self.subTest(payload=payload), self.assertRaises(
                discovery.SubmissionsSchemaError
            ):
                discovery.discover_form13f_accessions(
                    CIK,
                    REPORT_DATE,
                    fetcher={MAIN_URL: payload}.__getitem__,
                )

    def test_invalid_accession_date_and_noncanonical_form_fail_closed(self) -> None:
        bad_rows = (
            ("13F-HR", "bad", "2026-02-17", REPORT_DATE),
            ("13F-HR", ORIGINAL, "2025-02-17", REPORT_DATE),
            ("13F-HR", ORIGINAL, "2026-02-30", REPORT_DATE),
            ("13f-hr", ORIGINAL, "2026-02-17", REPORT_DATE),
            ("13F-HR", ORIGINAL, "2026-02-17", "12/31/2025"),
        )
        for bad_row in bad_rows:
            with self.subTest(row=bad_row), self.assertRaises(
                discovery.SubmissionsSchemaError
            ):
                discovery.discover_form13f_accessions(
                    CIK,
                    REPORT_DATE,
                    fetcher={
                        MAIN_URL: main_fixture(recent=table(bad_row))
                    }.__getitem__,
                )

    def test_unrelated_form_accession_anomaly_does_not_block_13f(self) -> None:
        recent = table(
            ("X-17A-5", "9999999997-14-013256", "2011-03-01", "2010-12-31"),
            ("13F-HR", ORIGINAL, "2026-02-17", REPORT_DATE),
        )

        result = discovery.discover_form13f_accessions(
            CIK,
            REPORT_DATE,
            fetcher={MAIN_URL: main_fixture(recent=recent)}.__getitem__,
        )

        self.assertEqual((ORIGINAL,), result.accessions)
        self.assertEqual(2, result.sources[0].row_count)

    def test_out_of_scope_historical_13f_accession_anomaly_does_not_block(
        self,
    ) -> None:
        recent = table(
            ("13F-HR", "9999999997-05-015772", "1999-02-11", "1998-12-31"),
            ("13F-HR", ORIGINAL, "2026-02-17", REPORT_DATE),
        )

        result = discovery.discover_form13f_accessions(
            CIK,
            REPORT_DATE,
            fetcher={MAIN_URL: main_fixture(recent=recent)}.__getitem__,
        )

        self.assertEqual((ORIGINAL,), result.accessions)
        self.assertEqual(2, result.sources[0].row_count)

    def test_unhashable_report_date_is_a_schema_error_not_a_type_error(self) -> None:
        malformed = {
            "form": ["10-K"],
            "accessionNumber": ["0001393818-26-000002"],
            "filingDate": ["2026-03-01"],
            "reportDate": [["2025-12-31"]],
        }
        with self.assertRaises(discovery.SubmissionsSchemaError):
            discovery.discover_form13f_accessions(
                CIK,
                REPORT_DATE,
                fetcher={
                    MAIN_URL: main_fixture(recent=malformed)
                }.__getitem__,
            )

    def test_archive_metadata_must_be_cik_bound_and_unique(self) -> None:
        bad_files = (
            [archive_descriptor(name="../escape.json")],
            [archive_descriptor(name="CIK0000000001-submissions-001.json")],
            [archive_descriptor(), archive_descriptor()],
            [archive_descriptor(count=-1)],
            [archive_descriptor(filing_from="2021-01-01", filing_to="2020-01-01")],
        )
        for files in bad_files:
            with self.subTest(files=files), self.assertRaises(
                discovery.SubmissionsSchemaError
            ):
                discovery.discover_form13f_accessions(
                    CIK,
                    REPORT_DATE,
                    fetcher={MAIN_URL: main_fixture(files=files)}.__getitem__,
                )

    def test_archive_count_and_range_must_match_download(self) -> None:
        archive = encoded(
            table(("13F-HR", OLD_ORIGINAL, "2020-02-14", "2019-12-31"))
        )
        cases = (
            archive_descriptor(count=2),
            archive_descriptor(
                filing_from="2020-01-01",
                filing_to="2020-02-14",
            ),
        )
        for descriptor in cases:
            payloads = {
                MAIN_URL: main_fixture(recent=table(), files=[descriptor]),
                SHARD_URL: archive,
            }
            with self.subTest(descriptor=descriptor), self.assertRaises(
                discovery.SubmissionsSchemaError
            ):
                discovery.discover_form13f_accessions(
                    CIK,
                    "2019-12-31",
                    fetcher=payloads.__getitem__,
                )

    def test_conflicting_duplicate_accession_fails_closed(self) -> None:
        recent = table(("13F-HR", ORIGINAL, "2026-02-17", REPORT_DATE))
        archive = table(("13F-HR/A", ORIGINAL, "2026-02-17", REPORT_DATE))
        payloads = {
            MAIN_URL: main_fixture(
                recent=recent,
                files=[
                    archive_descriptor(
                        count=1,
                        filing_from="2026-02-17",
                        filing_to="2026-02-17",
                    )
                ],
            ),
            SHARD_URL: encoded(archive),
        }
        with self.assertRaises(discovery.SubmissionsSchemaError):
            discovery.discover_form13f_accessions(
                CIK,
                REPORT_DATE,
                fetcher=payloads.__getitem__,
            )

    def test_empty_scope_nonbytes_and_malformed_json_are_rejected(self) -> None:
        with self.assertRaises(discovery.Sec13FAccessionDiscoveryError):
            discovery.discover_form13f_accessions(
                CIK,
                [],
                fetcher={MAIN_URL: main_fixture()}.__getitem__,
            )
        with self.assertRaises(discovery.SubmissionsFetchError):
            discovery.discover_form13f_accessions(
                CIK,
                REPORT_DATE,
                fetcher=lambda _url: "not bytes",
            )
        with self.assertRaises(discovery.SubmissionsSchemaError):
            discovery.discover_form13f_accessions(
                CIK,
                REPORT_DATE,
                fetcher=lambda _url: b"{",
            )


class HttpFetcherTests(unittest.TestCase):
    def test_user_agent_is_request_only_and_redirects_are_refused(self) -> None:
        private_agent = "Private Person private@example.test"
        session = FakeSession([FakeResponse(MAIN_URL, content=b"payload")])
        fetcher = discovery.make_sec_submissions_fetcher(
            private_agent,
            session=session,
            max_attempts=1,
            requests_per_second=8,
        )

        with mock.patch.object(discovery.time, "sleep"):
            self.assertEqual(b"payload", fetcher(MAIN_URL))
        self.assertEqual(private_agent, session.calls[0][1]["headers"]["User-Agent"])
        self.assertNotIn(private_agent, repr(fetcher))

        redirected = FakeSession(
            [FakeResponse(SHARD_URL, content=b"payload", status_code=200)]
        )
        rejecting_fetcher = discovery.make_sec_submissions_fetcher(
            private_agent,
            session=redirected,
            max_attempts=1,
        )
        with self.assertRaises(discovery.NonSECSubmissionsURL):
            rejecting_fetcher(MAIN_URL)

    def test_non_sec_url_is_rejected_before_network(self) -> None:
        session = FakeSession([])
        fetcher = discovery.make_sec_submissions_fetcher(
            "Test test@example.test",
            session=session,
        )
        with self.assertRaises(discovery.NonSECSubmissionsURL):
            fetcher("https://example.test/submissions.json")
        self.assertEqual([], session.calls)

    def test_retryable_status_is_bounded(self) -> None:
        session = FakeSession(
            [
                FakeResponse(MAIN_URL, status_code=503),
                FakeResponse(MAIN_URL, content=b"ok"),
            ]
        )
        fetcher = discovery.make_sec_submissions_fetcher(
            "Test test@example.test",
            session=session,
            max_attempts=2,
        )
        with mock.patch.object(discovery.time, "sleep"):
            self.assertEqual(b"ok", fetcher(MAIN_URL))
        self.assertEqual(2, len(session.calls))

    def test_invalid_user_agent_does_not_start_a_request(self) -> None:
        session = FakeSession([])
        with self.assertRaises(discovery.Sec13FAccessionDiscoveryError):
            discovery.make_sec_submissions_fetcher(
                "missing contact",
                session=session,
            )
        self.assertEqual([], session.calls)


if __name__ == "__main__":
    unittest.main()
