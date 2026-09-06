from __future__ import annotations

import copy
import io
import json
import stat
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import sec_13f_bulk_backfill as bulk


DATASET_URL = (
    "https://www.sec.gov/files/structureddata/data/"
    "form-13f-data-sets/2026q2_form13f.zip"
)
OLDER_DATASET_URL = (
    "https://www.sec.gov/files/structureddata/data/"
    "form-13f-data-sets/2025q4_form13f.zip"
)
ACCESSION = "0001234567-26-000001"
LEGACY_ACCESSION = "0001234567-04-000001"


SUBMISSION_COLUMNS = [
    "ACCESSION_NUMBER",
    "FILING_DATE",
    "SUBMISSIONTYPE",
    "CIK",
    "PERIODOFREPORT",
]
INFOTABLE_COLUMNS = [
    "ACCESSION_NUMBER",
    "INFOTABLE_SK",
    "NAMEOFISSUER",
    "TITLEOFCLASS",
    "CUSIP",
    "FIGI",
    "VALUE",
    "SSHPRNAMT",
    "SSHPRNAMTTYPE",
    "PUTCALL",
    "INVESTMENTDISCRETION",
    "OTHERMANAGER",
]


def tsv(columns: list[str], rows: list[list[object]]) -> str:
    output = io.StringIO(newline="")
    writer = __import__("csv").writer(output, delimiter="\t", lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)
    return output.getvalue()


def submission_row(
    *,
    accession: str = ACCESSION,
    cik: str = "1234567",
    report_date: str = "30-JUN-2026",
) -> list[object]:
    return [accession, "14-AUG-2026", "13F-HR", cik, report_date]


def information_row(
    *,
    accession: str = ACCESSION,
    infotable_sk: str = "1",
    issuer: str = "APPLE INC",
    security_class: str = "COM",
    cusip: str = "037833100",
    figi: str = "BBG000B9XRY4",
    value: object = "200000",
    shares: object = "1000",
    amount_type: str = "SH",
    put_call: str = "",
) -> list[object]:
    return [
        accession,
        infotable_sk,
        issuer,
        security_class,
        cusip,
        figi,
        value,
        shares,
        amount_type,
        put_call,
        "SOLE",
        "",
    ]


def dataset_zip(
    *,
    submissions: list[list[object]] | None = None,
    information: list[list[object]] | None = None,
    submission_columns: list[str] | None = None,
    information_columns: list[str] | None = None,
    member_prefix: str = "",
) -> bytes:
    output = io.BytesIO()
    prefix = f"{member_prefix}/" if member_prefix else ""
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        if prefix:
            archive.writestr(prefix, b"")
        archive.writestr(
            f"{prefix}SUBMISSION.tsv",
            tsv(
                submission_columns or SUBMISSION_COLUMNS,
                submissions if submissions is not None else [submission_row()],
            ),
        )
        archive.writestr(
            f"{prefix}INFOTABLE.tsv",
            tsv(
                information_columns or INFOTABLE_COLUMNS,
                information if information is not None else [information_row()],
            ),
        )
    return output.getvalue()


def archive_index_fixture(accession: str = LEGACY_ACCESSION) -> bytes:
    return json.dumps({
        "directory": {
            "item": [
                {"name": f"{accession}.txt", "type": "text/plain"},
                {"name": "inftable.txt", "type": "text/plain"},
            ]
        }
    }).encode()


def archive_submission_fixture(
    table: str,
    *,
    accession: str = LEGACY_ACCESSION,
    report_date: str = "20041231",
    filing_date: str = "20050214",
    document_type: str = "13F-HR",
    header_type: str = "13F-HR",
    acceptance_datetime: str | None = None,
    cover: str | None = None,
) -> bytes:
    acceptance_header = (
        f"<ACCEPTANCE-DATETIME>{acceptance_datetime}\n"
        if acceptance_datetime is not None
        else ""
    )
    cover_document = (
        f"""<DOCUMENT>
<TYPE>{document_type}
<SEQUENCE>1
<FILENAME>primary.xml
<DESCRIPTION>FORM 13F COVER PAGE
<TEXT>
{cover}
</TEXT>
</DOCUMENT>
"""
        if cover is not None
        else ""
    )
    return f"""<SEC-DOCUMENT>{accession}.txt
<SEC-HEADER>
<ACCESSION-NUMBER>{accession}
<CONFORMED-SUBMISSION-TYPE>{header_type}
<CONFORMED-PERIOD-OF-REPORT>{report_date}
<FILED-AS-OF-DATE>{filing_date}
{acceptance_header}<FILER><COMPANY-DATA><CENTRAL-INDEX-KEY>0001234567
</SEC-HEADER>
{cover_document}<DOCUMENT>
<TYPE>{document_type}
<SEQUENCE>2
<FILENAME>inftable.txt
<DESCRIPTION>FORM 13F INFORMATION TABLE
<TEXT>
{table}
</TEXT>
</DOCUMENT>
</SEC-DOCUMENT>""".encode()


def archive_xml_table() -> str:
    return """<XML>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>200</value>
    <shrsOrPrnAmt><sshPrnamt>1000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
  </infoTable>
</informationTable>
</XML>"""


def archive_cover(
    *,
    is_amendment: bool = False,
    amendment_type: str | None = None,
    entries: int = 1,
    value: str = "200",
) -> str:
    amendment = (
        f"<amendmentType>{amendment_type}</amendmentType>"
        if amendment_type is not None
        else ""
    )
    return f"""<XML>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
  <headerData><submissionType>13F-HR</submissionType></headerData>
  <formData>
    <coverPage>
      <isAmendment>{str(is_amendment).lower()}</isAmendment>
      {amendment}
    </coverPage>
    <summaryPage>
      <tableEntryTotal>{entries}</tableEntryTotal>
      <tableValueTotal>{value}</tableValueTotal>
    </summaryPage>
  </formData>
</edgarSubmission>
</XML>"""


def archive_legacy_text_table() -> str:
    return """<TABLE>
<C>NAME OF ISSUER<C>TITLE OF CLASS<C>CUSIP<C>VALUE<C>SHARES<C>SH/PRN<C>PUT/CALL
<C>APPLE INC<C>COM<C>037833100<C>200<C>1000<C>SH<C>
</TABLE>"""


def archive_legacy_html_table() -> str:
    return """<table>
<tr><th>Name of Issuer</th><th>Title of Class</th><th>CUSIP</th><th>Value</th><th>Shares</th><th>SH/PRN</th><th>Put/Call</th></tr>
<tr><td>APPLE INC</td><td>COM</td><td>037833100</td><td>200</td><td>1000</td><td>SH</td><td></td></tr>
</table>"""


def archive_legacy_fixed_width_table() -> str:
    return """<TABLE>
                                                           VALUE   SHARES/  SH/ PUT/
        NAME OF ISSUER          TITLE OF CLASS    CUSIP   (x$1000) PRN AMT  PRN CALL
<S>                            <C>              <C>       <C>      <C>      <C> <C>
APPLE INC                      COM              037833100      200  1000.00 SH
                                                           50   250.00 SH
</TABLE>"""


def fund_document(
    *,
    report_date: str = "2026-06-30",
    holding: dict | None = None,
) -> dict:
    return {
        "cik": 1234567,
        "name": "Fixture Manager",
        "quarters": [
            {
                "report_date": report_date,
                "accession": ACCESSION,
                "value_multiplier": 1,
                "holdings": [
                    holding
                    or {
                        "ticker": "CANON",
                        "issuer": "Canonical display issuer",
                        "class": "Canonical display class",
                        "cusip": "037833100",
                        "value": 200000,
                        "shares": 1000,
                        "holding_type": "EQUITY",
                        "share_amount_type": "SH",
                    }
                ],
            }
        ],
    }


class Sec13FBulkDiscoveryTests(unittest.TestCase):
    def test_discovery_keeps_only_official_sec_dataset_links(self) -> None:
        html = f"""
        <a href="{DATASET_URL}">new</a>
        <a href="/files/structureddata/data/form-13f-data-sets/2025q4_form13f.zip">old</a>
        <a href="https://evil.example/2024q1_form13f.zip">evil</a>
        <a href="https://www.sec.gov/files/other/2024q1_form13f.zip">wrong path</a>
        """
        self.assertEqual(
            [OLDER_DATASET_URL, DATASET_URL],
            bulk.discover_13f_dataset_urls(html),
        )

    def test_non_sec_and_lookalike_urls_fail_before_fetch(self) -> None:
        for url in (
            "https://evil.example/files/structureddata/data/form-13f-data-sets/2026q2_form13f.zip",
            "http://www.sec.gov/files/structureddata/data/form-13f-data-sets/2026q2_form13f.zip",
            "https://www.sec.gov.evil.example/files/structureddata/data/form-13f-data-sets/2026q2_form13f.zip",
            "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/not-form13f.zip",
        ):
            with self.subTest(url=url), self.assertRaises(bulk.NonSECDatasetURL):
                bulk.normalize_sec_13f_dataset_url(url)

    def test_accessionless_periods_are_discovered_from_sec_submissions(self) -> None:
        submissions_url = (
            "https://data.sec.gov/submissions/CIK0001234567.json"
        )
        payload = json.dumps({
            "cik": 1234567,
            "filings": {
                "recent": {
                    "form": ["13F-HR", "13F-HR/A", "10-K"],
                    "accessionNumber": [
                        ACCESSION,
                        "0001234567-26-000002",
                        "0001234567-26-000003",
                    ],
                    "filingDate": [
                        "2026-08-14",
                        "2026-08-20",
                        "2026-03-01",
                    ],
                    "reportDate": [
                        "2026-06-30",
                        "2026-06-30",
                        "2025-12-31",
                    ],
                },
                "files": [],
            },
        }).encode()
        result = bulk.discover_archive_fallback_targets_for_periods(
            [{"cik": "1234567", "report_date": "2026-06-30"}],
            fetcher={submissions_url: payload}.__getitem__,
        )

        self.assertEqual([], result["missing"])
        self.assertEqual(
            [ACCESSION, "0001234567-26-000002"],
            [target["accession"] for target in result["targets"]],
        )
        self.assertEqual(submissions_url, result["sources"][0]["url"])
        self.assertEqual(
            __import__("hashlib").sha256(payload).hexdigest(),
            result["sources"][0]["sha256"],
        )

    def test_live_discovery_uses_the_data_sec_specific_paced_fetcher(self) -> None:
        submissions_url = (
            "https://data.sec.gov/submissions/CIK0001234567.json"
        )
        payload = json.dumps({
            "cik": 1234567,
            "filings": {
                "recent": {
                    "form": ["13F-HR"],
                    "accessionNumber": [ACCESSION],
                    "filingDate": ["2026-08-14"],
                    "reportDate": ["2026-06-30"],
                },
                "files": [],
            },
        }).encode()
        fetch = mock.Mock(side_effect={submissions_url: payload}.__getitem__)
        with (
            mock.patch.object(
                bulk,
                "make_sec_submissions_fetcher",
                return_value=fetch,
            ) as submissions_fetcher,
            mock.patch.object(
                bulk,
                "make_sec_fetcher",
                side_effect=AssertionError("www.sec.gov fetcher is invalid here"),
            ) as generic_fetcher,
        ):
            result = bulk.discover_archive_fallback_targets_for_periods(
                [{"cik": "1234567", "report_date": "2026-06-30"}],
                user_agent="Private Contact private@example.test",
            )

        self.assertEqual([ACCESSION], [
            target["accession"] for target in result["targets"]
        ])
        submissions_fetcher.assert_called_once_with(
            "Private Contact private@example.test"
        )
        generic_fetcher.assert_not_called()

    def test_discovery_checkpoint_resumes_after_a_late_cik_failure(self) -> None:
        ciks = ("0001234567", "0007654321")
        accessions = (
            "0001234567-26-000001",
            "0007654321-26-000001",
        )
        urls = {
            cik: f"https://data.sec.gov/submissions/CIK{cik}.json"
            for cik in ciks
        }

        def submissions_payload(cik: str, accession: str) -> bytes:
            return json.dumps({
                "cik": int(cik),
                "filings": {
                    "recent": {
                        "form": ["13F-HR"],
                        "accessionNumber": [accession],
                        "filingDate": ["2026-08-14"],
                        "reportDate": ["2026-06-30"],
                    },
                    "files": [],
                },
            }).encode()

        periods = [
            {"cik": cik, "report_date": "2026-06-30"}
            for cik in ciks
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_path = Path(tmpdir) / "accessions.json"
            first_calls: list[str] = []

            def interrupted(url: str) -> bytes:
                first_calls.append(url)
                if url == urls[ciks[0]]:
                    return submissions_payload(ciks[0], accessions[0])
                raise TimeoutError("transient SEC outage")

            with self.assertRaisesRegex(
                bulk.BulkIndexRefreshError,
                f"CIK {ciks[1]}",
            ):
                bulk.discover_archive_fallback_targets_for_periods(
                    periods,
                    fetcher=interrupted,
                    user_agent="Private Contact private@example.test",
                    checkpoint_path=checkpoint_path,
                )

            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual([ciks[0]], list(checkpoint["completed"]))
            self.assertTrue(bulk._clean_checkpoint_checksum_valid(checkpoint))
            self.assertNotIn(
                "private@example.test",
                checkpoint_path.read_text(encoding="utf-8"),
            )

            resumed_calls: list[str] = []

            def resumed(url: str) -> bytes:
                resumed_calls.append(url)
                if url != urls[ciks[1]]:
                    raise AssertionError("completed CIK must come from checkpoint")
                return submissions_payload(ciks[1], accessions[1])

            result = bulk.discover_archive_fallback_targets_for_periods(
                periods,
                fetcher=resumed,
                checkpoint_path=checkpoint_path,
            )

        self.assertEqual(
            [urls[ciks[0]], urls[ciks[1]]],
            first_calls,
        )
        self.assertEqual([urls[ciks[1]]], resumed_calls)
        self.assertEqual(list(accessions), [
            target["accession"] for target in result["targets"]
        ])

    def test_corrupt_discovery_checkpoint_is_never_reused(self) -> None:
        submissions_url = (
            "https://data.sec.gov/submissions/CIK0001234567.json"
        )
        payload = json.dumps({
            "cik": 1234567,
            "filings": {
                "recent": {
                    "form": ["13F-HR"],
                    "accessionNumber": [ACCESSION],
                    "filingDate": ["2026-08-14"],
                    "reportDate": ["2026-06-30"],
                },
                "files": [],
            },
        }).encode()
        periods = [{"cik": "1234567", "report_date": "2026-06-30"}]
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_path = Path(tmpdir) / "accessions.json"
            bulk.discover_archive_fallback_targets_for_periods(
                periods,
                fetcher={submissions_url: payload}.__getitem__,
                checkpoint_path=checkpoint_path,
            )
            corrupt = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            corrupt["completed"]["0001234567"]["targets"][0][
                "accession"
            ] = "0001234567-26-999999"
            checkpoint_path.write_text(json.dumps(corrupt), encoding="utf-8")
            calls: list[str] = []

            def refetch(url: str) -> bytes:
                calls.append(url)
                return payload

            result = bulk.discover_archive_fallback_targets_for_periods(
                periods,
                fetcher=refetch,
                checkpoint_path=checkpoint_path,
            )

        self.assertEqual([submissions_url], calls)
        self.assertEqual((ACCESSION,), tuple(
            target["accession"] for target in result["targets"]
        ))

    def test_historical_shard_fallback_does_not_refetch_recent_json(self) -> None:
        submissions_url = (
            "https://data.sec.gov/submissions/CIK0001234567.json"
        )
        shard_name = "CIK0001234567-submissions-001.json"
        shard_url = f"https://data.sec.gov/submissions/{shard_name}"
        recent = json.dumps({
            "cik": 1234567,
            "filings": {
                "recent": {
                    "form": [],
                    "accessionNumber": [],
                    "filingDate": [],
                    "reportDate": [],
                },
                "files": [{
                    "name": shard_name,
                    "filingCount": 1,
                    "filingFrom": "2020-02-14",
                    "filingTo": "2020-02-14",
                }],
            },
        }).encode()
        shard = json.dumps({
            "form": ["13F-HR"],
            "accessionNumber": ["0001234567-20-000001"],
            "filingDate": ["2020-02-14"],
            "reportDate": ["2019-12-31"],
        }).encode()
        calls: list[str] = []

        def fetch(url: str) -> bytes:
            calls.append(url)
            return {submissions_url: recent, shard_url: shard}[url]

        result = bulk.discover_archive_fallback_targets_for_periods(
            [{"cik": "1234567", "report_date": "2019-12-31"}],
            fetcher=fetch,
        )

        self.assertEqual([submissions_url, shard_url], calls)
        self.assertEqual(
            ["0001234567-20-000001"],
            [target["accession"] for target in result["targets"]],
        )

    def test_rebuild_reuses_discovery_checkpoint_then_removes_it_on_success(
        self,
    ) -> None:
        submissions_url = (
            "https://data.sec.gov/submissions/CIK0001234567.json"
        )
        submissions = json.dumps({
            "cik": 1234567,
            "filings": {
                "recent": {
                    "form": ["13F-HR"],
                    "accessionNumber": [ACCESSION],
                    "filingDate": ["2026-08-14"],
                    "reportDate": ["2026-06-30"],
                },
                "files": [],
            },
        }).encode()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund = fund_document()
            del fund["quarters"][0]["accession"]
            fund_path = funds_dir / "1234567.json"
            fund_path.write_text(json.dumps(fund), encoding="utf-8")
            rebuild_checkpoint = root / "rebuild.json"
            discovery_checkpoint = (
                bulk._accession_discovery_checkpoint_path(rebuild_checkpoint)
            )
            self.assertEqual(
                root / "rebuild.accession-discovery.json",
                discovery_checkpoint,
            )
            first_calls: list[str] = []

            def interrupted(url: str) -> bytes:
                first_calls.append(url)
                if url == submissions_url:
                    return submissions
                if url == DATASET_URL:
                    raise TimeoutError("SEC bulk archive temporarily unavailable")
                raise AssertionError(f"unexpected URL: {url}")

            with self.assertRaises(bulk.BulkIndexRefreshError):
                bulk.rebuild_reported_identity_from_sec(
                    funds_dir,
                    state_path=root / "state.json",
                    index_dir=root / "indices",
                    checkpoint_path=rebuild_checkpoint,
                    dataset_urls=[DATASET_URL],
                    fetcher=interrupted,
                    user_agent="Private Contact private@example.test",
                )

            self.assertTrue(discovery_checkpoint.is_file())
            checkpoint_text = discovery_checkpoint.read_text(encoding="utf-8")
            self.assertNotIn("private@example.test", checkpoint_text)
            self.assertTrue(bulk._clean_checkpoint_checksum_valid(
                json.loads(checkpoint_text)
            ))
            resumed_calls: list[str] = []

            def resumed(url: str) -> bytes:
                resumed_calls.append(url)
                if url == submissions_url:
                    raise AssertionError(
                        "completed submissions discovery must be checkpointed"
                    )
                if url == DATASET_URL:
                    return dataset_zip()
                raise AssertionError(f"unexpected URL: {url}")

            result = bulk.rebuild_reported_identity_from_sec(
                funds_dir,
                state_path=root / "state.json",
                index_dir=root / "indices",
                checkpoint_path=rebuild_checkpoint,
                dataset_urls=[DATASET_URL],
                fetcher=resumed,
            )

            self.assertEqual(1, result.backfill.holdings_changed)
            self.assertFalse(discovery_checkpoint.exists())
            receipt_path = root / bulk.DEFAULT_COMPLETED_RECEIPT_PATH.name
            self.assertEqual(
                result.completed_rebuild_receipt,
                bulk.load_completed_clean_rebuild_receipt(
                    receipt_path,
                    state_path=root / "state.json",
                    verify_index_checksum=True,
                ),
            )
            self.assertEqual([submissions_url, DATASET_URL], first_calls)
            self.assertEqual([DATASET_URL], resumed_calls)
            persisted = json.loads(fund_path.read_text(encoding="utf-8"))
            self.assertEqual(
                ACCESSION,
                persisted["quarters"][0]["holdings"][0]["accession"],
            )

    def test_index_receipt_survives_fund_apply_failure_and_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "1234567.json"
            fund_path.write_text(json.dumps(fund_document()), encoding="utf-8")
            original_fund = fund_path.read_bytes()
            state_path = root / "state.json"
            index_dir = root / "indices"
            checkpoint_path = root / "rebuild.json"
            receipt_path = root / "completed-receipt.json"

            def interrupt_fund_apply(*_args: object, **_kwargs: object) -> None:
                persisted = json.loads(receipt_path.read_text(encoding="utf-8"))
                self.assertEqual(
                    bulk.COMPLETED_CLEAN_REBUILD_RECEIPT_SCOPE,
                    persisted["receipt_scope"],
                )
                raise bulk.BulkIndexRefreshError("injected fund apply failure")

            def initial_fetch(url: str) -> bytes:
                if url != DATASET_URL:
                    raise AssertionError(f"unexpected URL: {url}")
                return dataset_zip()

            with mock.patch.object(
                bulk,
                "backfill_fund_files",
                side_effect=interrupt_fund_apply,
            ), self.assertRaisesRegex(
                bulk.BulkIndexRefreshError,
                "injected fund apply failure",
            ):
                bulk.rebuild_reported_identity_from_sec(
                    funds_dir,
                    state_path=state_path,
                    index_dir=index_dir,
                    checkpoint_path=checkpoint_path,
                    completed_receipt_path=receipt_path,
                    dataset_urls=[DATASET_URL],
                    fetcher=initial_fetch,
                )

            self.assertEqual(original_fund, fund_path.read_bytes())
            self.assertTrue(
                bulk.reported_identity_backfill_audit(funds_dir)["needed"]
            )
            receipt = bulk.load_completed_clean_rebuild_receipt(
                receipt_path,
                state_path=state_path,
                verify_index_checksum=True,
            )
            self.assertIsNotNone(receipt)
            no_fetch = mock.Mock(
                side_effect=AssertionError("accepted index must be reused")
            )

            resumed = bulk.rebuild_reported_identity_from_sec(
                funds_dir,
                state_path=state_path,
                index_dir=index_dir,
                checkpoint_path=checkpoint_path,
                completed_receipt_path=receipt_path,
                completed_rebuild_receipt=receipt,
                dataset_urls=[DATASET_URL],
                fetcher=no_fetch,
            )

            no_fetch.assert_not_called()
            self.assertEqual(1, resumed.backfill.holdings_changed)
            persisted_fund = json.loads(fund_path.read_text(encoding="utf-8"))
            holding = persisted_fund["quarters"][0]["holdings"][0]
            self.assertEqual("APPLE INC", holding["reported_issuer"])
            self.assertEqual(ACCESSION, holding["accession"])
            self.assertEqual(
                bulk.COMPLETED_CLEAN_REBUILD_RECEIPT_SCOPE,
                resumed.completed_rebuild_receipt["receipt_scope"],
            )

    def test_only_periods_absent_from_index_need_accession_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            result = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=root / "indices",
                dataset_urls=[DATASET_URL],
                target_periods=[("1234567", "2026-06-30")],
                fetcher=lambda _url: dataset_zip(),
                recheck_recent_archives=0,
            )
            missing = bulk._periods_without_index_evidence(
                [
                    {"cik": "1234567", "report_date": "2026-06-30"},
                    {"cik": "1234567", "report_date": "2026-03-31"},
                ],
                state=result.state,
                state_path=state_path,
            )

        self.assertEqual(
            [{"cik": "0001234567", "report_date": "2026-03-31"}],
            missing,
        )

    def test_clean_rebuild_carries_prior_archive_target_for_accessionless_period(
        self,
    ) -> None:
        index_url = bulk.sec_archive_index_url("1234567", LEGACY_ACCESSION)
        submission_url = bulk.sec_archive_submission_url(
            "1234567", LEGACY_ACCESSION
        )
        payloads = {
            DATASET_URL: dataset_zip(submissions=[], information=[]),
            index_url: archive_index_fixture(),
            submission_url: archive_submission_fixture(
                archive_xml_table(),
            ),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            index_dir = root / "indices"
            initial = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[DATASET_URL],
                target_accessions=[LEGACY_ACCESSION],
                target_periods=[("1234567", "2004-12-31")],
                archive_fallback_targets=[{
                    "cik": "1234567",
                    "accession": LEGACY_ACCESSION,
                    "report_date": "2004-12-31",
                }],
                fetcher=payloads.__getitem__,
                full_rebuild=True,
                recheck_recent_archives=0,
            )
            self.assertFalse(initial.errors)
            period = [{"cik": "1234567", "report_date": "2004-12-31"}]
            self.assertEqual(
                [{
                    "cik": "0001234567",
                    "accession": LEGACY_ACCESSION,
                    "report_date": "2004-12-31",
                }],
                bulk._archive_targets_from_existing_index(
                    period,
                    state=initial.state,
                    state_path=state_path,
                ),
            )
            inconsistent = copy.deepcopy(initial.state)
            inconsistent["archive_sources"][submission_url]["cik"] = (
                "0007654321"
            )
            self.assertEqual(
                [{"cik": "0001234567", "report_date": "2004-12-31"}],
                bulk._periods_without_index_evidence(
                    period,
                    state=inconsistent,
                    state_path=state_path,
                ),
            )
            self.assertEqual(
                [],
                bulk._archive_targets_from_existing_index(
                    period,
                    state=inconsistent,
                    state_path=state_path,
                ),
            )

            fund = fund_document(report_date="2004-12-31")
            del fund["quarters"][0]["accession"]
            fund["quarters"][0]["value_multiplier"] = 1000
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "1234567.json"
            fund_path.write_text(json.dumps(fund), encoding="utf-8")
            calls: list[str] = []

            def fetch(url: str) -> bytes:
                calls.append(url)
                return payloads[url]

            with mock.patch.object(
                bulk,
                "discover_archive_fallback_targets_for_periods",
                wraps=bulk.discover_archive_fallback_targets_for_periods,
            ) as discover:
                rebuilt = bulk.rebuild_reported_identity_from_sec(
                    funds_dir,
                    state_path=state_path,
                    index_dir=index_dir,
                    checkpoint_path=root / "checkpoint.json",
                    dataset_urls=[DATASET_URL],
                    fetcher=fetch,
                )

            discover.assert_called_once_with(
                [],
                fetcher=fetch,
                user_agent=None,
                checkpoint_path=root / "checkpoint.accession-discovery.json",
            )
            self.assertEqual(
                [DATASET_URL, index_url, submission_url],
                calls,
            )
            self.assertFalse(rebuilt.refresh.errors)
            self.assertEqual(
                [LEGACY_ACCESSION],
                rebuilt.refresh.state["target_scope"]["accessions"],
            )
            persisted = json.loads(fund_path.read_text(encoding="utf-8"))
            holding = persisted["quarters"][0]["holdings"][0]
            self.assertEqual(LEGACY_ACCESSION, holding["accession"])
            self.assertEqual("APPLE INC", holding["reported_issuer"])

    def test_clean_rebuild_rejects_missing_target_period_before_fund_scan(
        self,
    ) -> None:
        target_period = {
            "cik": "0001234567",
            "report_date": "2026-06-30",
        }
        refresh = bulk.BulkIndexRefreshResult(
            state={"index": {"sha256": "a" * 64}},
            changed=True,
            refreshed_urls=(DATASET_URL,),
            reused_urls=(),
            errors=(),
        )
        enrichment = mock.Mock(
            side_effect=AssertionError(
                "period coverage must fail before archive enrichment"
            )
        )
        backfill = mock.Mock(
            side_effect=AssertionError(
                "period coverage must fail before the all-holdings scan"
            )
        )
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.multiple(
            bulk,
            reported_identity_backfill_audit=mock.Mock(
                return_value={"malformed_files": []}
            ),
            collect_backfill_targets_from_funds=mock.Mock(
                return_value={
                    "holdings_targeted": 1,
                    "periods": [target_period],
                }
            ),
            collect_archive_fallback_targets_from_funds=mock.Mock(
                return_value={"targets": [], "unaddressable": []}
            ),
            load_13f_bulk_index=mock.Mock(return_value=bulk._empty_state()),
            _archive_targets_from_existing_index=mock.Mock(return_value=[]),
            _periods_without_index_evidence=mock.Mock(
                side_effect=[[], [target_period]]
            ),
            discover_archive_fallback_targets_for_periods=mock.Mock(
                return_value={"targets": [], "missing": [], "sources": []}
            ),
            refresh_13f_bulk_index=mock.Mock(return_value=refresh),
            collect_archive_enrichment_targets_from_funds=enrichment,
            backfill_fund_files=backfill,
        ):
            with self.assertRaisesRegex(
                bulk.BulkIndexRefreshError,
                "clean index omitted 1 nonempty retained period",
            ):
                bulk.rebuild_reported_identity_from_sec(
                    Path(tmpdir) / "funds",
                    state_path=Path(tmpdir) / "state.json",
                    index_dir=Path(tmpdir) / "indices",
                    dataset_urls=[DATASET_URL],
                    fetcher=mock.Mock(),
                )

        enrichment.assert_not_called()
        backfill.assert_not_called()


class Sec13FBulkParserTests(unittest.TestCase):
    def test_decimal_canonicalization_preserves_non_none_zero(self) -> None:
        for value in (0, 0.0, Decimal("0"), Decimal("-0"), "0.000"):
            with self.subTest(value=value):
                self.assertEqual("0", bulk._decimal_text(value, field="value"))
        for value in (None, "", " ", -1, Decimal("NaN"), Decimal("Infinity")):
            with self.subTest(invalid=value):
                with self.assertRaises(bulk.DatasetParseError):
                    bulk._decimal_text(value, field="value")

    def test_parser_preserves_as_filed_identity_and_provenance(self) -> None:
        parsed = bulk.parse_13f_dataset_zip(
            dataset_zip(),
            source_url=DATASET_URL,
        )
        self.assertEqual(1, parsed["submission_count"])
        self.assertEqual(1, parsed["information_table_count"])
        row = parsed["records"][0]
        self.assertEqual("APPLE INC", row["reported_issuer"])
        self.assertEqual("COM", row["reported_class"])
        self.assertEqual("037833100", row["reported_cusip"])
        self.assertEqual("BBG000B9XRY4", row["reported_figi"])
        self.assertEqual(ACCESSION, row["accession"])
        self.assertEqual("2026-06-30", row["report_date"])
        self.assertEqual(DATASET_URL, row["source_url"])
        self.assertEqual(64, len(row["source_sha256"]))

    def test_schema_drift_missing_required_column_is_rejected(self) -> None:
        columns = [name for name in INFOTABLE_COLUMNS if name != "CUSIP"]
        row = information_row()
        row.pop(INFOTABLE_COLUMNS.index("CUSIP"))
        with self.assertRaisesRegex(bulk.DatasetParseError, "CUSIP"):
            bulk.parse_13f_dataset_zip(
                dataset_zip(information_columns=columns, information=[row]),
                source_url=DATASET_URL,
            )

    def test_official_common_top_level_directory_is_accepted(self) -> None:
        parsed = bulk.parse_13f_dataset_zip(
            dataset_zip(member_prefix="01JUN2025-31AUG2025_form13f"),
            source_url=DATASET_URL,
        )
        self.assertEqual(1, parsed["submission_count"])
        self.assertEqual(1, parsed["information_table_count"])

    def test_unsafe_or_ambiguous_nested_member_layouts_are_rejected(self) -> None:
        submissions = tsv(SUBMISSION_COLUMNS, [submission_row()])
        information = tsv(INFOTABLE_COLUMNS, [information_row()])
        cases = {
            "traversal": [
                ("../SUBMISSION.tsv", submissions),
                ("INFOTABLE.tsv", information),
            ],
            "absolute": [
                ("/SUBMISSION.tsv", submissions),
                ("/INFOTABLE.tsv", information),
            ],
            "drive absolute": [
                ("C:/SUBMISSION.tsv", submissions),
                ("C:/INFOTABLE.tsv", information),
            ],
            "deeper nesting": [
                ("outer/inner/SUBMISSION.tsv", submissions),
                ("outer/inner/INFOTABLE.tsv", information),
            ],
            "backslash": [
                ("outer\\SUBMISSION.tsv", submissions),
                ("outer\\INFOTABLE.tsv", information),
            ],
            "mixed root and nested": [
                ("SUBMISSION.tsv", submissions),
                ("outer/INFOTABLE.tsv", information),
            ],
            "different parents": [
                ("outer/SUBMISSION.tsv", submissions),
                ("other/INFOTABLE.tsv", information),
            ],
            "case collision": [
                ("outer/SUBMISSION.tsv", submissions),
                ("outer/submission.TSV", submissions),
                ("outer/INFOTABLE.tsv", information),
            ],
            "required ambiguity": [
                ("SUBMISSION.tsv", submissions),
                ("outer/SUBMISSION.tsv", submissions),
                ("INFOTABLE.tsv", information),
            ],
            "nonempty directory": [
                ("outer/", "not empty"),
                ("outer/SUBMISSION.tsv", submissions),
                ("outer/INFOTABLE.tsv", information),
            ],
            "directory case collision": [
                ("outer/", ""),
                ("OUTER/", ""),
                ("outer/SUBMISSION.tsv", submissions),
                ("outer/INFOTABLE.tsv", information),
            ],
        }
        for label, members in cases.items():
            with self.subTest(label=label):
                output = io.BytesIO()
                with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
                    for name, value in members:
                        archive.writestr(name, value)
                with self.assertRaises(bulk.DatasetParseError):
                    bulk.parse_13f_dataset_zip(
                        output.getvalue(),
                        source_url=DATASET_URL,
                    )

        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("SUBMISSION.tsv", submissions)
            archive.writestr("INFOTABLE.tsv", information)
            for index in range(bulk.MAX_ARCHIVE_MEMBERS - 1):
                archive.writestr(f"EXTRA-{index}/", b"")
        with self.assertRaisesRegex(bulk.DatasetParseError, "unsafe member count"):
            bulk.parse_13f_dataset_zip(
                output.getvalue(),
                source_url=DATASET_URL,
            )

        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
            symlink = zipfile.ZipInfo("outer/SUBMISSION.tsv")
            symlink.create_system = 3
            symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(symlink, "INFOTABLE.tsv")
            archive.writestr("outer/INFOTABLE.tsv", information)
        with self.assertRaises(bulk.DatasetParseError):
            bulk.parse_13f_dataset_zip(
                output.getvalue(),
                source_url=DATASET_URL,
            )

    def test_corrupt_and_unsafe_zips_are_rejected(self) -> None:
        with self.assertRaisesRegex(bulk.DatasetParseError, "invalid"):
            bulk.parse_13f_dataset_zip(b"not a zip", source_url=DATASET_URL)

        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            archive.writestr("../SUBMISSION.tsv", "bad")
            archive.writestr("INFOTABLE.tsv", "bad")
        with self.assertRaisesRegex(bulk.DatasetParseError, "unsafe"):
            bulk.parse_13f_dataset_zip(output.getvalue(), source_url=DATASET_URL)

    def test_conflicting_duplicate_information_key_is_rejected(self) -> None:
        rows = [
            information_row(infotable_sk="1", issuer="APPLE INC"),
            information_row(infotable_sk="1", issuer="NOT APPLE"),
        ]
        with self.assertRaisesRegex(bulk.DatasetParseError, "duplicate or conflicting"):
            bulk.parse_13f_dataset_zip(
                dataset_zip(information=rows),
                source_url=DATASET_URL,
            )


class SecArchiveFallbackParserTests(unittest.TestCase):
    def parse(self, table: str) -> dict:
        return bulk.parse_sec_archive_submission(
            archive_submission_fixture(table),
            cik="0001234567",
            accession=LEGACY_ACCESSION,
            report_date="2004-12-31",
        )

    def test_archive_index_is_bound_to_exact_accession(self) -> None:
        parsed = bulk.parse_sec_archive_index(
            archive_index_fixture(),
            cik="1234567",
            accession=LEGACY_ACCESSION,
        )
        self.assertEqual(64, len(parsed["sha256"]))
        self.assertEqual(
            bulk.sec_archive_submission_url("1234567", LEGACY_ACCESSION),
            parsed["submission_url"],
        )
        with self.assertRaisesRegex(bulk.DatasetParseError, "exactly one"):
            bulk.parse_sec_archive_index(
                archive_index_fixture(ACCESSION),
                cik="1234567",
                accession=LEGACY_ACCESSION,
            )

    def test_archive_xml_is_preferred_and_preserves_exact_fields(self) -> None:
        parsed = self.parse(archive_xml_table())
        self.assertEqual("sec_archive_xml", parsed["method"])
        self.assertEqual(1, parsed["information_table_count"])
        row = parsed["records"][0]
        self.assertEqual("APPLE INC", row["reported_issuer"])
        self.assertEqual("037833100", row["reported_cusip"])
        self.assertEqual("200", row["reported_value"])
        self.assertEqual(LEGACY_ACCESSION, row["accession"])

    def test_archive_cover_and_acceptance_metadata_are_checksum_bound(self) -> None:
        parsed = bulk.parse_sec_archive_submission(
            archive_submission_fixture(
                archive_xml_table(),
                acceptance_datetime="20050214123456",
                cover=archive_cover(),
            ),
            cik="0001234567",
            accession=LEGACY_ACCESSION,
            report_date="2004-12-31",
        )
        self.assertEqual("20050214123456", parsed["acceptance_datetime"])
        self.assertEqual(
            {
                "is_amendment": False,
                "amendment_type": None,
                "table_entry_total": 1,
                "table_value_total": "200",
            },
            parsed["cover_metadata"],
        )
        self.assertTrue(parsed["cover_metadata_consistent"])

    def test_archive_cover_header_conflict_is_retained_but_not_canonical(self) -> None:
        parsed = bulk.parse_sec_archive_submission(
            archive_submission_fixture(
                archive_xml_table(),
                acceptance_datetime="20050214123456",
                cover=archive_cover(
                    is_amendment=True,
                    amendment_type="RESTATEMENT",
                ),
            ),
            cik="0001234567",
            accession=LEGACY_ACCESSION,
            report_date="2004-12-31",
        )
        self.assertFalse(parsed["cover_metadata_consistent"])

    def test_archive_cover_totals_must_reproduce_information_table(self) -> None:
        parsed = bulk.parse_sec_archive_submission(
            archive_submission_fixture(
                archive_xml_table(),
                acceptance_datetime="20050214123456",
                cover=archive_cover(entries=2, value="201"),
            ),
            cik="0001234567",
            accession=LEGACY_ACCESSION,
            report_date="2004-12-31",
        )
        self.assertFalse(parsed["cover_metadata_consistent"])

    def test_archive_xml_preserves_checksum_bound_blank_descriptor(self) -> None:
        parsed = self.parse(
            archive_xml_table().replace(
                "<nameOfIssuer>APPLE INC</nameOfIssuer>",
                "<nameOfIssuer> </nameOfIssuer>",
            )
        )
        row = parsed["records"][0]
        self.assertIn("reported_issuer", row)
        self.assertEqual("", row["reported_issuer"])
        self.assertEqual("COM", row["reported_class"])
        self.assertEqual(64, len(row["source_sha256"]))

    def test_structurally_explicit_legacy_text_and_html_are_supported(self) -> None:
        for table in (
            archive_legacy_text_table(),
            archive_legacy_html_table(),
            archive_legacy_fixed_width_table(),
        ):
            with self.subTest(table=table[:20]):
                parsed = self.parse(table)
                self.assertEqual("sec_archive_legacy_table", parsed["method"])
                self.assertEqual("COM", parsed["records"][0]["reported_class"])

    def test_ambiguous_or_unstructured_legacy_content_fails_closed(self) -> None:
        ambiguous = archive_legacy_html_table() + archive_legacy_html_table()
        with self.assertRaisesRegex(bulk.DatasetParseError, "multiple"):
            self.parse(ambiguous)
        fixed_width_guess = """<TABLE>
APPLE INC  COM  037833100  200  1000 SH
</TABLE>"""
        with self.assertRaisesRegex(bulk.DatasetParseError, "none structurally"):
            self.parse(fixed_width_guess)

    def test_header_identity_conflict_fails_closed(self) -> None:
        payload = archive_submission_fixture(
            archive_xml_table(),
            report_date="20040930",
        )
        with self.assertRaisesRegex(bulk.DatasetParseError, "report date conflicts"):
            bulk.parse_sec_archive_submission(
                payload,
                cik="1234567",
                accession=LEGACY_ACCESSION,
                report_date="2004-12-31",
            )


class FilingChainSelectionTests(unittest.TestCase):
    @staticmethod
    def row(
        accession: str,
        *,
        accepted: str,
        submission_type: str = "13F-HR",
        is_amendment: int = 0,
        amendment_type: str | None = None,
        consistent: int = 1,
        cusip: str = "037833100",
        value: str = "200",
    ) -> dict:
        return {
            "accession": accession,
            "infotable_sk": "1",
            "cik": "0001234567",
            "report_date": "2026-06-30",
            "submission_type": submission_type,
            "reported_issuer": "APPLE INC",
            "reported_class": "COM",
            "reported_cusip": cusip,
            "reported_figi": "BBG000B9XRY4",
            "reported_value": value,
            "reported_shares": "1000",
            "share_amount_type": "SH",
            "put_call": "",
            "investment_discretion": "SOLE",
            "other_manager": None,
            "source_url": DATASET_URL,
            "source_sha256": "a" * 64,
            "acceptance_datetime": accepted,
            "cover_is_amendment": is_amendment,
            "cover_amendment_type": amendment_type,
            "cover_table_entry_total": 1,
            "cover_table_value_total": value,
            "cover_metadata_consistent": consistent,
        }

    def test_latest_coherent_restatement_replaces_original_and_bad_header(self) -> None:
        original = self.row(
            "0001234567-26-000001",
            accepted="20260814090000",
            value="250",
        )
        inconsistent = self.row(
            "0001234567-26-000002",
            accepted="20260814140000",
            is_amendment=1,
            amendment_type="RESTATEMENT",
            consistent=0,
        )
        restatement = self.row(
            "0001234567-26-000003",
            accepted="20260814130000",
            submission_type="13F-HR/A",
            is_amendment=1,
            amendment_type="RESTATEMENT",
        )
        selected = bulk._select_canonical_filing_chain(
            [original, inconsistent, restatement]
        )
        self.assertEqual(
            {"0001234567-26-000003"},
            {row["accession"] for row in selected},
        )

    def test_identical_duplicate_originals_use_latest_exact_evidence(self) -> None:
        early = self.row(
            "0001234567-26-000001",
            accepted="20260814090000",
        )
        late = self.row(
            "0001234567-26-000002",
            accepted="20260814120000",
        )
        selected = bulk._select_canonical_filing_chain([early, late])
        self.assertEqual([late], selected)

    def test_distinct_or_partially_enriched_originals_remain_ambiguous(self) -> None:
        early = self.row(
            "0001234567-26-000001",
            accepted="20260814090000",
        )
        distinct = self.row(
            "0001234567-26-000002",
            accepted="20260814120000",
            cusip="594918104",
        )
        self.assertEqual(
            2,
            len(bulk._select_canonical_filing_chain([early, distinct])),
        )
        partial = dict(distinct, acceptance_datetime=None)
        self.assertEqual(
            2,
            len(bulk._select_canonical_filing_chain([early, partial])),
        )


class Sec13FBulkRefreshTests(unittest.TestCase):
    def prepare_legacy_adoption_fixture(
        self,
        root: Path,
        *,
        dataset_urls: list[str] | None = None,
    ) -> dict[str, object]:
        state_path = root / "state.json"
        index_dir = root / "indices"
        urls = dataset_urls or [DATASET_URL]

        def initial_fetch(url: str) -> bytes:
            if url not in urls:
                raise AssertionError(f"unexpected URL: {url}")
            if url == OLDER_DATASET_URL:
                older_accession = "0001234567-25-000001"
                return dataset_zip(
                    submissions=[submission_row(
                        accession=older_accession,
                        report_date="31-DEC-2025",
                    )],
                    information=[information_row(
                        accession=older_accession,
                    )],
                )
            return dataset_zip()

        initial = bulk.refresh_13f_bulk_index(
            state_path=state_path,
            index_dir=index_dir,
            dataset_urls=urls,
            target_accessions=[],
            target_periods=[("1234567", "2026-06-30")],
            fetcher=initial_fetch,
            recheck_recent_archives=0,
        )
        self.assertFalse(initial.errors)
        self.assertEqual([], initial.state["target_scope"]["accessions"])

        funds_dir = root / "funds"
        funds_dir.mkdir()
        (funds_dir / "1234567.json").write_text(
            json.dumps(fund_document()),
            encoding="utf-8",
        )
        applied = bulk.backfill_fund_files(
            funds_dir,
            state_path=state_path,
            require_all_verified=True,
        )
        self.assertEqual(1, applied.holdings_changed)

        receipt_path = root / "adoption-receipt.json"
        receipt = bulk.prepare_unpublished_legacy_index_adoption(
            funds_dir,
            published_sec_security_state=False,
            state_path=state_path,
            receipt_path=receipt_path,
        )
        archive_targets = [{
            "cik": "0001234567",
            "accession": ACCESSION,
            "report_date": "2026-06-30",
        }]
        return {
            "state_path": state_path,
            "index_dir": index_dir,
            "funds_dir": funds_dir,
            "receipt_path": receipt_path,
            "receipt": receipt,
            "archive_targets": archive_targets,
        }

    def invoke_legacy_adoption(
        self,
        fixture: dict[str, object],
        *,
        receipt: dict[str, object] | None = None,
        dataset_urls: list[str] | None = None,
        target_accessions: list[str] | None = None,
        archive_targets: list[dict[str, str]] | None = None,
        allow: bool = True,
    ) -> tuple[bulk.BulkIndexRefreshResult, mock.Mock]:
        no_fetch = mock.Mock(
            side_effect=TimeoutError("legacy adoption must not fetch")
        )
        result = bulk.refresh_13f_bulk_index(
            state_path=fixture["state_path"],
            index_dir=fixture["index_dir"],
            dataset_urls=dataset_urls or [DATASET_URL],
            target_accessions=(
                [ACCESSION]
                if target_accessions is None
                else target_accessions
            ),
            target_periods=[("1234567", "2026-06-30")],
            archive_fallback_targets=(
                fixture["archive_targets"]
                if archive_targets is None
                else archive_targets
            ),
            clean_rebuild_checkpoint_path=(
                Path(fixture["state_path"]).parent / "checkpoint.json"
            ),
            completed_rebuild_receipt=(
                fixture["receipt"] if receipt is None else receipt
            ),
            allow_unpublished_legacy_index_adoption=allow,
            fetcher=no_fetch,
            full_rebuild=True,
            recheck_recent_archives=0,
        )
        return result, no_fetch

    def test_verified_unpublished_preplan_index_is_adopted_without_fetch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.prepare_legacy_adoption_fixture(Path(tmpdir))
            prepared = bulk.load_13f_bulk_index(fixture["state_path"])
            self.assertEqual(
                [ACCESSION],
                prepared["target_scope"]["accessions"],
            )
            self.assertNotIn("clean_rebuild_plan_sha256", prepared)
            self.assertEqual(
                bulk.LEGACY_INDEX_ADOPTION_RECEIPT_SCOPE,
                fixture["receipt"]["receipt_scope"],
            )

            result, no_fetch = self.invoke_legacy_adoption(fixture)

            self.assertFalse(result.errors)
            self.assertTrue(result.changed)
            no_fetch.assert_not_called()
            self.assertIn("clean_rebuild_plan_sha256", result.state)
            unchanged = copy.deepcopy(result.state)
            unchanged.pop("clean_rebuild_plan_sha256")
            self.assertEqual(prepared, unchanged)
            self.assertFalse(
                bulk.legacy_index_adoption_receipt_matches(
                    fixture["receipt"],
                    state_path=fixture["state_path"],
                    expected_plan_sha256=result.state[
                        "clean_rebuild_plan_sha256"
                    ],
                    dataset_urls=[DATASET_URL],
                    archive_targets=fixture["archive_targets"],
                )
            )
            normal_receipt = bulk.build_completed_clean_rebuild_receipt(
                fixture["state_path"]
            )
            self.assertEqual(
                bulk.COMPLETED_CLEAN_REBUILD_RECEIPT_SCOPE,
                normal_receipt["receipt_scope"],
            )
            with self.assertRaisesRegex(
                bulk.Sec13FBulkError,
                "pre-plan generation",
            ):
                bulk.prepare_unpublished_legacy_index_adoption(
                    fixture["funds_dir"],
                    published_sec_security_state=False,
                    state_path=fixture["state_path"],
                    receipt_path=fixture["receipt_path"],
                )

    def test_legacy_adoption_requires_explicit_permission_and_exact_receipt(
        self,
    ) -> None:
        for mutation in ("permission", "scope", "extra-key"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmpdir:
                fixture = self.prepare_legacy_adoption_fixture(Path(tmpdir))
                receipt = copy.deepcopy(fixture["receipt"])
                allow = True
                if mutation == "permission":
                    allow = False
                elif mutation == "scope":
                    receipt["receipt_scope"] = (
                        bulk.COMPLETED_CLEAN_REBUILD_RECEIPT_SCOPE
                    )
                else:
                    receipt["unexpected"] = True

                state_bytes = Path(fixture["state_path"]).read_bytes()
                result, fetch = self.invoke_legacy_adoption(
                    fixture,
                    receipt=receipt,
                    allow=allow,
                )

                self.assertTrue(result.errors)
                fetch.assert_called_once()
                self.assertEqual(
                    state_bytes,
                    Path(fixture["state_path"]).read_bytes(),
                )

    def test_legacy_adoption_rejects_scope_source_and_archive_mismatches(
        self,
    ) -> None:
        cases = {
            "target-scope": {
                "target_accessions": [],
            },
            "added-source": {
                "dataset_urls": [OLDER_DATASET_URL, DATASET_URL],
            },
            "archive-identity": {
                "archive_targets": [{
                    "cik": "0007654321",
                    "accession": ACCESSION,
                    "report_date": "2026-06-30",
                }],
            },
        }
        for name, kwargs in cases.items():
            with self.subTest(case=name), tempfile.TemporaryDirectory() as tmpdir:
                fixture = self.prepare_legacy_adoption_fixture(Path(tmpdir))
                state_bytes = Path(fixture["state_path"]).read_bytes()

                result, fetch = self.invoke_legacy_adoption(
                    fixture,
                    **kwargs,
                )

                self.assertTrue(result.errors)
                fetch.assert_called_once()
                self.assertEqual(
                    state_bytes,
                    Path(fixture["state_path"]).read_bytes(),
                )

    def test_legacy_adoption_rejects_source_removed_from_current_discovery(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.prepare_legacy_adoption_fixture(
                Path(tmpdir),
                dataset_urls=[OLDER_DATASET_URL, DATASET_URL],
            )
            state_bytes = Path(fixture["state_path"]).read_bytes()

            result, fetch = self.invoke_legacy_adoption(
                fixture,
                dataset_urls=[DATASET_URL],
            )

            self.assertTrue(result.errors)
            fetch.assert_called_once()
            self.assertEqual(
                state_bytes,
                Path(fixture["state_path"]).read_bytes(),
            )

    def test_legacy_adoption_rejects_changed_sqlite_bytes_and_schema(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.prepare_legacy_adoption_fixture(Path(tmpdir))
            state = bulk.load_13f_bulk_index(fixture["state_path"])
            index_path = bulk._index_path_from_state(
                state,
                fixture["state_path"],
            )
            self.assertIsNotNone(index_path)
            with index_path.open("r+b") as handle:
                handle.seek(-1, 2)
                final_byte = handle.read(1)
                handle.seek(-1, 2)
                handle.write(bytes([final_byte[0] ^ 1]))

            result, fetch = self.invoke_legacy_adoption(fixture)

            self.assertTrue(result.errors)
            fetch.assert_called_once()

        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.prepare_legacy_adoption_fixture(Path(tmpdir))
            state = json.loads(
                Path(fixture["state_path"]).read_text(encoding="utf-8")
            )
            state["index"]["schema_version"] = bulk.INDEX_SCHEMA_VERSION + 1
            Path(fixture["state_path"]).write_text(
                json.dumps(state),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                bulk.Sec13FBulkError,
                "unsupported SEC 13F evidence-index schema",
            ):
                bulk.prepare_unpublished_legacy_index_adoption(
                    fixture["funds_dir"],
                    published_sec_security_state=False,
                    state_path=fixture["state_path"],
                    receipt_path=fixture["receipt_path"],
                )

    def test_published_or_incomplete_corpus_cannot_prepare_legacy_adoption(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.prepare_legacy_adoption_fixture(Path(tmpdir))
            with self.assertRaisesRegex(
                bulk.Sec13FBulkError,
                "forbidden for published state",
            ):
                bulk.prepare_unpublished_legacy_index_adoption(
                    fixture["funds_dir"],
                    published_sec_security_state=True,
                    state_path=fixture["state_path"],
                    receipt_path=fixture["receipt_path"],
                )

            fund_path = Path(fixture["funds_dir"]) / "1234567.json"
            fund = json.loads(fund_path.read_text(encoding="utf-8"))
            del fund["quarters"][0]["holdings"][0]["reported_issuer"]
            fund_path.write_text(json.dumps(fund), encoding="utf-8")
            with self.assertRaisesRegex(
                bulk.Sec13FBulkError,
                "complete retained corpus",
            ):
                bulk.prepare_unpublished_legacy_index_adoption(
                    fixture["funds_dir"],
                    published_sec_security_state=False,
                    state_path=fixture["state_path"],
                    receipt_path=fixture["receipt_path"],
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.prepare_legacy_adoption_fixture(Path(tmpdir))
            state_bytes = Path(fixture["state_path"]).read_bytes()
            receipt_bytes = Path(fixture["receipt_path"]).read_bytes()
            fund_path = Path(fixture["funds_dir"]) / "1234567.json"
            fund = json.loads(fund_path.read_text(encoding="utf-8"))
            fund["quarters"][0]["holdings"][0]["reported_issuer"] = (
                "WRONG BUT STRUCTURALLY COMPLETE"
            )
            fund_path.write_text(json.dumps(fund), encoding="utf-8")

            with self.assertRaisesRegex(
                bulk.Sec13FBulkError,
                "full retained-corpus verification",
            ):
                bulk.prepare_unpublished_legacy_index_adoption(
                    fixture["funds_dir"],
                    published_sec_security_state=False,
                    state_path=fixture["state_path"],
                    receipt_path=fixture["receipt_path"],
                )

            self.assertEqual(
                state_bytes,
                Path(fixture["state_path"]).read_bytes(),
            )
            self.assertEqual(
                receipt_bytes,
                Path(fixture["receipt_path"]).read_bytes(),
            )

    def test_legacy_adoption_rejects_sqlite_metadata_schema_mismatch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.prepare_legacy_adoption_fixture(Path(tmpdir))
            state_path = Path(fixture["state_path"])
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            self.assertIsNotNone(index_path)
            connection = __import__("sqlite3").connect(index_path)
            try:
                connection.execute(
                    "UPDATE metadata SET value = ? WHERE key = 'schema_version'",
                    (str(bulk.INDEX_SCHEMA_VERSION + 1),),
                )
                connection.commit()
            finally:
                connection.close()
            state["index"]["sha256"] = bulk._sha256_file(index_path)
            state["index"]["size_bytes"] = index_path.stat().st_size
            state_path.write_text(json.dumps(state), encoding="utf-8")

            with self.assertRaisesRegex(
                bulk.Sec13FBulkError,
                "unsupported SEC 13F SQLite index schema",
            ):
                bulk.prepare_unpublished_legacy_index_adoption(
                    fixture["funds_dir"],
                    published_sec_security_state=False,
                    state_path=state_path,
                    receipt_path=fixture["receipt_path"],
                )

    def test_clean_rebuild_disk_preflight_is_injectable_and_clear(self) -> None:
        usage = type("Usage", (), {"free": 3 * 1024**3})()
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(
                bulk.BulkIndexRefreshError,
                r"3\.0 GiB available.*8\.0 GiB required",
            ):
                bulk.ensure_clean_rebuild_disk_space(
                    index_dir=Path(tmpdir) / "not-created",
                    checkpoint_path=Path(tmpdir) / "checkpoint.json",
                    disk_usage=lambda _path: usage,
                )
            accepted = bulk.ensure_clean_rebuild_disk_space(
                index_dir=Path(tmpdir),
                checkpoint_path=Path(tmpdir) / "checkpoint.json",
                minimum_free_bytes=2 * 1024**3,
                disk_usage=lambda _path: usage,
            )
        self.assertEqual(3 * 1024**3, accepted["available_bytes"])

    def test_live_identity_source_accepts_only_exact_accession_documents(
        self,
    ) -> None:
        live_url = (
            "https://www.sec.gov/Archives/edgar/data/1234567/"
            f"{ACCESSION.replace('-', '')}/infotable.xml"
        )
        self.assertEqual(
            live_url,
            bulk.normalize_sec_identity_source_url(
                live_url,
                accession=ACCESSION,
            ),
        )
        for unsafe in (
            live_url.replace(ACCESSION.replace("-", ""), "0" * 18),
            live_url + "?download=1",
            live_url.replace("www.sec.gov", "sec.gov.example.com"),
        ):
            with self.subTest(url=unsafe):
                with self.assertRaises(bulk.NonSECDatasetURL):
                    bulk.normalize_sec_identity_source_url(
                        unsafe,
                        accession=ACCESSION,
                    )

    def test_disk_preflight_credits_validated_resumable_index(self) -> None:
        usage = type("Usage", (), {"free": 4 * 1024**3})()
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            mock.patch.object(
                bulk,
                "_validated_resumable_partial_bytes",
                return_value=5 * 1024**3,
            ),
        ):
            result = bulk.ensure_clean_rebuild_disk_space(
                index_dir=Path(tmpdir),
                checkpoint_path=Path(tmpdir) / "checkpoint.json",
                disk_usage=lambda _path: usage,
            )
        self.assertEqual(3 * 1024**3, result["minimum_free_bytes"])
        self.assertEqual(5 * 1024**3, result["resumable_bytes"])

    def test_refresh_indexes_only_exact_target_accessions_or_periods(self) -> None:
        other_accession = "0007654321-26-000002"
        payload = dataset_zip(
            submissions=[
                submission_row(),
                submission_row(
                    accession=other_accession,
                    cik="7654321",
                ),
            ],
            information=[
                information_row(),
                information_row(
                    accession=other_accession,
                    infotable_sk="2",
                    cusip="594918104",
                ),
            ],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            result = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=root / "indices",
                dataset_urls=[DATASET_URL],
                target_accessions=[ACCESSION],
                fetcher=lambda _url: payload,
                recheck_recent_archives=0,
            )
            self.assertFalse(result.errors)
            self.assertEqual(1, result.state["summary"]["submissions"])
            self.assertEqual(1, result.state["summary"]["information_table_rows"])
            source = result.state["sources"][DATASET_URL]
            self.assertEqual(2, source["source_submission_count"])
            self.assertEqual(1, source["submission_count"])

    def test_irrelevant_blank_identity_is_counted_but_not_semantically_parsed(
        self,
    ) -> None:
        other_accession = "0007654321-26-000002"
        payload = dataset_zip(
            submissions=[
                submission_row(),
                submission_row(accession=other_accession, cik="7654321"),
            ],
            information=[
                information_row(),
                information_row(
                    accession=other_accession,
                    infotable_sk="2",
                    issuer=" ",
                    security_class="US LRG CAP ETF",
                    cusip="808524201",
                ),
            ],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            result = bulk.refresh_13f_bulk_index(
                state_path=root / "state.json",
                index_dir=root / "indices",
                dataset_urls=[DATASET_URL],
                target_accessions=[ACCESSION],
                fetcher=lambda _url: payload,
                recheck_recent_archives=0,
            )

        self.assertFalse(result.errors)
        source = result.state["sources"][DATASET_URL]
        self.assertEqual(2, source["source_information_table_count"])
        self.assertEqual(1, source["information_table_count"])
        self.assertEqual(1, result.state["summary"]["information_table_rows"])

    def test_retained_blank_descriptor_backfills_exact_empty_once(self) -> None:
        blank_accession = "0001643792-26-000009"
        payload = dataset_zip(
            submissions=[
                submission_row(
                    accession=blank_accession,
                    cik="1643792",
                    report_date="30-JUN-2026",
                )
            ],
            information=[
                information_row(
                    accession=blank_accession,
                    issuer=" ",
                    security_class="COM",
                    cusip="M46528101",
                    value="0",
                    shares="0",
                )
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            result = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=root / "indices",
                dataset_urls=[DATASET_URL],
                target_accessions=[blank_accession],
                fetcher=lambda _url: payload,
                recheck_recent_archives=0,
            )
            self.assertFalse(result.errors)

            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund = fund_document(holding={
                "ticker": "FRO",
                "issuer": "Frontline plc",
                "class": "COM",
                "cusip": "M46528101",
                "value": 0,
                "shares": 0,
                "holding_type": "EQUITY",
                "share_amount_type": "SH",
            })
            fund["cik"] = 1643792
            fund["quarters"][0]["accession"] = blank_accession
            fund_path = funds_dir / "1643792.json"
            fund_path.write_text(json.dumps(fund), encoding="utf-8")

            first = bulk.backfill_fund_files(
                funds_dir,
                state_path=state_path,
                require_all_verified=True,
            )
            first_bytes = fund_path.read_bytes()
            second = bulk.backfill_fund_files(
                funds_dir,
                state_path=state_path,
                require_all_verified=True,
            )
            second_bytes = fund_path.read_bytes()
            persisted = json.loads(second_bytes)
            holding = persisted["quarters"][0]["holdings"][0]
            audit = bulk.reported_identity_backfill_audit(funds_dir)
            verification = bulk.verify_reported_identity_against_sec(
                funds_dir,
                state_path=state_path,
                require_source_provenance=True,
            )

        self.assertEqual(1, first.holdings_changed)
        self.assertEqual(0, second.holdings_changed)
        self.assertEqual(first_bytes, second_bytes)
        self.assertEqual("Frontline plc", holding["issuer"])
        self.assertIn("reported_issuer", holding)
        self.assertEqual("", holding["reported_issuer"])
        self.assertEqual("COM", holding["reported_class"])
        self.assertEqual("M46528101", holding["reported_cusip"])
        self.assertFalse(audit["needed"])
        self.assertTrue(verification.ok)
        self.assertEqual(
            blank_accession,
            persisted["quarters"][0]["reported_identity_sources"][0][
                "accession"
            ],
        )

    def test_checkpoint_resumes_after_irrelevant_blank_identity_was_accepted(
        self,
    ) -> None:
        older_accession = "0001234567-25-000001"
        irrelevant_accession = "0007654321-25-000002"
        older_payload = dataset_zip(
            submissions=[
                submission_row(
                    accession=older_accession,
                    report_date="31-DEC-2025",
                ),
                submission_row(
                    accession=irrelevant_accession,
                    cik="7654321",
                    report_date="31-DEC-2025",
                ),
            ],
            information=[
                information_row(accession=older_accession),
                information_row(
                    accession=irrelevant_accession,
                    infotable_sk="2",
                    issuer=" ",
                    security_class="US LRG CAP ETF",
                    cusip="808524201",
                ),
            ],
        )
        current_payload = dataset_zip()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            index_dir = root / "indices"
            checkpoint_path = root / "checkpoint.json"
            first_calls: list[str] = []

            def interrupted(url: str) -> bytes:
                first_calls.append(url)
                if url == OLDER_DATASET_URL:
                    return older_payload
                raise TimeoutError("late SEC timeout")

            failed = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[OLDER_DATASET_URL, DATASET_URL],
                target_accessions=[older_accession, ACCESSION],
                fetcher=interrupted,
                full_rebuild=True,
                recheck_recent_archives=0,
                clean_rebuild_checkpoint_path=checkpoint_path,
            )
            self.assertTrue(failed.errors)
            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            older_source = checkpoint["sources"][OLDER_DATASET_URL]
            self.assertEqual(2, older_source["source_information_table_count"])
            self.assertEqual(1, older_source["information_table_count"])

            resumed_calls: list[str] = []

            def resumed(url: str) -> bytes:
                resumed_calls.append(url)
                return current_payload

            resumed_result = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[OLDER_DATASET_URL, DATASET_URL],
                target_accessions=[older_accession, ACCESSION],
                fetcher=resumed,
                full_rebuild=True,
                recheck_recent_archives=0,
                clean_rebuild_checkpoint_path=checkpoint_path,
            )

        self.assertFalse(resumed_result.errors)
        self.assertEqual([OLDER_DATASET_URL, DATASET_URL], first_calls)
        self.assertEqual([DATASET_URL], resumed_calls)
        self.assertEqual(2, resumed_result.state["summary"]["submissions"])
        self.assertEqual(
            2,
            resumed_result.state["summary"]["information_table_rows"],
        )

    def test_checkpoint_resumes_after_common_directory_archive(self) -> None:
        older_accession = "0001234567-25-000001"
        older_payload = dataset_zip(
            submissions=[
                submission_row(
                    accession=older_accession,
                    report_date="31-DEC-2025",
                )
            ],
            information=[information_row(accession=older_accession)],
            member_prefix="01JUN2025-31AUG2025_form13f",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            index_dir = root / "indices"
            checkpoint_path = root / "checkpoint.json"

            def interrupted(url: str) -> bytes:
                if url == OLDER_DATASET_URL:
                    return older_payload
                raise TimeoutError("late SEC timeout")

            failed = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[OLDER_DATASET_URL, DATASET_URL],
                target_accessions=[older_accession, ACCESSION],
                fetcher=interrupted,
                full_rebuild=True,
                recheck_recent_archives=0,
                clean_rebuild_checkpoint_path=checkpoint_path,
            )
            self.assertTrue(failed.errors)
            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            self.assertIn(OLDER_DATASET_URL, checkpoint["sources"])

            resumed_calls: list[str] = []

            def resumed(url: str) -> bytes:
                resumed_calls.append(url)
                return dataset_zip()

            resumed_result = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[OLDER_DATASET_URL, DATASET_URL],
                target_accessions=[older_accession, ACCESSION],
                fetcher=resumed,
                full_rebuild=True,
                recheck_recent_archives=0,
                clean_rebuild_checkpoint_path=checkpoint_path,
            )

        self.assertFalse(resumed_result.errors)
        self.assertEqual([DATASET_URL], resumed_calls)
        self.assertEqual(2, resumed_result.state["summary"]["submissions"])

    def test_refresh_is_checksum_backed_and_idempotent(self) -> None:
        payload = dataset_zip()
        calls: list[str] = []

        def fetch(url: str) -> bytes:
            calls.append(url)
            return payload

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            index_dir = root / "indices"
            first = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[DATASET_URL],
                target_accessions=[ACCESSION],
                fetcher=fetch,
                recheck_recent_archives=0,
                refreshed_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
            )
            first_bytes = state_path.read_bytes()
            second = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[DATASET_URL],
                target_accessions=[ACCESSION],
                fetcher=fetch,
                recheck_recent_archives=0,
                refreshed_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
            )

            self.assertTrue(first.changed)
            self.assertFalse(first.errors)
            self.assertFalse(second.changed)
            self.assertEqual(first.state, second.state)
            self.assertEqual(first_bytes, state_path.read_bytes())
            self.assertEqual([DATASET_URL], calls)
            loaded = bulk.load_13f_bulk_index(
                state_path,
                verify_index_checksum=True,
            )
            self.assertEqual(1, loaded["summary"]["information_table_rows"])

    def test_failed_recheck_retains_byte_identical_last_good_state(self) -> None:
        payload = dataset_zip()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            index_dir = root / "indices"
            accepted = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[DATASET_URL],
                target_accessions=[ACCESSION],
                fetcher=lambda _url: payload,
                recheck_recent_archives=0,
                refreshed_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
            )
            state_bytes = state_path.read_bytes()
            failed = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[DATASET_URL],
                target_accessions=[ACCESSION],
                fetcher=lambda _url: b"corrupt",
                recheck_recent_archives=1,
                refreshed_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
            )

            self.assertFalse(failed.changed)
            self.assertTrue(failed.errors)
            self.assertEqual(accepted.state, failed.state)
            self.assertEqual(state_bytes, state_path.read_bytes())
            bulk.load_13f_bulk_index(state_path, verify_index_checksum=True)

    def test_new_manifest_precedes_superseded_generation_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            index_dir = root / "indices"
            first = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[DATASET_URL],
                target_accessions=[ACCESSION],
                fetcher=lambda _url: dataset_zip(),
                recheck_recent_archives=0,
            )
            first_path = bulk._index_path_from_state(first.state, state_path)
            self.assertIsNotNone(first_path)

            observed: list[tuple[bool, str]] = []
            real_cleanup = bulk._cleanup_superseded_index_generations

            def observe_cleanup(**kwargs):
                durable = bulk.load_13f_bulk_index(state_path)
                observed.append((
                    first_path.exists(),
                    durable["index"]["path"],
                ))
                return real_cleanup(**kwargs)

            with mock.patch.object(
                bulk,
                "_cleanup_superseded_index_generations",
                side_effect=observe_cleanup,
            ):
                second = bulk.refresh_13f_bulk_index(
                    state_path=state_path,
                    index_dir=index_dir,
                    dataset_urls=[DATASET_URL],
                    target_accessions=[ACCESSION],
                    fetcher=lambda _url: dataset_zip(
                        information=[information_row(value="200001")]
                    ),
                    recheck_recent_archives=1,
                )

            second_path = bulk._index_path_from_state(second.state, state_path)
            self.assertTrue(second.changed)
            self.assertEqual([(True, second.state["index"]["path"])], observed)
            self.assertFalse(first_path.exists())
            self.assertTrue(second_path.exists())
            self.assertEqual(1, len(list(index_dir.glob("index-*.sqlite3"))))

    def test_clean_rebuild_resumes_completed_archives_from_private_checkpoint(
        self,
    ) -> None:
        older_accession = "0001234567-25-000001"
        older_payload = dataset_zip(
            submissions=[
                submission_row(
                    accession=older_accession,
                    report_date="31-DEC-2025",
                )
            ],
            information=[information_row(accession=older_accession)],
        )
        current_payload = dataset_zip()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            index_dir = root / "indices"
            checkpoint_path = root / "checkpoint.json"
            first_calls: list[str] = []

            def interrupted(url: str) -> bytes:
                first_calls.append(url)
                if url == OLDER_DATASET_URL:
                    return older_payload
                raise TimeoutError("late SEC timeout")

            failed = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[OLDER_DATASET_URL, DATASET_URL],
                target_accessions=[older_accession, ACCESSION],
                fetcher=interrupted,
                full_rebuild=True,
                recheck_recent_archives=0,
                clean_rebuild_checkpoint_path=checkpoint_path,
            )
            self.assertTrue(failed.errors)
            self.assertFalse(state_path.exists())
            self.assertTrue(checkpoint_path.is_file())
            self.assertEqual([OLDER_DATASET_URL, DATASET_URL], first_calls)
            partial_path = next(index_dir.glob("*.partial"))
            free = partial_path.stat().st_size + 1024
            usage = type("Usage", (), {"free": free})()
            capacity = bulk.ensure_clean_rebuild_disk_space(
                index_dir=index_dir,
                checkpoint_path=checkpoint_path,
                minimum_free_bytes=partial_path.stat().st_size + 2048,
                minimum_remaining_free_bytes=0,
                disk_usage=lambda _path: usage,
            )
            self.assertEqual(
                partial_path.stat().st_size,
                capacity["resumable_bytes"],
            )
            self.assertEqual(2048, capacity["minimum_free_bytes"])

            resumed_calls: list[str] = []

            def resumed(url: str) -> bytes:
                resumed_calls.append(url)
                return current_payload

            resumed_result = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[OLDER_DATASET_URL, DATASET_URL],
                target_accessions=[older_accession, ACCESSION],
                fetcher=resumed,
                full_rebuild=True,
                recheck_recent_archives=0,
                clean_rebuild_checkpoint_path=checkpoint_path,
            )

            self.assertFalse(resumed_result.errors)
            self.assertEqual([DATASET_URL], resumed_calls)
            self.assertFalse(checkpoint_path.exists())
            self.assertFalse(list(index_dir.glob("*.partial")))
            self.assertEqual(2, resumed_result.state["summary"]["submissions"])

            receipt = bulk.build_completed_clean_rebuild_receipt(state_path)
            no_fetch = mock.Mock(side_effect=AssertionError("must resume final index"))
            completed_resume = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[OLDER_DATASET_URL, DATASET_URL],
                target_accessions=[older_accession, ACCESSION],
                fetcher=no_fetch,
                full_rebuild=True,
                recheck_recent_archives=0,
                clean_rebuild_checkpoint_path=checkpoint_path,
                completed_rebuild_receipt=receipt,
            )
            self.assertFalse(completed_resume.changed)
            self.assertFalse(completed_resume.errors)
            no_fetch.assert_not_called()

            fresh_calls: list[str] = []

            def fresh_rebuild(url: str) -> bytes:
                fresh_calls.append(url)
                return older_payload if url == OLDER_DATASET_URL else current_payload

            independent = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[OLDER_DATASET_URL, DATASET_URL],
                target_accessions=[older_accession, ACCESSION],
                fetcher=fresh_rebuild,
                full_rebuild=True,
                recheck_recent_archives=0,
                clean_rebuild_checkpoint_path=checkpoint_path,
            )
            self.assertFalse(independent.errors)
            self.assertEqual([OLDER_DATASET_URL, DATASET_URL], fresh_calls)

    def test_discovered_accession_keeps_clean_rebuild_plan_stable_after_backfill(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund = fund_document()
            fund["quarters"][0].pop("accession")
            fund_path = funds_dir / "1234567.json"
            fund_path.write_text(json.dumps(fund), encoding="utf-8")
            discovery = {
                "targets": [{
                    "cik": "0001234567",
                    "accession": ACCESSION,
                    "report_date": "2026-06-30",
                }],
                "missing": [],
                "sources": [],
            }
            state_path = root / "state.json"
            index_dir = root / "indices"
            checkpoint_path = root / "checkpoint.json"
            receipt_path = root / "receipt.json"

            with mock.patch.object(
                bulk,
                "discover_archive_fallback_targets_for_periods",
                return_value=discovery,
            ):
                first = bulk.rebuild_reported_identity_from_sec(
                    funds_dir,
                    state_path=state_path,
                    index_dir=index_dir,
                    checkpoint_path=checkpoint_path,
                    completed_receipt_path=receipt_path,
                    dataset_urls=[DATASET_URL],
                    fetcher=lambda _url: dataset_zip(),
                )

            self.assertEqual(
                [ACCESSION],
                first.refresh.state["target_scope"]["accessions"],
            )
            no_fetch = mock.Mock(
                side_effect=AssertionError("stable plan must reuse the index")
            )
            second = bulk.rebuild_reported_identity_from_sec(
                funds_dir,
                state_path=state_path,
                index_dir=index_dir,
                checkpoint_path=checkpoint_path,
                completed_receipt_path=receipt_path,
                completed_rebuild_receipt=first.completed_rebuild_receipt,
                dataset_urls=[DATASET_URL],
                fetcher=no_fetch,
            )

            no_fetch.assert_not_called()
            self.assertFalse(second.refresh.changed)
            self.assertEqual(
                first.refresh.state["clean_rebuild_plan_sha256"],
                second.refresh.state["clean_rebuild_plan_sha256"],
            )

    def test_schema_failure_on_new_archive_does_not_publish_partial_index(self) -> None:
        bad_columns = [name for name in INFOTABLE_COLUMNS if name != "TITLEOFCLASS"]
        bad_row = information_row()
        bad_row.pop(INFOTABLE_COLUMNS.index("TITLEOFCLASS"))
        payload = dataset_zip(
            information_columns=bad_columns,
            information=[bad_row],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            result = bulk.refresh_13f_bulk_index(
                state_path=root / "state.json",
                index_dir=root / "indices",
                dataset_urls=[DATASET_URL],
                target_accessions=[ACCESSION],
                fetcher=lambda _url: payload,
                recheck_recent_archives=0,
            )
            self.assertTrue(result.errors)
            self.assertIsNone(result.state["index"])
            self.assertFalse((root / "state.json").exists())


class Sec13FBulkBackfillTests(unittest.TestCase):
    def build_index(
        self,
        root: Path,
        *,
        information: list[list[object]],
        submissions: list[list[object]] | None = None,
        target_accessions: list[str] | None = None,
    ) -> Path:
        state_path = root / "state.json"
        result = bulk.refresh_13f_bulk_index(
            state_path=state_path,
            index_dir=root / "indices",
            dataset_urls=[DATASET_URL],
            target_accessions=target_accessions or [ACCESSION],
            fetcher=lambda _url: dataset_zip(
                submissions=submissions,
                information=information,
            ),
            recheck_recent_archives=0,
            refreshed_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
        )
        self.assertFalse(result.errors)
        return state_path

    def add_filing_chain(
        self,
        state_path: Path,
        *,
        accession: str = ACCESSION,
        entry_total: int,
        value_total: object,
        consistent: int = 1,
        accepted: str = "20260814120000",
        is_amendment: int = 0,
        amendment_type: str | None = None,
    ) -> None:
        state = bulk.load_13f_bulk_index(state_path)
        index_path = bulk._index_path_from_state(state, state_path)
        assert index_path is not None
        source = state["sources"][DATASET_URL]
        connection = bulk._open_index(index_path, read_only=False)
        try:
            connection.execute(
                """
                INSERT INTO filing_chain(
                    accession, acceptance_datetime, cover_is_amendment,
                    cover_amendment_type, cover_table_entry_total,
                    cover_table_value_total, cover_metadata_consistent,
                    source_url, source_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    accession,
                    accepted,
                    is_amendment,
                    amendment_type,
                    entry_total,
                    str(value_total),
                    consistent,
                    DATASET_URL,
                    source["sha256"],
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def test_parallel_backfill_and_verification_match_serial_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            results = []
            persisted = []
            for workers in (1, 2):
                funds_dir = root / f"funds-{workers}"
                funds_dir.mkdir()
                for index in range(9):
                    (funds_dir / f"{index:07}.json").write_text(
                        json.dumps(fund_document()), encoding="utf-8",
                    )
                results.append(bulk.backfill_fund_files(
                    funds_dir, state_path=state_path,
                    require_all_verified=True, workers=workers,
                ))
                persisted.append([path.read_bytes() for path in sorted(funds_dir.glob("*.json"))])
            self.assertEqual(results[0], results[1])
            self.assertEqual(persisted[0], persisted[1])
            funds_dir = root / "funds-2"
            verified = [bulk.verify_reported_identity_against_sec(
                funds_dir, state_path=state_path,
                require_source_provenance=True, workers=workers,
            ) for workers in (1, 2)]
            self.assertEqual(verified[0], verified[1])
            self.assertTrue(verified[0].ok)
            path = funds_dir / "0000008.json"
            conflicting = json.loads(path.read_text())
            conflicting["quarters"][0]["holdings"][0]["reported_issuer"] = "NOT AS FILED"
            path.write_text(json.dumps(conflicting), encoding="utf-8")
            verified = [bulk.verify_reported_identity_against_sec(
                funds_dir, state_path=state_path,
                require_source_provenance=True, workers=workers,
            ) for workers in (1, 2)]
            self.assertEqual(verified[0], verified[1])
            self.assertFalse(verified[0].ok)

    def test_parallel_preflight_failure_prevents_every_file_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            funds_dir = root / "funds"
            funds_dir.mkdir()
            for index in range(9):
                (funds_dir / f"{index:07}.json").write_text(
                    json.dumps(fund_document()), encoding="utf-8",
                )
            conflicting = fund_document()
            conflicting["quarters"][0]["holdings"][0]["reported_issuer"] = "NOT AS FILED"
            bad_path = funds_dir / "0000008.json"
            bad_path.write_text(json.dumps(conflicting), encoding="utf-8")
            before = {path.name: path.read_bytes() for path in funds_dir.iterdir()}
            with self.assertRaisesRegex(bulk.BulkIndexRefreshError, "before apply"):
                bulk.backfill_fund_files(
                    funds_dir, state_path=state_path,
                    require_all_verified=True, workers=2,
                )
            self.assertEqual(before, {path.name: path.read_bytes() for path in funds_dir.iterdir()})
            bad_path.write_text("{not-json", encoding="utf-8")
            before[bad_path.name] = bad_path.read_bytes()
            with self.assertRaisesRegex(bulk.Sec13FBulkError, "cannot read"):
                bulk.backfill_fund_files(
                    funds_dir, state_path=state_path,
                    require_all_verified=True, workers=2,
                )
            self.assertEqual(before, {path.name: path.read_bytes() for path in funds_dir.iterdir()})

    def test_parallel_archive_enrichment_matches_serial_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            funds_dir = root / "funds"
            funds_dir.mkdir()
            for index in range(9):
                fund = fund_document()
                fund["quarters"][0].pop("accession")
                fund["quarters"][0]["holdings"][0]["cusip"] = "000000000"
                (funds_dir / f"{index:07}.json").write_text(json.dumps(fund), encoding="utf-8")
            results = [bulk.collect_archive_enrichment_targets_from_funds(
                funds_dir, state_path=state_path, workers=workers,
            ) for workers in (1, 2)]
            self.assertEqual(results[0], results[1])
            self.assertEqual([{
                "cik": "0001234567", "accession": ACCESSION, "report_date": "2026-06-30",
            }], results[0])

    def test_exact_backfill_never_rewrites_canonical_fields_and_is_idempotent(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "1234567.json"
            original = fund_document()
            canonical_before = copy.deepcopy(
                original["quarters"][0]["holdings"][0]
            )
            fund_path.write_text(json.dumps(original), encoding="utf-8")

            first = bulk.backfill_fund_files(
                funds_dir,
                state_path=state_path,
                verify_index_checksum=True,
            )
            first_bytes = fund_path.read_bytes()
            second = bulk.backfill_fund_files(
                funds_dir,
                state_path=state_path,
            )
            persisted = json.loads(fund_path.read_text())
            quarter = persisted["quarters"][0]
            holding = quarter["holdings"][0]

            self.assertEqual(1, first.files_changed)
            self.assertEqual(1, first.holdings_changed)
            self.assertEqual(0, second.files_changed)
            self.assertEqual(first_bytes, fund_path.read_bytes())
            for field, value in canonical_before.items():
                self.assertEqual(value, holding[field])
            self.assertEqual("APPLE INC", holding["reported_issuer"])
            self.assertEqual("COM", holding["reported_class"])
            self.assertEqual("037833100", holding["reported_cusip"])
            self.assertEqual("BBG000B9XRY4", holding["reported_figi"])
            self.assertEqual(ACCESSION, holding["accession"])
            self.assertEqual("2026-06-30", holding["report_date"])
            self.assertEqual(
                [{
                    "accession": ACCESSION,
                    "report_date": "2026-06-30",
                    "url": DATASET_URL,
                    "sha256": bulk.load_13f_bulk_index(state_path)["sources"][
                        DATASET_URL
                    ]["sha256"],
                }],
                quarter["reported_identity_sources"],
            )

    @staticmethod
    def corrupt_accessionless_fund() -> dict:
        return {
            "cik": 1234567,
            "name": "Fixture Manager",
            "quarters": [{
                "report_date": "2026-06-30",
                "filing_date": "2099-01-01",
                "total_value": 7,
                "num_holdings": 1,
                "holdings": [{
                    "ticker": "WRONG",
                    "issuer": "Wrong legacy issuer",
                    "class": "Wrong legacy class",
                    "cusip": "999999999",
                    "value": 7,
                    "shares": 1,
                    "holding_type": "EQUITY",
                }],
            }],
        }

    @staticmethod
    def whole_quarter_information_rows(
        *,
        accession: str = ACCESSION,
    ) -> list[list[object]]:
        return [
            information_row(
                accession=accession,
                infotable_sk="1",
                issuer="ALPHA AS FILED",
                security_class="COM A",
                value="120000",
                shares="600",
            ),
            information_row(
                accession=accession,
                infotable_sk="2",
                issuer="ALPHA AS FILED",
                security_class="COM A",
                value="80000",
                shares="400",
            ),
            information_row(
                accession=accession,
                infotable_sk="3",
                issuer="BETA AS FILED",
                security_class="COM B",
                cusip="594918104",
                figi="BBG000BPH459",
                value="40000",
                shares="200",
            ),
        ]

    def test_whole_quarter_rebuild_uses_only_complete_exact_sec_filing(
        self,
    ) -> None:
        rows = self.whole_quarter_information_rows()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            self.add_filing_chain(
                state_path,
                entry_total=3,
                value_total=240000,
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                original = self.corrupt_accessionless_fund()
                rebuilt, stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
                verification, issues = bulk._verify_fund_document_against_index(
                    rebuilt,
                    connection=connection,
                    require_source_provenance=True,
                )
                rerun, rerun_stats = bulk.backfill_fund_document(
                    rebuilt,
                    connection=connection,
                )
            finally:
                connection.close()

        quarter = rebuilt["quarters"][0]
        holdings = quarter["holdings"]
        self.assertEqual(2, quarter["num_holdings"])
        self.assertEqual(240000, quarter["total_value"])
        self.assertEqual("2026-08-14", quarter["filing_date"])
        self.assertEqual(1, quarter["value_multiplier"])
        self.assertEqual("high", quarter["value_unit_confidence"])
        self.assertEqual(
            {"ALPHA AS FILED", "BETA AS FILED"},
            {holding["issuer"] for holding in holdings},
        )
        self.assertEqual({None}, {holding["ticker"] for holding in holdings})
        self.assertEqual({ACCESSION}, {holding["accession"] for holding in holdings})
        self.assertEqual(
            {"2026-06-30"},
            {holding["report_date"] for holding in holdings},
        )
        self.assertEqual(
            {"ALPHA AS FILED", "BETA AS FILED"},
            {holding["reported_issuer"] for holding in holdings},
        )
        self.assertEqual(2, stats["exact_matches"])
        self.assertEqual(2, stats["holdings_changed"])
        self.assertEqual([], issues)
        self.assertEqual(2, verification["exact_matches"])
        self.assertEqual(rebuilt, rerun)
        self.assertEqual(0, rerun_stats["holdings_changed"])
        self.assertNotIn("composition_version", quarter)

    def test_strict_cutover_rejects_whole_quarter_economic_change_before_any_write(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root, information=self.whole_quarter_information_rows(),
            )
            self.add_filing_chain(state_path, entry_total=3, value_total=240000)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            first = funds_dir / "0000001.json"
            second = funds_dir / "0000002.json"
            first.write_text(json.dumps(fund_document()), encoding="utf-8")
            second.write_text(
                json.dumps(self.corrupt_accessionless_fund()), encoding="utf-8",
            )
            original_bytes = {path: path.read_bytes() for path in (first, second)}

            with self.assertRaisesRegex(
                bulk.BulkIndexRefreshError,
                "economic-position verification failed before apply: 0000002.json",
            ):
                bulk.backfill_fund_files(
                    funds_dir,
                    state_path=state_path,
                    require_all_verified=True,
                )

            self.assertEqual(original_bytes, {
                path: path.read_bytes() for path in (first, second)
            })

    def test_strict_cutover_rejects_type_drift_with_equal_value(
        self,
    ) -> None:
        rows = [
            information_row(
                infotable_sk="1", security_class="COM A",
                value="120000", shares="600",
            ),
            information_row(
                infotable_sk="2", security_class="COM B",
                value="80000", shares="400",
            ),
        ]
        for holding_type in ("CALL", "PUT", "NOTE", "PREF", "WARRANT"):
            with self.subTest(holding_type=holding_type):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    state_path = self.build_index(root, information=rows)
                    funds_dir = root / "funds"
                    funds_dir.mkdir()
                    path = funds_dir / "1234567.json"
                    fund = fund_document()
                    holding = fund["quarters"][0]["holdings"][0]
                    holding["holding_type"] = holding_type
                    path.write_text(json.dumps(fund), encoding="utf-8")
                    original = path.read_bytes()

                    with self.assertRaisesRegex(
                        bulk.BulkIndexRefreshError,
                        "economic-position verification failed before apply",
                    ):
                        bulk.backfill_fund_files(
                            funds_dir, state_path=state_path,
                            require_all_verified=True,
                        )
                    self.assertEqual(original, path.read_bytes())

    def test_whole_quarter_rebuild_rejects_any_retained_identity_metadata(
        self,
    ) -> None:
        rows = self.whole_quarter_information_rows()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            self.add_filing_chain(
                state_path,
                entry_total=3,
                value_total=240000,
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                cases = []
                holding_identity = self.corrupt_accessionless_fund()
                holding_identity["quarters"][0]["holdings"][0][
                    "reported_issuer"
                ] = "Retained identity"
                cases.append(("holding identity", holding_identity))
                quarter_accession = self.corrupt_accessionless_fund()
                quarter_accession["quarters"][0]["accession"] = ACCESSION
                cases.append(("quarter accession", quarter_accession))
                composition = self.corrupt_accessionless_fund()
                composition["quarters"][0]["composition_version"] = 2
                cases.append(("composition metadata", composition))

                for label, original in cases:
                    with self.subTest(label=label):
                        updated, stats = bulk.backfill_fund_document(
                            original,
                            connection=connection,
                        )
                        self.assertEqual(original, updated)
                        self.assertEqual(1, stats["unmatched"])
            finally:
                connection.close()

    def test_whole_quarter_rebuild_rejects_partial_exact_legacy_quarter(
        self,
    ) -> None:
        rows = self.whole_quarter_information_rows()
        fund = self.corrupt_accessionless_fund()
        fund["quarters"][0]["holdings"].insert(0, {
            "ticker": "OLD",
            "issuer": "Old display",
            "class": "Old class",
            "cusip": "037833100",
            "value": 200000,
            "shares": 1000,
            "holding_type": "EQUITY",
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            self.add_filing_chain(
                state_path,
                entry_total=3,
                value_total=240000,
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund,
                    connection=connection,
                )
            finally:
                connection.close()

        holdings = updated["quarters"][0]["holdings"]
        self.assertEqual(2, len(holdings))
        self.assertEqual("999999999", holdings[1]["cusip"])
        self.assertEqual(1, stats["exact_matches"])
        self.assertEqual(1, stats["unmatched"])

    def test_whole_quarter_rebuild_requires_exact_cover_reconciliation(
        self,
    ) -> None:
        rows = self.whole_quarter_information_rows()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                original = self.corrupt_accessionless_fund()
                missing, missing_stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
            finally:
                connection.close()
            self.assertEqual(original, missing)
            self.assertEqual(1, missing_stats["unmatched"])

            self.add_filing_chain(
                state_path,
                entry_total=4,
                value_total=240000,
            )
            connection = bulk._open_index(index_path, read_only=True)
            try:
                mismatched, mismatch_stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
            finally:
                connection.close()
            self.assertEqual(original, mismatched)
            self.assertEqual(1, mismatch_stats["unmatched"])

    def test_whole_quarter_cover_value_discrepancy_is_bounded_per_row(
        self,
    ) -> None:
        rows = self.whole_quarter_information_rows()
        for delta, should_rebuild in ((3, True), (4, False)):
            with self.subTest(delta=delta), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                state_path = self.build_index(root, information=rows)
                self.add_filing_chain(
                    state_path,
                    entry_total=3,
                    value_total=240000 + delta,
                    consistent=0,
                )
                state = bulk.load_13f_bulk_index(state_path)
                index_path = bulk._index_path_from_state(state, state_path)
                assert index_path is not None
                connection = bulk._open_index(index_path, read_only=True)
                try:
                    original = self.corrupt_accessionless_fund()
                    updated, stats = bulk.backfill_fund_document(
                        original,
                        connection=connection,
                    )
                finally:
                    connection.close()

                if should_rebuild:
                    quarter = updated["quarters"][0]
                    self.assertEqual(2, quarter["num_holdings"])
                    self.assertEqual(240000, quarter["total_value"])
                    self.assertEqual(2, stats["exact_matches"])
                else:
                    self.assertEqual(original, updated)
                    self.assertEqual(1, stats["unmatched"])

    def test_whole_quarter_rebuild_rejects_noncanonical_new_holdings_amendment(
        self,
    ) -> None:
        rows = self.whole_quarter_information_rows()
        amendment_submission = submission_row()
        amendment_submission[2] = "13F-HR/A"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root,
                information=rows,
                submissions=[amendment_submission],
            )
            self.add_filing_chain(
                state_path,
                entry_total=3,
                value_total=240000,
                is_amendment=1,
                amendment_type="NEW HOLDINGS",
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                original = self.corrupt_accessionless_fund()
                updated, stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(original, updated)
        self.assertEqual(1, stats["unmatched"])

    def test_whole_quarter_rebuild_rejects_original_plus_new_holdings(
        self,
    ) -> None:
        amendment_accession = "0001234567-26-000002"
        amendment_submission = submission_row(accession=amendment_accession)
        amendment_submission[2] = "13F-HR/A"
        original_rows = self.whole_quarter_information_rows()
        amendment_rows = [
            information_row(
                accession=amendment_accession,
                infotable_sk=str(index),
                issuer=row[2],
                security_class=row[3],
                cusip=row[4],
                figi=row[5],
                value=row[6],
                shares=row[7],
                amount_type=row[8],
                put_call=row[9],
            )
            for index, row in enumerate(original_rows, start=1)
        ]
        amendment_rows.append(
            information_row(
                accession=amendment_accession,
                infotable_sk="4",
                issuer="GAMMA AS FILED",
                security_class="COM",
                cusip="02079K305",
                figi="BBG009S39JX6",
                value="50000",
                shares="250",
            )
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root,
                information=[*original_rows, *amendment_rows],
                submissions=[
                    submission_row(),
                    amendment_submission,
                ],
                target_accessions=[ACCESSION, amendment_accession],
            )
            self.add_filing_chain(
                state_path,
                entry_total=3,
                value_total=240000,
            )
            self.add_filing_chain(
                state_path,
                accession=amendment_accession,
                entry_total=4,
                value_total=290000,
                accepted="20260814130000",
                is_amendment=1,
                amendment_type="NEW HOLDINGS",
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                original = self.corrupt_accessionless_fund()
                updated, stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(original, updated)
        self.assertEqual(1, stats["unmatched"])

    def test_row_backfill_uses_canonical_filing_when_amendment_repeats_rows(
        self,
    ) -> None:
        amendment_accession = "0001234567-26-000002"
        amendment_submission = submission_row(accession=amendment_accession)
        amendment_submission[2] = "13F-HR/A"
        original_row = information_row()
        amendment_rows = [
            information_row(
                accession=amendment_accession,
                infotable_sk="1",
            ),
            information_row(
                accession=amendment_accession,
                infotable_sk="2",
                issuer="MICROSOFT CORP",
                cusip="594918104",
                figi="BBG000BPH459",
                value="300000",
                shares="750",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root,
                information=[original_row, *amendment_rows],
                submissions=[
                    submission_row(),
                    amendment_submission,
                ],
                target_accessions=[ACCESSION, amendment_accession],
            )
            self.add_filing_chain(
                state_path,
                entry_total=1,
                value_total=200000,
            )
            self.add_filing_chain(
                state_path,
                accession=amendment_accession,
                entry_total=2,
                value_total=500000,
                accepted="20260814130000",
                is_amendment=1,
                amendment_type="NEW HOLDINGS",
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                original = fund_document()
                del original["quarters"][0]["accession"]
                original["quarters"][0]["filing_date"] = "2026-08"
                original["quarters"][0]["num_holdings"] = 1
                original["quarters"][0]["total_value"] = 200000
                rebuilt, stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
                verification, issues = (
                    bulk._verify_fund_document_against_index(
                        rebuilt,
                        connection=connection,
                        require_source_provenance=True,
                    )
                )
            finally:
                connection.close()

        quarter = rebuilt["quarters"][0]
        self.assertEqual(
            [ACCESSION],
            [holding["accession"] for holding in quarter["holdings"]],
        )
        self.assertEqual(1, stats["exact_matches"])
        self.assertEqual(1, stats["holdings_changed"])
        self.assertEqual([], issues)
        self.assertEqual(1, verification["exact_matches"])

    def test_row_backfill_rejects_identical_new_holdings_accession(
        self,
    ) -> None:
        amendment_accession = "0001234567-26-000002"
        amendment_submission = submission_row(accession=amendment_accession)
        amendment_submission[2] = "13F-HR/A"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root,
                information=[
                    information_row(),
                    information_row(
                        accession=amendment_accession,
                        infotable_sk="1",
                    ),
                ],
                submissions=[submission_row(), amendment_submission],
                target_accessions=[ACCESSION, amendment_accession],
            )
            self.add_filing_chain(
                state_path,
                entry_total=1,
                value_total=200000,
            )
            self.add_filing_chain(
                state_path,
                accession=amendment_accession,
                entry_total=1,
                value_total=200000,
                accepted="20260814130000",
                is_amendment=1,
                amendment_type="NEW HOLDINGS",
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                original = fund_document()
                del original["quarters"][0]["accession"]
                original["quarters"][0]["filing_date"] = "2026-08-14"
                original["quarters"][0]["num_holdings"] = 1
                original["quarters"][0]["total_value"] = 200000
                updated, stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(original, updated)
        self.assertEqual(1, stats["unmatched"])

    def test_accessionless_new_holdings_can_prove_holding_identity(self) -> None:
        amendment_accession = "0001234567-26-000002"
        amendment_submission = submission_row(accession=amendment_accession)
        amendment_submission[2] = "13F-HR/A"
        amendment_row = information_row(
            accession=amendment_accession,
            infotable_sk="1",
            issuer="MICROSOFT CORP",
            cusip="594918104",
            figi="BBG000BPH459",
            value="300000",
            shares="750",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root,
                information=[amendment_row],
                submissions=[amendment_submission],
                target_accessions=[amendment_accession],
            )
            self.add_filing_chain(
                state_path,
                accession=amendment_accession,
                entry_total=1,
                value_total=300000,
                is_amendment=1,
                amendment_type="NEW HOLDINGS",
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                original = fund_document(holding={
                    "ticker": "MSFT",
                    "issuer": "Microsoft display",
                    "class": "Common display",
                    "cusip": "594918104",
                    "value": 300000,
                    "shares": 750,
                    "holding_type": "EQUITY",
                })
                del original["quarters"][0]["accession"]
                original["quarters"][0]["filing_date"] = "2026-08-14"
                original["quarters"][0]["num_holdings"] = 1
                original["quarters"][0]["total_value"] = 300000
                updated, stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
            finally:
                connection.close()

        holding = updated["quarters"][0]["holdings"][0]
        self.assertEqual(amendment_accession, holding["accession"])
        self.assertEqual("MICROSOFT CORP", holding["reported_issuer"])
        self.assertEqual(1, stats["exact_matches"])
        self.assertEqual(1, stats["holdings_changed"])
        # Backfill binds this retained row to its exact as-filed source. It
        # deliberately does not claim that the amendment is a complete
        # composed quarter by adding a quarter-level accession.
        self.assertNotIn("accession", updated["quarters"][0])

    def test_whole_quarter_rebuild_does_not_guess_ambiguous_value_units(
        self,
    ) -> None:
        rows = [
            information_row(
                infotable_sk="1",
                issuer="ALPHA NOTE AS FILED",
                security_class="NOTE",
                value="200",
                shares="1000",
                amount_type="PRN",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            self.add_filing_chain(
                state_path,
                entry_total=1,
                value_total=200,
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                original = self.corrupt_accessionless_fund()
                updated, stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(original, updated)
        self.assertEqual(1, stats["unmatched"])

    def test_whole_quarter_rebuild_rejects_multiple_canonical_accessions(
        self,
    ) -> None:
        second_accession = "0001234567-26-000002"
        submissions = [
            submission_row(accession=ACCESSION),
            submission_row(accession=second_accession),
        ]
        rows = [
            *self.whole_quarter_information_rows(),
            information_row(
                accession=second_accession,
                infotable_sk="4",
                issuer="GAMMA AS FILED",
                security_class="COM",
                cusip="02079K305",
                figi="BBG009S39JX6",
                value="50000",
                shares="250",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root,
                information=rows,
                submissions=submissions,
                target_accessions=[ACCESSION, second_accession],
            )
            self.add_filing_chain(
                state_path,
                entry_total=3,
                value_total=240000,
                accepted="20260814120000",
            )
            self.add_filing_chain(
                state_path,
                accession=second_accession,
                entry_total=1,
                value_total=50000,
                accepted="20260814130000",
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            assert index_path is not None
            connection = bulk._open_index(index_path, read_only=True)
            try:
                original = self.corrupt_accessionless_fund()
                updated, stats = bulk.backfill_fund_document(
                    original,
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(original, updated)
        self.assertEqual(1, stats["unmatched"])

    def test_residual_discovery_targets_only_ambiguous_accessionless_period(self) -> None:
        second_accession = "0001234567-26-000002"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root,
                submissions=[
                    submission_row(accession=ACCESSION),
                    submission_row(accession=second_accession),
                ],
                information=[
                    information_row(accession=ACCESSION),
                    information_row(accession=second_accession),
                ],
                target_accessions=[ACCESSION, second_accession],
            )
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund = fund_document()
            del fund["quarters"][0]["accession"]
            (funds_dir / "1234567.json").write_text(
                json.dumps(fund),
                encoding="utf-8",
            )

            self.assertEqual(
                [
                    {
                        "cik": "0001234567",
                        "accession": ACCESSION,
                        "report_date": "2026-06-30",
                    },
                    {
                        "cik": "0001234567",
                        "accession": second_accession,
                        "report_date": "2026-06-30",
                    },
                ],
                bulk.collect_archive_enrichment_targets_from_funds(
                    funds_dir,
                    state_path=state_path,
                ),
            )

    def test_interrupted_per_file_apply_is_safe_to_rerun(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            funds_dir = root / "funds"
            funds_dir.mkdir()
            first_path = funds_dir / "0000001.json"
            second_path = funds_dir / "0000002.json"
            first_path.write_text(json.dumps(fund_document()), encoding="utf-8")
            second_path.write_text(json.dumps(fund_document()), encoding="utf-8")
            real_write = bulk._atomic_write_fund_json
            writes = 0

            def interrupt_second(path: Path, payload: dict) -> None:
                nonlocal writes
                if Path(path).parent == funds_dir:
                    writes += 1
                    if writes == 2:
                        raise OSError("simulated runner interruption")
                real_write(path, payload)

            with mock.patch.object(
                bulk,
                "_atomic_write_fund_json",
                side_effect=interrupt_second,
            ):
                with self.assertRaisesRegex(OSError, "runner interruption"):
                    bulk.backfill_fund_files(
                        funds_dir,
                        state_path=state_path,
                        require_all_verified=True,
                    )

            first = json.loads(first_path.read_text())
            second = json.loads(second_path.read_text())
            self.assertIn(
                "reported_issuer",
                first["quarters"][0]["holdings"][0],
            )
            self.assertNotIn(
                "reported_issuer",
                second["quarters"][0]["holdings"][0],
            )

            rerun = bulk.backfill_fund_files(
                funds_dir,
                state_path=state_path,
                require_all_verified=True,
            )
            verified = bulk.verify_reported_identity_against_sec(
                funds_dir,
                state_path=state_path,
                verify_index_checksum=True,
            )
            self.assertEqual(1, rerun.files_changed)
            self.assertTrue(verified.ok)

    def test_strict_verification_binds_persisted_source_to_exact_index_row(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "1234567.json"
            fund_path.write_text(json.dumps(fund_document()), encoding="utf-8")
            bulk.backfill_fund_files(
                funds_dir,
                state_path=state_path,
                require_all_verified=True,
            )
            tampered = json.loads(fund_path.read_text(encoding="utf-8"))
            tampered["quarters"][0]["reported_identity_sources"][0][
                "sha256"
            ] = "b" * 64
            fund_path.write_text(json.dumps(tampered), encoding="utf-8")

            rejected = bulk.verify_reported_identity_against_sec(
                funds_dir,
                state_path=state_path,
                require_source_provenance=True,
            )
            self.assertFalse(rejected.ok)
            self.assertEqual(1, rejected.conflicts)

            repaired = bulk.backfill_fund_files(
                funds_dir,
                state_path=state_path,
                require_all_verified=True,
            )
            accepted = bulk.verify_reported_identity_against_sec(
                funds_dir,
                state_path=state_path,
                require_source_provenance=True,
            )
            self.assertEqual(1, repaired.files_changed)
            self.assertTrue(accepted.ok)

    def test_target_collection_streams_one_fund_document_at_a_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            for index in range(4):
                (funds_dir / f"{index:07d}.json").write_text(
                    json.dumps(fund_document()),
                    encoding="utf-8",
                )

            original = bulk.collect_backfill_targets
            with mock.patch.object(
                bulk,
                "collect_backfill_targets",
                side_effect=lambda documents: (
                    self.assertNotIsInstance(documents, (list, tuple))
                    or original(documents)
                ),
            ):
                targets = bulk.collect_backfill_targets_from_funds(funds_dir)

            self.assertEqual(4, targets["holdings_targeted"])

    def test_legacy_thousands_value_matches_only_via_declared_multiplier(self) -> None:
        holding = {
            "issuer": "Canonical",
            "class": "Canonical",
            "cusip": "037833100",
            "value": 200000,
            "shares": 1000,
            "holding_type": "EQUITY",
            "share_amount_type": "SH",
        }
        fund = fund_document(report_date="2022-12-31", holding=holding)
        fund["quarters"][0]["value_multiplier"] = 1000
        fund["quarters"][0]["accession"] = ACCESSION
        submissions = [submission_row(report_date="31-DEC-2022")]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = dataset_zip(
                submissions=submissions,
                information=[information_row(value="200")],
            )
            state_path = root / "state.json"
            result = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=root / "indices",
                dataset_urls=[DATASET_URL],
                target_accessions=[ACCESSION],
                fetcher=lambda _url: payload,
                recheck_recent_archives=0,
            )
            self.assertFalse(result.errors)
            index_path = bulk._index_path_from_state(result.state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund,
                    connection=connection,
                )
            finally:
                connection.close()
            self.assertEqual(1, stats["exact_matches"])
            self.assertEqual(
                "APPLE INC",
                updated["quarters"][0]["holdings"][0]["reported_issuer"],
            )

    def test_candidate_uses_multiplier_for_exact_applied_accession(self) -> None:
        holding = {
            "cusip": "037833100",
            "value": 200000,
            "shares": 1000,
            "holding_type": "EQUITY",
            "share_amount_type": "SH",
        }
        record = {
            "accession": ACCESSION,
            "cusip_key": "037833100",
            "reported_value": "200",
            "reported_shares": "1000",
            "share_amount_type": "SH",
            "put_call": "",
        }
        quarter = {
            "report_date": "2025-06-30",
            "source_filings": [{
                "accession": ACCESSION,
                "applied": True,
                "reported_value_total": 200,
                "normalized_value_total": 200000,
                "value_multiplier": 1000,
            }],
        }

        self.assertTrue(
            bulk._candidate_matches_holding(holding, quarter, record)
        )
        wrong = dict(record, reported_value="200000")
        self.assertFalse(
            bulk._candidate_matches_holding(holding, quarter, wrong)
        )

    def test_candidate_uses_different_units_for_applied_accessions(self) -> None:
        holding = {
            "cusip": "037833100",
            "value": 200000,
            "shares": 1000,
            "holding_type": "EQUITY",
            "share_amount_type": "SH",
        }
        quarter = {
            "report_date": "2025-06-30",
            "source_filings": [
                {
                    "accession": ACCESSION,
                    "applied": True,
                    "reported_value_total": 200,
                    "normalized_value_total": 200000,
                    "value_multiplier": 1000,
                },
                {
                    "accession": LEGACY_ACCESSION,
                    "applied": True,
                    "reported_value_total": 200000,
                    "normalized_value_total": 200000,
                    "value_multiplier": 1,
                },
            ],
        }
        thousands_record = {
            "accession": ACCESSION,
            "cusip_key": "037833100",
            "reported_value": "200",
            "reported_shares": "1000",
            "share_amount_type": "SH",
            "put_call": "",
        }
        dollars_record = dict(
            thousands_record,
            accession=LEGACY_ACCESSION,
            reported_value="200000",
        )

        self.assertTrue(
            bulk._candidate_matches_holding(
                holding, quarter, thousands_record
            )
        )
        self.assertTrue(
            bulk._candidate_matches_holding(holding, quarter, dollars_record)
        )

    def test_candidate_rejects_corrupt_declared_multiplier(self) -> None:
        holding = {
            "cusip": "037833100",
            "value": 200000,
            "shares": 1000,
            "holding_type": "EQUITY",
            "share_amount_type": "SH",
        }
        record = {
            "accession": ACCESSION,
            "cusip_key": "037833100",
            "reported_value": "200",
            "reported_shares": "1000",
            "share_amount_type": "SH",
            "put_call": "",
        }
        quarter = {
            "source_filings": [{
                "accession": ACCESSION,
                "applied": True,
                "reported_value_total": 200,
                "normalized_value_total": 200,
                "value_multiplier": 1000,
            }],
        }

        self.assertFalse(
            bulk._candidate_matches_holding(holding, quarter, record)
        )

    def test_candidate_rejects_superseded_accession(self) -> None:
        holding = {
            "cusip": "037833100",
            "value": 200000,
            "shares": 1000,
            "holding_type": "EQUITY",
            "share_amount_type": "SH",
        }
        record = {
            "accession": ACCESSION,
            "cusip_key": "037833100",
            "reported_value": "200",
            "reported_shares": "1000",
            "share_amount_type": "SH",
            "put_call": "",
        }
        quarter = {
            "applied_accessions": [LEGACY_ACCESSION],
            "source_filings": [{
                "accession": ACCESSION,
                "applied": False,
                "reported_value_total": 200,
                "normalized_value_total": 200000,
                "value_multiplier": 1000,
            }],
        }

        self.assertFalse(
            bulk._candidate_matches_holding(holding, quarter, record)
        )

    def test_candidate_infers_only_documented_units_without_metadata(self) -> None:
        holding = {
            "cusip": "037833100",
            "value": 200000,
            "shares": 1000,
            "holding_type": "EQUITY",
            "share_amount_type": "SH",
        }
        base_record = {
            "accession": ACCESSION,
            "cusip_key": "037833100",
            "reported_shares": "1000",
            "share_amount_type": "SH",
            "put_call": "",
        }

        self.assertTrue(bulk._candidate_matches_holding(
            holding,
            {},
            dict(base_record, reported_value="200"),
        ))
        self.assertTrue(bulk._candidate_matches_holding(
            holding,
            {},
            dict(base_record, reported_value="200000"),
        ))
        self.assertFalse(bulk._candidate_matches_holding(
            holding,
            {},
            dict(base_record, reported_value="2000"),
        ))

    def test_candidate_honors_row_specific_historical_repair(self) -> None:
        quarter = {
            "value_unit_repair": {
                "confidence": "high",
                "evidence": {
                    "sec_accession": ACCESSION,
                    "row_value_multipliers": {
                        "default": 1000,
                        "002824100": 1,
                    },
                },
            },
        }
        base_record = {
            "accession": ACCESSION,
            "reported_shares": "1000",
            "share_amount_type": "SH",
            "put_call": "",
        }
        default_holding = {
            "cusip": "037833100",
            "value": 200000,
            "shares": 1000,
            "holding_type": "EQUITY",
            "share_amount_type": "SH",
        }
        override_holding = dict(default_holding, cusip="002824100", value=200)

        self.assertTrue(bulk._candidate_matches_holding(
            default_holding,
            quarter,
            dict(base_record, cusip_key="037833100", reported_value="200"),
        ))
        self.assertTrue(bulk._candidate_matches_holding(
            override_holding,
            quarter,
            dict(base_record, cusip_key="002824100", reported_value="200"),
        ))
        self.assertFalse(bulk._candidate_matches_holding(
            default_holding,
            quarter,
            dict(
                base_record,
                accession=LEGACY_ACCESSION,
                cusip_key="037833100",
                reported_value="200",
            ),
        ))

    def test_present_reported_fields_still_backfill_missing_provenance(self) -> None:
        fund = fund_document()
        holding = fund["quarters"][0]["holdings"][0]
        holding.update({
            "reported_issuer": "APPLE INC",
            "reported_class": "COM",
            "reported_cusip": "037833100",
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund,
                    connection=connection,
                )
            finally:
                connection.close()
        updated_holding = updated["quarters"][0]["holdings"][0]
        self.assertEqual(1, stats["holdings_changed"])
        self.assertEqual(ACCESSION, updated_holding["accession"])
        self.assertEqual("2026-06-30", updated_holding["report_date"])

    def test_exact_same_security_rows_can_match_deterministic_filing_sum(self) -> None:
        rows = [
            information_row(infotable_sk="1", value="150000", shares="750"),
            information_row(infotable_sk="2", value="50000", shares="250"),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund_document(),
                    connection=connection,
                )
            finally:
                connection.close()
        self.assertEqual(1, stats["exact_matches"])
        self.assertEqual(
            "APPLE INC",
            updated["quarters"][0]["holdings"][0]["reported_issuer"],
        )

    def test_lossy_legacy_bucket_is_split_from_exact_sec_identities(self) -> None:
        rows = [
            information_row(
                infotable_sk="1",
                issuer="ALPHA AS FILED",
                security_class="COM A",
                value="120000",
                shares="600",
            ),
            information_row(
                infotable_sk="2",
                issuer="ALPHA HOLDINGS",
                security_class="COM B",
                value="80000",
                shares="400",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund_document(),
                    connection=connection,
                )
                verification, issues = bulk._verify_fund_document_against_index(
                    updated,
                    connection=connection,
                    require_source_provenance=True,
                )
            finally:
                connection.close()

        holdings = updated["quarters"][0]["holdings"]
        self.assertEqual(2, len(holdings))
        self.assertEqual(2, stats["exact_matches"])
        self.assertEqual(0, stats["unmatched"])
        self.assertEqual(0, stats["ambiguous"])
        self.assertEqual([], issues)
        self.assertEqual(2, verification["exact_matches"])
        self.assertEqual(
            {"ALPHA AS FILED", "ALPHA HOLDINGS"},
            {holding["reported_issuer"] for holding in holdings},
        )
        self.assertEqual({None}, {holding["ticker"] for holding in holdings})
        self.assertNotIn(
            "Canonical display issuer",
            {holding["issuer"] for holding in holdings},
        )

    def test_figi_null_multiplicity_is_reconstructed_as_an_exact_multiset(
        self,
    ) -> None:
        rows = [
            information_row(
                infotable_sk="1",
                value="100000",
                shares="500",
                figi="BBG000B9XRY4",
            ),
            information_row(
                infotable_sk="2",
                value="100000",
                shares="500",
                figi="",
            ),
        ]
        fund = fund_document()
        legacy_holding = fund["quarters"][0]["holdings"][0]
        legacy_holding["value"] = 200000
        legacy_holding["shares"] = 1000
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund,
                    connection=connection,
                )
                verification, issues = bulk._verify_fund_document_against_index(
                    updated,
                    connection=connection,
                    require_source_provenance=True,
                )
                rerun, rerun_stats = bulk.backfill_fund_document(
                    updated,
                    connection=connection,
                )
            finally:
                connection.close()

        holdings = updated["quarters"][0]["holdings"]
        self.assertEqual(2, len(holdings))
        self.assertEqual(2, stats["exact_matches"])
        self.assertEqual(0, stats["unmatched"])
        self.assertEqual(0, stats["ambiguous"])
        self.assertEqual([], issues)
        self.assertEqual(2, verification["exact_matches"])
        self.assertEqual(
            {None, "BBG000B9XRY4"},
            {holding["reported_figi"] for holding in holdings},
        )
        self.assertTrue(all("reported_figi" in holding for holding in holdings))
        self.assertEqual(updated, rerun)
        self.assertEqual(0, rerun_stats["holdings_changed"])

    def test_incomplete_figi_null_multiplicity_remains_ambiguous(self) -> None:
        rows = [
            information_row(
                infotable_sk="1",
                value="200000",
                shares="1000",
                figi="BBG000B9XRY4",
            ),
            information_row(
                infotable_sk="2",
                value="200000",
                shares="1000",
                figi="",
            ),
        ]
        fund = fund_document()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund,
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(fund, updated)
        self.assertEqual(0, stats["exact_matches"])
        self.assertEqual(1, stats["ambiguous"])
        self.assertNotIn(
            "reported_figi",
            updated["quarters"][0]["holdings"][0],
        )

    def test_reconstructed_rows_cannot_reuse_a_removed_holding_cache_entry(
        self,
    ) -> None:
        rows = [
            information_row(
                infotable_sk="1",
                issuer="ALPHA AS FILED",
                security_class="COM A",
                value="120000",
                shares="600",
            ),
            information_row(
                infotable_sk="2",
                issuer="ALPHA HOLDINGS",
                security_class="COM B",
                value="80000",
                shares="400",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                # Force the legacy row and both replacements through the same
                # cache key. Object identity must still distinguish them.
                with mock.patch.object(bulk, "id", return_value=7, create=True):
                    updated, stats = bulk.backfill_fund_document(
                        fund_document(),
                        connection=connection,
                    )
            finally:
                connection.close()

        self.assertEqual(2, len(updated["quarters"][0]["holdings"]))
        self.assertEqual(2, stats["exact_matches"])
        self.assertEqual(0, stats["ambiguous"])

    def test_clean_rebuild_can_split_lossy_legacy_bucket_before_apply(self) -> None:
        rows = [
            information_row(
                infotable_sk="1",
                issuer="ALPHA AS FILED",
                security_class="COM A",
                value="120000",
                shares="600",
            ),
            information_row(
                infotable_sk="2",
                issuer="ALPHA HOLDINGS",
                security_class="COM B",
                value="80000",
                shares="400",
            ),
        ]
        payload = dataset_zip(information=rows)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "1234567.json"
            fund_path.write_text(json.dumps(fund_document()), encoding="utf-8")

            result = bulk.rebuild_reported_identity_from_sec(
                funds_dir,
                state_path=root / "state.json",
                index_dir=root / "indices",
                dataset_urls=[DATASET_URL],
                fetcher=lambda url: payload,
            )
            rebuilt = json.loads(fund_path.read_text(encoding="utf-8"))
            verification = bulk.verify_reported_identity_against_sec(
                funds_dir,
                state_path=root / "state.json",
                verify_index_checksum=True,
                require_source_provenance=True,
            )

        self.assertEqual(2, result.backfill.exact_matches)
        self.assertEqual(2, len(rebuilt["quarters"][0]["holdings"]))
        self.assertTrue(verification.ok)
        self.assertEqual(
            {"ALPHA AS FILED", "ALPHA HOLDINGS"},
            {
                holding["reported_issuer"]
                for holding in rebuilt["quarters"][0]["holdings"]
            },
        )

    def test_duplicate_zero_value_rows_match_through_aggregate(self) -> None:
        rows = [
            information_row(infotable_sk="1", value="0", shares="5"),
            information_row(infotable_sk="2", value="0", shares="5"),
        ]
        fund = fund_document()
        holding = fund["quarters"][0]["holdings"][0]
        holding["value"] = 0
        holding["shares"] = 10
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund,
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(1, stats["exact_matches"])
        updated_holding = updated["quarters"][0]["holdings"][0]
        self.assertEqual("APPLE INC", updated_holding["reported_issuer"])
        self.assertEqual("037833100", updated_holding["reported_cusip"])

    def test_evidence_equivalent_duplicate_rows_are_not_ambiguous(self) -> None:
        rows = [
            information_row(infotable_sk="1"),
            information_row(infotable_sk="2"),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund_document(),
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(1, stats["exact_matches"])
        self.assertEqual(0, stats["ambiguous"])
        self.assertEqual(
            "APPLE INC",
            updated["quarters"][0]["holdings"][0]["reported_issuer"],
        )

    def test_failed_atomic_replace_leaves_original_fund_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "1234567.json"
            original_bytes = json.dumps(fund_document()).encode()
            fund_path.write_bytes(original_bytes)
            with mock.patch.object(
                bulk.os,
                "replace",
                side_effect=OSError("simulated atomic switch failure"),
            ):
                with self.assertRaisesRegex(OSError, "simulated"):
                    bulk.backfill_fund_files(
                        funds_dir,
                        state_path=state_path,
                    )
            self.assertEqual(original_bytes, fund_path.read_bytes())
            self.assertEqual([], list(funds_dir.glob("*.tmp")))

    def test_ambiguous_exact_rows_and_reported_field_conflicts_fail_closed(self) -> None:
        rows = [
            information_row(infotable_sk="1", issuer="APPLE INC"),
            information_row(infotable_sk="2", issuer="APPLE INC CLASS DUP"),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                ambiguous, ambiguous_stats = bulk.backfill_fund_document(
                    fund_document(),
                    connection=connection,
                )
                conflicting_fund = fund_document()
                conflicting_fund["quarters"][0]["holdings"][0][
                    "reported_issuer"
                ] = "NOT AS FILED"
                conflict, conflict_stats = bulk.backfill_fund_document(
                    conflicting_fund,
                    connection=connection,
                )
            finally:
                connection.close()

            self.assertEqual(1, ambiguous_stats["ambiguous"])
            self.assertNotIn(
                "reported_issuer",
                ambiguous["quarters"][0]["holdings"][0],
            )
            # Both evidence rows conflict with the pre-existing immutable value;
            # no partial class/CUSIP fill is allowed.
            self.assertEqual(1, conflict_stats["conflicts"])
            self.assertNotIn(
                "reported_class",
                conflict["quarters"][0]["holdings"][0],
            )

    def test_unique_position_accession_narrows_prior_ambiguous_position(self) -> None:
        second_accession = "0001234567-26-000002"
        unique_cusip = "111111111"
        shared_cusip = "333333333"
        submissions = [
            submission_row(accession=ACCESSION),
            submission_row(accession=second_accession),
        ]
        rows = [
            information_row(
                accession=ACCESSION,
                infotable_sk="1",
                cusip=unique_cusip,
                value="100",
                shares="10",
            ),
            information_row(
                accession=ACCESSION,
                infotable_sk="2",
                cusip=shared_cusip,
                value="300",
                shares="30",
            ),
            information_row(
                accession=second_accession,
                infotable_sk="3",
                cusip=shared_cusip,
                value="300",
                shares="30",
            ),
        ]
        fund = fund_document()
        quarter = fund["quarters"][0]
        quarter.pop("accession")
        quarter["holdings"] = [
            {
                "ticker": None,
                "issuer": "Unique display",
                "class": "Canonical",
                "cusip": unique_cusip,
                "value": 100,
                "shares": 10,
                "holding_type": "EQUITY",
                "share_amount_type": "SH",
            },
            {
                "ticker": None,
                "issuer": "Shared display",
                "class": "Canonical",
                "cusip": shared_cusip,
                "value": 300,
                "shares": 30,
                "holding_type": "EQUITY",
                "share_amount_type": "SH",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root,
                submissions=submissions,
                information=rows,
                target_accessions=[ACCESSION, second_accession],
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund,
                    connection=connection,
                )
                verification, issues = bulk._verify_fund_document_against_index(
                    updated,
                    connection=connection,
                    require_source_provenance=True,
                )
            finally:
                connection.close()

        self.assertEqual(2, stats["exact_matches"])
        self.assertEqual(0, stats["ambiguous"])
        self.assertEqual(2, verification["exact_matches"])
        self.assertEqual([], issues)
        holdings = updated["quarters"][0]["holdings"]
        self.assertEqual(
            [ACCESSION, ACCESSION],
            [holding["accession"] for holding in holdings],
        )

    def test_multi_component_narrowing_remains_fail_closed(self) -> None:
        second_accession = "0001234567-26-000002"
        third_accession = "0001234567-26-000003"
        unique_a_cusip = "111111111"
        unique_b_cusip = "222222222"
        narrowed_cusip = "333333333"
        still_ambiguous_cusip = "444444444"
        submissions = [
            submission_row(accession=ACCESSION),
            submission_row(accession=second_accession),
            submission_row(accession=third_accession),
        ]
        rows = [
            information_row(
                accession=ACCESSION,
                infotable_sk="1",
                cusip=unique_a_cusip,
                value="100",
                shares="10",
            ),
            information_row(
                accession=second_accession,
                infotable_sk="2",
                cusip=unique_b_cusip,
                value="200",
                shares="20",
            ),
            information_row(
                accession=second_accession,
                infotable_sk="3",
                cusip=narrowed_cusip,
                value="300",
                shares="30",
            ),
            information_row(
                accession=third_accession,
                infotable_sk="4",
                cusip=narrowed_cusip,
                value="300",
                shares="30",
            ),
            information_row(
                accession=ACCESSION,
                infotable_sk="5",
                cusip=still_ambiguous_cusip,
                value="400",
                shares="40",
            ),
            information_row(
                accession=second_accession,
                infotable_sk="6",
                cusip=still_ambiguous_cusip,
                value="400",
                shares="40",
            ),
        ]
        fund = fund_document()
        quarter = fund["quarters"][0]
        quarter.pop("accession")
        quarter["holdings"] = [
            {
                "ticker": None,
                "issuer": f"Display {cusip}",
                "class": "Canonical",
                "cusip": cusip,
                "value": value,
                "shares": shares,
                "holding_type": "EQUITY",
                "share_amount_type": "SH",
            }
            for cusip, value, shares in (
                (unique_a_cusip, 100, 10),
                (unique_b_cusip, 200, 20),
                (narrowed_cusip, 300, 30),
                (still_ambiguous_cusip, 400, 40),
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(
                root,
                submissions=submissions,
                information=rows,
                target_accessions=[
                    ACCESSION,
                    second_accession,
                    third_accession,
                ],
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund,
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(3, stats["exact_matches"])
        self.assertEqual(1, stats["ambiguous"])
        holdings = {
            holding["cusip"]: holding
            for holding in updated["quarters"][0]["holdings"]
        }
        self.assertEqual(
            second_accession,
            holdings[narrowed_cusip]["accession"],
        )
        self.assertNotIn("accession", holdings[still_ambiguous_cusip])

    def test_explicit_blank_descriptor_is_not_overwritten(self) -> None:
        fund = fund_document()
        holding = fund["quarters"][0]["holdings"][0]
        holding.update({
            "reported_issuer": "",
            "reported_class": "COM",
            "reported_cusip": "037833100",
            "accession": ACCESSION,
            "report_date": "2026-06-30",
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                updated, stats = bulk.backfill_fund_document(
                    fund,
                    connection=connection,
                )
            finally:
                connection.close()

        self.assertEqual(1, stats["conflicts"])
        self.assertEqual(
            "",
            updated["quarters"][0]["holdings"][0]["reported_issuer"],
        )

    def test_option_side_is_rebuilt_exactly_but_cusip_is_not_fuzzy(self) -> None:
        rows = [information_row(put_call="CALL")]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=rows)
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                equity, equity_stats = bulk.backfill_fund_document(
                    fund_document(),
                    connection=connection,
                )
                wrong_cusip = fund_document()
                wrong_cusip["quarters"][0]["holdings"][0]["cusip"] = "03783310O"
                fuzzy, fuzzy_stats = bulk.backfill_fund_document(
                    wrong_cusip,
                    connection=connection,
                )
            finally:
                connection.close()
            self.assertEqual(1, equity_stats["exact_matches"])
            self.assertEqual(0, equity_stats["unmatched"])
            self.assertEqual(1, fuzzy_stats["unmatched"])
            self.assertEqual(
                "CALL",
                equity["quarters"][0]["holdings"][0]["put_call"],
            )
            self.assertEqual(
                "CALL",
                equity["quarters"][0]["holdings"][0]["holding_type"],
            )
            self.assertEqual(
                "037833100",
                equity["quarters"][0]["holdings"][0]["reported_cusip"],
            )
            self.assertNotIn(
                "reported_cusip",
                fuzzy["quarters"][0]["holdings"][0],
            )

    def test_target_summary_uses_accession_and_exact_filer_period(self) -> None:
        targets = bulk.collect_backfill_targets([fund_document()])
        self.assertEqual([ACCESSION], targets["accessions"])
        self.assertEqual(
            [{"cik": "0001234567", "report_date": "2026-06-30"}],
            targets["periods"],
        )
        self.assertEqual(1, targets["holdings_targeted"])
        self.assertEqual(1, targets["holdings_missing_reported_identity"])

    def test_clean_rebuild_rejects_every_complete_wrong_immutable_field(
        self,
    ) -> None:
        wrong_values = {
            "reported_issuer": "NOT AS FILED",
            "reported_class": "PREFERRED",
            "reported_cusip": "03783310O",
            "accession": "0001234567-26-999999",
            "report_date": "2026-03-31",
        }
        payload = dataset_zip()

        def fetch(url: str) -> bytes:
            if url == DATASET_URL:
                return payload
            raise TimeoutError("archive unavailable in fixture")

        for field, wrong_value in wrong_values.items():
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                funds_dir = root / "funds"
                funds_dir.mkdir()
                fund = fund_document()
                holding = fund["quarters"][0]["holdings"][0]
                holding.update({
                    "reported_issuer": "APPLE INC",
                    "reported_class": "COM",
                    "reported_cusip": "037833100",
                    "accession": ACCESSION,
                    "report_date": "2026-06-30",
                })
                holding[field] = wrong_value
                path = funds_dir / "1234567.json"
                path.write_text(json.dumps(fund), encoding="utf-8")
                original_bytes = path.read_bytes()

                with self.assertRaisesRegex(
                    bulk.BulkIndexRefreshError,
                    "retained-identity verification failed",
                ):
                    bulk.rebuild_reported_identity_from_sec(
                        funds_dir,
                        state_path=root / "state.json",
                        index_dir=root / "indices",
                        dataset_urls=[DATASET_URL],
                        fetcher=fetch,
                    )

                # Immutable fields are evidence, never repair targets.
                self.assertEqual(original_bytes, path.read_bytes())

    def test_clean_rebuild_verifies_nonzero_malformed_as_filed_cusip(
        self,
    ) -> None:
        malformed = "000000NAN"
        fund = fund_document()
        holding = fund["quarters"][0]["holdings"][0]
        holding.update({
            "cusip": malformed,
            "reported_issuer": "APPLE INC",
            "reported_class": "COM",
            "reported_cusip": malformed,
            "accession": ACCESSION,
            "report_date": "2026-06-30",
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            path = funds_dir / "1234567.json"
            path.write_text(json.dumps(fund), encoding="utf-8")
            state_path = root / "state.json"

            bulk.rebuild_reported_identity_from_sec(
                funds_dir,
                state_path=state_path,
                index_dir=root / "indices",
                dataset_urls=[DATASET_URL],
                fetcher=lambda _url: dataset_zip(
                    information=[information_row(cusip=malformed)]
                ),
            )
            verification = bulk.verify_reported_identity_against_sec(
                funds_dir,
                state_path=state_path,
                verify_index_checksum=True,
            )

            self.assertTrue(verification.ok)
            self.assertEqual(1, verification.exact_matches)
            self.assertEqual(0, verification.placeholder_holdings)
            self.assertEqual(
                malformed,
                json.loads(path.read_text())["quarters"][0]["holdings"][0][
                    "reported_cusip"
                ],
            )

    def test_corpus_is_validated_before_any_backfill_file_is_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            funds_dir = root / "funds"
            funds_dir.mkdir()
            first = funds_dir / "0000001.json"
            first.write_text(json.dumps(fund_document()), encoding="utf-8")
            original_bytes = first.read_bytes()
            (funds_dir / "0000002.json").write_text("{not-json", encoding="utf-8")

            with self.assertRaisesRegex(bulk.Sec13FBulkError, "cannot read"):
                bulk.backfill_fund_files(funds_dir, state_path=state_path)

            self.assertEqual(original_bytes, first.read_bytes())

    def test_strict_corpus_identity_conflict_prevents_all_replacements(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.build_index(root, information=[information_row()])
            funds_dir = root / "funds"
            funds_dir.mkdir()
            first = funds_dir / "0000001.json"
            first.write_text(json.dumps(fund_document()), encoding="utf-8")
            original_bytes = first.read_bytes()
            conflicting = fund_document()
            conflicting["quarters"][0]["holdings"][0].update({
                "reported_issuer": "NOT AS FILED",
                "reported_class": "COM",
                "reported_cusip": "037833100",
                "accession": ACCESSION,
                "report_date": "2026-06-30",
            })
            (funds_dir / "0000002.json").write_text(
                json.dumps(conflicting),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                bulk.BulkIndexRefreshError,
                "verification failed before apply",
            ):
                bulk.backfill_fund_files(
                    funds_dir,
                    state_path=state_path,
                    require_all_verified=True,
                )

            self.assertEqual(original_bytes, first.read_bytes())


class SecArchiveFallbackRefreshTests(unittest.TestCase):
    def empty_scoped_index(self, root: Path) -> Path:
        state_path = root / "state.json"
        result = bulk.refresh_13f_bulk_index(
            state_path=state_path,
            index_dir=root / "indices",
            dataset_urls=[DATASET_URL],
            target_accessions=[LEGACY_ACCESSION],
            fetcher=lambda _url: dataset_zip(),
            recheck_recent_archives=0,
        )
        self.assertFalse(result.errors)
        self.assertEqual(0, result.state["summary"]["submissions"])
        return state_path

    def legacy_fund(self) -> dict:
        fund = fund_document(report_date="2004-12-31")
        quarter = fund["quarters"][0]
        quarter["accession"] = LEGACY_ACCESSION
        quarter["value_multiplier"] = 1000
        quarter["holdings"][0]["value"] = 200000
        return fund

    def test_exact_archive_fallback_is_checksummed_then_backfills(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.empty_scoped_index(root)
            index_url = bulk.sec_archive_index_url("1234567", LEGACY_ACCESSION)
            submission_url = bulk.sec_archive_submission_url(
                "1234567", LEGACY_ACCESSION
            )
            payloads = {
                index_url: archive_index_fixture(),
                submission_url: archive_submission_fixture(
                    archive_legacy_text_table()
                ),
            }
            refreshed = bulk.refresh_sec_archive_fallbacks(
                [{
                    "cik": "0001234567",
                    "accession": LEGACY_ACCESSION,
                    "report_date": "2004-12-31",
                }],
                state_path=state_path,
                index_dir=root / "indices",
                fetcher=payloads.__getitem__,
                refreshed_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
            )
            self.assertTrue(refreshed.changed)
            self.assertEqual((LEGACY_ACCESSION,), refreshed.resolved_accessions)
            source = refreshed.state["archive_sources"][submission_url]
            self.assertEqual(64, len(source["sha256"]))
            self.assertEqual(64, len(source["index_sha256"]))
            self.assertEqual("sec_archive_legacy_table", source["method"])

            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "1234567.json"
            fund_path.write_text(json.dumps(self.legacy_fund()))
            applied = bulk.backfill_fund_files(
                funds_dir,
                state_path=state_path,
                verify_index_checksum=True,
            )
            persisted = json.loads(fund_path.read_text())
            quarter = persisted["quarters"][0]
            holding = quarter["holdings"][0]
            self.assertEqual(1, applied.holdings_changed)
            self.assertEqual("APPLE INC", holding["reported_issuer"])
            self.assertEqual(LEGACY_ACCESSION, holding["accession"])
            self.assertEqual(
                [{
                    "accession": LEGACY_ACCESSION,
                    "report_date": "2004-12-31",
                    "url": submission_url,
                    "sha256": source["sha256"],
                }],
                quarter["reported_identity_sources"],
            )
            self.assertEqual(
                1,
                len(list((root / "indices").glob("index-*.sqlite3"))),
            )
            provenance_before_cleanup = copy.deepcopy(
                quarter["reported_identity_sources"]
            )
            bulk.cleanup_13f_bulk_working_set(
                state_path=state_path,
                index_dir=root / "indices",
                checkpoint_path=root / "checkpoint.json",
            )
            persisted_after_cleanup = json.loads(fund_path.read_text())
            self.assertEqual(
                provenance_before_cleanup,
                persisted_after_cleanup["quarters"][0][
                    "reported_identity_sources"
                ],
            )

    def test_existing_bulk_accession_is_enriched_from_exact_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            index_dir = root / "indices"
            bulk_payload = dataset_zip(
                submissions=[submission_row(
                    accession=LEGACY_ACCESSION,
                    report_date="31-DEC-2004",
                )],
                information=[information_row(
                    accession=LEGACY_ACCESSION,
                    value="200",
                )],
            )
            initial = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=index_dir,
                dataset_urls=[DATASET_URL],
                target_accessions=[LEGACY_ACCESSION],
                target_periods=[("1234567", "2004-12-31")],
                fetcher=lambda _url: bulk_payload,
                recheck_recent_archives=0,
            )
            self.assertFalse(initial.errors)
            clean_plan_sha256 = "d" * 64
            initial_state = json.loads(state_path.read_text(encoding="utf-8"))
            initial_state["clean_rebuild_plan_sha256"] = clean_plan_sha256
            state_path.write_text(
                json.dumps(initial_state),
                encoding="utf-8",
            )
            index_url = bulk.sec_archive_index_url("1234567", LEGACY_ACCESSION)
            submission_url = bulk.sec_archive_submission_url(
                "1234567", LEGACY_ACCESSION
            )
            payloads = {
                index_url: archive_index_fixture(),
                submission_url: archive_submission_fixture(
                    archive_xml_table(),
                    acceptance_datetime="20050214123456",
                    cover=archive_cover(),
                ),
            }
            refreshed = bulk.refresh_sec_archive_fallbacks(
                [{
                    "cik": "1234567",
                    "accession": LEGACY_ACCESSION,
                    "report_date": "2004-12-31",
                }],
                state_path=state_path,
                index_dir=index_dir,
                fetcher=payloads.__getitem__,
            )
            self.assertTrue(refreshed.changed)
            self.assertEqual((LEGACY_ACCESSION,), refreshed.resolved_accessions)
            self.assertEqual(
                clean_plan_sha256,
                refreshed.state["clean_rebuild_plan_sha256"],
            )
            state = bulk.load_13f_bulk_index(state_path)
            index_path = bulk._index_path_from_state(state, state_path)
            connection = bulk._open_index(index_path, read_only=True)
            try:
                chain = connection.execute(
                    "SELECT * FROM filing_chain WHERE accession = ?",
                    (LEGACY_ACCESSION,),
                ).fetchone()
                row_source = connection.execute(
                    "SELECT source_url FROM information_table WHERE accession = ?",
                    (LEGACY_ACCESSION,),
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual("20050214123456", chain["acceptance_datetime"])
            self.assertEqual(1, chain["cover_metadata_consistent"])
            self.assertEqual(submission_url, row_source)

    def test_clean_refresh_integrates_fallback_before_single_finalization(
        self,
    ) -> None:
        index_url = bulk.sec_archive_index_url("1234567", LEGACY_ACCESSION)
        submission_url = bulk.sec_archive_submission_url(
            "1234567", LEGACY_ACCESSION
        )
        payloads = {
            DATASET_URL: dataset_zip(submissions=[], information=[]),
            index_url: archive_index_fixture(),
            submission_url: archive_submission_fixture(
                archive_legacy_text_table()
            ),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with (
                mock.patch.object(
                    bulk,
                    "_finalize_index",
                    wraps=bulk._finalize_index,
                ) as finalize,
                mock.patch.object(
                    bulk,
                    "_copy_index",
                    wraps=bulk._copy_index,
                ) as copy_index,
            ):
                result = bulk.refresh_13f_bulk_index(
                    state_path=root / "state.json",
                    index_dir=root / "indices",
                    dataset_urls=[DATASET_URL],
                    target_accessions=[LEGACY_ACCESSION],
                    target_periods=[("1234567", "2004-12-31")],
                    archive_fallback_targets=[{
                        "cik": "1234567",
                        "accession": LEGACY_ACCESSION,
                        "report_date": "2004-12-31",
                    }],
                    fetcher=payloads.__getitem__,
                    full_rebuild=True,
                    recheck_recent_archives=0,
                )

            self.assertFalse(result.errors)
            self.assertEqual(1, finalize.call_count)
            copy_index.assert_not_called()
            self.assertEqual(1, result.state["summary"]["archive_filings"])
            self.assertEqual(1, len(list((root / "indices").iterdir())))

    def test_exact_archive_repairs_bulk_filer_cik_misbinding(self) -> None:
        index_url = bulk.sec_archive_index_url("1234567", LEGACY_ACCESSION)
        submission_url = bulk.sec_archive_submission_url(
            "1234567", LEGACY_ACCESSION
        )
        payloads = {
            DATASET_URL: dataset_zip(
                submissions=[submission_row(
                    accession=LEGACY_ACCESSION,
                    cik="1960050",
                    report_date="31-DEC-2004",
                )],
                information=[information_row(
                    accession=LEGACY_ACCESSION,
                    value="200",
                )],
            ),
            index_url: archive_index_fixture(),
            submission_url: archive_submission_fixture(
                archive_xml_table(),
                acceptance_datetime="20050214123456",
                cover=archive_cover(),
            ),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            result = bulk.refresh_13f_bulk_index(
                state_path=state_path,
                index_dir=root / "indices",
                dataset_urls=[DATASET_URL],
                target_accessions=[LEGACY_ACCESSION],
                target_periods=[("1234567", "2004-12-31")],
                archive_fallback_targets=[{
                    "cik": "1234567",
                    "accession": LEGACY_ACCESSION,
                    "report_date": "2004-12-31",
                }],
                fetcher=payloads.__getitem__,
                full_rebuild=True,
                recheck_recent_archives=0,
            )
            self.assertFalse(result.errors)
            state = bulk.load_13f_bulk_index(state_path)
            connection = bulk._open_index(
                bulk._index_path_from_state(state, state_path),
                read_only=True,
            )
            try:
                submission = connection.execute(
                    "SELECT cik, report_date, source_url FROM submissions "
                    "WHERE accession = ?",
                    (LEGACY_ACCESSION,),
                ).fetchone()
            finally:
                connection.close()
            self.assertEqual("0001234567", submission["cik"])
            self.assertEqual("2004-12-31", submission["report_date"])
            self.assertEqual(submission_url, submission["source_url"])

    def test_final_cleanup_removes_only_private_managed_working_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.empty_scoped_index(root)
            index_dir = root / "indices"
            checkpoint = root / "checkpoint.json"
            checkpoint.write_text("{}", encoding="utf-8")
            discovery_checkpoint = (
                bulk._accession_discovery_checkpoint_path(checkpoint)
            )
            discovery_checkpoint.parent.mkdir(parents=True, exist_ok=True)
            discovery_checkpoint.write_text("{}", encoding="utf-8")
            receipt = root / bulk.DEFAULT_COMPLETED_RECEIPT_PATH.name
            receipt.write_text("{}", encoding="utf-8")
            unrelated = index_dir / "keep-me.txt"
            unrelated.write_text("user file", encoding="utf-8")

            removed = bulk.cleanup_13f_bulk_working_set(
                state_path=state_path,
                index_dir=index_dir,
                checkpoint_path=checkpoint,
            )

            self.assertIn(state_path, removed)
            self.assertIn(checkpoint, removed)
            self.assertIn(discovery_checkpoint, removed)
            self.assertIn(receipt, removed)
            self.assertFalse(state_path.exists())
            self.assertFalse(discovery_checkpoint.exists())
            self.assertFalse(receipt.exists())
            self.assertFalse(list(index_dir.glob("index-*.sqlite3")))
            self.assertEqual("user file", unrelated.read_text(encoding="utf-8"))

    def test_transient_archive_failure_preserves_state_and_fund_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = self.empty_scoped_index(root)
            state_bytes = state_path.read_bytes()
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "1234567.json"
            fund_path.write_text(json.dumps(self.legacy_fund()))
            fund_bytes = fund_path.read_bytes()

            def fail(_url: str) -> bytes:
                raise TimeoutError("temporary SEC timeout")

            result = bulk.refresh_sec_archive_fallbacks(
                [{
                    "cik": "1234567",
                    "accession": LEGACY_ACCESSION,
                    "report_date": "2004-12-31",
                }],
                state_path=state_path,
                index_dir=root / "indices",
                fetcher=fail,
            )
            self.assertFalse(result.changed)
            self.assertEqual(1, len(result.unresolved))
            self.assertIn("timeout", result.unresolved[0]["reason"])
            self.assertEqual(state_bytes, state_path.read_bytes())
            self.assertEqual(fund_bytes, fund_path.read_bytes())


class ReportedIdentityCompletenessAuditTests(unittest.TestCase):
    def complete_fund(self) -> dict:
        fund = fund_document()
        fund["quarters"][0]["reported_identity_sources"] = [{
            "accession": ACCESSION,
            "report_date": "2026-06-30",
            "url": DATASET_URL,
            "sha256": "a" * 64,
        }]
        fund["quarters"][0]["holdings"][0].update({
            "reported_issuer": "APPLE INC",
            "reported_class": "COM",
            "reported_cusip": "037833100",
            "accession": ACCESSION,
            "report_date": "2026-06-30",
        })
        return fund

    def test_complete_corpus_skips_backfill_and_partial_provenance_requires_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            path = funds_dir / "1234567.json"
            complete = self.complete_fund()
            path.write_text(json.dumps(complete), encoding="utf-8")
            self.assertFalse(bulk.reported_identity_backfill_needed(funds_dir))
            targets = bulk.collect_backfill_targets_from_funds(funds_dir)
            self.assertEqual(1, targets["holdings_targeted"])
            self.assertEqual(0, targets["holdings_missing_reported_identity"])

            del complete["quarters"][0]["holdings"][0]["accession"]
            path.write_text(json.dumps(complete), encoding="utf-8")
            audit = bulk.reported_identity_backfill_audit(funds_dir)
            self.assertTrue(audit["needed"])
            self.assertEqual(1, audit["incomplete_holdings"])
            self.assertEqual(1, audit["missing_or_invalid_fields"]["accession"])

    def test_explicit_empty_descriptor_is_complete_but_missing_key_is_not(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            path = funds_dir / "1234567.json"
            complete = self.complete_fund()
            holding = complete["quarters"][0]["holdings"][0]
            holding["reported_issuer"] = ""
            path.write_text(json.dumps(complete), encoding="utf-8")

            exact_empty = bulk.reported_identity_backfill_audit(funds_dir)
            self.assertFalse(exact_empty["needed"])
            self.assertEqual(
                0,
                exact_empty["missing_or_invalid_fields"]["reported_issuer"],
            )

            del holding["reported_issuer"]
            path.write_text(json.dumps(complete), encoding="utf-8")
            missing = bulk.reported_identity_backfill_audit(funds_dir)
            self.assertTrue(missing["needed"])
            self.assertEqual(
                1,
                missing["missing_or_invalid_fields"]["reported_issuer"],
            )

    def test_placeholder_holding_is_not_a_download_target(self) -> None:
        placeholder = fund_document(holding={
            "cusip": "000000000",
            "issuer": "N/A",
            "class": "N/A",
            "value": 0,
            "shares": 0,
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            (funds_dir / "1234567.json").write_text(json.dumps(placeholder))
            audit = bulk.reported_identity_backfill_audit(funds_dir)
            self.assertFalse(audit["needed"])
            self.assertEqual(1, audit["placeholder_holdings"])

            fetch = mock.Mock(side_effect=AssertionError("must not fetch"))
            rebuilt = bulk.rebuild_reported_identity_from_sec(
                funds_dir,
                state_path=funds_dir / "state.json",
                index_dir=funds_dir / "indices",
                fetcher=fetch,
            )
            self.assertEqual(0, rebuilt.backfill.holdings_scanned)
            fetch.assert_not_called()

    def test_nonzero_placeholder_shaped_identifier_is_not_exempt(self) -> None:
        malformed = fund_document(holding={
            "cusip": "000000000",
            "issuer": "N/A",
            "class": "N/A",
            "value": 1,
            "shares": 0,
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            (funds_dir / "1234567.json").write_text(json.dumps(malformed))
            audit = bulk.reported_identity_backfill_audit(funds_dir)
            targets = bulk.collect_backfill_targets_from_funds(funds_dir)

            self.assertTrue(audit["needed"])
            self.assertEqual(0, audit["placeholder_holdings"])
            self.assertEqual(1, audit["holdings_scanned"])
            self.assertEqual(1, targets["holdings_targeted"])

    def test_malformed_json_and_missing_directory_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            (funds_dir / "bad.json").write_text("{not-json", encoding="utf-8")
            audit = bulk.reported_identity_backfill_audit(funds_dir)
            self.assertTrue(audit["needed"])
            self.assertEqual(["bad.json"], audit["malformed_files"])
            self.assertTrue(
                bulk.reported_identity_backfill_needed(root / "missing")
            )


if __name__ == "__main__":
    unittest.main()
