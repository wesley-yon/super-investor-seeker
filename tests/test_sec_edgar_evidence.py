from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from urllib.parse import urlsplit

import sec_edgar_evidence as evidence
import sec_security_master as security_master_module


SCHEDULE_ACCESSION = "0000000123-26-000001"
SCHEDULE_URL = (
    "https://www.sec.gov/Archives/edgar/data/123/000000012326000001/primary_doc.xml"
)
IXBRL_ACCESSION = "0001652044-26-000002"
IXBRL_URL = (
    "https://www.sec.gov/Archives/edgar/data/1652044/"
    "000165204426000002/goog-20260630.htm"
)
SECOND_SCHEDULE_ACCESSION = "0000000456-26-000002"
SECOND_SCHEDULE_URL = (
    "https://www.sec.gov/Archives/edgar/data/456/000000045626000002/primary_doc.xml"
)
FTD_PROOF_URL = (
    "https://www.sec.gov/files/data/fails-deliver-data/cnsfails202606b.zip"
)
FTD_PROOF_SHA256 = "d" * 64
COMPANY_PROOF_SHA256 = "e" * 64


def schedule_fixture(
    *,
    security_class: str = "Class A Common Stock",
    cusip: str = "02079K305",
    issuer_cik: str = "0001652044",
) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13G">
  <schemaVersion>X0202</schemaVersion>
  <headerData><submissionType>SCHEDULE 13G</submissionType></headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>{security_class}</securitiesClassTitle>
      <eventDateRequiresFilingThisStatement>06/30/2026</eventDateRequiresFilingThisStatement>
      <issuerInfo>
        <issuerCik>{issuer_cik}</issuerCik>
        <issuerName>Alphabet Inc.</issuerName>
        <issuerCusips><issuerCusipNumber>{cusip}</issuerCusipNumber></issuerCusips>
      </issuerInfo>
    </coverPageHeader>
  </formData>
</edgarSubmission>""".encode()


def context_xml(context_id: str, member: str | None = None) -> str:
    segment = ""
    if member:
        segment = f"""<xbrli:segment>
          <xbrldi:explicitMember dimension="us-gaap:StatementClassOfStockAxis">{member}</xbrldi:explicitMember>
        </xbrli:segment>"""
    return f"""<xbrli:context id="{context_id}">
      <xbrli:entity>
        <xbrli:identifier scheme="http://www.sec.gov/CIK">0001652044</xbrli:identifier>
        {segment}
      </xbrli:entity>
      <xbrli:period>
        <xbrli:startDate>2025-07-01</xbrli:startDate>
        <xbrli:endDate>2026-06-30</xbrli:endDate>
      </xbrli:period>
    </xbrli:context>"""


def ixbrl_fixture(
    *,
    class_a_title: str = "Class A Common Stock",
    extra_contexts: str = "",
    extra_facts: str = "",
) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:dei="http://xbrl.sec.gov/dei/2026"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:us-gaap="http://fasb.org/us-gaap/2026">
  <body>
    <div style="display:none">
      <ix:header>
        <ix:hidden>
          <ix:nonNumeric contextRef="document" name="dei:EntityCentralIndexKey">0001652044</ix:nonNumeric>
          <ix:nonNumeric contextRef="document" name="dei:DocumentType">10-K</ix:nonNumeric>
          <ix:nonNumeric contextRef="document" name="dei:DocumentPeriodEndDate">June 30, 2026</ix:nonNumeric>
        </ix:hidden>
        <ix:resources>
          {context_xml("document")}
          {context_xml("class-a", "us-gaap:ClassACommonStockMember")}
          {context_xml("class-c", "us-gaap:ClassCCommonStockMember")}
          {extra_contexts}
        </ix:resources>
      </ix:header>
    </div>
    <ix:nonNumeric contextRef="class-a" name="dei:Security12bTitle">{class_a_title}</ix:nonNumeric>
    <ix:nonNumeric contextRef="class-a" name="dei:TradingSymbol">GOOGL</ix:nonNumeric>
    <ix:nonNumeric contextRef="class-a" name="dei:SecurityExchangeName">NASDAQ</ix:nonNumeric>
    <ix:nonNumeric contextRef="class-c" name="dei:Security12bTitle">Class C Common Stock</ix:nonNumeric>
    <ix:nonNumeric contextRef="class-c" name="dei:TradingSymbol">GOOG</ix:nonNumeric>
    <ix:nonNumeric contextRef="class-c" name="dei:SecurityExchangeName">NASDAQ</ix:nonNumeric>
    {extra_facts}
  </body>
</html>""".encode()


def search_hit(
    *,
    accession: str = SCHEDULE_ACCESSION,
    archive_cik: str = "0000000123",
    filing_date: str = "2026-07-02",
    primary_document: str = "primary_doc.xml",
) -> dict:
    return {
        "_id": f"{accession}:{primary_document}",
        "_source": {
            "adsh": accession,
            "ciks": [archive_cik],
            "file_date": filing_date,
            "form": "SCHEDULE 13G",
            "root_forms": ["SCHEDULE 13G"],
            "schema_version": "X0202",
            "xsl": "xslSCHEDULE_13G_X02",
        },
    }


def search_fixture(*hits: dict, total: int | None = None) -> bytes:
    count = len(hits) if total is None else total
    return json.dumps(
        {
            "hits": {
                "total": {"value": count, "relation": "eq"},
                "hits": list(hits),
            }
        },
        sort_keys=True,
    ).encode()


def submissions_fixture() -> bytes:
    return json.dumps(
        {
            "cik": 1652044,
            "filings": {
                "recent": {
                    "accessionNumber": [IXBRL_ACCESSION],
                    "filingDate": ["2026-08-01"],
                    "reportDate": ["2026-06-30"],
                    "form": ["10-K"],
                    "isInlineXBRL": [1],
                    "primaryDocument": ["goog-20260630.htm"],
                }
            },
        },
        sort_keys=True,
    ).encode()


class FakeResponse:
    def __init__(
        self,
        url: str,
        *,
        status_code: int = 200,
        content: bytes = b"payload",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.url = url
        self.status_code = status_code
        self.content = content
        self.headers = {} if headers is None else dict(headers)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise evidence.requests.HTTPError(
                f"HTTP {self.status_code}",
                response=self,
            )


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls = []

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def refreshed_cache(*, security_class: str = "Class A Common Stock") -> dict:
    payloads = {
        SCHEDULE_URL: schedule_fixture(security_class=security_class),
        IXBRL_URL: ixbrl_fixture(class_a_title=security_class),
    }
    sources = [
        evidence.FilingSource(
            evidence.SCHEDULE_13DG,
            SCHEDULE_URL,
            SCHEDULE_ACCESSION,
        ),
        evidence.FilingSource(
            evidence.PERIODIC_IXBRL,
            IXBRL_URL,
            IXBRL_ACCESSION,
        ),
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        return evidence.refresh_sec_edgar_evidence(
            sources,
            cache_path=Path(tmpdir) / "sec_edgar_evidence.json",
            fetcher=payloads.__getitem__,
            refreshed_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )


def master_record(
    *,
    cusip: str = "02079K305",
    instrument_type: str = "EQUITY",
    status: str = "unresolved",
    ticker: str | None = None,
    ticker_source: str | None = None,
    ticker_as_of: str | None = None,
    reported_issuer: str = "Alphabet Inc.",
    reported_class: str = "Class A Common Stock",
) -> dict:
    record = {
        "cusip": cusip,
        "instrument_type": instrument_type,
        "mapping_status": status,
        "ticker": ticker,
        "ticker_source": ticker_source,
        "ticker_as_of": ticker_as_of,
        "last_verification_date": ticker_as_of if status == "resolved" else None,
        "resolution_reason": "fixture",
        "symbol_evidence": [],
        "reported_issuer": reported_issuer,
        "reported_issuers": [reported_issuer],
        "reported_class": reported_class,
        "reported_classes": [reported_class],
    }
    if status == "resolved" and ticker_source == "sec_ftd":
        assert ticker is not None and ticker_as_of is not None
        prior_date = (
            date.fromisoformat(ticker_as_of) - timedelta(days=1)
        ).isoformat()
        source_ref = {"url": FTD_PROOF_URL, "sha256": FTD_PROOF_SHA256}
        symbol_evidence = [
            {
                "settlement_date": observed_at,
                "symbol": ticker,
                "observation_count": 1,
                "descriptions": [reported_issuer],
                "sources": [source_ref],
            }
            for observed_at in (prior_date, ticker_as_of)
        ]
        record.update({
            "mapping_method": "exact_ftd_symbol_with_sec_metadata_validation",
            "effective_from": prior_date,
            "effective_to": None,
            "confirmation_dates": [prior_date, ticker_as_of],
            "symbol_validation_sources": ["sec_company_tickers"],
            "symbol_validation_titles": [reported_issuer],
            "symbol_validation_exchanges": [],
            "symbol_evidence": symbol_evidence,
            "symbol_intervals": security_master_module._build_symbol_intervals(
                symbol_evidence
            ),
        })
    return record


def security_master(*records: dict) -> dict:
    by_key = {
        f"{record['cusip']}|{record['instrument_type']}": record for record in records
    }
    statuses = {
        "ambiguous": 0,
        "malformed_as_filed": 0,
        "no_listed_symbol": 0,
        "resolved": 0,
        "unresolved": 0,
    }
    for record in records:
        statuses[record["mapping_status"]] += 1
    sources = []
    if any(
        record.get("mapping_status") == "resolved"
        and record.get("ticker_source") == "sec_ftd"
        for record in records
    ):
        sources = sorted(
            [
                {
                    "url": security_master_module.SEC_COMPANY_TICKERS_URL,
                    "sha256": COMPANY_PROOF_SHA256,
                    "kind": "sec_company_tickers",
                    "schema_sha256": "f" * 64,
                },
                {
                    "url": FTD_PROOF_URL,
                    "sha256": FTD_PROOF_SHA256,
                    "kind": "sec_ftd_archive",
                    "schema_sha256": "a" * 64,
                },
            ],
            key=lambda source: (
                source["url"],
                source["kind"],
                source["sha256"],
            ),
        )
    return {
        "schema_version": 1,
        "generated_at": "2026-06-30T00:00:00Z",
        "source_state_sha256": "0" * 64,
        "universe_sha256": "1" * 64,
        "policy": {"min_confirmation_dates": 2},
        "sources": sources,
        "records": by_key,
        "quarantine": {},
        "summary": statuses,
    }


class SecEdgarEvidenceParserTests(unittest.TestCase):
    def test_schedule_parser_proves_exact_cover_identity(self) -> None:
        records = evidence.parse_schedule_13dg_xml(
            schedule_fixture(),
            accession=SCHEDULE_ACCESSION,
            source_url=SCHEDULE_URL,
        )

        self.assertEqual(1, len(records))
        self.assertEqual(
            {
                "kind": "schedule_13dg",
                "cusip": "02079K305",
                "issuer_cik": "0001652044",
                "issuer_name": "Alphabet Inc.",
                "security_class": "Class A Common Stock",
                "security_class_key": "class a common stock",
                "filing_type": "SCHEDULE 13G",
                "accession": SCHEDULE_ACCESSION,
                "url": SCHEDULE_URL,
                "as_of": "2026-06-30",
            },
            records[0],
        )

    def test_ixbrl_parser_keeps_multiclass_facts_in_their_contexts(self) -> None:
        records = evidence.parse_periodic_ixbrl(
            ixbrl_fixture(),
            accession=IXBRL_ACCESSION,
            source_url=IXBRL_URL,
        )

        self.assertEqual(["GOOGL", "GOOG"], [row["ticker"] for row in records])
        by_ticker = {row["ticker"]: row for row in records}
        self.assertEqual("Class A Common Stock", by_ticker["GOOGL"]["security_class"])
        self.assertEqual("class-a", by_ticker["GOOGL"]["context_id"])
        self.assertEqual(
            [
                {
                    "dimension": "us-gaap:StatementClassOfStockAxis",
                    "member": "us-gaap:ClassACommonStockMember",
                }
            ],
            by_ticker["GOOGL"]["dimensions"],
        )
        self.assertEqual("Class C Common Stock", by_ticker["GOOG"]["security_class"])
        self.assertEqual("class-c", by_ticker["GOOG"]["context_id"])
        self.assertEqual("NASDAQ", by_ticker["GOOG"]["exchange"])

    def test_exact_class_bridge_resolves_cusip_and_preserves_proof(self) -> None:
        schedule = evidence.parse_schedule_13dg_xml(
            schedule_fixture(),
            accession=SCHEDULE_ACCESSION,
            source_url=SCHEDULE_URL,
        )
        periodic = evidence.parse_periodic_ixbrl(
            ixbrl_fixture(),
            accession=IXBRL_ACCESSION,
            source_url=IXBRL_URL,
        )

        self.assertEqual(
            [
                {
                    "cusip": "02079K305",
                    "issuer_cik": "0001652044",
                    "issuer_name": "Alphabet Inc.",
                    "security_class": "Class A Common Stock",
                    "ticker": "GOOGL",
                    "exchange": "NASDAQ",
                    "exchanges": ["NASDAQ"],
                    "mapping_status": "resolved",
                    "cusip_source": "sec_schedule_13dg",
                    "ticker_source": "sec_ixbrl",
                    "ticker_as_of": "2026-06-30",
                    "schedule_13dg_accession": SCHEDULE_ACCESSION,
                    "schedule_13dg_url": SCHEDULE_URL,
                    "schedule_13dg_as_of": "2026-06-30",
                    "ixbrl_accession": IXBRL_ACCESSION,
                    "ixbrl_url": IXBRL_URL,
                    "ixbrl_as_of": "2026-06-30",
                    "ixbrl_context_ids": ["class-a"],
                }
            ],
            evidence.bridge_sec_evidence(schedule, periodic),
        )

    def test_near_class_name_does_not_resolve(self) -> None:
        schedule = evidence.parse_schedule_13dg_xml(
            schedule_fixture(security_class="Class A Common Shares"),
            accession=SCHEDULE_ACCESSION,
            source_url=SCHEDULE_URL,
        )
        periodic = evidence.parse_periodic_ixbrl(
            ixbrl_fixture(),
            accession=IXBRL_ACCESSION,
            source_url=IXBRL_URL,
        )

        self.assertEqual([], evidence.bridge_sec_evidence(schedule, periodic))

    def test_conflicting_same_class_contexts_fail_closed(self) -> None:
        conflict_context = context_xml(
            "class-a-conflict",
            "us-gaap:ClassACommonStockMember",
        )
        conflict_facts = """
          <ix:nonNumeric contextRef="class-a-conflict" name="dei:Security12bTitle">Class A Common Stock</ix:nonNumeric>
          <ix:nonNumeric contextRef="class-a-conflict" name="dei:TradingSymbol">WRONG</ix:nonNumeric>
          <ix:nonNumeric contextRef="class-a-conflict" name="dei:SecurityExchangeName">NASDAQ</ix:nonNumeric>
        """
        schedule = evidence.parse_schedule_13dg_xml(
            schedule_fixture(),
            accession=SCHEDULE_ACCESSION,
            source_url=SCHEDULE_URL,
        )
        periodic = evidence.parse_periodic_ixbrl(
            ixbrl_fixture(
                extra_contexts=conflict_context,
                extra_facts=conflict_facts,
            ),
            accession=IXBRL_ACCESSION,
            source_url=IXBRL_URL,
        )

        self.assertEqual([], evidence.bridge_sec_evidence(schedule, periodic))

    def test_invalid_duplicate_fact_cannot_be_ignored(self) -> None:
        periodic = evidence.parse_periodic_ixbrl(
            ixbrl_fixture(
                extra_facts="""
                  <ix:nonNumeric contextRef="class-a" name="dei:TradingSymbol">not a symbol</ix:nonNumeric>
                """,
            ),
            accession=IXBRL_ACCESSION,
            source_url=IXBRL_URL,
        )

        self.assertEqual(["GOOG"], [record["ticker"] for record in periodic])

    def test_facts_from_different_contexts_are_not_cross_joined(self) -> None:
        split_facts = f"""<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:dei="http://xbrl.sec.gov/dei/2026"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:xbrli="http://www.xbrl.org/2003/instance">
  <body><ix:header><ix:hidden>
    <ix:nonNumeric contextRef="document" name="dei:EntityCentralIndexKey">0001652044</ix:nonNumeric>
    <ix:nonNumeric contextRef="document" name="dei:DocumentType">10-K</ix:nonNumeric>
    <ix:nonNumeric contextRef="document" name="dei:DocumentPeriodEndDate">June 30, 2026</ix:nonNumeric>
  </ix:hidden><ix:resources>
    {context_xml("document")}
    {context_xml("title")}
    {context_xml("listing")}
  </ix:resources></ix:header>
  <ix:nonNumeric contextRef="title" name="dei:Security12bTitle">Class A Common Stock</ix:nonNumeric>
  <ix:nonNumeric contextRef="listing" name="dei:TradingSymbol">GOOGL</ix:nonNumeric>
  <ix:nonNumeric contextRef="listing" name="dei:SecurityExchangeName">NASDAQ</ix:nonNumeric>
  </body>
</html>""".encode()

        self.assertEqual(
            [],
            evidence.parse_periodic_ixbrl(
                split_facts,
                accession=IXBRL_ACCESSION,
                source_url=IXBRL_URL,
            ),
        )

    def test_exchange_transformation_uses_sec_code(self) -> None:
        payload = ixbrl_fixture().replace(
            b">NASDAQ</ix:nonNumeric>",
            b' format="ixt-sec:exchnameen">The Nasdaq Stock Market LLC</ix:nonNumeric>',
        )
        records = evidence.parse_periodic_ixbrl(
            payload,
            accession=IXBRL_ACCESSION,
            source_url=IXBRL_URL,
        )

        self.assertEqual({"NASDAQ"}, {record["exchange"] for record in records})

    def test_malformed_documents_are_rejected(self) -> None:
        with self.assertRaises(evidence.EvidenceParseError):
            evidence.parse_schedule_13dg_xml(
                b"<not-xml",
                accession=SCHEDULE_ACCESSION,
                source_url=SCHEDULE_URL,
            )
        with self.assertRaises(evidence.EvidenceParseError):
            evidence.parse_schedule_13dg_xml(
                schedule_fixture(cusip="02079K304"),
                accession=SCHEDULE_ACCESSION,
                source_url=SCHEDULE_URL,
            )
        with self.assertRaises(evidence.EvidenceParseError):
            evidence.parse_periodic_ixbrl(
                b"<html><broken></html>",
                accession=IXBRL_ACCESSION,
                source_url=IXBRL_URL,
            )


class SecEdgarEvidenceFetcherTests(unittest.TestCase):
    def test_fetcher_rejects_non_sec_url_before_network(self) -> None:
        session = FakeSession([])
        fetcher = evidence.make_sec_discovery_fetcher(
            "Test Agent test@example.com",
            session=session,
            max_attempts=1,
        )

        with self.assertRaises(evidence.NonSECFilingURL):
            fetcher("https://example.com/search?q=02079K305")

        self.assertEqual([], session.calls)

    def test_fetcher_revalidates_redirect_destination(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")
        session = FakeSession(
            [FakeResponse("https://example.com/LATEST/search-index?q=02079K305")]
        )
        fetcher = evidence.make_sec_discovery_fetcher(
            "Test Agent test@example.com",
            session=session,
            max_attempts=1,
        )

        with self.assertRaises(evidence.NonSECFilingURL):
            fetcher(search_url)

        self.assertEqual(1, len(session.calls))
        self.assertIs(False, session.calls[0][1]["allow_redirects"])

    def test_discovery_fetcher_never_sends_user_agent_to_redirect_target(
        self,
    ) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")
        session = FakeSession([
            FakeResponse(
                search_url,
                status_code=302,
                headers={"Location": "https://example.com/collect"},
            )
        ])
        fetcher = evidence.make_sec_discovery_fetcher(
            "Private Agent private@example.test",
            session=session,
            max_attempts=1,
            pace=lambda: None,
        )

        with self.assertRaises(evidence.NonSECFilingURL):
            fetcher(search_url)

        self.assertEqual([search_url], [call[0] for call in session.calls])
        self.assertIs(False, session.calls[0][1]["allow_redirects"])

    def test_discovery_fetcher_validates_and_paces_safe_filing_redirect(
        self,
    ) -> None:
        path = urlsplit(SCHEDULE_URL).path
        redirected_url = f"https://sec.gov{path}"
        session = FakeSession([
            FakeResponse(
                SCHEDULE_URL,
                status_code=302,
                headers={"Location": redirected_url},
            ),
            FakeResponse(redirected_url, content=b"filing"),
        ])
        shared_pace = mock.Mock()
        fetcher = evidence.make_sec_discovery_fetcher(
            "Private Agent private@example.test",
            session=session,
            max_attempts=1,
            pace=shared_pace,
        )

        self.assertEqual(b"filing", fetcher(SCHEDULE_URL))

        self.assertEqual(
            [SCHEDULE_URL, redirected_url],
            [call[0] for call in session.calls],
        )
        self.assertEqual(2, shared_pace.call_count)
        self.assertTrue(all(
            call[1]["allow_redirects"] is False for call in session.calls
        ))

    def test_filing_fetcher_validates_each_redirect_before_request(self) -> None:
        path = urlsplit(SCHEDULE_URL).path
        redirected_url = f"https://sec.gov{path}"
        session = FakeSession([
            FakeResponse(
                SCHEDULE_URL,
                status_code=302,
                headers={"Location": redirected_url},
            ),
            FakeResponse(redirected_url, content=b"filing"),
        ])
        fetcher = evidence.make_sec_filing_fetcher(
            "Private Agent private@example.test",
            session=session,
        )

        self.assertEqual(b"filing", fetcher(SCHEDULE_URL))

        self.assertEqual(
            [SCHEDULE_URL, redirected_url],
            [call[0] for call in session.calls],
        )
        self.assertTrue(all(
            call[1]["allow_redirects"] is False for call in session.calls
        ))

    def test_filing_fetcher_does_not_follow_unsafe_redirects(self) -> None:
        for target in (
            "https://example.com/collect",
            SCHEDULE_URL.replace("https://", "http://", 1),
            SECOND_SCHEDULE_URL,
        ):
            with self.subTest(target=target):
                session = FakeSession([
                    FakeResponse(
                        SCHEDULE_URL,
                        status_code=302,
                        headers={"Location": target},
                    )
                ])
                fetcher = evidence.make_sec_filing_fetcher(
                    "Private Agent private@example.test",
                    session=session,
                )

                with self.assertRaises(evidence.NonSECFilingURL):
                    fetcher(SCHEDULE_URL)

                self.assertEqual(
                    [SCHEDULE_URL],
                    [call[0] for call in session.calls],
                )
                self.assertIs(
                    False,
                    session.calls[0][1]["allow_redirects"],
                )

    def test_fetcher_retries_only_bounded_transient_responses(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")
        session = FakeSession(
            [
                FakeResponse(search_url, status_code=503),
                FakeResponse(search_url, status_code=429),
                FakeResponse(search_url, content=b"success"),
            ]
        )
        shared_pace = mock.Mock()
        with mock.patch.object(evidence.time, "sleep") as sleep:
            fetcher = evidence.make_sec_discovery_fetcher(
                "Test Agent test@example.com",
                session=session,
                max_attempts=3,
                backoff_seconds=0.25,
                pace=shared_pace,
            )

            self.assertEqual(b"success", fetcher(search_url))

        self.assertEqual(3, len(session.calls))
        self.assertEqual(3, shared_pace.call_count)
        self.assertEqual([mock.call(0.25), mock.call(0.5)], sleep.call_args_list)

    def test_fetcher_stops_at_configured_attempt_bound(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")
        session = FakeSession(
            [
                FakeResponse(search_url, status_code=503),
                FakeResponse(search_url, status_code=503),
                FakeResponse(search_url, content=b"must-not-fetch"),
            ]
        )
        with mock.patch.object(evidence.time, "sleep"):
            fetcher = evidence.make_sec_discovery_fetcher(
                "Test Agent test@example.com",
                session=session,
                max_attempts=2,
                backoff_seconds=0.01,
                pace=lambda: None,
            )

            with self.assertRaises(evidence.requests.HTTPError):
                fetcher(search_url)

        self.assertEqual(2, len(session.calls))

    def test_fetcher_spaces_request_starts_at_no_more_than_eight_per_second(
        self,
    ) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")
        session = FakeSession([FakeResponse(search_url), FakeResponse(search_url)])
        with (
            mock.patch.object(
                evidence.time,
                "monotonic",
                side_effect=[100.0, 100.0],
            ),
            mock.patch.object(evidence.time, "sleep") as sleep,
        ):
            fetcher = evidence.make_sec_discovery_fetcher(
                "Test Agent test@example.com",
                session=session,
                max_attempts=1,
                max_requests_per_second=8,
            )
            fetcher(search_url)
            fetcher(search_url)

        self.assertEqual(2, len(session.calls))
        sleep.assert_called_once_with(0.125)


class SecEdgarEvidenceDiscoveryTests(unittest.TestCase):
    def test_corrupt_search_response_is_retryable(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")

        result = evidence.discover_sec_edgar_sources(
            ["02079K305"],
            fetcher={search_url: b"{"}.__getitem__,
        )

        self.assertEqual("transient_error", result.diagnostics[0].status)
        self.assertFalse(result.diagnostics[0].terminal)
        self.assertEqual(
            "malformed_search_response",
            result.diagnostics[0].reason,
        )

    def test_decoded_search_contract_change_is_fatal(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")

        with self.assertRaises(evidence.EvidenceSchemaError):
            evidence.discover_sec_edgar_sources(
                ["02079K305"],
                fetcher={search_url: b'{"hits": []}'}.__getitem__,
            )

    def test_decoded_submissions_contract_change_is_fatal(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")
        submissions_url = evidence.build_sec_submissions_url("0001652044")
        payloads = {
            search_url: search_fixture(search_hit()),
            SCHEDULE_URL: schedule_fixture(),
            submissions_url: b'{"cik": 1652044, "filings": {}}',
        }

        with self.assertRaises(evidence.EvidenceSchemaError):
            evidence.discover_sec_edgar_sources(
                ["02079K305"],
                fetcher=payloads.__getitem__,
            )

    def test_corrupt_schedule_document_is_retryable(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")

        result = evidence.discover_sec_edgar_sources(
            ["02079K305"],
            fetcher={
                search_url: search_fixture(search_hit()),
                SCHEDULE_URL: b"<malformed",
            }.__getitem__,
        )

        self.assertEqual("transient_error", result.diagnostics[0].status)
        self.assertFalse(result.diagnostics[0].terminal)
        self.assertEqual(
            "schedule_candidate_fetch_or_parse_failed",
            result.diagnostics[0].reason,
        )

    def test_zero_hits_is_terminal_no_evidence(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")

        result = evidence.discover_sec_edgar_sources(
            ["02079K305"],
            fetcher={search_url: search_fixture()}.__getitem__,
        )

        self.assertEqual((), result.sources)
        self.assertEqual(1, len(result.fetched_sources))
        self.assertRegex(result.fetched_sources[0].sha256 or "", r"^[0-9a-f]{64}$")
        self.assertEqual(
            evidence.DiscoveryDiagnostic(
                cusip="02079K305",
                status="no_evidence",
                terminal=True,
                reason="no_structured_schedule_hits",
            ),
            result.diagnostics[0],
        )

    def test_wrong_cusip_search_hit_cannot_become_a_source(self) -> None:
        requested_cusip = "594918104"
        search_url = evidence.build_sec_cusip_search_url(requested_cusip)
        fetches = {
            search_url: search_fixture(search_hit()),
            SCHEDULE_URL: schedule_fixture(cusip="02079K305"),
        }

        result = evidence.discover_sec_edgar_sources(
            [requested_cusip],
            fetcher=fetches.__getitem__,
        )

        self.assertEqual((), result.sources)
        self.assertEqual("no_evidence", result.diagnostics[0].status)
        self.assertEqual("no_exact_schedule_cusip", result.diagnostics[0].reason)
        self.assertTrue(result.diagnostics[0].terminal)
        self.assertEqual(1, result.diagnostics[0].schedule_candidate_count)
        self.assertEqual(0, result.diagnostics[0].exact_schedule_count)
        self.assertNotIn(
            evidence.build_sec_submissions_url("0001652044"),
            {item.url for item in result.fetched_sources},
        )

    def test_conflicting_schedule_issuer_or_class_fails_closed(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")
        search_payload = search_fixture(
            search_hit(),
            search_hit(
                accession=SECOND_SCHEDULE_ACCESSION,
                archive_cik="0000000456",
                filing_date="2026-07-03",
            ),
        )
        conflict_documents = {
            "issuer": schedule_fixture(
                issuer_cik="0000789019",
            ),
            "class": schedule_fixture(
                security_class="Class C Common Stock",
            ),
        }
        for conflict_kind, second_document in conflict_documents.items():
            with self.subTest(conflict=conflict_kind):
                result = evidence.discover_sec_edgar_sources(
                    ["02079K305"],
                    fetcher={
                        search_url: search_payload,
                        SCHEDULE_URL: schedule_fixture(),
                        SECOND_SCHEDULE_URL: second_document,
                    }.__getitem__,
                )

                self.assertEqual((), result.sources)
                self.assertEqual("conflict", result.diagnostics[0].status)
                self.assertTrue(result.diagnostics[0].terminal)
                self.assertEqual(
                    "conflicting_schedule_identities",
                    result.diagnostics[0].reason,
                )
                self.assertEqual(2, result.diagnostics[0].exact_schedule_count)

    def test_discovery_is_deterministic_and_refresh_compatible(self) -> None:
        search_url = evidence.build_sec_cusip_search_url("02079K305")
        submissions_url = evidence.build_sec_submissions_url("0001652044")
        discovery_payloads = {
            search_url: search_fixture(search_hit()),
            SCHEDULE_URL: schedule_fixture(),
            submissions_url: submissions_fixture(),
            IXBRL_URL: ixbrl_fixture(),
        }
        pace_calls = []

        first = evidence.discover_sec_edgar_sources(
            ["02079K305", "02079K305"],
            fetcher=discovery_payloads.__getitem__,
            pace=lambda: pace_calls.append("fetch"),
        )
        second = evidence.discover_sec_edgar_sources(
            "02079K305",
            fetcher=discovery_payloads.__getitem__,
        )

        self.assertEqual(first, second)
        self.assertEqual(first.to_dict(), second.to_dict())
        self.assertEqual(4, len(pace_calls))
        self.assertEqual(
            {
                evidence.FilingSource(
                    evidence.SCHEDULE_13DG,
                    SCHEDULE_URL,
                    SCHEDULE_ACCESSION,
                ),
                evidence.FilingSource(
                    evidence.PERIODIC_IXBRL,
                    IXBRL_URL,
                    IXBRL_ACCESSION,
                ),
            },
            set(first.sources),
        )
        self.assertEqual("sources_found", first.diagnostics[0].status)
        self.assertTrue(first.diagnostics[0].terminal)
        self.assertEqual("0001652044", first.diagnostics[0].issuer_cik)
        self.assertEqual("Class A Common Stock", first.diagnostics[0].security_class)
        self.assertEqual(4, len(first.fetched_sources))
        self.assertTrue(
            all(item.outcome == "fetched" for item in first.fetched_sources)
        )
        self.assertTrue(
            all(
                item.url.startswith("https://")
                and item.sha256 is not None
                and len(item.sha256) == 64
                for item in first.fetched_sources
            )
        )

        cache = evidence.refresh_sec_edgar_evidence(
            first.sources,
            cache_path=None,
            fetcher={
                SCHEDULE_URL: schedule_fixture(),
                IXBRL_URL: ixbrl_fixture(),
            }.__getitem__,
            refreshed_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )
        self.assertEqual("GOOGL", cache["records"]["02079K305"]["ticker"])
        evidence.validate_sec_edgar_evidence_cache(cache)

    def test_transient_fetch_failure_is_not_terminal_no_evidence(self) -> None:
        def unavailable(_url: str) -> bytes:
            raise ConnectionError("temporary SEC failure")

        result = evidence.discover_sec_edgar_sources(
            ["02079K305"],
            fetcher=unavailable,
        )

        self.assertEqual((), result.sources)
        self.assertEqual("transient_error", result.diagnostics[0].status)
        self.assertFalse(result.diagnostics[0].terminal)
        self.assertEqual("search_fetch_failed", result.diagnostics[0].reason)
        self.assertEqual("transient_error", result.fetched_sources[0].outcome)
        self.assertIsNone(result.fetched_sources[0].sha256)


class SecEdgarEvidenceRefreshTests(unittest.TestCase):
    def test_cache_merge_replaces_documents_without_duplicate_or_refetch(self) -> None:
        existing = refreshed_cache()

        merged = evidence.merge_sec_edgar_evidence_caches(
            existing,
            existing,
            refreshed_at=datetime(2026, 7, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(existing["summary"], merged["summary"])
        self.assertEqual(existing["records"], merged["records"])
        evidence.validate_sec_edgar_evidence_cache(merged)

        retired = evidence.merge_sec_edgar_evidence_caches(
            existing,
            None,
            retired_urls={SCHEDULE_URL},
            refreshed_at=datetime(2026, 7, 2, tzinfo=timezone.utc),
        )
        self.assertEqual({}, retired["records"])
        self.assertEqual([IXBRL_URL], [row["url"] for row in retired["sources"]])
        evidence.validate_sec_edgar_evidence_cache(retired)

    def test_non_sec_and_mismatched_accession_urls_fail_before_fetch(self) -> None:
        calls = []

        def fetcher(url: str) -> bytes:
            calls.append(url)
            return b""

        unsafe_sources = [
            evidence.FilingSource(
                evidence.SCHEDULE_13DG,
                SCHEDULE_URL.replace("www.sec.gov", "www.sec.gov.evil.test"),
                SCHEDULE_ACCESSION,
            ),
            evidence.FilingSource(
                evidence.SCHEDULE_13DG,
                SCHEDULE_URL.replace("https://", "http://"),
                SCHEDULE_ACCESSION,
            ),
            evidence.FilingSource(
                evidence.SCHEDULE_13DG,
                SCHEDULE_URL,
                "0000000123-26-999999",
            ),
        ]
        for source in unsafe_sources:
            with self.subTest(url=source.url, accession=source.accession):
                with self.assertRaises(evidence.SecEdgarEvidenceError):
                    evidence.refresh_sec_edgar_evidence(
                        [source],
                        fetcher=fetcher,
                    )
        self.assertEqual([], calls)

    def test_explicit_source_refresh_persists_deterministic_exact_proof(self) -> None:
        payloads = {
            SCHEDULE_URL: schedule_fixture(),
            IXBRL_URL: ixbrl_fixture(),
        }
        sources = [
            evidence.FilingSource(
                evidence.SCHEDULE_13DG,
                SCHEDULE_URL,
                SCHEDULE_ACCESSION,
            ),
            evidence.FilingSource(
                evidence.PERIODIC_IXBRL,
                IXBRL_URL,
                IXBRL_ACCESSION,
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / ".cache" / "sec_edgar_evidence.json"
            cache = evidence.refresh_sec_edgar_evidence(
                sources,
                cache_path=cache_path,
                fetcher=payloads.__getitem__,
                refreshed_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            )

            self.assertEqual(cache, json.loads(cache_path.read_text()))

        self.assertEqual("2026-07-01T00:00:00Z", cache["generated_at"])
        self.assertEqual(
            {
                "source_count": 2,
                "schedule_record_count": 1,
                "ixbrl_record_count": 2,
                "resolved_count": 1,
                "unresolved_count": 0,
            },
            cache["summary"],
        )
        resolved = cache["records"]["02079K305"]
        self.assertEqual("GOOGL", resolved["ticker"])
        self.assertEqual(SCHEDULE_URL, resolved["schedule_13dg_url"])
        self.assertEqual(IXBRL_URL, resolved["ixbrl_url"])
        self.assertEqual("2026-06-30", resolved["ticker_as_of"])
        self.assertTrue(
            all(
                source["url"].startswith("https://www.sec.gov/Archives/edgar/data/")
                for source in cache["sources"]
            )
        )

    def test_in_memory_refresh_validates_without_writing_third_file(self) -> None:
        payloads = {
            SCHEDULE_URL: schedule_fixture(),
            IXBRL_URL: ixbrl_fixture(),
        }
        sources = [
            evidence.FilingSource(
                evidence.SCHEDULE_13DG,
                SCHEDULE_URL,
                SCHEDULE_ACCESSION,
            ),
            evidence.FilingSource(
                evidence.PERIODIC_IXBRL,
                IXBRL_URL,
                IXBRL_ACCESSION,
            ),
        ]
        with mock.patch.object(evidence, "_atomic_write_json") as atomic_write:
            cache = evidence.refresh_sec_edgar_evidence(
                sources,
                cache_path=None,
                fetcher=payloads.__getitem__,
                refreshed_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            )

        atomic_write.assert_not_called()
        evidence.validate_sec_edgar_evidence_cache(cache)

    def test_public_cache_validator_rejects_summary_tampering(self) -> None:
        cache = refreshed_cache()
        cache["summary"]["resolved_count"] += 1

        with self.assertRaisesRegex(
            evidence.SecEdgarEvidenceError,
            "summary mismatch",
        ):
            evidence.validate_sec_edgar_evidence_cache(cache)

    def test_failed_refresh_leaves_last_cache_untouched(self) -> None:
        source = evidence.FilingSource(
            evidence.SCHEDULE_13DG,
            SCHEDULE_URL,
            SCHEDULE_ACCESSION,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "sec_edgar_evidence.json"
            cache_path.write_text('{"generation":"last-good"}\n')

            with self.assertRaises(evidence.EvidenceRefreshError):
                evidence.refresh_sec_edgar_evidence(
                    [source],
                    cache_path=cache_path,
                    fetcher=lambda _url: b"<malformed",
                )

            self.assertEqual(
                {"generation": "last-good"},
                json.loads(cache_path.read_text()),
            )

    def test_atomic_replace_failure_cleans_temp_and_preserves_last_cache(self) -> None:
        payloads = {
            SCHEDULE_URL: schedule_fixture(),
            IXBRL_URL: ixbrl_fixture(),
        }
        sources = [
            {
                "kind": evidence.SCHEDULE_13DG,
                "url": SCHEDULE_URL,
                "accession": SCHEDULE_ACCESSION,
            },
            {
                "kind": evidence.PERIODIC_IXBRL,
                "url": IXBRL_URL,
                "accession": IXBRL_ACCESSION,
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "sec_edgar_evidence.json"
            cache_path.write_text('{"generation":"last-good"}\n')
            with mock.patch.object(
                evidence.os,
                "replace",
                side_effect=OSError("simulated replace failure"),
            ):
                with self.assertRaises(evidence.EvidenceRefreshError):
                    evidence.refresh_sec_edgar_evidence(
                        sources,
                        cache_path=cache_path,
                        fetcher=payloads.__getitem__,
                    )

            self.assertEqual(
                {"generation": "last-good"},
                json.loads(cache_path.read_text()),
            )
            self.assertEqual([], list(Path(tmpdir).glob(".*.tmp")))


class SecEdgarEvidenceMasterApplicationTests(unittest.TestCase):
    def test_exact_depositary_share_classes_are_equity_eligible(self) -> None:
        for security_class in (
            "ADR",
            "ADS",
            "American Depositary Shares",
            "Global Depositary Receipts",
            "GDR",
        ):
            with self.subTest(security_class=security_class):
                updated = evidence.apply_sec_edgar_evidence(
                    security_master(master_record(reported_class=security_class)),
                    refreshed_cache(security_class=security_class),
                )

                result = updated["records"]["02079K305|EQUITY"]
                self.assertEqual("resolved", result["mapping_status"])
                self.assertEqual("GOOGL", result["ticker"])
                self.assertEqual("sec_ixbrl", result["ticker_source"])

    def test_depositary_wording_does_not_override_preferred_or_debt(self) -> None:
        for security_class in (
            "American Depositary Shares Representing Series A Preferred Stock",
            "American Depositary Shares Representing Senior Notes",
        ):
            with self.subTest(security_class=security_class):
                updated = evidence.apply_sec_edgar_evidence(
                    security_master(master_record(reported_class=security_class)),
                    refreshed_cache(security_class=security_class),
                )

                result = updated["records"]["02079K305|EQUITY"]
                self.assertEqual("ambiguous", result["mapping_status"])
                self.assertIsNone(result["ticker"])
                self.assertEqual(
                    "sec_edgar_security_class_instrument_type_conflict",
                    result["resolution_reason"],
                )

    def test_applies_only_exact_eligible_record_and_preserves_ftd_result(self) -> None:
        unresolved = master_record()
        ftd_resolved = master_record(
            cusip="037833100",
            status="resolved",
            ticker="AAPL",
            ticker_source="sec_ftd",
            ticker_as_of="2026-06-29",
            reported_issuer="Apple Inc.",
            reported_class="COM",
        )
        master = security_master(unresolved, ftd_resolved)
        original = json.loads(json.dumps(master))
        cache = refreshed_cache()

        updated = evidence.apply_sec_edgar_evidence(master, cache)

        self.assertEqual(original, master)
        resolved = updated["records"]["02079K305|EQUITY"]
        self.assertEqual("resolved", resolved["mapping_status"])
        self.assertEqual("GOOGL", resolved["ticker"])
        self.assertEqual("sec_ixbrl", resolved["ticker_source"])
        self.assertEqual("2026-06-30", resolved["ticker_as_of"])
        self.assertEqual("2026-06-30", resolved["last_verification_date"])
        self.assertEqual(
            "exact_sec_schedule_13dg_ixbrl_class_bridge",
            resolved["resolution_reason"],
        )
        self.assertEqual(
            SCHEDULE_URL, resolved["sec_edgar_evidence"]["schedule_13dg"]["url"]
        )
        self.assertRegex(
            resolved["sec_edgar_evidence"]["ixbrl"]["sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertEqual(
            original["records"]["037833100|EQUITY"],
            updated["records"]["037833100|EQUITY"],
        )
        self.assertEqual(4, len(updated["sources"]))
        self.assertEqual(updated, evidence.apply_sec_edgar_evidence(updated, cache))

        tampered = json.loads(json.dumps(updated))
        tampered["records"]["02079K305|EQUITY"]["sec_edgar_evidence"][
            "ixbrl"
        ]["sha256"] = "9" * 64
        with self.assertRaises(security_master_module.SecurityMasterError):
            security_master_module.validate_security_master(tampered)

    def test_conflicting_resolved_ftd_record_is_never_overwritten(self) -> None:
        ftd_record = master_record(
            status="resolved",
            ticker="GOOG",
            ticker_source="sec_ftd",
            ticker_as_of="2026-06-29",
        )
        master = security_master(ftd_record)

        updated = evidence.apply_sec_edgar_evidence(master, refreshed_cache())

        self.assertEqual(
            master["records"]["02079K305|EQUITY"],
            updated["records"]["02079K305|EQUITY"],
        )

    def test_stale_ixbrl_bridge_cannot_remain_a_current_ticker(self) -> None:
        stale_master = security_master(master_record())
        stale_master["generated_at"] = "2027-08-01T00:00:00Z"

        updated = evidence.apply_sec_edgar_evidence(
            stale_master,
            refreshed_cache(),
        )

        result = updated["records"]["02079K305|EQUITY"]
        self.assertEqual("unresolved", result["mapping_status"])
        self.assertEqual(
            "sec_edgar_evidence_is_stale",
            result["resolution_reason"],
        )
        self.assertIsNone(result["ticker"])
        self.assertIsNone(result["ticker_source"])
        self.assertEqual("rejected", result["sec_edgar_evidence"]["status"])
        self.assertEqual(
            "sec_edgar_evidence_is_stale",
            result["sec_edgar_evidence"]["reason"],
        )

    def test_issuer_and_class_conflicts_fail_closed(self) -> None:
        cache = refreshed_cache()
        fixtures = {
            "issuer": master_record(reported_issuer="Different Corporation"),
            "class": master_record(reported_class="Class C Common Stock"),
            "type": master_record(
                instrument_type="PREF",
                reported_class="Class A Preferred Stock",
            ),
        }
        for conflict_kind, record in fixtures.items():
            with self.subTest(conflict=conflict_kind):
                updated = evidence.apply_sec_edgar_evidence(
                    security_master(record),
                    cache,
                )
                result = next(iter(updated["records"].values()))
                self.assertEqual("ambiguous", result["mapping_status"])
                self.assertIsNone(result["ticker"])
                self.assertIsNone(result["ticker_source"])
                self.assertIsNone(result["ticker_as_of"])
                self.assertIsNone(result["last_verification_date"])
                self.assertIn("conflict", result["resolution_reason"])

    def test_conflicting_ixbrl_cache_candidates_fail_closed(self) -> None:
        cache = refreshed_cache()
        conflicting = dict(cache["ixbrl_evidence"][0])
        conflicting["ticker"] = "WRONG"
        conflicting["context_id"] = "class-a-conflict"
        cache["ixbrl_evidence"].append(conflicting)

        updated = evidence.apply_sec_edgar_evidence(
            security_master(master_record()),
            cache,
        )

        result = updated["records"]["02079K305|EQUITY"]
        self.assertEqual("ambiguous", result["mapping_status"])
        self.assertEqual("conflicting_sec_ixbrl_tickers", result["resolution_reason"])
        self.assertIsNone(result["ticker"])

    def test_tampered_non_sec_evidence_reference_fails_closed(self) -> None:
        cache = refreshed_cache()
        unsafe_url = "https://example.com/Archives/edgar/data/123/filing.xml"
        cache["schedule_evidence"][0]["url"] = unsafe_url
        cache["records"]["02079K305"]["schedule_13dg_url"] = unsafe_url

        updated = evidence.apply_sec_edgar_evidence(
            security_master(master_record()),
            cache,
        )

        result = updated["records"]["02079K305|EQUITY"]
        self.assertEqual("ambiguous", result["mapping_status"])
        self.assertEqual(
            "invalid_sec_edgar_evidence_reference",
            result["resolution_reason"],
        )
        self.assertIsNone(result["ticker"])

    def test_ineligible_type_and_status_are_unchanged(self) -> None:
        note = master_record(
            instrument_type="NOTE",
            reported_class="Senior Notes",
        )
        deleted = master_record(status="no_listed_symbol")
        master = security_master(note, deleted)

        updated = evidence.apply_sec_edgar_evidence(master, refreshed_cache())

        self.assertEqual(master["records"], updated["records"])


if __name__ == "__main__":
    unittest.main()
