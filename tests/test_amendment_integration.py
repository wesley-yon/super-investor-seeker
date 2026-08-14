"""Integration coverage for authoritative 13F quarter replay."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pipeline


CIK = 1393818
REPORT_DATE = "2025-12-31"
BASE_ACCESSION = "0001193125-26-054623"
ADDITION_ACCESSION = "0001193125-26-226614"


def holding(cusip: str, value: int) -> dict:
    return {
        "ticker": None,
        "issuer": f"ISSUER {cusip}",
        "cusip": cusip,
        "class": "COM",
        "value": value,
        "shares": value,
        "holding_type": "EQUITY",
    }


def component(
    accession: str,
    kind: str,
    accepted_at: str,
    rows: list[dict],
    amendment_number: int | None = None,
) -> dict:
    return {
        "cik": CIK,
        "report_date": REPORT_DATE,
        "filing_date": accepted_at[:10],
        "accepted_at": accepted_at,
        "accession": accession,
        "form_type": "13F-HR" if kind == "ORIGINAL" else "13F-HR/A",
        "amendment_number": amendment_number,
        "amendment_kind": kind,
        "reported_entry_total": len(rows),
        "reported_value_total": sum(row["value"] for row in rows),
        "cover_reported_entry_total": len(rows),
        "cover_reported_value_total": sum(row["value"] for row in rows),
        "cover_reconciliation_status": "EXACT",
        "source_hash": hashlib.sha256(accession.encode()).hexdigest(),
        "holdings": rows,
    }


def filing_row(accession: str, form_type: str, accepted_at: str) -> dict:
    return {
        "cik": CIK,
        "name": "Blackstone Inc.",
        "form_type": form_type,
        "accession": accession,
        "date_filed": accepted_at[:10],
        "accepted_at": accepted_at,
        "report_date": REPORT_DATE,
        "filename": "",
    }


BASE_ROW = filing_row(
    BASE_ACCESSION, "13F-HR", "2026-02-17T21:10:04.000Z"
)
ADDITION_ROW = filing_row(
    ADDITION_ACCESSION, "13F-HR/A", "2026-05-15T20:04:03.000Z"
)
BASE_COMPONENT = component(
    BASE_ACCESSION,
    "ORIGINAL",
    BASE_ROW["accepted_at"],
    [holding("111111111", 100), holding("222222222", 200)],
)
ADDITION_COMPONENT = component(
    ADDITION_ACCESSION,
    "NEW_HOLDINGS",
    ADDITION_ROW["accepted_at"],
    [holding(f"33333333{index}", index) for index in range(1, 6)],
    amendment_number=1,
)


class DiscoveryTests(unittest.TestCase):
    def test_quarter_limit_keeps_every_component_in_oldest_selected_quarter(self) -> None:
        rows = [
            {**BASE_ROW, "report_date": "2025-09-30"},
            {**ADDITION_ROW, "report_date": "2025-09-30"},
            {**BASE_ROW, "accession": "newest", "report_date": "2025-12-31"},
            {**BASE_ROW, "accession": "oldest", "report_date": "2025-06-30"},
        ]
        with mock.patch.object(
            pipeline, "_discover_submission_filings", return_value=(rows, "Fund")
        ):
            selected, _ = pipeline.get_13f_filings_for_cik(CIK, 2)

        self.assertEqual(
            {"newest", BASE_ACCESSION, ADDITION_ACCESSION},
            {row["accession"] for row in selected},
        )

    def test_mismatched_submission_arrays_fail_closed(self) -> None:
        payload = {
            "form": ["13F-HR"],
            "accessionNumber": [BASE_ACCESSION],
            "filingDate": ["2026-02-17"],
            "reportDate": [],
        }
        with self.assertRaises(pipeline.FilingDiscoveryError):
            pipeline._submission_rows(payload, CIK, "Fund", "fixture")


class FilingComponentTests(unittest.TestCase):
    class Response:
        def __init__(self, *, payload=None, content: bytes = b"") -> None:
            self.payload = payload
            self.content = content

        def json(self):
            return self.payload

    @staticmethod
    def primary(
        value_total: int,
        entry_total: int = 1,
        *,
        filer_cik: int | None = CIK,
        manager_name: str | None = "Blackstone Inc.",
    ) -> bytes:
        identity = "" if filer_cik is None else f"""
          <filer><credentials><cik>{filer_cik:010d}</cik></credentials></filer>
          <filingManager><name>{manager_name or ''}</name></filingManager>
        """
        return f"""<edgarSubmission>
          <periodOfReport>12-31-2025</periodOfReport>
          {identity}
          <isAmendment>false</isAmendment>
          <tableEntryTotal>{entry_total}</tableEntryTotal>
          <tableValueTotal>{value_total}</tableValueTotal>
        </edgarSubmission>""".encode()

    INFO_TABLE = b"""<informationTable>
      <infoTable>
        <nameOfIssuer>EXAMPLE INC</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>111111111</cusip>
        <value>100</value>
        <shrsOrPrnAmt><sshPrnamt>10</sshPrnamt></shrsOrPrnAmt>
        <investmentDiscretion>SHARED-DEFINED</investmentDiscretion>
        <otherManager>07</otherManager>
      </infoTable>
    </informationTable>"""

    def responses(
        self,
        value_total: int,
        info_table: bytes | None = None,
        entry_total: int = 1,
        *,
        filer_cik: int | None = CIK,
        manager_name: str | None = "Blackstone Inc.",
    ):
        index = self.Response(payload={
            "directory": {"item": [
                {"name": "primary_doc.xml"},
                {"name": "information_table.xml"},
            ]}
        })
        primary = self.Response(content=self.primary(
            value_total,
            entry_total,
            filer_cik=filer_cik,
            manager_name=manager_name,
        ))
        information = self.Response(content=info_table or self.INFO_TABLE)

        def get(url: str):
            if url.endswith("index.json"):
                return index
            if url.endswith("primary_doc.xml"):
                return primary
            if url.endswith("information_table.xml"):
                return information
            raise AssertionError(url)

        return get

    def test_component_reconciles_cover_totals_before_composition(self) -> None:
        with (
            mock.patch.object(
                pipeline.HTTP,
                "get",
                side_effect=self.responses(value_total=100),
            ),
            mock.patch.object(
                pipeline,
                "load_prior_value_unit_context",
                return_value=(None, None),
            ),
        ):
            parsed = pipeline.fetch_filing_holdings(
                CIK, BASE_ACCESSION, filing=BASE_ROW
            )

        self.assertEqual("ORIGINAL", parsed["amendment_kind"])
        self.assertEqual(1, parsed["reported_entry_total"])
        self.assertEqual(100, parsed["reported_value_total"])
        self.assertEqual(1, parsed["cover_reported_entry_total"])
        self.assertEqual(100, parsed["cover_reported_value_total"])
        self.assertEqual("EXACT", parsed["cover_reconciliation_status"])
        self.assertEqual(1, parsed["value_multiplier"])
        self.assertEqual(100, parsed["normalized_value_total"])
        self.assertEqual(
            "weighted_equity_dollars",
            parsed["value_unit_method"],
        )
        self.assertEqual(1, len(parsed["holdings"]))

    def test_component_rejects_primary_filer_cik_conflict(self) -> None:
        with mock.patch.object(
            pipeline.HTTP,
            "get",
            side_effect=self.responses(
                value_total=100,
                filer_cik=1845943,
            ),
        ):
            with self.assertRaisesRegex(
                pipeline.FilingParseError,
                "filer CIK conflict",
            ):
                pipeline.fetch_filing_holdings(
                    CIK,
                    BASE_ACCESSION,
                    filing=BASE_ROW,
                )

    def test_component_records_prior_manager_name_without_quarantining_cik(self) -> None:
        with (
            mock.patch.object(
                pipeline.HTTP,
                "get",
                side_effect=self.responses(
                    value_total=100,
                    manager_name="Prior Adviser Name, LLC",
                ),
            ),
            mock.patch.object(
                pipeline,
                "load_prior_value_unit_context",
                return_value=(None, None),
            ),
            self.assertLogs(pipeline.log, level="WARNING") as captured,
        ):
            parsed = pipeline.fetch_filing_holdings(
                CIK,
                BASE_ACCESSION,
                filing=BASE_ROW,
            )

        self.assertEqual(
            {
                "discovery_name": "Blackstone Inc.",
                "primary_name": "Prior Adviser Name, LLC",
            },
            parsed["filer_name_discrepancy"],
        )
        self.assertTrue(any(
            "filing-manager name differs" in message
            for message in captured.output
        ))

    def test_component_passes_trusted_adjacent_holdings_to_normalizer(
        self,
    ) -> None:
        adjacent = [holding("111111111", 100)]
        with (
            mock.patch.object(
                pipeline.HTTP,
                "get",
                side_effect=self.responses(value_total=100),
            ),
            mock.patch.object(
                pipeline,
                "load_prior_value_unit_context",
                return_value=(1, adjacent),
            ),
            mock.patch.object(
                pipeline,
                "load_peer_value_unit_prices",
                return_value={},
            ),
            mock.patch.object(
                pipeline,
                "normalize_value_units",
                wraps=pipeline.normalize_value_units,
            ) as normalizer,
        ):
            pipeline.fetch_filing_holdings(
                CIK,
                BASE_ACCESSION,
                filing=BASE_ROW,
            )

        self.assertEqual(1, normalizer.call_args.kwargs["prior_multiplier"])
        self.assertIs(
            adjacent,
            normalizer.call_args.kwargs["adjacent_holdings"],
        )

    def test_adjacent_unit_contradiction_becomes_parse_failure(self) -> None:
        with (
            mock.patch.object(
                pipeline.HTTP,
                "get",
                side_effect=self.responses(value_total=100),
            ),
            mock.patch.object(
                pipeline,
                "normalize_value_units",
                side_effect=pipeline.AmbiguousValueUnits("contradiction"),
            ),
        ):
            with self.assertRaisesRegex(
                pipeline.FilingParseError,
                "ambiguous value units.*contradiction",
            ):
                pipeline.fetch_filing_holdings(
                    CIK,
                    BASE_ACCESSION,
                    filing=BASE_ROW,
                )

    def test_manager_fields_survive_information_table_parse(self) -> None:
        parsed = pipeline.parse_information_table(self.INFO_TABLE)

        self.assertIsNotNone(parsed)
        self.assertEqual("SHARED-DEFINED", parsed[0]["investment_discretion"])
        self.assertEqual("07", parsed[0]["other_manager"])

    def test_exhausted_index_fetch_is_classified_as_transient(self) -> None:
        with mock.patch.object(
            pipeline.HTTP,
            "get",
            side_effect=RuntimeError("GET failed after retries"),
        ):
            with self.assertRaisesRegex(
                pipeline.FilingFetchError,
                "index fetch failed",
            ):
                pipeline.fetch_filing_holdings(
                    CIK, BASE_ACCESSION, filing=BASE_ROW
                )

    def test_invalid_index_payload_is_a_transient_fetch_failure(self) -> None:
        response = mock.Mock()
        response.json.side_effect = ValueError("invalid JSON")
        with mock.patch.object(pipeline.HTTP, "get", return_value=response):
            with self.assertRaisesRegex(
                pipeline.FilingFetchError,
                "index response is not valid JSON",
            ):
                pipeline.fetch_filing_holdings(
                    CIK, BASE_ACCESSION, filing=BASE_ROW
                )

    def test_malformed_index_shape_is_a_scoped_parse_failure(self) -> None:
        for payload, message in (
            ({"directory": None}, "index directory"),
            ({"directory": {"item": {}}}, "index item list"),
            ({"directory": {"item": [{"name": None}]}}, "index item name"),
        ):
            with self.subTest(payload=payload):
                response = mock.Mock()
                response.json.return_value = payload
                with mock.patch.object(
                    pipeline.HTTP, "get", return_value=response
                ):
                    with self.assertRaisesRegex(
                        pipeline.FilingParseError,
                        message,
                    ) as raised:
                        pipeline.fetch_filing_holdings(
                            CIK, BASE_ACCESSION, filing=BASE_ROW
                        )
                self.assertNotIsInstance(
                    raised.exception,
                    pipeline.FilingFetchError,
                )

    def test_unique_complete_table_survives_cover_value_mismatch(self) -> None:
        with (
            mock.patch.object(
                pipeline.HTTP,
                "get",
                side_effect=self.responses(value_total=99),
            ),
            mock.patch.object(
                pipeline,
                "load_prior_value_unit_context",
                return_value=(None, None),
            ),
        ):
            parsed = pipeline.fetch_filing_holdings(
                CIK, BASE_ACCESSION, filing=BASE_ROW
            )

        self.assertEqual(100, parsed["reported_value_total"])
        self.assertEqual(99, parsed["cover_reported_value_total"])
        self.assertEqual(
            "MISMATCH_UNIQUE_TABLE",
            parsed["cover_reconciliation_status"],
        )

    def test_multiple_complete_tables_with_cover_mismatch_fail_closed(
        self,
    ) -> None:
        index = self.Response(payload={
            "directory": {"item": [
                {"name": "primary_doc.xml"},
                {"name": "information_table_a.xml"},
                {"name": "information_table_b.xml"},
            ]}
        })
        primary = self.Response(content=self.primary(99))
        information = self.Response(content=self.INFO_TABLE)

        def get(url: str):
            if url.endswith("index.json"):
                return index
            if url.endswith("primary_doc.xml"):
                return primary
            if "information_table_" in url:
                return information
            raise AssertionError(url)

        with mock.patch.object(pipeline.HTTP, "get", side_effect=get):
            with self.assertRaisesRegex(
                pipeline.FilingParseError,
                "multiple internally complete table candidates",
            ):
                pipeline.fetch_filing_holdings(
                    CIK, BASE_ACCESSION, filing=BASE_ROW
                )

    def test_cover_mismatch_fallback_requires_every_candidate_fetch(
        self,
    ) -> None:
        index = self.Response(payload={
            "directory": {"item": [
                {"name": "primary_doc.xml"},
                {"name": "information_table_a.xml"},
                {"name": "information_table_b.xml"},
            ]}
        })
        primary = self.Response(content=self.primary(99))
        information = self.Response(content=self.INFO_TABLE)

        def get(url: str):
            if url.endswith("index.json"):
                return index
            if url.endswith("primary_doc.xml"):
                return primary
            if url.endswith("information_table_a.xml"):
                return information
            if url.endswith("information_table_b.xml"):
                raise RuntimeError("temporary fetch failure")
            raise AssertionError(url)

        with mock.patch.object(pipeline.HTTP, "get", side_effect=get):
            with self.assertRaisesRegex(
                pipeline.FilingFetchError,
                "candidate XML fetch failed",
            ):
                pipeline.fetch_filing_holdings(
                    CIK, BASE_ACCESSION, filing=BASE_ROW
                )

    def test_nonzero_row_dropped_by_parser_fails_reconciliation(self) -> None:
        invalid_nonzero = b"""<informationTable><infoTable>
          <nameOfIssuer>N/A</nameOfIssuer><titleOfClass>NONE</titleOfClass>
          <cusip>000000000</cusip><value>100</value>
          <shrsOrPrnAmt><sshPrnamt>0</sshPrnamt></shrsOrPrnAmt>
        </infoTable></informationTable>"""
        with mock.patch.object(
            pipeline.HTTP,
            "get",
            side_effect=self.responses(100, invalid_nonzero),
        ):
            with self.assertRaisesRegex(
                pipeline.FilingParseError, "nonzero information-table rows"
            ):
                pipeline.fetch_filing_holdings(
                    CIK, BASE_ACCESSION, filing=BASE_ROW
                )

    def test_zero_placeholder_is_a_complete_empty_portfolio(self) -> None:
        zero_placeholder = b"""<informationTable><infoTable>
          <nameOfIssuer>NONE</nameOfIssuer><titleOfClass>NONE</titleOfClass>
          <cusip>000000000</cusip><value>0</value>
          <shrsOrPrnAmt><sshPrnamt>0</sshPrnamt></shrsOrPrnAmt>
        </infoTable></informationTable>"""
        with mock.patch.object(
            pipeline.HTTP,
            "get",
            side_effect=self.responses(0, zero_placeholder),
        ):
            parsed = pipeline.fetch_filing_holdings(
                CIK, BASE_ACCESSION, filing=BASE_ROW
            )

        self.assertEqual([], parsed["holdings"])
        quarter = pipeline.compose_quarter_filings([parsed])
        self.assertEqual(0, quarter["num_holdings"])
        self.assertEqual(0, quarter["total_value"])

        # Some valid confidential-treatment filings declare 0/0 on the cover
        # while still including the same single dummy XML row.
        with mock.patch.object(
            pipeline.HTTP,
            "get",
            side_effect=self.responses(0, zero_placeholder, entry_total=0),
        ):
            cover_zero = pipeline.fetch_filing_holdings(
                CIK, BASE_ACCESSION, filing=BASE_ROW
            )
        self.assertEqual([], cover_zero["holdings"])

    def test_empty_table_cannot_satisfy_cover_that_reports_one_row(self) -> None:
        empty_table = b"<informationTable></informationTable>"
        with mock.patch.object(
            pipeline.HTTP,
            "get",
            side_effect=self.responses(0, empty_table, entry_total=1),
        ):
            with self.assertRaisesRegex(
                pipeline.FilingParseError, "entry-total mismatch"
            ):
                pipeline.fetch_filing_holdings(
                    CIK, BASE_ACCESSION, filing=BASE_ROW
                )


class ReplayIntegrationTests(unittest.TestCase):
    def test_transient_component_fetch_is_quarantined_with_its_type(self) -> None:
        state = {"processed": [], "_processed_set": set()}
        with mock.patch.object(
            pipeline,
            "fetch_filing_holdings",
            side_effect=pipeline.FilingFetchError(
                "SEC resource remained unavailable"
            ),
        ):
            replayed = pipeline.replay_quarters_for_cik(
                CIK,
                [BASE_ROW],
                {},
                4,
                state,
                quarantine_failures=True,
                discovered_submission=([BASE_ROW], "Example Manager"),
            )

        self.assertEqual(0, replayed)
        self.assertEqual(
            pipeline.FilingFetchError.__name__,
            state["_quarantined"][BASE_ACCESSION]["reason"],
        )
        self.assertNotIn(BASE_ACCESSION, state["_processed_set"])

    def test_blackstone_additions_are_added_to_base_not_used_as_replacement(self) -> None:
        quarter = pipeline.compose_quarter_filings(
            [BASE_COMPONENT, ADDITION_COMPONENT]
        )

        self.assertEqual(BASE_ACCESSION, quarter["base_accession"])
        self.assertEqual(
            [BASE_ACCESSION, ADDITION_ACCESSION], quarter["applied_accessions"]
        )
        self.assertEqual(7, quarter["num_holdings"])
        self.assertEqual(315, quarter["total_value"])

    def test_late_supplement_retries_archives_then_publishes_complete_chain(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            state = {
                "processed": [],
                "_processed_set": set(),
                "_quarantined": {ADDITION_ACCESSION: {"reason": "prior failure"}},
            }
            cusip_map: dict[str, str] = {}
            components = {
                BASE_ACCESSION: BASE_COMPONENT,
                ADDITION_ACCESSION: ADDITION_COMPONENT,
            }

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    side_effect=[
                        ([ADDITION_ROW], "Blackstone Inc."),
                        ([BASE_ROW, ADDITION_ROW], "Blackstone Inc."),
                    ],
                ) as discover,
                mock.patch.object(
                    pipeline,
                    "fetch_filing_holdings",
                    side_effect=lambda cik, accession, filing=None: components[accession],
                ),
                mock.patch.object(pipeline, "update_cusip_map"),
            ):
                processed = pipeline.replay_quarters_for_cik(
                    CIK, [ADDITION_ROW], cusip_map, 4, state
                )

            self.assertEqual(1, processed)
            self.assertEqual(2, discover.call_count)
            fund = json.loads((funds_dir / f"{CIK}.json").read_text())
            self.assertEqual(7, fund["quarters"][0]["num_holdings"])
            self.assertEqual(
                {BASE_ACCESSION, ADDITION_ACCESSION}, state["_processed_set"]
            )
            self.assertNotIn(ADDITION_ACCESSION, state["_quarantined"])

    def test_ambiguous_chain_byte_preserves_last_known_good_and_state(self) -> None:
        unknown_accession = "0001193125-26-999999"
        unknown_row = filing_row(
            unknown_accession, "13F-HR/A", "2026-06-01T12:00:00Z"
        )
        unknown_component = component(
            unknown_accession,
            "UNKNOWN",
            unknown_row["accepted_at"],
            [holding("999999999", 1)],
            amendment_number=1,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / f"{CIK}.json"
            original_bytes = b'{"cik": 1393818, "name": "LKG", "quarters": []}\n'
            fund_path.write_bytes(original_bytes)
            state = {"processed": [], "_processed_set": set()}

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    return_value=([BASE_ROW, unknown_row], "Blackstone Inc."),
                ),
                mock.patch.object(
                    pipeline,
                    "fetch_filing_holdings",
                    side_effect=lambda cik, accession, filing=None: {
                        BASE_ACCESSION: BASE_COMPONENT,
                        unknown_accession: unknown_component,
                    }[accession],
                ),
                mock.patch.object(pipeline, "update_cusip_map"),
            ):
                with self.assertRaisesRegex(
                    pipeline.FilingChainError, "unknown semantics"
                ):
                    pipeline.replay_quarters_for_cik(
                        CIK, [unknown_row], {}, 4, state
                    )

            self.assertEqual(original_bytes, fund_path.read_bytes())
            self.assertEqual(set(), state["_processed_set"])

            pipeline.quarantine_replay_failure(
                state, CIK, [unknown_row], pipeline.FilingChainError(
                    "unknown_amendment_type", "unknown semantics"
                )
            )
            self.assertEqual(
                "unknown_amendment_type",
                state["_quarantined"][unknown_accession]["reason"],
            )

    def test_migration_target_retries_until_atomic_publication_succeeds(self) -> None:
        unknown_component = {
            **ADDITION_COMPONENT,
            "amendment_kind": "UNKNOWN",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / f"{CIK}.json"
            legacy_fund = {
                "cik": CIK,
                "name": "Legacy Blackstone",
                "quarters": [{
                    "report_date": REPORT_DATE,
                    "filing_date": "2026-05-15",
                    "accession": ADDITION_ACCESSION,
                    "holdings": [holding("999999999", 1)],
                }],
            }
            fund_path.write_text(json.dumps(legacy_fund))
            legacy_bytes = fund_path.read_bytes()
            state = {
                "processed": [ADDITION_ACCESSION],
                "_processed_set": {ADDITION_ACCESSION},
                "_quarantined": {},
                "amendment_migration_pending": {},
            }
            components = {
                BASE_ACCESSION: BASE_COMPONENT,
                ADDITION_ACCESSION: unknown_component,
            }

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    return_value=([BASE_ROW, ADDITION_ROW], "Blackstone Inc."),
                ),
                mock.patch.object(
                    pipeline,
                    "fetch_filing_holdings",
                    side_effect=lambda cik, accession, filing=None: components[accession],
                ),
                mock.patch.object(pipeline, "update_cusip_map"),
            ):
                processed = pipeline.replay_quarters_for_cik(
                    CIK,
                    [ADDITION_ROW],
                    {},
                    4,
                    state,
                    force=True,
                    include_archives=True,
                    preserve_history=True,
                    quarantine_failures=True,
                    replace_only=True,
                    track_migration_targets=True,
                )

                self.assertEqual(0, processed)
                self.assertEqual(legacy_bytes, fund_path.read_bytes())
                self.assertIn(
                    ADDITION_ACCESSION,
                    state["amendment_migration_pending"],
                )
                self.assertIn(ADDITION_ACCESSION, state["_quarantined"])
                self.assertNotIn(ADDITION_ACCESSION, state["_processed_set"])

                components[ADDITION_ACCESSION] = ADDITION_COMPONENT
                processed = pipeline.replay_quarters_for_cik(
                    CIK,
                    [ADDITION_ROW],
                    {},
                    4,
                    state,
                    force=True,
                    include_archives=True,
                    preserve_history=True,
                    quarantine_failures=True,
                    replace_only=True,
                )

            self.assertEqual(1, processed)
            repaired = json.loads(fund_path.read_text())
            self.assertEqual(
                pipeline.AMENDMENT_REDUCER_VERSION,
                repaired["quarters"][0]["composition_version"],
            )
            self.assertNotIn(
                ADDITION_ACCESSION,
                state["amendment_migration_pending"],
            )
            self.assertNotIn(ADDITION_ACCESSION, state["_quarantined"])

    def test_unparseable_component_before_restatement_is_safely_superseded(self) -> None:
        failed_accession = "0001193125-26-111111"
        restatement_accession = "0001193125-26-222222"
        failed_row = filing_row(
            failed_accession, "13F-HR/A", "2026-03-01T12:00:00Z"
        )
        restatement_row = filing_row(
            restatement_accession, "13F-HR/A", "2026-04-01T12:00:00Z"
        )
        restatement_component = component(
            restatement_accession,
            "RESTATEMENT",
            restatement_row["accepted_at"],
            [holding("888888888", 80)],
            amendment_number=2,
        )
        components = {
            BASE_ACCESSION: BASE_COMPONENT,
            restatement_accession: restatement_component,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            state = {"processed": [], "_processed_set": set()}

            def fetch(cik, accession, filing=None):
                if accession == failed_accession:
                    raise pipeline.FilingParseError("malformed superseded filing")
                return components[accession]

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    return_value=(
                        [BASE_ROW, failed_row, restatement_row],
                        "Blackstone Inc.",
                    ),
                ),
                mock.patch.object(pipeline, "fetch_filing_holdings", side_effect=fetch),
                mock.patch.object(pipeline, "update_cusip_map"),
            ):
                processed = pipeline.replay_quarters_for_cik(
                    CIK, [restatement_row], {}, 4, state
                )

            self.assertEqual(1, processed)
            fund = json.loads((funds_dir / f"{CIK}.json").read_text())
            quarter = fund["quarters"][0]
            self.assertEqual(restatement_accession, quarter["base_accession"])
            self.assertEqual(80, quarter["total_value"])

    def test_state_does_not_advance_when_atomic_fund_save_fails(self) -> None:
        state = {"processed": [], "_processed_set": set()}
        with (
            mock.patch.object(
                pipeline,
                "_discover_submission_filings",
                return_value=([BASE_ROW], "Blackstone Inc."),
            ),
            mock.patch.object(
                pipeline,
                "fetch_filing_holdings",
                return_value=BASE_COMPONENT,
            ),
            mock.patch.object(pipeline, "update_cusip_map"),
            mock.patch.object(pipeline, "merge_composed_quarters_into_fund", return_value={}),
            mock.patch.object(pipeline, "save_fund", side_effect=OSError("disk full")),
        ):
            with self.assertRaisesRegex(OSError, "disk full"):
                pipeline.replay_quarters_for_cik(
                    CIK, [BASE_ROW], {}, 4, state
                )

        self.assertEqual(set(), state["_processed_set"])

    def test_one_bad_report_date_does_not_block_good_date_for_same_cik(self) -> None:
        good_date = "2026-03-31"
        good_accession = "0001193125-26-300001"
        bad_base_accession = "0001193125-26-300002"
        bad_accession = "0001193125-26-300003"

        def dated_row(accession, form_type, accepted_at, report_date):
            return {
                **filing_row(accession, form_type, accepted_at),
                "report_date": report_date,
            }

        good_row = dated_row(
            good_accession, "13F-HR", "2026-05-15T12:00:00Z", good_date
        )
        bad_base_row = dated_row(
            bad_base_accession, "13F-HR", "2026-02-15T12:00:00Z", REPORT_DATE
        )
        bad_row = dated_row(
            bad_accession, "13F-HR/A", "2026-05-16T12:00:00Z", REPORT_DATE
        )
        good_component = {
            **component(
                good_accession,
                "ORIGINAL",
                good_row["accepted_at"],
                [holding("777777777", 70)],
            ),
            "report_date": good_date,
        }
        bad_base_component = component(
            bad_base_accession,
            "ORIGINAL",
            bad_base_row["accepted_at"],
            [holding("111111111", 100)],
        )
        bad_component = component(
            bad_accession,
            "UNKNOWN",
            bad_row["accepted_at"],
            [holding("999999999", 9)],
            amendment_number=1,
        )
        components = {
            good_accession: good_component,
            bad_base_accession: bad_base_component,
            bad_accession: bad_component,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            state = {"processed": [], "_processed_set": set()}
            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    return_value=(
                        [good_row, bad_base_row, bad_row],
                        "Blackstone Inc.",
                    ),
                ),
                mock.patch.object(
                    pipeline,
                    "fetch_filing_holdings",
                    side_effect=lambda cik, accession, filing=None: components[accession],
                ),
                mock.patch.object(pipeline, "update_cusip_map"),
            ):
                processed = pipeline.replay_quarters_for_cik(
                    CIK,
                    [good_row, bad_row],
                    {},
                    4,
                    state,
                    quarantine_failures=True,
                )

            self.assertEqual(1, processed)
            fund = json.loads((funds_dir / f"{CIK}.json").read_text())
            self.assertEqual([good_date], [q["report_date"] for q in fund["quarters"]])
            self.assertIn(good_accession, state["_processed_set"])
            self.assertNotIn(bad_accession, state["_processed_set"])
            self.assertEqual(
                REPORT_DATE,
                state["_quarantined"][bad_accession]["report_date"],
            )


class AmendmentMigrationOutcomeTests(unittest.TestCase):
    @staticmethod
    def write_fund(
        funds_dir: Path,
        *,
        composition_version: int,
        source_filings: list[dict] | None = None,
    ) -> None:
        funds_dir.mkdir()
        (funds_dir / f"{CIK}.json").write_text(json.dumps({
            "cik": CIK,
            "name": "Blackstone Inc.",
            "quarters": [{
                "report_date": REPORT_DATE,
                "composition_version": composition_version,
                "source_filings": source_filings or [],
                "holdings": [],
            }],
        }))

    def test_inventory_includes_only_v1_new_holdings_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            funds_dir.mkdir()
            (funds_dir / f"{CIK}.json").write_text(json.dumps({
                "cik": CIK,
                "name": "Blackstone Inc.",
                "quarters": [
                    {
                        "report_date": REPORT_DATE,
                        "composition_version": 1,
                        "source_filings": [
                            {
                                "accession": BASE_ACCESSION,
                                "amendment_kind": "ORIGINAL",
                            },
                            {
                                "accession": ADDITION_ACCESSION,
                                "amendment_kind": "NEW_HOLDINGS",
                                "filing_date": ADDITION_ROW["date_filed"],
                                "accepted_at": ADDITION_ROW["accepted_at"],
                            },
                        ],
                    },
                    {
                        "report_date": "2025-09-30",
                        "composition_version": pipeline.AMENDMENT_REDUCER_VERSION,
                        "source_filings": [{
                            "accession": "0001193125-26-333333",
                            "amendment_kind": "NEW_HOLDINGS",
                        }],
                    },
                    {
                        "report_date": "2025-06-30",
                        "composition_version": 1,
                        "source_filings": [{
                            "accession": "0001193125-26-444444",
                            "amendment_kind": "RESTATEMENT",
                        }],
                    },
                ],
            }))

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                triggers = pipeline.retained_new_holdings_migration_triggers()

        self.assertEqual([ADDITION_ROW], triggers)

    def test_outcome_accepts_published_v2_quarter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            funds_dir.mkdir()
            quarter = pipeline.compose_quarter_filings(
                [BASE_COMPONENT, ADDITION_COMPONENT]
            )
            (funds_dir / f"{CIK}.json").write_text(json.dumps({
                "cik": CIK,
                "name": "Blackstone Inc.",
                "quarters": [quarter],
            }))
            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                errors = pipeline.amendment_migration_outcome_errors(
                    [ADDITION_ROW],
                    {"_processed_set": {ADDITION_ACCESSION}},
                )

        self.assertEqual([], errors)

    def test_outcome_rejects_invalid_or_incomplete_v2_quarter(self) -> None:
        cases = {
            "incomplete": lambda quarter: quarter.__setitem__(
                "is_complete", False
            ),
            "invalid hash": lambda quarter: quarter.__setitem__(
                "composition_hash", "0" * 64
            ),
        }
        for label, mutate in cases.items():
            with self.subTest(label), tempfile.TemporaryDirectory() as temp_dir:
                funds_dir = Path(temp_dir) / "funds"
                funds_dir.mkdir()
                quarter = pipeline.compose_quarter_filings(
                    [BASE_COMPONENT, ADDITION_COMPONENT]
                )
                mutate(quarter)
                (funds_dir / f"{CIK}.json").write_text(json.dumps({
                    "cik": CIK,
                    "name": "Blackstone Inc.",
                    "quarters": [quarter],
                }))

                with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                    errors = pipeline.amendment_migration_outcome_errors(
                        [ADDITION_ROW],
                        {"_processed_set": {ADDITION_ACCESSION}},
                    )

                self.assertEqual(1, len(errors))
                self.assertIn(ADDITION_ACCESSION, errors[0])

    def test_outcome_accepts_only_exact_durable_v1_quarantine(self) -> None:
        pending_target = {"cik": CIK, "report_date": REPORT_DATE}
        quarantined = {ADDITION_ACCESSION: {"reason": "replay failed"}}
        accepted_state = {
            "amendment_migration_pending": {
                ADDITION_ACCESSION: pending_target,
            },
            "_quarantined": quarantined,
            "_processed_set": set(),
        }
        rejected_states = {
            "different pending accession": {
                "amendment_migration_pending": {
                    "0001193125-26-999999": pending_target,
                },
                "_quarantined": quarantined,
                "_processed_set": set(),
            },
            "wrong pending target": {
                "amendment_migration_pending": {
                    ADDITION_ACCESSION: {
                        "cik": CIK,
                        "report_date": "2025-09-30",
                    },
                },
                "_quarantined": quarantined,
                "_processed_set": set(),
            },
            "not quarantined": {
                "amendment_migration_pending": {
                    ADDITION_ACCESSION: pending_target,
                },
                "_quarantined": {},
                "_processed_set": set(),
            },
            "already processed": {
                "amendment_migration_pending": {
                    ADDITION_ACCESSION: pending_target,
                },
                "_quarantined": quarantined,
                "_processed_set": {ADDITION_ACCESSION},
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            self.write_fund(funds_dir, composition_version=1)
            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                self.assertEqual(
                    [],
                    pipeline.amendment_migration_outcome_errors(
                        [ADDITION_ROW], accepted_state
                    ),
                )
                for label, state in rejected_states.items():
                    with self.subTest(label):
                        errors = pipeline.amendment_migration_outcome_errors(
                            [ADDITION_ROW], state
                        )
                        self.assertEqual(1, len(errors))
                        self.assertIn(ADDITION_ACCESSION, errors[0])

    def test_unresolved_v1_quarter_is_withheld_from_fund_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            funds_dir.mkdir()
            (funds_dir / f"{CIK}.json").write_text(json.dumps({
                "cik": CIK,
                "name": "Blackstone Inc.",
                "quarters": [
                    {
                        "report_date": REPORT_DATE,
                        "composition_version": 1,
                        "source_filings": [{
                            "accession": ADDITION_ACCESSION,
                            "amendment_kind": "NEW_HOLDINGS",
                        }],
                        "holdings": [holding("999999999", 1)],
                    },
                    {
                        "report_date": "2025-09-30",
                        "composition_version": 1,
                        "source_filings": [],
                        "holdings": [],
                    },
                ],
            }))

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                withheld = pipeline.withhold_unmigrated_new_holdings_quarters(
                    [ADDITION_ROW]
                )

            fund = json.loads(
                (funds_dir / f"{CIK}.json").read_text()
            )

        self.assertEqual(1, withheld)
        self.assertEqual(
            ["2025-09-30"],
            [quarter["report_date"] for quarter in fund["quarters"]],
        )

    def test_pending_retry_restores_a_withheld_report_date(self) -> None:
        state = {
            "_processed_set": set(),
            "_quarantined": {
                ADDITION_ACCESSION: {"reason": "prior replay failure"}
            },
            "amendment_migration_pending": {
                ADDITION_ACCESSION: {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                },
            },
        }
        components = {
            BASE_ACCESSION: BASE_COMPONENT,
            ADDITION_ACCESSION: ADDITION_COMPONENT,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            funds_dir = Path(temp_dir) / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / f"{CIK}.json"
            fund_path.write_text(json.dumps({
                "cik": CIK,
                "name": "Blackstone Inc.",
                "quarters": [],
            }))

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    return_value=(
                        [BASE_ROW, ADDITION_ROW],
                        "Blackstone Inc.",
                    ),
                ),
                mock.patch.object(
                    pipeline,
                    "fetch_filing_holdings",
                    side_effect=lambda cik, accession, filing=None: components[
                        accession
                    ],
                ),
                mock.patch.object(pipeline, "update_cusip_map"),
            ):
                retried = pipeline.retry_pending_amendment_migrations(
                    state, {}, 4
                )

            restored = json.loads(fund_path.read_text())

        self.assertEqual(1, retried)
        self.assertEqual(
            [REPORT_DATE],
            [quarter["report_date"] for quarter in restored["quarters"]],
        )
        self.assertEqual(
            pipeline.AMENDMENT_REDUCER_VERSION,
            restored["quarters"][0]["composition_version"],
        )
        self.assertIn(ADDITION_ACCESSION, state["_processed_set"])
        self.assertNotIn(
            ADDITION_ACCESSION,
            state["amendment_migration_pending"],
        )
        self.assertNotIn(ADDITION_ACCESSION, state["_quarantined"])


class RepairModeTests(unittest.TestCase):
    def test_repair_interruption_checkpoints_completed_replay_progress(
        self,
    ) -> None:
        state = {"_processed_set": set()}
        cusip_map = {}

        def interrupt_after_progress(*_args, **_kwargs) -> int:
            state["amendment_migration_pending"] = {
                ADDITION_ACCESSION: {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                }
            }
            cusip_map["123456789"] = "XYZ"
            raise KeyboardInterrupt

        with (
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(
                pipeline, "load_cusip_map", return_value=cusip_map
            ),
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[(2026, 2)]
            ),
            mock.patch.object(
                pipeline, "download_company_idx", return_value=[ADDITION_ROW]
            ),
            mock.patch.object(
                pipeline,
                "replay_quarters_for_cik",
                side_effect=interrupt_after_progress,
            ),
            mock.patch.object(pipeline, "save_state") as save_state,
            mock.patch.object(pipeline, "save_cusip_map") as save_map,
        ):
            with self.assertRaises(KeyboardInterrupt):
                pipeline.repair_amendments(4, rebuild_outputs=False)

        save_state.assert_called_once_with(state)
        save_map.assert_called_once_with(cusip_map)
        self.assertEqual("XYZ", cusip_map["123456789"])

    def test_health_gate_uses_existing_corpus_quarantine_ceiling(self) -> None:
        self.assertIsNone(
            pipeline.initial_migration_health_error(
                "amendment migration",
                total=146,
                resolved=108,
                unresolved=38,
                quarantine_budget=56,
            )
        )
        error = pipeline.initial_migration_health_error(
            "amendment migration",
            total=146,
            resolved=89,
            unresolved=57,
            quarantine_budget=56,
        )
        self.assertIsNotNone(error)
        self.assertIn("allowed 56", error)
        self.assertIsNotNone(
            pipeline.initial_migration_health_error(
                "amendment migration",
                total=146,
                resolved=0,
                unresolved=146,
                quarantine_budget=146,
            )
        )

    def test_quarantine_budget_is_durable_and_quarter_deduplicated(
        self,
    ) -> None:
        accessions = [
            "0001193125-26-300001",
            "0001193125-26-300002",
            "0001193125-26-300003",
            "0001193125-26-300004",
            "0001193125-26-300005",
        ]
        state = {
            "_processed_set": {accessions[4]},
            "amendment_migration_pending": {
                accession: {
                    "cik": CIK if index < 2 else CIK + index,
                    "report_date": (
                        REPORT_DATE if index < 2 else "2025-09-30"
                    ),
                }
                for index, accession in enumerate(accessions)
            },
            "_quarantined": {
                accessions[0]: {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                },
                accessions[1]: {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                },
                accessions[3]: {
                    "cik": CIK + 30,
                    "report_date": "2025-09-30",
                },
                accessions[4]: {
                    "cik": CIK + 4,
                    "report_date": "2025-09-30",
                },
            },
        }

        self.assertEqual(
            1, pipeline.amendment_migration_quarantine_budget(state)
        )

    def test_pending_migration_retries_are_limited_to_weekly(self) -> None:
        now = datetime(2026, 7, 25, 12, tzinfo=timezone.utc)
        state = {
            "amendment_migration_pending": {
                ADDITION_ACCESSION: {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                },
            },
            "amendment_migration_last_retry": (
                now - timedelta(days=6)
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        self.assertFalse(
            pipeline.amendment_migration_retry_due(state, now=now)
        )
        state["amendment_migration_last_retry"] = (
            now - timedelta(days=7)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.assertTrue(
            pipeline.amendment_migration_retry_due(state, now=now)
        )

    def test_systemic_migration_failure_keeps_v1_quarter_published(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / f"{CIK}.json"
            original_fund = {
                "cik": CIK,
                "name": "Blackstone Inc.",
                "quarters": [{
                    "report_date": REPORT_DATE,
                    "composition_version": 1,
                    "source_filings": [{
                        "accession": ADDITION_ACCESSION,
                        "amendment_kind": "NEW_HOLDINGS",
                    }],
                    "holdings": [holding("999999999", 1)],
                }],
            }
            fund_path.write_text(json.dumps(original_fund))
            state_path = root / "pipeline_state.json"
            state_path.write_text(json.dumps({
                "processed": [],
                "quarantined": {},
                "amendment_reducer_version": 1,
                "amendment_migration_pending": {},
            }))

            def quarantine_every_target(
                _cik: int,
                triggers: list[dict],
                _cusip_map: dict[str, str],
                _quarters_n: int,
                state: dict,
                **_kwargs,
            ) -> int:
                state.setdefault("_quarantined", {})[
                    ADDITION_ACCESSION
                ] = {"reason": "SEC Archive unavailable"}
                state.setdefault("amendment_migration_pending", {})[
                    ADDITION_ACCESSION
                ] = {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                }
                return 0

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(pipeline, "STATE_PATH", state_path),
                mock.patch.object(
                    pipeline, "LEGACY_STATE_PATH", root / "missing-state.json"
                ),
                mock.patch.object(pipeline, "load_cusip_map", return_value={}),
                mock.patch.object(
                    pipeline,
                    "retained_new_holdings_migration_triggers",
                    return_value=[ADDITION_ROW],
                ),
                mock.patch.object(
                    pipeline, "get_recent_filing_quarters", return_value=[]
                ),
                mock.patch.object(
                    pipeline,
                    "replay_quarters_for_cik",
                    side_effect=quarantine_every_target,
                ),
                mock.patch.object(pipeline, "save_cusip_map"),
            ):
                succeeded = pipeline.repair_amendments(
                    8,
                    quarantine_failures=True,
                    mark_migration=True,
                )

            persisted_state = json.loads(state_path.read_text())
            persisted_fund = json.loads(fund_path.read_text())

        self.assertFalse(succeeded)
        self.assertEqual(1, persisted_state["amendment_reducer_version"])
        self.assertIn(
            ADDITION_ACCESSION,
            persisted_state["amendment_migration_pending"],
        )
        self.assertEqual(original_fund["quarters"], persisted_fund["quarters"])

    def test_disjoint_corpus_quarantine_budget_allows_bounded_new_targets(
        self,
    ) -> None:
        targets = [
            {
                **ADDITION_ROW,
                "cik": CIK + index,
                "report_date": report_date,
                "accession": f"0001193125-26-22661{index}",
            }
            for index, report_date in enumerate(
                ["2025-06-30", "2025-09-30", "2025-12-31"]
            )
        ]
        baseline_targets = [
            {
                "cik": CIK + 10 + index,
                "report_date": report_date,
                "accession": f"0001193125-25-30000{index}",
            }
            for index, report_date in enumerate(
                ["2024-06-30", "2024-09-30"]
            )
        ]
        successful_accession = targets[0]["accession"]
        state = {
            "processed": [],
            "_processed_set": set(),
            "_quarantined": {
                target["accession"]: {
                    "cik": target["cik"],
                    "report_date": target["report_date"],
                    "reason": "known ambiguity",
                }
                for target in baseline_targets
            },
            "amendment_reducer_version": 1,
            "amendment_migration_pending": {
                target["accession"]: {
                    "cik": target["cik"],
                    "report_date": target["report_date"],
                }
                for target in baseline_targets
            },
        }

        def resolve_one_target(
            _cik: int,
            triggers: list[dict],
            _cusip_map: dict[str, str],
            _quarters_n: int,
            replay_state: dict,
            **_kwargs,
        ) -> int:
            target = triggers[0]
            accession = target["accession"]
            if accession == successful_accession:
                replay_state["_processed_set"].add(accession)
                return 1
            replay_state["amendment_migration_pending"][accession] = {
                "cik": target["cik"],
                "report_date": target["report_date"],
            }
            replay_state["_quarantined"][accession] = {
                "cik": target["cik"],
                "report_date": target["report_date"],
                "reason": "new deterministic ambiguity",
            }
            return 0

        with (
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(
                pipeline,
                "retained_new_holdings_migration_triggers",
                return_value=targets,
            ),
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[]
            ),
            mock.patch.object(
                pipeline,
                "replay_quarters_for_cik",
                side_effect=resolve_one_target,
            ),
            mock.patch.object(
                pipeline, "amendment_migration_outcome_errors", return_value=[]
            ),
            mock.patch.object(
                pipeline,
                "withhold_unmigrated_new_holdings_quarters",
                return_value=2,
            ) as withhold,
            mock.patch.object(pipeline, "save_state"),
            mock.patch.object(pipeline, "save_cusip_map"),
            mock.patch.object(pipeline, "rebuild_registry_backed_outputs"),
        ):
            succeeded = pipeline.repair_amendments(
                8,
                quarantine_failures=True,
                mark_migration=True,
            )

        self.assertTrue(succeeded)
        self.assertEqual(
            pipeline.AMENDMENT_REDUCER_VERSION,
            state["amendment_reducer_version"],
        )
        self.assertEqual(
            {
                target["accession"]
                for target in [*baseline_targets, *targets[1:]]
            },
            set(state["amendment_migration_pending"]),
        )
        withhold.assert_called_once_with(targets)

    def test_one_isolated_quarter_failure_may_finish_migration(
        self,
    ) -> None:
        failed_accession = "0001193125-26-226615"
        failed_target = {
            **ADDITION_ROW,
            "cik": CIK + 1,
            "report_date": "2025-09-30",
            "accession": failed_accession,
        }
        state = {
            "processed": [],
            "_processed_set": {ADDITION_ACCESSION},
            "_quarantined": {
                failed_accession: {"reason": "isolated parse failure"},
            },
            "amendment_reducer_version": 1,
            "amendment_migration_pending": {
                failed_accession: {
                    "cik": failed_target["cik"],
                    "report_date": failed_target["report_date"],
                },
            },
        }
        with (
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(
                pipeline,
                "retained_new_holdings_migration_triggers",
                return_value=[ADDITION_ROW, failed_target],
            ),
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[]
            ),
            mock.patch.object(
                pipeline, "replay_quarters_for_cik", return_value=1
            ),
            mock.patch.object(
                pipeline, "amendment_migration_outcome_errors", return_value=[]
            ),
            mock.patch.object(
                pipeline,
                "withhold_unmigrated_new_holdings_quarters",
                return_value=1,
            ) as withhold,
            mock.patch.object(pipeline, "save_state"),
            mock.patch.object(pipeline, "save_cusip_map"),
            mock.patch.object(pipeline, "rebuild_registry_backed_outputs"),
        ):
            succeeded = pipeline.repair_amendments(
                8,
                quarantine_failures=True,
                mark_migration=True,
            )

        self.assertTrue(succeeded)
        self.assertEqual(
            pipeline.AMENDMENT_REDUCER_VERSION,
            state["amendment_reducer_version"],
        )
        withhold.assert_called_once_with([ADDITION_ROW, failed_target])

    def test_withholding_failure_keeps_retry_queue_durable(self) -> None:
        second_cik = CIK + 1
        second_accession = "0001193125-26-226615"
        second_target = {
            **ADDITION_ROW,
            "cik": second_cik,
            "accession": second_accession,
        }
        targets = [ADDITION_ROW, second_target]
        state = {
            "processed": [],
            "_processed_set": set(),
            "_quarantined": {
                target["accession"]: {"reason": "ambiguous"}
                for target in targets
            },
            "amendment_migration_pending": {
                target["accession"]: {
                    "cik": target["cik"],
                    "report_date": target["report_date"],
                }
                for target in targets
            },
            "amendment_reducer_version": 1,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            state_path = root / "pipeline_state.json"
            for target in targets:
                (funds_dir / f"{target['cik']}.json").write_text(json.dumps({
                    "cik": target["cik"],
                    "name": "Fund",
                    "quarters": [{
                        "report_date": REPORT_DATE,
                        "composition_version": 1,
                        "source_filings": [{
                            "accession": target["accession"],
                            "amendment_kind": "NEW_HOLDINGS",
                        }],
                        "holdings": [holding("999999999", 1)],
                    }],
                }))

            original_save_fund = pipeline.save_fund
            save_calls = 0

            def fail_second_fund_save(cik: int, fund: dict) -> None:
                nonlocal save_calls
                save_calls += 1
                if save_calls == 2:
                    raise OSError("simulated second fund write failure")
                original_save_fund(cik, fund)

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(pipeline, "STATE_PATH", state_path),
                mock.patch.object(pipeline, "load_state", return_value=state),
                mock.patch.object(pipeline, "load_cusip_map", return_value={}),
                mock.patch.object(
                    pipeline,
                    "retained_new_holdings_migration_triggers",
                    return_value=targets,
                ),
                mock.patch.object(
                    pipeline, "get_recent_filing_quarters", return_value=[]
                ),
                mock.patch.object(
                    pipeline, "replay_quarters_for_cik", return_value=0
                ),
                mock.patch.object(
                    pipeline,
                    "initial_migration_health_error",
                    return_value=None,
                ),
                mock.patch.object(
                    pipeline,
                    "save_fund",
                    side_effect=fail_second_fund_save,
                ),
                mock.patch.object(pipeline, "save_cusip_map"),
                mock.patch.object(pipeline, "rebuild_registry_backed_outputs"),
            ):
                succeeded = pipeline.repair_amendments(
                    8,
                    quarantine_failures=True,
                    mark_migration=True,
                )

            persisted_state = json.loads(state_path.read_text())
            first_fund = json.loads(
                (funds_dir / f"{CIK}.json").read_text()
            )

        self.assertFalse(succeeded)
        self.assertEqual(2, save_calls)
        self.assertEqual(1, persisted_state["amendment_reducer_version"])
        self.assertEqual(
            set(state["amendment_migration_pending"]),
            set(persisted_state["amendment_migration_pending"]),
        )
        self.assertEqual([], first_fund["quarters"])

    def test_repair_scans_all_amendments_and_forces_authoritative_replay(self) -> None:
        state = {
            "processed": [ADDITION_ACCESSION],
            "_processed_set": {ADDITION_ACCESSION},
            "amendment_reducer_version": 0,
        }
        with (
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[(2026, 2)]
            ),
            mock.patch.object(
                pipeline,
                "download_company_idx",
                return_value=[BASE_ROW, ADDITION_ROW],
            ) as download,
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(
                pipeline, "retained_new_holdings_migration_triggers", return_value=[]
            ),
            mock.patch.object(
                pipeline, "amendment_migration_outcome_errors", return_value=[]
            ),
            mock.patch.object(pipeline, "replay_quarters_for_cik", return_value=1) as replay,
            mock.patch.object(pipeline, "save_state"),
            mock.patch.object(pipeline, "save_cusip_map"),
            mock.patch.object(pipeline, "rebuild_registry_backed_outputs"),
        ):
            self.assertTrue(pipeline.repair_amendments(
                8, quarantine_failures=True, mark_migration=True
            ))

        download.assert_called_once_with(2026, 2, strict=True)
        args, kwargs = replay.call_args
        self.assertEqual(CIK, args[0])
        self.assertEqual([ADDITION_ACCESSION], [row["accession"] for row in args[1]])
        self.assertEqual(8, args[3])
        self.assertTrue(kwargs["force"])
        self.assertTrue(kwargs["include_archives"])
        self.assertTrue(kwargs["preserve_history"])
        self.assertTrue(kwargs["quarantine_failures"])
        self.assertTrue(kwargs["replace_only"])
        self.assertTrue(kwargs["track_migration_targets"])
        self.assertEqual(
            pipeline.AMENDMENT_REDUCER_VERSION,
            state["amendment_reducer_version"],
        )

    def test_normal_run_performs_one_time_broad_migration_before_ingest(self) -> None:
        legacy_state = {
            "processed": [],
            "_processed_set": set(),
            "amendment_reducer_version": 0,
        }
        migrated_state = {
            "processed": [],
            "_processed_set": set(),
            "amendment_reducer_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
        }
        with (
            mock.patch.object(
                pipeline, "load_state", side_effect=[legacy_state, migrated_state]
            ),
            mock.patch.object(pipeline, "repair_amendments", return_value=True) as repair,
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[(2026, 3)]
            ),
            mock.patch.object(pipeline, "download_company_idx", return_value=[]),
            mock.patch.object(pipeline, "save_state"),
            mock.patch.object(pipeline, "save_cusip_map"),
        ):
            self.assertTrue(pipeline.run_all(4, rebuild_outputs=False))

        repair.assert_called_once_with(
            pipeline.AMENDMENT_MIGRATION_FILING_QUARTERS,
            rebuild_outputs=False,
            quarantine_failures=True,
            mark_migration=True,
        )

    def test_normal_run_retries_pending_migration_outside_current_index_window(self) -> None:
        state = {
            "processed": [],
            "_processed_set": set(),
            "amendment_reducer_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "amendment_migration_pending": {
                ADDITION_ACCESSION: {"cik": CIK, "report_date": REPORT_DATE}
            },
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
        }
        with (
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(
                pipeline, "retry_pending_amendment_migrations", return_value=0
            ) as retry,
            mock.patch.object(
                pipeline, "enforce_published_quarter_health", return_value=0
            ) as enforce_health,
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[(2026, 3)]
            ),
            mock.patch.object(pipeline, "download_company_idx", return_value=[]),
            mock.patch.object(pipeline, "save_state"),
            mock.patch.object(pipeline, "save_cusip_map"),
        ):
            self.assertTrue(pipeline.run_all(4, rebuild_outputs=False))

        retry.assert_called_once_with(state, {}, 4)
        enforce_health.assert_called_once_with(state)

    def test_retry_state_is_checkpointed_before_index_discovery_failure(
        self,
    ) -> None:
        state = {
            "processed": [],
            "_processed_set": set(),
            "amendment_reducer_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "amendment_migration_pending": {
                ADDITION_ACCESSION: {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                }
            },
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
            "quarter_health_pending": {},
        }
        cusip_map = {}
        with (
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(
                pipeline, "load_cusip_map", return_value=cusip_map
            ),
            mock.patch.object(
                pipeline, "retry_pending_amendment_migrations", return_value=1
            ) as retry,
            mock.patch.object(
                pipeline, "enforce_published_quarter_health", return_value=0
            ) as enforce_health,
            mock.patch.object(pipeline, "save_state") as save_state,
            mock.patch.object(pipeline, "save_cusip_map") as save_map,
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[(2026, 3)]
            ),
            mock.patch.object(
                pipeline,
                "download_company_idx",
                side_effect=pipeline.FilingDiscoveryError("index offline"),
            ),
        ):
            self.assertFalse(pipeline.run_all(4, rebuild_outputs=False))

        retry.assert_called_once_with(state, cusip_map, 4)
        enforce_health.assert_called_once_with(state)
        save_state.assert_called_once_with(state)
        save_map.assert_called_once_with(cusip_map)

    def test_retry_interruption_checkpoints_before_discovery(self) -> None:
        state = {
            "processed": [],
            "_processed_set": set(),
            "amendment_reducer_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "amendment_migration_pending": {
                ADDITION_ACCESSION: {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                }
            },
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
            "quarter_health_pending": {},
        }
        cusip_map = {}

        def interrupt_after_progress(*_args, **_kwargs) -> int:
            state["retry_progress"] = "durable"
            cusip_map["123456789"] = "XYZ"
            raise KeyboardInterrupt

        with (
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(
                pipeline, "load_cusip_map", return_value=cusip_map
            ),
            mock.patch.object(
                pipeline,
                "retry_pending_amendment_migrations",
                side_effect=interrupt_after_progress,
            ),
            mock.patch.object(pipeline, "save_state") as save_state,
            mock.patch.object(pipeline, "save_cusip_map") as save_map,
            mock.patch.object(pipeline, "get_recent_filing_quarters") as recent,
        ):
            self.assertFalse(pipeline.run_all(4, rebuild_outputs=False))

        save_state.assert_called_once_with(state)
        save_map.assert_called_once_with(cusip_map)
        recent.assert_not_called()
        self.assertEqual("durable", state["retry_progress"])
        self.assertEqual("XYZ", cusip_map["123456789"])

    def test_outer_worker_quarantines_only_unprocessed_trigger(self) -> None:
        state = {
            "processed": [BASE_ACCESSION],
            "_processed_set": {BASE_ACCESSION},
            "_quarantined": {},
            "amendment_reducer_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "amendment_migration_pending": {},
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
        }
        with (
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(pipeline, "retry_pending_amendment_migrations"),
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[(2026, 3)]
            ),
            mock.patch.object(
                pipeline,
                "download_company_idx",
                return_value=[BASE_ROW, ADDITION_ROW],
            ),
            mock.patch.object(
                pipeline,
                "replay_quarters_for_cik",
                side_effect=pipeline.FilingParseError("new trigger failed"),
            ) as replay,
            mock.patch.object(pipeline, "save_state"),
            mock.patch.object(pipeline, "save_cusip_map"),
        ):
            self.assertTrue(pipeline.run_all(4, rebuild_outputs=False))

        replayed_triggers = replay.call_args.args[1]
        self.assertEqual([ADDITION_ACCESSION], [
            trigger["accession"] for trigger in replayed_triggers
        ])
        self.assertIn(BASE_ACCESSION, state["_processed_set"])
        self.assertNotIn(BASE_ACCESSION, state["_quarantined"])
        self.assertNotIn(ADDITION_ACCESSION, state["_processed_set"])
        self.assertIn(ADDITION_ACCESSION, state["_quarantined"])


if __name__ == "__main__":
    unittest.main()
