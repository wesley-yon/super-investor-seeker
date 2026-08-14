"""Focused unit coverage for deterministic 13F amendment replay.

The fixtures are deliberately small and synthetic.  These tests exercise the
pure primary-document parser and filing-chain reducer without touching SEC,
OpenFIGI, pipeline state, or generated data.
"""

from __future__ import annotations

import copy
import hashlib
import unittest

import pipeline


CIK = 123456
REPORT_DATE = "2025-12-31"

ORIGINAL_ACCESSION = "0000123456-26-000001"
NEW_HOLDINGS_1_ACCESSION = "0000123456-26-000002"
NEW_HOLDINGS_2_ACCESSION = "0000123456-26-000003"
RESTATEMENT_ACCESSION = "0000123456-26-000004"
UNKNOWN_ACCESSION = "0000123456-26-000005"


def primary_document_xml(
    *,
    is_amendment: bool,
    amendment_number: int | None = None,
    amendment_type: str | None = None,
    entry_total: int = 2,
    value_total: int = 150,
) -> bytes:
    amendment_no_xml = (
        f"<amendmentNo>{amendment_number}</amendmentNo>"
        if amendment_number is not None
        else ""
    )
    amendment_info_xml = (
        "<amendmentInfo>"
        f"<amendmentType>{amendment_type}</amendmentType>"
        "</amendmentInfo>"
        if amendment_type is not None
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
  <headerData>
    <filerInfo>
      <periodOfReport>12-31-2025</periodOfReport>
      <filer>
        <credentials><cik>0000123456</cik></credentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <reportCalendarOrQuarter>12-31-2025</reportCalendarOrQuarter>
      <filingManager>
        <name>Example Capital Management, LLC</name>
        <address>
          <street1>100 Main Street</street1>
          <city>New York</city>
          <stateOrCountry>NY</stateOrCountry>
          <zipCode>10001</zipCode>
        </address>
      </filingManager>
      <form13FFileNumber>028-12345</form13FFileNumber>
      <isAmendment>{str(is_amendment).lower()}</isAmendment>
      {amendment_no_xml}
      {amendment_info_xml}
    </coverPage>
    <summaryPage>
      <tableEntryTotal>{entry_total}</tableEntryTotal>
      <tableValueTotal>{value_total}</tableValueTotal>
    </summaryPage>
  </formData>
</edgarSubmission>
""".encode("utf-8")


def holding(
    cusip: str,
    value: int,
    shares: int,
    *,
    issuer: str | None = None,
    other_manager: str = "1",
) -> dict:
    return {
        "ticker": None,
        "issuer": issuer or f"ISSUER {cusip}",
        "cusip": cusip,
        "class": "COM",
        "value": value,
        "shares": shares,
        "holding_type": "EQUITY",
        "share_amount_type": "SH",
        "investment_discretion": "SOLE",
        "other_manager": other_manager,
    }


def component(
    accession: str,
    amendment_kind: str,
    accepted_at: str,
    *,
    amendment_number: int | None = None,
    holdings: list[dict] | None = None,
    source_hash: str | None = None,
) -> dict:
    rows = copy.deepcopy(holdings or [])
    return {
        "cik": CIK,
        "report_date": REPORT_DATE,
        "accession": accession,
        "accepted_at": accepted_at,
        "filing_date": accepted_at[:10],
        "form_type": "13F-HR" if amendment_kind == "ORIGINAL" else "13F-HR/A",
        "amendment_kind": amendment_kind,
        "amendment_number": amendment_number,
        "holdings": rows,
        "reported_entry_total": len(rows),
        "reported_value_total": sum(row["value"] for row in rows),
        "source_hash": source_hash or hashlib.sha256(accession.encode("utf-8")).hexdigest(),
        "parser_version": 1,
    }


BASE_ROWS = [holding("111111111", 100, 10, other_manager="1")]
NEW_ROWS_1 = [holding("111111111", 50, 5, other_manager="2")]
NEW_ROWS_2 = [holding("222222222", 25, 5)]
RESTATED_ROWS = [
    holding("111111111", 80, 8),
    holding("333333333", 20, 2),
]


def original() -> dict:
    return component(
        ORIGINAL_ACCESSION,
        "ORIGINAL",
        "2026-02-14T15:00:00Z",
        holdings=BASE_ROWS,
    )


def new_holdings_1() -> dict:
    return component(
        NEW_HOLDINGS_1_ACCESSION,
        "NEW_HOLDINGS",
        "2026-05-15T15:00:00Z",
        amendment_number=1,
        holdings=NEW_ROWS_1,
    )


def new_holdings_2() -> dict:
    return component(
        NEW_HOLDINGS_2_ACCESSION,
        "NEW_HOLDINGS",
        "2026-06-01T15:00:00Z",
        amendment_number=2,
        holdings=NEW_ROWS_2,
    )


def restatement(*, amendment_number: int = 2) -> dict:
    return component(
        RESTATEMENT_ACCESSION,
        "RESTATEMENT",
        "2026-06-15T15:00:00Z",
        amendment_number=amendment_number,
        holdings=RESTATED_ROWS,
    )


class PrimaryDocumentMetadataTests(unittest.TestCase):
    def test_original_metadata(self) -> None:
        metadata = pipeline.parse_primary_document(
            primary_document_xml(is_amendment=False),
            "13F-HR",
        )

        self.assertEqual(REPORT_DATE, metadata["report_date"])
        self.assertFalse(metadata["is_amendment"])
        self.assertIsNone(metadata["amendment_number"])
        self.assertEqual("ORIGINAL", metadata["amendment_kind"])
        self.assertEqual(2, metadata["reported_entry_total"])
        self.assertEqual(150, metadata["reported_value_total"])
        self.assertEqual(CIK, metadata["filer_cik"])
        self.assertEqual(
            "Example Capital Management, LLC",
            metadata["filing_manager_name"],
        )
        self.assertEqual(
            {
                "street1": "100 Main Street",
                "city": "New York",
                "state_or_country": "NY",
                "zip_code": "10001",
            },
            metadata["filing_manager_address"],
        )
        self.assertEqual("028-12345", metadata["form_13f_file_number"])

    def test_supported_amendment_metadata(self) -> None:
        cases = (
            (" NEW HOLDINGS ", "NEW_HOLDINGS"),
            ("RESTATEMENT", "RESTATEMENT"),
        )

        for sec_value, expected_kind in cases:
            with self.subTest(amendment_type=sec_value):
                metadata = pipeline.parse_primary_document(
                    primary_document_xml(
                        is_amendment=True,
                        amendment_number=1,
                        amendment_type=sec_value,
                        entry_total=1,
                        value_total=50,
                    ),
                    "13F-HR/A",
                )

                self.assertEqual(REPORT_DATE, metadata["report_date"])
                self.assertTrue(metadata["is_amendment"])
                self.assertEqual(1, metadata["amendment_number"])
                self.assertEqual(expected_kind, metadata["amendment_kind"])
                self.assertEqual(1, metadata["reported_entry_total"])
                self.assertEqual(50, metadata["reported_value_total"])

    def test_unknown_or_missing_amendment_type_is_not_inferred(self) -> None:
        for amendment_type in (None, "OTHER CORRECTION"):
            with self.subTest(amendment_type=amendment_type):
                metadata = pipeline.parse_primary_document(
                    primary_document_xml(
                        is_amendment=True,
                        amendment_number=1,
                        amendment_type=amendment_type,
                    ),
                    "13F-HR/A",
                )

                self.assertTrue(metadata["is_amendment"])
                self.assertEqual(1, metadata["amendment_number"])
                self.assertEqual("UNKNOWN", metadata["amendment_kind"])


class AmendmentChainReducerTests(unittest.TestCase):
    def test_source_provenance_preserves_filer_name_discrepancy(self) -> None:
        filing = original()
        filing["filer_name_discrepancy"] = {
            "discovery_name": "Current Adviser, LLC",
            "primary_name": "Prior Adviser, LLC",
        }

        quarter = pipeline.compose_quarter_filings([filing])

        self.assertEqual(
            filing["filer_name_discrepancy"],
            quarter["source_filings"][0]["filer_name_discrepancy"],
        )

    def assert_holding_totals(
        self,
        quarter: dict,
        expected: dict[str, tuple[int, int]],
    ) -> None:
        actual = {
            row["cusip"]: (row["value"], row["shares"])
            for row in quarter["holdings"]
        }
        self.assertEqual(expected, actual)
        self.assertEqual(sum(value for value, _shares in expected.values()), quarter["total_value"])
        self.assertEqual(len(expected), quarter["num_holdings"])
        self.assertIsInstance(quarter["composition_hash"], str)
        self.assertRegex(quarter["composition_hash"], r"^[0-9a-f]{64}$")
        self.assertEqual(2, pipeline.AMENDMENT_REDUCER_VERSION)
        self.assertEqual(
            pipeline.AMENDMENT_REDUCER_VERSION,
            quarter["composition_version"],
        )
        self.assertTrue(quarter["is_complete"])

    def assert_source_provenance(
        self,
        quarter: dict,
        filings: list[dict],
        applied_accessions: list[str],
    ) -> None:
        unique_by_accession: dict[str, dict] = {}
        for filing in filings:
            unique_by_accession.setdefault(filing["accession"], filing)
        expected_sources = sorted(
            unique_by_accession.values(),
            key=lambda filing: (filing["accepted_at"], filing["accession"]),
        )
        sources = quarter["source_filings"]

        self.assertEqual(
            [filing["accession"] for filing in expected_sources],
            [source["accession"] for source in sources],
        )
        self.assertEqual(
            applied_accessions,
            [source["accession"] for source in sources if source["applied"]],
        )
        required_keys = {
            "accession",
            "form_type",
            "filing_date",
            "accepted_at",
            "amendment_number",
            "amendment_kind",
            "source_hash",
            "reported_entry_total",
            "reported_value_total",
            "applied",
            "composition_action",
        }
        base_index = next(
            index
            for index, source in enumerate(expected_sources)
            if source["accession"] == quarter["base_accession"]
        )
        for source_index, (expected, source) in enumerate(
            zip(expected_sources, sources)
        ):
            self.assertTrue(required_keys.issubset(source))
            for key in required_keys - {"applied", "composition_action"}:
                self.assertEqual(expected[key], source[key])
            self.assertEqual(
                expected["accession"] in applied_accessions,
                source["applied"],
            )
            if source_index < base_index:
                expected_action = "SUPERSEDED"
            elif source_index == base_index:
                expected_action = (
                    "REPLACE"
                    if source["amendment_kind"] == "NEW_HOLDINGS"
                    else "BASE"
                )
            else:
                expected_action = "APPEND"
            self.assertEqual(expected_action, source["composition_action"])

            if expected_action in {"APPEND", "REPLACE"}:
                overlap = source.get("new_holdings_overlap")
                self.assertIsInstance(overlap, dict)
                self.assertEqual(
                    {
                        "identity_version",
                        "matched_rows",
                        "prior_rows",
                        "amendment_rows",
                        "exact_positions",
                    },
                    set(overlap),
                )
                self.assertEqual(1, overlap["identity_version"])

    def test_reducer_matrix(self) -> None:
        cases = (
            (
                "original",
                [original()],
                ORIGINAL_ACCESSION,
                [ORIGINAL_ACCESSION],
                {"111111111": (100, 10)},
            ),
            (
                "original_then_new_holdings",
                [original(), new_holdings_1()],
                ORIGINAL_ACCESSION,
                [ORIGINAL_ACCESSION, NEW_HOLDINGS_1_ACCESSION],
                {"111111111": (150, 15)},
            ),
            (
                "multiple_new_holdings",
                [original(), new_holdings_1(), new_holdings_2()],
                ORIGINAL_ACCESSION,
                [
                    ORIGINAL_ACCESSION,
                    NEW_HOLDINGS_1_ACCESSION,
                    NEW_HOLDINGS_2_ACCESSION,
                ],
                {"111111111": (150, 15), "222222222": (25, 5)},
            ),
            (
                "restatement_replaces_original",
                [original(), restatement(amendment_number=1)],
                RESTATEMENT_ACCESSION,
                [RESTATEMENT_ACCESSION],
                {"111111111": (80, 8), "333333333": (20, 2)},
            ),
            (
                "restatement_resets_earlier_supplement",
                [original(), new_holdings_1(), restatement()],
                RESTATEMENT_ACCESSION,
                [RESTATEMENT_ACCESSION],
                {"111111111": (80, 8), "333333333": (20, 2)},
            ),
            (
                "supplement_after_restatement",
                [
                    original(),
                    restatement(amendment_number=1),
                    component(
                        NEW_HOLDINGS_2_ACCESSION,
                        "NEW_HOLDINGS",
                        "2026-07-01T15:00:00Z",
                        amendment_number=2,
                        holdings=NEW_ROWS_2,
                    ),
                ],
                RESTATEMENT_ACCESSION,
                [RESTATEMENT_ACCESSION, NEW_HOLDINGS_2_ACCESSION],
                {
                    "111111111": (80, 8),
                    "222222222": (25, 5),
                    "333333333": (20, 2),
                },
            ),
        )

        for name, filings, base_accession, applied_accessions, totals in cases:
            with self.subTest(name=name):
                quarter = pipeline.compose_quarter_filings(filings)

                self.assertEqual(REPORT_DATE, quarter["report_date"])
                self.assertEqual(base_accession, quarter["base_accession"])
                self.assertEqual(applied_accessions, quarter["applied_accessions"])
                self.assert_holding_totals(quarter, totals)
                self.assert_source_provenance(quarter, filings, applied_accessions)

    def test_same_cusip_for_different_manager_is_a_disjoint_supplement(self) -> None:
        quarter = pipeline.compose_quarter_filings(
            [original(), new_holdings_1()]
        )

        self.assertEqual(
            [ORIGINAL_ACCESSION, NEW_HOLDINGS_1_ACCESSION],
            quarter["applied_accessions"],
        )
        self.assert_holding_totals(quarter, {"111111111": (150, 15)})
        self.assertNotIn("investment_discretion", quarter["holdings"][0])
        self.assertNotIn("other_manager", quarter["holdings"][0])
        supplement_source = quarter["source_filings"][1]
        self.assertEqual("APPEND", supplement_source["composition_action"])
        self.assertEqual(
            {
                "identity_version": 1,
                "matched_rows": 0,
                "prior_rows": 1,
                "amendment_rows": 1,
                "exact_positions": False,
            },
            supplement_source["new_holdings_overlap"],
        )

    def test_complete_position_copy_replaces_even_below_five_rows(self) -> None:
        base_rows = [
            holding(f"30000000{index}", 100 + index, 10 + index)
            for index in range(4)
        ]
        replacement_rows = [
            holding(f"30000000{index}", 900 + index, 90 + index)
            for index in reversed(range(4))
        ]
        base = component(
            ORIGINAL_ACCESSION,
            "ORIGINAL",
            "2026-02-14T15:00:00Z",
            holdings=base_rows,
        )
        replacement = component(
            NEW_HOLDINGS_1_ACCESSION,
            "NEW_HOLDINGS",
            "2026-05-15T15:00:00Z",
            amendment_number=1,
            holdings=replacement_rows,
        )

        quarter = pipeline.compose_quarter_filings([base, replacement])

        self.assertEqual(NEW_HOLDINGS_1_ACCESSION, quarter["base_accession"])
        self.assertEqual(
            [NEW_HOLDINGS_1_ACCESSION],
            quarter["applied_accessions"],
        )
        self.assert_holding_totals(
            quarter,
            {
                row["cusip"]: (row["value"], row["shares"])
                for row in replacement_rows
            },
        )
        self.assertEqual(
            ["SUPERSEDED", "REPLACE"],
            [
                source["composition_action"]
                for source in quarter["source_filings"]
            ],
        )
        replacement_source = quarter["source_filings"][1]
        self.assertEqual("NEW_HOLDINGS", replacement_source["amendment_kind"])
        self.assertEqual(
            {
                "identity_version": 1,
                "matched_rows": 4,
                "prior_rows": 4,
                "amendment_rows": 4,
                "exact_positions": True,
            },
            replacement_source["new_holdings_overlap"],
        )

    def test_full_portfolio_amendment_total_is_not_doubled(self) -> None:
        base = component(
            ORIGINAL_ACCESSION,
            "ORIGINAL",
            "2026-02-14T15:00:00Z",
            holdings=[holding("141832900", 45_449_051_090, 10)],
        )
        replacement = component(
            NEW_HOLDINGS_1_ACCESSION,
            "NEW_HOLDINGS",
            "2026-05-15T15:00:00Z",
            amendment_number=1,
            holdings=[holding("141832900", 46_309_195_208, 11)],
        )

        quarter = pipeline.compose_quarter_filings([base, replacement])

        self.assertEqual(46_309_195_208, quarter["total_value"])
        self.assertNotEqual(91_758_246_298, quarter["total_value"])
        self.assertEqual(
            [NEW_HOLDINGS_1_ACCESSION],
            quarter["applied_accessions"],
        )

    def test_manager_lists_are_canonicalized_before_overlap(self) -> None:
        base = component(
            ORIGINAL_ACCESSION,
            "ORIGINAL",
            "2026-02-14T15:00:00Z",
            holdings=[
                holding("321321321", 100, 10, other_manager="02, 01")
            ],
        )
        replacement = component(
            NEW_HOLDINGS_1_ACCESSION,
            "NEW_HOLDINGS",
            "2026-05-15T15:00:00Z",
            amendment_number=1,
            holdings=[
                holding("321321321", 110, 11, other_manager="1 2")
            ],
        )

        quarter = pipeline.compose_quarter_filings([base, replacement])

        self.assertEqual(
            [NEW_HOLDINGS_1_ACCESSION],
            quarter["applied_accessions"],
        )

    def test_bidirectional_ninety_percent_overlap_is_replacement(self) -> None:
        base_rows = [
            holding(f"40000000{index}", 100 + index, 10 + index)
            for index in range(10)
        ]
        replacement_rows = [
            holding(f"40000000{index}", 500 + index, 50 + index)
            for index in range(9)
        ] + [holding("499999999", 999, 99)]
        base = component(
            ORIGINAL_ACCESSION,
            "ORIGINAL",
            "2026-02-14T15:00:00Z",
            holdings=base_rows,
        )
        replacement = component(
            NEW_HOLDINGS_1_ACCESSION,
            "NEW_HOLDINGS",
            "2026-05-15T15:00:00Z",
            amendment_number=1,
            holdings=replacement_rows,
        )

        quarter = pipeline.compose_quarter_filings([base, replacement])

        self.assertEqual(NEW_HOLDINGS_1_ACCESSION, quarter["base_accession"])
        self.assertEqual(
            [NEW_HOLDINGS_1_ACCESSION],
            quarter["applied_accessions"],
        )
        self.assertEqual(
            {
                "identity_version": 1,
                "matched_rows": 9,
                "prior_rows": 10,
                "amendment_rows": 10,
                "exact_positions": False,
            },
            quarter["source_filings"][1]["new_holdings_overlap"],
        )
        self.assert_holding_totals(
            quarter,
            {
                row["cusip"]: (row["value"], row["shares"])
                for row in replacement_rows
            },
        )

    def test_overlap_below_ninety_percent_is_ambiguous(self) -> None:
        base_rows = [
            holding(f"50000000{index}", 100 + index, 10 + index)
            for index in range(10)
        ]
        partial_copy = [
            holding(f"50000000{index}", 500 + index, 50 + index)
            for index in range(8)
        ] + [
            holding("599999998", 998, 98),
            holding("599999999", 999, 99),
        ]
        base = component(
            ORIGINAL_ACCESSION,
            "ORIGINAL",
            "2026-02-14T15:00:00Z",
            holdings=base_rows,
        )
        amendment = component(
            NEW_HOLDINGS_1_ACCESSION,
            "NEW_HOLDINGS",
            "2026-05-15T15:00:00Z",
            amendment_number=1,
            holdings=partial_copy,
        )

        self.assert_chain_error(
            [base, amendment],
            "ambiguous_new_holdings_overlap",
        )

    def test_subset_overlap_is_ambiguous_not_a_replacement(self) -> None:
        base_rows = [
            holding(f"60000000{index}", 100 + index, 10 + index)
            for index in range(10)
        ]
        subset = [
            holding(f"60000000{index}", 500 + index, 50 + index)
            for index in range(3)
        ]
        base = component(
            ORIGINAL_ACCESSION,
            "ORIGINAL",
            "2026-02-14T15:00:00Z",
            holdings=base_rows,
        )
        amendment = component(
            NEW_HOLDINGS_1_ACCESSION,
            "NEW_HOLDINGS",
            "2026-05-15T15:00:00Z",
            amendment_number=1,
            holdings=subset,
        )

        self.assert_chain_error(
            [base, amendment],
            "ambiguous_new_holdings_overlap",
        )

    def test_later_replacement_resets_prior_supplements_then_allows_append(
        self,
    ) -> None:
        base_rows = [
            holding(f"70000000{index}", 100 + index, 10 + index)
            for index in range(6)
        ]
        first_supplement_rows = [
            holding(f"70000000{index}", 200 + index, 20 + index)
            for index in range(6, 10)
        ]
        replacement_rows = [
            holding(f"70000000{index}", 500 + index, 50 + index)
            for index in range(9)
        ] + [holding("799999999", 999, 99)]
        final_supplement_rows = [holding("800000000", 1000, 100)]
        replacement_accession = "0000123456-26-000006"
        final_accession = "0000123456-26-000007"
        filings = [
            component(
                ORIGINAL_ACCESSION,
                "ORIGINAL",
                "2026-02-14T15:00:00Z",
                holdings=base_rows,
            ),
            component(
                NEW_HOLDINGS_1_ACCESSION,
                "NEW_HOLDINGS",
                "2026-05-15T15:00:00Z",
                amendment_number=1,
                holdings=first_supplement_rows,
            ),
            component(
                replacement_accession,
                "NEW_HOLDINGS",
                "2026-06-01T15:00:00Z",
                amendment_number=2,
                holdings=replacement_rows,
            ),
            component(
                final_accession,
                "NEW_HOLDINGS",
                "2026-07-01T15:00:00Z",
                amendment_number=3,
                holdings=final_supplement_rows,
            ),
        ]

        quarter = pipeline.compose_quarter_filings(filings)

        self.assertEqual(replacement_accession, quarter["base_accession"])
        self.assertEqual(
            [replacement_accession, final_accession],
            quarter["applied_accessions"],
        )
        self.assertEqual(
            ["SUPERSEDED", "SUPERSEDED", "REPLACE", "APPEND"],
            [
                source["composition_action"]
                for source in quarter["source_filings"]
            ],
        )
        self.assertEqual(
            {
                "identity_version": 1,
                "matched_rows": 9,
                "prior_rows": 10,
                "amendment_rows": 10,
                "exact_positions": False,
            },
            quarter["source_filings"][2]["new_holdings_overlap"],
        )
        self.assertEqual(
            {
                "identity_version": 1,
                "matched_rows": 0,
                "prior_rows": 10,
                "amendment_rows": 1,
                "exact_positions": False,
            },
            quarter["source_filings"][3]["new_holdings_overlap"],
        )
        expected_rows = replacement_rows + final_supplement_rows
        self.assert_holding_totals(
            quarter,
            {
                row["cusip"]: (row["value"], row["shares"])
                for row in expected_rows
            },
        )
        self.assert_source_provenance(
            quarter,
            filings,
            [replacement_accession, final_accession],
        )

    def test_input_order_is_deterministic(self) -> None:
        filings = [original(), new_holdings_1(), restatement(), component(
            NEW_HOLDINGS_2_ACCESSION,
            "NEW_HOLDINGS",
            "2026-07-01T15:00:00Z",
            amendment_number=3,
            holdings=NEW_ROWS_2,
        )]

        forward = pipeline.compose_quarter_filings(copy.deepcopy(filings))
        reverse = pipeline.compose_quarter_filings(copy.deepcopy(list(reversed(filings))))
        shuffled = pipeline.compose_quarter_filings(
            copy.deepcopy([filings[2], filings[0], filings[3], filings[1]])
        )

        self.assertEqual(forward, reverse)
        self.assertEqual(forward, shuffled)
        self.assertEqual(
            [RESTATEMENT_ACCESSION, NEW_HOLDINGS_2_ACCESSION],
            forward["applied_accessions"],
        )

    def assert_chain_error(self, filings: list[dict], reason: str) -> None:
        with self.assertRaises(pipeline.FilingChainError) as caught:
            pipeline.compose_quarter_filings(filings)
        self.assertEqual(reason, caught.exception.reason)

    def test_unknown_in_active_chain_is_quarantined(self) -> None:
        unknown = component(
            UNKNOWN_ACCESSION,
            "UNKNOWN",
            "2026-05-15T15:00:00Z",
            amendment_number=1,
            holdings=NEW_ROWS_1,
        )

        self.assert_chain_error(
            [original(), unknown],
            "unknown_amendment_type",
        )

    def test_unknown_before_later_restatement_is_superseded(self) -> None:
        unknown = component(
            UNKNOWN_ACCESSION,
            "UNKNOWN",
            "2026-05-15T15:00:00Z",
            amendment_number=1,
            holdings=NEW_ROWS_1,
        )
        later_restatement = restatement(amendment_number=2)

        quarter = pipeline.compose_quarter_filings(
            [original(), unknown, later_restatement]
        )

        self.assertEqual(RESTATEMENT_ACCESSION, quarter["base_accession"])
        self.assertEqual([RESTATEMENT_ACCESSION], quarter["applied_accessions"])
        self.assert_holding_totals(
            quarter,
            {"111111111": (80, 8), "333333333": (20, 2)},
        )

    def test_unparseable_number_before_later_restatement_is_superseded(self) -> None:
        unknown = component(
            UNKNOWN_ACCESSION,
            "UNKNOWN",
            "2026-05-15T15:00:00Z",
            amendment_number=None,
            holdings=[],
        )
        later_restatement = restatement(amendment_number=2)

        quarter = pipeline.compose_quarter_filings(
            [original(), unknown, later_restatement]
        )

        self.assertEqual(RESTATEMENT_ACCESSION, quarter["base_accession"])
        self.assertEqual([RESTATEMENT_ACCESSION], quarter["applied_accessions"])

    def test_new_holdings_without_base_is_incomplete(self) -> None:
        self.assert_chain_error([new_holdings_1()], "missing_base")

    def test_empty_new_holdings_amendment_is_quarantined(self) -> None:
        base = original()
        amendment = component(
            NEW_HOLDINGS_1_ACCESSION,
            "NEW_HOLDINGS",
            "2026-05-15T15:00:00Z",
            amendment_number=1,
            holdings=[],
        )

        self.assert_chain_error(
            [base, amendment],
            "empty_new_holdings",
        )

    def test_identical_duplicate_accession_is_idempotent(self) -> None:
        supplement = new_holdings_1()

        quarter = pipeline.compose_quarter_filings(
            [original(), supplement, copy.deepcopy(supplement)]
        )

        self.assertEqual(
            [ORIGINAL_ACCESSION, NEW_HOLDINGS_1_ACCESSION],
            quarter["applied_accessions"],
        )
        self.assert_holding_totals(quarter, {"111111111": (150, 15)})
        self.assert_source_provenance(
            quarter,
            [original(), supplement, copy.deepcopy(supplement)],
            [ORIGINAL_ACCESSION, NEW_HOLDINGS_1_ACCESSION],
        )

    def test_conflicting_duplicate_accession_is_quarantined(self) -> None:
        first = new_holdings_1()
        conflicting = copy.deepcopy(first)
        conflicting["source_hash"] = "f" * 64
        conflicting["holdings"][0]["value"] = 500

        self.assert_chain_error(
            [original(), first, conflicting],
            "duplicate_accession_conflict",
        )

    def test_amendment_numbering_conflicts_are_quarantined(self) -> None:
        duplicate_number = new_holdings_2()
        duplicate_number["amendment_number"] = 1
        reverse_number = new_holdings_2()
        reverse_number["accepted_at"] = "2026-05-01T15:00:00Z"

        cases = (
            ("gap", [original(), component(
                NEW_HOLDINGS_1_ACCESSION,
                "NEW_HOLDINGS",
                "2026-05-15T15:00:00Z",
                amendment_number=2,
                holdings=NEW_ROWS_1,
            )]),
            ("duplicate", [original(), new_holdings_1(), duplicate_number]),
            ("non_monotonic", [original(), new_holdings_1(), reverse_number]),
        )

        for name, filings in cases:
            with self.subTest(name=name):
                self.assert_chain_error(filings, "amendment_number_conflict")

    def test_equal_acceptance_time_uses_declared_amendment_sequence(self) -> None:
        second = new_holdings_2()
        second["accepted_at"] = new_holdings_1()["accepted_at"]

        quarter = pipeline.compose_quarter_filings(
            [original(), second, new_holdings_1()]
        )

        self.assertEqual(
            [
                ORIGINAL_ACCESSION,
                NEW_HOLDINGS_1_ACCESSION,
                NEW_HOLDINGS_2_ACCESSION,
            ],
            quarter["applied_accessions"],
        )

    def test_equal_acceptance_time_without_unique_sequence_is_quarantined(self) -> None:
        second = new_holdings_2()
        second["accepted_at"] = new_holdings_1()["accepted_at"]
        second["amendment_number"] = 1

        self.assert_chain_error(
            [original(), new_holdings_1(), second], "ambiguous_order"
        )


if __name__ == "__main__":
    unittest.main()
