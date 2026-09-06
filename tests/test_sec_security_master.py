from __future__ import annotations

import copy
import io
import json
import stat
import subprocess
import sys
import tempfile
import unittest
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import requests

import sec_security_master as master


SHA_A = "a" * 64
SHA_B = "b" * 64
FTD_URL = "https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608a.zip"
OLD_FTD_URL = "https://www.sec.gov/files/data/fails-deliver-data/cnsfails202607b.zip"
REVISED_FTD_URL = (
    "https://www.sec.gov/files/data/other/fails-deliver-data/"
    "cnsfails202308b_0.zip"
)
LEGACY_FTD_URL = (
    "https://www.sec.gov/files/data/fails-deliver-data/"
    "cnsp_sec_fails_2009q2.zip"
)
FTD_2004_Q1_URL = (
    "https://www.sec.gov/files/data/fails-deliver-data/"
    "cnsp_sec_fails_2004q1.zip"
)
FTD_2004_Q2_URL = (
    "https://www.sec.gov/files/data/fails-deliver-data/"
    "cnsp_sec_fails_2004q2.zip"
)
LIST_URL = "https://www.sec.gov/files/investment/13flist2026q2-txt.txt"


def make_ftd_pipe(rows: list[tuple[str, str, str, int, str, str]]) -> bytes:
    lines = [
        "SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE"
    ]
    lines.extend("|".join(map(str, row)) for row in rows)
    return ("\r\n".join(lines) + "\r\n").encode()


def make_ftd_zip(rows: list[tuple[str, str, str, int, str, str]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("cnsfails.txt", make_ftd_pipe(rows))
    return output.getvalue()


def make_quarterly_ftd_zip(
    rows_by_member: list[list[tuple[str, str, str, int, str, str]]],
) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for index, rows in enumerate(rows_by_member, start=1):
            archive.writestr(f"fails-part-{index}.txt", make_ftd_pipe(rows))
    return output.getvalue()


def make_13f_line(
    cusip: str,
    issuer: str,
    description: str,
    *,
    option: str = "",
    status: str = "",
) -> str:
    return (
        f"{cusip:<9.9}{option:<1.1}{issuer:<30.30}{description:<27.27}"
        f"{status:<3.3}{'':9}E"
    )


def source_state(
    *,
    rows: list[dict] | None = None,
    symbols: list[str] | None = None,
    symbol_titles: dict[str, list[str]] | None = None,
    symbol_exchanges: dict[str, list[str]] | None = None,
    official_rows: list[dict] | None = None,
) -> dict:
    sources: dict[str, dict] = {}
    if rows is not None:
        rows_by_url: dict[str, list[dict]] = {}
        for row in rows:
            raw_date = str(row["settlement_date"])
            parsed_date = datetime.strptime(
                raw_date,
                "%Y-%m-%d" if "-" in raw_date else "%Y%m%d",
            ).date()
            base = "https://www.sec.gov/files/data/fails-deliver-data/"
            if parsed_date < date(2009, 7, 1):
                quarter = (parsed_date.month - 1) // 3 + 1
                url = (
                    f"{base}cnsp_sec_fails_{parsed_date.year}q{quarter}.zip"
                )
            else:
                half = "a" if parsed_date.day <= 14 else "b"
                url = (
                    f"{base}cnsfails{parsed_date.year}{parsed_date.month:02d}"
                    f"{half}.zip"
                )
            rows_by_url.setdefault(url, []).append(row)
        for url, archive_rows in rows_by_url.items():
            compacted_rows = master.compact_ftd_records(archive_rows)
            sources[url] = {
                "url": url,
                "kind": "sec_ftd_archive",
                "sha256": SHA_A,
                "accepted_at": "2026-08-20T12:00:00Z",
                "records": compacted_rows,
                "record_count": len(compacted_rows),
                "raw_record_count": len(archive_rows),
                "filter_all_cusips": True,
            }
    if symbols is not None:
        symbols = sorted(set(symbols))
        sources[master.SEC_COMPANY_TICKERS_URL] = {
            "url": master.SEC_COMPANY_TICKERS_URL,
            "kind": "sec_company_tickers",
            "sha256": SHA_B,
            "accepted_at": "2026-08-20T12:00:00Z",
            "symbols": symbols,
            "symbol_titles": symbol_titles or {},
            "symbol_exchanges": symbol_exchanges or {},
            "symbol_count": len(symbols),
        }
    if official_rows is not None:
        sources[LIST_URL] = {
            "url": LIST_URL,
            "kind": "sec_13f_list",
            "sha256": "c" * 64,
            "accepted_at": "2026-08-20T12:00:00Z",
            "list_period": "2026Q2",
            "records": official_rows,
            "record_count": len(official_rows),
        }
    return {
        "schema_version": master.SOURCE_STATE_SCHEMA_VERSION,
        "updated_at": "2026-08-20T12:00:00Z",
        "current_filter_universe_sha256": None,
        "current_filter_universe_count": 0,
        "ftd_processed_filter_universe_sha256": None,
        "ftd_processed_filter_universe_count": 0,
        "ftd_filter_cusips": [],
        "ftd_timeline": {},
        "ftd_mutable_tail": {},
        "filter_universes": {},
        "required_filter_coverage_urls": [],
        "edgar_evidence": {},
        "edgar_discovery": {},
        "sources": sources,
    }


def ftd_record(
    settlement_date: str,
    symbol: str = "AAPL",
    *,
    cusip: str = "037833100",
    description: str = "APPLE INC",
) -> dict:
    return {
        "settlement_date": settlement_date,
        "cusip": cusip,
        "symbol": symbol,
        "quantity": 100,
        "description": description,
        "price": "200.00",
    }


def compact_2004_boundary_pair(
    boundary_rows: list[tuple[str, str, str, int, str, str]],
) -> tuple[dict, dict]:
    filter_digest = master._filter_universe_sha256(["037833100"])
    q1 = master._compact_ftd_payload(
        make_ftd_zip([
            ("20040322", "037833100", "AAPL", 50, "APPLE INC", "99"),
            *boundary_rows,
        ]),
        source_url=FTD_2004_Q1_URL,
        target_cusips={"037833100"},
        filter_universe_sha256=filter_digest,
    )
    q2 = master._compact_ftd_payload(
        make_ftd_zip([
            *boundary_rows,
            ("20040503", "037833100", "AAPL", 75, "APPLE INC", "101"),
            ("20040601", "037833100", "AAPL", 80, "APPLE INC", "102"),
        ]),
        source_url=FTD_2004_Q2_URL,
        target_cusips={"037833100"},
        filter_universe_sha256=filter_digest,
    )
    return q1, q2


def source_state_with_2004_boundary(
    q1: dict,
    q2: dict | None = None,
) -> dict:
    state = source_state()
    state["ftd_filter_cusips"] = ["037833100"]
    for url, sha256, parsed in (
        (FTD_2004_Q1_URL, SHA_A, q1),
        (FTD_2004_Q2_URL, SHA_B, q2),
    ):
        if parsed is None:
            continue
        state["sources"][url] = master._accepted_source_entry(
            url=url,
            kind="sec_ftd_archive",
            sha256=sha256,
            accepted_at="2026-08-20T12:00:00Z",
            parsed=parsed,
        )
    return state


def official_record(
    *,
    cusip: str = "037833100",
    issuer: str = "APPLE INC",
    description: str = "COM",
    status: str = "",
    option_indicator: str = "",
) -> dict:
    return {
        "cusip": cusip,
        "issuer": issuer,
        "description": description,
        "status": status,
        "option_indicator": option_indicator,
    }


def edgar_discovery_record(
    *,
    checked_at: str = "2026-08-20T12:00:00Z",
    status: str = "sources_found",
    terminal: bool = True,
    include_success: bool = False,
    last_successful_check_at: str | None = "2026-08-20T12:00:00Z",
) -> dict:
    record = {
        "cusip": "02079K305",
        "status": status,
        "terminal": terminal,
        "reason": "exact_schedule_cusip_and_ixbrl_class_bridge",
        "issuer_cik": "0001652044",
        "security_class": "Class A Common Stock",
        "schedule_candidate_count": 1,
        "exact_schedule_count": 1,
        "periodic_candidate_count": 1,
        "source_accessions": [
            "0000000123-26-000001",
            "0001652044-26-000002",
        ],
        "record_sha256": "d" * 64,
        "checked_at": checked_at,
    }
    if include_success:
        record["last_successful_check_at"] = last_successful_check_at
    return record


def numbered_cusip(index: int) -> str:
    first_eight = f"{index:08d}"
    return first_eight + str(master.calculate_cusip_check_digit(first_eight))


class CusipValidationTests(unittest.TestCase):
    def test_unknown_instrument_type_is_rejected_without_coercion(self) -> None:
        with self.assertRaisesRegex(
            master.SecurityMasterError,
            "unsupported security-master instrument type: CRYPTO",
        ):
            master.normalize_instrument_type("crypto")
        with self.assertRaises(master.SecurityMasterError):
            master.rebuild_security_master(
                source_state(),
                [{"cusip": "037833100", "instrument_type": "CRYPTO"}],
            )

    def test_check_digit_accepts_cusip_and_quarantines_bad_digit(self) -> None:
        self.assertEqual(0, master.calculate_cusip_check_digit("03783310"))
        self.assertTrue(master.is_valid_cusip("037833100"))
        self.assertEqual(
            "check_digit_mismatch",
            master.cusip_quarantine_reason("037833101"),
        )
        self.assertEqual(
            "check_digit_must_be_numeric",
            master.cusip_quarantine_reason("03783310A"),
        )

    def test_cins_uses_same_check_digit_algorithm(self) -> None:
        first_eight = "G1151C10"
        cins = first_eight + str(master.calculate_cusip_check_digit(first_eight))
        self.assertTrue(master.is_valid_cusip(cins))

    def test_all_zero_placeholder_is_quarantined_despite_valid_check_digit(
        self,
    ) -> None:
        self.assertEqual(0, master.calculate_cusip_check_digit("00000000"))
        self.assertEqual(
            "synthetic_or_placeholder_identifier",
            master.cusip_quarantine_reason("000000000"),
        )
        self.assertFalse(master.is_valid_cusip("000000000"))


class DiscoveryAndParserTests(unittest.TestCase):
    @staticmethod
    def archive_url_for_period(period: tuple) -> str:
        base = "https://www.sec.gov/files/data/fails-deliver-data/"
        if period[0] == "quarter":
            return f"{base}cnsp_sec_fails_{period[1]}q{period[2]}.zip"
        return f"{base}cnsfails{period[1]}{period[2]:02d}{period[3]}.zip"

    def test_ftd_discovery_ignores_non_sec_and_sorts_periods(self) -> None:
        html = f"""
        <a href="https://evil.example/cnsfails202609a.zip">external</a>
        <a href="{FTD_URL}">August</a>
        <a href="{LEGACY_FTD_URL}">legacy quarterly</a>
        <a href="/files/data/fails-deliver-data/cnsfails202607b.zip">July</a>
        <a href="/files/data/fails-deliver-data/not-an-archive.zip">other</a>
        """
        self.assertEqual(
            [LEGACY_FTD_URL, OLD_FTD_URL, FTD_URL],
            master.discover_ftd_urls(html),
        )
        self.assertEqual(
            [FTD_URL],
            master.select_recent_ftd_urls(
                [OLD_FTD_URL, FTD_URL],
                as_of=date(2026, 8, 20),
                lookback_months=1,
            ),
        )

    def test_ftd_discovery_accepts_numeric_cms_revision_suffix(self) -> None:
        revised_2019_url = (
            "https://www.sec.gov/files/data/fails-deliver-data/"
            "cnsfails201910a_0.zip"
        )
        html = f"""
        <a href="{REVISED_FTD_URL}">August 2023 second half</a>
        <a href="/files/data/fails-deliver-data/cnsfails201910a_0.zip">
          October 2019 first half
        </a>
        """

        self.assertEqual(
            [revised_2019_url, REVISED_FTD_URL],
            master.discover_ftd_urls(html),
        )
        self.assertEqual(
            ("half_month", 2023, 8, "b"),
            master._ftd_archive_period_key(REVISED_FTD_URL),
        )
        self.assertEqual(
            master._ftd_archive_period_key(REVISED_FTD_URL),
            master._ftd_archive_period_key(
                REVISED_FTD_URL.replace("_0.zip", ".zip")
            ),
        )

    def test_revised_ftd_archive_uses_period_for_dates_and_sorting(self) -> None:
        master._validate_ftd_archive_dates(
            [
                {"settlement_date": "2023-08-15"},
                {"settlement_date": "2023-08-31"},
            ],
            source_url=REVISED_FTD_URL,
        )
        with self.assertRaises(master.SourceSchemaError):
            master._validate_ftd_archive_dates(
                [{"settlement_date": "2023-08-14"}],
                source_url=REVISED_FTD_URL,
            )
        self.assertEqual(
            [REVISED_FTD_URL],
            master.select_recent_ftd_urls(
                [REVISED_FTD_URL],
                as_of=date(2023, 8, 31),
                lookback_months=1,
            ),
        )

        revised_quarterly_url = LEGACY_FTD_URL.replace(".zip", "_2.zip")
        master._validate_ftd_archive_dates(
            [
                {"settlement_date": "2009-04-01"},
                {"settlement_date": "2009-06-30"},
            ],
            source_url=revised_quarterly_url,
        )
        self.assertEqual(
            master._ftd_archive_period_key(LEGACY_FTD_URL),
            master._ftd_archive_period_key(revised_quarterly_url),
        )

    def test_ftd_discovery_rejects_two_urls_for_same_period(self) -> None:
        unsuffixed = REVISED_FTD_URL.replace("_0.zip", ".zip")
        with self.assertRaisesRegex(
            master.SourceSchemaError,
            "multiple URLs for archive period 2023-08b",
        ):
            master._validate_ftd_archive_discovery(
                [unsuffixed, REVISED_FTD_URL],
                as_of=date(2023, 10, 31),
                require_full_history=False,
            )

    def test_ftd_discovery_rejects_malformed_revision_suffix(self) -> None:
        malformed = REVISED_FTD_URL.replace("_0.zip", "_latest.zip")
        html = f'<a href="{malformed}">malformed revision</a>'
        self.assertEqual([], master.discover_ftd_urls(html))
        with self.assertRaisesRegex(
            master.SourceSchemaError,
            "no recognized SEC period",
        ):
            master._ftd_archive_date_bounds(malformed)

    def test_revised_ftd_url_is_valid_state_and_master_provenance(self) -> None:
        revised_url = FTD_URL.replace(".zip", "_0.zip")
        state = source_state(rows=[ftd_record("2026-08-01")])
        original_url = next(iter(state["sources"]))
        archive = state["sources"].pop(original_url)
        archive["url"] = revised_url
        state["sources"][revised_url] = archive

        master.source_state_sha256(state)
        rebuilt = master.rebuild_security_master(
            state,
            [{"cusip": "037833100", "instrument_type": "EQUITY"}],
        )
        master.validate_security_master(rebuilt)
        self.assertIn(
            revised_url,
            [source["url"] for source in rebuilt["sources"]],
        )

        duplicate_state = master._normalize_source_state(state)
        duplicate_url = revised_url.replace("_0.zip", ".zip")
        duplicate_source = copy.deepcopy(
            duplicate_state["sources"][revised_url]
        )
        duplicate_source["url"] = duplicate_url
        duplicate_state["sources"][duplicate_url] = duplicate_source
        with self.assertRaisesRegex(
            master.SourceSchemaError,
            "multiple URLs for archive period 2026-08a",
        ):
            master.source_state_sha256(duplicate_state)

        duplicate_master = copy.deepcopy(rebuilt)
        duplicate_provenance = copy.deepcopy(
            next(
                source
                for source in duplicate_master["sources"]
                if source["kind"] == "sec_ftd_archive"
            )
        )
        duplicate_provenance["url"] = duplicate_url
        duplicate_master["sources"].append(duplicate_provenance)
        duplicate_master["sources"].sort(
            key=lambda source: (
                source["url"],
                source["kind"],
                source["sha256"],
            )
        )
        with self.assertRaisesRegex(
            master.SourceSchemaError,
            "multiple URLs for archive period 2026-08a",
        ):
            master.validate_security_master(duplicate_master)

    def test_recent_ftd_selection_includes_overlapping_quarterly_bundle(
        self,
    ) -> None:
        self.assertEqual(
            [LEGACY_FTD_URL],
            master.select_recent_ftd_urls(
                [LEGACY_FTD_URL],
                as_of=date(2009, 6, 10),
                lookback_months=1,
            ),
        )

    def test_full_ftd_discovery_requires_early_and_middle_continuity(self) -> None:
        as_of = date(2009, 9, 15)
        expected_periods = master._expected_ftd_archive_periods(as_of=as_of)
        urls = [self.archive_url_for_period(period) for period in expected_periods]
        validated = master._validate_ftd_archive_discovery(
            urls,
            as_of=as_of,
            require_full_history=True,
        )
        self.assertEqual(len(expected_periods), len(validated))
        self.assertIn(("quarter", 2004, 1), expected_periods)

        for missing_period in (("quarter", 2004, 1), ("quarter", 2007, 3)):
            with self.subTest(missing_period=missing_period):
                incomplete = [
                    url
                    for url in urls
                    if master._ftd_archive_period_key(url) != missing_period
                ]
                with self.assertRaisesRegex(
                    master.SourceParseError,
                    master._ftd_period_label(missing_period),
                ):
                    master._validate_ftd_archive_discovery(
                        incomplete,
                        as_of=as_of,
                        require_full_history=True,
                    )

    def test_incremental_ftd_discovery_rejects_disappeared_period(self) -> None:
        with self.assertRaisesRegex(
            master.SourceParseError,
            "previously discovered.*2026-07b",
        ):
            master._validate_ftd_archive_discovery(
                [FTD_URL],
                as_of=date(2026, 8, 20),
                require_full_history=False,
                prior_urls=[OLD_FTD_URL, FTD_URL],
            )

    def test_ftd_pipe_and_zip_preserve_invalid_cusip_for_quarantine(self) -> None:
        rows = [
            ("20260801", "037833100", "AAPL", 100, "APPLE INC", "200.00"),
            ("20260804", "037833101", "AAPL", 50, "APPLE INC", "201.00"),
        ]
        parsed_pipe = master.parse_ftd_pipe(make_ftd_pipe(rows))
        parsed_zip = master.parse_ftd_zip(make_ftd_zip(rows))
        self.assertEqual(parsed_pipe, parsed_zip)
        self.assertEqual("2026-08-01", parsed_zip[0]["settlement_date"])
        self.assertEqual("037833101", parsed_zip[1]["cusip"])

    def test_ftd_quarterly_zip_combines_all_monthly_members(self) -> None:
        rows_by_member = [
            [("20090401", "037833100", "AAPL", 100, "APPLE INC", "100")],
            [("20090501", "037833100", "AAPL", 90, "APPLE INC", "110")],
            [("20090601", "037833100", "AAPL", 80, "APPLE INC", "120")],
        ]
        parsed = master.parse_ftd_zip(make_quarterly_ftd_zip(rows_by_member))
        self.assertEqual(
            ["2009-04-01", "2009-05-01", "2009-06-01"],
            [row["settlement_date"] for row in parsed],
        )

    def test_ftd_dates_must_match_the_archive_url_period(self) -> None:
        valid_cases = (
            (FTD_URL, ["2026-08-01", "2026-08-14"]),
            (OLD_FTD_URL, ["2026-07-15", "2026-07-31"]),
            (LEGACY_FTD_URL, ["2009-04-01", "2009-06-30"]),
        )
        for url, dates in valid_cases:
            with self.subTest(url=url, dates=dates):
                master._validate_ftd_archive_dates(
                    [{"settlement_date": value} for value in dates],
                    source_url=url,
                )

        invalid_cases = (
            (FTD_URL, "2026-08-15"),
            (OLD_FTD_URL, "2026-07-14"),
            (LEGACY_FTD_URL, "2009-07-01"),
            (FTD_2004_Q1_URL, "2004-02-29"),
            (FTD_2004_Q1_URL, "2004-03-21"),
            (FTD_2004_Q1_URL, "2004-04-01"),
        )
        for url, value in invalid_cases:
            with self.subTest(url=url, value=value):
                with self.assertRaises(master.SourceSchemaError):
                    master._validate_ftd_archive_dates(
                        [{"settlement_date": value}],
                        source_url=url,
                    )

    def test_july_2009_transition_uses_disjoint_14_15_cutover(self) -> None:
        base = (
            "https://www.sec.gov/files/data/"
            "frequently-requested-foia-document-fails-deliver-data/"
        )
        july_a = f"{base}cnsfails200907a.zip"
        july_b = f"{base}cnsfails200907b.zip"

        self.assertEqual(
            (date(2009, 7, 1), date(2009, 7, 14)),
            master._ftd_archive_date_bounds(july_a),
        )
        self.assertEqual(
            (date(2009, 7, 15), date(2009, 7, 31)),
            master._ftd_archive_date_bounds(july_b),
        )

        accepted_a = master._compact_ftd_payload(
            make_ftd_zip([
                ("20090714", "037833100", "AAPL", 110, "APPLE INC", "99"),
            ]),
            source_url=july_a,
            target_cusips={"037833100"},
            filter_universe_sha256=SHA_A,
        )
        accepted_b = master._compact_ftd_payload(
            make_ftd_zip([
                ("20090715", "037833100", "AAPL", 100, "APPLE INC", "100"),
                ("20090731", "037833100", "AAPL", 90, "APPLE INC", "101"),
            ]),
            source_url=july_b,
            target_cusips={"037833100"},
            filter_universe_sha256=SHA_A,
        )
        self.assertEqual("2009-07-15", accepted_b["first_settlement_date"])
        self.assertEqual(
            ["2009-07-15", "2009-07-31"],
            accepted_b["compact_records"][0]["observation_dates"],
        )

        timeline: dict[str, list[dict]] = {}
        for parsed, url, digest in (
            (accepted_a, july_a, SHA_A),
            (accepted_b, july_b, SHA_B),
        ):
            master._append_ftd_observations_to_timeline(
                timeline,
                master._ftd_observations_from_archive_records(
                    parsed["compact_records"],
                    source_url=url,
                    source_sha256=digest,
                ),
            )
        observations = timeline["037833100"][0]["observations"]
        self.assertEqual(
            ["2009-07-14", "2009-07-15", "2009-07-31"],
            [item["settlement_date"] for item in observations],
        )
        self.assertEqual(
            [[july_a], [july_b], [july_b]],
            [
                [source["url"] for source in item["sources"]]
                for item in observations
            ],
        )

        with self.assertRaisesRegex(
            master.SourceSchemaError,
            "outside archive period",
        ):
            master._compact_ftd_payload(
                make_ftd_zip([
                    (
                        "20090715",
                        "037833100",
                        "AAPL",
                        100,
                        "APPLE INC",
                        "100",
                    ),
                ]),
                source_url=july_a,
                target_cusips={"037833100"},
                filter_universe_sha256=SHA_A,
            )

    def test_quarterly_ftd_ingestion_requires_every_documented_month(self) -> None:
        q2_rows = [
            ("20090401", "037833100", "AAPL", 100, "APPLE INC", "100"),
            ("20090601", "037833100", "AAPL", 80, "APPLE INC", "120"),
        ]
        with self.assertRaisesRegex(
            master.SourceParseError,
            "missing settlement-month coverage: 2009-05",
        ):
            master._compact_ftd_payload(
                make_ftd_zip(q2_rows),
                source_url=LEGACY_FTD_URL,
                target_cusips={"037833100"},
                filter_universe_sha256="a" * 64,
            )

        accepted = master._compact_ftd_payload(
            make_ftd_zip([
                ("20040322", "037833100", "AAPL", 80, "APPLE INC", "120"),
                ("20040401", "037833100", "AAPL", 75, "APPLE INC", "121"),
            ]),
            source_url=FTD_2004_Q1_URL,
            target_cusips={"037833100"},
            filter_universe_sha256="a" * 64,
        )
        self.assertEqual(2, accepted["raw_record_count"])
        self.assertEqual(
            ["2004-03-22"],
            accepted["compact_records"][0]["observation_dates"],
        )

    def test_audited_2004_q1_spillover_is_excluded_and_q2_owned(self) -> None:
        boundary_rows = [
            ("20040401", "037833100", "AAPL", 100, "APPLE INC", "100.00"),
            (
                "20040401",
                "594918104",
                "MSFT",
                200,
                "MICROSOFT CORP",
                "200.00",
            ),
            (
                "20040401",
                "594918104",
                "MSFT",
                200,
                "MICROSOFT CORP",
                "200.00",
            ),
        ]
        filter_digest = master._filter_universe_sha256(["037833100"])
        q1 = master._compact_ftd_payload(
            make_ftd_zip([
                ("20040322", "037833100", "AAPL", 50, "APPLE INC", "99"),
                *boundary_rows,
            ]),
            source_url=FTD_2004_Q1_URL,
            target_cusips={"037833100"},
            filter_universe_sha256=filter_digest,
        )
        q2 = master._compact_ftd_payload(
            make_ftd_zip([
                *reversed(boundary_rows),
                ("20040503", "037833100", "AAPL", 75, "APPLE INC", "101"),
                ("20040601", "037833100", "AAPL", 80, "APPLE INC", "102"),
            ]),
            source_url=FTD_2004_Q2_URL,
            target_cusips={"037833100"},
            filter_universe_sha256=filter_digest,
        )

        self.assertEqual(date(2004, 3, 22), master.FTD_ARCHIVE_HISTORY_START)
        self.assertEqual(q1["boundary_date_proofs"], q2["boundary_date_proofs"])
        self.assertEqual(
            [{
                "date": "2004-04-01",
                "row_count": 3,
                "row_multiset_sha256": q1["boundary_date_proofs"][0][
                    "row_multiset_sha256"
                ],
            }],
            q1["boundary_date_proofs"],
        )
        self.assertRegex(
            q1["boundary_date_proofs"][0]["row_multiset_sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertEqual("2004-03-22", q1["first_settlement_date"])
        self.assertEqual("2004-04-01", q1["last_settlement_date"])
        self.assertEqual(["2004-03", "2004-04"], q1["observed_months"])
        self.assertEqual(
            ["2004-03-22"],
            q1["compact_records"][0]["observation_dates"],
        )
        self.assertEqual(
            ["2004-04-01", "2004-05-03", "2004-06-01"],
            q2["compact_records"][0]["observation_dates"],
        )
        self.assertEqual(
            4,
            q1["compact_records"][0]["row_count"]
            + q2["compact_records"][0]["row_count"],
        )

        different_filter = master._compact_ftd_payload(
            make_ftd_zip([
                ("20040322", "037833100", "AAPL", 50, "APPLE INC", "99"),
                *reversed(boundary_rows),
            ]),
            source_url=FTD_2004_Q1_URL,
            target_cusips={"594918104"},
            filter_universe_sha256=master._filter_universe_sha256(
                ["594918104"]
            ),
        )
        self.assertEqual(
            q1["boundary_date_proofs"],
            different_filter["boundary_date_proofs"],
        )

    def test_2004_q1_rejects_every_undeclared_out_of_period_date(self) -> None:
        for settlement_date in ("20040229", "20040321", "20040402"):
            with self.subTest(settlement_date=settlement_date):
                with self.assertRaises(master.SourceSchemaError):
                    master._compact_ftd_payload(
                        make_ftd_zip([
                            (
                                settlement_date,
                                "037833100",
                                "AAPL",
                                100,
                                "APPLE INC",
                                "100",
                            ),
                        ]),
                        source_url=FTD_2004_Q1_URL,
                        target_cusips={"037833100"},
                        filter_universe_sha256=SHA_A,
                    )

    def test_operational_ftd_compaction_streams_without_raw_row_list(
        self,
    ) -> None:
        rows = [
            (
                "20260801",
                "594918104",
                "MSFT",
                index + 1,
                "MICROSOFT CORP",
                "500",
            )
            for index in range(5_000)
        ]
        rows.extend([
            ("20260801", "037833100", "AAPL", 100, "APPLE INC", "200"),
            ("20260804", "037833100", "AAPL", 200, "APPLE INC", "202"),
        ])
        with mock.patch.object(
            master,
            "parse_ftd_zip",
            side_effect=AssertionError("must not materialize raw rows"),
        ):
            accepted = master._compact_ftd_payload(
                make_ftd_zip(rows),
                source_url=FTD_URL,
                target_cusips={"037833100"},
                filter_universe_sha256=SHA_A,
            )

        self.assertEqual(5_002, accepted["raw_record_count"])
        self.assertEqual([], accepted["boundary_date_proofs"])
        self.assertEqual(1, len(accepted["compact_records"]))
        self.assertEqual(
            ["2026-08-01", "2026-08-04"],
            accepted["compact_records"][0]["observation_dates"],
        )

    def test_persisted_ftd_dates_are_bound_to_the_archive_url(self) -> None:
        invalid = source_state(rows=[ftd_record("2026-08-16")])
        generated_url = next(iter(invalid["sources"]))
        archive = invalid["sources"].pop(generated_url)
        archive["url"] = FTD_URL
        invalid["sources"][FTD_URL] = archive

        with self.assertRaises(master.SecurityMasterError):
            master.source_state_sha256(invalid)

    def test_ftd_parser_rejects_wrong_header_and_bad_zip(self) -> None:
        with self.assertRaises(master.SourceSchemaError):
            master.parse_ftd_pipe(b"DATE|ID|TICKER\n20260801|X|Y\n")
        with self.assertRaises(master.SourceParseError) as raised:
            master.parse_ftd_zip(b"not a zip")
        self.assertNotIsInstance(raised.exception, master.SourceSchemaError)

    def test_symbol_parser_distinguishes_schema_from_invalid_json(self) -> None:
        with self.assertRaises(master.SourceParseError) as invalid_json:
            master.parse_sec_company_exchange_symbols(b"{")
        self.assertNotIsInstance(
            invalid_json.exception,
            master.SourceSchemaError,
        )
        with self.assertRaises(master.SourceSchemaError):
            master.parse_sec_company_exchange_symbols({
                "fields": ["cik", "name", "securitySymbol", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
            })

    def test_discovers_latest_official_list_txt(self) -> None:
        html = """
        <a href="/files/investment/13flist2025q4.txt">old</a>
        <a href="https://www.sec.gov/files/investment/13flist2026q1-txt.txt">q1</a>
        <a href="/files/investment/13flist2026q2-txt.txt">current</a>
        <a href="https://evil.example/13flist2099q4-txt.txt">external</a>
        """
        self.assertEqual(LIST_URL, master.discover_latest_13f_list_url(html))

    def test_parses_official_list_fixed_width_fields(self) -> None:
        payload = "\n".join([
            make_13f_line("037833100", "APPLE INC", "COM", status="*A*"),
            make_13f_line(
                "594918104", "MICROSOFT CORP", "CALL", option="*", status="*D*"
            ),
        ])
        records = master.parse_official_13f_list(payload)
        self.assertEqual(
            {
                "cusip": "037833100",
                "option_indicator": "",
                "issuer": "APPLE INC",
                "description": "COM",
                "status": "*A*",
            },
            records[0],
        )
        self.assertEqual("*", records[1]["option_indicator"])
        self.assertEqual("*D*", records[1]["status"])

    def test_official_list_deduplicates_only_complete_normalized_rows(self) -> None:
        apple = make_13f_line("037833100", "APPLE INC", "COM")
        whitespace_equivalent = make_13f_line(
            "037833100",
            "APPLE  INC",
            "COM",
        )
        distinct_status = make_13f_line(
            "037833100",
            "APPLE INC",
            "COM",
            status="*D*",
        )
        distinct_class = make_13f_line(
            "037833100",
            "APPLE INC",
            "CALL",
        )
        distinct_option = make_13f_line(
            "037833100",
            "APPLE INC",
            "CALL",
            option="*",
        )
        payload = "\n".join([
            distinct_option,
            apple,
            whitespace_equivalent,
            distinct_status,
            apple,
            distinct_class,
        ])

        records = master.parse_official_13f_list(payload)

        self.assertEqual(4, len(records))
        self.assertEqual(
            records,
            master.parse_official_13f_list(
                "\n".join(reversed(payload.splitlines()))
            ),
        )
        self.assertEqual(
            {
                ("COM", "", ""),
                ("COM", "*D*", ""),
                ("CALL", "", ""),
                ("CALL", "", "*"),
            },
            {
                (
                    record["description"],
                    record["status"],
                    record["option_indicator"],
                )
                for record in records
            },
        )

    def test_accepted_official_list_counts_unique_logical_records(self) -> None:
        row = make_13f_line("037833100", "APPLE INC", "COM")
        parsed = master.parse_official_13f_list(f"{row}\n{row}\n")

        accepted = master._accepted_source_entry(
            url=LIST_URL,
            kind="sec_13f_list",
            sha256="c" * 64,
            accepted_at="2026-08-20T12:00:00Z",
            parsed=parsed,
        )

        self.assertEqual(1, accepted["record_count"])
        self.assertEqual(1, len(accepted["records"]))
        state = source_state()
        state["sources"][LIST_URL] = accepted
        master.source_state_sha256(state)

    def test_official_list_rejects_fixed_width_schema_drift(self) -> None:
        row = make_13f_line("037833100", "APPLE INC", "COM")
        malformed_rows = (
            row[:69],
            f"{row[:70]}X{row[71:]}",
            f"{row}X",
            f"{'':9}{row[9:]}",
        )

        for malformed in malformed_rows:
            with self.subTest(malformed=malformed):
                with self.assertRaises(master.SourceSchemaError):
                    master.parse_official_13f_list(malformed)

    def test_current_sec_symbol_parsers_cover_all_payload_shapes(self) -> None:
        company = {"0": {"ticker": "AAPL", "title": "Apple Inc."}}
        exchange = {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
        }
        funds = {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [[1, "S1", "C1", "SPY"]],
        }
        self.assertEqual(["AAPL"], master.parse_sec_company_symbols(company))
        self.assertEqual(
            ["AAPL"], master.parse_sec_company_exchange_symbols(exchange)
        )
        self.assertEqual(["SPY"], master.parse_sec_fund_symbols(funds))

    def test_non_sec_urls_are_rejected_before_fetch(self) -> None:
        with self.assertRaises(master.NonSECURL):
            master.normalize_sec_url("https://example.com/file.zip")
        with self.assertRaises(master.NonSECURL):
            master.normalize_sec_url("http://www.sec.gov/file.zip")


class SourceStateCompactionTests(unittest.TestCase):
    def test_ftd_checkpoint_order_ignores_first_seen_insertion_order(self) -> None:
        state = source_state()
        state["ftd_timeline"] = {
            "594918104": [{"marker": "first observed"}],
            "037833100": [{"marker": "observed later"}],
        }
        state["ftd_mutable_tail"] = {
            FTD_URL: {"marker": "newer"},
            OLD_FTD_URL: {"marker": "older"},
        }

        master._canonicalize_ftd_checkpoint_order(state)
        first = json.dumps(state, sort_keys=False)
        master._canonicalize_ftd_checkpoint_order(state)

        self.assertEqual(
            ["037833100", "594918104"],
            list(state["ftd_timeline"]),
        )
        self.assertEqual(
            [OLD_FTD_URL, FTD_URL],
            list(state["ftd_mutable_tail"]),
        )
        self.assertEqual(first, json.dumps(state, sort_keys=False))

    def test_source_state_rejects_request_metadata_before_persisting(self) -> None:
        for field, value in (
            ("user_agent", "private contact"),
            ("request_headers", {"User-Agent": "private contact"}),
            ("response_headers", {"Set-Cookie": "private"}),
            ("authorization", "private"),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "state.json"
                state = source_state()
                state[field] = value

                with self.assertRaisesRegex(
                    master.SecurityMasterError,
                    "forbidden request metadata",
                ):
                    master.save_source_state(state, path)

                self.assertFalse(path.exists())

        nested_states = []
        profile_state = source_state()
        profile_digest = master._filter_universe_sha256(["037833100"])
        profile_state["filter_universes"][profile_digest] = {
            "cusips": ["037833100"],
            "count": 1,
            "user_agent": "private contact",
        }
        nested_states.append(profile_state)
        discovery_state = source_state()
        discovery_state["edgar_discovery"] = {
            "request_headers": {"User-Agent": "private contact"},
        }
        nested_states.append(discovery_state)

        for index, state in enumerate(nested_states):
            with (
                self.subTest(nested=index),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                path = Path(tmpdir) / "state.json"
                with self.assertRaisesRegex(
                    master.SecurityMasterError,
                    "forbidden request metadata",
                ):
                    master.save_source_state(state, path)
                self.assertFalse(path.exists())

        for field, value in (
            ("user_agent", "private contact"),
            ("request_headers", {"User-Agent": "private contact"}),
            ("response_headers", {"Set-Cookie": "private"}),
            ("authorization", "private"),
        ):
            with (
                self.subTest(field=f"source.{field}"),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                path = Path(tmpdir) / "state.json"
                state = source_state(symbols=["AAPL"])
                state["sources"][master.SEC_COMPANY_TICKERS_URL][field] = value

                with self.assertRaisesRegex(
                    master.SecurityMasterError,
                    "forbidden request metadata",
                ):
                    master.save_source_state(state, path)

                self.assertFalse(path.exists())

    def test_source_state_rejects_sensitive_key_name_variants(self) -> None:
        variants = (
            "SEC_USER_AGENT",
            "UserAgent",
            "user-agent",
            "x-api-key",
            "access_token",
            "client-secret",
            "proxyAuthorization",
            "requestHeaders",
            "response-metadata",
        )
        for field in variants:
            with (
                self.subTest(field=field),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                path = Path(tmpdir) / "state.json"
                state = source_state()
                state["edgar_discovery"] = {
                    "legacy_diagnostics": {field: "private"}
                }

                with self.assertRaisesRegex(
                    master.SecurityMasterError,
                    "forbidden request metadata",
                ):
                    master.save_source_state(state, path)

                self.assertFalse(path.exists())

    def test_source_state_sensitive_key_check_avoids_value_scanning(self) -> None:
        state = source_state()
        state["edgar_discovery"] = {
            "token_count": 3,
            "secretary_name": "UserAgent access_token",
            "password_policy_version": 2,
            "user_agent_verified": False,
            "column_headers": ["authorization", "cookie"],
        }

        self.assertRegex(master.source_state_sha256(state), r"^[0-9a-f]{64}$")

    def test_optional_source_fields_reject_explicit_null(self) -> None:
        cases = []
        current = source_state(symbols=["AAPL"])
        current["sources"][master.SEC_COMPANY_TICKERS_URL][
            "last_successful_check_at"
        ] = None
        cases.append(current)
        funds = source_state(symbols=["AAPL"])
        fund_source = funds["sources"].pop(master.SEC_COMPANY_TICKERS_URL)
        fund_source["url"] = master.SEC_FUND_TICKERS_URL
        fund_source["kind"] = "sec_fund_tickers"
        fund_source["fund_records"] = None
        funds["sources"][master.SEC_FUND_TICKERS_URL] = fund_source
        cases.append(funds)

        for index, state in enumerate(cases):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "state.json"
                with self.assertRaises(master.SecurityMasterError):
                    master.save_source_state(state, path)
                self.assertFalse(path.exists())

    def test_2004_boundary_proof_partial_checkpoint_round_trips(self) -> None:
        boundary_rows = [
            ("20040401", "037833100", "AAPL", 100, "APPLE INC", "100"),
            (
                "20040401",
                "594918104",
                "MSFT",
                200,
                "MICROSOFT CORP",
                "200",
            ),
        ]
        q1, q2 = compact_2004_boundary_pair(boundary_rows)
        partial = source_state_with_2004_boundary(q1)

        master._validate_ftd_boundary_duplicate_proofs(
            partial,
            require_complete=False,
        )
        with self.assertRaises(master.SecurityMasterError):
            master._validate_ftd_boundary_duplicate_proofs(
                partial,
                require_complete=True,
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "state.json"
            master.save_source_state(partial, path)
            restored = master.load_source_state(path)
        self.assertEqual(
            q1["boundary_date_proofs"],
            restored["sources"][FTD_2004_Q1_URL]["boundary_date_proofs"],
        )

        complete = source_state_with_2004_boundary(q1, q2)
        master._validate_ftd_boundary_duplicate_proofs(
            complete,
            require_complete=True,
        )
        self.assertRegex(master.source_state_sha256(complete), r"^[0-9a-f]{64}$")

    def test_2004_boundary_proof_binds_full_row_multiset(self) -> None:
        boundary_rows = [
            ("20040401", "037833100", "AAPL", 100, "APPLE INC", "100"),
            (
                "20040401",
                "594918104",
                "MSFT",
                200,
                "MICROSOFT CORP",
                "200",
            ),
            (
                "20040401",
                "594918104",
                "MSFT",
                200,
                "MICROSOFT CORP",
                "200",
            ),
        ]
        q1, _ = compact_2004_boundary_pair(boundary_rows)
        variants = {
            "multiplicity": boundary_rows[:-1],
            "quantity": [
                boundary_rows[0],
                (
                    "20040401",
                    "594918104",
                    "MSFT",
                    201,
                    "MICROSOFT CORP",
                    "200",
                ),
                boundary_rows[2],
            ],
            "description": [
                boundary_rows[0],
                (
                    "20040401",
                    "594918104",
                    "MSFT",
                    200,
                    "MICROSOFT CORPORATION",
                    "200",
                ),
                boundary_rows[2],
            ],
            "price": [
                boundary_rows[0],
                (
                    "20040401",
                    "594918104",
                    "MSFT",
                    200,
                    "MICROSOFT CORP",
                    "201",
                ),
                boundary_rows[2],
            ],
        }
        for label, rows in variants.items():
            with self.subTest(label=label):
                _, mismatched_q2 = compact_2004_boundary_pair(rows)
                state = source_state_with_2004_boundary(q1, mismatched_q2)
                with self.assertRaises(master.SecurityMasterError):
                    master._validate_ftd_boundary_duplicate_proofs(
                        state,
                        require_complete=True,
                    )

    def test_2004_boundary_proof_schema_is_fail_closed(self) -> None:
        rows = [
            ("20040401", "037833100", "AAPL", 100, "APPLE INC", "100")
        ]
        q1, q2 = compact_2004_boundary_pair(rows)
        complete = source_state_with_2004_boundary(q1, q2)
        malformed_values = (
            ("date", "2004-04-02"),
            ("row_count", 0),
            ("row_multiset_sha256", "not-a-sha256"),
        )
        for field, value in malformed_values:
            with self.subTest(field=field):
                malformed = copy.deepcopy(complete)
                malformed["sources"][FTD_2004_Q1_URL][
                    "boundary_date_proofs"
                ][0][field] = value
                with self.assertRaises(master.SecurityMasterError):
                    master.source_state_sha256(malformed)

        missing = copy.deepcopy(complete)
        missing["sources"][FTD_2004_Q2_URL]["boundary_date_proofs"] = []
        with self.assertRaises(master.SecurityMasterError):
            master.source_state_sha256(missing)

    def test_2004_q1_cannot_own_april_boundary_timeline_evidence(self) -> None:
        rows = [
            ("20040401", "037833100", "AAPL", 100, "APPLE INC", "100")
        ]
        q1, q2 = compact_2004_boundary_pair(rows)
        complete = source_state_with_2004_boundary(q1, q2)
        observations = master._ftd_observations_from_archive_records(
            q2["compact_records"],
            source_url=FTD_2004_Q2_URL,
            source_sha256=SHA_B,
        )
        master._append_ftd_observations_to_timeline(
            complete["ftd_timeline"],
            observations,
        )
        self.assertRegex(master.source_state_sha256(complete), r"^[0-9a-f]{64}$")

        tampered = copy.deepcopy(complete)
        interval = tampered["ftd_timeline"]["037833100"][0]
        april_witness = next(
            witness
            for witness in interval["observations"]
            if witness["settlement_date"] == "2004-04-01"
        )
        april_witness["sources"] = [{
            "url": FTD_2004_Q1_URL,
            "sha256": SHA_A,
        }]
        master._refresh_ftd_interval_projection(interval)

        with self.assertRaises(master.SecurityMasterError):
            master.source_state_sha256(tampered)

    def test_boundary_inventory_upgrade_rejects_changed_timeline_witness(
        self,
    ) -> None:
        boundary_rows = [
            ("20040401", "037833100", "AAPL", 100, "APPLE INC", "100")
        ]
        q1, q2 = compact_2004_boundary_pair(boundary_rows)
        state = source_state_with_2004_boundary(q1, q2)
        observations = master._ftd_observations_from_archive_records(
            q1["compact_records"],
            source_url=FTD_2004_Q1_URL,
            source_sha256=SHA_A,
        )
        master._append_ftd_observations_to_timeline(
            state["ftd_timeline"],
            observations,
        )
        state["ftd_timeline"]["037833100"][0]["observations"][0][
            "observation_count"
        ] += 1

        with self.assertRaisesRegex(
            master.SourceSchemaError,
            "does not reproduce its retained timeline evidence",
        ):
            master._upgrade_ftd_boundary_inventory(
                state,
                prior=state["sources"][FTD_2004_Q1_URL],
                parsed=q1,
                source_url=FTD_2004_Q1_URL,
                source_sha256=SHA_A,
                accepted_at="2026-08-21T12:00:00Z",
            )

    def test_required_ftd_urls_use_archive_chronology_across_2009_layouts(
        self,
    ) -> None:
        july_2009 = (
            "https://www.sec.gov/files/data/fails-deliver-data/"
            "cnsfails200907a.zip"
        )
        state = source_state()
        state["required_filter_coverage_urls"] = [LEGACY_FTD_URL, july_2009]

        self.assertNotEqual(
            state["required_filter_coverage_urls"],
            sorted(state["required_filter_coverage_urls"]),
        )
        self.assertRegex(master.source_state_sha256(state), r"^[0-9a-f]{64}$")

    def test_timeline_bounds_dates_sources_and_changing_descriptions(self) -> None:
        url = FTD_URL
        observations = []
        for index in range(200):
            settlement_date = date(2020, 1, 1) + timedelta(days=index)
            observations.append({
                "settlement_date": settlement_date.isoformat(),
                "symbol": "AAPL",
                "observation_count": 1,
                "descriptions": [f"APPLE DESCRIPTION {index:03d}"],
                "sources": [{"url": url, "sha256": SHA_A}],
            })

        timeline: dict[str, list[dict]] = {}
        master._append_ftd_observations_to_timeline(
            timeline,
            {"037833100": observations},
        )

        interval = timeline["037833100"][0]
        self.assertEqual(200, interval["observation_date_count"])
        self.assertLessEqual(
            len(interval["observation_dates"]),
            master.FTD_MAX_RECENT_EXACT_DATES + 1,
        )
        self.assertLessEqual(
            len(interval["descriptions"]),
            master.FTD_MAX_RECENT_EXACT_DATES + 1,
        )
        self.assertEqual(1, len(interval["sources"]))
        self.assertLess(len(json.dumps(timeline)), 50_000)

    def test_many_same_symbol_archives_remain_bounded_per_cusip(self) -> None:
        def build(day_count: int) -> dict[str, list[dict]]:
            timeline: dict[str, list[dict]] = {}
            cusips = [numbered_cusip(index) for index in range(1, 21)]
            for index in range(day_count):
                settlement_date = (
                    date(2020, 1, 1) + timedelta(days=index)
                ).isoformat()
                master._append_ftd_observations_to_timeline(
                    timeline,
                    {
                        cusip: [{
                            "settlement_date": settlement_date,
                            "symbol": f"S{offset:02d}",
                            "observation_count": 1,
                            "descriptions": [f"ISSUER {offset:02d}"],
                            "sources": [{"url": FTD_URL, "sha256": SHA_A}],
                        }]
                        for offset, cusip in enumerate(cusips)
                    },
                )
            return timeline

        short = build(40)
        long = build(200)
        self.assertLess(len(json.dumps(long)), len(json.dumps(short)) * 2)
        for intervals in long.values():
            self.assertEqual(1, len(intervals))
            self.assertEqual(200, intervals[0]["observation_date_count"])
            self.assertLessEqual(
                len(intervals[0]["observation_dates"]),
                master.FTD_MAX_RECENT_EXACT_DATES + 1,
            )

    def test_symbol_set_changes_keep_reuse_and_same_date_conflict_explicit(
        self,
    ) -> None:
        observations = [
            ("2026-08-01", "AAPL"),
            ("2026-08-02", "AAPX"),
            ("2026-08-02", "AAPL"),
            ("2026-08-03", "AAPL"),
        ]
        timeline: dict[str, list[dict]] = {}
        master._append_ftd_observations_to_timeline(
            timeline,
            {
                "037833100": [{
                    "settlement_date": observed_at,
                    "symbol": symbol,
                    "observation_count": 1,
                    "descriptions": ["APPLE INC"],
                    "sources": [{"url": FTD_URL, "sha256": SHA_A}],
                } for observed_at, symbol in observations]
            },
        )

        self.assertEqual(
            [["AAPL"], ["AAPL", "AAPX"], ["AAPL"]],
            [interval["symbols"] for interval in timeline["037833100"]],
        )
        self.assertIsNone(timeline["037833100"][1]["symbol"])

    def test_duplicate_rows_collapse_to_small_deterministic_date_proof(self) -> None:
        apple_rows = [ftd_record("2026-08-01") for _ in range(2_000)]
        apple_rows.extend(ftd_record("2026-08-04") for _ in range(500))
        other_rows = [
            ftd_record(
                "2026-08-01",
                symbol="MSFT",
                cusip="594918104",
                description="MICROSOFT CORP",
            )
            for _ in range(500)
        ]
        raw_rows = apple_rows + other_rows

        first = master.compact_ftd_records(raw_rows, {"037833100"})
        second = master.compact_ftd_records(
            reversed(raw_rows),
            {"037833100"},
        )

        self.assertEqual(first, second)
        self.assertEqual(1, len(first))
        self.assertEqual(2_500, first[0]["row_count"])
        self.assertEqual(
            ["2026-08-01", "2026-08-04"],
            first[0]["observation_dates"],
        )
        self.assertEqual(2, first[0]["distinct_settlement_date_count"])
        self.assertNotIn("quantity", first[0])
        compact_bytes = json.dumps(
            first,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        raw_bytes = json.dumps(raw_rows, sort_keys=True).encode()
        self.assertLess(len(compact_bytes), len(raw_bytes) // 100)

    def test_v1_raw_state_read_migrates_and_writes_only_v2(self) -> None:
        legacy = {
            "schema_version": master.LEGACY_SOURCE_STATE_SCHEMA_VERSION,
            "updated_at": "2026-08-20T12:00:00Z",
            "sources": {
                FTD_URL: {
                    "url": FTD_URL,
                    "kind": "sec_ftd_archive",
                    "sha256": SHA_A,
                    "accepted_at": "2026-08-20T12:00:00Z",
                    "records": [
                        ftd_record("2026-08-01"),
                        ftd_record("2026-08-01"),
                        ftd_record("2026-08-04"),
                    ],
                    "record_count": 3,
                }
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "state.json"
            path.write_text(json.dumps(legacy), encoding="utf-8")
            migrated = master.load_source_state(path)
            self.assertEqual(master.SOURCE_STATE_SCHEMA_VERSION, migrated["schema_version"])
            archive = migrated["sources"][FTD_URL]
            self.assertTrue(archive["filter_all_cusips"])
            self.assertEqual(1, archive["record_count"])
            self.assertEqual(3, archive["raw_record_count"])
            self.assertEqual({}, migrated["edgar_evidence"])
            self.assertEqual({}, migrated["edgar_discovery"])

            master.save_source_state(legacy, path)
            persisted = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(master.SOURCE_STATE_SCHEMA_VERSION, persisted["schema_version"])
            self.assertEqual(1, persisted["sources"][FTD_URL]["record_count"])

    def test_malformed_v2_compact_state_is_rejected_before_migration(
        self,
    ) -> None:
        legacy = source_state(rows=[ftd_record("2026-08-01")])
        legacy["schema_version"] = master.COMPACT_SOURCE_STATE_SCHEMA_VERSION
        legacy["sources"][FTD_URL]["records"][0][
            "distinct_settlement_date_count"
        ] = 99
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "state.json"
            path.write_text(json.dumps(legacy), encoding="utf-8")
            with self.assertRaisesRegex(
                master.SecurityMasterError,
                "invalid compact date proof",
            ):
                master.load_source_state(path)

    def test_legacy_edgar_discovery_round_trips_without_digest_change(self) -> None:
        state = source_state()
        state["edgar_discovery"] = {
            "schema_version": master.LEGACY_EDGAR_DISCOVERY_SCHEMA_VERSION,
            "records": {"02079K305": edgar_discovery_record()},
            "fetched_sources": {},
        }
        expected_digest = master.source_state_sha256(state)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "state.json"
            master.save_source_state(state, path)
            loaded = master.load_source_state(path)

        self.assertEqual(state["edgar_discovery"], loaded["edgar_discovery"])
        self.assertEqual(expected_digest, master.source_state_sha256(loaded))
        self.assertEqual(
            "2026-08-20T12:00:00Z",
            master._edgar_successful_checkpoints_by_cusip(loaded)[
                "02079K305"
            ],
        )

    def test_current_edgar_discovery_requires_success_clock(self) -> None:
        valid = source_state()
        valid["edgar_discovery"] = {
            "schema_version": master.EDGAR_DISCOVERY_SCHEMA_VERSION,
            "records": {
                "02079K305": edgar_discovery_record(include_success=True)
            },
            "fetched_sources": {},
        }
        master.source_state_sha256(valid)

        missing_clock = copy.deepcopy(valid)
        del missing_clock["edgar_discovery"]["records"]["02079K305"][
            "last_successful_check_at"
        ]
        bad_clock = copy.deepcopy(valid)
        bad_clock["edgar_discovery"]["records"]["02079K305"][
            "last_successful_check_at"
        ] = "not-a-timestamp"
        unknown_version = copy.deepcopy(valid)
        unknown_version["edgar_discovery"]["schema_version"] = 99

        for invalid in (missing_clock, bad_clock, unknown_version):
            with self.subTest(invalid=invalid):
                with self.assertRaises(master.SecurityMasterError):
                    master.source_state_sha256(invalid)

    def test_official_list_source_state_requires_canonical_records(self) -> None:
        valid = source_state(official_rows=[official_record()])
        master.source_state_sha256(valid)

        mutations = []
        wrong_count = copy.deepcopy(valid)
        wrong_count["sources"][LIST_URL]["record_count"] = 2
        mutations.append(wrong_count)
        wrong_period = copy.deepcopy(valid)
        wrong_period["sources"][LIST_URL]["list_period"] = "2026Q1"
        mutations.append(wrong_period)
        extra_field = copy.deepcopy(valid)
        extra_field["sources"][LIST_URL]["records"][0]["vendor_id"] = "x"
        mutations.append(extra_field)
        duplicate = copy.deepcopy(valid)
        duplicate["sources"][LIST_URL]["records"] *= 2
        duplicate["sources"][LIST_URL]["record_count"] = 2
        mutations.append(duplicate)

        for state in mutations:
            with self.subTest(state=state):
                with self.assertRaises(master.SecurityMasterError):
                    master.source_state_sha256(state)

    def test_validation_source_state_requires_canonical_symbol_metadata(self) -> None:
        valid = source_state(
            symbols=["AAPL", "MSFT"],
            symbol_titles={
                "AAPL": ["Apple Inc."],
                "MSFT": ["Microsoft Corporation"],
            },
            symbol_exchanges={"AAPL": ["Nasdaq"], "MSFT": ["Nasdaq"]},
        )
        master.source_state_sha256(valid)

        mutations = []
        unordered = copy.deepcopy(valid)
        unordered["sources"][master.SEC_COMPANY_TICKERS_URL]["symbols"].reverse()
        mutations.append(unordered)
        wrong_count = copy.deepcopy(valid)
        wrong_count["sources"][master.SEC_COMPANY_TICKERS_URL]["symbol_count"] = 1
        mutations.append(wrong_count)
        unknown_title = copy.deepcopy(valid)
        unknown_title["sources"][master.SEC_COMPANY_TICKERS_URL][
            "symbol_titles"
        ]["GOOG"] = ["Alphabet Inc."]
        mutations.append(unknown_title)
        duplicate_exchange = copy.deepcopy(valid)
        duplicate_exchange["sources"][master.SEC_COMPANY_TICKERS_URL][
            "symbol_exchanges"
        ]["AAPL"] = ["Nasdaq", "Nasdaq"]
        mutations.append(duplicate_exchange)

        for state in mutations:
            with self.subTest(state=state):
                with self.assertRaises(master.SecurityMasterError):
                    master.source_state_sha256(state)


class RebuildAndResolutionTests(unittest.TestCase):
    def test_blank_reported_identity_is_preserved_with_exact_evidence(self) -> None:
        cusip = "M46528101"
        evidence = {
            "reported_cusip": cusip,
            "reported_issuer": "",
            "reported_class": "",
            "accession": "0001643792-26-000009",
            "report_date": "2026-06-30",
            "url": "https://www.sec.gov/Archives/edgar/data/1643792/"
            "000164379226000009/informationtable.xml",
            "sha256": "e" * 64,
        }
        built = master.rebuild_security_master(
            source_state(),
            [{
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "issuer": "DISPLAY ISSUER MUST NOT BECOME REPORTED DATA",
                "security_class": "DISPLAY CLASS MUST NOT BECOME REPORTED DATA",
                "reported_issuer": "",
                "reported_class": "",
                "reported_identity_evidence": [evidence],
            }],
        )

        record = built["records"][f"{cusip}|EQUITY"]
        self.assertEqual(
            [{
                "reported_cusip": cusip,
                "reported_issuer": "",
                "reported_class": "",
            }],
            record["reported_identities"],
        )
        self.assertEqual([evidence], record["reported_identity_evidence"])
        self.assertIsNone(record["issuer"])
        self.assertIsNone(record["security_class"])
        self.assertTrue(master.audit_security_master(
            built,
            enforce_reported_identity_evidence=True,
        )["reported_identity_evidence_gate_passed"])

        substituted = copy.deepcopy(built)
        substituted["records"][f"{cusip}|EQUITY"]["reported_identities"][0][
            "reported_issuer"
        ] = "DISPLAY ISSUER MUST NOT BECOME REPORTED DATA"
        with self.assertRaises(master.SecurityMasterError):
            master.validate_security_master(substituted)

    def test_reported_identity_keeps_checksummed_sec_source_reference(self) -> None:
        evidence = {
            "reported_cusip": "037833100",
            "reported_issuer": "APPLE INC",
            "reported_class": "COM",
            "accession": "0001067983-26-000001",
            "report_date": "2026-06-30",
            "url": "https://www.sec.gov/files/structureddata/data/"
            "form-13f-data-sets/2026q2_form13f.zip",
            "sha256": "d" * 64,
        }
        built = master.rebuild_security_master(
            source_state(),
            [{
                "cusip": "037833100",
                "instrument_type": "EQUITY",
                "issuer": "APPLE INC",
                "security_class": "COM",
                "reported_identity_evidence": [evidence, evidence],
            }],
        )
        record = built["records"]["037833100|EQUITY"]
        self.assertEqual([evidence], record["reported_identity_evidence"])
        audit = master.audit_security_master(
            built,
            enforce_reported_identity_evidence=True,
        )
        self.assertTrue(audit["reported_identity_evidence_gate_passed"])

        tampered = copy.deepcopy(built)
        tampered["records"]["037833100|EQUITY"][
            "reported_identity_evidence"
        ][0]["sha256"] = "not-a-hash"
        with self.assertRaises(master.SecurityMasterError):
            master.validate_security_master(tampered)

        wrong_accession_url = copy.deepcopy(evidence)
        wrong_accession_url["url"] = (
            "https://www.sec.gov/Archives/edgar/data/1067983/"
            "000106798326999999/informationtable.xml"
        )
        with self.assertRaises(master.SecurityMasterError):
            master.rebuild_security_master(
                source_state(),
                [{
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "issuer": "APPLE INC",
                    "security_class": "COM",
                    "reported_identity_evidence": [wrong_accession_url],
                }],
            )

        missing = master.rebuild_security_master(
            source_state(),
            [{
                "cusip": "037833100",
                "instrument_type": "EQUITY",
                "issuer": "APPLE INC",
                "security_class": "COM",
            }],
        )
        missing_audit = master.audit_security_master(
            missing,
            enforce_reported_identity_evidence=True,
        )
        self.assertFalse(missing_audit["reported_identity_evidence_gate_passed"])
        self.assertIn(
            "reported_identity_evidence_incomplete",
            missing_audit["issues"],
        )

    def test_preferred_slash_symbol_uses_shared_publishable_grammar(self) -> None:
        state = source_state(
            rows=[
                ftd_record(
                    "2026-08-01",
                    "BAC/PL",
                    description="BANK OF AMERICA CORP",
                ),
                ftd_record(
                    "2026-08-04",
                    "BAC/PL",
                    description="BANK OF AMERICA CORP",
                ),
            ],
            symbols=["BAC/PL"],
            symbol_titles={"BAC/PL": ["Bank of America Corporation"]},
            official_rows=[
                official_record(
                    issuer="BANK OF AMERICA CORP",
                    description="PREFERRED STOCK",
                )
            ],
        )
        result = master.rebuild_security_master(
            state,
            [{
                "cusip": "037833100",
                "instrument_type": "PREF",
                "reported_issuer": "BANK OF AMERICA CORP",
                "reported_class": "PREFERRED STOCK",
            }],
        )
        record = result["records"]["037833100|PREF"]
        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("BAC/PL", record["ticker"])

    def apple_state(self, **overrides) -> dict:
        values = {
            "rows": [ftd_record("2026-08-01"), ftd_record("2026-08-04")],
            "symbols": ["AAPL"],
            "symbol_titles": {"AAPL": ["Apple Inc."]},
            "symbol_exchanges": {"AAPL": ["Nasdaq"]},
            "official_rows": [official_record()],
        }
        values.update(overrides)
        return source_state(**values)

    def apple_universe(self, **overrides) -> list[dict]:
        security = {
            "cusip": "037833100",
            "instrument_type": "EQUITY",
            "reported_issuer": "APPLE INC",
            "reported_class": "COM",
        }
        security.update(overrides)
        return [security]

    def test_master_audit_is_reprojected_from_retained_sec_state(self) -> None:
        state = self.apple_state()
        built = master.rebuild_security_master(state, self.apple_universe())

        self.assertEqual(
            built["audit"],
            master.project_master_audit(built, state),
        )

        forged = copy.deepcopy(built)
        forged["audit"]["active_non_option_official_cusip_count"] = 5_000
        forged["audit"]["ftd_evidenced_official_cusip_count"] = 5_000
        forged["audit"]["ftd_coverage_ratio"] = 1.0
        # The forged document remains structurally self-consistent, but its
        # production claims must not equal the deterministic SEC projection.
        master.validate_security_master(forged)
        self.assertNotEqual(
            forged["audit"],
            master.project_master_audit(forged, state),
        )

    def test_rebuild_embeds_checksummed_unique_fund_series_name(self) -> None:
        cusip = "78462F103"
        cik = "0000884394"
        fund_page_url = master.sec_fund_series_url(cik)
        state = source_state(
            rows=[
                ftd_record(
                    "2026-08-01",
                    symbol="SPY",
                    cusip=cusip,
                    description="SPDR S&P 500 ETF TRUST",
                ),
                ftd_record(
                    "2026-08-04",
                    symbol="SPY",
                    cusip=cusip,
                    description="SPDR S&P 500 ETF TRUST",
                ),
            ],
            official_rows=[
                official_record(
                    cusip=cusip,
                    issuer="SPDR S&P 500 ETF TRUST",
                    description="UNIT SER 1",
                )
            ],
        )
        state["sources"][master.SEC_FUND_TICKERS_URL] = {
            "url": master.SEC_FUND_TICKERS_URL,
            "kind": "sec_fund_tickers",
            "sha256": "d" * 64,
            "accepted_at": "2026-08-20T12:00:00Z",
            "symbols": ["SPY"],
            "symbol_titles": {"SPY": ["SPDR S&P 500 ETF Trust"]},
            "symbol_exchanges": {},
            "symbol_count": 1,
            "fund_records": [
                {
                    "symbol": "SPY",
                    "cik": cik,
                    "series_id": "S000002745",
                    "class_id": "C000007635",
                }
            ],
        }
        state["sources"][fund_page_url] = {
            "url": fund_page_url,
            "kind": "sec_fund_series",
            "sha256": "e" * 64,
            "accepted_at": "2026-08-20T12:00:00Z",
            "last_successful_check_at": "2026-08-20T12:00:00Z",
            "cik": cik,
            "series_names": {"S000002745": "SPDR S&P 500 ETF Trust"},
            "class_names": {"C000007635": "SPDR S&P 500 ETF Trust"},
        }

        rebuilt = master.rebuild_security_master(
            state,
            [
                {
                    "cusip": cusip,
                    "instrument_type": "EQUITY",
                    "reported_issuer": "SPDR S&P 500 ETF TRUST",
                    "reported_class": "UNIT SER 1",
                }
            ],
        )

        record = rebuilt["records"][f"{cusip}|EQUITY"]
        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("SPDR S&P 500 ETF Trust", record["fund_series_name"])
        self.assertEqual(fund_page_url, record["fund_series_evidence"]["url"])
        self.assertEqual("e" * 64, record["fund_series_evidence"]["sha256"])

    def test_rebuild_is_deterministic_and_resolves_exact_key(self) -> None:
        state = self.apple_state()
        first = master.rebuild_security_master(state, self.apple_universe())
        second = master.rebuild_security_master(state, self.apple_universe())
        self.assertEqual(first, second)
        record = first["records"]["037833100|EQUITY"]
        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("AAPL", record["ticker"])
        self.assertEqual("sec_ftd", record["ticker_source"])
        self.assertEqual("2026-08-04", record["ticker_as_of"])
        self.assertEqual("2026-08-04", record["last_verification_date"])
        self.assertEqual(
            "exact_ftd_symbol_with_sec_metadata_validation",
            record["mapping_method"],
        )
        self.assertEqual("2026-08-01", record["effective_from"])
        self.assertIsNone(record["effective_to"])
        self.assertEqual("APPLE INC", record["issuer"])
        self.assertEqual("COM", record["security_class"])
        self.assertEqual("Nasdaq", record["exchange"])
        self.assertEqual(["Nasdaq"], record["exchanges"])
        self.assertEqual("sec_13f_list", record["security_label_source"])
        self.assertEqual("2026Q2", record["official_13f"]["period"])
        self.assertEqual(record, master.resolve_security(first, "037833100", "EQUITY"))
        self.assertEqual(
            "unresolved",
            master.resolve_security(first, "037833100", "PREF")["mapping_status"],
        )

        invalid_method = copy.deepcopy(first)
        invalid_method["records"]["037833100|EQUITY"]["mapping_method"] = (
            "issuer_name_guess"
        )
        with self.assertRaises(master.SecurityMasterError):
            master.validate_security_master(invalid_method)

        invalid_interval = copy.deepcopy(first)
        invalid_interval["records"]["037833100|EQUITY"]["effective_from"] = (
            "2026-08-05"
        )
        with self.assertRaises(master.SecurityMasterError):
            master.validate_security_master(invalid_interval)

    def test_active_official_identity_overrides_noisy_filer_descriptors(self) -> None:
        state = self.apple_state()
        built = master.rebuild_security_master(
            state,
            [
                {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "reported_issuer": "APPLE INC",
                    "reported_class": "COM",
                },
                {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "reported_issuer": "AON PLC",
                    "reported_class": "COMMON STOCK",
                },
                {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "reported_issuer": "Apple Inc.",
                    "reported_class": "CALL",
                },
            ],
        )

        record = built["records"]["037833100|EQUITY"]
        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("AAPL", record["ticker"])
        self.assertEqual("sec_ftd", record["ticker_source"])
        self.assertEqual("APPLE INC", record["issuer"])
        self.assertEqual("COM", record["security_class"])
        self.assertIn("AON PLC", record["reported_issuers"])
        self.assertIn("CALL", record["reported_classes"])
        master.validate_security_master(built)

    def test_conflicting_active_official_identity_fails_closed(self) -> None:
        state = self.apple_state(
            official_rows=[
                official_record(),
                official_record(issuer="MICROSOFT CORP"),
            ]
        )
        record = master.rebuild_security_master(
            state,
            self.apple_universe(),
        )["records"]["037833100|EQUITY"]

        self.assertEqual("ambiguous", record["mapping_status"])
        self.assertEqual(
            "conflicting_active_official_13f_identity",
            record["resolution_reason"],
        )
        self.assertIsNone(record["ticker"])

    def test_no_official_identity_keeps_reported_conflict_fail_closed(self) -> None:
        state = self.apple_state(official_rows=None)
        record = master.rebuild_security_master(
            state,
            [
                *self.apple_universe(),
                {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "reported_issuer": "MICROSOFT CORP",
                    "reported_class": "COM",
                },
            ],
        )["records"]["037833100|EQUITY"]

        self.assertEqual("ambiguous", record["mapping_status"])
        self.assertEqual(
            "conflicting_reported_issuer_or_class",
            record["resolution_reason"],
        )
        self.assertIsNone(record["ticker"])

    def test_exact_ftd_proof_resolves_preferred_and_warrant_classes(
        self,
    ) -> None:
        preferred_cusip = numbered_cusip(7001)
        warrant_cusip = numbered_cusip(7002)
        fixtures = (
            (preferred_cusip, "PREF", "PRFA", "PFD SER A"),
            (warrant_cusip, "WARRANT", "WRTS", "WARRANTS"),
        )
        state = source_state(
            rows=[
                ftd_record(
                    settlement_date,
                    symbol,
                    cusip=cusip,
                    description="EXAMPLE CAPITAL INC",
                )
                for cusip, _instrument_type, symbol, _security_class in fixtures
                for settlement_date in ("2026-08-01", "2026-08-04")
            ],
            symbols=[symbol for _cusip, _type, symbol, _class in fixtures],
            symbol_titles={
                symbol: ["Example Capital Inc."]
                for _cusip, _type, symbol, _class in fixtures
            },
            symbol_exchanges={
                symbol: ["NYSE"]
                for _cusip, _type, symbol, _class in fixtures
            },
            official_rows=[
                official_record(
                    cusip=cusip,
                    issuer="EXAMPLE CAPITAL INC",
                    description=security_class,
                )
                for cusip, _instrument_type, _symbol, security_class in fixtures
            ],
        )
        universe = [
            {
                "cusip": cusip,
                "instrument_type": instrument_type,
                "reported_issuer": "EXAMPLE CAPITAL INC",
                "reported_class": security_class,
            }
            for cusip, instrument_type, _symbol, security_class in fixtures
        ]

        records = master.rebuild_security_master(state, universe)["records"]

        for cusip, instrument_type, symbol, security_class in fixtures:
            with self.subTest(instrument_type=instrument_type):
                record = records[f"{cusip}|{instrument_type}"]
                self.assertEqual("resolved", record["mapping_status"])
                self.assertEqual(symbol, record["ticker"])
                self.assertEqual("sec_ftd", record["ticker_source"])
                self.assertEqual(
                    "exact_ftd_symbol_with_sec_metadata_validation",
                    record["mapping_method"],
                )
                self.assertEqual("2026-08-04", record["ticker_as_of"])
                self.assertEqual("2026-08-01", record["effective_from"])
                self.assertEqual("NYSE", record["exchange"])
                self.assertEqual(security_class, record["security_class"])
                self.assertEqual("active", record["official_13f_status"])
                self.assertNotIn(f"{cusip}|EQUITY", records)

    def test_clean_rebuilds_are_byte_identical_after_clock_normalization(
        self,
    ) -> None:
        first_state = self.apple_state()
        second_state = copy.deepcopy(first_state)
        second_state["updated_at"] = "2026-08-21T12:00:00Z"
        for source in second_state["sources"].values():
            source["accepted_at"] = "2026-08-21T12:00:00Z"
            if "last_successful_check_at" in source:
                source["last_successful_check_at"] = "2026-08-21T12:00:00Z"

        first = master.rebuild_security_master(
            first_state,
            self.apple_universe(),
        )
        second = master.rebuild_security_master(
            second_state,
            self.apple_universe(),
        )

        self.assertNotEqual(first, second)
        self.assertEqual(
            master.normalized_security_master_bytes(first),
            master.normalized_security_master_bytes(second),
        )

    def test_resolved_ftd_ticker_is_bound_to_exact_sec_source_hashes(self) -> None:
        built = master.rebuild_security_master(
            self.apple_state(), self.apple_universe()
        )

        unlisted_hash = copy.deepcopy(built)
        unlisted_record = unlisted_hash["records"]["037833100|EQUITY"]
        for evidence in unlisted_record["symbol_evidence"]:
            evidence["sources"][0]["sha256"] = "9" * 64
        unlisted_record["symbol_intervals"][0]["sources"][0]["sha256"] = (
            "9" * 64
        )
        with self.assertRaises(master.SecurityMasterError):
            master.validate_security_master(unlisted_hash)

        wrong_source_kind = copy.deepcopy(built)
        wrong_record = wrong_source_kind["records"]["037833100|EQUITY"]
        company_source = next(
            source
            for source in wrong_source_kind["sources"]
            if source["kind"] == "sec_company_tickers"
        )
        replacement = {
            "url": company_source["url"],
            "sha256": company_source["sha256"],
        }
        for evidence in wrong_record["symbol_evidence"]:
            evidence["sources"][0].update(replacement)
        wrong_record["symbol_intervals"][0]["sources"][0].update(replacement)
        with self.assertRaises(master.SecurityMasterError):
            master.validate_security_master(wrong_source_kind)

        missing_validation_proof = copy.deepcopy(built)
        missing_validation_proof["records"]["037833100|EQUITY"][
            "symbol_validation_titles"
        ] = []
        with self.assertRaises(master.SecurityMasterError):
            master.validate_security_master(missing_validation_proof)

    def test_validator_rejects_ftd_proof_on_ineligible_identity_type(
        self,
    ) -> None:
        built = master.rebuild_security_master(
            self.apple_state(), self.apple_universe()
        )

        for instrument_type in ("NOTE", "CALL", "PUT"):
            with self.subTest(instrument_type=instrument_type):
                tampered = copy.deepcopy(built)
                record = tampered["records"].pop("037833100|EQUITY")
                record["instrument_type"] = instrument_type
                tampered["records"][f"037833100|{instrument_type}"] = record

                with self.assertRaises(master.SecurityMasterError):
                    master.validate_security_master(tampered)

    def test_validator_replays_ftd_class_and_issuer_compatibility(self) -> None:
        state = self.apple_state(
            rows=[
                ftd_record("2026-08-01", description="APPLE INC"),
                ftd_record("2026-08-04", description="APPLE INCORPORATED"),
            ],
            official_rows=[
                official_record(description="CLASS A COMMON STOCK")
            ],
        )
        built = master.rebuild_security_master(
            state,
            self.apple_universe(reported_class="CL A"),
        )
        key = "037833100|EQUITY"
        self.assertEqual("resolved", built["records"][key]["mapping_status"])
        master.validate_security_master(built)

        for conflict in (
            "reported debt class",
            "official class designator",
            "reported issuer",
            "FTD description",
            "SEC validation title",
            "official issuer",
        ):
            with self.subTest(conflict=conflict):
                tampered = copy.deepcopy(built)
                record = tampered["records"][key]
                if conflict == "reported debt class":
                    record["reported_class"] = "Senior Notes"
                    record["reported_classes"] = ["Senior Notes"]
                elif conflict == "official class designator":
                    record["official_13f"]["records"][0]["description"] = (
                        "CLASS C COMMON STOCK"
                    )
                elif conflict == "reported issuer":
                    record["reported_issuer"] = "MICROSOFT CORP"
                    record["reported_issuers"] = ["MICROSOFT CORP"]
                elif conflict == "FTD description":
                    for item in record["symbol_evidence"]:
                        item["descriptions"] = ["MICROSOFT CORP"]
                    for interval in record["symbol_intervals"]:
                        interval["descriptions"] = ["MICROSOFT CORP"]
                elif conflict == "SEC validation title":
                    record["symbol_validation_titles"] = [
                        "Microsoft Corporation"
                    ]
                else:
                    record["official_13f"]["records"][0]["issuer"] = (
                        "MICROSOFT CORP"
                    )

                with self.assertRaises(master.SecurityMasterError):
                    master.validate_security_master(tampered)

    def test_validator_rejects_ixbrl_proof_on_master_identity_conflict(
        self,
    ) -> None:
        from tests.test_sec_edgar_evidence import refreshed_cache

        state = self.apple_state()
        state["edgar_evidence"] = refreshed_cache()
        built = master.rebuild_security_master(
            state,
            [
                *self.apple_universe(),
                {
                    "cusip": "02079K305",
                    "instrument_type": "EQUITY",
                    "reported_issuer": "Alphabet Inc.",
                    "reported_class": "Class A Common Stock",
                },
            ],
        )
        alphabet_key = "02079K305|EQUITY"
        self.assertEqual(
            "sec_ixbrl",
            built["records"][alphabet_key]["ticker_source"],
        )
        cases = {
            "ineligible instrument": {
                "instrument_type": "NOTE",
            },
            "proof class family": {
                "instrument_type": "PREF",
            },
            "as-filed debt class": {
                "reported_class": "Senior Notes",
                "reported_classes": ["Senior Notes"],
            },
            "as-filed share class": {
                "reported_class": "Class C Common Stock",
                "reported_classes": ["Class C Common Stock"],
            },
            "as-filed issuer": {
                "reported_issuer": "Unrelated Corporation",
                "reported_issuers": ["Unrelated Corporation"],
            },
            "issuer CIK": {
                "issuer_cik": "0000789019",
            },
        }
        for conflict, changes in cases.items():
            with self.subTest(conflict=conflict):
                tampered = copy.deepcopy(built)
                record = tampered["records"].pop(alphabet_key)
                record.update(changes)
                tampered["records"][
                    f"02079K305|{record['instrument_type']}"
                ] = record

                with self.assertRaises(master.SecurityMasterError):
                    master.validate_security_master(tampered)

    def test_coverage_gate_counts_only_the_trailing_available_year(self) -> None:
        state = self.apple_state(
            rows=[
                ftd_record("2026-08-04"),
                ftd_record(
                    "2024-01-15",
                    "MSFT",
                    cusip="594918104",
                    description="MICROSOFT CORP",
                ),
            ],
            symbols=["AAPL", "MSFT"],
            symbol_titles={
                "AAPL": ["Apple Inc."],
                "MSFT": ["Microsoft Corporation"],
            },
            official_rows=[
                official_record(),
                official_record(
                    cusip="594918104",
                    issuer="MICROSOFT CORP",
                ),
            ],
        )
        built = master.rebuild_security_master(
            state,
            [
                *self.apple_universe(),
                {
                    "cusip": "594918104",
                    "instrument_type": "EQUITY",
                    "reported_issuer": "MICROSOFT CORP",
                    "reported_class": "COM",
                },
            ],
        )

        self.assertEqual(366, built["audit"]["ftd_coverage_window_days"])
        self.assertEqual(
            "2025-08-03",
            built["audit"]["ftd_coverage_cutoff_date"],
        )
        self.assertEqual(
            1,
            built["audit"]["ftd_evidenced_official_cusip_count"],
        )
        self.assertEqual(0.5, built["audit"]["ftd_coverage_ratio"])

    def test_common_equities_and_convertible_notes_acceptance_fixture(self) -> None:
        equities = [
            ("037833100", "AAPL", "APPLE INC", "Apple Inc.", "COM"),
            (
                "30231G102",
                "XOM",
                "EXXON MOBIL CORP",
                "ExxonMobil Holdings Corp",
                "COM",
            ),
            (
                "76954A103",
                "RIVN",
                "RIVIAN AUTOMOTIVE INC",
                "Rivian Automotive, Inc.",
                "CLASS A COM",
            ),
        ]
        rows = [
            ftd_record(
                settlement_date,
                symbol,
                cusip=cusip,
                description=issuer,
            )
            for cusip, symbol, issuer, _title, _security_class in equities
            for settlement_date in ("2026-08-01", "2026-08-04")
        ]
        state = source_state(
            rows=rows,
            symbols=[symbol for _cusip, symbol, *_rest in equities],
            symbol_titles={
                symbol: [title]
                for _cusip, symbol, _issuer, title, _security_class in equities
            },
            symbol_exchanges={
                symbol: ["Nasdaq" if symbol != "XOM" else "NYSE"]
                for _cusip, symbol, *_rest in equities
            },
            official_rows=[
                official_record(
                    cusip=cusip,
                    issuer=issuer,
                    description=security_class,
                )
                for cusip, _symbol, issuer, _title, security_class in equities
            ],
        )
        notes = [
            (
                "76954AAD5",
                "RIVIAN AUTOMOTIVE INC",
                "4.625% CONVERTIBLE SENIOR NOTES DUE 2029",
            ),
            (
                "090043AF7",
                "BILL HOLDINGS INC",
                "0% CONVERTIBLE SENIOR NOTES DUE 2027",
            ),
            (
                "26210CAC8",
                "DROPBOX INC",
                "0% CONVERTIBLE SENIOR NOTES DUE 2026",
            ),
            (
                "26210CAD6",
                "DROPBOX INC",
                "0% CONVERTIBLE SENIOR NOTES DUE 2028",
            ),
        ]
        universe = [
            {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "reported_issuer": issuer,
                "reported_class": security_class,
            }
            for cusip, _symbol, issuer, _title, security_class in equities
        ] + [
            {
                "cusip": cusip,
                "instrument_type": "NOTE",
                "reported_issuer": issuer,
                "reported_class": security_class,
            }
            for cusip, issuer, security_class in notes
        ]

        built = master.rebuild_security_master(state, universe)

        for cusip, symbol, *_rest in equities:
            record = built["records"][f"{cusip}|EQUITY"]
            self.assertEqual("resolved", record["mapping_status"])
            self.assertEqual(symbol, record["ticker"])
            self.assertEqual("sec_ftd", record["ticker_source"])
            self.assertEqual(
                record["ticker_as_of"],
                record["last_verification_date"],
            )
        for cusip, _issuer, _security_class in notes:
            record = built["records"][f"{cusip}|NOTE"]
            self.assertEqual("no_listed_symbol", record["mapping_status"])
            self.assertEqual(
                "instrument_type_not_ftd_eligible",
                record["resolution_reason"],
            )
            self.assertIsNone(record["ticker"])
            self.assertIsNone(record["ticker_source"])
            self.assertIsNone(record["ticker_as_of"])
            self.assertIsNone(record["last_verification_date"])

    def test_embedded_edgar_cache_survives_state_round_trip_and_rebuild(self) -> None:
        # Reuse the public EDGAR refresh fixture so this integration test is
        # locked to the same complete, independently validated cache contract.
        from tests.test_sec_edgar_evidence import refreshed_cache

        state = self.apple_state()
        state["edgar_evidence"] = refreshed_cache()
        state["edgar_discovery"] = {
            "diagnostics": [{"cusip": "02079K305", "status": "sources_found"}]
        }
        universe = [
            {
                "cusip": "037833100",
                "instrument_type": "EQUITY",
                "reported_issuer": "APPLE INC",
                "reported_class": "COM",
            },
            {
                "cusip": "02079K305",
                "instrument_type": "EQUITY",
                "reported_issuer": "Alphabet Inc.",
                "reported_class": "Class A Common Stock",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            master.save_source_state(state, state_path)
            persisted = master.load_source_state(state_path)
            built = master.rebuild_security_master(persisted, universe)

        apple = built["records"]["037833100|EQUITY"]
        alphabet = built["records"]["02079K305|EQUITY"]
        self.assertEqual("AAPL", apple["ticker"])
        self.assertEqual("sec_ftd", apple["ticker_source"])
        self.assertEqual("GOOGL", alphabet["ticker"])
        self.assertEqual("sec_ixbrl", alphabet["ticker_source"])
        self.assertEqual("2026-06-30", alphabet["ticker_as_of"])
        self.assertEqual("2026-06-30", alphabet["last_verification_date"])
        self.assertEqual(
            state["edgar_discovery"],
            persisted["edgar_discovery"],
        )

    def test_recent_symbol_conflict_is_ambiguous(self) -> None:
        state = self.apple_state(
            rows=[
                ftd_record("2026-08-01", "AAPL"),
                ftd_record("2026-08-04", "AAPX"),
            ],
            symbols=["AAPL", "AAPX"],
            symbol_titles={"AAPL": ["Apple Inc."], "AAPX": ["Apple Inc."]},
        )
        record = master.rebuild_security_master(
            state, self.apple_universe(), min_confirmation_dates=1
        )["records"]["037833100|EQUITY"]
        self.assertEqual("ambiguous", record["mapping_status"])
        self.assertIsNone(record["ticker"])
        self.assertIsNone(record["ticker_source"])
        self.assertIsNone(record["ticker_as_of"])
        self.assertEqual(
            [("AAPL", "2026-08-01"), ("AAPX", "2026-08-04")],
            [
                (interval["symbol"], interval["first_seen"])
                for interval in record["symbol_intervals"]
            ],
        )

    def test_recent_interval_conflict_survives_global_witness_bound(self) -> None:
        rows = [ftd_record("2026-08-01", "AAPL")]
        rows.extend(
            ftd_record(f"2026-08-{day:02d}", "AAPX")
            for day in range(2, 32)
        )
        state = self.apple_state(
            rows=rows,
            symbols=["AAPL", "AAPX"],
            symbol_titles={"AAPL": ["Apple Inc."], "AAPX": ["Apple Inc."]},
        )

        record = master.rebuild_security_master(
            state,
            self.apple_universe(),
            min_confirmation_dates=1,
        )["records"]["037833100|EQUITY"]

        self.assertEqual("ambiguous", record["mapping_status"])
        self.assertEqual(
            "conflicting_recent_ftd_symbols", record["resolution_reason"]
        )
        self.assertEqual(["AAPL", "AAPX"], record["candidate_symbols"])
        self.assertEqual(
            31,
            len({
                item["settlement_date"] for item in record["symbol_evidence"]
            }),
        )

    def test_recent_description_conflict_survives_more_than_eight_dates(
        self,
    ) -> None:
        rows = [
            ftd_record(
                "2026-08-01",
                description="MICROSOFT CORP",
            )
        ]
        rows.extend(
            ftd_record(
                f"2026-08-{day:02d}",
                description="APPLE INC",
            )
            for day in range(2, 14)
        )
        record = master.rebuild_security_master(
            self.apple_state(rows=rows),
            self.apple_universe(),
            min_confirmation_dates=1,
        )["records"]["037833100|EQUITY"]

        self.assertEqual("ambiguous", record["mapping_status"])
        self.assertEqual(
            "issuer_conflict_with_ftd_description",
            record["resolution_reason"],
        )

    def test_description_older_than_recent_window_no_longer_blocks(self) -> None:
        record = master.rebuild_security_master(
            self.apple_state(rows=[
                ftd_record(
                    "2026-01-02",
                    description="MICROSOFT CORP",
                ),
                ftd_record("2026-08-01", description="APPLE INC"),
                ftd_record("2026-08-04", description="APPLE INC"),
            ]),
            self.apple_universe(),
        )["records"]["037833100|EQUITY"]

        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("AAPL", record["ticker"])
        self.assertNotIn(
            "MICROSOFT CORP",
            {
                description
                for item in record["symbol_evidence"]
                if item["settlement_date"] >= "2026-07-04"
                for description in item["descriptions"]
            },
        )

    def test_recent_window_cannot_exceed_exact_witness_retention(self) -> None:
        with self.assertRaisesRegex(
            master.SecurityMasterError,
            "exact FTD witness retention limit",
        ):
            master.rebuild_security_master(
                self.apple_state(),
                self.apple_universe(),
                recent_window_days=32,
            )

    def test_ticker_change_is_stored_as_ordered_symbol_intervals(self) -> None:
        state = self.apple_state(rows=[
            ftd_record("2025-01-02", "OLD"),
            ftd_record("2025-01-06", "OLD"),
            ftd_record("2026-08-01", "AAPL"),
            ftd_record("2026-08-04", "AAPL"),
        ])
        record = master.rebuild_security_master(
            state, self.apple_universe()
        )["records"]["037833100|EQUITY"]
        intervals = record["symbol_intervals"]
        self.assertEqual(["OLD", "AAPL"], [item["symbol"] for item in intervals])
        self.assertEqual(
            ["2025-01-02", "2025-01-06"],
            intervals[0]["observation_dates"],
        )
        self.assertEqual("2025-01-02", intervals[0]["first_seen"])
        self.assertEqual("2025-01-06", intervals[0]["last_seen"])
        self.assertEqual(2, intervals[0]["observation_date_count"])
        self.assertEqual(2, intervals[0]["observation_count"])
        self.assertEqual(
            [{
                "url": (
                    "https://www.sec.gov/files/data/fails-deliver-data/"
                    "cnsfails202501a.zip"
                ),
                "sha256": SHA_A,
            }],
            intervals[0]["sources"],
        )
        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("AAPL", record["ticker"])

    def test_reused_symbol_starts_a_new_time_versioned_interval(self) -> None:
        state = self.apple_state(rows=[
            ftd_record("2024-01-02", "AAPL"),
            ftd_record("2025-01-02", "MIDDLE"),
            ftd_record("2026-08-01", "AAPL"),
            ftd_record("2026-08-04", "AAPL"),
        ])
        record = master.rebuild_security_master(
            state, self.apple_universe()
        )["records"]["037833100|EQUITY"]
        self.assertEqual(
            [
                ("AAPL", "2024-01-02", "2024-01-02"),
                ("MIDDLE", "2025-01-02", "2025-01-02"),
                ("AAPL", "2026-08-01", "2026-08-04"),
            ],
            [
                (item["symbol"], item["first_seen"], item["last_seen"])
                for item in record["symbol_intervals"]
            ],
        )
        self.assertEqual("AAPL", record["ticker"])

    def test_historical_symbol_does_not_override_recent_symbol(self) -> None:
        state = self.apple_state(rows=[
            ftd_record("2025-01-02", "OLD"),
            ftd_record("2026-08-01", "AAPL"),
            ftd_record("2026-08-04", "AAPL"),
        ])
        record = master.rebuild_security_master(
            state, self.apple_universe()
        )["records"]["037833100|EQUITY"]
        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("AAPL", record["ticker"])

    def test_newer_cusip_with_same_symbol_supersedes_stale_current_claim(
        self,
    ) -> None:
        old_cusip = "037833118"
        new_cusip = "037833126"
        state = source_state(
            rows=[
                ftd_record("2026-01-02", cusip=old_cusip),
                ftd_record("2026-01-06", cusip=old_cusip),
                ftd_record("2026-08-01", cusip=new_cusip),
                ftd_record("2026-08-04", cusip=new_cusip),
            ],
            symbols=["AAPL"],
            symbol_titles={"AAPL": ["Apple Inc."]},
            official_rows=[
                official_record(cusip=old_cusip),
                official_record(cusip=new_cusip),
            ],
        )
        universe = [
            {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "reported_issuer": "APPLE INC",
                "reported_class": "COM",
            }
            for cusip in (old_cusip, new_cusip)
        ]

        records = master.rebuild_security_master(state, universe)["records"]

        old = records[f"{old_cusip}|EQUITY"]
        new = records[f"{new_cusip}|EQUITY"]
        self.assertEqual("no_listed_symbol", old["mapping_status"])
        self.assertEqual(
            "current_symbol_observed_on_newer_cusip",
            old["resolution_reason"],
        )
        self.assertEqual([new_cusip], old["superseded_by_cusips"])
        self.assertIsNone(old["ticker"])
        self.assertEqual("AAPL", old["symbol_intervals"][0]["symbol"])
        self.assertEqual("resolved", new["mapping_status"])
        self.assertEqual("AAPL", new["ticker"])

    def test_one_newer_exact_observation_withdraws_stale_current_claim(
        self,
    ) -> None:
        old_cusip = "037833118"
        new_cusip = "037833126"
        state = source_state(
            rows=[
                ftd_record("2026-01-02", cusip=old_cusip),
                ftd_record("2026-01-06", cusip=old_cusip),
                ftd_record("2026-08-04", cusip=new_cusip),
            ],
            symbols=["AAPL"],
            symbol_titles={"AAPL": ["Apple Inc."]},
            official_rows=[
                official_record(cusip=old_cusip),
                official_record(cusip=new_cusip),
            ],
        )
        universe = [
            {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "reported_issuer": "APPLE INC",
                "reported_class": "COM",
            }
            for cusip in (old_cusip, new_cusip)
        ]

        records = master.rebuild_security_master(state, universe)["records"]

        old = records[f"{old_cusip}|EQUITY"]
        new = records[f"{new_cusip}|EQUITY"]
        self.assertEqual("no_listed_symbol", old["mapping_status"])
        self.assertEqual(
            "current_symbol_observed_on_newer_cusip",
            old["resolution_reason"],
        )
        self.assertEqual([new_cusip], old["superseded_by_cusips"])
        self.assertIsNone(old["ticker"])
        self.assertEqual("unresolved", new["mapping_status"])
        self.assertEqual(
            "insufficient_distinct_ftd_settlement_dates",
            new["resolution_reason"],
        )
        self.assertEqual("AAPL", new["candidate_ticker"])
        self.assertIsNone(new["ticker"])

    def test_concurrent_same_symbol_cusips_keep_independent_exact_proof(
        self,
    ) -> None:
        first_cusip = "037833118"
        second_cusip = "037833126"
        state = source_state(
            rows=[
                ftd_record("2026-07-20", cusip=first_cusip),
                ftd_record("2026-07-23", cusip=first_cusip),
                ftd_record("2026-08-01", cusip=second_cusip),
                ftd_record("2026-08-04", cusip=second_cusip),
            ],
            symbols=["AAPL"],
            symbol_titles={"AAPL": ["Apple Inc."]},
            official_rows=[
                official_record(cusip=first_cusip),
                official_record(cusip=second_cusip),
            ],
        )
        universe = [
            {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "reported_issuer": "APPLE INC",
                "reported_class": "COM",
            }
            for cusip in (first_cusip, second_cusip)
        ]

        records = master.rebuild_security_master(state, universe)["records"]

        for cusip in (first_cusip, second_cusip):
            with self.subTest(cusip=cusip):
                record = records[f"{cusip}|EQUITY"]
                self.assertEqual("resolved", record["mapping_status"])
                self.assertEqual("AAPL", record["ticker"])

    def test_reused_ticker_with_incompatible_current_issuer_fails_closed(
        self,
    ) -> None:
        old_cusip = "037833118"
        new_cusip = "037833126"
        state = source_state(
            rows=[
                ftd_record(
                    "2026-01-02",
                    symbol="REUSE",
                    cusip=old_cusip,
                    description="OLD ISSUER CORP",
                ),
                ftd_record(
                    "2026-01-06",
                    symbol="REUSE",
                    cusip=old_cusip,
                    description="OLD ISSUER CORP",
                ),
                ftd_record(
                    "2026-08-01",
                    symbol="REUSE",
                    cusip=new_cusip,
                    description="NEW ISSUER CORP",
                ),
                ftd_record(
                    "2026-08-04",
                    symbol="REUSE",
                    cusip=new_cusip,
                    description="NEW ISSUER CORP",
                ),
            ],
            symbols=["REUSE"],
            symbol_titles={"REUSE": ["New Issuer Corporation"]},
            official_rows=[
                official_record(cusip=old_cusip, issuer="OLD ISSUER CORP"),
                official_record(cusip=new_cusip, issuer="NEW ISSUER CORP"),
            ],
        )
        universe = [
            {
                "cusip": old_cusip,
                "instrument_type": "EQUITY",
                "reported_issuer": "OLD ISSUER CORP",
                "reported_class": "COM",
            },
            {
                "cusip": new_cusip,
                "instrument_type": "EQUITY",
                "reported_issuer": "NEW ISSUER CORP",
                "reported_class": "COM",
            },
        ]

        records = master.rebuild_security_master(state, universe)["records"]

        self.assertEqual(
            "ambiguous",
            records[f"{old_cusip}|EQUITY"]["mapping_status"],
        )
        self.assertIsNone(records[f"{old_cusip}|EQUITY"]["ticker"])
        self.assertEqual(
            "resolved",
            records[f"{new_cusip}|EQUITY"]["mapping_status"],
        )
        self.assertEqual("REUSE", records[f"{new_cusip}|EQUITY"]["ticker"])

    def test_effective_from_is_active_interval_first_seen(self) -> None:
        state = self.apple_state(rows=[
            ftd_record("2025-01-02", "AAPL"),
            ftd_record("2026-08-01", "AAPL"),
            ftd_record("2026-08-04", "AAPL"),
        ])

        record = master.rebuild_security_master(
            state, self.apple_universe()
        )["records"]["037833100|EQUITY"]

        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("2025-01-02", record["effective_from"])

    def test_titleless_current_symbol_membership_cannot_resolve(self) -> None:
        state = self.apple_state(symbol_titles={})

        record = master.rebuild_security_master(
            state, self.apple_universe()
        )["records"]["037833100|EQUITY"]

        self.assertEqual("unresolved", record["mapping_status"])
        self.assertEqual(
            "symbol_lacks_current_sec_issuer_metadata",
            record["resolution_reason"],
        )
        self.assertIsNone(record["ticker"])

    def test_symbol_must_match_current_sec_input_exactly(self) -> None:
        state = self.apple_state(
            rows=[
                ftd_record("2026-08-01", "AAPL.X"),
                ftd_record("2026-08-04", "AAPL.X"),
            ],
            symbols=["AAPL-X"],
            symbol_titles={"AAPL-X": ["Apple Inc."]},
            symbol_exchanges={},
        )
        record = master.rebuild_security_master(
            state, self.apple_universe()
        )["records"]["037833100|EQUITY"]
        self.assertEqual("unresolved", record["mapping_status"])
        self.assertEqual(
            "symbol_absent_from_current_sec_validation_inputs",
            record["resolution_reason"],
        )
        self.assertIsNone(record["ticker"])

    def test_bad_check_digit_is_quarantined_in_master(self) -> None:
        state = source_state(
            rows=[
                ftd_record("2026-08-01", cusip="037833101"),
                ftd_record("2026-08-04", cusip="037833101"),
            ],
            symbols=["AAPL"],
            symbol_titles={"AAPL": ["Apple Inc."]},
        )
        built = master.rebuild_security_master(
            state, {"037833101": "EQUITY"}
        )
        record = built["records"]["037833101|EQUITY"]
        self.assertEqual("malformed_as_filed", record["mapping_status"])
        self.assertIn("037833101|EQUITY", built["quarantine"])
        self.assertIsNone(record["ticker"])

        bad_summary = copy.deepcopy(built)
        bad_summary["summary"]["malformed_as_filed"] = 0
        with self.assertRaisesRegex(
            master.SecurityMasterError,
            "summary does not match",
        ):
            master.validate_security_master(bad_summary)

        missing_quarantine = copy.deepcopy(built)
        missing_quarantine["quarantine"] = {}
        with self.assertRaisesRegex(
            master.SecurityMasterError,
            "quarantine does not match",
        ):
            master.validate_security_master(missing_quarantine)

        wrong_status = copy.deepcopy(built)
        wrong_status["records"]["037833101|EQUITY"][
            "mapping_status"
        ] = "unresolved"
        wrong_status["records"]["037833101|EQUITY"][
            "resolution_reason"
        ] = "no_exact_sec_symbol_evidence"
        wrong_status["summary"]["malformed_as_filed"] = 0
        wrong_status["summary"]["unresolved"] = 1
        with self.assertRaisesRegex(
            master.SecurityMasterError,
            "not quarantined exactly",
        ):
            master.validate_security_master(wrong_status)

    def test_note_and_debt_class_never_inherit_equity_ticker(self) -> None:
        state = self.apple_state()
        universe = [
            {
                "cusip": "037833100",
                "instrument_type": "NOTE",
                "reported_issuer": "APPLE INC",
                "reported_class": "SDBCV 3.625 2030",
            },
            {
                "cusip": "037833100",
                "instrument_type": "EQUITY",
                "reported_issuer": "APPLE INC",
                "reported_class": "SDBCV 3.625 2030",
            },
        ]
        records = master.rebuild_security_master(state, universe)["records"]
        self.assertEqual(
            "no_listed_symbol", records["037833100|NOTE"]["mapping_status"]
        )
        self.assertEqual(
            "no_listed_symbol", records["037833100|EQUITY"]["mapping_status"]
        )
        self.assertTrue(
            records["037833100|EQUITY"]["resolution_reason"].startswith("debt_")
        )

    def test_official_class_adds_exact_type_and_rejects_broad_equity(self) -> None:
        state = self.apple_state(
            official_rows=[official_record(description="CONV NOTE")],
        )
        records = master.rebuild_security_master(
            state,
            [{
                "cusip": "037833100",
                "instrument_type": "EQUITY",
                "reported_issuer": "APPLE INC",
                "reported_class": "CONV NOTE",
            }],
        )["records"]

        self.assertIn("037833100|NOTE", records)
        self.assertEqual(
            "no_listed_symbol",
            records["037833100|NOTE"]["mapping_status"],
        )
        self.assertEqual(
            "instrument_type_not_ftd_eligible",
            records["037833100|NOTE"]["resolution_reason"],
        )
        self.assertEqual(
            "no_listed_symbol",
            records["037833100|EQUITY"]["mapping_status"],
        )
        self.assertEqual(
            "official_13f_class_conflicts_with_instrument_type",
            records["037833100|EQUITY"]["resolution_reason"],
        )
        self.assertIsNone(records["037833100|EQUITY"]["ticker"])

    def test_option_underlying_equity_requires_official_non_option_class(self) -> None:
        state = self.apple_state()
        record = master.rebuild_security_master(
            state,
            self.apple_universe(reported_class="CALL"),
        )["records"]["037833100|EQUITY"]
        self.assertEqual("resolved", record["mapping_status"])

        without_official = self.apple_state(official_rows=None)
        record = master.rebuild_security_master(
            without_official,
            self.apple_universe(reported_class="CALL"),
        )["records"]["037833100|EQUITY"]
        self.assertEqual("no_listed_symbol", record["mapping_status"])

    def test_issuer_conflict_fails_closed_despite_exact_cusip(self) -> None:
        state = self.apple_state(rows=[
            ftd_record("2026-08-01", description="MICROSOFT CORP"),
            ftd_record("2026-08-04", description="MICROSOFT CORP"),
        ])
        record = master.rebuild_security_master(
            state, self.apple_universe()
        )["records"]["037833100|EQUITY"]
        self.assertEqual("ambiguous", record["mapping_status"])
        self.assertEqual(
            "issuer_conflict_with_ftd_description",
            record["resolution_reason"],
        )
        self.assertIsNone(record["ticker"])

    def test_ftd_semicolon_class_suffix_is_not_an_issuer_conflict(self) -> None:
        state = self.apple_state(rows=[
            ftd_record("2026-08-01", description="APPLE INC;COM NPV"),
            ftd_record("2026-08-04", description="APPLE INC;COM NPV"),
        ])
        record = master.rebuild_security_master(
            state,
            self.apple_universe(),
        )["records"]["037833100|EQUITY"]

        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("AAPL", record["ticker"])

    def test_one_unvalidated_ftd_symbol_does_not_displace_proven_symbol(
        self,
    ) -> None:
        state = self.apple_state(
            rows=[
                ftd_record("2026-08-01", "AAPL"),
                ftd_record("2026-08-04", "AAPL"),
                ftd_record("2026-08-05", "AAPLXXXX"),
            ],
            symbols=["AAPL"],
            symbol_titles={"AAPL": ["Apple Inc."]},
        )
        record = master.rebuild_security_master(
            state,
            self.apple_universe(),
        )["records"]["037833100|EQUITY"]

        self.assertEqual("resolved", record["mapping_status"])
        self.assertEqual("AAPL", record["ticker"])
        self.assertEqual("2026-08-04", record["ticker_as_of"])
        self.assertEqual("2026-08-04", record["last_verification_date"])
        self.assertEqual(
            ["AAPL", "AAPLXXXX"],
            [interval["symbol"] for interval in record["symbol_intervals"]],
        )

    def test_repeated_unvalidated_ftd_symbol_remains_a_conflict(self) -> None:
        state = self.apple_state(
            rows=[
                ftd_record("2026-08-01", "AAPL"),
                ftd_record("2026-08-02", "AAPL"),
                ftd_record("2026-08-03", "AAPLXXXX"),
                ftd_record("2026-08-04", "AAPLXXXX"),
            ],
            symbols=["AAPL"],
            symbol_titles={"AAPL": ["Apple Inc."]},
        )
        record = master.rebuild_security_master(
            state,
            self.apple_universe(),
        )["records"]["037833100|EQUITY"]

        self.assertEqual("ambiguous", record["mapping_status"])
        self.assertEqual(
            "conflicting_recent_ftd_symbols",
            record["resolution_reason"],
        )
        self.assertIsNone(record["ticker"])

    def test_current_sec_title_conflict_fails_closed(self) -> None:
        state = self.apple_state(
            symbol_titles={"AAPL": ["Microsoft Corporation"]}
        )
        record = master.rebuild_security_master(
            state, self.apple_universe()
        )["records"]["037833100|EQUITY"]
        self.assertEqual("ambiguous", record["mapping_status"])
        self.assertEqual(
            "issuer_conflict_with_sec_company_title",
            record["resolution_reason"],
        )

    def test_deleted_or_option_only_official_row_is_not_resolved(self) -> None:
        for row in (
            official_record(status="*D*"),
            official_record(description="CALL", option_indicator="*"),
        ):
            with self.subTest(row=row):
                state = self.apple_state(official_rows=[row])
                record = master.rebuild_security_master(
                    state, self.apple_universe()
                )["records"]["037833100|EQUITY"]
                self.assertEqual("no_listed_symbol", record["mapping_status"])
                self.assertIsNone(record["ticker"])


class AcceptanceAuditTests(unittest.TestCase):
    def test_fund_series_class_identity_satisfies_fund_metadata_sanity(
        self,
    ) -> None:
        populations, metadata_populations = (
            master._current_symbol_source_populations({
                "sources": {
                    master.SEC_FUND_TICKERS_URL: {
                        "kind": "sec_fund_tickers",
                        "symbols": ["VFIAX"],
                        "symbol_titles": {},
                        "fund_records": [{
                            "symbol": "VFIAX",
                            "cik": "0000102909",
                            "series_id": "S000002839",
                            "class_id": "C000007946",
                        }],
                    }
                }
            })
        )
        self.assertEqual(1, populations["sec_fund_tickers"])
        self.assertEqual(1, metadata_populations["sec_fund_tickers"])

    def coverage_master(self, *, total: int, evidenced: int) -> dict:
        cusips = [numbered_cusip(index) for index in range(total)]
        rows = [
            ftd_record(
                "2026-08-18",
                symbol="TEST",
                cusip=cusip,
                description=f"ISSUER {index}",
            )
            for index, cusip in enumerate(cusips[:evidenced])
        ]
        official_rows = [
            official_record(
                cusip=cusip,
                issuer=f"ISSUER {index}",
                description="COM",
            )
            for index, cusip in enumerate(cusips)
        ]
        state = source_state(rows=rows, official_rows=official_rows)
        accepted_at = "2026-08-20T12:00:00Z"
        state["sources"].update({
            master.SEC_COMPANY_TICKERS_URL: {
                "url": master.SEC_COMPANY_TICKERS_URL,
                "kind": "sec_company_tickers",
                "sha256": "d" * 64,
                "accepted_at": accepted_at,
                "symbols": ["TEST"],
                "symbol_titles": {"TEST": ["Test Issuer"]},
                "symbol_exchanges": {},
                "symbol_count": 1,
            },
            master.SEC_COMPANY_EXCHANGE_TICKERS_URL: {
                "url": master.SEC_COMPANY_EXCHANGE_TICKERS_URL,
                "kind": "sec_company_exchange_tickers",
                "sha256": "e" * 64,
                "accepted_at": accepted_at,
                "symbols": ["TEST"],
                "symbol_titles": {"TEST": ["Test Issuer"]},
                "symbol_exchanges": {},
                "symbol_count": 1,
            },
            master.SEC_FUND_TICKERS_URL: {
                "url": master.SEC_FUND_TICKERS_URL,
                "kind": "sec_fund_tickers",
                "sha256": "f" * 64,
                "accepted_at": accepted_at,
                "symbols": ["TEST"],
                "symbol_titles": {"TEST": ["Test Issuer Fund"]},
                "symbol_exchanges": {},
                "symbol_count": 1,
            },
            master.FTD_PAGE_URL: {
                "url": master.FTD_PAGE_URL,
                "kind": "sec_ftd_index",
                "sha256": "1" * 64,
                "accepted_at": accepted_at,
                "discovered_urls": [FTD_URL],
            },
            master.OFFICIAL_13F_LIST_PAGE_URL: {
                "url": master.OFFICIAL_13F_LIST_PAGE_URL,
                "kind": "sec_13f_list_index",
                "sha256": "2" * 64,
                "accepted_at": accepted_at,
                "discovered_urls": [LIST_URL],
            },
        })
        return master.rebuild_security_master(state, [])

    def ixbrl_master(self, *, checked_at: str | None) -> dict:
        from tests.test_sec_edgar_evidence import refreshed_cache

        current = self.coverage_master(total=20, evidenced=19)
        unresolved = master.rebuild_security_master(
            source_state(),
            [{
                "cusip": "02079K305",
                "instrument_type": "EQUITY",
                "reported_issuer": "Alphabet Inc.",
                "reported_class": "Class A Common Stock",
            }],
        )["records"]["02079K305|EQUITY"]
        current["records"]["02079K305|EQUITY"] = unresolved
        current["records"] = {
            key: current["records"][key] for key in sorted(current["records"])
        }
        current["summary"]["unresolved"] += 1
        (
            current["audit"]["reported_identity_count"],
            current["audit"]["evidenced_reported_identity_count"],
        ) = master._reported_identity_evidence_counts(current["records"])
        return __import__("sec_edgar_evidence").apply_sec_edgar_evidence(
            current,
            refreshed_cache(),
            successful_checkpoints={"02079K305": checked_at},
        )

    def test_audit_gate_accepts_95_percent_and_rejects_below_it(self) -> None:
        passing = self.coverage_master(total=20, evidenced=19)
        metadata = passing["audit"]
        self.assertEqual(20, metadata["active_non_option_official_cusip_count"])
        self.assertEqual(19, metadata["ftd_evidenced_official_cusip_count"])
        self.assertEqual(0.95, metadata["ftd_coverage_ratio"])
        self.assertEqual(2, metadata["ftd_source_age_days"])
        self.assertRegex(
            metadata["active_non_option_official_cusips_sha256"],
            r"^[0-9a-f]{64}$",
        )
        result = master.audit_security_master(passing)
        self.assertTrue(result["coverage_gate_passed"])
        self.assertTrue(result["ok"])

        failing = self.coverage_master(total=20, evidenced=18)
        result = master.audit_security_master(failing)
        self.assertFalse(result["coverage_gate_passed"])
        self.assertFalse(result["ok"])
        self.assertIn("ftd_coverage_below_minimum", result["issues"])

    def test_clean_population_and_completed_quarter_floors_fail_closed(
        self,
    ) -> None:
        current = self.coverage_master(total=20, evidenced=19)
        result = master.audit_security_master(
            current,
            minimum_current_symbol_population_by_kind={
                "sec_company_tickers": 2,
                "sec_company_exchange_tickers": 2,
                "sec_fund_tickers": 2,
            },
            minimum_current_symbol_title_ratio=0.8,
            minimum_active_official_cusip_count=21,
            enforce_latest_completed_official_period=True,
            as_of=date(2026, 12, 1),
        )
        self.assertFalse(result["ok"])
        self.assertEqual(
            [
                "sec_company_exchange_tickers",
                "sec_company_tickers",
                "sec_fund_tickers",
            ],
            result["below_minimum_symbol_kinds"],
        )
        self.assertIn(
            "current_symbol_source_population_regressed",
            result["issues"],
        )
        self.assertIn("official_13f_population_regressed", result["issues"])
        self.assertIn("official_13f_period_is_stale", result["issues"])

    def test_official_period_grace_passes_day_45_and_fails_day_46(
        self,
    ) -> None:
        current = self.coverage_master(total=20, evidenced=19)
        current["audit"]["official_13f_period"] = "2026Q1"

        day_45 = master.audit_security_master(
            current,
            enforce_latest_completed_official_period=True,
            as_of=date(2026, 8, 14),
        )
        self.assertEqual(
            "2026Q1",
            day_45["expected_latest_completed_official_period"],
        )
        self.assertTrue(day_45["official_period_gate_passed"])
        self.assertTrue(day_45["ok"])

        day_46 = master.audit_security_master(
            current,
            enforce_latest_completed_official_period=True,
            as_of=date(2026, 8, 15),
        )
        self.assertEqual(
            "2026Q2",
            day_46["expected_latest_completed_official_period"],
        )
        self.assertFalse(day_46["official_period_gate_passed"])
        self.assertFalse(day_46["ok"])
        self.assertIn("official_13f_period_is_stale", day_46["issues"])

    def test_schema_two_master_remains_readable_under_legacy_gate(self) -> None:
        legacy = self.coverage_master(total=20, evidenced=19)
        legacy["audit"]["schema_version"] = 2
        legacy["audit"].pop("successful_check_checkpoint_days")
        legacy["audit"].pop("required_current_source_checkpoints")

        master.validate_security_master(legacy)
        result = master.audit_security_master(legacy)
        self.assertFalse(
            result["required_current_source_freshness_available"]
        )
        self.assertTrue(
            result["required_current_source_freshness_gate_passed"]
        )
        self.assertTrue(result["ok"])

    def test_ixbrl_successful_check_passes_day_45_and_fails_day_46(self) -> None:
        current = self.ixbrl_master(checked_at="2026-07-15T00:00:00Z")

        day_45 = master.audit_security_master(
            current,
            as_of=date(2026, 8, 29),
        )
        self.assertEqual(
            45,
            day_45["sec_ixbrl_source_age_days"]["02079K305|EQUITY"],
        )
        self.assertTrue(day_45["sec_ixbrl_source_freshness_gate_passed"])
        self.assertTrue(day_45["ok"])

        day_46 = master.audit_security_master(
            current,
            as_of=date(2026, 8, 30),
        )
        self.assertEqual(
            ["02079K305|EQUITY"],
            day_46["stale_sec_ixbrl_security_keys"],
        )
        self.assertFalse(day_46["sec_ixbrl_source_freshness_gate_passed"])
        self.assertIn("sec_ixbrl_source_is_stale", day_46["issues"])

        interim = master.audit_security_master(
            current,
            as_of=date(2026, 8, 30),
            enforce_sec_ixbrl_freshness=False,
        )
        self.assertEqual(
            ["02079K305|EQUITY"],
            interim["stale_sec_ixbrl_security_keys"],
        )
        self.assertTrue(interim["sec_ixbrl_source_freshness_gate_passed"])
        self.assertTrue(interim["ok"])

    def test_ixbrl_missing_check_fails_but_schema_three_stays_readable(self) -> None:
        current = self.ixbrl_master(checked_at=None)
        result = master.audit_security_master(
            current,
            as_of=date(2026, 8, 29),
        )
        self.assertEqual(
            ["02079K305|EQUITY"],
            result["missing_sec_ixbrl_security_keys"],
        )
        self.assertIn("sec_ixbrl_source_date_unavailable", result["issues"])

        legacy = copy.deepcopy(current)
        legacy["audit"]["schema_version"] = (
            master.CURRENT_SOURCE_MASTER_AUDIT_SCHEMA_VERSION
        )
        legacy["audit"].pop("sec_ixbrl_source_checkpoints")
        master.validate_security_master(legacy)
        legacy_result = master.audit_security_master(
            legacy,
            as_of=date(2026, 8, 29),
        )
        self.assertFalse(legacy_result["sec_ixbrl_source_freshness_available"])
        self.assertTrue(legacy_result["sec_ixbrl_source_freshness_gate_passed"])
        self.assertTrue(legacy_result["ok"])

    def test_unexplained_regression_over_one_point_fails_closed(self) -> None:
        current = self.coverage_master(total=20, evidenced=19)
        prior = copy.deepcopy(current)
        prior["audit"].update({
            "active_non_option_official_cusip_count": 200,
            "ftd_evidenced_official_cusip_count": 194,
            "ftd_coverage_ratio": 0.97,
        })
        current["audit"].update({
            "active_non_option_official_cusip_count": 200,
            "ftd_evidenced_official_cusip_count": 191,
            "ftd_coverage_ratio": 0.955,
        })

        result = master.audit_security_master(current, prior_master=prior)
        self.assertEqual(-1.5, result["coverage_change_percentage_points"])
        self.assertFalse(result["regression_gate_passed"])
        self.assertFalse(result["ok"])
        self.assertIn(
            "unexplained_material_ftd_coverage_regression",
            result["issues"],
        )

        explained = master.audit_security_master(
            current,
            prior_master=prior,
            regression_explanation="SEC added securities before FTD history arrived",
        )
        self.assertTrue(explained["regression_gate_passed"])
        self.assertTrue(explained["ok"])

    def test_staleness_and_schema_change_are_explicit_gates(self) -> None:
        prior = self.coverage_master(total=20, evidenced=19)
        stale = master.audit_security_master(
            prior,
            as_of=date(2026, 10, 4),
        )
        self.assertEqual(47, stale["ftd_source_age_days"])
        self.assertFalse(stale["source_staleness_gate_passed"])
        self.assertIn("ftd_source_is_stale", stale["issues"])

        changed = copy.deepcopy(prior)
        changed["audit"]["source_schema_sha256_by_kind"][
            "sec_ftd_archive"
        ] = "d" * 64
        result = master.audit_security_master(changed, prior_master=prior)
        self.assertEqual(["sec_ftd_archive"], result["schema_changed_kinds"])
        self.assertFalse(result["schema_change_gate_passed"])
        self.assertFalse(result["ok"])

    def test_normalized_record_schema_does_not_depend_on_row_population(
        self,
    ) -> None:
        empty = {
            "kind": "sec_ftd_archive",
            "records": [],
        }
        populated = {
            "kind": "sec_ftd_archive",
            "records": master.compact_ftd_records([
                ftd_record("2026-08-01"),
            ]),
        }
        self.assertEqual(
            master._source_schema_fingerprint(empty),
            master._source_schema_fingerprint(populated),
        )


class PersistenceAndRefreshTests(unittest.TestCase):
    def test_offline_new_identity_extension_preserves_proof_and_defers_new_symbols(self):
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            prior = master.refresh_security_master(
                self.universe(), master_path=root / 'master.json', source_state_path=root / 'state.json',
                fetcher=payloads.__getitem__, now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                recheck_recent_archives=0)
            additions = [{'cusip': '594918104', 'instrument_type': 'EQUITY', 'reported_issuer': 'MICROSOFT CORP', 'reported_class': 'COM'},
                         {'cusip': '037833100', 'instrument_type': 'NOTE', 'reported_issuer': 'APPLE INC', 'reported_class': 'NOTE'}]
            with mock.patch.object(requests.Session, 'request', side_effect=AssertionError('offline extension must not fetch')):
                extended = master._retain_prior_mappings_with_unresolved_extensions(
                    prior.master, prior.state, additions,
                    new_identity_reason='sec_evidence_refresh_pending_new_identity')
            self.assertEqual(prior.master['records']['037833100|EQUITY'], extended['records']['037833100|EQUITY'])
            self.assertEqual('AAPL', extended['records']['037833100|EQUITY']['ticker'])
            for key in ('594918104|EQUITY', '037833100|NOTE'):
                self.assertIsNone(extended['records'][key]['ticker'])
            self.assertEqual('sec_evidence_refresh_pending_new_identity', extended['records']['594918104|EQUITY']['resolution_reason'])
            self.assertEqual(master.project_master_audit(extended, prior.state), extended['audit'])
            master.save_security_master_pair(extended, prior.state, master_path=root / 'master.json', source_state_path=root / 'state.json')
            loaded, source = master.load_security_master_pair(master_path=root / 'master.json', source_state_path=root / 'state.json')
            self.assertEqual(extended, loaded)
            self.assertEqual(prior.state, source)

    def make_payloads(self) -> dict[str, bytes]:
        ftd_page = f'<a href="{FTD_URL}">August 2026 first half</a>'.encode()
        list_page = f'<a href="{LIST_URL}">TXT</a>'.encode()
        return {
            master.SEC_COMPANY_TICKERS_URL: json.dumps({
                "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}
            }).encode(),
            master.SEC_COMPANY_EXCHANGE_TICKERS_URL: json.dumps({
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
            }).encode(),
            master.SEC_FUND_TICKERS_URL: json.dumps({
                "fields": ["cik", "seriesId", "classId", "symbol"],
                "data": [[1, "S1", "C1", "SPY"]],
            }).encode(),
            master.FTD_PAGE_URL: ftd_page,
            FTD_URL: make_ftd_zip([
                ("20260801", "037833100", "AAPL", 100, "APPLE INC", "200"),
                ("20260804", "037833100", "AAPL", 200, "APPLE INC", "202"),
            ]),
            master.OFFICIAL_13F_LIST_PAGE_URL: list_page,
            LIST_URL: make_13f_line("037833100", "APPLE INC", "COM").encode(),
        }

    def universe(self) -> list[dict]:
        return [{
            "cusip": "037833100",
            "instrument_type": "EQUITY",
            "reported_issuer": "APPLE INC",
            "reported_class": "COM",
        }]

    def test_incremental_checkpoints_order_later_first_seen_cusips(self) -> None:
        payloads = self.make_payloads()
        base = "https://www.sec.gov/files/data/fails-deliver-data/"
        archives = [
            ("cnsfails202606a.zip", "20260601", "594918104", "MSFT"),
            ("cnsfails202606b.zip", "20260616", "037833100", "AAPL"),
            ("cnsfails202607a.zip", "20260701", "037833100", "AAPL"),
            ("cnsfails202607b.zip", "20260716", "037833100", "AAPL"),
            ("cnsfails202608a.zip", "20260803", "037833100", "AAPL"),
        ]
        payloads[master.FTD_PAGE_URL] = "".join(
            f'<a href="{base}{name}">{name}</a>'
            for name, _date, _cusip, _symbol in archives
        ).encode()
        for name, settlement_date, cusip, symbol in archives:
            issuer = "MICROSOFT CORP" if symbol == "MSFT" else "APPLE INC"
            payloads[f"{base}{name}"] = make_ftd_zip([
                (settlement_date, cusip, symbol, 100, issuer, "100"),
                (settlement_date, cusip, symbol, 200, issuer, "101"),
            ])
        payloads[master.SEC_COMPANY_TICKERS_URL] = json.dumps({
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {
                "cik_str": 789019,
                "ticker": "MSFT",
                "title": "Microsoft Corporation",
            },
        }).encode()
        payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [320193, "Apple Inc.", "AAPL", "Nasdaq"],
                [789019, "Microsoft Corporation", "MSFT", "Nasdaq"],
            ],
        }).encode()
        universe = [
            *self.universe(),
            {
                "cusip": "594918104",
                "instrument_type": "EQUITY",
                "reported_issuer": "MICROSOFT CORP",
                "reported_class": "COM",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            result = master.refresh_security_master(
                universe,
                master_path=root / "master.json",
                source_state_path=root / "state.json",
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                lookback_months=3,
                recheck_recent_archives=0,
            )

            self.assertFalse(result.errors)
            self.assertEqual(
                ["037833100", "594918104"],
                list(result.state["ftd_timeline"]),
            )
            persisted = master.load_source_state(root / "state.json")
            self.assertEqual(
                list(result.state["ftd_timeline"]),
                list(persisted["ftd_timeline"]),
            )

    def test_refresh_persists_url_hashes_and_is_idempotent(self) -> None:
        payloads = self.make_payloads()
        calls: list[str] = []

        def fetch(url: str) -> bytes:
            calls.append(url)
            return payloads[url]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            first = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=fetch,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            self.assertTrue(first.changed)
            self.assertFalse(first.errors)
            self.assertTrue(first.acceptance["ok"])
            self.assertEqual(7, len(first.state["sources"]))
            for url, entry in first.state["sources"].items():
                self.assertEqual(url, entry["url"])
                self.assertRegex(entry["sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(
                {"AAPL": ["Nasdaq"]},
                first.state["sources"][
                    master.SEC_COMPANY_EXCHANGE_TICKERS_URL
                ]["symbol_exchanges"],
            )
            self.assertEqual(
                "resolved",
                first.master["records"]["037833100|EQUITY"]["mapping_status"],
            )
            self.assertEqual(
                "Nasdaq",
                first.master["records"]["037833100|EQUITY"]["exchange"],
            )
            state_bytes = state_path.read_bytes()
            master_bytes = master_path.read_bytes()

            calls.clear()
            second = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=fetch,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
            )
            self.assertFalse(second.changed)
            self.assertFalse(second.errors)
            self.assertEqual(state_bytes, state_path.read_bytes())
            self.assertEqual(master_bytes, master_path.read_bytes())
            self.assertIn(FTD_URL, calls)  # latest archive is SHA-rechecked

    def test_unchanged_v2_refresh_durably_persists_v3_migration(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            first = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            legacy = copy.deepcopy(first.state)
            legacy["schema_version"] = master.COMPACT_SOURCE_STATE_SCHEMA_VERSION
            legacy["sources"][FTD_URL]["records"] = legacy[
                "ftd_mutable_tail"
            ][FTD_URL]["records"]
            for field in (
                "ftd_filter_cusips",
                "ftd_timeline",
                "ftd_mutable_tail",
                "ftd_processed_filter_universe_sha256",
                "ftd_processed_filter_universe_count",
            ):
                legacy.pop(field, None)
            state_path.write_text(
                json.dumps(legacy, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )

            result = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
            )

            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertTrue(result.changed)
            self.assertEqual(master.SOURCE_STATE_SCHEMA_VERSION, persisted["schema_version"])
            self.assertNotIn("records", persisted["sources"][FTD_URL])
            self.assertIn(FTD_URL, persisted["ftd_mutable_tail"])

    def test_row_free_v3_ftd_source_migrates_and_persists_v4_slot(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            first = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            legacy = copy.deepcopy(first.state)
            legacy["schema_version"] = master.TIMELINE_SOURCE_STATE_SCHEMA_VERSION
            legacy["sources"][FTD_URL].pop("boundary_date_proofs")
            self.assertNotIn("records", legacy["sources"][FTD_URL])
            state_path.write_text(
                json.dumps(legacy, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )

            migrated = master.load_source_state(state_path)
            self.assertEqual(master.SOURCE_STATE_SCHEMA_VERSION, migrated["schema_version"])
            self.assertEqual(
                [],
                migrated["sources"][FTD_URL]["boundary_date_proofs"],
            )

            result = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertTrue(result.changed)
            self.assertEqual(master.SOURCE_STATE_SCHEMA_VERSION, persisted["schema_version"])
            self.assertEqual(
                [],
                persisted["sources"][FTD_URL]["boundary_date_proofs"],
            )

    def test_refresh_refetches_incomplete_2004_boundary_proofs(self) -> None:
        boundary_rows = [
            ("20040401", "037833100", "AAPL", 100, "APPLE INC", "100")
        ]
        q1_payload = make_ftd_zip([
            ("20040322", "037833100", "AAPL", 50, "APPLE INC", "99"),
            *boundary_rows,
        ])
        q2_payload = make_ftd_zip([
            *boundary_rows,
            ("20040503", "037833100", "AAPL", 75, "APPLE INC", "101"),
            ("20040601", "037833100", "AAPL", 80, "APPLE INC", "102"),
        ])
        q1, q2 = compact_2004_boundary_pair(boundary_rows)
        state = source_state_with_2004_boundary(q1, q2)
        prior_master = master.rebuild_security_master(
            state,
            self.universe(),
        )
        payloads = self.make_payloads()
        payloads.update({
            FTD_2004_Q1_URL: q1_payload,
            FTD_2004_Q2_URL: q2_payload,
            master.FTD_PAGE_URL: (
                f'<a href="{FTD_2004_Q1_URL}">2004 Q1</a>'
                f'<a href="{FTD_2004_Q2_URL}">2004 Q2</a>'
                f'<a href="{FTD_URL}">August 2026</a>'
            ).encode(),
        })
        for url, payload in (
            (FTD_2004_Q1_URL, q1_payload),
            (FTD_2004_Q2_URL, q2_payload),
        ):
            source = state["sources"][url]
            source["sha256"] = master._payload_sha256(payload)
            source["date_inventory_complete"] = False
            source["boundary_date_proofs"] = []
            self.assertTrue(
                master._archive_filter_covers(
                    source,
                    state,
                    {"037833100"},
                )
            )
        prior_master["source_state_sha256"] = master.source_state_sha256(state)

        calls: list[str] = []

        def fetch(url: str) -> bytes:
            calls.append(url)
            return payloads[url]

        selected = [FTD_2004_Q1_URL, FTD_2004_Q2_URL, FTD_URL]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.save_security_master_pair(
                prior_master,
                state,
                master_path=master_path,
                source_state_path=state_path,
            )
            with mock.patch.object(
                master,
                "select_recent_ftd_urls",
                return_value=selected,
            ):
                result = master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=fetch,
                    now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                    recheck_recent_archives=0,
                )

        self.assertFalse(result.errors)
        self.assertIn(FTD_2004_Q1_URL, calls)
        self.assertIn(FTD_2004_Q2_URL, calls)
        for url in (FTD_2004_Q1_URL, FTD_2004_Q2_URL):
            source = result.state["sources"][url]
            self.assertTrue(source["date_inventory_complete"])
            self.assertEqual(1, len(source["boundary_date_proofs"]))

    def test_v3_boundary_refetch_does_not_reappend_stable_timeline(self) -> None:
        boundary_rows = [
            ("20040401", "037833100", "AAPL", 100, "APPLE INC", "100")
        ]
        q1_payload = make_ftd_zip([
            ("20040322", "037833100", "AAPL", 50, "APPLE INC", "99"),
            *boundary_rows,
        ])
        q2_payload = make_ftd_zip([
            *boundary_rows,
            ("20040503", "037833100", "AAPL", 75, "APPLE INC", "101"),
            ("20040601", "037833100", "AAPL", 80, "APPLE INC", "102"),
        ])
        q1, q2 = compact_2004_boundary_pair(boundary_rows)
        state = source_state_with_2004_boundary(q1, q2)
        source_payloads = {
            FTD_2004_Q1_URL: q1_payload,
            FTD_2004_Q2_URL: q2_payload,
        }
        for url, parsed in (
            (FTD_2004_Q1_URL, q1),
            (FTD_2004_Q2_URL, q2),
        ):
            digest = master._payload_sha256(source_payloads[url])
            state["sources"][url]["sha256"] = digest
            observations = master._ftd_observations_from_archive_records(
                parsed["compact_records"],
                source_url=url,
                source_sha256=digest,
            )
            master._append_ftd_observations_to_timeline(
                state["ftd_timeline"],
                observations,
            )
            state["sources"][url].pop("boundary_date_proofs")
            state["sources"][url]["filter_all_cusips"] = True
            state["sources"][url].pop("filter_universe_sha256")
            state["sources"][url].pop("filter_universe_count")
        state["schema_version"] = master.TIMELINE_SOURCE_STATE_SCHEMA_VERSION
        state["ftd_processed_all_cusips"] = True
        retained_source_fields = {
            url: {
                field: copy.deepcopy(state["sources"][url].get(field))
                for field in (
                    "record_count",
                    "matched_record_count",
                    "matched_cusip_count",
                    "filter_universe_sha256",
                    "filter_universe_count",
                    "filter_all_cusips",
                )
                if field in state["sources"][url]
            }
            for url in (FTD_2004_Q1_URL, FTD_2004_Q2_URL)
        }

        payloads = self.make_payloads()
        payloads.update(source_payloads)
        payloads[OLD_FTD_URL] = make_ftd_zip([
            ("20260716", "037833100", "AAPL", 100, "APPLE INC", "190"),
        ])
        selected = [
            FTD_2004_Q1_URL,
            FTD_2004_Q2_URL,
            OLD_FTD_URL,
            FTD_URL,
        ]
        payloads[master.FTD_PAGE_URL] = "".join(
            f'<a href="{url}">{url}</a>' for url in selected
        ).encode()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            state_path.write_text(
                json.dumps(state, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            migrated = master.load_source_state(state_path)
            timeline_before = json.dumps(
                migrated["ftd_timeline"],
                sort_keys=True,
                separators=(",", ":"),
            )

            calls: list[str] = []

            def fetch(url: str) -> bytes:
                calls.append(url)
                return payloads[url]

            result = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=fetch,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                recheck_recent_archives=0,
            )

        self.assertFalse(result.errors)
        self.assertEqual(
            timeline_before,
            json.dumps(
                result.state["ftd_timeline"],
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        self.assertIn(FTD_2004_Q1_URL, calls)
        self.assertIn(FTD_2004_Q2_URL, calls)
        for url in (FTD_2004_Q1_URL, FTD_2004_Q2_URL):
            source = result.state["sources"][url]
            self.assertTrue(source["date_inventory_complete"])
            self.assertEqual(1, len(source["boundary_date_proofs"]))
            self.assertEqual(
                retained_source_fields[url],
                {
                    field: source[field]
                    for field in retained_source_fields[url]
                },
            )

    def test_v3_boundary_pair_failure_preserves_exact_prior_files(self) -> None:
        boundary_rows = [
            ("20040401", "037833100", "AAPL", 100, "APPLE INC", "100")
        ]
        q1_payload = make_ftd_zip([
            ("20040322", "037833100", "AAPL", 50, "APPLE INC", "99"),
            *boundary_rows,
        ])
        q2_payload = make_ftd_zip([
            *boundary_rows,
            ("20040503", "037833100", "AAPL", 75, "APPLE INC", "101"),
            ("20040601", "037833100", "AAPL", 80, "APPLE INC", "102"),
        ])
        q1, q2 = compact_2004_boundary_pair(boundary_rows)
        state = source_state_with_2004_boundary(q1, q2)
        for url, payload, parsed in (
            (FTD_2004_Q1_URL, q1_payload, q1),
            (FTD_2004_Q2_URL, q2_payload, q2),
        ):
            digest = master._payload_sha256(payload)
            state["sources"][url]["sha256"] = digest
            observations = master._ftd_observations_from_archive_records(
                parsed["compact_records"],
                source_url=url,
                source_sha256=digest,
            )
            master._append_ftd_observations_to_timeline(
                state["ftd_timeline"],
                observations,
            )
            state["sources"][url].pop("boundary_date_proofs")
        state["schema_version"] = master.TIMELINE_SOURCE_STATE_SCHEMA_VERSION

        payloads = self.make_payloads()
        payloads[FTD_2004_Q1_URL] = q1_payload
        payloads[FTD_2004_Q2_URL] = b"not a ZIP archive"
        payloads[master.FTD_PAGE_URL] = (
            f'<a href="{FTD_2004_Q1_URL}">2004 Q1</a>'
            f'<a href="{FTD_2004_Q2_URL}">2004 Q2</a>'
            f'<a href="{FTD_URL}">August 2026</a>'
        ).encode()
        calls: list[str] = []

        def fetch(url: str) -> bytes:
            calls.append(url)
            return payloads[url]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            state_path.write_text(
                json.dumps(state, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            master.save_security_master(
                master.load_security_master(master_path),
                master_path,
            )
            state_bytes = state_path.read_bytes()
            master_bytes = master_path.read_bytes()

            with self.assertRaises(master.SourceSchemaChangeError):
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=fetch,
                    now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                    recheck_recent_archives=0,
                )

            self.assertEqual(state_bytes, state_path.read_bytes())
            self.assertEqual(master_bytes, master_path.read_bytes())
        self.assertLess(
            calls.index(FTD_2004_Q1_URL),
            calls.index(FTD_2004_Q2_URL),
        )

    def test_changed_latest_tail_archive_is_replaced_without_full_rebuild(
        self,
    ) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            first = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            prior_sha256 = first.state["sources"][FTD_URL]["sha256"]

            payloads[FTD_URL] = make_ftd_zip([
                ("20260801", "037833100", "AAPX", 100, "APPLE INC", "200"),
                ("20260804", "037833100", "AAPX", 200, "APPLE INC", "202"),
            ])
            payloads[master.SEC_COMPANY_TICKERS_URL] = json.dumps({
                "0": {"ticker": "AAPX", "title": "Apple Inc."}
            }).encode()
            payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPX", "Nasdaq"]],
            }).encode()

            second = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
            )

            self.assertNotEqual(
                prior_sha256, second.state["sources"][FTD_URL]["sha256"]
            )
            self.assertEqual(
                "AAPX",
                second.master["records"]["037833100|EQUITY"]["ticker"],
            )
            self.assertEqual(
                {"AAPX"},
                {
                    row["symbol"]
                    for row in second.state["ftd_mutable_tail"][FTD_URL][
                        "records"
                    ]
                },
            )
            self.assertEqual({}, second.state["ftd_timeline"])

    def test_breaking_json_schema_change_is_fatal_and_preserves_lkg(
        self,
    ) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            master_bytes = master_path.read_bytes()
            state_bytes = state_path.read_bytes()
            payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
                "fields": ["cik", "name", "securitySymbol", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
            }).encode()

            with self.assertRaises(master.SourceSchemaChangeError) as raised:
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=payloads.__getitem__,
                    now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                )

            self.assertIn("sec_company_exchange_tickers", str(raised.exception))
            self.assertIn(
                master.SEC_COMPANY_EXCHANGE_TICKERS_URL,
                str(raised.exception),
            )
            self.assertEqual(master_bytes, master_path.read_bytes())
            self.assertEqual(state_bytes, state_path.read_bytes())

    def test_breaking_ftd_header_is_fatal_but_corrupt_zip_is_retriable(
        self,
    ) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            master_bytes = master_path.read_bytes()
            state_bytes = state_path.read_bytes()

            renamed_header = io.BytesIO()
            with zipfile.ZipFile(
                renamed_header,
                "w",
                zipfile.ZIP_DEFLATED,
            ) as archive:
                archive.writestr(
                    "cnsfails.txt",
                    b"DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE\n"
                    b"20260801|037833100|AAPL|100|APPLE INC|200\n",
                )
            payloads[FTD_URL] = renamed_header.getvalue()
            with self.assertRaises(master.SourceSchemaChangeError):
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=payloads.__getitem__,
                    now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                )
            self.assertEqual(master_bytes, master_path.read_bytes())
            self.assertEqual(state_bytes, state_path.read_bytes())

            payloads[FTD_URL] = b"not a zip"
            result = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
            )
            self.assertTrue(
                any("invalid FTD ZIP" in error for error in result.errors)
            )
            self.assertEqual(master_bytes, master_path.read_bytes())
            self.assertEqual(state_bytes, state_path.read_bytes())

    def test_additive_json_field_does_not_trigger_schema_alert(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
                "fields": ["exchange", "ticker", "name", "cik", "sector"],
                "data": [["Nasdaq", "AAPL", "Apple Inc.", 320193, "Tech"]],
            }).encode()

            result = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
            )
            self.assertFalse(result.errors)
            self.assertTrue(result.acceptance["schema_change_gate_passed"])

    def test_unchanged_sources_refresh_checkpoint_without_daily_churn(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=lambda url: payloads[url],
                now=datetime(2026, 8, 5, 12, tzinfo=timezone.utc),
            )

            checkpointed = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=lambda url: payloads[url],
                now=datetime(2026, 9, 4, 12, tzinfo=timezone.utc),
            )
            self.assertTrue(checkpointed.changed)
            required_urls = {
                master.SEC_COMPANY_TICKERS_URL,
                master.SEC_COMPANY_EXCHANGE_TICKERS_URL,
                master.SEC_FUND_TICKERS_URL,
                master.FTD_PAGE_URL,
                master.OFFICIAL_13F_LIST_PAGE_URL,
                LIST_URL,
            }
            for url in required_urls:
                self.assertEqual(
                    "2026-09-04T12:00:00Z",
                    checkpointed.state["sources"][url][
                        "last_successful_check_at"
                    ],
                )
            state_bytes = state_path.read_bytes()
            master_bytes = master_path.read_bytes()

            next_day = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=lambda url: payloads[url],
                now=datetime(2026, 9, 5, 12, tzinfo=timezone.utc),
            )
            self.assertFalse(next_day.changed)
            self.assertEqual(state_bytes, state_path.read_bytes())
            self.assertEqual(master_bytes, master_path.read_bytes())

    def test_old_failed_metadata_source_blocks_publication(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=lambda url: payloads[url],
                now=datetime(2026, 8, 5, 12, tzinfo=timezone.utc),
            )
            master_bytes = master_path.read_bytes()
            september_url = (
                "https://www.sec.gov/files/data/fails-deliver-data/"
                "cnsfails202609b.zip"
            )
            payloads[master.FTD_PAGE_URL] = (
                f'<a href="{FTD_URL}">August</a>'
                f'<a href="{september_url}">September</a>'
            ).encode()
            payloads[september_url] = make_ftd_zip([
                ("20260918", "037833100", "AAPL", 100, "APPLE INC", "300"),
                ("20260919", "037833100", "AAPL", 200, "APPLE INC", "301"),
            ])

            def fetch(url: str) -> bytes:
                if url == master.SEC_COMPANY_TICKERS_URL:
                    raise ConnectionError("SEC metadata unavailable")
                return payloads[url]

            with self.assertRaises(
                master.SecurityMasterAcceptanceError
            ) as raised:
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=fetch,
                    now=datetime(2026, 9, 21, 12, tzinfo=timezone.utc),
                )

            audit = raised.exception.audit
            self.assertIn(
                "required_current_sec_source_is_stale",
                audit["issues"],
            )
            self.assertEqual(
                {
                    "sec_13f_list",
                    "sec_13f_list_index",
                    "sec_company_exchange_tickers",
                    "sec_company_tickers",
                    "sec_ftd_index",
                    "sec_fund_tickers",
                },
                set(audit["stale_required_current_source_kinds"]),
            )
            self.assertFalse(audit["source_staleness_gate_passed"])
            self.assertIn("ftd_source_is_stale", audit["issues"])
            self.assertFalse(
                audit["required_current_source_freshness_gate_passed"]
            )
            self.assertEqual(master_bytes, master_path.read_bytes())
            retained = master.load_source_state(state_path)["sources"][
                master.SEC_COMPANY_TICKERS_URL
            ]
            self.assertEqual(
                "2026-08-05T12:00:00Z",
                retained["last_successful_check_at"],
            )

    def test_failed_coverage_gate_keeps_master_and_accepts_raw_state(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=lambda url: payloads[url],
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            master_bytes = master_path.read_bytes()
            state_bytes = state_path.read_bytes()

            expanded_rows = [
                make_13f_line("037833100", "APPLE INC", "COM")
            ] + [
                make_13f_line(
                    numbered_cusip(index),
                    f"ISSUER {index}",
                    "COM",
                )
                for index in range(1, 20)
            ]
            payloads[LIST_URL] = "\n".join(expanded_rows).encode()
            with self.assertRaises(
                master.SecurityMasterAcceptanceError
            ) as raised:
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=lambda url: payloads[url],
                    now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                )

            self.assertEqual(
                "sec_security_master_acceptance_failed",
                raised.exception.error_code,
            )
            self.assertIn(
                "ftd_coverage_below_minimum",
                raised.exception.audit["issues"],
            )
            self.assertEqual(master_bytes, master_path.read_bytes())
            self.assertEqual(state_bytes, state_path.read_bytes())
            self.assertEqual(
                1,
                master.load_source_state(state_path)["sources"][LIST_URL][
                    "record_count"
                ],
            )

    def test_valid_truncated_symbol_feed_cannot_withdraw_aapl(self) -> None:
        payloads = self.make_payloads()
        payloads[master.SEC_COMPANY_TICKERS_URL] = json.dumps({
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft"},
        }).encode()
        payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [320193, "Apple Inc.", "AAPL", "Nasdaq"],
                [789019, "Microsoft", "MSFT", "Nasdaq"],
            ],
        }).encode()
        policy = {
            "minimum_current_symbol_population_by_kind": {
                "sec_company_tickers": 1,
                "sec_company_exchange_tickers": 1,
                "sec_fund_tickers": 1,
            },
            "minimum_current_symbol_title_ratio": 0.0,
            "minimum_active_official_cusip_count": 1,
            "enforce_latest_completed_official_period": True,
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            first = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                **policy,
            )
            self.assertEqual(1.0, first.acceptance["ftd_coverage_ratio"])
            state_bytes = state_path.read_bytes()
            master_bytes = master_path.read_bytes()

            payloads[master.SEC_COMPANY_TICKERS_URL] = json.dumps({
                "0": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft"}
            }).encode()
            payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[789019, "Microsoft", "MSFT", "Nasdaq"]],
            }).encode()

            with self.assertRaises(master.SecurityMasterAcceptanceError) as raised:
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=payloads.__getitem__,
                    now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                    **policy,
                )

            self.assertEqual(1.0, raised.exception.audit["ftd_coverage_ratio"])
            self.assertIn(
                "current_symbol_source_population_regressed",
                raised.exception.audit["issues"],
            )
            self.assertIn(
                "resolved_mapping_population_regressed",
                raised.exception.audit["issues"],
            )
            self.assertEqual(state_bytes, state_path.read_bytes())
            self.assertEqual(master_bytes, master_path.read_bytes())

    def test_failed_freshness_gate_keeps_byte_identical_last_good(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=lambda url: payloads[url],
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            master_bytes = master_path.read_bytes()
            state_bytes = state_path.read_bytes()

            with self.assertRaises(
                master.SecurityMasterAcceptanceError
            ) as raised:
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=lambda url: payloads[url],
                    now=datetime(2026, 10, 4, 12, tzinfo=timezone.utc),
                )

            self.assertIn(
                "ftd_source_is_stale",
                raised.exception.audit["issues"],
            )
            self.assertEqual(master_bytes, master_path.read_bytes())
            self.assertEqual(state_bytes, state_path.read_bytes())

    def test_new_repo_cusip_reprocesses_only_the_rolling_window(self) -> None:
        payloads = self.make_payloads()
        payloads[master.FTD_PAGE_URL] = (
            f'<a href="{OLD_FTD_URL}">July</a>'
            f'<a href="{FTD_URL}">August</a>'
        ).encode()
        mixed_rows = [
            ("20260801", "037833100", "AAPL", 100, "APPLE INC", "200"),
            ("20260804", "037833100", "AAPL", 200, "APPLE INC", "202"),
            (
                "20260801",
                "594918104",
                "MSFT",
                100,
                "MICROSOFT CORP",
                "500",
            ),
            (
                "20260804",
                "594918104",
                "MSFT",
                200,
                "MICROSOFT CORP",
                "501",
            ),
        ]
        payloads[FTD_URL] = make_ftd_zip(mixed_rows)
        payloads[OLD_FTD_URL] = make_ftd_zip([
            ("20260716", "037833100", "AAPL", 100, "APPLE INC", "190"),
            (
                "20260716",
                "594918104",
                "MSFT",
                100,
                "MICROSOFT CORP",
                "490",
            ),
        ])
        payloads[master.SEC_COMPANY_TICKERS_URL] = json.dumps({
            "0": {"ticker": "AAPL", "title": "Apple Inc."},
            "1": {"ticker": "MSFT", "title": "Microsoft Corporation"},
        }).encode()
        payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [320193, "Apple Inc.", "AAPL", "Nasdaq"],
                [789019, "Microsoft Corporation", "MSFT", "Nasdaq"],
            ],
        }).encode()

        microsoft = {
            "cusip": "594918104",
            "instrument_type": "EQUITY",
            "reported_issuer": "MICROSOFT CORP",
            "reported_class": "COM",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            first = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=lambda url: payloads[url],
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                lookback_months=2,
                recheck_recent_archives=0,
            )
            self.assertIn(OLD_FTD_URL, first.state["sources"])
            for url in (OLD_FTD_URL, FTD_URL):
                self.assertEqual(
                    {"037833100"},
                    {
                        record["cusip"]
                        for record in first.state["ftd_mutable_tail"][url][
                            "records"
                        ]
                    },
                )

            calls: list[str] = []

            def fetch(url: str) -> bytes:
                calls.append(url)
                return payloads[url]

            second = master.refresh_security_master(
                [*self.universe(), microsoft],
                master_path=master_path,
                source_state_path=state_path,
                fetcher=fetch,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                lookback_months=1,
                recheck_recent_archives=0,
            )
            self.assertIn(FTD_URL, calls)
            self.assertNotIn(OLD_FTD_URL, calls)
            self.assertEqual(
                {"037833100", "594918104"},
                master._archive_filter_universe(
                    second.state["sources"][FTD_URL], second.state
                ),
            )
            self.assertEqual(
                {"037833100"},
                master._archive_filter_universe(
                    second.state["sources"][OLD_FTD_URL], second.state
                ),
            )
            self.assertEqual(
                "MSFT",
                second.master["records"]["594918104|EQUITY"]["ticker"],
            )
            self.assertTrue(second.acceptance["filter_universe_gate_passed"])

    def test_out_of_sort_target_expansions_use_one_append_only_filter_log(
        self,
    ) -> None:
        payloads = self.make_payloads()
        payloads[FTD_URL] = make_ftd_zip([
            ("20260801", "037833100", "AAPL", 100, "APPLE INC", "200"),
            ("20260804", "037833100", "AAPL", 200, "APPLE INC", "202"),
            ("20260801", "594918104", "MSFT", 100, "MICROSOFT CORP", "500"),
            ("20260804", "594918104", "MSFT", 200, "MICROSOFT CORP", "501"),
            ("20260801", "02079K305", "GOOGL", 100, "ALPHABET INC", "180"),
            ("20260804", "02079K305", "GOOGL", 200, "ALPHABET INC", "181"),
        ])
        payloads[master.SEC_COMPANY_TICKERS_URL] = json.dumps({
            "0": {"ticker": "AAPL", "title": "Apple Inc."},
            "1": {"ticker": "MSFT", "title": "Microsoft Corporation"},
            "2": {"ticker": "GOOGL", "title": "Alphabet Inc."},
        }).encode()
        payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [320193, "Apple Inc.", "AAPL", "Nasdaq"],
                [789019, "Microsoft Corporation", "MSFT", "Nasdaq"],
                [1652044, "Alphabet Inc.", "GOOGL", "Nasdaq"],
            ],
        }).encode()
        microsoft = {
            "cusip": "594918104",
            "instrument_type": "EQUITY",
            "reported_issuer": "MICROSOFT CORP",
            "reported_class": "COM",
        }
        alphabet = {
            "cusip": "02079K305",
            "instrument_type": "EQUITY",
            "reported_issuer": "ALPHABET INC",
            "reported_class": "CL A",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            universes = [
                self.universe(),
                [*self.universe(), microsoft],
                [*self.universe(), microsoft, alphabet],
            ]
            for offset, universe in enumerate(universes):
                master.refresh_security_master(
                    universe,
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=payloads.__getitem__,
                    now=datetime(2026, 8, 20 + offset, 12, tzinfo=timezone.utc),
                    lookback_months=1,
                    recheck_recent_archives=0,
                )

            state = master.load_source_state(state_path)
            self.assertEqual(
                ["037833100", "594918104", "02079K305"],
                state["ftd_filter_cusips"],
            )
            self.assertLessEqual(len(state["filter_universes"]), 2)
            self.assertEqual(
                set(state["ftd_filter_cusips"]),
                master._archive_filter_universe(
                    state["sources"][FTD_URL], state
                ),
            )
            self.assertNotIn("records", state["sources"][FTD_URL])

            calls: list[str] = []

            def fetch(url: str) -> bytes:
                calls.append(url)
                return payloads[url]

            master.refresh_security_master(
                universes[-1],
                master_path=master_path,
                source_state_path=state_path,
                fetcher=fetch,
                now=datetime(2026, 8, 23, 12, tzinfo=timezone.utc),
                lookback_months=1,
                recheck_recent_archives=0,
            )
            self.assertNotIn(FTD_URL, calls)

    def test_interrupted_incremental_refresh_never_publishes_state_only(self) -> None:
        payloads = self.make_payloads()
        payloads[master.FTD_PAGE_URL] = (
            f'<a href="{OLD_FTD_URL}">July</a>'
            f'<a href="{FTD_URL}">August</a>'
        ).encode()
        payloads[OLD_FTD_URL] = make_ftd_zip([
            ("20260716", "037833100", "AAPL", 100, "APPLE INC", "190"),
        ])
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            interrupted_calls: list[str] = []

            def interrupting_fetch(url: str) -> bytes:
                interrupted_calls.append(url)
                if url == FTD_URL:
                    raise KeyboardInterrupt("runner terminated")
                return payloads[url]

            with self.assertRaises(KeyboardInterrupt):
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=interrupting_fetch,
                    now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                    lookback_months=2,
                    recheck_recent_archives=0,
                )

            checkpoint = master.load_source_state(state_path)
            self.assertEqual({}, checkpoint["sources"])
            self.assertFalse(master_path.exists())
            self.assertEqual([], checkpoint["required_filter_coverage_urls"])

            resumed_calls: list[str] = []

            def resumed_fetch(url: str) -> bytes:
                resumed_calls.append(url)
                return payloads[url]

            result = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=resumed_fetch,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                lookback_months=2,
                recheck_recent_archives=0,
            )

            self.assertIn(OLD_FTD_URL, resumed_calls)
            self.assertIn(FTD_URL, resumed_calls)
            self.assertIn(FTD_URL, result.state["sources"])
            self.assertTrue(result.acceptance["ok"])
            self.assertEqual(
                "AAPL",
                result.master["records"]["037833100|EQUITY"]["ticker"],
            )

    def test_partial_stable_archive_append_rolls_back_and_resumes(self) -> None:
        stable_first_url = (
            "https://www.sec.gov/files/data/fails-deliver-data/"
            "cnsfails202604b.zip"
        )
        stable_second_url = (
            "https://www.sec.gov/files/data/fails-deliver-data/"
            "cnsfails202605a.zip"
        )
        tail_first_url = (
            "https://www.sec.gov/files/data/fails-deliver-data/"
            "cnsfails202605b.zip"
        )
        tail_second_url = (
            "https://www.sec.gov/files/data/fails-deliver-data/"
            "cnsfails202606a.zip"
        )
        archive_urls = [
            stable_first_url,
            stable_second_url,
            tail_first_url,
            tail_second_url,
        ]
        payloads = self.make_payloads()
        payloads[master.FTD_PAGE_URL] = "".join(
            f'<a href="{url}">archive</a>' for url in reversed(archive_urls)
        ).encode()
        payloads[stable_first_url] = make_ftd_zip([
            ("20260416", "037833100", "AAPL", 100, "APPLE INC", "190"),
        ])
        payloads[stable_second_url] = make_ftd_zip([
            ("20260501", "037833100", "AAPL", 100, "APPLE INC", "195"),
            (
                "20260501",
                "02079K305",
                "GOOGL",
                100,
                "ALPHABET INC",
                "170",
            ),
            (
                "20260501",
                "594918104",
                "MSFT",
                100,
                "MICROSOFT CORP",
                "495",
            ),
        ])
        payloads[tail_first_url] = make_ftd_zip([
            (
                "20260516",
                "594918104",
                "MSFT",
                100,
                "MICROSOFT CORP",
                "500",
            ),
        ])
        payloads[tail_second_url] = make_ftd_zip([
            ("20260601", "037833100", "AAPL", 100, "APPLE INC", "200"),
        ])
        payloads[master.SEC_COMPANY_TICKERS_URL] = json.dumps({
            "0": {"ticker": "AAPL", "title": "Apple Inc."},
            "1": {"ticker": "GOOGL", "title": "Alphabet Inc."},
            "2": {"ticker": "MSFT", "title": "Microsoft Corporation"},
        }).encode()
        payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [320193, "Apple Inc.", "AAPL", "Nasdaq"],
                [1652044, "Alphabet Inc.", "GOOGL", "Nasdaq"],
                [789019, "Microsoft Corporation", "MSFT", "Nasdaq"],
            ],
        }).encode()
        universe = [
            *self.universe(),
            {
                "cusip": "594918104",
                "instrument_type": "EQUITY",
                "reported_issuer": "MICROSOFT CORP",
                "reported_class": "COM",
            },
            {
                "cusip": "02079K305",
                "instrument_type": "EQUITY",
                "reported_issuer": "ALPHABET INC",
                "reported_class": "CL A",
            },
        ]
        original_append = master._append_ftd_observations_to_timeline
        append_calls = 0

        def fail_during_second_stable_archive(
            timeline: dict[str, list[dict]],
            observations_by_cusip: dict[str, list[dict]],
        ) -> None:
            nonlocal append_calls
            append_calls += 1
            if append_calls != 2:
                original_append(timeline, observations_by_cusip)
                return
            partial_cusips = sorted(observations_by_cusip)[:2]
            original_append(
                timeline,
                {
                    cusip: observations_by_cusip[cusip]
                    for cusip in partial_cusips
                },
            )
            raise RuntimeError("injected partial stable-archive append")

        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.object(
            master,
            "_validate_ftd_archive_discovery",
            side_effect=lambda urls, **_kwargs: list(urls),
        ):
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            with mock.patch.object(
                master,
                "_append_ftd_observations_to_timeline",
                side_effect=fail_during_second_stable_archive,
            ), self.assertRaisesRegex(
                master.SecurityMasterError,
                "injected partial stable-archive append",
            ):
                master.refresh_security_master(
                    universe,
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=payloads.__getitem__,
                    now=datetime(2026, 6, 20, 12, tzinfo=timezone.utc),
                    lookback_months=None,
                    recheck_recent_archives=0,
                )

            checkpoint = master.load_source_state(state_path)
            checkpoint_archives = [
                url
                for url, source in checkpoint["sources"].items()
                if source["kind"] == "sec_ftd_archive"
            ]
            self.assertEqual([stable_first_url], checkpoint_archives)
            self.assertEqual(
                archive_urls,
                checkpoint["required_filter_coverage_urls"],
            )
            self.assertEqual(["037833100"], list(checkpoint["ftd_timeline"]))
            self.assertEqual(
                "2026-04-16",
                checkpoint["ftd_timeline"]["037833100"][0]["last_seen"],
            )
            self.assertEqual({}, checkpoint["ftd_mutable_tail"])
            self.assertFalse(master_path.exists())

            resumed_calls: list[str] = []

            def resumed_fetch(url: str) -> bytes:
                resumed_calls.append(url)
                return payloads[url]

            result = master.refresh_security_master(
                universe,
                master_path=master_path,
                source_state_path=state_path,
                fetcher=resumed_fetch,
                now=datetime(2026, 6, 20, 12, tzinfo=timezone.utc),
                lookback_months=None,
                recheck_recent_archives=0,
            )

            self.assertNotIn(stable_first_url, resumed_calls)
            self.assertTrue(
                set(archive_urls[1:]).issubset(resumed_calls),
            )
            final_archive_urls = [
                url
                for url, source in result.state["sources"].items()
                if source["kind"] == "sec_ftd_archive"
            ]
            self.assertEqual(archive_urls, final_archive_urls)
            self.assertEqual(
                ["02079K305", "037833100", "594918104"],
                list(result.state["ftd_timeline"]),
            )
            self.assertEqual(
                [tail_first_url, tail_second_url],
                list(result.state["ftd_mutable_tail"]),
            )
            self.assertTrue(master_path.exists())
            self.assertTrue(result.acceptance["ok"])
            self.assertRegex(master.source_state_sha256(result.state), r"^[0-9a-f]{64}$")
            master.validate_security_master(result.master)

    def test_failed_older_archive_never_checkpoints_a_later_interval(
        self,
    ) -> None:
        june_url = (
            "https://www.sec.gov/files/data/fails-deliver-data/"
            "cnsfails202606b.zip"
        )
        payloads = self.make_payloads()
        payloads[master.FTD_PAGE_URL] = (
            f'<a href="{june_url}">June</a>'
            f'<a href="{OLD_FTD_URL}">July</a>'
            f'<a href="{FTD_URL}">August</a>'
        ).encode()
        payloads[june_url] = make_ftd_zip([
            ("20260616", "037833100", "AAPL", 100, "APPLE INC", "180"),
        ])
        payloads[OLD_FTD_URL] = make_ftd_zip([
            ("20260716", "037833100", "AAPL", 100, "APPLE INC", "190"),
        ])

        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.object(
            master,
            "_validate_ftd_archive_discovery",
            side_effect=lambda urls, **_kwargs: list(urls),
        ):
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            failed_calls: list[str] = []

            def fail_middle(url: str) -> bytes:
                failed_calls.append(url)
                if url == OLD_FTD_URL:
                    raise ConnectionError("temporary archive outage")
                return payloads[url]

            with self.assertRaises(master.SecurityMasterError):
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=fail_middle,
                    now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                    lookback_months=None,
                    recheck_recent_archives=0,
                )

            self.assertIn(june_url, failed_calls)
            self.assertIn(OLD_FTD_URL, failed_calls)
            self.assertNotIn(FTD_URL, failed_calls)
            checkpoint = master.load_source_state(state_path)
            self.assertIn(june_url, checkpoint["sources"])
            self.assertNotIn(OLD_FTD_URL, checkpoint["sources"])
            self.assertNotIn(FTD_URL, checkpoint["sources"])

            resumed_calls: list[str] = []

            def resume(url: str) -> bytes:
                resumed_calls.append(url)
                return payloads[url]

            result = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=resume,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                lookback_months=None,
                recheck_recent_archives=0,
            )

            self.assertNotIn(june_url, resumed_calls)
            self.assertIn(OLD_FTD_URL, resumed_calls)
            self.assertIn(FTD_URL, resumed_calls)
            self.assertTrue(result.acceptance["ok"])

    def test_filter_gap_fetch_failure_preserves_last_good_master(self) -> None:
        payloads = self.make_payloads()
        microsoft = {
            "cusip": "594918104",
            "instrument_type": "EQUITY",
            "reported_issuer": "MICROSOFT CORP",
            "reported_class": "COM",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                recheck_recent_archives=0,
            )
            last_good_master = master_path.read_bytes()
            prior_state = state_path.read_bytes()

            def failed_gap_fetch(url: str) -> bytes:
                if url == FTD_URL:
                    raise requests.ConnectionError("archive unavailable")
                return payloads[url]

            result = master.refresh_security_master(
                [*self.universe(), microsoft],
                master_path=master_path,
                source_state_path=state_path,
                fetcher=failed_gap_fetch,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                recheck_recent_archives=0,
            )

            self.assertTrue(result.errors)
            self.assertEqual(prior_state, state_path.read_bytes())
            self.assertNotEqual(last_good_master, master_path.read_bytes())
            self.assertEqual(
                "resolved",
                result.master["records"]["037833100|EQUITY"][
                    "mapping_status"
                ],
            )
            deferred = result.master["records"]["594918104|EQUITY"]
            self.assertEqual("unresolved", deferred["mapping_status"])
            self.assertIsNone(deferred["ticker"])
            self.assertEqual(
                "sec_source_refresh_failed_new_identity_deferred",
                deferred["resolution_reason"],
            )

    def test_new_official_list_failure_rolls_back_mixed_source_refresh(self) -> None:
        payloads = self.make_payloads()
        microsoft = {
            "cusip": "594918104",
            "instrument_type": "EQUITY",
            "reported_issuer": "MICROSOFT CORP",
            "reported_class": "COM",
        }
        next_list_url = (
            "https://www.sec.gov/files/investment/13flist2026q3-txt.txt"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            first = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                recheck_recent_archives=0,
            )
            prior_apple = copy.deepcopy(
                first.master["records"]["037833100|EQUITY"]
            )
            prior_state_bytes = state_path.read_bytes()

            payloads[master.OFFICIAL_13F_LIST_PAGE_URL] = (
                f'<a href="{next_list_url}">2026 Q3</a>'
            ).encode()
            payloads[master.SEC_COMPANY_TICKERS_URL] = json.dumps({
                "0": {"ticker": "AAPL", "title": "Apple Inc."},
                "1": {
                    "ticker": "MSFT",
                    "title": "Microsoft Corporation",
                },
            }).encode()
            payloads[master.SEC_COMPANY_EXCHANGE_TICKERS_URL] = json.dumps({
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [
                    [320193, "Apple Inc.", "AAPL", "Nasdaq"],
                    [789019, "Microsoft Corporation", "MSFT", "Nasdaq"],
                ],
            }).encode()
            payloads[FTD_URL] = make_ftd_zip([
                ("20260801", "037833100", "AAPL", 100, "APPLE INC", "200"),
                ("20260804", "037833100", "AAPL", 200, "APPLE INC", "202"),
                (
                    "20260801",
                    "594918104",
                    "MSFT",
                    100,
                    "MICROSOFT CORP",
                    "500",
                ),
                (
                    "20260804",
                    "594918104",
                    "MSFT",
                    200,
                    "MICROSOFT CORP",
                    "501",
                ),
            ])

            def fetch(url: str) -> bytes:
                if url == next_list_url:
                    raise requests.ConnectionError(
                        "new quarterly list temporarily unavailable"
                    )
                return payloads[url]

            result = master.refresh_security_master(
                [*self.universe(), microsoft],
                master_path=master_path,
                source_state_path=state_path,
                fetcher=fetch,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                recheck_recent_archives=2,
            )

            self.assertTrue(
                any(next_list_url in error for error in result.errors)
            )
            self.assertEqual(prior_state_bytes, state_path.read_bytes())
            self.assertEqual(
                prior_apple,
                result.master["records"]["037833100|EQUITY"],
            )
            microsoft_record = result.master["records"]["594918104|EQUITY"]
            self.assertEqual("unresolved", microsoft_record["mapping_status"])
            self.assertIsNone(microsoft_record["ticker"])
            self.assertEqual(
                "sec_source_refresh_failed_new_identity_deferred",
                microsoft_record["resolution_reason"],
            )
            self.assertNotIn(next_list_url, result.state["sources"])

    def test_full_rebuild_source_failure_never_replaces_master(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            prior_master_bytes = master_path.read_bytes()

            def fetch(url: str) -> bytes:
                if url == master.SEC_COMPANY_TICKERS_URL:
                    raise requests.ConnectionError("metadata unavailable")
                return payloads[url]

            with self.assertRaisesRegex(
                master.SecurityMasterError,
                "full SEC security-master rebuild had source failures",
            ):
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=fetch,
                    now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                    lookback_months=None,
                )

            self.assertEqual(prior_master_bytes, master_path.read_bytes())

    def test_refresh_failure_retains_byte_identical_last_good(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=lambda url: payloads[url],
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
            )
            state_bytes = state_path.read_bytes()
            master_bytes = master_path.read_bytes()

            def unavailable(_url: str) -> bytes:
                raise requests.ConnectionError("temporary outage")

            result = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=unavailable,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
            )
            self.assertFalse(result.changed)
            self.assertTrue(result.errors)
            self.assertEqual(state_bytes, state_path.read_bytes())
            self.assertEqual(master_bytes, master_path.read_bytes())
            self.assertEqual(
                "resolved",
                result.master["records"]["037833100|EQUITY"]["mapping_status"],
            )

    def test_disappeared_ftd_link_retains_byte_identical_last_good(self) -> None:
        payloads = self.make_payloads()
        payloads[master.FTD_PAGE_URL] = (
            f'<a href="{OLD_FTD_URL}">July</a>'
            f'<a href="{FTD_URL}">August</a>'
        ).encode()
        payloads[OLD_FTD_URL] = make_ftd_zip([
            ("20260716", "037833100", "AAPL", 100, "APPLE INC", "190"),
        ])
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                lookback_months=2,
                recheck_recent_archives=0,
            )
            prior_state = state_path.read_bytes()
            prior_master = master_path.read_bytes()
            payloads[master.FTD_PAGE_URL] = (
                f'<a href="{FTD_URL}">August</a>'
            ).encode()

            result = master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                lookback_months=2,
                recheck_recent_archives=0,
            )
            final_state = state_path.read_bytes()
            final_master = master_path.read_bytes()

        self.assertTrue(
            any("previously discovered" in error for error in result.errors)
        )
        self.assertEqual(prior_state, final_state)
        self.assertEqual(prior_master, final_master)

    def test_atomic_load_save_round_trip(self) -> None:
        state = source_state(rows=[], symbols=["AAPL"])
        built = master.rebuild_security_master(state, {"037833100": "EQUITY"})
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            master_path = root / "master.json"
            master.save_source_state(state, state_path)
            master.save_security_master(built, master_path)
            self.assertEqual(state, master.load_source_state(state_path))
            self.assertEqual(built, master.load_security_master(master_path))
            self.assertEqual([], list(root.glob("*.tmp")))

    def test_atomic_source_state_write_cleans_up_after_interrupt(self) -> None:
        state = source_state(rows=[], symbols=["AAPL"])
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            master.save_source_state(state, state_path)
            before = state_path.read_bytes()

            with mock.patch.object(
                master.json,
                "dump",
                side_effect=KeyboardInterrupt("runner terminated"),
            ), self.assertRaises(KeyboardInterrupt):
                master.save_source_state(state, state_path)

            self.assertEqual(before, state_path.read_bytes())
            self.assertEqual(
                [],
                list(root.glob(f".{state_path.name}.*.tmp")),
            )

    def test_master_write_interrupt_restores_prior_source_state(self) -> None:
        payloads = self.make_payloads()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_path = root / "state.json"
            master_path = root / "master.json"
            master.refresh_security_master(
                self.universe(),
                master_path=master_path,
                source_state_path=state_path,
                fetcher=payloads.__getitem__,
                now=datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
                lookback_months=1,
                recheck_recent_archives=0,
            )
            prior_state = state_path.read_bytes()
            prior_master = master_path.read_bytes()

            # A byte-level source change forces the state-first half of the
            # paired publish while preserving the parsed SEC ticker content.
            payloads[master.SEC_COMPANY_TICKERS_URL] += b"\n"
            interruption = KeyboardInterrupt("master write interrupted")
            with (
                mock.patch.object(
                    master,
                    "save_security_master_pair",
                    side_effect=interruption,
                ),
                self.assertRaises(KeyboardInterrupt) as raised,
            ):
                master.refresh_security_master(
                    self.universe(),
                    master_path=master_path,
                    source_state_path=state_path,
                    fetcher=payloads.__getitem__,
                    now=datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                    lookback_months=1,
                    recheck_recent_archives=0,
                )

            self.assertIs(interruption, raised.exception)
            self.assertEqual(prior_state, state_path.read_bytes())
            self.assertEqual(prior_master, master_path.read_bytes())


class SecurityMasterPairTransactionTests(unittest.TestCase):
    def pair(self, *symbols: str) -> tuple[dict, dict]:
        state = source_state(rows=[], symbols=list(symbols) or ["AAPL"])
        built = master.rebuild_security_master(
            state,
            {"037833100": "EQUITY"},
        )
        return built, state

    def test_pair_round_trip_binding_permissions_and_reentrant_lock(self) -> None:
        built, state = self.pair("AAPL")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"

            master.save_security_master_pair(
                built,
                state,
                master_path=master_path,
                source_state_path=state_path,
            )
            with master.security_master_pair_lock(
                master_path=master_path,
                source_state_path=state_path,
            ) as locked_pair:
                # Public loaders may be nested by callers such as snapshot
                # tooling without deadlocking the process on its own flock.
                self.assertEqual(
                    locked_pair,
                    master.load_security_master_pair(
                        master_path=master_path,
                        source_state_path=state_path,
                    ),
                )

            loaded_master, loaded_state = master.load_security_master_pair(
                master_path=master_path,
                source_state_path=state_path,
            )
            self.assertEqual(built, loaded_master)
            self.assertEqual(state, loaded_state)
            self.assertEqual(
                0o600,
                stat.S_IMODE(master_path.stat().st_mode),
            )
            self.assertEqual(0o600, stat.S_IMODE(state_path.stat().st_mode))
            self.assertEqual(
                0o600,
                stat.S_IMODE(
                    (root / master._PAIR_LOCK_NAME).stat().st_mode
                ),
            )
            self.assertFalse((root / master._PAIR_MARKER_NAME).exists())
            self.assertEqual(
                [],
                [
                    item.name
                    for item in root.iterdir()
                    if master._PAIR_RUN_ARTIFACT_RE.fullmatch(item.name)
                ],
            )

    def test_pair_rejects_digest_mismatch_cross_parent_and_symlink(self) -> None:
        built, state = self.pair("AAPL")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            other = root / "other"
            other.mkdir()
            with self.assertRaisesRegex(
                master.SecurityMasterError,
                "share one parent",
            ):
                master.save_security_master_pair(
                    built,
                    state,
                    master_path=root / "master.json",
                    source_state_path=other / "state.json",
                )

            mismatched = copy.deepcopy(built)
            mismatched["source_state_sha256"] = "f" * 64
            with self.assertRaisesRegex(
                master.SecurityMasterError,
                "not bound",
            ):
                master.save_security_master_pair(
                    mismatched,
                    state,
                    master_path=root / "master.json",
                    source_state_path=root / "state.json",
                )

            target = root / "real-state.json"
            target.write_text("{}", encoding="utf-8")
            symlink = root / "state.json"
            symlink.symlink_to(target)
            with self.assertRaisesRegex(
                master.SecurityMasterError,
                "regular file",
            ):
                master.save_security_master_pair(
                    built,
                    state,
                    master_path=root / "master.json",
                    source_state_path=symlink,
                )

    def test_post_replace_fsync_failure_restores_old_pair_and_primary(self) -> None:
        old_master, old_state = self.pair("AAPL")
        new_master, new_state = self.pair("AAPL", "MSFT")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.save_security_master_pair(
                old_master,
                old_state,
                master_path=master_path,
                source_state_path=state_path,
            )
            old_master_bytes = master_path.read_bytes()
            old_state_bytes = state_path.read_bytes()
            original_fsync = master._fsync_directory
            failed = False
            primary = OSError("state replacement directory fsync failed")

            def fail_after_state_replace(path: Path) -> None:
                nonlocal failed
                original_fsync(path)
                if (
                    not failed
                    and (root / master._PAIR_MARKER_NAME).exists()
                    and state_path.exists()
                    and state_path.read_bytes() != old_state_bytes
                ):
                    failed = True
                    raise primary

            with (
                mock.patch.object(
                    master,
                    "_fsync_directory",
                    side_effect=fail_after_state_replace,
                ),
                self.assertRaises(OSError) as raised,
            ):
                master.save_security_master_pair(
                    new_master,
                    new_state,
                    master_path=master_path,
                    source_state_path=state_path,
                )

            self.assertIs(primary, raised.exception)
            self.assertEqual(old_master_bytes, master_path.read_bytes())
            self.assertEqual(old_state_bytes, state_path.read_bytes())
            self.assertFalse((root / master._PAIR_MARKER_NAME).exists())

    def test_exception_after_both_replaces_forces_old_pair_not_commit(self) -> None:
        old_master, old_state = self.pair("AAPL")
        new_master, new_state = self.pair("AAPL", "MSFT")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            master.save_security_master_pair(
                old_master,
                old_state,
                master_path=master_path,
                source_state_path=state_path,
            )
            old_master_bytes = master_path.read_bytes()
            old_state_bytes = state_path.read_bytes()
            original_replace = master._replace_pair_member
            primary = OSError("raised after second replace and directory fsync")

            def fail_after_both_landed(*args, **kwargs) -> None:
                original_replace(*args, **kwargs)
                if (
                    kwargs.get("member") == "master"
                    and kwargs.get("role") == "install"
                ):
                    self.assertNotEqual(
                        old_master_bytes,
                        master_path.read_bytes(),
                    )
                    self.assertNotEqual(old_state_bytes, state_path.read_bytes())
                    raise primary

            with (
                mock.patch.object(
                    master,
                    "_replace_pair_member",
                    side_effect=fail_after_both_landed,
                ),
                self.assertRaises(OSError) as raised,
            ):
                master.save_security_master_pair(
                    new_master,
                    new_state,
                    master_path=master_path,
                    source_state_path=state_path,
                )

            self.assertIs(primary, raised.exception)
            self.assertEqual(old_master_bytes, master_path.read_bytes())
            self.assertEqual(old_state_bytes, state_path.read_bytes())
            self.assertFalse((root / master._PAIR_MARKER_NAME).exists())

    def test_marker_unlink_fsync_exception_still_restores_old_pair(self) -> None:
        old_master, old_state = self.pair("AAPL")
        new_master, new_state = self.pair("AAPL", "MSFT")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            marker_path = root / master._PAIR_MARKER_NAME
            master.save_security_master_pair(
                old_master,
                old_state,
                master_path=master_path,
                source_state_path=state_path,
            )
            old_master_bytes = master_path.read_bytes()
            old_state_bytes = state_path.read_bytes()
            original_fsync = master._fsync_directory
            failed = False
            primary = OSError("marker unlink directory fsync failed")

            def fail_after_marker_unlink(path: Path) -> None:
                nonlocal failed
                original_fsync(path)
                if (
                    not failed
                    and not marker_path.exists()
                    and master_path.read_bytes() != old_master_bytes
                    and state_path.read_bytes() != old_state_bytes
                ):
                    failed = True
                    raise primary

            with (
                mock.patch.object(
                    master,
                    "_fsync_directory",
                    side_effect=fail_after_marker_unlink,
                ),
                self.assertRaises(OSError) as raised,
            ):
                master.save_security_master_pair(
                    new_master,
                    new_state,
                    master_path=master_path,
                    source_state_path=state_path,
                )

            self.assertIs(primary, raised.exception)
            self.assertEqual(old_master_bytes, master_path.read_bytes())
            self.assertEqual(old_state_bytes, state_path.read_bytes())
            self.assertFalse(marker_path.exists())

    def test_first_cutover_interruption_restores_both_missing_targets(self) -> None:
        built, state = self.pair("AAPL")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            original_replace = master._replace_pair_member
            primary = KeyboardInterrupt("interrupted after first replace")

            def interrupt_after_state(*args, **kwargs) -> None:
                original_replace(*args, **kwargs)
                if kwargs.get("member") == "state":
                    raise primary

            with (
                mock.patch.object(
                    master,
                    "_replace_pair_member",
                    side_effect=interrupt_after_state,
                ),
                self.assertRaises(KeyboardInterrupt) as raised,
            ):
                master.save_security_master_pair(
                    built,
                    state,
                    master_path=master_path,
                    source_state_path=state_path,
                )

            self.assertIs(primary, raised.exception)
            self.assertFalse(master_path.exists())
            self.assertFalse(state_path.exists())
            self.assertFalse((root / master._PAIR_MARKER_NAME).exists())

    def test_subprocess_crash_after_first_replace_recovers_old_pair(self) -> None:
        old_master, old_state = self.pair("AAPL")
        new_master, new_state = self.pair("AAPL", "MSFT")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            new_master_path = root / "input-master.json"
            new_state_path = root / "input-state.json"
            master.save_security_master_pair(
                old_master,
                old_state,
                master_path=master_path,
                source_state_path=state_path,
            )
            old_master_bytes = master_path.read_bytes()
            old_state_bytes = state_path.read_bytes()
            new_master_path.write_text(json.dumps(new_master), encoding="utf-8")
            new_state_path.write_text(json.dumps(new_state), encoding="utf-8")
            script = "\n".join((
                "import json, os, pathlib",
                "import sec_security_master as module",
                f"root = pathlib.Path({str(root)!r})",
                "original = module._replace_pair_member",
                "def crash(*args, **kwargs):",
                "    original(*args, **kwargs)",
                "    if kwargs.get('member') == 'state': os._exit(73)",
                "module._replace_pair_member = crash",
                "module.save_security_master_pair(",
                "    json.loads((root / 'input-master.json').read_text()),",
                "    json.loads((root / 'input-state.json').read_text()),",
                "    master_path=root / 'master.json',",
                "    source_state_path=root / 'state.json',",
                ")",
            ))

            completed = subprocess.run(
                [sys.executable, "-c", script],
                cwd=Path(master.__file__).resolve().parent,
                check=False,
            )
            self.assertEqual(73, completed.returncode)
            self.assertTrue((root / master._PAIR_MARKER_NAME).exists())
            self.assertNotEqual(old_state_bytes, state_path.read_bytes())
            self.assertEqual(old_master_bytes, master_path.read_bytes())

            # Either authoritative single-file loader must trigger pair
            # recovery before exposing one side of an interrupted publish.
            recovered_state = master.load_source_state(state_path)
            recovered_master = master.load_security_master(master_path)
            self.assertEqual(old_master, recovered_master)
            self.assertEqual(old_state, recovered_state)
            self.assertEqual(old_master_bytes, master_path.read_bytes())
            self.assertEqual(old_state_bytes, state_path.read_bytes())
            self.assertFalse((root / master._PAIR_MARKER_NAME).exists())

    def test_subprocess_crash_after_both_replaces_finalizes_new_pair(self) -> None:
        old_master, old_state = self.pair("AAPL")
        new_master, new_state = self.pair("AAPL", "MSFT")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "master.json"
            state_path = root / "state.json"
            new_master_path = root / "input-master.json"
            new_state_path = root / "input-state.json"
            master.save_security_master_pair(
                old_master,
                old_state,
                master_path=master_path,
                source_state_path=state_path,
            )
            new_master_path.write_text(json.dumps(new_master), encoding="utf-8")
            new_state_path.write_text(json.dumps(new_state), encoding="utf-8")
            script = "\n".join((
                "import json, os, pathlib",
                "import sec_security_master as module",
                f"root = pathlib.Path({str(root)!r})",
                "original = module._replace_pair_member",
                "def crash(*args, **kwargs):",
                "    original(*args, **kwargs)",
                "    if kwargs.get('member') == 'master': os._exit(74)",
                "module._replace_pair_member = crash",
                "module.save_security_master_pair(",
                "    json.loads((root / 'input-master.json').read_text()),",
                "    json.loads((root / 'input-state.json').read_text()),",
                "    master_path=root / 'master.json',",
                "    source_state_path=root / 'state.json',",
                ")",
            ))

            completed = subprocess.run(
                [sys.executable, "-c", script],
                cwd=Path(master.__file__).resolve().parent,
                check=False,
            )
            self.assertEqual(74, completed.returncode)
            self.assertTrue((root / master._PAIR_MARKER_NAME).exists())

            recovered_master, recovered_state = master.load_security_master_pair(
                master_path=master_path,
                source_state_path=state_path,
            )
            self.assertEqual(new_master, recovered_master)
            self.assertEqual(new_state, recovered_state)
            self.assertFalse((root / master._PAIR_MARKER_NAME).exists())


class FetcherTests(unittest.TestCase):
    def response(
        self,
        status: int,
        *,
        url: str = master.SEC_COMPANY_TICKERS_URL,
        content: bytes = b"ok",
        headers: dict[str, str] | None = None,
    ) -> mock.Mock:
        response = mock.Mock()
        response.status_code = status
        response.url = url
        response.content = content
        response.headers = headers or {}
        if status >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(str(status))
        return response

    def setUp(self) -> None:
        master._SEC_NEXT_REQUEST_AT = 0.0

    def test_fetcher_retries_503_with_bounded_backoff(self) -> None:
        session = mock.Mock()
        session.get.side_effect = [self.response(503), self.response(200)]
        fetch = master.make_sec_fetcher(
            "Agent agent@example.com",
            session=session,
        )
        with (
            mock.patch.object(master.time, "monotonic", side_effect=[0.0, 1.0]),
            mock.patch.object(master.time, "sleep") as sleep,
        ):
            self.assertEqual(b"ok", fetch(master.SEC_COMPANY_TICKERS_URL))
        self.assertEqual(2, session.get.call_count)
        sleep.assert_called_once_with(1.0)

    def test_fetcher_retries_all_transient_statuses_and_caps_retry_after(self) -> None:
        for status in (403, 429, 500, 502, 503, 504):
            with self.subTest(status=status):
                master._SEC_NEXT_REQUEST_AT = 0.0
                session = mock.Mock()
                session.get.side_effect = [
                    self.response(status, headers={"Retry-After": "999"}),
                    self.response(200),
                ]
                fetch = master.make_sec_fetcher(
                    "Agent agent@example.com",
                    session=session,
                    max_retry_delay=2.0,
                )
                with (
                    mock.patch.object(
                        master.time, "monotonic", side_effect=[0.0, 3.0]
                    ),
                    mock.patch.object(master.time, "sleep") as sleep,
                ):
                    self.assertEqual(
                        b"ok", fetch(master.SEC_COMPANY_TICKERS_URL)
                    )
                sleep.assert_called_once_with(2.0)

    def test_fetcher_retries_connection_and_timeout_errors(self) -> None:
        for error in (
            requests.ConnectionError("connection reset"),
            requests.Timeout("request timed out"),
        ):
            with self.subTest(error=type(error).__name__):
                master._SEC_NEXT_REQUEST_AT = 0.0
                session = mock.Mock()
                session.get.side_effect = [error, self.response(200)]
                fetch = master.make_sec_fetcher(
                    "Agent agent@example.com",
                    session=session,
                )
                with (
                    mock.patch.object(
                        master.time,
                        "monotonic",
                        side_effect=[0.0, 1.0],
                    ),
                    mock.patch.object(master.time, "sleep") as sleep,
                ):
                    self.assertEqual(
                        b"ok",
                        fetch(master.SEC_COMPANY_TICKERS_URL),
                    )
                self.assertEqual(2, session.get.call_count)
                sleep.assert_called_once_with(1.0)

    def test_fetcher_exhausts_transient_errors_and_fails_permanent_http(self) -> None:
        session = mock.Mock()
        session.get.side_effect = [
            requests.ConnectionError("offline"),
            requests.ConnectionError("offline"),
        ]
        fetch = master.make_sec_fetcher(
            "Agent agent@example.com",
            session=session,
            max_attempts=2,
        )
        with (
            mock.patch.object(
                master.time,
                "monotonic",
                side_effect=[0.0, 1.0],
            ),
            mock.patch.object(master.time, "sleep"),
        ):
            with self.assertRaises(requests.ConnectionError):
                fetch(master.SEC_COMPANY_TICKERS_URL)
        self.assertEqual(2, session.get.call_count)

        master._SEC_NEXT_REQUEST_AT = 0.0
        session = mock.Mock()
        session.get.return_value = self.response(401)
        fetch = master.make_sec_fetcher(
            "Agent agent@example.com",
            session=session,
        )
        with (
            mock.patch.object(master.time, "monotonic", return_value=0.0),
            mock.patch.object(master.time, "sleep") as sleep,
        ):
            with self.assertRaises(requests.HTTPError):
                fetch(master.SEC_COMPANY_TICKERS_URL)
        self.assertEqual(1, session.get.call_count)
        sleep.assert_not_called()

    def test_fetcher_paces_consecutive_requests_at_eight_per_second(self) -> None:
        session = mock.Mock()
        session.get.side_effect = [self.response(200), self.response(200)]
        fetch = master.make_sec_fetcher(
            "Agent agent@example.com",
            session=session,
        )
        with (
            mock.patch.object(
                master.time,
                "monotonic",
                side_effect=[0.0, 0.0, 0.125],
            ),
            mock.patch.object(master.time, "sleep") as sleep,
        ):
            fetch(master.SEC_COMPANY_TICKERS_URL)
            fetch(master.SEC_COMPANY_TICKERS_URL)
        sleep.assert_called_once_with(0.125)

    def test_fetcher_rejects_external_redirect_target(self) -> None:
        session = mock.Mock()
        session.get.return_value = self.response(
            200, url="https://example.com/redirected"
        )
        fetch = master.make_sec_fetcher(
            "Agent agent@example.com",
            session=session,
        )
        with mock.patch.object(master.time, "monotonic", return_value=0.0):
            with self.assertRaises(master.NonSECURL):
                fetch(master.SEC_COMPANY_TICKERS_URL)

    def test_fetcher_does_not_follow_external_location_header(self) -> None:
        session = mock.Mock()
        session.get.return_value = self.response(
            302,
            headers={"Location": "https://example.com/redirected"},
        )
        fetch = master.make_sec_fetcher(
            "Agent agent@example.com",
            session=session,
        )
        with mock.patch.object(master.time, "monotonic", return_value=0.0):
            with self.assertRaises(master.NonSECURL):
                fetch(master.SEC_COMPANY_TICKERS_URL)
        self.assertEqual(1, session.get.call_count)


if __name__ == "__main__":
    unittest.main()
