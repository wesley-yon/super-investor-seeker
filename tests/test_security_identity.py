"""Provider-neutral tests for immutable public security identity."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import unittest
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock

import pipeline
import security_identity
import security_master_migration
import validate_data
from security_identity import (
    FUND_IDENTITY_TICKER_SOURCES,
    INSTRUMENT_TYPES,
    VALID_INSTRUMENT_TYPES,
    compose_security_label,
    holding_instrument_type,
    is_canonical_security_identifier,
    is_mutual_fund_ticker,
    is_synthetic_identifier,
    normalize_instrument_type,
    normalize_note_security_label,
    normalize_security_identifier,
    normalize_security_kind,
    normalize_security_label,
    parse_stock_lookup_id,
    published_holding_instrument_type,
    registry_entry_has_equity_fund_identity,
    registry_entry_has_trusted_fund_symbol_evidence,
    stock_filename,
    stock_lookup_id,
    synthetic_identifier_ticker_hint,
)


class InstrumentIdentityTests(unittest.TestCase):
    def test_instrument_types_are_finite_and_stable(self) -> None:
        self.assertEqual(
            ("EQUITY", "PREF", "NOTE", "WARRANT", "CALL", "PUT", "OPT"),
            INSTRUMENT_TYPES,
        )
        self.assertEqual(set(INSTRUMENT_TYPES), set(VALID_INSTRUMENT_TYPES))
        self.assertEqual("EQUITY", normalize_instrument_type(None))
        self.assertEqual("NOTE", normalize_instrument_type(" note "))
        self.assertEqual("EQUITY", normalize_instrument_type("invented"))

    def test_explicit_put_call_outranks_saved_type(self) -> None:
        self.assertEqual(
            "PUT",
            holding_instrument_type({"put_call": "put", "holding_type": "NOTE"}),
        )
        self.assertEqual("CALL", holding_instrument_type({"put_call": "CALL"}))
        self.assertEqual("WARRANT", holding_instrument_type({"holding_type": "WARRANT"}))

    def test_stock_identity_keeps_option_sides_separate(self) -> None:
        self.assertEqual("037833100", stock_lookup_id("037833100", "EQUITY"))
        self.assertEqual("037833100|CALL", stock_lookup_id("037833100", "CALL"))
        self.assertEqual("037833100|PUT", stock_lookup_id("037833100", "PUT"))
        self.assertEqual(
            ("037833100", "CALL"),
            parse_stock_lookup_id("037833100|CALL"),
        )
        self.assertEqual("037833100__CALL.json", stock_filename("037833100", "CALL"))
        self.assertEqual("037833100__PUT.json", stock_filename("037833100", "PUT"))

    def test_identifier_normalization_does_not_repair_as_filed_values(self) -> None:
        self.assertEqual("BAD-CUSIP", normalize_security_identifier(" bad-cusip "))
        self.assertTrue(is_canonical_security_identifier("BAD-CUSIP"))
        self.assertFalse(is_canonical_security_identifier(" bad-cusip "))

    def test_shared_identity_policy_is_used_by_pipeline_and_validator(self) -> None:
        self.assertIs(
            pipeline.published_holding_instrument_type,
            security_identity.published_holding_instrument_type,
        )
        self.assertIs(
            validate_data.published_holding_instrument_type,
            security_identity.published_holding_instrument_type,
        )
        self.assertIs(
            pipeline.is_synthetic_identifier,
            security_identity.is_synthetic_identifier,
        )
        self.assertIs(
            validate_data.is_synthetic_identifier,
            security_identity.is_synthetic_identifier,
        )

    def test_stock_lookup_id_blank_invalid_and_round_trip_cases(self) -> None:
        self.assertEqual("", stock_lookup_id(None, "CALL"))
        self.assertEqual("", stock_lookup_id("  ", "PUT"))
        self.assertEqual(
            ("037833100", "EQUITY"),
            parse_stock_lookup_id(" 037833100 "),
        )
        self.assertEqual(
            ("037833100", "EQUITY"),
            parse_stock_lookup_id("037833100|unknown"),
        )
        for instrument_type in INSTRUMENT_TYPES:
            with self.subTest(instrument_type=instrument_type):
                lookup_id = stock_lookup_id("abc123", instrument_type)
                self.assertEqual(
                    ("ABC123", instrument_type),
                    parse_stock_lookup_id(lookup_id),
                )

    def test_stock_file_helpers_preserve_the_public_layout(self) -> None:
        self.assertEqual("BRK-B._X", security_identity.safe_ticker(" brk-b./x "))
        self.assertEqual(
            "037833100",
            security_identity.stock_file_stem("037833100"),
        )
        self.assertEqual(
            "037833100__CALL",
            security_identity.stock_file_stem("037833100|CALL"),
        )
        self.assertEqual(
            "BRK_B__PUT",
            security_identity.stock_file_stem("brk/b|put"),
        )
        self.assertEqual("", stock_filename("", "CALL"))


class FrontendIdentityContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = (Path(__file__).resolve().parents[1] / "index.html").read_text()

    def test_frontend_supported_types_match_shared_contract(self) -> None:
        match = re.search(
            r"function normalizeInstrumentType\(type\).*?"
            r"return \[(.*?)\]\.includes",
            self.html,
            re.DOTALL,
        )
        self.assertIsNotNone(match)
        self.assertEqual(
            INSTRUMENT_TYPES,
            tuple(re.findall(r'"([A-Z]+)"', match.group(1))),
        )

    def test_frontend_normalizes_type_and_uses_exact_stock_identity(self) -> None:
        self.assertRegex(
            self.html,
            r"function normalizeInstrumentType\(type\)\s*\{\s*"
            r'const t = String\(type \|\| "EQUITY"\)'
            r"\.trim\(\)\.toUpperCase\(\);",
        )
        self.assertRegex(
            self.html,
            r"(?s)function holdingHistoryKey\(h\)\s*\{\s*"
            r"return stockLookupId\(.*?holdingPublishedInstrumentType\(h\)",
        )
        for field in (
            "cusip: parsed.id_base",
            "instrument_type: parsed.instrument_type",
            "stock_id: parsed.stock_id",
        ):
            self.assertIn(field, self.html)


class DisplayMetadataTests(unittest.TestCase):
    def test_safe_labels_reject_bare_identifiers_and_placeholders(self) -> None:
        self.assertIsNone(normalize_security_label("037833100", "037833100"))
        self.assertIsNone(normalize_security_label("N/A", "037833100"))
        self.assertEqual(
            "APPLE INC — COM",
            normalize_security_label("  APPLE INC   — COM ", "037833100"),
        )

    def test_fallback_label_uses_issuer_and_informative_class(self) -> None:
        self.assertEqual(
            "RIVIAN AUTOMOTIVE INC — NOTE 3.625% 10/15/30",
            compose_security_label(
                "RIVIAN AUTOMOTIVE INC",
                "NOTE 3.625% 10/15/30",
                "NOTE",
                "76954AAD5",
            ),
        )
        self.assertEqual(
            "EQUITY SECURITY",
            compose_security_label(None, "COM", "EQUITY", "OOOOOOOOO"),
        )

    def test_note_terms_are_labels_not_common_tickers(self) -> None:
        self.assertEqual(
            "RIVN 3.625 10/15/30",
            normalize_note_security_label(" RIVN  3.625  10/15/30 2030 "),
        )
        self.assertEqual(
            "BILL 0 04/01/30",
            normalize_note_security_label("BILL 0 04/01/30"),
        )
        self.assertIsNone(normalize_note_security_label("RIVN"))

    def test_security_kinds_are_finite(self) -> None:
        self.assertEqual("COMMON", normalize_security_kind(" common "))
        self.assertEqual("CLOSED-END FUND", normalize_security_kind("closed-end fund"))
        self.assertIsNone(normalize_security_kind("generic equity"))

    def test_labels_reject_controls_placeholders_and_overlong_values(self) -> None:
        for raw in (
            None,
            "",
            "   ",
            "#N/A",
            "N/A",
            "NONE",
            "INVALID",
            "LOOK IT UP",
            "#N/A INVALID SECURITY",
            "ACME INVALID SECURITY",
            "0",
            "698",
            "NEE\n7.375 02/15/29",
            "NEE\x00 7.375 02/15/29",
            "\u200bBSV",
            "INNOVIZ TECHNOLOGIES LTD W EXP 04/05/202",
            "X" * 161,
        ):
            with self.subTest(raw=raw):
                self.assertIsNone(normalize_security_label(raw))
        self.assertIsNone(normalize_security_label("65339F655", "65339f655"))
        self.assertEqual("3M CO", normalize_security_label("3M CO"))

    def test_canonical_fallback_labels_keep_useful_class_detail(self) -> None:
        self.assertEqual(
            "NEXTERA ENERGY INC — UNIT 02/15/2029",
            compose_security_label(
                "NEXTERA ENERGY INC",
                "UNIT 02/15/2029",
                "PREF",
                "65339F655",
            ),
        )
        self.assertEqual(
            "MA-COM TECH — 0 12/15/29",
            compose_security_label(
                "MA-COM TECH",
                "0 15/12/2029",
                "NOTE",
                "55405YAC4",
            ),
        )
        self.assertEqual(
            "VANGUARD BD INDEX FDS",
            compose_security_label(
                "VANGUARD BD INDEX FDS",
                "COMMON STOCK",
                "EQUITY",
                "921937108",
            ),
        )
        self.assertEqual(
            "PREF SECURITY",
            compose_security_label(
                "65339F655",
                "PREF",
                "PREF",
                "65339F655",
            ),
        )


class FundIdentityTests(unittest.TestCase):
    def test_all_trusted_ticker_sources_are_sec_specific(self) -> None:
        self.assertTrue(FUND_IDENTITY_TICKER_SOURCES)
        self.assertTrue(all(source.startswith("sec_") for source in FUND_IDENTITY_TICKER_SOURCES))

    def test_mutual_fund_symbol_requires_sec_provenance(self) -> None:
        proven = {
            "type": "EQUITY",
            "ticker": "SWPPX",
            "mapping_status": "resolved",
            "ticker_source": "sec_ftd",
            "ticker_as_of": "2026-08-29",
            "sources": ["sec_ftd", "sec_fund_series"],
        }
        self.assertTrue(is_mutual_fund_ticker("SWPPX"))
        self.assertTrue(registry_entry_has_trusted_fund_symbol_evidence(proven))
        self.assertTrue(registry_entry_has_equity_fund_identity(proven))
        self.assertFalse(
            registry_entry_has_trusted_fund_symbol_evidence(
                {**proven, "sources": ["issuer_name_guess"]}
            )
        )
        self.assertFalse(
            registry_entry_has_trusted_fund_symbol_evidence(
                {
                    **proven,
                    "ticker_source": "sec_fund_series",
                    "sources": ["sec_fund_series"],
                }
            )
        )

    def test_published_type_preserves_all_saved_types_despite_registry_metadata(self) -> None:
        entries = [
            {"type": "NOTE", "security_kind": "BOND", "security_kind_source": "sec_13f_list"},
            {"type": "PREF", "security_kind": "PREFERRED", "security_kind_source": "sec_13f_list"},
            {"type": "WARRANT", "security_kind": "WARRANT", "security_kind_source": "sec_13f_list"},
            {"type": "EQUITY", "security_kind": "ETF", "ticker": "SPY",
             "mapping_status": "resolved", "ticker_source": "sec_ftd",
             "sources": ["sec_ftd", "sec_fund_series"]},
        ]
        for entry in entries:
            for kind in ("EQUITY", "NOTE", "PREF", "WARRANT", "CALL", "PUT", "OPT"):
                with self.subTest(entry=entry, kind=kind):
                    self.assertEqual(kind, published_holding_instrument_type(
                        {"holding_type": kind}, entry,
                    ))
            self.assertEqual("CALL", published_holding_instrument_type(
                {"holding_type": "NOTE", "put_call": "CALL"}, entry,
            ))


class AtomicPipelineMutationTests(unittest.TestCase):
    @staticmethod
    def _fund_fixture() -> dict:
        return {
            "cik": 1,
            "name": "Durability Probe",
            "quarters": [
                {
                    "report_date": "2025-12-31",
                    "total_value": 100,
                    "holdings": [
                        {
                            "cusip": "037833100",
                            "ticker": "OLD",
                            "issuer": "Old Issuer",
                            "holding_type": "EQUITY",
                            "shares": 1,
                            "value": 100,
                        }
                    ],
                }
            ],
        }

    def test_fund_rewrite_preserves_original_on_write_failure(self) -> None:
        registry = {
            "037833100": {
                "ticker": "AAPL",
                "name": "Apple Inc",
                "type": "EQUITY",
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            fund_path = funds_dir / "1.json"
            original = json.dumps(self._fund_fixture()).encode()
            fund_path.write_bytes(original)
            with (
                mock.patch.multiple(
                    pipeline,
                    DATA_DIR=data_dir,
                    FUNDS_DIR=funds_dir,
                    load_cusip_registry=mock.Mock(return_value=registry),
                ),
                mock.patch.object(
                    pipeline.json,
                    "dump",
                    side_effect=OSError("injected write failure"),
                ),
                self.assertRaisesRegex(OSError, "injected write failure"),
            ):
                pipeline.canonicalize_fund_files()

            self.assertEqual(original, fund_path.read_bytes())
            self.assertEqual([], list(funds_dir.glob("*.tmp.*")))

    def test_atomic_json_write_cleans_up_after_keyboard_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "authoritative.json"
            original = b'{"generation":"current"}'
            path.write_bytes(original)

            with (
                mock.patch.object(
                    pipeline.json,
                    "dump",
                    side_effect=KeyboardInterrupt,
                ),
                self.assertRaises(KeyboardInterrupt),
            ):
                pipeline._atomic_write_json(path, {"generation": "next"})

            self.assertEqual(original, path.read_bytes())
            self.assertEqual([], list(path.parent.glob("*.tmp.*")))

    def test_atomic_json_write_preserves_primary_error_when_cleanup_fails(
        self,
    ) -> None:
        primary = KeyboardInterrupt("primary write interruption")
        cleanup = OSError("secondary temp cleanup failure")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "authoritative.json"
            original = b'{"generation":"current"}'
            path.write_bytes(original)

            with (
                mock.patch.object(
                    pipeline.json,
                    "dump",
                    side_effect=primary,
                ),
                mock.patch.object(Path, "unlink", side_effect=cleanup),
                self.assertRaises(KeyboardInterrupt) as raised,
            ):
                pipeline._atomic_write_json(path, {"generation": "next"})

            self.assertIs(primary, raised.exception)
            self.assertEqual(original, path.read_bytes())

    def test_atomic_json_write_preserves_primary_error_when_cleanup_fsync_fails(
        self,
    ) -> None:
        primary = KeyboardInterrupt("primary write interruption")
        cleanup = OSError("secondary directory fsync failure")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "authoritative.json"
            original = b'{"generation":"current"}'
            path.write_bytes(original)

            with (
                mock.patch.object(
                    pipeline.json,
                    "dump",
                    side_effect=primary,
                ),
                mock.patch.object(
                    pipeline,
                    "_fsync_directory",
                    side_effect=cleanup,
                ),
                self.assertRaises(KeyboardInterrupt) as raised,
            ):
                pipeline._atomic_write_json(path, {"generation": "next"})

            self.assertIs(primary, raised.exception)
            self.assertEqual(original, path.read_bytes())
            self.assertEqual([], list(path.parent.glob("*.tmp.*")))

    def test_atomic_json_write_fsyncs_file_and_parent_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "authoritative.json"
            with mock.patch.object(pipeline.os, "fsync") as fsync:
                pipeline._atomic_write_json(path, {"generation": "next"})
            self.assertEqual(2, fsync.call_count)

    def test_derived_build_failure_preserves_current_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            stocks_dir.mkdir()
            (funds_dir / "1.json").write_text(json.dumps(self._fund_fixture()))
            (stocks_dir / "current.json").write_text('{"generation":"current"}')
            index_path = data_dir / "index.json"
            funds_index_path = data_dir / "funds-index.json"
            index_path.write_text('{"generation":"current-index"}')
            funds_index_path.write_text('{"generation":"current-funds-index"}')
            expected_stocks = {
                path.name: path.read_bytes() for path in stocks_dir.glob("*.json")
            }
            expected_index = index_path.read_bytes()
            expected_funds_index = funds_index_path.read_bytes()

            with (
                mock.patch.multiple(
                    pipeline,
                    DATA_DIR=data_dir,
                    FUNDS_DIR=funds_dir,
                    STOCKS_DIR=stocks_dir,
                    INDEX_PATH=index_path,
                    FUNDS_INDEX_PATH=funds_index_path,
                    load_cusip_registry=mock.Mock(return_value={}),
                ),
                mock.patch.object(
                    pipeline.json,
                    "dump",
                    side_effect=OSError("injected build failure"),
                ),
                self.assertRaisesRegex(OSError, "injected build failure"),
            ):
                pipeline.regenerate_stock_files_and_index(state={})

            self.assertEqual(
                expected_stocks,
                {
                    path.name: path.read_bytes()
                    for path in stocks_dir.glob("*.json")
                },
            )
            self.assertEqual(expected_index, index_path.read_bytes())
            self.assertEqual(expected_funds_index, funds_index_path.read_bytes())
            self.assertEqual([], list(Path(tmpdir).glob(".derived-stage-*")))
            self.assertEqual([], list(Path(tmpdir).glob(".derived-backup-*")))

    def test_derived_publish_rolls_back_partial_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            stocks_dir.mkdir()
            (funds_dir / "1.json").write_text(json.dumps(self._fund_fixture()))
            current_stock = b'{"generation":"current"}'
            current_index = b'{"generation":"current-index"}'
            current_funds_index = b'{"generation":"current-funds-index"}'
            (stocks_dir / "current.json").write_bytes(current_stock)
            index_path = data_dir / "index.json"
            funds_index_path = data_dir / "funds-index.json"
            index_path.write_bytes(current_index)
            funds_index_path.write_bytes(current_funds_index)
            real_replace = pipeline.os.replace

            def fail_during_publish(source, target) -> None:
                source_path = Path(source)
                if (
                    source_path.name == "funds-index.json"
                    and source_path.parent.name.startswith(".derived-stage-")
                ):
                    raise OSError("injected publish failure")
                real_replace(source, target)

            with (
                mock.patch.multiple(
                    pipeline,
                    DATA_DIR=data_dir,
                    FUNDS_DIR=funds_dir,
                    STOCKS_DIR=stocks_dir,
                    INDEX_PATH=index_path,
                    FUNDS_INDEX_PATH=funds_index_path,
                    load_cusip_registry=mock.Mock(return_value={}),
                ),
                mock.patch.object(
                    pipeline.os,
                    "replace",
                    side_effect=fail_during_publish,
                ),
                self.assertRaisesRegex(OSError, "injected publish failure"),
            ):
                pipeline.regenerate_stock_files_and_index(state={})

            self.assertEqual(
                {"current.json": current_stock},
                {
                    path.name: path.read_bytes()
                    for path in stocks_dir.glob("*.json")
                },
            )
            self.assertEqual(current_index, index_path.read_bytes())
            self.assertEqual(current_funds_index, funds_index_path.read_bytes())
            self.assertEqual([], list(Path(tmpdir).glob(".derived-stage-*")))
            self.assertEqual([], list(Path(tmpdir).glob(".derived-backup-*")))

    def test_interrupted_publish_restores_previous_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            data_dir = root / "data"
            stocks_dir = data_dir / "stocks"
            stocks_dir.mkdir(parents=True)
            (stocks_dir / "new.json").write_text('{"generation":"new"}')
            index_path = data_dir / "index.json"
            funds_index_path = data_dir / "funds-index.json"
            index_path.write_text('{"generation":"new-index"}')
            funds_index_path.write_text('{"generation":"new-funds-index"}')

            backup_root = root / ".derived-backup-interrupted"
            backup_stocks = backup_root / "stocks"
            backup_stocks.mkdir(parents=True)
            (backup_stocks / "old.json").write_text('{"generation":"old"}')
            (backup_root / "index.json").write_text('{"generation":"old-index"}')
            (backup_root / "funds-index.json").write_text(
                '{"generation":"old-funds-index"}'
            )
            (backup_root / "transaction.json").write_text(
                json.dumps(
                    {
                        "status": "prepared",
                        "present": ["stocks", "funds-index.json", "index.json"],
                    }
                )
            )
            stale_stage = root / ".derived-stage-interrupted"
            stale_stage.mkdir()
            (stale_stage / "partial.json").write_text("{}")

            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                STOCKS_DIR=stocks_dir,
                INDEX_PATH=index_path,
                FUNDS_INDEX_PATH=funds_index_path,
            ):
                pipeline._recover_interrupted_derived_publishes()

            self.assertEqual(
                {"old.json": b'{"generation":"old"}'},
                {
                    path.name: path.read_bytes()
                    for path in stocks_dir.glob("*.json")
                },
            )
            self.assertEqual(b'{"generation":"old-index"}', index_path.read_bytes())
            self.assertEqual(
                b'{"generation":"old-funds-index"}',
                funds_index_path.read_bytes(),
            )
            self.assertFalse(backup_root.exists())
            self.assertFalse(stale_stage.exists())

    def test_completed_publish_keeps_new_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            data_dir = root / "data"
            stocks_dir = data_dir / "stocks"
            stocks_dir.mkdir(parents=True)
            (stocks_dir / "new.json").write_text('{"generation":"new"}')
            index_path = data_dir / "index.json"
            funds_index_path = data_dir / "funds-index.json"
            index_path.write_text('{"generation":"new-index"}')
            funds_index_path.write_text('{"generation":"new-funds-index"}')
            backup_root = root / ".derived-backup-completed"
            (backup_root / "stocks").mkdir(parents=True)
            (backup_root / "stocks" / "old.json").write_text("{}")
            (backup_root / "transaction.json").write_text(
                json.dumps(
                    {
                        "status": "published",
                        "present": ["stocks", "funds-index.json", "index.json"],
                    }
                )
            )

            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                STOCKS_DIR=stocks_dir,
                INDEX_PATH=index_path,
                FUNDS_INDEX_PATH=funds_index_path,
            ):
                pipeline._recover_interrupted_derived_publishes()

            self.assertEqual(
                {"new.json": b'{"generation":"new"}'},
                {
                    path.name: path.read_bytes()
                    for path in stocks_dir.glob("*.json")
                },
            )
            self.assertEqual(b'{"generation":"new-index"}', index_path.read_bytes())
            self.assertEqual(
                b'{"generation":"new-funds-index"}',
                funds_index_path.read_bytes(),
            )
            self.assertFalse(backup_root.exists())

    def test_derived_output_lock_serializes_processes(self) -> None:
        script = """
import os
import sys
import time
from pathlib import Path

import pipeline

pipeline.DATA_DIR = Path(sys.argv[1])
attempted_path = Path(sys.argv[4])

@pipeline._serialize_pipeline_maintenance
def probe():
    log_path = Path(sys.argv[2])
    token = sys.argv[3]
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"start {token}\\n")
        handle.flush()
        os.fsync(handle.fileno())
    if token == "first":
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not attempted_path.exists():
            time.sleep(0.01)
        if not attempted_path.exists():
            raise RuntimeError("second lock probe did not attempt entry")
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"end {token}\\n")
        handle.flush()
        os.fsync(handle.fileno())

if sys.argv[3] == "second":
    attempted_path.write_text("ready", encoding="utf-8")
probe()
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            data_dir = root / "data"
            log_path = root / "events.log"
            attempted_path = root / "second-attempted"
            command_root = Path(__file__).resolve().parents[1]
            first = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    script,
                    str(data_dir),
                    str(log_path),
                    "first",
                    str(attempted_path),
                ],
                cwd=command_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if log_path.exists() and "start first" in log_path.read_text():
                    break
                time.sleep(0.01)
            else:
                first.kill()
                self.fail("first lock probe did not start")

            second = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    script,
                    str(data_dir),
                    str(log_path),
                    "second",
                    str(attempted_path),
                ],
                cwd=command_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            first_stdout, first_stderr = first.communicate(timeout=5)
            second_stdout, second_stderr = second.communicate(timeout=5)
            self.assertEqual(
                0,
                first.returncode,
                msg=f"stdout={first_stdout!r} stderr={first_stderr!r}",
            )
            self.assertEqual(
                0,
                second.returncode,
                msg=f"stdout={second_stdout!r} stderr={second_stderr!r}",
            )
            self.assertEqual(
                ["start first", "end first", "start second", "end second"],
                log_path.read_text().splitlines(),
            )

    def test_parent_lock_remains_leased_until_inherited_worker_exits(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            worker_entered = threading.Event()
            release_worker = threading.Event()
            contender_entered = threading.Event()

            @pipeline._serialize_pipeline_maintenance
            def inherited_critical_section() -> None:
                worker_entered.set()
                if not release_worker.wait(5):
                    raise TimeoutError("inherited worker was not released")

            @pipeline._inherit_pipeline_maintenance
            def worker() -> None:
                inherited_critical_section()

            @pipeline._serialize_pipeline_maintenance
            def outer_workflow() -> threading.Thread:
                thread = threading.Thread(target=worker, daemon=True)
                thread.start()
                if not worker_entered.wait(2):
                    raise TimeoutError("inherited worker did not start")
                return thread

            @pipeline._serialize_pipeline_maintenance
            def contender() -> None:
                contender_entered.set()

            with mock.patch.object(pipeline, "DATA_DIR", data_dir):
                worker_thread = outer_workflow()
                contender_thread = threading.Thread(target=contender, daemon=True)
                contender_thread.start()
                entered_while_worker_active = contender_entered.wait(0.2)
                release_worker.set()
                worker_thread.join(2)
                contender_thread.join(2)

            self.assertFalse(entered_while_worker_active)
            self.assertFalse(worker_thread.is_alive())
            self.assertFalse(contender_thread.is_alive())
            self.assertTrue(contender_entered.is_set())


class RegistryPublicationGateTests(unittest.TestCase):
    def test_security_master_cli_uses_economics_preserving_regeneration(
        self,
    ) -> None:
        projection = {
            "schema_version": 1,
            "corpus": {
                "fund_count": 0,
                "quarter_count": 0,
                "holding_count": 0,
                "total_value": "0",
                "position_digest_algorithm": (
                    security_master_migration.POSITION_DIGEST_ALGORITHM
                ),
                "position_sha256": "same",
            },
            "mappings": {},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            rebuild_outputs = mock.Mock()
            with (
                mock.patch(
                    "sys.argv",
                    [
                        "pipeline.py",
                        "--regenerate-only",
                        "--rebuild-security-master",
                    ],
                ),
                mock.patch.multiple(
                    pipeline,
                    DATA_DIR=data_dir,
                    FUNDS_DIR=data_dir / "funds",
                    STOCKS_DIR=data_dir / "stocks",
                    USER_AGENT="SEC cutover test ops@example.org",
                    SEC_SECURITY_MASTER_MIGRATION_REPORT_PATH=(
                        data_dir / "migration.json"
                    ),
                ),
                mock.patch.object(pipeline, "load_state", return_value={}),
                mock.patch.object(pipeline, "enforce_published_quarter_health"),
                mock.patch.object(pipeline, "save_state"),
                mock.patch.object(
                    pipeline,
                    "capture_cutover_projection",
                    side_effect=[projection, projection],
                ),
                mock.patch.object(pipeline, "load_cusip_registry", return_value={}),
                mock.patch.object(pipeline, "rebuild_tickers_in_place"),
                mock.patch.object(
                    pipeline,
                    "rebuild_registry_backed_outputs",
                    rebuild_outputs,
                ),
                mock.patch.object(
                    pipeline,
                    "load_security_master",
                    return_value={"generated_at": "2026-09-05T00:00:00Z"},
                ),
                mock.patch.object(pipeline, "write_cutover_difference_report"),
            ):
                self.assertEqual(0, pipeline.main())

        rebuild_outputs.assert_called_once_with(
            preserve_position_economics=True,
            apply_quantity_policy=False,
        )

    def test_clean_rebuild_can_explicitly_apply_quantity_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            outputs = mock.Mock()
            with mock.patch("sys.argv", ["pipeline.py", "--regenerate-only", "--rebuild-security-master", "--apply-quantity-policy"]), mock.patch.multiple(
                pipeline, DATA_DIR=data_dir, FUNDS_DIR=data_dir / "funds", STOCKS_DIR=data_dir / "stocks",
                USER_AGENT="test ops@example.org", load_state=mock.Mock(return_value={}),
                enforce_published_quarter_health=mock.Mock(), save_state=mock.Mock(),
                rebuild_tickers_in_place=mock.Mock(), rebuild_registry_backed_outputs=outputs,
                capture_cutover_projection=mock.Mock(return_value=None),
                load_cusip_registry=mock.Mock(return_value={}),
            ):
                self.assertEqual(0, pipeline.main())
            self.assertTrue(outputs.call_args.kwargs["preserve_position_economics"])
            self.assertTrue(outputs.call_args.kwargs["apply_quantity_policy"])

    def test_incremental_cli_applies_quantities_after_identity_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            outputs = mock.Mock()
            with mock.patch("sys.argv", ["pipeline.py", "--regenerate-only", "--refresh-security-master"]), mock.patch.multiple(
                pipeline, DATA_DIR=data_dir, FUNDS_DIR=data_dir / "funds", STOCKS_DIR=data_dir / "stocks",
                USER_AGENT="test ops@example.org", load_state=mock.Mock(return_value={}),
                enforce_published_quarter_health=mock.Mock(), save_state=mock.Mock(),
                rebuild_tickers_in_place=mock.Mock(), rebuild_registry_backed_outputs=outputs,
            ):
                self.assertEqual(0, pipeline.main())
            self.assertTrue(outputs.call_args.kwargs["preserve_position_economics"])
            self.assertTrue(outputs.call_args.kwargs["apply_quantity_policy"])

    def test_incremental_quantity_policy_keeps_saved_position_identity(self) -> None:
        registry = pipeline.CusipRegistry({}, observed_cusips=frozenset())
        canonicalize, repair = mock.Mock(), mock.Mock()
        with mock.patch.multiple(
            pipeline, build_cusip_registry=mock.Mock(return_value=registry),
            write_security_labels=mock.Mock(), validate_cusip_registry=mock.Mock(return_value=[]),
            canonicalize_fund_files=canonicalize, repair_zero_share_holdings_in_place=repair,
            upgrade_composition_hashes_in_place=mock.Mock(), regenerate_stock_files_and_index=mock.Mock(),
            write_ticker_health_report=mock.Mock(),
        ):
            pipeline.rebuild_registry_backed_outputs(preserve_position_economics=True, apply_quantity_policy=True)
        canonicalize.assert_called_once_with(preserve_position_identity=True)
        repair.assert_called_once_with()

    def test_registry_issues_block_derived_publication(self) -> None:
        canonicalize = mock.Mock()
        with mock.patch.multiple(
            pipeline,
            build_cusip_registry=mock.Mock(return_value=pipeline.CusipRegistry()),
            write_security_labels=mock.Mock(),
            validate_cusip_registry=mock.Mock(
                return_value=["ticker provenance mismatch"]
            ),
            canonicalize_fund_files=canonicalize,
        ):
            with self.assertRaisesRegex(
                pipeline.FundDataError,
                "SEC registry publication gate failed",
            ):
                pipeline.rebuild_registry_backed_outputs()
        canonicalize.assert_not_called()

    def test_registry_validation_reuses_builder_observed_cusips(self) -> None:
        observed_cusips = frozenset({"037833100"})
        registry = pipeline.CusipRegistry(
            {"037833100": {"ticker": "AAPL"}},
            observed_cusips=observed_cusips,
        )
        validator = mock.Mock(return_value=[])
        with mock.patch.multiple(
            pipeline,
            build_cusip_registry=mock.Mock(return_value=registry),
            write_security_labels=mock.Mock(),
            validate_cusip_registry=validator,
            canonicalize_fund_files=mock.Mock(),
            repair_zero_share_holdings_in_place=mock.Mock(),
            upgrade_composition_hashes_in_place=mock.Mock(),
            regenerate_stock_files_and_index=mock.Mock(),
            write_ticker_health_report=mock.Mock(),
        ):
            pipeline.rebuild_registry_backed_outputs()
        validator.assert_called_once_with(current_cusips=observed_cusips)

    def test_identity_is_canonicalized_before_zero_share_repair(self) -> None:
        calls: list[str] = []
        registry = pipeline.CusipRegistry({}, observed_cusips=frozenset())
        with mock.patch.multiple(
            pipeline,
            build_cusip_registry=mock.Mock(
                side_effect=lambda **_kwargs: calls.append("registry") or registry
            ),
            write_security_labels=mock.Mock(
                side_effect=lambda _registry: calls.append("labels")
            ),
            validate_cusip_registry=mock.Mock(
                side_effect=lambda **_kwargs: calls.append("validate") or []
            ),
            canonicalize_fund_files=mock.Mock(
                side_effect=lambda: calls.append("canonicalize")
            ),
            repair_zero_share_holdings_in_place=mock.Mock(
                side_effect=lambda: calls.append("zero_share")
            ),
            upgrade_composition_hashes_in_place=mock.Mock(
                side_effect=lambda: calls.append("upgrade")
            ),
            regenerate_stock_files_and_index=mock.Mock(
                side_effect=lambda: calls.append("stocks")
            ),
            write_ticker_health_report=mock.Mock(
                side_effect=lambda: calls.append("health")
            ),
        ):
            pipeline.rebuild_registry_backed_outputs()

        self.assertEqual(
            [
                "registry",
                "labels",
                "validate",
                "canonicalize",
                "zero_share",
                "upgrade",
                "stocks",
                "health",
            ],
            calls,
        )

    def test_security_master_publication_preserves_position_economics(
        self,
    ) -> None:
        registry = pipeline.CusipRegistry({}, observed_cusips=frozenset())
        canonicalize = mock.Mock()
        repair = mock.Mock()
        with mock.patch.multiple(
            pipeline,
            build_cusip_registry=mock.Mock(return_value=registry),
            write_security_labels=mock.Mock(),
            validate_cusip_registry=mock.Mock(return_value=[]),
            canonicalize_fund_files=canonicalize,
            repair_zero_share_holdings_in_place=repair,
            upgrade_composition_hashes_in_place=mock.Mock(),
            regenerate_stock_files_and_index=mock.Mock(),
            write_ticker_health_report=mock.Mock(),
        ):
            pipeline.rebuild_registry_backed_outputs(
                preserve_position_economics=True,
            )

        canonicalize.assert_called_once_with(
            preserve_position_identity=True,
        )
        repair.assert_not_called()


class CanonicalizationAndTypePreservationTests(unittest.TestCase):
    def test_security_master_ticker_rewrite_does_not_reclassify_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "123456.json"
            fund_path.write_text(json.dumps({
                "cik": 123456,
                "quarters": [{
                    "report_date": "2026-06-30",
                    "holdings": [{
                        "cusip": "111111118",
                        "ticker": "OLD",
                        "issuer": "EXAMPLE INC",
                        "class": "COM",
                        "holding_type": "CALL",
                        "option_type": "CALL",
                        "value": 100,
                        "shares": 10,
                    }],
                }],
            }))
            refresh_result = mock.Mock(master={})
            with (
                mock.patch.multiple(
                    pipeline,
                    FUNDS_DIR=funds_dir,
                    refresh_sec_security_master_from_funds=mock.Mock(
                        return_value=refresh_result,
                    ),
                ),
                mock.patch.object(
                    pipeline,
                    "_resolve_loaded_security",
                    return_value={
                        "mapping_status": "resolved",
                        "ticker": "NEW",
                    },
                ),
            ):
                self.assertEqual(
                    1,
                    pipeline.rebuild_tickers_in_place(
                        full_refresh=True,
                        refresh_master=True,
                    ),
                )

            holding = json.loads(
                fund_path.read_text()
            )["quarters"][0]["holdings"][0]
            self.assertEqual("NEW", holding["ticker"])
            self.assertEqual("CALL", holding["holding_type"])
            self.assertEqual("CALL", holding["option_type"])

    def test_cutover_canonicalization_preserves_projected_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            fund_path = funds_dir / "123456.json"
            fund_path.write_text(json.dumps({
                "cik": 123456,
                "quarters": [{
                    # Current-parser dating would normally allow the legacy
                    # CALL label to be reclassified from its COM class.
                    "report_date": "2026-06-30",
                    "holdings": [{
                        "cusip": "111111118",
                        "ticker": "OLD",
                        "issuer": "EXAMPLE INC",
                        "class": "COM",
                        "holding_type": "CALL",
                        "option_type": "CALL",
                        "value": 100,
                        "shares": 10,
                    }],
                }],
            }))
            registry = {
                "111111118": {
                    "ticker": "NEW",
                    "underlying_ticker": "NEW",
                    "name": "EXAMPLE INC",
                    "type": "EQUITY",
                }
            }
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                load_cusip_registry=mock.Mock(return_value=registry),
            ):
                self.assertEqual(
                    1,
                    pipeline.canonicalize_fund_files(
                        preserve_position_identity=True,
                    ),
                )

            holding = json.loads(
                fund_path.read_text()
            )["quarters"][0]["holdings"][0]
            self.assertEqual("CALL", holding["holding_type"])
            self.assertNotIn("option_type", holding)
            self.assertEqual("NEW", holding["ticker"])

    def test_canonicalization_clears_unproven_ticker_without_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            fund_path = funds_dir / "123456.json"
            fund_path.write_text(
                json.dumps(
                    {
                        "cik": 123456,
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "holdings": [
                                    {
                                        "cusip": "46090E103",
                                        "ticker": "QQQ",
                                        "issuer": "INVESCO QQQ",
                                        "reported_issuer": "INVESCO QQQ",
                                        "put_call": "CALL",
                                        "holding_type": "PUT",
                                        "option_type": "PUT",
                                        "value": 100,
                                        "shares": 10,
                                    }
                                ],
                            }
                        ],
                    }
                )
            )
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                load_cusip_registry=mock.Mock(return_value={}),
            ):
                self.assertEqual(1, pipeline.canonicalize_fund_files())

            holding = json.loads(fund_path.read_text())["quarters"][0]["holdings"][0]
            self.assertEqual("CALL", holding["holding_type"])
            self.assertNotIn("option_type", holding)
            self.assertIsNone(holding["ticker"])
            self.assertEqual("INVESCO QQQ", holding["issuer"])

    def test_stock_regeneration_never_reuses_unproven_holding_ticker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            fund_path = funds_dir / "123456.json"
            fund_path.write_text(json.dumps({
                "cik": 123456,
                "name": "Example Fund",
                "quarters": [{
                    "report_date": "2025-12-31",
                    "total_value": 100,
                    "holdings": [{
                        "cusip": "037833100",
                        "ticker": "UNPROVEN",
                        "issuer": "Unproven Vendor Label",
                        "reported_issuer": "APPLE INC",
                        "holding_type": "EQUITY",
                        "shares": 1,
                        "value": 100,
                    }],
                }],
            }))
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                STOCKS_DIR=stocks_dir,
                INDEX_PATH=data_dir / "index.json",
                FUNDS_INDEX_PATH=data_dir / "funds-index.json",
                load_cusip_registry=mock.Mock(return_value={}),
            ):
                pipeline.regenerate_stock_files_and_index(state={})

            stock = json.loads((stocks_dir / "037833100.json").read_text())
            index = json.loads((data_dir / "index.json").read_text())
            self.assertEqual("037833100", stock["ticker"])
            self.assertEqual("APPLE INC", stock["issuer"])
            self.assertEqual([], index["tickers"])

    def test_canonicalization_preserves_raw_filing_evidence_and_row_grain(self) -> None:
        reported_fields = {
            "reported_issuer": "INVESCO QQQ TRUST SERIES 1",
            "reported_class": "UNIT SER 1",
            "reported_cusip": "46090E103",
            "reported_figi": "BBG000BDTBL9",
            "accession": "0000000000-26-000001",
            "report_date": "2025-12-31",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            fund_path = funds_dir / "123456.json"
            fund_path.write_text(
                json.dumps(
                    {
                        "cik": 123456,
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "composition_hash": "unchanged",
                                "holdings": [
                                    {
                                        "cusip": "46090E103",
                                        "issuer": "INVESCO QQQ",
                                        "class": "UNIT SER 1",
                                        "put_call": "CALL",
                                        "holding_type": "PUT",
                                        "value": 60,
                                        "shares": 6,
                                        **reported_fields,
                                    },
                                    {
                                        "cusip": "46090E103",
                                        "issuer": "INVESCO QQQ",
                                        "class": "CALL",
                                        "option_type": "CALL",
                                        "value": 40,
                                        "shares": 4,
                                        **reported_fields,
                                    },
                                ],
                            }
                        ],
                    }
                )
            )
            registry = {
                "46090E103": {
                    "ticker": "QQQ",
                    "underlying_ticker": "QQQ",
                    "name": "INVESCO QQQ TRUST",
                    "type": "EQUITY",
                }
            }
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                load_cusip_registry=mock.Mock(return_value=registry),
            ):
                self.assertEqual(1, pipeline.canonicalize_fund_files())

            quarter = json.loads(fund_path.read_text())["quarters"][0]
            self.assertEqual("unchanged", quarter["composition_hash"])
            self.assertEqual(2, len(quarter["holdings"]))
            for holding in quarter["holdings"]:
                self.assertEqual("CALL", holding["holding_type"])
                self.assertEqual("QQQ", holding["ticker"])
                self.assertEqual("INVESCO QQQ TRUST", holding["issuer"])
                self.assertNotIn("option_type", holding)
                for field, value in reported_fields.items():
                    self.assertEqual(value, holding[field])

    def test_current_parser_provenance_separates_equity_from_same_cusip_put(self) -> None:
        quarter = {
            "report_date": "2025-12-31",
            "filing_date": "2026-05-01",
            "applied_accessions": ["source-1"],
            "source_filings": [
                {
                    "accession": "source-1",
                    "filing_date": "2026-05-01",
                    "applied": True,
                }
            ],
        }
        explicit_put = {
            "cusip": "46090E103",
            "class": "ETF",
            "put_call": "PUT",
            "holding_type": "PUT",
        }
        underlying_equity = {
            "cusip": "46090E103",
            "class": "ETF",
            "holding_type": "PUT",
        }
        self.assertEqual(
            "PUT",
            pipeline._canonical_holding_type_for_quarter(quarter, explicit_put),
        )
        self.assertEqual(
            "EQUITY",
            pipeline._canonical_holding_type_for_quarter(quarter, underlying_equity),
        )

    def test_legacy_option_without_raw_side_requires_migration_evidence(self) -> None:
        holding = {
            "cusip": "46090E103",
            "issuer": "INVESCO QQQ TRUST, SERIES 1",
            "class": "ETF",
            "holding_type": "PUT",
        }
        self.assertEqual("PUT", pipeline.classify_saved_holding(holding))
        self.assertEqual(
            "EQUITY",
            pipeline.classify_saved_holding(
                holding,
                allow_missing_option_side_reclassification=True,
            ),
        )
        self.assertEqual(
            "CALL",
            pipeline.classify_saved_holding(
                {**holding, "class": "CALL", "holding_type": "PUT"}
            ),
        )

    def test_explicit_non_equity_classes_are_preserved(self) -> None:
        cases = (
            ("CNV", "037833100", "NOTE"),
            ("US TREASURY", "91282CJL6", "NOTE"),
            ("*W EXP 06/01/2030", "037833100", "WARRANT"),
            ("PREFERRED STOCK", "037833100", "PREF"),
            ("COMMON STOCK WARRANTS", "037833100", "WARRANT"),
        )
        for security_class, cusip, expected in cases:
            with self.subTest(security_class=security_class):
                self.assertEqual(
                    expected,
                    pipeline.classify_saved_holding(
                        {
                            "issuer": "EXAMPLE ISSUER",
                            "class": security_class,
                            "cusip": cusip,
                            "holding_type": "EQUITY",
                        }
                    ),
                )

    def test_note_terms_cannot_be_invented_across_fields(self) -> None:
        axiom_unit = {
            "issuer": "Axiom Intelligence Acquisition Corp 1",
            "class": "06/10/2030",
            "cusip": "G0750N120",
            "holding_type": "EQUITY",
        }
        self.assertEqual("EQUITY", pipeline._classify_holding(axiom_unit))
        self.assertEqual("EQUITY", pipeline.classify_saved_holding(axiom_unit))
        for debt_row in (
            {"issuer": "EXAMPLE ISSUER 1 06/10/2030", "class": "SECURITY"},
            {"issuer": "EXAMPLE ISSUER", "class": "1 06/10/2030"},
        ):
            with self.subTest(debt_row=debt_row):
                self.assertEqual("NOTE", pipeline._classify_holding(debt_row))

    def test_hash_bound_type_is_immutable_during_regeneration(self) -> None:
        holding = {
            "issuer": "EXAMPLE ISSUER",
            "class": "NOTE",
            "cusip": "123456789",
            "holding_type": "EQUITY",
        }
        self.assertEqual(
            "EQUITY",
            pipeline._canonical_holding_type_for_quarter(
                {"composition_hash_version": pipeline.COMPOSITION_HASH_VERSION},
                holding,
            ),
        )
        self.assertEqual(
            "NOTE",
            pipeline._canonical_holding_type_for_quarter(
                {"composition_hash_version": 1},
                holding,
            ),
        )

    def test_fund_strategy_words_do_not_override_equity_class(self) -> None:
        for security_class in (
            "CONV",
            "CONVERTIBLE EQTY",
            "CONVERTIBLE TOTAL RETURN FUND",
            "CONVERTIBLE BOND ETF",
            "US TREASURY ETF",
        ):
            with self.subTest(security_class=security_class):
                self.assertEqual(
                    "EQUITY",
                    pipeline.classify_saved_holding(
                        {
                            "issuer": "EXAMPLE FUND",
                            "class": security_class,
                            "cusip": "922040845",
                            "holding_type": "EQUITY",
                        }
                    ),
                )

    def test_ambiguous_saved_non_equity_type_is_preserved(self) -> None:
        for security_class, saved_type in (
            ("SECURITY", "NOTE"),
            ("ADR", "PREF"),
            ("7.25% DEP SHS A", "NOTE"),
            ("COM NOTE 0.250% 9/1", "NOTE"),
        ):
            with self.subTest(security_class=security_class, saved_type=saved_type):
                self.assertEqual(
                    saved_type,
                    pipeline.classify_saved_holding(
                        {"class": security_class, "holding_type": saved_type}
                    ),
                )

    def test_stock_regeneration_aggregates_class_variants_by_exact_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            (funds_dir / "123456.json").write_text(
                json.dumps(
                    {
                        "cik": 123456,
                        "name": "Example Fund",
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "filing_date": "2026-02-13",
                                "total_value": 100,
                                "num_holdings": 2,
                                "holdings": [
                                    {
                                        "cusip": "46090E103",
                                        "issuer": "INVESCO QQQ",
                                        "class": "UNIT SER 1",
                                        "value": 60,
                                        "shares": 6,
                                        "put_call": "CALL",
                                        "holding_type": "PUT",
                                    },
                                    {
                                        "cusip": "46090E103",
                                        "issuer": "INVESCO QQQ",
                                        "class": "COM",
                                        "value": 40,
                                        "shares": 4,
                                        "put_call": "CALL",
                                        "holding_type": "CALL",
                                    },
                                ],
                            }
                        ],
                    }
                )
            )
            registry = {
                "46090E103": {
                    "ticker": "QQQ",
                    "name": "INVESCO QQQ TRUST",
                    "type": "EQUITY",
                    "underlying_ticker": "QQQ",
                    "underlying_ticker_source": "sec_ftd",
                    "underlying_ticker_as_of": "2026-06-30",
                }
            }
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                STOCKS_DIR=stocks_dir,
                INDEX_PATH=data_dir / "index.json",
                FUNDS_INDEX_PATH=data_dir / "funds-index.json",
                load_cusip_registry=mock.Mock(return_value=registry),
            ):
                pipeline.canonicalize_fund_files()
                pipeline.regenerate_stock_files_and_index(state={})

            call_path = stocks_dir / "46090E103__CALL.json"
            self.assertTrue(call_path.exists())
            self.assertFalse((stocks_dir / "46090E103__PUT.json").exists())
            call_stock = json.loads(call_path.read_text())
            self.assertEqual("46090E103|CALL", call_stock["stock_id"])
            self.assertEqual("CALL", call_stock["instrument_type"])
            history = call_stock["holders"][0]["history"][0]
            self.assertEqual(100, history["value"])
            self.assertEqual(10, history["shares"])


class ZeroShareIdentityTests(unittest.TestCase):
    def test_repair_does_not_mix_same_cusip_option_prices(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            report_date = "2025-12-31"
            (funds_dir / "100.json").write_text(
                json.dumps(
                    {
                        "cik": 100,
                        "quarters": [
                            {
                                "report_date": report_date,
                                "holdings": [
                                    {
                                        "cusip": "037833100",
                                        "class": "COM",
                                        "value": 100,
                                        "shares": 10,
                                        "holding_type": "EQUITY",
                                    },
                                    {
                                        "cusip": "037833100",
                                        "class": "COM",
                                        "value": 1000,
                                        "shares": 10,
                                        "put_call": "CALL",
                                        "holding_type": "CALL",
                                    },
                                ],
                            }
                        ],
                    }
                )
            )
            target_path = funds_dir / "200.json"
            target_path.write_text(
                json.dumps(
                    {
                        "cik": 200,
                        "quarters": [
                            {
                                "report_date": report_date,
                                "holdings": [
                                    {
                                        "cusip": "037833100",
                                        "class": "COM",
                                        "value": 50,
                                        "shares": 0,
                                        "holding_type": "EQUITY",
                                    }
                                ],
                            }
                        ],
                    }
                )
            )
            for path in funds_dir.glob("*.json"):
                fund = json.loads(path.read_text())
                quarter = fund["quarters"][0]
                quarter["reported_identity_sources"] = [{"accession": "a", "url": "https://www.sec.gov/Archives/a", "sha256": "a" * 64}]
                for row in quarter["holdings"]:
                    row.update({"share_amount_type": "SH", "accession": "a"})
                path.write_text(json.dumps(fund))
            reference = json.loads((funds_dir / "100.json").read_text())
            for cik in (101, 102):
                reference["cik"] = cik
                (funds_dir / f"{cik}.json").write_text(json.dumps(reference))
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
            ):
                self.assertEqual(1, pipeline.repair_zero_share_holdings_in_place())

            holding = json.loads(target_path.read_text())["quarters"][0]["holdings"][0]
            self.assertEqual(5, holding["shares"])
            self.assertEqual(0, holding["reported_shares"])
            self.assertTrue(holding["shares_imputed"])

    def test_validation_does_not_mix_same_cusip_instruments(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            (funds_dir / "100.json").write_text(
                json.dumps(
                    {
                        "cik": 100,
                        "name": "Example Fund",
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "filing_date": "2026-02-13",
                                "total_value": 150,
                                "num_holdings": 2,
                                "holdings": [
                                    {
                                        "cusip": "037833100",
                                        "holding_type": "EQUITY",
                                        "value": 100,
                                        "shares": 10,
                                    },
                                    {
                                        "cusip": "037833100",
                                        "holding_type": "CALL",
                                        "put_call": "CALL",
                                        "value": 50,
                                        "shares": 0,
                                    },
                                ],
                            }
                        ],
                    }
                )
            )
            errors: list[str] = []
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                validate_data.validate_funds(errors, {})
            self.assertEqual([], errors)

    def test_validator_requires_reproducible_imputed_shares(self) -> None:
        import quantity_estimation as quantity
        peers = [{"cik": str(cik), "report_date": "2025-12-31", "cusip": "037833100", "instrument_type": "EQUITY", "unit": "SH", "value": 1000, "quantity": 10, "source": {"accession": "a", "url": "https://www.sec.gov/Archives/a", "sha256": "a" * 64}} for cik in (100, 101, 102)]
        reference = quantity.peer_reference(("2025-12-31", "037833100", "EQUITY"), peers, exclude_cik="200")
        reference_id = quantity.canonical_json_hash(reference)

        def validate_target(target_holding: dict) -> list[str]:
            with tempfile.TemporaryDirectory() as tmpdir:
                funds_dir = Path(tmpdir) / "funds"
                funds_dir.mkdir()
                report_date = "2025-12-31"
                (funds_dir / "100.json").write_text(
                    json.dumps(
                        {
                            "cik": 100,
                            "name": "Reference Fund",
                            "quarters": [
                                {
                                    "report_date": report_date,
                                    "filing_date": "2026-02-13",
                                    "total_value": 1000,
                                    "num_holdings": 1,
                                    "holdings": [
                                        {
                                            "cusip": "037833100",
                                            "holding_type": "EQUITY",
                                            "value": 1000,
                                            "shares": 10,
                                        }
                                    ],
                                }
                            ],
                        }
                    )
                )
                (funds_dir / "200.json").write_text(
                    json.dumps(
                        {
                            "cik": 200,
                            "name": "Target Fund",
                            "quarters": [
                                {
                                    "report_date": report_date,
                                    "filing_date": "2026-02-13",
                                    "total_value": 250,
                                    "num_holdings": 1,
                                    "holdings": [target_holding],
                                }
                            ],
                        }
                    )
                )
                quantity.atomic_json(Path(tmpdir) / ".cache/quantity_estimation_evidence.json", {"schema_version": 1, "references": {reference_id: reference}})
                errors: list[str] = []
                with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                    validate_data.validate_funds(errors, {})
                return errors

        valid = {
            "cusip": "037833100",
            "holding_type": "EQUITY",
            "value": 250,
            "shares": 2.5,
            "reported_shares": 0,
            "shares_imputed": True,
            "share_amount_type": "SH",
            "quantity_estimate": {"policy_version": 1, "reference_id": reference_id, "method": "sec_same_quarter_median", "unit": "SH"},
        }
        self.assertEqual([], validate_target(valid))
        self.assertTrue(
            any(
                "expected literal true" in error
                for error in validate_target({**valid, "shares_imputed": False})
            )
        )
        self.assertTrue(
            any(
                "must preserve reported zero" in error
                for error in validate_target({**valid, "reported_shares": 1})
            )
        )
        self.assertTrue(
            any(
                "does not reproduce its frozen reference" in error
                for error in validate_target({**valid, "shares": 2.500001})
            )
        )
        self.assertTrue(
            any(
                "different security or quarter" in error
                for error in validate_target(
                    {**valid, "holding_type": "CALL", "put_call": "CALL"}
                )
            )
        )


class IdentityValidationTests(unittest.TestCase):
    def test_fund_holding_validator_requires_canonical_option_identity_and_cusip(
        self,
    ) -> None:
        errors: list[str] = []
        lookup_id = validate_data.validate_fund_holding_identity(
            {
                "cusip": "46090E103",
                "holding_type": "PUT",
                "put_call": "CALL",
                "option_type": "PUT",
            },
            "fixture holding",
            errors,
        )
        self.assertEqual("46090E103|CALL", lookup_id)
        self.assertTrue(any("obsolete option_type" in error for error in errors))
        self.assertTrue(
            any(
                "put_call CALL inconsistent with holding_type PUT" in error
                for error in errors
            )
        )

        errors.clear()
        validate_data.validate_fund_holding_identity(
            {"ticker": "AAPL", "holding_type": "EQUITY"},
            "fixture holding",
            errors,
        )
        self.assertTrue(
            any("invalid canonical cusip None" in error for error in errors)
        )

    def test_stock_validator_rejects_suffix_payload_type_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stocks_dir = Path(tmpdir) / "stocks"
            stocks_dir.mkdir()
            (stocks_dir / "46090E103__CALL.json").write_text(
                json.dumps(
                    {
                        "stock_id": "46090E103|CALL",
                        "cusip": "46090E103",
                        "ticker": "QQQ",
                        "issuer": "INVESCO QQQ TRUST",
                        "instrument_type": "PUT",
                        "holders": [],
                    }
                )
            )
            errors: list[str] = []
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(errors)

        self.assertTrue(
            any(
                "exact cusip/type identity is 46090E103|PUT" in error
                for error in errors
            )
        )
        self.assertTrue(
            any(
                "exact cusip/type filename 46090E103__PUT.json" in error
                for error in errors
            )
        )

    def test_stock_validator_rejects_orphan_stock_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stocks_dir = Path(tmpdir)
            (stocks_dir / "037833100.json").write_text(
                json.dumps(
                    {
                        "stock_id": "037833100",
                        "cusip": "037833100",
                        "instrument_type": "EQUITY",
                        "holders": [],
                    }
                )
            )
            errors: list[str] = []
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(errors, {}, {})

        self.assertTrue(
            any("has no retained fund position" in error for error in errors)
        )


class SecFundSeriesMetadataTests(unittest.TestCase):
    PAGE = """
    <table>
      <tr><td colspan="3"><b>CIK</b></td><td></td><td></td></tr>
      <tr><td></td><td colspan="2"><b>Series</b></td>
        <td></td><td><b>Ticker</b></td></tr>
      <tr><td></td><td></td><td><b>Class/Contract</b></td>
        <td><b>Name</b></td><td><b>Symbol</b></td></tr>
      <tr><td></td><td colspan="2"><a href="?CIK=S000055059">
        S000055059</a></td><td>
        iShares iBonds Dec 2026 Term Corporate ETF
      </td><td></td></tr>
      <tr><td></td><td></td><td><a href="?CIK=C000173141">
        C000173141</a></td><td>
        iShares iBonds Dec 2026 Term Corporate ETF
      </td><td>IBDR</td></tr>
      <tr><td></td><td colspan="2"><a href="?CIK=S000063115">
        S000063115</a></td><td>
        iShares iBonds Dec 2026 Term Muni Bond ETF
      </td><td></td></tr>
      <tr><td></td><td></td><td><a href="?CIK=C000204676">
        C000204676</a></td><td>
        iShares iBonds Dec 2026 Term Muni Bond ETF
      </td><td>IBMO</td></tr>
      <tr><td></td><td colspan="2"><a href="?CIK=S000008999">
        S000008999</a></td><td>AMERICAN MUTUAL FUND</td><td></td></tr>
      <tr><td></td><td></td><td><a href="?CIK=C000068556">
        C000068556</a></td><td>Class F-2</td><td>AMRFX</td></tr>
    </table>
    """

    def test_series_parser_extracts_exact_series_and_class_names(self) -> None:
        series_names, class_names = pipeline._parse_sec_fund_series_page(self.PAGE)
        self.assertEqual(
            "iShares iBonds Dec 2026 Term Corporate ETF",
            series_names["S000055059"],
        )
        self.assertEqual(
            "iShares iBonds Dec 2026 Term Corporate ETF",
            class_names["C000173141"],
        )
        self.assertEqual(
            "AMERICAN MUTUAL FUND",
            series_names["S000008999"],
        )
        self.assertEqual(
            "Class F-2",
            class_names["C000068556"],
        )

    def test_series_parser_fails_closed_on_bad_headers_and_conflicts(self) -> None:
        missing_header = """
        <table><tr><td><a href="?CIK=S000055059">S000055059</a></td>
        <td>Fund Name</td><td>IBDR</td></tr></table>
        """
        ambiguous_header = """
        <table><tr><td><b>Name</b></td><td><b>Name</b></td></tr>
        <tr><td><a href="?CIK=S000055059">S000055059</a></td>
        <td>Fund Name</td></tr></table>
        """
        conflicting_names = """
        <table>
          <tr><td colspan="3"><b>CIK</b></td><td></td><td></td></tr>
          <tr><td></td><td colspan="2"><b>Series</b></td>
            <td></td><td><b>Ticker</b></td></tr>
          <tr><td></td><td></td><td><b>Class/Contract</b></td>
            <td><b>Name</b></td><td><b>Symbol</b></td></tr>
          <tr><td></td><td colspan="2"><a href="?CIK=S000000001">
            S000000001</a></td><td>First Name</td></tr>
          <tr><td></td><td colspan="2"><a href="?CIK=S000000001">
            S000000001</a></td><td>Conflicting Name</td></tr>
        </table>
        """
        for changed_layout in (
            missing_header,
            ambiguous_header,
            conflicting_names,
            "<html><body><table><tr><th>Fund identifier</th>"
            "<th>Fund title</th></tr><tr><td>S000055059</td>"
            "<td>Fund Name</td></tr></table></body></html>",
        ):
            with self.subTest(changed_layout=changed_layout):
                with self.assertRaises(pipeline.SourceSchemaError):
                    pipeline._parse_sec_fund_series_page(changed_layout)

        with self.assertRaises(pipeline.SourceParseError):
            pipeline._parse_sec_fund_series_page("<html><body>Busy</body></html>")

class SyntheticIdentifierTests(unittest.TestCase):
    def test_synthetic_identifiers_remain_visible_but_unresolved(self) -> None:
        self.assertTrue(is_synthetic_identifier("OOOOOOOOO"))
        self.assertTrue(is_synthetic_identifier("0000AAPL"))
        self.assertEqual("AAPL", synthetic_identifier_ticker_hint("0000AAPL"))
        self.assertIsNone(synthetic_identifier_ticker_hint("037833100"))


if __name__ == "__main__":
    unittest.main()
