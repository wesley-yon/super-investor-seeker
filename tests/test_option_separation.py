import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pipeline
import validate_data


INFO_TABLE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>100</value>
    <shrsOrPrnAmt>
      <sshPrnamt>10</sshPrnamt>
    </shrsOrPrnAmt>
  </infoTable>
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>50</value>
    <shrsOrPrnAmt>
      <sshPrnamt>5</sshPrnamt>
    </shrsOrPrnAmt>
    <putCall>CALL</putCall>
  </infoTable>
</informationTable>
"""


DECIMAL_SHARES_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>100</value>
    <shrsOrPrnAmt>
      <sshPrnamt>10.5</sshPrnamt>
    </shrsOrPrnAmt>
  </infoTable>
</informationTable>
"""


class PipelineIdentityTests(unittest.TestCase):
    def test_company_ticker_cache_follows_active_data_dir(self) -> None:
        payload = {
            "0": {
                "cik_str": 123456,
                "ticker": "TEST",
                "title": "TEST COMPANY",
            }
        }
        response = mock.Mock()
        response.json.return_value = payload

        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            original_data_dir = pipeline.DATA_DIR
            try:
                pipeline.DATA_DIR = data_dir
                with mock.patch.object(
                    pipeline.HTTP,
                    "get",
                    return_value=response,
                ):
                    loaded = pipeline._load_company_tickers_data()
            finally:
                pipeline.DATA_DIR = original_data_dir

            cache_path = data_dir / "company_tickers.json"
            self.assertEqual(payload, loaded)
            self.assertEqual(payload, json.loads(cache_path.read_text()))

    def test_parse_and_consolidate_keep_equity_and_call_separate(self) -> None:
        holdings = pipeline.parse_information_table(INFO_TABLE_XML)

        self.assertIsNotNone(holdings)
        self.assertEqual(["EQUITY", "CALL"], [h["holding_type"] for h in holdings])

        consolidated = pipeline.consolidate_holdings(holdings)
        self.assertEqual(2, len(consolidated))

        by_type = {h["holding_type"]: h for h in consolidated}
        self.assertEqual(100, by_type["EQUITY"]["value"])
        self.assertEqual(10, by_type["EQUITY"]["shares"])
        self.assertEqual(50, by_type["CALL"]["value"])
        self.assertEqual(5, by_type["CALL"]["shares"])
        self.assertEqual("CALL", by_type["CALL"]["put_call"])

    def test_classify_saved_holding_preserves_existing_option_type(self) -> None:
        holding = {
            "issuer": "APPLE INC",
            "cusip": "037833100",
            "class": "COM",
            "holding_type": "CALL",
            "value": 50,
            "shares": 5,
        }

        self.assertEqual("CALL", pipeline.classify_saved_holding(holding))

    def test_public_identity_normalizes_confirmed_bonds_and_funds(
        self,
    ) -> None:
        legacy_call = {
            "cusip": "343498AC5",
            "ticker": "FLO",
            "holding_type": "CALL",
            "put_call": "CALL",
        }
        bond_entry = {
            "type": "NOTE",
            "security_kind": "BOND",
        }
        common_entry = {
            "type": "EQUITY",
            "security_kind": "COMMON",
        }

        self.assertEqual(
            "NOTE",
            pipeline.published_holding_instrument_type(
                legacy_call,
                bond_entry,
            ),
        )
        self.assertEqual(
            "NOTE",
            validate_data.published_holding_instrument_type(
                legacy_call,
                bond_entry,
            ),
        )
        self.assertEqual(
            "343498AC5|NOTE",
            validate_data.holding_stock_id(
                legacy_call,
                {"343498AC5": bond_entry},
            ),
        )
        self.assertEqual(
            "CALL",
            pipeline.published_holding_instrument_type(
                legacy_call,
                common_entry,
            ),
        )
        for kind, raw_type in (
            ("ETF", "NOTE"),
            ("MUTUAL FUND", "PREF"),
            ("CLOSED-END FUND", "WARRANT"),
        ):
            holding = {
                "cusip": "921937827",
                "holding_type": raw_type,
            }
            registry_entry = {
                "type": "EQUITY",
                "security_kind": kind,
            }
            with self.subTest(kind=kind, raw_type=raw_type):
                self.assertEqual(
                    "EQUITY",
                    pipeline.published_holding_instrument_type(
                        holding,
                        registry_entry,
                    ),
                )
                self.assertEqual(
                    "EQUITY",
                    validate_data.published_holding_instrument_type(
                        holding,
                        registry_entry,
                    ),
                )
        for option_type in ("CALL", "PUT", "OPT"):
            option = {
                "cusip": "921937827",
                "holding_type": option_type,
                "put_call": option_type if option_type != "OPT" else "",
            }
            registry_entry = {
                "type": "EQUITY",
                "security_kind": "ETF",
            }
            with self.subTest(option_type=option_type):
                self.assertEqual(
                    option_type,
                    pipeline.published_holding_instrument_type(
                        option,
                        registry_entry,
                    ),
                )
                self.assertEqual(
                    option_type,
                    validate_data.published_holding_instrument_type(
                        option,
                        registry_entry,
                    ),
                )
        ambiguous_fund_entry = {
            "ticker": "BCOIX",
            "security_label": "BCOIX",
            "type": "EQUITY",
            "sources": ["cusip_map_vetted"],
        }
        self.assertEqual(
            "EQUITY",
            pipeline.published_holding_instrument_type(
                {"cusip": "057071870", "holding_type": "NOTE"},
                ambiguous_fund_entry,
            ),
        )
        self.assertEqual(
            "EQUITY",
            validate_data.published_holding_instrument_type(
                {"cusip": "057071870", "holding_type": "NOTE"},
                ambiguous_fund_entry,
            ),
        )
        self.assertEqual(
            "CALL",
            pipeline.published_holding_instrument_type(
                {"cusip": "057071870", "holding_type": "CALL"},
                ambiguous_fund_entry,
            ),
        )
        for non_fund_entry in (
            {
                "ticker": "AMCX",
                "security_label": "AMCX",
                "type": "EQUITY",
            },
            {
                "ticker": None,
                "security_label": "BCOIX",
                "type": "EQUITY",
            },
        ):
            with self.subTest(non_fund_entry=non_fund_entry):
                self.assertEqual(
                    "NOTE",
                    pipeline.published_holding_instrument_type(
                        {"cusip": "000000001", "holding_type": "NOTE"},
                        non_fund_entry,
                    ),
                )
        self.assertEqual("CALL", legacy_call["holding_type"])

    def test_parse_information_table_keeps_decimal_share_counts(self) -> None:
        holdings = pipeline.parse_information_table(DECIMAL_SHARES_XML)

        self.assertIsNotNone(holdings)
        self.assertEqual(10.5, holdings[0]["shares"])

    def test_update_cusip_map_repairs_duplicate_ticker_collision(self) -> None:
        holdings = [
            {
                "issuer": "SOUTHERN CO",
                "cusip": "842587107",
                "class": "COM",
                "value": 20,
                "shares": 2,
                "holding_type": "EQUITY",
            },
            {
                "issuer": "SOUTHERN MO BANCORP INC",
                "cusip": "843380106",
                "class": "COM",
                "value": 15,
                "shares": 1,
                "holding_type": "EQUITY",
            },
        ]
        cusip_map = {
            "842587107": "SO",
            "843380106": "SO",
        }

        with mock.patch.object(
            pipeline,
            "resolve_cusips_via_openfigi",
            return_value={"842587107": "SO", "843380106": "SMBC"},
        ) as mock_figi:
            pipeline.update_cusip_map(cusip_map, holdings)

        mock_figi.assert_called_once_with(["842587107", "843380106"])
        self.assertEqual("SO", holdings[0]["ticker"])
        self.assertEqual("SMBC", holdings[1]["ticker"])
        self.assertEqual("SMBC", cusip_map["843380106"])

    def test_update_cusip_map_rejects_plain_symbol_for_note_row(self) -> None:
        holdings = [
            {
                "issuer": "OBSCURE INSTRUMENT LTD",
                "cusip": "000000001",
                "class": "NOTE",
                "value": 15,
                "shares": 1,
                "holding_type": "NOTE",
            }
        ]
        cusip_map = {"000000001": "ZZZZ"}

        with mock.patch.object(pipeline, "resolve_cusips_via_openfigi") as mock_figi:
            pipeline.update_cusip_map(cusip_map, holdings)

        mock_figi.assert_not_called()
        self.assertIsNone(holdings[0]["ticker"])
        self.assertEqual("ZZZZ", cusip_map["000000001"])

    def test_update_cusip_map_applies_manual_override_before_openfigi(self) -> None:
        holdings = [
            {
                "issuer": "CYBERARK SOFTWARE LTD",
                "cusip": "M2682V108",
                "class": "SHS",
                "value": 15,
                "shares": 1,
                "holding_type": "EQUITY",
            }
        ]
        cusip_map = {}

        with mock.patch.object(pipeline, "resolve_cusips_via_openfigi") as mock_figi:
            pipeline.update_cusip_map(cusip_map, holdings)

        mock_figi.assert_not_called()
        self.assertEqual("CYBR", holdings[0]["ticker"])
        self.assertEqual("CYBR", cusip_map["M2682V108"])

    def test_stock_files_are_cusip_keyed_and_split_by_instrument_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_dir = tmp / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            stocks_dir.mkdir(parents=True)

            fund = {
                "cik": 123456,
                "name": "Example Fund",
                "quarters": [
                    {
                        "report_date": "2025-12-31",
                        "filing_date": "2026-02-14",
                        "total_value": 41_990,
                        "num_holdings": 13,
                        "holdings": [
                            {
                                "ticker": "AAPL",
                                "issuer": "APPLE INC",
                                "cusip": "037833100",
                                "class": "COM",
                                "value": 100,
                                "shares": 10,
                                "holding_type": "EQUITY",
                            },
                            {
                                "ticker": "AAPL",
                                "issuer": "APPLE INC",
                                "cusip": "037833100",
                                "class": "COM",
                                "value": 50,
                                "shares": 5,
                                "holding_type": "CALL",
                                "put_call": "CALL",
                            },
                            {
                                "ticker": "BLK",
                                "issuer": "BLACKROCK INC",
                                "cusip": "09290D101",
                                "class": "COM",
                                "value": 75,
                                "shares": 3,
                                "holding_type": "EQUITY",
                            },
                            {
                                "ticker": "BLK",
                                "issuer": "ISHARES INC",
                                "cusip": "46434G772",
                                "class": "MSCI TAIWAN ETF",
                                "value": 25,
                                "shares": 1,
                                "holding_type": "EQUITY",
                            },
                            {
                                "ticker": "FLO",
                                "issuer": "FLOWSERVE CORP",
                                "cusip": "343498AC5",
                                "class": "5.900% SR NT 2030",
                                "value": 25,
                                "shares": 1,
                                "holding_type": "CALL",
                                "put_call": "CALL",
                            },
                            {
                                "ticker": None,
                                "issuer": "PRUDENTIAL FINL INC",
                                "cusip": "744320888",
                                "class": "COMMON STOCK",
                                "value": 35_617,
                                "shares": 4,
                                "holding_type": "EQUITY",
                            },
                            {
                                "ticker": None,
                                "issuer": "PRUDENTIAL FINL INC",
                                "cusip": "744320888",
                                "class": "PREFERRED STOCK",
                                "value": 6_008,
                                "shares": 3,
                                "holding_type": "PREF",
                            },
                            {
                                "ticker": None,
                                "issuer": "VANGUARD BD INDEX FDS",
                                "cusip": "921937827",
                                "class": "SHORT TRM BOND",
                                "value": 20,
                                "shares": 2,
                                "holding_type": "NOTE",
                            },
                            {
                                "ticker": "BSV",
                                "issuer": "VANGUARD BD INDEX FDS",
                                "cusip": "921937827",
                                "class": "ETF",
                                "value": 30,
                                "shares": 3,
                                "holding_type": "EQUITY",
                            },
                            {
                                "ticker": "BSV",
                                "issuer": "VANGUARD BD INDEX FDS",
                                "cusip": "921937827",
                                "class": "CALL",
                                "value": 10,
                                "shares": 1,
                                "holding_type": "CALL",
                                "put_call": "CALL",
                            },
                            {
                                "ticker": "PFF",
                                "issuer": "ISHARES TR",
                                "cusip": "464288687",
                                "class": "PFD AND INCM SEC",
                                "value": 15,
                                "shares": 2,
                                "holding_type": "PREF",
                            },
                            {
                                "ticker": "PFF",
                                "issuer": "ISHARES TR",
                                "cusip": "464288687",
                                "class": "NOTE",
                                "value": 5,
                                "shares": 1,
                                "holding_type": "NOTE",
                            },
                            {
                                "ticker": "PFF",
                                "issuer": "ISHARES TR",
                                "cusip": "464288687",
                                "class": "PUT",
                                "value": 10,
                                "shares": 1,
                                "holding_type": "PUT",
                                "put_call": "PUT",
                            },
                        ],
                    }
                ],
            }
            (funds_dir / "123456.json").write_text(json.dumps(fund, indent=2))

            original_data_dir = pipeline.DATA_DIR
            original_funds_dir = pipeline.FUNDS_DIR
            original_stocks_dir = pipeline.STOCKS_DIR
            original_index_path = pipeline.INDEX_PATH
            original_funds_index_path = pipeline.FUNDS_INDEX_PATH
            original_registry_path = pipeline.CUSIP_REGISTRY_PATH
            original_legacy_registry_path = (
                pipeline.LEGACY_CUSIP_REGISTRY_PATH
            )
            registry_path = data_dir / "fixture-cusip-registry.json"
            registry_path.write_text(json.dumps({
                "037833100": {
                    "ticker": "AAPL",
                    "name": "APPLE INC",
                },
                "09290D101": {
                    "ticker": "BLK",
                    "name": "BLACKROCK INC",
                },
                "46434G772": {
                    "ticker": "BLK",
                    "name": "ISHARES INC",
                },
                "343498AC5": {
                    "ticker": None,
                    "name": "FLOWSERVE CORP",
                    "type": "NOTE",
                    "security_kind": "BOND",
                },
                "744320888": {
                    "ticker": None,
                    "name": "PRUDENTIAL FINL INC",
                    "type": "NOTE",
                    "security_kind": "BOND",
                },
                "921937827": {
                    "ticker": "BSV",
                    "name": "VANGUARD SHORT-TERM BOND ETF",
                    "type": "EQUITY",
                    "security_kind": "ETF",
                },
                "464288687": {
                    "ticker": "PFF",
                    "name": "ISHARES PREFERRED AND INCOME SECURITIES ETF",
                    "type": "EQUITY",
                    "security_kind": "ETF",
                },
            }))
            try:
                pipeline.DATA_DIR = data_dir
                pipeline.FUNDS_DIR = funds_dir
                pipeline.STOCKS_DIR = stocks_dir
                pipeline.INDEX_PATH = data_dir / "index.json"
                pipeline.FUNDS_INDEX_PATH = data_dir / "funds-index.json"
                pipeline.CUSIP_REGISTRY_PATH = registry_path
                pipeline.LEGACY_CUSIP_REGISTRY_PATH = (
                    data_dir / "missing-public-registry.json"
                )
                pipeline.regenerate_stock_files_and_index()
            finally:
                pipeline.DATA_DIR = original_data_dir
                pipeline.FUNDS_DIR = original_funds_dir
                pipeline.STOCKS_DIR = original_stocks_dir
                pipeline.INDEX_PATH = original_index_path
                pipeline.FUNDS_INDEX_PATH = original_funds_index_path
                pipeline.CUSIP_REGISTRY_PATH = original_registry_path
                pipeline.LEGACY_CUSIP_REGISTRY_PATH = (
                    original_legacy_registry_path
                )

            equity_stock = json.loads((stocks_dir / "037833100.json").read_text())
            call_stock = json.loads((stocks_dir / "037833100__CALL.json").read_text())
            blk_stock = json.loads((stocks_dir / "09290D101.json").read_text())
            ishare_stock = json.loads((stocks_dir / "46434G772.json").read_text())
            bond_stock = json.loads(
                (stocks_dir / "343498AC5__NOTE.json").read_text()
            )
            prudential_note_stock = json.loads(
                (stocks_dir / "744320888__NOTE.json").read_text()
            )
            bsv_stock = json.loads(
                (stocks_dir / "921937827.json").read_text()
            )
            bsv_call = json.loads(
                (stocks_dir / "921937827__CALL.json").read_text()
            )
            pff_stock = json.loads(
                (stocks_dir / "464288687.json").read_text()
            )
            pff_put = json.loads(
                (stocks_dir / "464288687__PUT.json").read_text()
            )
            index = json.loads((data_dir / "index.json").read_text())
            funds_index = json.loads((data_dir / "funds-index.json").read_text())

            self.assertEqual(index["funds"], funds_index["funds"])
            self.assertEqual(index["last_updated"], funds_index["last_updated"])
            self.assertEqual(index["total_filers"], funds_index["total_filers"])
            self.assertEqual(index["total_tickers"], funds_index["total_tickers"])
            self.assertNotIn("tickers", funds_index)

            self.assertEqual("037833100", equity_stock["stock_id"])
            self.assertEqual("037833100", equity_stock["cusip"])
            self.assertEqual("AAPL", equity_stock["ticker"])
            self.assertEqual("EQUITY", equity_stock["instrument_type"])
            self.assertEqual(100, equity_stock["holders"][0]["history"][0]["value"])

            self.assertEqual("037833100|CALL", call_stock["stock_id"])
            self.assertEqual("CALL", call_stock["instrument_type"])
            self.assertEqual(50, call_stock["holders"][0]["history"][0]["value"])

            self.assertEqual("09290D101", blk_stock["stock_id"])
            self.assertEqual("BLACKROCK INC", blk_stock["issuer"])
            self.assertEqual(75, blk_stock["holders"][0]["history"][0]["value"])

            self.assertEqual("46434G772", ishare_stock["stock_id"])
            self.assertEqual("ISHARES INC", ishare_stock["issuer"])
            self.assertEqual(25, ishare_stock["holders"][0]["history"][0]["value"])
            self.assertEqual("343498AC5|NOTE", bond_stock["stock_id"])
            self.assertEqual("NOTE", bond_stock["instrument_type"])
            self.assertEqual(25, bond_stock["holders"][0]["history"][0]["value"])
            self.assertFalse((stocks_dir / "343498AC5__CALL.json").exists())
            self.assertEqual(
                "744320888|NOTE",
                prudential_note_stock["stock_id"],
            )
            self.assertEqual(
                "NOTE",
                prudential_note_stock["instrument_type"],
            )
            self.assertEqual("744320888", prudential_note_stock["ticker"])
            self.assertEqual(
                41_625,
                prudential_note_stock["holders"][0]["history"][0]["value"],
            )
            self.assertEqual(
                7,
                prudential_note_stock["holders"][0]["history"][0]["shares"],
            )
            self.assertFalse((stocks_dir / "744320888.json").exists())
            self.assertFalse(
                (stocks_dir / "744320888__PREF.json").exists()
            )
            self.assertEqual(50, bsv_stock["holders"][0]["history"][0]["value"])
            self.assertEqual(5, bsv_stock["holders"][0]["history"][0]["shares"])
            self.assertEqual(10, bsv_call["holders"][0]["history"][0]["value"])
            self.assertFalse((stocks_dir / "921937827__NOTE.json").exists())
            self.assertEqual(20, pff_stock["holders"][0]["history"][0]["value"])
            self.assertEqual(3, pff_stock["holders"][0]["history"][0]["shares"])
            self.assertEqual(10, pff_put["holders"][0]["history"][0]["value"])
            self.assertFalse((stocks_dir / "464288687__NOTE.json").exists())
            self.assertFalse((stocks_dir / "464288687__PREF.json").exists())

            holder_counts = {
                entry["stock_id"]: entry["holder_count"]
                for entry in index["tickers"]
            }
            self.assertEqual(
                {
                    "037833100": 1,
                    "037833100|CALL": 1,
                    "09290D101": 1,
                    "46434G772": 1,
                    "921937827": 1,
                    "921937827|CALL": 1,
                    "464288687": 1,
                    "464288687|PUT": 1,
                },
                holder_counts,
            )
            current_holder_counts = {
                entry["stock_id"]: entry["current_holder_count"]
                for entry in index["tickers"]
            }
            self.assertEqual(holder_counts, current_holder_counts)
            self.assertEqual(
                {"2025-12-31"},
                {
                    entry["last_seen"]
                    for entry in index["tickers"]
                },
            )
            stock_ids = {(entry["stock_id"], entry["ticker"]) for entry in index["tickers"]}
            self.assertEqual(
                {
                    ("037833100", "AAPL"),
                    ("037833100|CALL", "AAPL"),
                    ("09290D101", "BLK"),
                    ("46434G772", "BLK"),
                    ("921937827", "BSV"),
                    ("921937827|CALL", "BSV"),
                    ("464288687", "PFF"),
                    ("464288687|PUT", "PFF"),
                },
                stock_ids,
            )

            stock_files = {
                path.stem: path for path in stocks_dir.glob("*.json")
            }
            missing_bsv = json.loads(json.dumps(index))
            missing_bsv["tickers"] = [
                entry
                for entry in missing_bsv["tickers"]
                if entry["stock_id"] != "921937827"
            ]
            missing_bsv["total_tickers"] = len(missing_bsv["tickers"])
            errors: list[str] = []
            validate_data.validate_index(
                missing_bsv,
                {"123456": funds_dir / "123456.json"},
                stock_files,
                json.loads(registry_path.read_text()),
                errors,
                [],
            )
            self.assertTrue(
                any(
                    "ticker-backed listed-fund EQUITY rows" in error
                    for error in errors
                ),
                errors,
            )

            invalid_note_path = stocks_dir / "921937827__NOTE.json"
            invalid_note_path.write_text(json.dumps({
                **bsv_stock,
                "stock_id": "921937827|NOTE",
                "instrument_type": "NOTE",
            }))
            original_validate_stocks_dir = validate_data.STOCKS_DIR
            try:
                validate_data.STOCKS_DIR = stocks_dir
                errors = []
                validate_data.validate_stocks(
                    errors,
                    registry=json.loads(registry_path.read_text()),
                )
            finally:
                validate_data.STOCKS_DIR = original_validate_stocks_dir
                invalid_note_path.unlink()
            self.assertTrue(
                any(
                    "generated listed-fund stock artifacts" in error
                    for error in errors
                ),
                errors,
            )

            for field, invalid in (
                ("holder_count", 999),
                ("current_holder_count", 0),
                ("last_seen", "2025-09-30"),
                ("issuer", "WRONG ISSUER"),
            ):
                with self.subTest(recomputed_index_field=field):
                    broken = json.loads(json.dumps(index))
                    target = next(
                        entry
                        for entry in broken["tickers"]
                        if entry["stock_id"] == "037833100"
                    )
                    target[field] = invalid
                    errors: list[str] = []
                    validate_data.validate_index(
                        broken,
                        {"123456": funds_dir / "123456.json"},
                        stock_files,
                        {},
                        errors,
                        [],
                    )
                    self.assertTrue(
                        any(
                            f"{field}=" in error
                            and "expected" in error
                            for error in errors
                        ),
                        errors,
                    )

    def test_registry_identity_does_not_follow_option_notional(self) -> None:
        shared_cusip = {
            "total_value": 1_000,
            "put_call_value": {"CALL": 900},
            "class_value": {"COM": 1_000},
            "issuer_value": {"EXAMPLE INC": 1_000},
            "instrument_type_value": {"EQUITY": 100, "CALL": 900},
            "instrument_type_count": {"EQUITY": 1, "CALL": 1},
            "non_option_class_value": {"COM": 100},
            "non_option_class_count": {"COM": 1},
            "non_option_issuer_value": {"EXAMPLE INC": 100},
            "non_option_issuer_count": {"EXAMPLE INC": 1},
        }
        option_only_cusip = {
            "total_value": 900,
            "put_call_value": {"CALL": 900},
            "class_value": {"COM": 900},
            "issuer_value": {"EXAMPLE INC": 900},
            "instrument_type_value": {"CALL": 900},
            "instrument_type_count": {"CALL": 1},
        }
        zero_value_equity = {
            "total_value": 0,
            "put_call_value": {},
            "class_value": {"COM": 0},
            "issuer_value": {"EXAMPLE INC": 0},
            "instrument_type_value": {"EQUITY": 0},
            "instrument_type_count": {"EQUITY": 1},
            "non_option_class_value": {"COM": 0},
            "non_option_class_count": {"COM": 1},
            "non_option_issuer_value": {"EXAMPLE INC": 0},
            "non_option_issuer_count": {"EXAMPLE INC": 1},
        }
        zero_value_call = {
            "total_value": 0,
            "put_call_value": {"CALL": 0},
            "class_value": {"COM": 0},
            "issuer_value": {"EXAMPLE INC": 0},
            "instrument_type_value": {"CALL": 0},
            "instrument_type_count": {"CALL": 1},
        }

        self.assertEqual(
            "EQUITY",
            pipeline._registry_type_from_evidence(shared_cusip),
        )
        self.assertEqual(
            "CALL",
            pipeline._registry_type_from_evidence(option_only_cusip),
        )
        self.assertEqual(
            "EQUITY",
            pipeline._registry_type_from_evidence(zero_value_equity),
        )
        self.assertEqual(
            "CALL",
            pipeline._registry_type_from_evidence(zero_value_call),
        )
        self.assertEqual(
            "NOTE",
            pipeline._registry_type_from_evidence(
                option_only_cusip,
                {
                    "status": "matched",
                    "ticker": "FLO 2.4 03/15/31",
                    "securityDescription": "FLO 2.4 03/15/31",
                    "securityType": "GLOBAL",
                    "securityType2": "Corp",
                    "marketSector": "Corp",
                },
            ),
        )
        self.assertEqual(
            "NOTE",
            pipeline._registry_type_from_evidence(
                shared_cusip,
                {
                    "status": "matched",
                    "securityType": "GLOBAL",
                    "securityType2": "Corp",
                    "marketSector": "Corp",
                },
            ),
        )
        self.assertEqual(
            "NOTE",
            pipeline._registry_type_from_evidence(
                shared_cusip,
                identifier="590188108",
            ),
        )
        self.assertEqual(
            "NOTE",
            pipeline._registry_type_from_evidence(
                option_only_cusip,
                prior_entry={
                    "security_kind": "BOND",
                    "security_kind_source": "openfigi",
                },
            ),
        )

    def test_equity_ticker_backfill_requires_current_unowned_consensus(
        self,
    ) -> None:
        def equity(
            issuer: str,
            last_seen: str,
            holder_count: int,
        ) -> dict:
            return {
                "ticker": None,
                "name": issuer,
                "dominant_issuer": issuer,
                "dominant_class": "COM",
                "type": "EQUITY",
                "last_seen": last_seen,
                "holder_count": holder_count,
                "total_value": 100,
                "sources": ["filer_dominant"],
            }

        def option(
            issuer: str,
            ticker: str,
            option_type: str,
            last_seen: str = "2026-03-31",
        ) -> dict:
            return {
                "ticker": ticker,
                "name": issuer,
                "dominant_issuer": issuer,
                "dominant_class": option_type,
                "type": option_type,
                "last_seen": last_seen,
                "holder_count": 1,
                "total_value": 10,
                "sources": ["filer_dominant", "derived_option_text"],
            }

        registry = {
            "15961R105": equity(
                "CHARGEPOINT HOLDINGS INC",
                "2025-09-30",
                200,
            ),
            "15961R303": equity(
                "CHARGEPOINT HOLDINGS INC",
                "2026-06-30",
                190,
            ),
            "15961R903": option(
                "CHARGEPOINT HOLDINGS INC",
                "CHPT",
                "CALL",
            ),
            "15961R953": option(
                "CHARGEPOINT HOLDINGS INC",
                "CHPT",
                "PUT",
            ),
        }
        self.assertEqual(
            1,
            pipeline._backfill_equity_tickers_from_option_consensus(
                registry,
                sec_titles={"CHPT": "ChargePoint Holdings, Inc."},
            ),
        )
        self.assertIsNone(registry["15961R105"]["ticker"])
        self.assertEqual("CHPT", registry["15961R303"]["ticker"])
        self.assertIn(
            "option_family_consensus",
            registry["15961R303"]["sources"],
        )
        self.assertEqual(
            ["15961R903", "15961R953"],
            registry["15961R303"]["ticker_evidence_cusips"],
        )
        self.assertEqual(
            "ChargePoint Holdings, Inc.",
            registry["15961R303"]["name"],
        )
        company_tickers = {
            "0": {
                "ticker": "CHPT",
                "title": "ChargePoint Holdings, Inc.",
            },
        }
        errors: list[str] = []
        validate_data.validate_registry(
            set(registry),
            errors,
            registry,
            company_tickers,
        )
        self.assertEqual([], errors)
        corrupted = json.loads(json.dumps(registry))
        corrupted["15961R303"]["ticker_evidence_cusips"] = [
            "15961R903",
        ]
        errors.clear()
        validate_data.validate_registry(
            set(corrupted),
            errors,
            corrupted,
            company_tickers,
        )
        self.assertTrue(
            any(
                "option-family ticker derivations" in error
                for error in errors
            ),
            errors,
        )

        tied_consensus = json.loads(json.dumps(registry))
        tied_consensus["15961R105"]["last_seen"] = "2026-06-30"
        errors.clear()
        validate_data.validate_registry(
            set(tied_consensus),
            errors,
            tied_consensus,
            company_tickers,
        )
        self.assertTrue(
            any(
                "option-family ticker derivations" in error
                for error in errors
            ),
            errors,
        )

        linked_tied_consensus = json.loads(json.dumps(tied_consensus))
        for option_cusip in ("15961R903", "15961R953"):
            linked_tied_consensus[option_cusip]["underlying_cusip"] = (
                "15961R303"
            )
        errors.clear()
        validate_data.validate_registry(
            set(linked_tied_consensus),
            errors,
            linked_tied_consensus,
            company_tickers,
        )
        self.assertEqual([], errors)

        preferred_consensus = json.loads(json.dumps(registry))
        preferred_consensus["15961R303"].update({
            "dominant_class": "PREFERRED STOCK",
            "security_kind": "PREFERRED",
            "security_kind_source": "openfigi",
        })
        errors.clear()
        validate_data.validate_registry(
            set(preferred_consensus),
            errors,
            preferred_consensus,
            company_tickers,
        )
        self.assertTrue(
            any(
                "option-family ticker derivations" in error
                for error in errors
            ),
            errors,
        )

        filer_preferred_consensus = json.loads(json.dumps(registry))
        filer_preferred_consensus["15961R303"]["dominant_class"] = (
            "PREFERRED STOCK"
        )
        errors.clear()
        validate_data.validate_registry(
            set(filer_preferred_consensus),
            errors,
            filer_preferred_consensus,
            company_tickers,
        )
        self.assertTrue(
            any(
                "option-family ticker derivations" in error
                for error in errors
            ),
            errors,
        )

        blocked_by_owner = {
            "977852102": equity(
                "WOLFSPEED INC",
                "2026-03-31",
                200,
            ),
            "977852902": option(
                "WOLFSPEED INC",
                "WOLF",
                "CALL",
                "2025-06-30",
            ),
            "97785W106": {
                **equity("WOLFSPEED INC", "2026-06-30", 250),
                "ticker": "WOLF",
            },
        }
        self.assertEqual(
            0,
            pipeline._backfill_equity_tickers_from_option_consensus(
                blocked_by_owner,
                sec_titles={"WOLF": "Wolfspeed, Inc."},
            ),
        )
        self.assertIsNone(blocked_by_owner["977852102"]["ticker"])

        disagreement = {
            "123456789": equity("EXAMPLE INC", "2026-06-30", 10),
            "123456909": option("EXAMPLE INC", "GOOD", "CALL"),
            "123456959": option("EXAMPLE INC", "WRONG", "PUT"),
        }
        self.assertEqual(
            0,
            pipeline._backfill_equity_tickers_from_option_consensus(
                disagreement,
                sec_titles={},
            ),
        )
        self.assertIsNone(disagreement["123456789"]["ticker"])

        one_sided = {
            "654321789": equity("EXAMPLE INC", "2026-06-30", 10),
            "654321909": option("EXAMPLE INC", "GOOD", "CALL"),
        }
        self.assertEqual(
            0,
            pipeline._backfill_equity_tickers_from_option_consensus(
                one_sided,
                sec_titles={"GOOD": "Example Inc."},
            ),
        )

        sec_mismatch = {
            "234567891": equity("EXAMPLE INC", "2026-06-30", 10),
            "234567901": option("EXAMPLE INC", "GOOD", "CALL"),
            "234567951": option("EXAMPLE INC", "GOOD", "PUT"),
        }
        self.assertEqual(
            0,
            pipeline._backfill_equity_tickers_from_option_consensus(
                sec_mismatch,
                sec_titles={"GOOD": "Unrelated Corp."},
            ),
        )

        tied_targets = {
            "345678111": equity("EXAMPLE INC", "2026-06-30", 10),
            "345678222": equity("EXAMPLE INC", "2026-06-30", 9),
            "345678901": option("EXAMPLE INC", "GOOD", "CALL"),
            "345678951": option("EXAMPLE INC", "GOOD", "PUT"),
        }
        self.assertEqual(
            0,
            pipeline._backfill_equity_tickers_from_option_consensus(
                tied_targets,
                sec_titles={"GOOD": "Example Inc."},
            ),
        )

    def test_prudential_options_cannot_backfill_a_preferred_security(
        self,
    ) -> None:
        def equity(
            ticker,
            last_seen: str,
        ) -> dict:
            return {
                "ticker": ticker,
                "name": "PRUDENTIAL FINANCIAL INC",
                "dominant_issuer": "PRUDENTIAL FINANCIAL INC",
                "dominant_class": "COM",
                "type": "EQUITY",
                "last_seen": last_seen,
                "holder_count": 1,
                "total_value": 100,
                "sources": ["filer_dominant"],
            }

        def option(option_type: str) -> dict:
            return {
                "ticker": None,
                "name": "PRUDENTIAL FINANCIAL INC",
                "dominant_issuer": "PRUDENTIAL FINANCIAL INC",
                "dominant_class": option_type,
                "type": option_type,
                "last_seen": "2026-03-31",
                "holder_count": 1,
                "total_value": 10,
                "sources": ["filer_dominant"],
            }

        sec_data = {
            "0": {
                "cik_str": 1137774,
                "ticker": "PRU",
                "title": "PRUDENTIAL FINANCIAL INC",
            },
            "1": {
                "cik_str": 1137774,
                "ticker": "PFH",
                "title": "PRUDENTIAL FINANCIAL INC",
            },
        }
        sec_titles, name_to_ticker = pipeline._company_ticker_indexes(sec_data)
        self.assertEqual(
            {"PFH", "PRU"},
            set(sec_titles),
        )
        self.assertNotIn(
            pipeline.normalize_name("PRUDENTIAL FINANCIAL INC"),
            name_to_ticker,
        )

        registry = {
            "744320102": equity("PRU", "2026-06-30"),
            # A transient rebuild misclassified this preferred as Equity before
            # the trusted security-kind enrichment pass ran.
            "744320888": equity(None, "2026-03-31"),
            "744320902": option("CALL"),
            "744320952": option("PUT"),
        }
        self.assertEqual(
            (0, 0),
            pipeline._apply_option_underlying_derivations(
                registry,
                name_to_ticker=name_to_ticker,
                sec_titles=sec_titles,
            ),
        )
        self.assertIsNone(registry["744320902"]["ticker"])
        self.assertIsNone(registry["744320952"]["ticker"])

        # Even stale option-text evidence cannot select the older tickerless
        # row while a newer same-family Equity exists.
        for cusip in ("744320902", "744320952"):
            registry[cusip]["ticker"] = "PFH"
            registry[cusip]["sources"].append("derived_option_text")
        self.assertEqual(
            0,
            pipeline._backfill_equity_tickers_from_option_consensus(
                registry,
                sec_titles=sec_titles,
            ),
        )
        self.assertIsNone(registry["744320888"]["ticker"])

        # Trusted CUSIP-level Preferred proof independently excludes the row,
        # even if the newer common-stock sibling is absent from a fixture.
        preferred_only = {
            cusip: json.loads(json.dumps(entry))
            for cusip, entry in registry.items()
            if cusip != "744320102"
        }
        self.assertEqual(
            0,
            pipeline._backfill_equity_tickers_from_option_consensus(
                preferred_only,
                sec_titles=sec_titles,
                prior_registry={
                    "744320888": {
                        "type": "PREF",
                        "security_kind": "PREFERRED",
                        "security_kind_source": "openfigi",
                    },
                },
            ),
        )
        self.assertIsNone(preferred_only["744320888"]["ticker"])

    def test_current_holder_baseline_prefers_newer_tie_and_skips_withheld(
        self,
    ) -> None:
        funds = [
            {"cik": 1, "q": [20261, 20254]},
            {"cik": 2, "q": [20261, 20254]},
            {"cik": 3, "q": [20262, 20261]},
            {"cik": 4, "q": [20262, 20261]},
            {"cik": 5, "q": [20263, 20262], "status": "WITHHELD"},
        ]

        self.assertEqual(
            20262,
            pipeline._modal_latest_reporting_quarter(funds),
        )
        self.assertEqual(
            {3: 20262, 4: 20262},
            pipeline._current_fund_quarters(funds, 20262),
        )
        self.assertEqual(
            20262,
            validate_data._index_modal_latest_reporting_quarter(funds),
        )

    def test_rebuild_tickers_in_place_repairs_suspect_duplicate_cusips(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_dir = tmp / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            stocks_dir.mkdir(parents=True)

            fund = {
                "cik": 123456,
                "name": "Example Fund",
                "quarters": [
                    {
                        "report_date": "2025-12-31",
                        "filing_date": "2026-02-14",
                        "total_value": 35,
                        "num_holdings": 2,
                        "holdings": [
                            {
                                "ticker": "SO",
                                "issuer": "SOUTHERN CO",
                                "cusip": "842587107",
                                "class": "COM",
                                "value": 20,
                                "shares": 2,
                                "holding_type": "EQUITY",
                            },
                            {
                                "ticker": "SO",
                                "issuer": "SOUTHERN MO BANCORP INC",
                                "cusip": "843380106",
                                "class": "COM",
                                "value": 15,
                                "shares": 1,
                                "holding_type": "EQUITY",
                            },
                        ],
                    }
                ],
            }
            (funds_dir / "123456.json").write_text(json.dumps(fund, indent=2))
            (data_dir / "cusip_map.json").write_text(
                json.dumps({"842587107": "SO", "843380106": "SO"}, indent=2),
            )

            original_data_dir = pipeline.DATA_DIR
            original_funds_dir = pipeline.FUNDS_DIR
            original_stocks_dir = pipeline.STOCKS_DIR
            original_index_path = pipeline.INDEX_PATH
            original_cusip_map_path = pipeline.CUSIP_MAP_PATH
            try:
                pipeline.DATA_DIR = data_dir
                pipeline.FUNDS_DIR = funds_dir
                pipeline.STOCKS_DIR = stocks_dir
                pipeline.INDEX_PATH = data_dir / "index.json"
                pipeline.CUSIP_MAP_PATH = data_dir / "cusip_map.json"

                with mock.patch.object(
                    pipeline,
                    "resolve_cusips_via_openfigi",
                    return_value={"842587107": "SO", "843380106": "SMBC"},
                ) as mock_figi:
                    updated = pipeline.rebuild_tickers_in_place(
                        company_ticker_data=[],
                    )
            finally:
                pipeline.DATA_DIR = original_data_dir
                pipeline.FUNDS_DIR = original_funds_dir
                pipeline.STOCKS_DIR = original_stocks_dir
                pipeline.INDEX_PATH = original_index_path
                pipeline.CUSIP_MAP_PATH = original_cusip_map_path

            rebuilt_fund = json.loads((funds_dir / "123456.json").read_text())
            rebuilt_map = json.loads((data_dir / "cusip_map.json").read_text())

            mock_figi.assert_called_once_with(
                ["842587107", "843380106"],
                force_refresh=False,
            )
            self.assertEqual(1, updated)
            self.assertEqual("SO", rebuilt_fund["quarters"][0]["holdings"][0]["ticker"])
            self.assertEqual("SMBC", rebuilt_fund["quarters"][0]["holdings"][1]["ticker"])
            self.assertEqual("SMBC", rebuilt_map["843380106"])

    def test_rebuild_tickers_in_place_seeds_map_from_existing_holdings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_dir = tmp / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            cache_dir = tmp / ".cache"
            funds_dir.mkdir(parents=True)
            stocks_dir.mkdir(parents=True)
            cache_dir.mkdir(parents=True)

            fund = {
                "cik": 123456,
                "name": "Example Fund",
                "quarters": [
                    {
                        "report_date": "2025-12-31",
                        "filing_date": "2026-02-14",
                        "total_value": 20,
                        "num_holdings": 1,
                        "holdings": [
                            {
                                "ticker": "AON",
                                "issuer": "AON PLC",
                                "cusip": "G0403H108",
                                "class": "COM",
                                "value": 20,
                                "shares": 2,
                                "holding_type": "EQUITY",
                            }
                        ],
                    }
                ],
            }
            (funds_dir / "123456.json").write_text(json.dumps(fund, indent=2))

            original_data_dir = pipeline.DATA_DIR
            original_funds_dir = pipeline.FUNDS_DIR
            original_stocks_dir = pipeline.STOCKS_DIR
            original_index_path = pipeline.INDEX_PATH
            original_cusip_map_path = pipeline.CUSIP_MAP_PATH
            original_legacy_cusip_map_path = pipeline.LEGACY_CUSIP_MAP_PATH
            try:
                pipeline.DATA_DIR = data_dir
                pipeline.FUNDS_DIR = funds_dir
                pipeline.STOCKS_DIR = stocks_dir
                pipeline.INDEX_PATH = data_dir / "index.json"
                pipeline.CUSIP_MAP_PATH = cache_dir / "cusip_map.json"
                pipeline.LEGACY_CUSIP_MAP_PATH = data_dir / "cusip_map.json"

                with mock.patch.object(pipeline, "get_openfigi_api_key", return_value=""):
                    with mock.patch.object(pipeline, "resolve_cusips_via_openfigi") as mock_figi:
                        updated = pipeline.rebuild_tickers_in_place(
                            company_ticker_data=[],
                        )
            finally:
                pipeline.DATA_DIR = original_data_dir
                pipeline.FUNDS_DIR = original_funds_dir
                pipeline.STOCKS_DIR = original_stocks_dir
                pipeline.INDEX_PATH = original_index_path
                pipeline.CUSIP_MAP_PATH = original_cusip_map_path
                pipeline.LEGACY_CUSIP_MAP_PATH = original_legacy_cusip_map_path

            rebuilt_map = json.loads((cache_dir / "cusip_map.json").read_text())

            mock_figi.assert_not_called()
            self.assertEqual(0, updated)
            self.assertEqual("AON", rebuilt_map["G0403H108"])

    def test_full_cusip_refresh_prunes_stale_entries_and_refreshes_current_ones(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_dir = tmp / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            stocks_dir.mkdir(parents=True)

            fund = {
                "cik": 123456,
                "name": "Example Fund",
                "quarters": [
                    {
                        "report_date": "2025-12-31",
                        "filing_date": "2026-02-14",
                        "total_value": 35,
                        "num_holdings": 2,
                        "holdings": [
                            {
                                "ticker": "OLD1",
                                "issuer": "CURRENT ONE INC",
                                "cusip": "111111111",
                                "class": "COM",
                                "value": 20,
                                "shares": 2,
                                "holding_type": "EQUITY",
                            },
                            {
                                "ticker": "KEEP2",
                                "issuer": "CURRENT TWO INC",
                                "cusip": "222222222",
                                "class": "COM",
                                "value": 15,
                                "shares": 1,
                                "holding_type": "EQUITY",
                            },
                        ],
                    }
                ],
            }
            (funds_dir / "123456.json").write_text(json.dumps(fund, indent=2))
            (data_dir / "cusip_map.json").write_text(
                json.dumps(
                    {
                        "111111111": "OLD1",
                        "222222222": "KEEP2",
                        "999999999": "STALE9",
                    },
                    indent=2,
                ),
            )

            original_data_dir = pipeline.DATA_DIR
            original_funds_dir = pipeline.FUNDS_DIR
            original_stocks_dir = pipeline.STOCKS_DIR
            original_index_path = pipeline.INDEX_PATH
            original_cusip_map_path = pipeline.CUSIP_MAP_PATH
            try:
                pipeline.DATA_DIR = data_dir
                pipeline.FUNDS_DIR = funds_dir
                pipeline.STOCKS_DIR = stocks_dir
                pipeline.INDEX_PATH = data_dir / "index.json"
                pipeline.CUSIP_MAP_PATH = data_dir / "cusip_map.json"

                with mock.patch.object(
                    pipeline,
                    "get_openfigi_api_key",
                    return_value="test-key",
                ):
                    with mock.patch.object(
                        pipeline,
                        "resolve_cusips_via_openfigi",
                        return_value={"111111111": "NEW1"},
                    ) as mock_figi:
                        updated = pipeline.rebuild_tickers_in_place(
                            full_refresh=True,
                            company_ticker_data=[],
                        )
            finally:
                pipeline.DATA_DIR = original_data_dir
                pipeline.FUNDS_DIR = original_funds_dir
                pipeline.STOCKS_DIR = original_stocks_dir
                pipeline.INDEX_PATH = original_index_path
                pipeline.CUSIP_MAP_PATH = original_cusip_map_path

            rebuilt_fund = json.loads((funds_dir / "123456.json").read_text())
            rebuilt_map = json.loads((data_dir / "cusip_map.json").read_text())

            mock_figi.assert_called_once_with(
                ["111111111", "222222222"],
                force_refresh=True,
            )
            self.assertEqual(1, updated)
            self.assertEqual("NEW1", rebuilt_fund["quarters"][0]["holdings"][0]["ticker"])
            self.assertEqual("KEEP2", rebuilt_fund["quarters"][0]["holdings"][1]["ticker"])
            self.assertEqual({"111111111": "NEW1", "222222222": "KEEP2"}, rebuilt_map)

    def test_state_stays_public_and_cusip_cache_stays_private(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_dir = tmp / "data"
            cache_dir = tmp / ".cache"
            data_dir.mkdir(parents=True)
            cache_dir.mkdir(parents=True)

            cached_state = cache_dir / "pipeline_state.json"
            legacy_cusip_map = data_dir / "cusip_map.json"
            cached_state.write_text(
                json.dumps(
                    {
                        "processed": ["0000000000-00-000001"],
                        "last_run": "2026-04-17T00:00:00Z",
                    },
                    indent=2,
                ),
            )
            legacy_cusip_map.write_text(
                json.dumps({"111111111": "TEST"}, indent=2),
            )

            original_state_path = pipeline.STATE_PATH
            original_legacy_state_path = pipeline.LEGACY_STATE_PATH
            original_cusip_map_path = pipeline.CUSIP_MAP_PATH
            original_legacy_cusip_map_path = pipeline.LEGACY_CUSIP_MAP_PATH
            try:
                pipeline.STATE_PATH = data_dir / "pipeline_state.json"
                pipeline.LEGACY_STATE_PATH = cached_state
                pipeline.CUSIP_MAP_PATH = cache_dir / "cusip_map.json"
                pipeline.LEGACY_CUSIP_MAP_PATH = legacy_cusip_map

                state = pipeline.load_state()
                cusip_map = pipeline.load_cusip_map()
            finally:
                pipeline.STATE_PATH = original_state_path
                pipeline.LEGACY_STATE_PATH = original_legacy_state_path
                pipeline.CUSIP_MAP_PATH = original_cusip_map_path
                pipeline.LEGACY_CUSIP_MAP_PATH = original_legacy_cusip_map_path

            self.assertEqual({"0000000000-00-000001"}, state["_processed_set"])
            self.assertEqual({"111111111": "TEST"}, cusip_map)
            self.assertEqual(
                ["0000000000-00-000001"],
                json.loads((data_dir / "pipeline_state.json").read_text())["processed"],
            )
            self.assertEqual(
                {"111111111": "TEST"},
                json.loads((cache_dir / "cusip_map.json").read_text()),
            )

    def test_zero_share_repair_imputes_when_value_exceeds_one_share(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_dir = tmp / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)

            reference = {
                "cik": 1,
                "name": "Reference Fund",
                "quarters": [
                    {
                        "report_date": "2025-12-31",
                        "filing_date": "2026-02-14",
                        "total_value": 1000,
                        "num_holdings": 1,
                        "holdings": [
                            {
                                "ticker": "TEST",
                                "issuer": "TEST CORP",
                                "cusip": "123456789",
                                "class": "COM",
                                "value": 1000,
                                "shares": 10,
                                "holding_type": "EQUITY",
                            }
                        ],
                    }
                ],
            }
            missing = {
                "cik": 2,
                "name": "Missing Shares Fund",
                "quarters": [
                    {
                        "report_date": "2025-12-31",
                        "filing_date": "2026-02-14",
                        "total_value": 250,
                        "num_holdings": 1,
                        "holdings": [
                            {
                                "ticker": "TEST",
                                "issuer": "TEST CORP",
                                "cusip": "123456789",
                                "class": "COM",
                                "value": 250,
                                "shares": 0,
                                "holding_type": "EQUITY",
                            }
                        ],
                    }
                ],
            }
            (funds_dir / "1.json").write_text(json.dumps(reference, indent=2))
            (funds_dir / "2.json").write_text(json.dumps(missing, indent=2))

            original_funds_dir = pipeline.FUNDS_DIR
            try:
                pipeline.FUNDS_DIR = funds_dir
                updated = pipeline.repair_zero_share_holdings_in_place()
            finally:
                pipeline.FUNDS_DIR = original_funds_dir

            repaired = json.loads((funds_dir / "2.json").read_text())
            holding = repaired["quarters"][0]["holdings"][0]

            self.assertEqual(1, updated)
            self.assertEqual(2.5, holding["shares"])
            self.assertTrue(holding["shares_imputed"])
            self.assertEqual(0, holding["reported_shares"])

    def test_zero_share_repair_keeps_plausible_sub_share_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_dir = tmp / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)

            reference = {
                "cik": 1,
                "name": "Reference Fund",
                "quarters": [
                    {
                        "report_date": "2025-12-31",
                        "filing_date": "2026-02-14",
                        "total_value": 1000,
                        "num_holdings": 1,
                        "holdings": [
                            {
                                "ticker": "TEST",
                                "issuer": "TEST CORP",
                                "cusip": "123456789",
                                "class": "COM",
                                "value": 1000,
                                "shares": 10,
                                "holding_type": "EQUITY",
                            }
                        ],
                    }
                ],
            }
            dust = {
                "cik": 2,
                "name": "Dust Fund",
                "quarters": [
                    {
                        "report_date": "2025-12-31",
                        "filing_date": "2026-02-14",
                        "total_value": 90,
                        "num_holdings": 1,
                        "holdings": [
                            {
                                "ticker": "TEST",
                                "issuer": "TEST CORP",
                                "cusip": "123456789",
                                "class": "COM",
                                "value": 90,
                                "shares": 0,
                                "holding_type": "EQUITY",
                            }
                        ],
                    }
                ],
            }
            (funds_dir / "1.json").write_text(json.dumps(reference, indent=2))
            (funds_dir / "2.json").write_text(json.dumps(dust, indent=2))

            original_funds_dir = pipeline.FUNDS_DIR
            try:
                pipeline.FUNDS_DIR = funds_dir
                updated = pipeline.repair_zero_share_holdings_in_place()
            finally:
                pipeline.FUNDS_DIR = original_funds_dir

            repaired = json.loads((funds_dir / "2.json").read_text())
            holding = repaired["quarters"][0]["holdings"][0]

            self.assertEqual(0, updated)
            self.assertEqual(0, holding["shares"])
            self.assertNotIn("shares_imputed", holding)

    def test_zero_share_repair_rebuilds_prior_imputations_idempotently(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            report_date = "2025-12-31"

            (funds_dir / "1.json").write_text(
                json.dumps(
                    {
                        "cik": 1,
                        "quarters": [
                            {
                                "report_date": report_date,
                                "holdings": [
                                    {
                                        "cusip": "123456789",
                                        "class": "COM",
                                        "value": 1000,
                                        "shares": 10,
                                        "holding_type": "EQUITY",
                                    }
                                ],
                            }
                        ],
                    }
                )
            )
            target_path = funds_dir / "2.json"
            target_path.write_text(
                json.dumps(
                    {
                        "cik": 2,
                        "quarters": [
                            {
                                "report_date": report_date,
                                "holdings": [
                                    {
                                        "cusip": "123456789",
                                        "class": "COM",
                                        "value": 250,
                                        "shares": 999,
                                        "reported_shares": 0,
                                        "shares_imputed": True,
                                        "holding_type": "EQUITY",
                                    },
                                    {
                                        "cusip": "123456789",
                                        "class": "COM",
                                        "value": 250,
                                        "shares": 999,
                                        "reported_shares": 0,
                                        "shares_imputed": True,
                                        "put_call": "CALL",
                                        "holding_type": "CALL",
                                    },
                                ],
                            }
                        ],
                    }
                )
            )

            original_holdings = json.loads(target_path.read_text())[
                "quarters"
            ][0]["holdings"]
            original_hash = pipeline.calculate_composition_hash(
                report_date,
                "base-accession",
                ["base-accession"],
                ["a" * 64],
                original_holdings,
                composition_version=1,
            )

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                self.assertEqual(1, pipeline.repair_zero_share_holdings_in_place())

            repaired = json.loads(target_path.read_text())
            equity, call = repaired["quarters"][0]["holdings"]
            self.assertEqual(2.5, equity["shares"])
            self.assertIs(equity["shares_imputed"], True)
            self.assertEqual(0, equity["reported_shares"])
            self.assertEqual(0, call["shares"])
            self.assertEqual(0, call["reported_shares"])
            self.assertNotIn("shares_imputed", call)
            self.assertEqual(
                original_hash,
                pipeline.calculate_composition_hash(
                    report_date,
                    "base-accession",
                    ["base-accession"],
                    ["a" * 64],
                    repaired["quarters"][0]["holdings"],
                    composition_version=1,
                ),
            )

            first_result = target_path.read_text()
            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                self.assertEqual(1, pipeline.repair_zero_share_holdings_in_place())
            self.assertEqual(first_result, target_path.read_text())


if __name__ == "__main__":
    unittest.main()
