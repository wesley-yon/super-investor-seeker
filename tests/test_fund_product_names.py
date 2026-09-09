"""Issuer descriptions never become symbol or instrument-type authority."""
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
import unittest
from unittest.mock import patch

import fund_product_names as names
from scripts.review_fund_product_names import parse_dws_pdf_text, parse_page


class FundProductNameTests(unittest.TestCase):
    def test_issuer_page_parser_preserves_series_and_rejects_identity_conflicts(self):
        page = b'''<title>NJUL - Innovator Growth-100 Power Buffer ETF</title>
        <div><div class="al_fund">Ticker</div><div class="ar_fund">NJUL</div></div>
        <div><div class="al_fund">CUSIP</div><div class="ar_fund">45782C276</div></div>
        <div><div class="al_fund">Series</div><div class="ar_fund">July</div></div>
        <div><div class="al_fund">Last Price</div><div>Irrelevant market widget</div></div>'''
        args = dict(url="https://www.innovatoretfs.com/etf/default.aspx?ticker=njul",
                    cusip="45782C276", ticker="NJUL")
        self.assertEqual("Innovator Growth-100 Power Buffer ETF — July", parse_page(page, **args)["name"])
        for bad in (page.replace(b"45782C276", b"45782C615"),
                    page + b'<div><div class="al_fund">CUSIP</div><div class="ar_fund">45782C615</div></div>'):
            with self.assertRaises(ValueError):
                parse_page(bad, **args)

    def setUp(self):
        self.cusip = "45782C276"
        self.row = {"type": "EQUITY", "security_kind": "ETF",
                    "mapping_status": "resolved", "ticker": "NJUL"}

    def test_unresolved_exact_etf_gets_name_without_symbol_or_status_changes(self):
        row = {"type": "EQUITY", "security_kind": "ETF", "mapping_status": "unresolved", "ticker": None}
        before = deepcopy(row)
        self.assertEqual("Leverage Shares 2x Long AAOI Daily ETF", names.reviewed_product_name("88340C412", row))
        self.assertEqual(before, row)
        for change in ({"type": "NOTE"}, {"security_kind": "BOND"}, {"ticker": "AAOG"},
                       {"mapping_status": "quarantined"}, {"mapping_status": "no_symbol"}):
            self.assertIsNone(names.reviewed_product_name("88340C412", {**row, **change}))

    def test_ambiguous_exact_etf_gets_only_a_description(self):
        row = {"type": "EQUITY", "security_kind": "ETF", "mapping_status": "ambiguous", "ticker": None,
               "candidate_ticker": "APRT", "resolution_reason": "issuer_conflict_with_ftd_description"}
        before = deepcopy(row)
        self.assertEqual("AllianzIM U.S. Equity Buffer10 Apr ETF", names.reviewed_product_name("00888H109", row))
        self.assertEqual(before, row)
        for change in ({"ticker": "APRT"}, {"ticker": "OTHER"}, {"type": "NOTE"},
                       {"security_kind": "BOND"}, {"mapping_status": "quarantined"}):
            self.assertIsNone(names.reviewed_product_name("00888H109", {**row, **change}))

    def test_new_issuer_parsers_reject_mismatched_and_duplicate_identity_fields(self):
        cases = [
            ("https://www.allianzim.com/etfs/aprt/", "00888H109", "APRT",
             '<ul><li><span class="page-sidebar__label">Ticker</span><div class="page-sidebar__value">APRT</div></li>'
             '<li><span class="page-sidebar__label">CUSIP</span><div class="page-sidebar__value">00888H109</div></li>'
             '<li><span class="page-sidebar__label">Fund Name</span><div class="page-sidebar__value">AllianzIM U.S. Equity Buffer10 Apr ETF</div></li></ul>'),
            ("https://bluemontefunds.com/blgr", "301505418", "BLGR",
             '<h1>BLGR</h1><h2>Bluemonte Large Cap Growth ETF</h2><table>'
             '<tr><td>Ticker</td><td>BLGR</td></tr><tr><td>Cusip</td><td>301505418</td></tr></table>'),
            ("https://coinshares.com/us/etf/wgmi/", "91917A207", "WGMI",
             '<ul><li><span class="table-name">Ticker</span><button><span>WGMI</span></button></li>'
             '<li><span class="table-name">CUSIP</span><button><span>91917A207</span></button></li>'
             '<li><span class="table-name">Product name</span><button><span>CoinShares Bitcoin Mining and Digital Power ETF</span></button></li></ul>'),
            ("https://www.rexshares.com/tslz/", "26923N181", "TSLZ",
             '<h2>T-REX 2X Inverse Tesla Daily Target ETF</h2>'
             '<div class="t-row"><div class="t-label">Ticker</div><div class="t-data">TSLZ</div></div>'
             '<div class="t-row"><div class="t-label">CUSIP</div><div class="t-data">26923N181</div></div>'),
        ]
        for url, cusip, ticker, page in cases:
            args = dict(url=url, cusip=cusip, ticker=ticker)
            with self.subTest(url=url):
                self.assertIn("ETF", parse_page(page.encode(), **args)["name"])
                for bad in (page.replace(cusip, "000000000"), page.replace(ticker, "OTHER"), page + page):
                    with self.assertRaises(ValueError):
                        parse_page(bad.encode(), **args)

    def test_pdf_factsheet_uses_fund_identity_and_excludes_benchmark(self):
        text = '''Xtrackers MSCI EAFE High Dividend Yield Equity ETF Q2 | 6.30.26 Ticker: HDEF
        Objective and strategy ETF details (6/30/26) NYSE ticker HDEF NAV ticker HDEF.NV
        CUSIP 233051630 Index details Ticker M1EAHDVD'''
        args = dict(cusip="233051630", ticker="HDEF")
        self.assertEqual("Xtrackers MSCI EAFE High Dividend Yield Equity ETF",
                         parse_dws_pdf_text(text, **args)["name"])
        for bad in (text.replace("233051630", "000000000"), text.replace("NYSE ticker HDEF", "NYSE ticker OTHER"),
                    text.replace("CUSIP 233051630", "CUSIP 233051630 CUSIP 000000000"),
                    text.replace("Ticker: HDEF", "Ticker: OTHER")):
            with self.assertRaises(ValueError):
                parse_dws_pdf_text(bad, **args)

    def test_leverage_parser_requires_matching_provider_product_and_identifiers(self):
        page = b'''<h1>2x Long AAOI Daily ETF</h1>
        <div><span>Ticker</span><span>AAOG</span></div>
        <div><span>CUSIP</span><span>88340C412</span></div>
        <script type="application/ld+json">{"@type":"FinancialProduct","name":"2X Long AAOI Daily ETF",
        "alternateName":"AAOG","provider":{"name":"Leverage Shares"}}</script>'''
        args = dict(url="https://leverageshares.com/us/etfs/leverage-shares-2x-long-aaoi-daily-etf",
                    cusip="88340C412", ticker="AAOG")
        self.assertEqual("Leverage Shares 2x Long AAOI Daily ETF", parse_page(page, **args)["name"])
        for bad in (page.replace(b"88340C412", b"88340F209"),
                    page.replace(b'"alternateName":"AAOG"', b'"alternateName":"BEG"'),
                    page.replace(b'"name":"Leverage Shares"', b'"name":"Other Provider"'),
                    page.replace(b'"name":"2X Long AAOI Daily ETF"', b'"name":"2x Short AAOI Daily ETF"')):
            with self.assertRaises(ValueError):
                parse_page(bad, **args)

    def test_bondbloxx_parser_scopes_identity_to_fund_not_benchmark(self):
        page = b'''<table><tbody><tr><td>Product Name</td><td>BondBloxx IR+M Tax-Aware Intermediate Duration ETF</td></tr>
        <tr><td>Ticker</td><td>TXXI</td></tr><tr><td>CUSIP</td><td>09789C663</td></tr></tbody></table>
        <table><tr><td>Performance Benchmark</td><td>Bond Index</td></tr><tr><td>Ticker</td><td>LMBITR</td></tr></table>'''
        args = dict(url="https://bondbloxxetf.com/bondbloxx-irm-tax-aware-intermediate-duration-etf/",
                    cusip="09789C663", ticker="TXXI")
        self.assertEqual("BondBloxx IR+M Tax-Aware Intermediate Duration ETF", parse_page(page, **args)["name"])
        for bad in (page.replace(b"09789C663", b"09789C697"), page.replace(b"TXXI", b"TAXM"), page + page):
            with self.assertRaises(ValueError):
                parse_page(bad, **args)

    def test_exact_identity_and_series_are_preserved_without_mutation(self):
        before = deepcopy(self.row)
        result = names.reviewed_product_name(self.cusip, self.row)
        self.assertEqual("Innovator Growth-100 Power Buffer ETF — July", result)
        self.assertEqual(before, self.row)
        self.assertNotIn("ticker_source", self.row)

    def test_no_names_for_debt_options_or_inconsistent_symbol_states(self):
        cases = [{"type": kind} for kind in ("NOTE", "PREF", "CALL", "PUT", "OPT")]
        cases += [{"security_kind": "BOND"}, {"security_kind": "COMMON"},
                  {"ticker": "OTHER"}, {"ticker": None}, {"mapping_status": "unresolved"}]
        for change in cases:
            with self.subTest(change=change):
                self.assertIsNone(names.reviewed_product_name(self.cusip, {**self.row, **change}))
        self.assertIsNone(names.reviewed_product_name("45782C999", self.row))

    def test_copied_description_and_source_do_not_validate_another_cusip(self):
        row = {**self.row, "product_name_source": names.PRODUCT_NAME_SOURCE,
               "product_name": names.reviewed_product_name(self.cusip, self.row)}
        self.assertTrue(names.valid_reviewed_product_name(self.cusip, row))
        self.assertFalse(names.valid_reviewed_product_name("45782C615", row))
        self.assertFalse(names.valid_reviewed_product_name(self.cusip, {**row, "type": "NOTE"}))

    def test_review_integrity_and_expiration_are_checked(self):
        raw = names.REVIEW_PATH.read_bytes()
        review = json.loads(raw)
        as_of = date.fromisoformat(review["as_of"])
        self.assertEqual(218, len(names.validate_review_bytes(raw, as_of=as_of)))
        with self.assertRaisesRegex(ValueError, "checksum"):
            names.validate_review_bytes(raw + b" ", as_of=as_of)
        with self.assertRaisesRegex(ValueError, "revalidation"):
            names.validate_review_bytes(raw, as_of=as_of + timedelta(days=91))
        for field, value in (("url", "https://example.com/fund"), ("instrument_type", "NOTE"),
                             ("sha256", "missing"), ("name", "")):
            changed = deepcopy(review)
            changed["securities"][self.cusip][field] = value
            invalid = json.dumps(changed).encode()
            with self.subTest(field=field), patch.object(names, "REVIEW_SHA256", hashlib.sha256(invalid).hexdigest()):
                with self.assertRaisesRegex(ValueError, "proof"):
                    names.validate_review_bytes(invalid, as_of=as_of)

    def test_reviewed_innovator_products_are_distinguishable(self):
        entries = [e for e in names.load_review().values() if "innovatoretfs.com" in e["url"]]
        self.assertEqual(165, len(entries))
        self.assertEqual(len(entries), len({e["name"].casefold() for e in entries}))
