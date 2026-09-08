"""Issuer descriptions never become symbol or instrument-type authority."""
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
import unittest
from unittest.mock import patch

import fund_product_names as names
from scripts.review_fund_product_names import parse_page


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

    def test_exact_identity_and_series_are_preserved_without_mutation(self):
        before = deepcopy(self.row)
        result = names.reviewed_product_name(self.cusip, self.row)
        self.assertEqual("Innovator Growth-100 Power Buffer ETF — July", result)
        self.assertEqual(before, self.row)
        self.assertNotIn("ticker_source", self.row)

    def test_no_guessing_for_debt_options_unresolved_or_different_symbols(self):
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
        self.assertEqual(171, len(names.validate_review_bytes(raw, as_of=as_of)))
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
