"""Behavioral tests: SEC semantics, source retention, and unsafe input rejection."""

import json
import unittest

from insider_pipeline.parser import OwnershipXMLParseError, parse_ownership_xml


ACCESSION = "0001234567-26-000001"
URL = "https://www.sec.gov/Archives/edgar/data/64040/example.xml"


def filing(body="", form="4", header="", namespace=""):
    return ('''<?xml version="1.0" encoding="UTF-8"?>
<ownershipDocument%s>
  <schemaVersion>X0508</schemaVersion><documentType>%s</documentType>
  <periodOfReport>2026-09-01</periodOfReport>
  <issuer><issuerCik>0000064040</issuerCik><issuerName>S&amp;P Global Inc.</issuerName>
    <issuerTradingSymbol>SPGI</issuerTradingSymbol></issuer>
  %s
  <reportingOwner><reportingOwnerId><rptOwnerCik>0001000001</rptOwnerCik>
    <rptOwnerName>Example One</rptOwnerName></reportingOwnerId>
    <reportingOwnerAddress><rptOwnerStreet1>1 Test Street</rptOwnerStreet1>
      <rptOwnerStreet2>Suite 20</rptOwnerStreet2><rptOwnerCity>New York</rptOwnerCity>
      <rptOwnerState>NY</rptOwnerState><rptOwnerZipCode>10001</rptOwnerZipCode>
      <rptOwnerStateDescription>NEW YORK</rptOwnerStateDescription></reportingOwnerAddress>
    <reportingOwnerRelationship><isDirector>1</isDirector><isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner><isOther>0</isOther>
      <officerTitle>CEO &amp; President</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  %s
  <ownerSignature><signatureName>/s/ Example One by Attorney</signatureName>
    <signatureDate>2026-09-03</signatureDate></ownerSignature>
</ownershipDocument>''' % (namespace, form, header, body)).encode("utf-8")


def transaction(code="P", shares="2.50", price="10.1250", derivative=False, extra="", price_refs=""):
    tag = "derivativeTransaction" if derivative else "nonDerivativeTransaction"
    return '''<%s>
  <securityTitle><value>%s</value><footnoteId id="F1"/></securityTitle>
  <transactionDate><value>2026-09-01</value></transactionDate>
  <deemedExecutionDate><value>2026-09-02</value></deemedExecutionDate>
  <transactionCoding><transactionFormType>4</transactionFormType>
    <transactionCode>%s</transactionCode><equitySwapInvolved>0</equitySwapInvolved>
  </transactionCoding><transactionTimeliness><value>E</value></transactionTimeliness>
  <transactionAmounts><transactionShares><value>%s</value></transactionShares>
    <transactionPricePerShare>%s%s</transactionPricePerShare>
    <transactionAcquiredDisposedCode><value>%s</value></transactionAcquiredDisposedCode>
  </transactionAmounts>
  <postTransactionAmounts><sharesOwnedFollowingTransaction><value>001234.5000</value>
    </sharesOwnedFollowingTransaction></postTransactionAmounts>
  <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership>
    <natureOfOwnership><value>By Trust</value><footnoteId id="F2"/></natureOfOwnership>
  </ownershipNature>%s
</%s>''' % (tag, "Stock Option" if derivative else "Common Stock", code, shares,
           "<value>%s</value>" % price if price is not None else "", price_refs,
           "D" if code == "S" else "A", extra, tag)


FOOTNOTES = '''<footnotes><footnote id="F1">Security explanation &amp; context.</footnote>
  <footnote id="F2">Held by a family trust.</footnote></footnotes>'''


class OwnershipParserTests(unittest.TestCase):
    def parse(self, data):
        return parse_ownership_xml(data, ACCESSION, URL, "2026-09-03")

    def test_precise_decimal_and_complete_non_derivative_fields(self):
        result = self.parse(filing("<nonDerivativeTable>" + transaction() + "</nonDerivativeTable>" + FOOTNOTES))
        self.assertEqual(len(result["transactions"]), 1)
        row = result["transactions"][0]
        self.assertEqual(row["row_id"], ACCESSION + ":ND-T:1")
        self.assertEqual(row["transaction_shares"], "2.50")
        self.assertEqual(row["transaction_price_per_share"], "10.1250")
        self.assertEqual(row["transaction_value"], "25.312500")
        self.assertEqual(row["transaction_value_basis"], "reported_shares_times_reported_price")
        self.assertEqual(row["shares_owned_following"], "001234.5000")
        self.assertEqual(row["deemed_execution_date"], "2026-09-02")
        self.assertEqual(row["nature_of_ownership"], "By Trust")
        self.assertEqual(row["direct_or_indirect"], "I")
        self.assertEqual(row["transaction_timeliness"], "E")
        self.assertEqual(row["equity_swap_involved"], "0")
        self.assertEqual(row["classification"], "purchase")
        self.assertEqual(row["market_scope"], "open_or_private")
        self.assertEqual(row["source_url"], URL)
        self.assertEqual(row["warnings"], [])

    def test_footnote_links_and_unknown_source_fields_remain(self):
        extra = '''<futureBlock><futureNumeric unit="units"> 00012.3400 </futureNumeric>
          <futureName>First</futureName><futureName>Second</futureName></futureBlock>'''
        result = self.parse(filing("<nonDerivativeTable>" + transaction(extra=extra) + "</nonDerivativeTable>" + FOOTNOTES))
        row = result["transactions"][0]
        self.assertEqual(row["normalized_footnote_refs"]["security_title"], ["F1"])
        self.assertEqual(row["normalized_footnote_refs"]["nature_of_ownership"], ["F2"])
        self.assertEqual(row["footnote_refs"]["securityTitle/value"], ["F1"])
        self.assertEqual(row["raw_fields"]["securityTitle/footnoteId/@id"], "F1")
        self.assertEqual(row["raw_fields"]["futureBlock/futureNumeric"], " 00012.3400 ")
        self.assertEqual(row["raw_fields"]["futureBlock/futureNumeric/@unit"], "units")
        self.assertEqual(row["raw_fields"]["futureBlock/futureName[1]"], "First")
        self.assertEqual(row["raw_fields"]["futureBlock/futureName[2]"], "Second")
        self.assertEqual(result["footnotes"][0]["text"], "Security explanation & context.")
        self.assertIn("nonDerivativeTable/nonDerivativeTransaction/futureBlock/futureNumeric", result["filing"]["raw_fields"])

    def test_joint_owners_do_not_multiply_transactions(self):
        second_owner = '''<reportingOwner><reportingOwnerId><rptOwnerCik>0001000002</rptOwnerCik>
          <rptOwnerName>Example Two</rptOwnerName></reportingOwnerId>
          <reportingOwnerRelationship><isDirector>0</isDirector><isOther>1</isOther>
            <otherText>Trustee</otherText></reportingOwnerRelationship></reportingOwner>'''
        result = self.parse(filing(second_owner + "<nonDerivativeTable>" + transaction() + "</nonDerivativeTable>" + FOOTNOTES))
        self.assertEqual(len(result["owners"]), 2)
        self.assertEqual(len(result["transactions"]), 1)
        owner = result["owners"][0]
        self.assertEqual(owner["officer_title"], "CEO & President")
        self.assertEqual(owner["is_director"], "1")
        self.assertEqual(owner["zip_code"], "10001")
        self.assertEqual(result["owners"][1]["other_text"], "Trustee")
        row = result["transactions"][0]
        self.assertEqual(row["owner_ciks"], "0001000001 | 0001000002")
        self.assertEqual(row["owner_names"], "Example One | Example Two")
        self.assertEqual(len(json.loads(row["owners_json"])), 2)

    def test_all_form_types_and_amendments_keep_rows_independent(self):
        for form in ("3", "3/A", "3A", "4", "4/A", "4A", "5", "5/A", "5A"):
            with self.subTest(form=form):
                result = self.parse(filing("<nonDerivativeTable>" + transaction() + transaction() + "</nonDerivativeTable>" + FOOTNOTES,
                                          form=form, header="<dateOfOriginalSubmission>2026-08-01</dateOfOriginalSubmission>"))
                self.assertEqual(result["filing"]["is_amendment"], form.endswith("A"))
                self.assertEqual(result["filing"]["date_of_original_submission"], "2026-08-01")
                self.assertEqual(len(result["transactions"]), 2)
                self.assertNotEqual(result["transactions"][0]["row_id"], result["transactions"][1]["row_id"])

    def test_derivative_and_both_holdings_tables(self):
        derivative_fields = '''<conversionOrExercisePrice><value>123.4500</value></conversionOrExercisePrice>
          <exerciseDate><value>2027-01-01</value></exerciseDate><expirationDate><value>2030-01-01</value></expirationDate>
          <underlyingSecurity><underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
            <underlyingSecurityShares><value>1000.000</value></underlyingSecurityShares></underlyingSecurity>'''
        holding_fields = '''<securityTitle><value>Common Stock</value></securityTitle>
          <postTransactionAmounts><sharesOwnedFollowingTransaction><value>250</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
          <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>'''
        body = ("<nonDerivativeTable><nonDerivativeHolding>" + holding_fields + "</nonDerivativeHolding></nonDerivativeTable>"
                + "<derivativeTable>" + transaction("M", derivative=True, extra=derivative_fields)
                + "<derivativeHolding>" + holding_fields + derivative_fields + "</derivativeHolding></derivativeTable>" + FOOTNOTES)
        result = self.parse(filing(body, form="5"))
        self.assertEqual([row["row_id"] for row in result["holdings"]], [ACCESSION + ":ND-H:1", ACCESSION + ":D-H:1"])
        self.assertEqual(result["transactions"][0]["row_id"], ACCESSION + ":D-T:1")
        derivative = result["transactions"][0]
        self.assertEqual(derivative["conversion_or_exercise_price"], "123.4500")
        self.assertEqual(derivative["underlying_security_shares"], "1000.000")
        self.assertEqual(derivative["exercise_date"], "2027-01-01")
        self.assertEqual(derivative["expiration_date"], "2030-01-01")
        self.assertEqual(derivative["classification"], "exercise_or_conversion")
        self.assertEqual(result["holdings"][0]["shares_owned_following"], "250")
        self.assertEqual(result["holdings"][0]["transaction_value"], "")
        self.assertEqual(result["holdings"][0]["classification"], "holding")

    def test_missing_footnote_only_zero_and_invalid_prices(self):
        cases = [(None, ""), ("0", "0.00"), ("N/A", ""), ("NaN", ""), ("Infinity", ""), ("1,000.00", ""), ("-1", "")]
        for price, expected in cases:
            with self.subTest(price=price):
                body = "<nonDerivativeTable>" + transaction(price=price, price_refs='<footnoteId id="F2"/>') + "</nonDerivativeTable>" + FOOTNOTES
                row = self.parse(filing(body))["transactions"][0]
                self.assertEqual(row["transaction_value"], expected)
                self.assertEqual(row["transaction_price_per_share"], "" if price is None else price)
                self.assertEqual(row["normalized_footnote_refs"]["transaction_price_per_share"], ["F2"])
                self.assertEqual(bool(row["warnings"]), price not in (None, "0"))

    def test_decimal_product_is_not_rounded_at_default_context_precision(self):
        body = "<nonDerivativeTable>" + transaction(shares="123456789012345678901234567890", price="0.000000000000000000000000000001") + "</nonDerivativeTable>" + FOOTNOTES
        row = self.parse(filing(body))["transactions"][0]
        self.assertEqual(row["transaction_value"], "0.123456789012345678901234567890")

    def test_reported_total_value_is_separate_from_calculated_value(self):
        # Even a nonconforming source with both alternative amounts must retain
        # both values instead of silently replacing one with the other.
        derivative = transaction(derivative=True, shares="2.50", price="10.1250").replace(
            "</transactionAmounts>",
            '<transactionTotalValue><value>30.000000</value><footnoteId id="F2"/></transactionTotalValue></transactionAmounts>',
        )
        body = "<derivativeTable>" + derivative + "</derivativeTable>" + FOOTNOTES
        row = self.parse(filing(body))["transactions"][0]
        self.assertEqual(row["reported_transaction_total_value"], "30.000000")
        self.assertEqual(row["transaction_value"], "25.312500")
        self.assertEqual(row["raw_fields"]["transactionAmounts/transactionTotalValue/value"], "30.000000")
        self.assertEqual(row["normalized_footnote_refs"]["reported_transaction_total_value"], ["F2"])
        self.assertIn("BOTH_TRANSACTION_SHARES_AND_TOTAL_VALUE_REPORTED", row["warnings"])

    def test_total_value_without_shares_is_not_multiplied_by_price(self):
        derivative = transaction(derivative=True, shares="", price="10.1250").replace(
            "</transactionAmounts>",
            '<transactionTotalValue><value>1000.00</value></transactionTotalValue></transactionAmounts>',
        )
        row = self.parse(filing("<derivativeTable>" + derivative + "</derivativeTable>" + FOOTNOTES))["transactions"][0]
        self.assertEqual(row["reported_transaction_total_value"], "1000.00")
        self.assertEqual(row["transaction_value"], "")
        self.assertEqual(row["transaction_value_basis"], "")
        self.assertEqual(row["warnings"], [])

    def test_late_form3_holding_on_form5_preserves_transaction_form_type(self):
        body = '''<nonDerivativeTable><nonDerivativeHolding>
          <securityTitle><value>Common Stock</value></securityTitle>
          <transactionCoding><transactionFormType>3</transactionFormType></transactionCoding>
          <postTransactionAmounts><sharesOwnedFollowingTransaction><value>10</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
        </nonDerivativeHolding></nonDerivativeTable>'''
        row = self.parse(filing(body, form="5"))["holdings"][0]
        self.assertEqual(row["document_type"], "5")
        self.assertEqual(row["transaction_form_type"], "3")
        self.assertEqual(row["classification"], "holding")
        self.assertEqual(row["transaction_code"], "")
        self.assertEqual(row["warnings"], [])

    def test_code_classifications_do_not_confuse_cash_flows(self):
        expectations = {"P": "purchase", "S": "sale", "A": "award", "F": "tax_or_exercise_payment",
                        "M": "exercise_or_conversion", "C": "exercise_or_conversion", "X": "exercise_or_conversion",
                        "O": "exercise_or_conversion", "G": "gift", "D": "issuer_disposition", "J": "other", "?": "unknown"}
        body = "<nonDerivativeTable>" + "".join(transaction(code=code) for code in expectations) + "</nonDerivativeTable>" + FOOTNOTES
        result = self.parse(filing(body))
        for row, (code, expected) in zip(result["transactions"], expectations.items()):
            self.assertEqual(row["transaction_code"], code)
            self.assertEqual(row["classification"], expected)
            self.assertEqual(row["market_scope"], "open_or_private" if code in {"P", "S"} else "")
        self.assertIn("UNKNOWN_TRANSACTION_CODE:?", result["transactions"][-1]["warnings"])
        self.assertEqual(result["transactions"][-1]["raw_fields"]["transactionCoding/transactionCode"], "?")

    def test_plan_checkbox_is_filing_only_and_absence_stays_unknown(self):
        body = "<nonDerivativeTable>" + transaction("S") + transaction("A") + "</nonDerivativeTable>" + FOOTNOTES
        for checkbox in ("", "<aff10b5One>0</aff10b5One>", "<aff10b5One>1</aff10b5One>"):
            with self.subTest(checkbox=checkbox):
                result = self.parse(filing(body, header=checkbox))
                self.assertEqual(result["filing"]["aff10b5_one"], "1" if ">1<" in checkbox else "0" if checkbox else "")
                for row in result["transactions"]:
                    self.assertNotIn("aff10b5_one", row)
                    self.assertNotIn("is_10b5_1", row)

    def test_namespaces_no_securities_and_signatures(self):
        result = self.parse(filing(form="3", header="<noSecuritiesOwned>1</noSecuritiesOwned>", namespace=' xmlns="urn:sec:ownership"'))
        self.assertEqual(result["filing"]["no_securities_owned"], "1")
        self.assertEqual(result["transactions"], [])
        self.assertEqual(result["holdings"], [])
        self.assertEqual(result["signatures"][0]["signature_name"], "/s/ Example One by Attorney")
        self.assertEqual(result["signatures"][0]["signature_date"], "2026-09-03")
        self.assertEqual(result["filing"]["issuer_name"], "S&P Global Inc.")

    def test_xml_encoding_bom_processing_instructions_and_prefixes(self):
        text = filing().decode("utf-8")
        for data in (
            b"\xef\xbb\xbf" + text.encode("utf-8"),
            text.replace('encoding="UTF-8"', 'encoding="UTF-16"').encode("utf-16"),
            text.replace("<ownershipDocument>", '<?xml-stylesheet href="untrusted.xsl" type="text/xsl"?><ownershipDocument>') .encode("utf-8"),
            text.replace("<ownershipDocument>", '<sec:ownershipDocument xmlns:sec="urn:sec:ownership">').replace("</ownershipDocument>", "</sec:ownershipDocument>") .encode("utf-8"),
        ):
            with self.subTest(prefix=data[:30]):
                self.assertEqual(self.parse(data)["filing"]["issuer_name"], "S&P Global Inc.")

    def test_invalid_xml_control_character_is_rejected_without_silent_removal(self):
        with self.assertRaises(OwnershipXMLParseError):
            self.parse(filing(header="<remarks>Before\x01After</remarks>"))

    def test_unresolved_and_duplicate_footnotes_are_visible(self):
        body = "<nonDerivativeTable>" + transaction() + "</nonDerivativeTable><footnotes><footnote id=\"F1\">One</footnote><footnote id=\"F1\">Two</footnote></footnotes>"
        result = self.parse(filing(body))
        self.assertEqual(len(result["footnotes"]), 2)
        self.assertIn("DUPLICATE_FOOTNOTE_ID:F1", result["filing"]["warnings"])
        self.assertIn("UNRESOLVED_FOOTNOTE_ID:F2", result["filing"]["warnings"])
        self.assertIn("UNRESOLVED_FOOTNOTE_ID:F2", result["transactions"][0]["warnings"])

    def test_malformed_nonownership_and_unsupported_forms_rejected(self):
        for data in (b"<ownershipDocument>", b"<html>Access denied</html>", filing(form="10-K")):
            with self.subTest(data=data[:40]):
                with self.assertRaises(OwnershipXMLParseError):
                    self.parse(data)

    def test_dtd_external_entities_and_utf16_entities_rejected(self):
        unsafe = '''<?xml version="1.0"?><!DOCTYPE ownershipDocument [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
          <ownershipDocument><documentType>4</documentType><remarks>&xxe;</remarks></ownershipDocument>'''
        inputs = [unsafe.encode(), unsafe.replace('version="1.0"', 'version="1.0" encoding="UTF-16"').encode("utf-16"),
                  b'<!DOCTYPE ownershipDocument SYSTEM "https://example.invalid/remote.dtd"><ownershipDocument><documentType>4</documentType></ownershipDocument>']
        for data in inputs:
            with self.subTest(encoding=data[:20]):
                with self.assertRaisesRegex(OwnershipXMLParseError, "prohibited"):
                    self.parse(data)

    def test_repeated_parse_is_deterministic(self):
        data = filing("<nonDerivativeTable>" + transaction() + "</nonDerivativeTable>" + FOOTNOTES)
        self.assertEqual(self.parse(data), self.parse(data))

    def test_pathological_nesting_is_rejected_before_recursive_capture(self):
        with self.assertRaisesRegex(OwnershipXMLParseError, "nesting"):
            self.parse(filing("<future>" * 130 + "x" + "</future>" * 130))


if __name__ == "__main__":
    unittest.main()
