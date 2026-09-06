"""Untrusted filing XML must not resolve local files or process DTDs."""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from lxml import etree

import pipeline
import sec_13f_bulk_backfill as bulk
from tests.test_sec_13f_bulk_backfill import archive_cover, archive_xml_table


FILING = """<root><periodOfReport>06-30-2026</periodOfReport>
<infoTable><nameOfIssuer>APPLE INC</nameOfIssuer><titleOfClass>COM</titleOfClass>
<cusip>037833100</cusip><value>200</value><sshPrnamt>10</sshPrnamt>
<sshPrnamtType>SH</sshPrnamtType></infoTable></root>"""


class FilingXMLSecurityTests(unittest.TestCase):
    def assert_filing_rejected(self, payload: bytes) -> None:
        with self.assertRaises(pipeline.FilingParseError):
            pipeline.parse_primary_document(payload)
        with self.assertRaises(pipeline.FilingParseError):
            pipeline._information_table_totals(payload)
        self.assertIsNone(pipeline.parse_information_table(payload))

    def test_plain_filing_keeps_metadata_totals_and_holdings(self) -> None:
        payload = FILING.encode()
        self.assertEqual("2026-06-30", pipeline.parse_primary_document(payload)["report_date"])
        self.assertEqual((1, 200), pipeline._information_table_totals(payload))
        rows = pipeline.parse_information_table(payload)
        self.assertEqual("APPLE INC", rows[0]["reported_issuer"])
        self.assertEqual(200, rows[0]["value"])

    def test_internal_entities_and_empty_doctypes_reject_whole_filing(self) -> None:
        entity = '<!DOCTYPE root [<!ENTITY issuer "LOCAL MARKER">]>'
        for xml in (
            "<!DOCTYPE root>" + FILING,
            entity + FILING.replace("APPLE INC", "&issuer;"),
        ):
            for encoding in ("utf-8", "utf-16"):
                with self.subTest(xml=xml[:60], encoding=encoding):
                    self.assert_filing_rejected(xml.encode(encoding))

    def test_external_parameter_entity_cannot_supply_filing_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            harmless_dtd = Path(directory) / "harmless.dtd"
            harmless_dtd.write_text('<!ENTITY issuer "LOCAL AUDIT MARKER">')
            xml = (
                '<!DOCTYPE root [<!ENTITY % external SYSTEM "'
                + harmless_dtd.as_uri()
                + '">%external;]>'
                + FILING.replace("APPLE INC", "&issuer;")
            )
            # Another module's global parser choice must not weaken this boundary.
            external_lookups = []

            class RecordingResolver(etree.Resolver):
                def resolve(self, system_url, public_id, context):
                    external_lookups.append(system_url)
                    return None

            parser_factory = etree.XMLParser

            def recording_parser(*args, **kwargs):
                parser = parser_factory(*args, **kwargs)
                parser.resolvers.add(RecordingResolver())
                return parser

            previous = etree.get_default_parser()
            etree.set_default_parser(recording_parser(resolve_entities=True))
            try:
                with mock.patch.object(etree, "XMLParser", side_effect=recording_parser):
                    self.assert_filing_rejected(xml.encode())
                self.assertEqual([], external_lookups)
            finally:
                etree.set_default_parser(previous)


class ArchiveXMLSecurityTests(unittest.TestCase):
    def parse_rows(self, xml: str):
        return bulk._parse_archive_xml_rows(
            xml,
            accession="0001234567-04-000001",
            source_url=(
                "https://www.sec.gov/Archives/edgar/data/1234567/"
                "000123456704000001/0001234567-04-000001.txt"
            ),
            source_sha256="a" * 64,
            document_number=1,
        )

    def test_plain_archive_cover_and_rows_still_parse(self) -> None:
        self.assertIsNotNone(bulk._parse_archive_cover_metadata(archive_cover()))
        self.assertEqual("037833100", self.parse_rows(archive_xml_table())[0]["reported_cusip"])

    def test_dtd_inputs_are_rejected_before_expat_receives_them(self) -> None:
        for declaration in (
            "<!DOCTYPE root>",
            '<!DOCTYPE root [<!ENTITY value "200">]>',
            '<!DOCTYPE root [<!ATTLIST e a0 CDATA "" a1 CDATA "">]>',
            '<!DOCTYPE root [<!ENTITY % external SYSTEM "file:///unused.dtd">%external;]>',
        ):
            xml = declaration + FILING
            for payload in (
                xml,
                xml.encode("utf-16le").decode("latin1"),
                xml.encode("utf-16be").decode("latin1"),
            ):
                with self.subTest(declaration=declaration, nul="\x00" in payload):
                    with mock.patch.object(bulk.ElementTree, "fromstring") as parse:
                        self.assertIsNone(bulk._parse_archive_cover_metadata(payload))
                        self.assertIsNone(self.parse_rows(payload))
                        parse.assert_not_called()


if __name__ == "__main__":
    unittest.main()
