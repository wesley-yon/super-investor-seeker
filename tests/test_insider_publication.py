from __future__ import annotations

import copy
import fcntl
import itertools
import json
import os
import select
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import data_contract
import insider_publication as publication_module
from insider_parser import INSIDER_PARSER_VERSION, parse_ownership_xml
from insider_pipeline import issuer_record_from_normalized, reduce_issuer_state
from insider_publication import (
    INSIDER_PUBLIC_CONTRACT_VERSION,
    MAX_PUBLIC_FILING_DETAIL_BYTES,
    MAX_PUBLIC_ISSUERS,
    MAX_PUBLIC_SECURITY_PAYLOAD_BYTES,
    InsiderPublicationError,
    build_insider_publication,
    build_static_insider_metric_projection,
    canonical_public_json_bytes,
    combine_insider_publications,
    validate_insider_public_tree,
    write_insider_publication,
)
from security_identity import stock_file_stem
from tests.test_insider_metrics import transaction_row


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "insider_filings"
ORACLE = json.loads((FIXTURE_ROOT / "expectations.json").read_text())
AS_OF = "2026-06-30T20:45:00Z"
SYNC_AT = "2026-06-30T20:40:00Z"
STOCK_ID = "03770N101"


def parse_case(name: str) -> dict[str, object]:
    case = ORACLE["filings"][name]
    return parse_ownership_xml(
        (FIXTURE_ROOT / case["filename"]).read_bytes(),
        accession_number=case["accession_number"],
        filing_date=case["filing_date"],
        accepted_at=case["accepted_at"],
        source_index_url=case["source_index_url"],
        source_document_url=case["source_document_url"],
    )


def issuer_state(filings: list[dict[str, object]]) -> dict[str, object]:
    records = [
        issuer_record_from_normalized(
            filing,
            parser_version=INSIDER_PARSER_VERSION,
        )
        for filing in filings
    ]
    issuer = filings[0]["issuer"]
    assert isinstance(issuer, dict)
    reduced = reduce_issuer_state(
        issuer_cik=issuer["cik"],
        records=records,
    )
    return reduced.issuer_state


def security_mapping(
    filing: dict[str, object],
    *,
    stock_id: str = STOCK_ID,
) -> dict[str, dict[str, object]]:
    keys = {
        row["security_class_key"]
        for collection in ("transactions", "holdings")
        for row in filing[collection]
        if row["source_table"] == "non_derivative"
    }
    return {
        key: {
            "stockId": stock_id,
            "fileStem": stock_file_stem(stock_id),
            "ticker": "TST",
            "companyName": "Synthetic Test Issuer",
            "securityType": "Common Stock",
            "securityTypeLabel": "COMMON STOCK",
            "cusip": stock_id,
            "primary": True,
        }
        for key in keys
    }


def walk_keys(value: object) -> list[str]:
    keys: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            keys.append(key)
            keys.extend(walk_keys(item))
    elif isinstance(value, list):
        for item in value:
            keys.extend(walk_keys(item))
    return keys


class InsiderPublicationTests(unittest.TestCase):
    def test_static_projection_embeds_every_bounded_row_for_client_pagination(
        self,
    ) -> None:
        rows = [
            transaction_row(
                f"row-{index}",
                accession=f"0000000001-26-{index:06d}",
                transaction_date="2026-06-01",
            )
            for index in range(1, 102)
        ]
        rows.extend(
            (
                transaction_row(
                    "old-row",
                    accession="0000000001-20-000001",
                    transaction_date="2020-01-01",
                ),
                transaction_row(
                    "non-ps-row",
                    accession="0000000001-26-000102",
                    transaction_date="2026-06-02",
                    code="A",
                ),
            )
        )
        projection = build_static_insider_metric_projection(
            rows,
            security_id=STOCK_ID,
            as_of=AS_OF,
            holdings=[],
            quality={"latestSuccessfulSyncAt": SYNC_AT},
        )

        self.assertEqual("all", projection["filters"]["range"])
        self.assertEqual("all", projection["filters"]["transactionScope"])
        self.assertEqual(103, projection["transactions"]["total"])
        self.assertEqual(103, len(projection["transactions"]["items"]))
        self.assertEqual(
            {"0000000001-20-000001", "0000000001-26-000102"},
            {
                item["accessionNumber"]
                for item in projection["transactions"]["items"]
                if item["accessionNumber"]
                in {"0000000001-20-000001", "0000000001-26-000102"}
            },
        )
        self.assertIsNone(projection["transactions"]["nextCursor"])
        self.assertEqual(
            {"itemCount": 103, "mode": "client", "pageSize": 100},
            projection["staticPagination"],
        )

    def test_insider_contract_is_separate_from_the_existing_site_contract(
        self,
    ) -> None:
        self.assertEqual(5, data_contract.DATA_CONTRACT_VERSION)
        self.assertEqual(1, INSIDER_PUBLIC_CONTRACT_VERSION)

    def test_publication_projects_fixture_rows_once_and_reconciles(self) -> None:
        simple = parse_case("form4_simple_purchase")
        joint = parse_case("form4_joint_sale_derivative")
        filings = [simple, joint]
        mappings = security_mapping(simple)
        mappings.update(security_mapping(joint))

        first = build_insider_publication(
            filings,
            issuer_state=issuer_state(filings),
            security_mappings=mappings,
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        second = build_insider_publication(
            reversed(filings),
            issuer_state=issuer_state(filings),
            security_mappings=dict(reversed(list(mappings.items()))),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        self.assertEqual(first, second)
        self.assertEqual({stock_file_stem(STOCK_ID)}, set(first.security_payloads))
        self.assertEqual(
            {
                "0000000001-26-000001",
                "0000000001-26-000002",
            },
            set(first.filing_payloads),
        )

        page = first.security_payloads[stock_file_stem(STOCK_ID)]
        self.assertEqual(5, page["data_contract_version"])
        self.assertEqual(1, page["insider_public_contract_version"])
        self.assertEqual("security_insider_activity", page["payloadType"])
        self.assertEqual(STOCK_ID, page["security"]["id"])
        self.assertEqual("1265.3625", page["summary"]["purchases"]["value"])
        self.assertEqual("5031.25", page["summary"]["sales"]["value"])
        self.assertEqual(2, page["summary"]["sales"]["transactionCount"])
        self.assertEqual(1, page["summary"]["sales"]["ownerGroupCount"])
        self.assertEqual(1, page["summary"]["sales"]["missingValueCount"])
        self.assertEqual("-3765.8875", page["summary"]["netPS"]["value"])
        self.assertEqual(3, page["transactions"]["total"])
        self.assertEqual(
            [
                "0000000001-26-000001",
                "0000000001-26-000002",
            ],
            [item["accessionNumber"] for item in page["filingRefs"]],
        )
        self.assertNotIn("filingPayloads", first.manifest)
        self.assertEqual(
            {
                (item["accessionNumber"], item["transactionDate"], item["value"])
                for item in page["transactions"]["items"]
            },
            {
                (item["accessionNumber"], item["transactionDate"], item["value"])
                for item in page["chartEvents"]
            },
        )
        sale_owners = {
            item["ownerGroup"]["displayName"]
            for item in page["transactions"]["items"]
            if item["transactionCode"] == "S"
        }
        self.assertEqual(1, len(sale_owners))
        self.assertTrue(
            any(
                item["ownerGroup"]["ownerCount"] == 2
                for item in page["transactions"]["items"]
                if item["transactionCode"] == "S"
            )
        )
        self.assertEqual(2, page["dataQuality"]["unmappedSecurityRowCount"])
        self.assertTrue(page["dataQuality"]["partial"])
        self.assertEqual(
            "not_integrated_phase4", page["dataFreshness"]["priceDataStatus"]
        )
        self.assertIn(
            "Owner counts group identical names as reported in each filing",
            page["methodologyBanner"]["text"],
        )
        self.assertLessEqual(
            len(canonical_public_json_bytes(page)),
            MAX_PUBLIC_SECURITY_PAYLOAD_BYTES,
        )

        detail = first.filing_payloads["0000000001-26-000002"]
        self.assertEqual("insider_filing_detail", detail["payloadType"])
        self.assertEqual(
            [
                {
                    "companyTitle": "Chief Executive Officer",
                    "nameAsFiled": "SYNTHETIC OWNER BETA",
                    "roles": ["Officer", "Director"],
                },
                {
                    "companyTitle": "10% Owner",
                    "nameAsFiled": "SYNTHETIC TEST ENTITY",
                    "roles": ["TenPercentOwner", "Other"],
                },
            ],
            detail["owners"],
        )
        self.assertEqual(3, len(detail["transactions"]))
        self.assertTrue(
            any(item["priceIsWeightedAverage"] for item in detail["transactions"])
        )
        self.assertNotIn("footnotes", detail)
        self.assertNotIn("amendmentHistory", detail)
        self.assertNotIn("fieldFootnoteLinks", detail)
        self.assertNotIn("lineage", detail)
        self.assertNotIn("signatures", detail)
        self.assertNotIn("remarks", detail["filing"])
        self.assertNotIn("securityTitleAsFiled", walk_keys(detail))
        self.assertNotIn("underlyingSecurityTitle", walk_keys(detail))
        self.assertNotIn("key", detail["ownerGroup"])
        self.assertEqual(
            {
                "displayName",
                "isJoint",
                "ownerCount",
                "primaryTitle",
                "roles",
            },
            set(detail["ownerGroup"]),
        )
        self.assertEqual(
            ORACLE["filings"]["form4_joint_sale_derivative"]["source_document_url"],
            detail["source"]["documentUrl"],
        )
        self.assertLessEqual(
            len(canonical_public_json_bytes(detail)),
            MAX_PUBLIC_FILING_DETAIL_BYTES,
        )

        forbidden_keys = {
            "address",
            "attributes",
            "amendmentHistory",
            "children",
            "field_sources",
            "fieldFootnoteLinks",
            "fieldFootnotes",
            "footnoteIds",
            "footnotes",
            "has_restricted_address_source",
            "lineage",
            "normalized_sha256",
            "ownerAggregationSlot",
            "ownerGroupKey",
            "owner_group_key",
            "parse_error",
            "parserVersion",
            "parser_version",
            "privacy",
            "raw_document",
            "raw_footnote",
            "raw_owner",
            "raw_row",
            "raw_signature",
            "requiresReview",
            "remarks",
            "restrictedAddress",
            "restricted_address",
            "rowKey",
            "securityTitleAsFiled",
            "signature",
            "signatures",
            "sourcePath",
            "sourceRowIndex",
            "sourceRowKeys",
            "sourceTable",
            "source_path",
            "underlyingSecurityTitle",
            "unknown_elements",
            "warnings",
        }
        for payload in [
            *first.security_payloads.values(),
            *first.filing_payloads.values(),
        ]:
            keys = set(walk_keys(payload))
            self.assertTrue(forbidden_keys.isdisjoint(keys), forbidden_keys & keys)
            rendered = canonical_public_json_bytes(payload)
            self.assertNotIn(b"PRIVATE_SENTINEL", rendered)
            self.assertNotIn(b"/Users/", rendered)
        private_address_values = {
            value
            for filing in filings
            for owner in filing["owners"]
            for field, value in owner["restricted_address"].items()
            if value and field in {"street1", "street2", "city", "state_description"}
        }
        rendered_publication = b"".join(
            canonical_public_json_bytes(payload)
            for payload in [
                *first.security_payloads.values(),
                *first.filing_payloads.values(),
            ]
        )
        for private_value in private_address_values:
            self.assertNotIn(private_value.encode(), rendered_publication)
        private_narrative_values = {
            value
            for filing in filings
            for value in (
                filing["remarks"],
                *(footnote["text"] for footnote in filing["footnotes"]),
                *(signature["name"] for signature in filing["signatures"]),
                *(
                    row["nature_of_ownership"]
                    for collection in ("transactions", "holdings")
                    for row in filing[collection]
                ),
            )
            if value
        }
        for private_value in private_narrative_values:
            self.assertNotIn(private_value.encode(), rendered_publication)

    def test_filing_references_are_scoped_to_each_security(self) -> None:
        simple = parse_case("form4_simple_purchase")
        joint = parse_case("form4_joint_sale_derivative")
        filings = [simple, joint]
        mappings = security_mapping(simple)
        joint_transactions = joint["transactions"]
        assert isinstance(joint_transactions, list)
        derivative_key = next(
            row["security_class_key"]
            for row in joint_transactions
            if row["source_table"] == "derivative"
        )
        derivative_stock_id = "SYNTHOPT1"
        mappings[derivative_key] = {
            "stockId": derivative_stock_id,
            "fileStem": stock_file_stem(derivative_stock_id),
            "ticker": "OPT",
            "companyName": "Synthetic Test Issuer",
            "securityType": "Option",
            "securityTypeLabel": "OPTION",
            "cusip": None,
            "primary": False,
        }

        publication = build_insider_publication(
            filings,
            issuer_state=issuer_state(filings),
            security_mappings=mappings,
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        common_page = publication.security_payloads[stock_file_stem(STOCK_ID)]
        derivative_page = publication.security_payloads[
            stock_file_stem(derivative_stock_id)
        ]
        common_refs = common_page["filingRefs"]
        derivative_refs = derivative_page["filingRefs"]
        assert isinstance(common_refs, list)
        assert isinstance(derivative_refs, list)
        self.assertEqual(
            ["0000000001-26-000001", "0000000001-26-000002"],
            [ref["accessionNumber"] for ref in common_refs],
        )
        self.assertEqual(
            ["0000000001-26-000002"],
            [ref["accessionNumber"] for ref in derivative_refs],
        )
        self.assertEqual(
            {"0000000001-26-000001", "0000000001-26-000002"},
            set(publication.filing_payloads),
        )

    def test_untrusted_officer_title_cannot_smuggle_personal_information(self) -> None:
        case = ORACLE["filings"]["form4_joint_sale_derivative"]
        fixture = (FIXTURE_ROOT / case["filename"]).read_bytes()
        for unsafe_title in (
            b"123 Main Street, Springfield, 90210",
            b"JOHN Q PUBLIC",
            b"Chief, 12 Broadway",
            b"Chief 555-123-4567",
        ):
            with self.subTest(unsafe_title=unsafe_title):
                raw_xml = fixture.replace(b"TEST CHIEF EXECUTIVE", unsafe_title)
                filing = parse_ownership_xml(
                    raw_xml,
                    accession_number=case["accession_number"],
                    filing_date=case["filing_date"],
                    accepted_at=case["accepted_at"],
                    source_index_url=case["source_index_url"],
                    source_document_url=case["source_document_url"],
                )

                publication = build_insider_publication(
                    [filing],
                    issuer_state=issuer_state([filing]),
                    security_mappings=security_mapping(filing),
                    as_of=AS_OF,
                    latest_successful_sync_at=SYNC_AT,
                )

                accession = filing["accession_number"]
                assert isinstance(accession, str)
                detail = publication.filing_payloads[accession]
                owners = detail["owners"]
                owner_group = detail["ownerGroup"]
                assert isinstance(owners, list)
                assert isinstance(owners[0], dict)
                assert isinstance(owner_group, dict)
                self.assertEqual("Officer", owners[0]["companyTitle"])
                self.assertEqual("Officer", owner_group["primaryTitle"])
                rendered = canonical_public_json_bytes(detail)
                self.assertNotIn(unsafe_title, rendered)

    def test_reporting_owner_cik_cannot_be_smuggled_through_public_name(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        fixture = (FIXTURE_ROOT / case["filename"]).read_bytes()
        raw_xml = fixture.replace(
            b"SYNTHETIC OWNER ALPHA",
            b"SYNTHETIC OWNER 0000000002",
        )
        filing = parse_ownership_xml(
            raw_xml,
            accession_number=case["accession_number"],
            filing_date=case["filing_date"],
            accepted_at=case["accepted_at"],
            source_index_url=case["source_index_url"],
            source_document_url=case["source_document_url"],
        )

        with self.assertRaisesRegex(InsiderPublicationError, "owner name"):
            build_insider_publication(
                [filing],
                issuer_state=issuer_state([filing]),
                security_mappings=security_mapping(filing),
                as_of=AS_OF,
                latest_successful_sync_at=SYNC_AT,
            )

    def test_reporting_owner_address_cannot_be_smuggled_through_public_name(
        self,
    ) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        fixture = (FIXTURE_ROOT / case["filename"]).read_bytes()
        for unsafe_name in (
            b"123 Main St",
            b"45 Oak Rd",
            b"12 First Ave",
            b"PO Box 99",
            b"P.O. Box 99",
            b"123 Main Street, Unit 4",
            b"123 Main Street, Suite 5",
            b"SYNTHETIC OWNER 90210",
            b"SYNTHETIC OWNER K1A 0B1",
            b"URL: example.test",
            b"URI: example.test",
            b"Website: example.test",
            b"Web: example.test",
            b"Homepage: example.test",
            b"Site: example.test",
            b"Contact: example.test",
            b"URL example.test",
            b"Website example.test",
            b"Web Site: example.test",
            b"Home Page: example.test",
            b"Internet: example.test",
            b"URL - example.test",
            b"Contact = example.test",
            b"example.test",
            b"example.com/path",
            b"ftp://example.test",
            b"ftp:example.test",
            b"sftp://example.test",
            b"192.0.2.1",
            b"[2001:db8::1]",
            b"Box 12",
            b"Postal Box 12",
            b"Mailbox 12",
            b"Drawer 9",
            b"Lock Box 12",
            b"Post Box 12",
            b"P.O.B. 12",
            b"POB 12",
            b"Rural Route 2 Box 5",
            b"RR 2 Box 5",
            b"HC 3 Box 10",
            b"Route 2 Box 5",
            b"Rte 2 Box 5",
            b"General Delivery",
            b"Private Bag 4",
            b"Locked Bag 3",
            b"Poste Restante",
            b"C/O SYNTHETIC OWNER",
            b"CARE-OF SYNTHETIC OWNER",
            b"CARE.OF SYNTHETIC OWNER",
            b"C O SYNTHETIC OWNER",
            b"C.O. SYNTHETIC OWNER",
            b"ATTN: SYNTHETIC OWNER",
            b"ATTN SYNTHETIC OWNER",
            b"ATTENTION SYNTHETIC OWNER",
        ):
            with self.subTest(unsafe_name=unsafe_name):
                raw_xml = fixture.replace(b"SYNTHETIC OWNER ALPHA", unsafe_name)
                filing = parse_ownership_xml(
                    raw_xml,
                    accession_number=case["accession_number"],
                    filing_date=case["filing_date"],
                    accepted_at=case["accepted_at"],
                    source_index_url=case["source_index_url"],
                    source_document_url=case["source_document_url"],
                )

                with self.assertRaisesRegex(InsiderPublicationError, "owner name"):
                    build_insider_publication(
                        [filing],
                        issuer_state=issuer_state([filing]),
                        security_mappings=security_mapping(filing),
                        as_of=AS_OF,
                        latest_successful_sync_at=SYNC_AT,
                    )

    def test_public_name_privacy_gate_preserves_safe_legal_names(self) -> None:
        case = ORACLE["filings"]["form4_simple_purchase"]
        fixture = (FIXTURE_ROOT / case["filename"]).read_bytes()
        for safe_name in (
            b"WEBSTER FAMILY TRUST",
            b"BOX INC",
            b"CONTACT ENERGY LIMITED",
            b"SITEONE LANDSCAPE SUPPLY INC",
            b"GENERAL DYNAMICS CORPORATION",
            b"3M COMPANY",
            b"ST CLAIR FAMILY TRUST",
            b"ACME CO",
            b"J. P. MORGAN TRUST",
        ):
            with self.subTest(safe_name=safe_name):
                raw_xml = fixture.replace(b"SYNTHETIC OWNER ALPHA", safe_name)
                filing = parse_ownership_xml(
                    raw_xml,
                    accession_number=case["accession_number"],
                    filing_date=case["filing_date"],
                    accepted_at=case["accepted_at"],
                    source_index_url=case["source_index_url"],
                    source_document_url=case["source_document_url"],
                )

                publication = build_insider_publication(
                    [filing],
                    issuer_state=issuer_state([filing]),
                    security_mappings=security_mapping(filing),
                    as_of=AS_OF,
                    latest_successful_sync_at=SYNC_AT,
                )
                accession = filing["accession_number"]
                assert isinstance(accession, str)
                detail = publication.filing_payloads[accession]
                owners = detail["owners"]
                assert isinstance(owners, list)
                assert isinstance(owners[0], dict)
                self.assertEqual(safe_name.decode("ascii"), owners[0]["nameAsFiled"])

    def test_resolved_amendment_supersedes_original_without_overwrite(self) -> None:
        original = parse_case("form4_simple_purchase")
        amendment = parse_case("form4_amendment")
        filings = [original, amendment]
        mappings = security_mapping(original)
        mappings.update(security_mapping(amendment))

        publication = build_insider_publication(
            filings,
            issuer_state=issuer_state(filings),
            security_mappings=mappings,
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        page = publication.security_payloads[stock_file_stem(STOCK_ID)]
        self.assertEqual("1296.225", page["summary"]["purchases"]["value"])
        self.assertEqual(1, page["summary"]["purchases"]["transactionCount"])
        self.assertEqual(
            ["0000000001-26-000005"],
            [item["accessionNumber"] for item in page["transactions"]["items"]],
        )
        original_detail = publication.filing_payloads["0000000001-26-000001"]
        amendment_detail = publication.filing_payloads["0000000001-26-000005"]
        self.assertFalse(original_detail["filing"]["isCurrentEffectiveVersion"])
        self.assertTrue(amendment_detail["filing"]["isCurrentEffectiveVersion"])
        self.assertNotIn("amendmentHistory", original_detail)
        self.assertNotIn("amendmentHistory", amendment_detail)
        self.assertEqual(0, page["dataQuality"]["unresolvedAmendmentCount"])

    def test_unresolved_amendment_stays_visible_but_is_not_mislabeled_superseded(
        self,
    ) -> None:
        amendment = parse_case("form4_amendment")
        publication = build_insider_publication(
            [amendment],
            issuer_state=issuer_state([amendment]),
            security_mappings=security_mapping(amendment),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        page = publication.security_payloads[stock_file_stem(STOCK_ID)]
        detail = publication.filing_payloads[amendment["accession_number"]]
        self.assertEqual(0, page["transactions"]["total"])
        self.assertEqual(1, page["dataQuality"]["unresolvedAmendmentCount"])
        self.assertTrue(page["dataQuality"]["partial"])
        self.assertIsNone(detail["filing"]["isCurrentEffectiveVersion"])
        self.assertTrue(
            all(row["isSuperseded"] is None for row in detail["transactions"])
        )
        self.assertNotIn("amendmentHistory", detail)

    def test_latest_reported_holdings_are_mapped_without_derivative_inference(
        self,
    ) -> None:
        filing = parse_case("form3_holdings_only")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        page = publication.security_payloads[stock_file_stem(STOCK_ID)]
        self.assertEqual(0, page["transactions"]["total"])
        holdings = page["sidebar"]["latestReportedHoldings"]
        all_holdings = (
            holdings["officersAndDirectors"] + holdings["tenPercentOwnersAndEntities"]
        )
        self.assertTrue(all_holdings)
        self.assertEqual("100.000000001", all_holdings[0]["shares"])
        self.assertEqual(1, page["dataQuality"]["unmappedSecurityRowCount"])
        detail = publication.filing_payloads[filing["accession_number"]]
        detail_holdings = detail["holdings"]
        assert isinstance(detail_holdings, list)
        self.assertTrue(detail_holdings)
        for public_holding in detail_holdings:
            assert isinstance(public_holding, dict)
            self.assertNotIn("ownerAggregationSlot", public_holding)
            self.assertNotIn("ownerGroupKey", public_holding)
            self.assertNotIn("privateOwnerGroupKey", public_holding)
        self.assertNotIn(
            b"ownerAggregationSlot",
            canonical_public_json_bytes(detail),
        )
        derivative = [
            row for row in detail_holdings if row["underlyingShares"] is not None
        ]
        self.assertEqual(1, len(derivative))
        self.assertIsNone(derivative[0]["normalizedSecurityId"])

    def test_writer_rejects_traversal_keys_before_touching_private_state(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            private = root / "data/insiders/private/state.json"
            private.parent.mkdir(parents=True)
            private.write_text("PRIVATE_SENTINEL")

            malicious_security = copy.deepcopy(publication)
            security_payload = malicious_security.security_payloads.pop(
                stock_file_stem(STOCK_ID)
            )
            malicious_security.security_payloads["../../private/state"] = (
                security_payload
            )
            with self.assertRaisesRegex(InsiderPublicationError, "security file stem"):
                write_insider_publication(
                    malicious_security,
                    repository_root=root,
                )
            self.assertEqual("PRIVATE_SENTINEL", private.read_text())

            malicious_filing = copy.deepcopy(publication)
            filing_payload = malicious_filing.filing_payloads.pop(
                "0000000001-26-000001"
            )
            malicious_filing.filing_payloads["../../private/state"] = filing_payload
            with self.assertRaisesRegex(InsiderPublicationError, "filing accession"):
                write_insider_publication(
                    malicious_filing,
                    repository_root=root,
                )
            self.assertEqual("PRIVATE_SENTINEL", private.read_text())

    def test_writer_rejects_valid_descendant_replacement_before_commit(self) -> None:
        filing = parse_case("form4_simple_purchase")
        expected_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        foreign_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of="2026-07-01T20:45:00Z",
            latest_successful_sync_at="2026-07-01T20:40:00Z",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            foreign_root = parent / "foreign"
            target_root = parent / "target"
            foreign_root.mkdir()
            target_root.mkdir()
            write_insider_publication(
                foreign_publication,
                repository_root=foreign_root,
            )
            foreign_public = foreign_root / "data/insiders/public"
            real_checkpoint = publication_module._publication_checkpoint
            swapped = False

            def replace_stage_descendants(
                label: str,
                root_path: Path,
                root_fd: int,
                data_fd: int,
                insiders_fd: int,
            ) -> None:
                nonlocal swapped
                if label == "before_commit" and not swapped:
                    insiders_root = target_root / "data/insiders"
                    stages = list(insiders_root.glob(".public.prepare-*"))
                    self.assertEqual(1, len(stages))
                    stage = stages[0]
                    for directory_name in ("securities", "filings"):
                        shutil.rmtree(stage / directory_name)
                        shutil.copytree(
                            foreign_public / directory_name,
                            stage / directory_name,
                        )
                    shutil.copy2(
                        foreign_public / "manifest.json",
                        stage / "manifest.json",
                    )
                    swapped = True
                real_checkpoint(label, root_path, root_fd, data_fd, insiders_fd)

            with mock.patch(
                "insider_publication._publication_checkpoint",
                side_effect=replace_stage_descendants,
            ):
                with self.assertRaisesRegex(
                    InsiderPublicationError,
                    "staged public tree changed",
                ):
                    write_insider_publication(
                        expected_publication,
                        repository_root=target_root,
                    )

            self.assertTrue(swapped)
            self.assertFalse((target_root / "data/insiders/public").exists())
            self.assertEqual([], validate_insider_public_tree(foreign_public))

    def test_writer_rolls_back_installed_descendant_replacement(self) -> None:
        filing = parse_case("form4_simple_purchase")

        def publication_at(as_of: str, sync_at: str):
            return build_insider_publication(
                [filing],
                issuer_state=issuer_state([filing]),
                security_mappings=security_mapping(filing),
                as_of=as_of,
                latest_successful_sync_at=sync_at,
            )

        prior_publication = publication_at(AS_OF, SYNC_AT)
        expected_publication = publication_at(
            "2026-07-01T20:45:00Z",
            "2026-07-01T20:40:00Z",
        )
        foreign_publication = publication_at(
            "2026-07-02T20:45:00Z",
            "2026-07-02T20:40:00Z",
        )

        for checkpoint in ("before_backup_cleanup", "before_return"):
            with (
                self.subTest(checkpoint=checkpoint),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                parent = Path(tmpdir)
                foreign_root = parent / "foreign"
                target_root = parent / "target"
                foreign_root.mkdir()
                target_root.mkdir()
                write_insider_publication(
                    foreign_publication,
                    repository_root=foreign_root,
                )
                write_insider_publication(
                    prior_publication,
                    repository_root=target_root,
                )
                foreign_public = foreign_root / "data/insiders/public"
                target_public = target_root / "data/insiders/public"
                real_checkpoint = publication_module._publication_checkpoint
                swapped = False

                def replace_installed_descendants(
                    label: str,
                    root_path: Path,
                    root_fd: int,
                    data_fd: int,
                    insiders_fd: int,
                ) -> None:
                    nonlocal swapped
                    if label == checkpoint and not swapped:
                        for directory_name in ("securities", "filings"):
                            shutil.rmtree(target_public / directory_name)
                            shutil.copytree(
                                foreign_public / directory_name,
                                target_public / directory_name,
                            )
                        shutil.copy2(
                            foreign_public / "manifest.json",
                            target_public / "manifest.json",
                        )
                        swapped = True
                    real_checkpoint(label, root_path, root_fd, data_fd, insiders_fd)

                with mock.patch(
                    "insider_publication._publication_checkpoint",
                    side_effect=replace_installed_descendants,
                ):
                    with self.assertRaisesRegex(
                        InsiderPublicationError,
                        "staged public tree changed",
                    ):
                        write_insider_publication(
                            expected_publication,
                            repository_root=target_root,
                        )

                self.assertTrue(swapped)
                self.assertEqual([], validate_insider_public_tree(target_public))
                manifest = json.loads((target_public / "manifest.json").read_text())
                self.assertEqual(AS_OF, manifest["asOf"])

    def test_writer_retains_backup_when_its_descendants_change(self) -> None:
        filing = parse_case("form4_simple_purchase")

        def publication_at(as_of: str, sync_at: str):
            return build_insider_publication(
                [filing],
                issuer_state=issuer_state([filing]),
                security_mappings=security_mapping(filing),
                as_of=as_of,
                latest_successful_sync_at=sync_at,
            )

        prior_publication = publication_at(AS_OF, SYNC_AT)
        expected_as_of = "2026-07-01T20:45:00Z"
        expected_publication = publication_at(
            expected_as_of,
            "2026-07-01T20:40:00Z",
        )
        foreign_as_of = "2026-07-02T20:45:00Z"
        foreign_publication = publication_at(
            foreign_as_of,
            "2026-07-02T20:40:00Z",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            foreign_root = parent / "foreign"
            target_root = parent / "target"
            foreign_root.mkdir()
            target_root.mkdir()
            write_insider_publication(
                foreign_publication,
                repository_root=foreign_root,
            )
            write_insider_publication(
                prior_publication,
                repository_root=target_root,
            )
            foreign_public = foreign_root / "data/insiders/public"
            insiders_root = target_root / "data/insiders"
            target_public = insiders_root / "public"
            real_checkpoint = publication_module._publication_checkpoint
            swapped = False

            def replace_backup_descendants(
                label: str,
                root_path: Path,
                root_fd: int,
                data_fd: int,
                insiders_fd: int,
            ) -> None:
                nonlocal swapped
                if label == "before_backup_cleanup" and not swapped:
                    backups = list(insiders_root.glob(".public.backup-*"))
                    self.assertEqual(1, len(backups))
                    backup = backups[0]
                    for entry in list(backup.iterdir()):
                        if entry.is_dir():
                            shutil.rmtree(entry)
                        else:
                            entry.unlink()
                    for entry in foreign_public.iterdir():
                        target = backup / entry.name
                        if entry.is_dir():
                            shutil.copytree(entry, target)
                        else:
                            shutil.copy2(entry, target)
                    swapped = True
                real_checkpoint(label, root_path, root_fd, data_fd, insiders_fd)

            with mock.patch(
                "insider_publication._publication_checkpoint",
                side_effect=replace_backup_descendants,
            ):
                with self.assertRaisesRegex(
                    InsiderPublicationError,
                    "backup tree digest",
                ):
                    write_insider_publication(
                        expected_publication,
                        repository_root=target_root,
                    )

            self.assertTrue(swapped)
            self.assertEqual([], validate_insider_public_tree(target_public))
            self.assertEqual(
                expected_as_of,
                json.loads((target_public / "manifest.json").read_text())["asOf"],
            )
            backups = list(insiders_root.glob(".public.backup-*"))
            self.assertEqual(1, len(backups))
            self.assertEqual(
                foreign_as_of,
                json.loads((backups[0] / "manifest.json").read_text())["asOf"],
            )
            self.assertEqual(
                1,
                len(list(insiders_root.glob(".public.transaction-*.json"))),
            )

    def test_writer_rejects_post_seal_byte_identical_descendant_replacement(
        self,
    ) -> None:
        filing = parse_case("form4_simple_purchase")

        def publication_at(as_of: str, sync_at: str):
            return build_insider_publication(
                [filing],
                issuer_state=issuer_state([filing]),
                security_mappings=security_mapping(filing),
                as_of=as_of,
                latest_successful_sync_at=sync_at,
            )

        prior_publication = publication_at(AS_OF, SYNC_AT)
        expected_publication = publication_at(
            "2026-07-01T20:45:00Z",
            "2026-07-01T20:40:00Z",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(prior_publication, repository_root=root)
            public_root = root / "data/insiders/public"
            real_replace = publication_module._replace_publication_directory_at
            replaced = False

            def replace_then_swap_descendant(
                parent_fd: int,
                source_name: str,
                target_name: str,
                label: str,
                *args: object,
                **kwargs: object,
            ) -> None:
                nonlocal replaced
                real_replace(
                    parent_fd,
                    source_name,
                    target_name,
                    label,
                    *args,
                    **kwargs,
                )
                if label == "public output publication" and not replaced:
                    security_path = next((public_root / "securities").glob("*.json"))
                    replacement = security_path.with_suffix(".foreign")
                    replacement.write_bytes(security_path.read_bytes())
                    os.replace(replacement, security_path)
                    replaced = True

            with mock.patch(
                "insider_publication._replace_publication_directory_at",
                side_effect=replace_then_swap_descendant,
            ):
                with self.assertRaisesRegex(
                    InsiderPublicationError,
                    "staged public tree changed",
                ):
                    write_insider_publication(
                        expected_publication,
                        repository_root=root,
                    )

            self.assertTrue(replaced)
            self.assertEqual([], validate_insider_public_tree(public_root))
            self.assertEqual(
                AS_OF,
                json.loads((public_root / "manifest.json").read_text())["asOf"],
            )

    def test_path_validator_holds_shared_publication_lock(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(publication, repository_root=root)
            public_root = root / "data/insiders/public"
            real_flock = publication_module.fcntl.flock
            lock_operations: list[int] = []

            def record_lock(descriptor: int, operation: int) -> None:
                lock_operations.append(operation)
                real_flock(descriptor, operation)

            with mock.patch(
                "insider_publication.fcntl.flock",
                side_effect=record_lock,
            ):
                self.assertEqual([], validate_insider_public_tree(public_root))

            self.assertEqual(
                [publication_module.fcntl.LOCK_SH, publication_module.fcntl.LOCK_UN],
                lock_operations,
            )

    def test_path_validator_holds_shared_lock_through_semantic_validation(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(publication, repository_root=root)
            insiders_root = root / "data/insiders"
            public_root = insiders_root / "public"
            real_validate = publication_module._validate_insider_public_snapshot_bytes
            validation_hook_ran = False

            def validate_while_probing_exclusive_lock(snapshot):
                nonlocal validation_hook_ran
                validation_hook_ran = True
                probe_fd = publication_module._open_publication_directory(
                    insiders_root,
                    "insider data root probe",
                )
                acquired = False
                try:
                    try:
                        fcntl.flock(probe_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        acquired = True
                    except BlockingIOError:
                        pass
                    self.assertFalse(
                        acquired,
                        "exclusive publication lock was acquired during semantic validation",
                    )
                finally:
                    if acquired:
                        fcntl.flock(probe_fd, fcntl.LOCK_UN)
                    os.close(probe_fd)
                return real_validate(snapshot)

            with mock.patch(
                "insider_publication._validate_insider_public_snapshot_bytes",
                side_effect=validate_while_probing_exclusive_lock,
            ):
                self.assertEqual([], validate_insider_public_tree(public_root))

            self.assertTrue(validation_hook_ran)

    def test_optional_path_validator_does_not_mask_semantic_io_failure(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(publication, repository_root=root)
            public_root = root / "data/insiders/public"
            with mock.patch(
                "insider_publication._validate_insider_public_snapshot_bytes",
                side_effect=FileNotFoundError("synthetic semantic read failure"),
            ):
                errors = publication_module.validate_optional_insider_public_tree(
                    public_root
                )

            self.assertTrue(errors)
            self.assertTrue(
                any("synthetic semantic read failure" in error for error in errors),
                errors,
            )

    def test_path_validator_waits_for_exclusive_publication_lock(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(publication, repository_root=root)
            insiders_root = root / "data/insiders"
            public_root = insiders_root / "public"
            insiders_fd = publication_module._open_publication_directory(
                insiders_root,
                "insider data root",
            )
            process: subprocess.Popen[str] | None = None
            locked = False
            try:
                fcntl.flock(insiders_fd, fcntl.LOCK_EX)
                locked = True
                script = (
                    "from pathlib import Path\n"
                    "from insider_publication import validate_insider_public_tree\n"
                    "print('READY', flush=True)\n"
                    f"errors = validate_insider_public_tree(Path({str(public_root)!r}))\n"
                    "print(f'DONE {len(errors)}', flush=True)\n"
                )
                process = subprocess.Popen(
                    [sys.executable, "-c", script],
                    cwd=Path(__file__).resolve().parents[1],
                    env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                assert process.stdout is not None
                self.assertEqual("READY\n", process.stdout.readline())
                readable, _, _ = select.select([process.stdout], [], [], 0.2)
                self.assertEqual([], readable)

                fcntl.flock(insiders_fd, fcntl.LOCK_UN)
                locked = False
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual("DONE 0\n", stdout)
                self.assertEqual("", stderr)
                self.assertEqual(0, process.returncode)
            finally:
                if locked:
                    fcntl.flock(insiders_fd, fcntl.LOCK_UN)
                os.close(insiders_fd)
                if process is not None and process.poll() is None:
                    process.kill()
                    process.communicate()

    def test_writer_retains_backup_changed_after_final_digest_check(self) -> None:
        filing = parse_case("form4_simple_purchase")

        def publication_at(as_of: str, sync_at: str):
            return build_insider_publication(
                [filing],
                issuer_state=issuer_state([filing]),
                security_mappings=security_mapping(filing),
                as_of=as_of,
                latest_successful_sync_at=sync_at,
            )

        prior_publication = publication_at(AS_OF, SYNC_AT)
        expected_publication = publication_at(
            "2026-07-01T20:45:00Z",
            "2026-07-01T20:40:00Z",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(prior_publication, repository_root=root)
            insiders_root = root / "data/insiders"
            real_remove = publication_module._remove_owned_publication_directory_at
            injected = False

            def inject_before_cleanup(
                parent_fd: int,
                name: str,
                directory_fd: int,
                label: str,
                *args: object,
                **kwargs: object,
            ) -> None:
                nonlocal injected
                if label == "publication backup" and not injected:
                    foreign_fd = os.open(
                        "foreign-after-digest.json",
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o600,
                        dir_fd=directory_fd,
                    )
                    try:
                        os.write(foreign_fd, b"FOREIGN\n")
                        os.fsync(foreign_fd)
                    finally:
                        os.close(foreign_fd)
                    injected = True
                real_remove(
                    parent_fd,
                    name,
                    directory_fd,
                    label,
                    *args,
                    **kwargs,
                )

            with mock.patch(
                "insider_publication._remove_owned_publication_directory_at",
                side_effect=inject_before_cleanup,
            ):
                with self.assertRaisesRegex(
                    InsiderPublicationError,
                    "backup.*(?:digest|changed)|publication rollback failed",
                ):
                    write_insider_publication(
                        expected_publication,
                        repository_root=root,
                    )

            self.assertTrue(injected)
            backups = list(insiders_root.glob(".public.backup-*"))
            self.assertEqual(1, len(backups))
            self.assertEqual(
                b"FOREIGN\n",
                (backups[0] / "foreign-after-digest.json").read_bytes(),
            )
            self.assertEqual(
                1,
                len(list(insiders_root.glob(".public.transaction-*.json"))),
            )

    def test_writer_retains_changed_prejournal_stage(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            insiders_root = root / "data/insiders"
            injected = False

            def inject_then_fail_record_creation(
                parent_fd: int,
                transaction: object,
            ) -> int:
                nonlocal injected
                stage_name = getattr(getattr(transaction, "stage"), "name")
                stage_fd = publication_module._open_publication_directory_at(
                    parent_fd,
                    stage_name,
                    "publication staging directory",
                )
                try:
                    foreign_fd = os.open(
                        "foreign-prejournal.json",
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o600,
                        dir_fd=stage_fd,
                    )
                    try:
                        os.write(foreign_fd, b"FOREIGN\n")
                        os.fsync(foreign_fd)
                    finally:
                        os.close(foreign_fd)
                finally:
                    os.close(stage_fd)
                injected = True
                raise RuntimeError("simulated pre-journal failure")

            with mock.patch(
                "insider_publication._create_publication_transaction_at",
                side_effect=inject_then_fail_record_creation,
            ):
                with self.assertRaisesRegex(RuntimeError, "pre-journal failure"):
                    write_insider_publication(publication, repository_root=root)

            self.assertTrue(injected)
            stages = list(insiders_root.glob(".public.prepare-*"))
            self.assertEqual(1, len(stages))
            self.assertEqual(
                b"FOREIGN\n",
                (stages[0] / "foreign-prejournal.json").read_bytes(),
            )
            self.assertEqual([], list(insiders_root.glob(".public.transaction-*.json")))

    def test_writer_rejects_repository_root_replacement_before_commit(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            root = parent / "repository"
            root.mkdir()
            displaced = parent / "displaced"
            replacement = parent / "replacement"
            replacement.mkdir()
            sentinel = replacement / "replacement-only.txt"
            sentinel.write_text("UNTOUCHED\n")
            real_checkpoint = getattr(
                __import__("insider_publication"),
                "_publication_checkpoint",
                lambda *_args, **_kwargs: None,
            )
            displaced_once = False

            def displace_before_commit(label: str, *args: object) -> None:
                nonlocal displaced_once
                if label == "before_commit" and not displaced_once:
                    displaced_once = True
                    root.rename(displaced)
                    replacement.rename(root)
                real_checkpoint(label, *args)

            with mock.patch(
                "insider_publication._publication_checkpoint",
                create=True,
                side_effect=displace_before_commit,
            ):
                with self.assertRaisesRegex(
                    InsiderPublicationError,
                    "repository root changed",
                ):
                    write_insider_publication(publication, repository_root=root)

            self.assertTrue(displaced_once)
            self.assertEqual("UNTOUCHED\n", (root / "replacement-only.txt").read_text())
            self.assertFalse((root / "data").exists())
            self.assertFalse((displaced / "data/insiders/public").exists())
            self.assertFalse(
                any(
                    entry.name.startswith(".public.prepare-")
                    for entry in (displaced / "data/insiders").iterdir()
                )
            )

    def test_writer_rejects_data_or_insider_root_replacement_before_commit(
        self,
    ) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        for component, message in (
            ("data", "data root changed"),
            ("insiders", "insider data root changed"),
        ):
            with (
                self.subTest(component=component),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                parent = Path(tmpdir)
                root = parent / "repository"
                root.mkdir()
                displaced = parent / f"displaced-{component}"
                replacement = parent / f"replacement-{component}"
                replacement.mkdir()
                sentinel = replacement / "replacement-only.txt"
                sentinel.write_text("UNTOUCHED\n")
                real_checkpoint = getattr(
                    __import__("insider_publication"),
                    "_publication_checkpoint",
                )
                displaced_once = False

                def displace_before_commit(label: str, *args: object) -> None:
                    nonlocal displaced_once
                    if label == "before_commit" and not displaced_once:
                        displaced_once = True
                        if component == "data":
                            target = root / "data"
                        else:
                            target = root / "data/insiders"
                        target.rename(displaced)
                        replacement.rename(target)
                    real_checkpoint(label, *args)

                with mock.patch(
                    "insider_publication._publication_checkpoint",
                    side_effect=displace_before_commit,
                ):
                    with self.assertRaisesRegex(InsiderPublicationError, message):
                        write_insider_publication(publication, repository_root=root)

                self.assertTrue(displaced_once)
                if component == "data":
                    replacement_target = root / "data"
                    detached_insiders = displaced / "insiders"
                else:
                    replacement_target = root / "data/insiders"
                    detached_insiders = displaced
                self.assertEqual(
                    "UNTOUCHED\n",
                    (replacement_target / "replacement-only.txt").read_text(),
                )
                self.assertFalse((replacement_target / "public").exists())
                self.assertFalse((detached_insiders / "public").exists())
                self.assertFalse(
                    any(
                        entry.name.startswith(".public.prepare-")
                        for entry in detached_insiders.iterdir()
                    )
                )

    def test_writer_revalidates_repository_root_immediately_before_return(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            root = parent / "repository"
            root.mkdir()
            displaced = parent / "displaced"
            replacement = parent / "replacement"
            replacement.mkdir()
            (replacement / "replacement-only.txt").write_text("UNTOUCHED\n")
            real_checkpoint = getattr(
                __import__("insider_publication"),
                "_publication_checkpoint",
            )
            displaced_once = False

            def displace_before_return(label: str, *args: object) -> None:
                nonlocal displaced_once
                if label == "before_return" and not displaced_once:
                    displaced_once = True
                    root.rename(displaced)
                    replacement.rename(root)
                real_checkpoint(label, *args)

            with mock.patch(
                "insider_publication._publication_checkpoint",
                side_effect=displace_before_return,
            ):
                with self.assertRaisesRegex(
                    InsiderPublicationError,
                    "repository root changed",
                ):
                    write_insider_publication(publication, repository_root=root)

            self.assertTrue(displaced_once)
            self.assertEqual(
                "UNTOUCHED\n",
                (root / "replacement-only.txt").read_text(),
            )
            self.assertFalse((root / "data").exists())
            detached_public = displaced / "data/insiders/public"
            self.assertEqual([], validate_insider_public_tree(detached_public))
            transaction_records = list(
                (displaced / "data/insiders").glob(".public.transaction-*.json")
            )
            self.assertEqual(1, len(transaction_records))

            write_insider_publication(publication, repository_root=displaced)
            self.assertEqual([], validate_insider_public_tree(detached_public))
            self.assertEqual(
                [],
                list((displaced / "data/insiders").glob(".public.transaction-*")),
            )

    def test_writer_is_atomic_idempotent_and_removes_only_stale_public_output(
        self,
    ) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            private = root / "data/insiders/private/sentinel.txt"
            private.parent.mkdir(parents=True)
            private.write_text("PRIVATE_SENTINEL")
            first = write_insider_publication(publication, repository_root=root)
            public_root = root / "data/insiders/public"
            security_path = public_root / "securities/03770N101.json"
            filing_path = public_root / "filings/0000000001-26-000001.json"
            manifest_path = public_root / "manifest.json"
            self.assertTrue(security_path.is_file())
            self.assertTrue(filing_path.is_file())
            self.assertTrue(manifest_path.is_file())
            first_files = {
                path.relative_to(public_root).as_posix(): path.read_bytes()
                for path in public_root.rglob("*")
                if path.is_file()
            }
            (public_root / "filings/stale.json").write_text("stale")
            second = write_insider_publication(publication, repository_root=root)
            second_files = {
                path.relative_to(public_root).as_posix(): path.read_bytes()
                for path in public_root.rglob("*")
                if path.is_file()
            }
            self.assertEqual(first, second)
            self.assertEqual(first_files, second_files)
            self.assertEqual("PRIVATE_SENTINEL", private.read_text())
            self.assertEqual(1, first["securityPayloadCount"])
            self.assertEqual(1, first["filingPayloadCount"])

    def test_writer_retains_unjournaled_legacy_backup(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            insiders_root = root / "data/insiders"
            public_root = insiders_root / "public"
            write_insider_publication(publication, repository_root=root)
            backup = insiders_root / ".public.backup"
            shutil.copytree(public_root, backup)
            before = {
                path.relative_to(backup).as_posix(): path.read_bytes()
                for path in backup.rglob("*")
                if path.is_file()
            }

            with self.assertRaisesRegex(
                InsiderPublicationError,
                "unknown publication backup",
            ):
                write_insider_publication(publication, repository_root=root)

            after = {
                path.relative_to(backup).as_posix(): path.read_bytes()
                for path in backup.rglob("*")
                if path.is_file()
            }
            self.assertEqual(before, after)
            self.assertEqual([], validate_insider_public_tree(public_root))

    def test_writer_retains_unjournaled_prepare_prefix_directory(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            insiders_root = root / "data/insiders"
            write_insider_publication(publication, repository_root=root)
            foreign = insiders_root / f".public.prepare-{'f' * 64}"
            foreign.mkdir()
            sentinel = foreign / "foreign-sentinel"
            sentinel.write_text("DO NOT DELETE\n")

            with self.assertRaisesRegex(
                InsiderPublicationError,
                "unknown publication staging directory",
            ):
                write_insider_publication(publication, repository_root=root)

            self.assertEqual("DO NOT DELETE\n", sentinel.read_text())

    def test_writer_retains_unjournaled_dynamic_backup(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            insiders_root = root / "data/insiders"
            public_root = insiders_root / "public"
            write_insider_publication(publication, repository_root=root)
            backup = insiders_root / f".public.backup-{'e' * 64}"
            shutil.copytree(public_root, backup)
            before = {
                path.relative_to(backup).as_posix(): path.read_bytes()
                for path in backup.rglob("*")
                if path.is_file()
            }

            with self.assertRaisesRegex(
                InsiderPublicationError,
                "unknown publication backup",
            ):
                write_insider_publication(publication, repository_root=root)

            after = {
                path.relative_to(backup).as_posix(): path.read_bytes()
                for path in backup.rglob("*")
                if path.is_file()
            }
            self.assertEqual(before, after)
            self.assertEqual([], validate_insider_public_tree(public_root))

    def test_prepared_transaction_recovery_restores_prior_generation(self) -> None:
        filing = parse_case("form4_simple_purchase")
        first_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        second_as_of = "2026-07-01T20:45:00Z"
        second_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=second_as_of,
            latest_successful_sync_at="2026-07-01T20:40:00Z",
        )

        for crash_point, token in (("after_backup", "a"), ("after_publish", "b")):
            with (
                self.subTest(crash_point=crash_point),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                base = Path(tmpdir)
                root = base / "repository"
                source = base / "second-generation"
                root.mkdir()
                source.mkdir()
                write_insider_publication(first_publication, repository_root=root)
                write_insider_publication(second_publication, repository_root=source)
                insiders_root = root / "data/insiders"
                public_root = insiders_root / "public"
                transaction_id = token * 64
                record_name, stage_name, backup_name = (
                    publication_module._publication_transaction_names(transaction_id)
                )
                stage_root = insiders_root / stage_name
                shutil.copytree(source / "data/insiders/public", stage_root)

                insiders_fd = publication_module._open_publication_directory(
                    insiders_root,
                    "insider data root",
                )
                stage_fd = publication_module._open_publication_directory_at(
                    insiders_fd,
                    stage_name,
                    "publication staging directory",
                )
                public_fd = publication_module._open_publication_directory_at(
                    insiders_fd,
                    "public",
                    "public output root",
                )
                record_fd: int | None = None
                try:
                    transaction = publication_module._PublicationTransaction(
                        transaction_id=transaction_id,
                        state="prepared",
                        stage=publication_module._publication_directory_identity(
                            stage_fd,
                            name=stage_name,
                            label="publication staging directory",
                        ),
                        backup=publication_module._publication_directory_identity(
                            public_fd,
                            name=backup_name,
                            label="public output root",
                        ),
                        tree_sha256=publication_module._publication_tree_sha256(
                            publication_module._expected_publication_snapshot(
                                second_publication
                            )
                        ),
                        stage_tree_seal_sha256=publication_module._publication_descendant_seal_sha256(
                            publication_module._read_validated_insider_public_snapshot_sealed_fd(
                                stage_fd
                            )[1]
                        ),
                        backup_tree_sha256=publication_module._publication_backup_tree_sha256(
                            public_fd
                        ),
                    )
                    record_fd = publication_module._create_publication_transaction_at(
                        insiders_fd,
                        transaction,
                    )
                finally:
                    if record_fd is not None:
                        os.close(record_fd)
                    os.close(public_fd)
                    os.close(stage_fd)
                    os.close(insiders_fd)

                public_root.rename(insiders_root / backup_name)
                if crash_point == "after_publish":
                    stage_root.rename(public_root)

                with mock.patch(
                    "insider_publication._write_insider_publication_unlocked",
                    return_value={"recovered": True},
                ) as normal_writer:
                    result = write_insider_publication(
                        first_publication,
                        repository_root=root,
                    )

                self.assertEqual({"recovered": True}, result)
                normal_writer.assert_called_once()
                self.assertEqual([], validate_insider_public_tree(public_root))
                manifest = json.loads((public_root / "manifest.json").read_text())
                self.assertEqual(AS_OF, manifest["asOf"])
                self.assertFalse((insiders_root / stage_name).exists())
                self.assertFalse((insiders_root / backup_name).exists())
                self.assertFalse((insiders_root / record_name).exists())

    def test_prepared_recovery_rejects_backup_changed_after_final_digest_check(
        self,
    ) -> None:
        filing = parse_case("form4_simple_purchase")
        first_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        second_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of="2026-07-01T20:45:00Z",
            latest_successful_sync_at="2026-07-01T20:40:00Z",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            root = base / "repository"
            source = base / "second-generation"
            root.mkdir()
            source.mkdir()
            write_insider_publication(first_publication, repository_root=root)
            write_insider_publication(second_publication, repository_root=source)
            insiders_root = root / "data/insiders"
            public_root = insiders_root / "public"
            transaction_id = "7" * 64
            record_name, stage_name, backup_name = (
                publication_module._publication_transaction_names(transaction_id)
            )
            stage_root = insiders_root / stage_name
            shutil.copytree(source / "data/insiders/public", stage_root)

            insiders_fd = publication_module._open_publication_directory(
                insiders_root,
                "insider data root",
            )
            stage_fd = publication_module._open_publication_directory_at(
                insiders_fd,
                stage_name,
                "publication staging directory",
            )
            public_fd = publication_module._open_publication_directory_at(
                insiders_fd,
                "public",
                "public output root",
            )
            record_fd: int | None = None
            try:
                transaction = publication_module._PublicationTransaction(
                    transaction_id=transaction_id,
                    state="prepared",
                    stage=publication_module._publication_directory_identity(
                        stage_fd,
                        name=stage_name,
                        label="publication staging directory",
                    ),
                    backup=publication_module._publication_directory_identity(
                        public_fd,
                        name=backup_name,
                        label="public output root",
                    ),
                    tree_sha256=publication_module._publication_tree_sha256(
                        publication_module._expected_publication_snapshot(
                            second_publication
                        )
                    ),
                    stage_tree_seal_sha256=publication_module._publication_descendant_seal_sha256(
                        publication_module._read_validated_insider_public_snapshot_sealed_fd(
                            stage_fd
                        )[1]
                    ),
                    backup_tree_sha256=publication_module._publication_backup_tree_sha256(
                        public_fd
                    ),
                )
                record_fd = publication_module._create_publication_transaction_at(
                    insiders_fd,
                    transaction,
                )
            finally:
                if record_fd is not None:
                    os.close(record_fd)
                os.close(public_fd)
                os.close(stage_fd)
                os.close(insiders_fd)

            public_root.rename(insiders_root / backup_name)
            real_replace = publication_module._replace_publication_directory_at
            injected = False

            def inject_before_restore(
                parent_fd: int,
                source_name: str,
                target_name: str,
                label: str,
                *args: object,
                **kwargs: object,
            ) -> None:
                nonlocal injected
                if label == "publication backup recovery" and not injected:
                    (
                        insiders_root / backup_name / "foreign-after-digest.json"
                    ).write_bytes(b"FOREIGN\n")
                    injected = True
                real_replace(
                    parent_fd,
                    source_name,
                    target_name,
                    label,
                    *args,
                    **kwargs,
                )

            with (
                mock.patch(
                    "insider_publication._replace_publication_directory_at",
                    side_effect=inject_before_restore,
                ),
                mock.patch(
                    "insider_publication._write_insider_publication_unlocked",
                    return_value={"unexpected": True},
                ) as normal_writer,
                self.assertRaisesRegex(
                    InsiderPublicationError,
                    "backup.*(?:digest|changed)",
                ),
            ):
                write_insider_publication(first_publication, repository_root=root)

            normal_writer.assert_not_called()
            self.assertTrue(injected)
            self.assertFalse(public_root.exists())
            backup_root = insiders_root / backup_name
            self.assertTrue(backup_root.is_dir())
            self.assertEqual(
                b"FOREIGN\n",
                (backup_root / "foreign-after-digest.json").read_bytes(),
            )
            self.assertTrue((insiders_root / stage_name).is_dir())
            self.assertTrue((insiders_root / record_name).is_file())

    def test_published_transaction_recovery_retains_new_generation(self) -> None:
        filing = parse_case("form4_simple_purchase")
        first_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        second_as_of = "2026-07-01T20:45:00Z"
        second_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=second_as_of,
            latest_successful_sync_at="2026-07-01T20:40:00Z",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(first_publication, repository_root=root)
            real_checkpoint = publication_module._publication_checkpoint

            def interrupt_after_publish(
                label: str,
                root_path: Path,
                root_fd: int,
                data_fd: int,
                insiders_fd: int,
            ) -> None:
                if label == "before_backup_cleanup":
                    raise RuntimeError("simulated process interruption")
                real_checkpoint(
                    label,
                    root_path,
                    root_fd,
                    data_fd,
                    insiders_fd,
                )

            with mock.patch(
                "insider_publication._publication_checkpoint",
                side_effect=interrupt_after_publish,
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "simulated process interruption",
                ):
                    write_insider_publication(
                        second_publication,
                        repository_root=root,
                    )

            insiders_root = root / "data/insiders"
            public_root = insiders_root / "public"
            manifest = json.loads((public_root / "manifest.json").read_text())
            self.assertEqual(second_as_of, manifest["asOf"])
            self.assertEqual(1, len(list(insiders_root.glob(".public.backup-*"))))
            self.assertEqual(
                1,
                len(list(insiders_root.glob(".public.transaction-*.json"))),
            )

            with mock.patch(
                "insider_publication._write_insider_publication_unlocked",
                return_value={"recovered": True},
            ) as normal_writer:
                result = write_insider_publication(
                    second_publication,
                    repository_root=root,
                )

            self.assertEqual({"recovered": True}, result)
            normal_writer.assert_called_once()
            self.assertEqual([], validate_insider_public_tree(public_root))
            manifest = json.loads((public_root / "manifest.json").read_text())
            self.assertEqual(second_as_of, manifest["asOf"])
            self.assertEqual([], list(insiders_root.glob(".public.backup-*")))
            self.assertEqual(
                [],
                list(insiders_root.glob(".public.transaction-*")),
            )

    def test_transaction_recovery_rejects_valid_descendant_replacement(self) -> None:
        filing = parse_case("form4_simple_purchase")
        first_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        second_as_of = "2026-07-01T20:45:00Z"
        second_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=second_as_of,
            latest_successful_sync_at="2026-07-01T20:40:00Z",
        )
        foreign_as_of = "2026-07-02T20:45:00Z"
        foreign_publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=foreign_as_of,
            latest_successful_sync_at="2026-07-02T20:40:00Z",
        )
        expected_tree_sha256 = publication_module._publication_tree_sha256(
            publication_module._expected_publication_snapshot(second_publication)
        )

        for state, token in (("prepared", "f"), ("published", "9")):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as tmpdir:
                base = Path(tmpdir)
                root = base / "repository"
                second_source = base / "second-generation"
                foreign_source = base / "foreign-generation"
                root.mkdir()
                second_source.mkdir()
                foreign_source.mkdir()
                write_insider_publication(first_publication, repository_root=root)
                write_insider_publication(
                    second_publication,
                    repository_root=second_source,
                )
                write_insider_publication(
                    foreign_publication,
                    repository_root=foreign_source,
                )

                insiders_root = root / "data/insiders"
                public_root = insiders_root / "public"
                transaction_id = token * 64
                record_name, stage_name, backup_name = (
                    publication_module._publication_transaction_names(transaction_id)
                )
                stage_root = insiders_root / stage_name
                shutil.copytree(
                    second_source / "data/insiders/public",
                    stage_root,
                )

                insiders_fd = publication_module._open_publication_directory(
                    insiders_root,
                    "insider data root",
                )
                stage_fd = publication_module._open_publication_directory_at(
                    insiders_fd,
                    stage_name,
                    "publication staging directory",
                )
                public_fd = publication_module._open_publication_directory_at(
                    insiders_fd,
                    "public",
                    "public output root",
                )
                record_fd: int | None = None
                try:
                    transaction = publication_module._PublicationTransaction(
                        transaction_id=transaction_id,
                        state=state,
                        stage=publication_module._publication_directory_identity(
                            stage_fd,
                            name=stage_name,
                            label="publication staging directory",
                        ),
                        backup=publication_module._publication_directory_identity(
                            public_fd,
                            name=backup_name,
                            label="public output root",
                        ),
                        tree_sha256=expected_tree_sha256,
                        stage_tree_seal_sha256=publication_module._publication_descendant_seal_sha256(
                            publication_module._read_validated_insider_public_snapshot_sealed_fd(
                                stage_fd
                            )[1]
                        ),
                        backup_tree_sha256=publication_module._publication_backup_tree_sha256(
                            public_fd
                        ),
                    )
                    record_fd = publication_module._create_publication_transaction_at(
                        insiders_fd,
                        transaction,
                    )
                finally:
                    if record_fd is not None:
                        os.close(record_fd)
                    os.close(public_fd)
                    os.close(stage_fd)
                    os.close(insiders_fd)

                public_root.rename(insiders_root / backup_name)
                if state == "published":
                    stage_root.rename(public_root)
                    active_root = public_root
                else:
                    active_root = stage_root

                for entry in list(active_root.iterdir()):
                    if entry.is_dir():
                        shutil.rmtree(entry)
                    else:
                        entry.unlink()
                for entry in (foreign_source / "data/insiders/public").iterdir():
                    target = active_root / entry.name
                    if entry.is_dir():
                        shutil.copytree(entry, target)
                    else:
                        shutil.copy2(entry, target)

                with mock.patch(
                    "insider_publication._write_insider_publication_unlocked",
                ) as normal_writer:
                    with self.assertRaisesRegex(
                        InsiderPublicationError,
                        "tree digest",
                    ):
                        write_insider_publication(
                            second_publication,
                            repository_root=root,
                        )

                normal_writer.assert_not_called()
                self.assertTrue((insiders_root / record_name).is_file())
                self.assertTrue((insiders_root / backup_name).is_dir())
                self.assertEqual(
                    AS_OF,
                    json.loads(
                        (insiders_root / backup_name / "manifest.json").read_text()
                    )["asOf"],
                )
                self.assertEqual(
                    foreign_as_of,
                    json.loads((active_root / "manifest.json").read_text())["asOf"],
                )
                if state == "prepared":
                    self.assertTrue(stage_root.is_dir())
                    self.assertFalse(public_root.exists())
                else:
                    self.assertFalse(stage_root.exists())
                    self.assertTrue(public_root.is_dir())

    def test_transaction_recovery_rejects_stage_identity_replacement(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(publication, repository_root=root)
            insiders_root = root / "data/insiders"
            public_root = insiders_root / "public"
            transaction_id = "c" * 64
            record_name, stage_name, backup_name = (
                publication_module._publication_transaction_names(transaction_id)
            )
            stage_root = insiders_root / stage_name
            shutil.copytree(public_root, stage_root)
            insiders_fd = publication_module._open_publication_directory(
                insiders_root,
                "insider data root",
            )
            stage_fd = publication_module._open_publication_directory_at(
                insiders_fd,
                stage_name,
                "publication staging directory",
            )
            public_fd = publication_module._open_publication_directory_at(
                insiders_fd,
                "public",
                "public output root",
            )
            record_fd: int | None = None
            try:
                transaction = publication_module._PublicationTransaction(
                    transaction_id=transaction_id,
                    state="prepared",
                    stage=publication_module._publication_directory_identity(
                        stage_fd,
                        name=stage_name,
                        label="publication staging directory",
                    ),
                    backup=publication_module._publication_directory_identity(
                        public_fd,
                        name=backup_name,
                        label="public output root",
                    ),
                    tree_sha256=publication_module._publication_tree_sha256(
                        publication_module._expected_publication_snapshot(publication)
                    ),
                    stage_tree_seal_sha256=publication_module._publication_descendant_seal_sha256(
                        publication_module._read_validated_insider_public_snapshot_sealed_fd(
                            stage_fd
                        )[1]
                    ),
                    backup_tree_sha256=publication_module._publication_backup_tree_sha256(
                        public_fd
                    ),
                )
                record_fd = publication_module._create_publication_transaction_at(
                    insiders_fd,
                    transaction,
                )
            finally:
                if record_fd is not None:
                    os.close(record_fd)
                os.close(public_fd)
                os.close(stage_fd)
                os.close(insiders_fd)

            detached = insiders_root / "detached-original-stage"
            stage_root.rename(detached)
            shutil.copytree(public_root, stage_root)
            replacement_sentinel = stage_root / "replacement-sentinel"
            replacement_sentinel.write_text("FOREIGN\n")

            with mock.patch(
                "insider_publication._write_insider_publication_unlocked",
            ) as normal_writer:
                with self.assertRaisesRegex(
                    InsiderPublicationError,
                    "identity mismatch",
                ):
                    write_insider_publication(publication, repository_root=root)

            normal_writer.assert_not_called()
            self.assertEqual("FOREIGN\n", replacement_sentinel.read_text())
            self.assertTrue(detached.is_dir())
            self.assertTrue((insiders_root / record_name).is_file())
            self.assertEqual([], validate_insider_public_tree(public_root))

    def test_malformed_transaction_record_and_stage_are_retained(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(publication, repository_root=root)
            insiders_root = root / "data/insiders"
            transaction_id = "d" * 64
            record_name, stage_name, _ = (
                publication_module._publication_transaction_names(transaction_id)
            )
            stage_root = insiders_root / stage_name
            stage_root.mkdir()
            sentinel = stage_root / "sentinel"
            sentinel.write_text("RETAIN\n")
            record = insiders_root / record_name
            record.write_bytes(b'{"kind":"forged","kind":"duplicate"}\n')
            record.chmod(0o600)

            with self.assertRaisesRegex(
                InsiderPublicationError,
                "publication transaction record",
            ):
                write_insider_publication(publication, repository_root=root)

            self.assertEqual("RETAIN\n", sentinel.read_text())
            self.assertEqual(
                b'{"kind":"forged","kind":"duplicate"}\n',
                record.read_bytes(),
            )

    def test_stage_creation_failure_preserves_foreign_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            parent_fd = publication_module._open_publication_directory(
                parent,
                "insider data root",
            )
            transaction_id = "e" * 64
            _, stage_name, _ = publication_module._publication_transaction_names(
                transaction_id
            )
            detached_name = "detached-owned-stage"
            real_fsync = publication_module._fsync_publication_directory
            replaced = False

            def replace_before_fsync(directory_fd: int, label: str) -> None:
                nonlocal replaced
                if label == "publication staging directory" and not replaced:
                    replaced = True
                    os.rename(
                        stage_name,
                        detached_name,
                        src_dir_fd=parent_fd,
                        dst_dir_fd=parent_fd,
                    )
                    os.mkdir(stage_name, mode=0o700, dir_fd=parent_fd)
                    raise OSError("simulated staging fsync failure")
                real_fsync(directory_fd, label)

            try:
                with (
                    mock.patch(
                        "insider_publication.secrets.token_hex",
                        return_value=transaction_id,
                    ),
                    mock.patch(
                        "insider_publication._fsync_publication_directory",
                        side_effect=replace_before_fsync,
                    ),
                    self.assertRaisesRegex(
                        OSError,
                        "simulated staging fsync failure",
                    ),
                ):
                    publication_module._create_publication_stage_at(parent_fd)
            finally:
                os.close(parent_fd)

            self.assertTrue(replaced)
            self.assertTrue((parent / stage_name).is_dir())
            self.assertTrue((parent / detached_name).is_dir())

    def test_publication_corpus_and_global_size_limits_fail_closed(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with self.assertRaisesRegex(
            InsiderPublicationError,
            "publication corpus size",
        ):
            combine_insider_publications(
                itertools.repeat(publication, MAX_PUBLIC_ISSUERS + 1)
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with mock.patch("insider_publication.MAX_PUBLIC_SECURITY_FILES", 0):
                with self.assertRaisesRegex(
                    InsiderPublicationError,
                    "publication file count",
                ):
                    write_insider_publication(publication, repository_root=root)
            with mock.patch("insider_publication.MAX_PUBLIC_TOTAL_BYTES", 1):
                with self.assertRaisesRegex(
                    InsiderPublicationError,
                    "publication byte limit",
                ):
                    write_insider_publication(publication, repository_root=root)

    def test_writer_validates_staging_before_replacing_a_good_publication(self) -> None:
        filing = parse_case("form4_simple_purchase")
        publication = build_insider_publication(
            [filing],
            issuer_state=issuer_state([filing]),
            security_mappings=security_mapping(filing),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(publication, repository_root=root)
            public_root = root / "data/insiders/public"
            before = {
                path.relative_to(public_root).as_posix(): path.read_bytes()
                for path in public_root.rglob("*")
                if path.is_file()
            }
            page = publication.security_payloads[stock_file_stem(STOCK_ID)]
            page["ownerDirectory"] = [
                {"name": "Synthetic Owner", "address": "PRIVATE STREET"}
            ]

            with self.assertRaisesRegex(
                InsiderPublicationError,
                "staged public tree",
            ):
                write_insider_publication(publication, repository_root=root)

            after = {
                path.relative_to(public_root).as_posix(): path.read_bytes()
                for path in public_root.rglob("*")
                if path.is_file()
            }
            self.assertEqual(before, after)

    def test_corpus_combines_multiple_issuers_deterministically(self) -> None:
        issuer_one = parse_case("form4_simple_purchase")
        issuer_five = parse_case("form3_holdings_only")
        first = build_insider_publication(
            [issuer_one],
            issuer_state=issuer_state([issuer_one]),
            security_mappings=security_mapping(issuer_one),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        second = build_insider_publication(
            [issuer_five],
            issuer_state=issuer_state([issuer_five]),
            security_mappings=security_mapping(
                issuer_five,
                stock_id="594918104",
            ),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )

        corpus = combine_insider_publications([first, second])
        reversed_corpus = combine_insider_publications([second, first])

        self.assertEqual(corpus, reversed_corpus)
        self.assertEqual(
            ["0000000001", "0000000005"],
            corpus.manifest["issuerCiks"],
        )
        self.assertNotIn("issuerCik", corpus.manifest)
        self.assertEqual(2, len(corpus.security_payloads))
        self.assertEqual(2, len(corpus.filing_payloads))
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_insider_publication(corpus, repository_root=root)
            self.assertEqual(
                [],
                validate_insider_public_tree(root / "data/insiders/public"),
            )

        collision = build_insider_publication(
            [issuer_five],
            issuer_state=issuer_state([issuer_five]),
            security_mappings=security_mapping(issuer_five),
            as_of=AS_OF,
            latest_successful_sync_at=SYNC_AT,
        )
        with self.assertRaisesRegex(
            InsiderPublicationError,
            "security file collision",
        ):
            combine_insider_publications([first, collision])

    def test_bad_state_mapping_and_unbounded_text_fail_closed(self) -> None:
        filing = parse_case("form4_simple_purchase")
        state = issuer_state([filing])
        mappings = security_mapping(filing)
        bad_mapping = dict(mappings)
        only_key = next(iter(bad_mapping))
        bad_mapping["f" * 64] = bad_mapping.pop(only_key)
        with self.assertRaisesRegex(InsiderPublicationError, "mapping"):
            build_insider_publication(
                [filing],
                issuer_state=state,
                security_mappings=bad_mapping,
                as_of=AS_OF,
                latest_successful_sync_at=SYNC_AT,
            )

        oversized = json.loads(json.dumps(filing))
        oversized["remarks"] = "x" * 100_001
        with self.assertRaises(InsiderPublicationError):
            build_insider_publication(
                [oversized],
                issuer_state=state,
                security_mappings=mappings,
                as_of=AS_OF,
                latest_successful_sync_at=SYNC_AT,
            )


if __name__ == "__main__":
    unittest.main()
