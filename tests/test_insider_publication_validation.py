from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import validate_data
from insider_parser import INSIDER_PARSER_VERSION, parse_ownership_xml
from insider_pipeline import issuer_record_from_normalized, reduce_issuer_state
from insider_publication import (
    build_insider_publication,
    canonical_public_json_bytes,
    write_insider_publication,
)
from security_identity import stock_file_stem


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "insider_filings"
ORACLE = json.loads((FIXTURE_ROOT / "expectations.json").read_text())
STOCK_ID = "03770N101"


def parsed(name: str) -> dict[str, object]:
    case = ORACLE["filings"][name]
    return parse_ownership_xml(
        (FIXTURE_ROOT / case["filename"]).read_bytes(),
        accession_number=case["accession_number"],
        filing_date=case["filing_date"],
        accepted_at=case["accepted_at"],
        source_index_url=case["source_index_url"],
        source_document_url=case["source_document_url"],
    )


def publication_fixture():
    filings = [parsed("form4_simple_purchase"), parsed("form4_joint_sale_derivative")]
    records = [
        issuer_record_from_normalized(
            filing,
            parser_version=INSIDER_PARSER_VERSION,
        )
        for filing in filings
    ]
    state = reduce_issuer_state(
        issuer_cik="0000000001",
        records=records,
    ).issuer_state
    class_keys = {
        row["security_class_key"]
        for filing in filings
        for collection in ("transactions", "holdings")
        for row in filing[collection]
        if row["source_table"] == "non_derivative"
    }
    mappings = {
        key: {
            "stockId": STOCK_ID,
            "fileStem": stock_file_stem(STOCK_ID),
            "ticker": "TST",
            "companyName": "Synthetic Test Issuer",
            "securityType": "Common Stock",
            "securityTypeLabel": "COMMON STOCK",
            "cusip": STOCK_ID,
            "primary": True,
        }
        for key in class_keys
    }
    return build_insider_publication(
        filings,
        issuer_state=state,
        security_mappings=mappings,
        as_of="2026-06-30T20:45:00Z",
        latest_successful_sync_at="2026-06-30T20:40:00Z",
    )


def holding_publication_fixture():
    filing = parsed("form3_holdings_only")
    issuer = filing["issuer"]
    assert isinstance(issuer, dict)
    issuer_cik = issuer["cik"]
    assert isinstance(issuer_cik, str)
    state = reduce_issuer_state(
        issuer_cik=issuer_cik,
        records=[
            issuer_record_from_normalized(
                filing,
                parser_version=INSIDER_PARSER_VERSION,
            )
        ],
    ).issuer_state
    holding_rows = filing["holdings"]
    assert isinstance(holding_rows, list)
    class_keys = {
        row["security_class_key"]
        for row in holding_rows
        if row["source_table"] == "non_derivative"
    }
    mappings = {
        key: {
            "stockId": STOCK_ID,
            "fileStem": stock_file_stem(STOCK_ID),
            "ticker": "TST",
            "companyName": "Synthetic Test Issuer",
            "securityType": "Common Stock",
            "securityTypeLabel": "COMMON STOCK",
            "cusip": STOCK_ID,
            "primary": True,
        }
        for key in class_keys
    }
    return build_insider_publication(
        [filing],
        issuer_state=state,
        security_mappings=mappings,
        as_of="2026-06-30T20:45:00Z",
        latest_successful_sync_at="2026-06-30T20:40:00Z",
    )


def untrusted_public_json_bytes(payload: object) -> bytes:
    """Serialize attacker-controlled JSON without publisher validation."""

    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def update_manifest_entry(public_root: Path, relative: str) -> None:
    path = public_root / relative
    payload = path.read_bytes()
    manifest_path = public_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    security_entries = manifest["securityPayloads"]
    if relative.startswith("securities/"):
        entry = next(item for item in security_entries if item["path"] == relative)
        entry["bytes"] = len(payload)
        entry["sha256"] = hashlib.sha256(payload).hexdigest()
    elif relative.startswith("filings/"):
        matched = False
        for security_entry in security_entries:
            security_path = public_root / security_entry["path"]
            page = json.loads(security_path.read_text())
            for filing_ref in page["filingRefs"]:
                if filing_ref["path"] != relative:
                    continue
                filing_ref["bytes"] = len(payload)
                filing_ref["sha256"] = hashlib.sha256(payload).hexdigest()
                matched = True
            encoded_page = canonical_public_json_bytes(page)
            security_path.write_bytes(encoded_page)
            security_entry["bytes"] = len(encoded_page)
            security_entry["sha256"] = hashlib.sha256(encoded_page).hexdigest()
        if not matched:
            raise AssertionError(f"unreferenced filing payload: {relative}")
    else:
        raise AssertionError(f"unsupported public payload: {relative}")
    manifest_path.write_bytes(canonical_public_json_bytes(manifest))


class InsiderPublicTreeValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.publication = publication_fixture()

    def write_tree(self, root: Path) -> Path:
        write_insider_publication(copy.deepcopy(self.publication), repository_root=root)
        return root / "data/insiders/public"

    def validate(self, public_root: Path) -> list[str]:
        errors: list[str] = []
        validate_data.validate_insider_public_data(public_root, errors)
        return errors

    def test_generated_tree_reconciles_to_filing_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            self.assertEqual([], self.validate(public_root))

    def test_summary_tampering_is_detected_after_manifest_is_resealed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            relative = "securities/03770N101.json"
            page_path = public_root / relative
            page = json.loads(page_path.read_text())
            page["summary"]["purchases"]["value"] = "999999"
            page_path.write_bytes(canonical_public_json_bytes(page))
            update_manifest_entry(public_root, relative)

            errors = self.validate(public_root)
            self.assertTrue(any("reconcile" in error for error in errors), errors)

    def test_resealed_page_items_must_derive_from_filing_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            relative = "securities/03770N101.json"
            page_path = public_root / relative
            page = json.loads(page_path.read_text())
            for item in page["transactions"]["items"]:
                item["transactionLabel"] = "TAMPERED PUBLIC LABEL"
            latest = page["summary"]["latestMeaningfulTransaction"]
            if latest is not None:
                latest["transactionLabel"] = "TAMPERED PUBLIC LABEL"
            page_path.write_bytes(canonical_public_json_bytes(page))
            update_manifest_entry(public_root, relative)

            errors = self.validate(public_root)
            self.assertTrue(
                any(
                    "filing detail" in error or "detail rows" in error
                    for error in errors
                ),
                errors,
            )

    def test_security_page_nested_objects_are_strictly_allowlisted(self) -> None:
        mutations = {
            "security owner address": lambda page: page["security"].__setitem__(
                "ownerAddress",
                "PRIVATE STREET",
            ),
            "methodology owner address": lambda page: page[
                "methodologyBanner"
            ].__setitem__(
                "ownerAddress",
                "PRIVATE STREET",
            ),
            "filters private field": lambda page: page["filters"].__setitem__(
                "ownerAddress",
                "PRIVATE STREET",
            ),
            "filters benign-key personal text": lambda page: page[
                "filters"
            ].__setitem__(
                "freeform_private_note",
                "100 Main Street; 555-123-4567; /private/owner/path",
            ),
            "freshness private field": lambda page: page["dataFreshness"].__setitem__(
                "sourcePath",
                "/private/var/insiders.json",
            ),
            "quality private field": lambda page: page["dataQuality"].__setitem__(
                "parserVersion",
                "private-parser-v1",
            ),
            "purchase summary private field": lambda page: page["summary"][
                "purchases"
            ].__setitem__(
                "ownerAddress",
                "PRIVATE STREET",
            ),
            "net summary private field": lambda page: page["summary"][
                "netPS"
            ].__setitem__(
                "sourcePath",
                "/private/var/insiders.json",
            ),
            "sidebar private field": lambda page: page["sidebar"].__setitem__(
                "ownerDirectory",
                ["PRIVATE OWNER"],
            ),
            "ranking private field": lambda page: page["sidebar"]["topBuyers"][
                0
            ].__setitem__(
                "ownerGroupKey",
                "f" * 64,
            ),
            "holdings private field": lambda page: page["sidebar"][
                "latestReportedHoldings"
            ].__setitem__(
                "sourcePath",
                "/private/var/insiders.json",
            ),
            "rule private field": lambda page: page["sidebar"]["rule10b51"].__setitem__(
                "remarks",
                "PRIVATE NARRATIVE",
            ),
            "local path in approved company field": lambda page: page[
                "security"
            ].__setitem__(
                "companyName",
                "/private/var/insiders.json",
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmpdir:
                public_root = self.write_tree(Path(tmpdir))
                relative = "securities/03770N101.json"
                page_path = public_root / relative
                page = json.loads(page_path.read_text())
                mutate(page)
                page_path.write_bytes(untrusted_public_json_bytes(page))
                update_manifest_entry(public_root, relative)

                errors = self.validate(public_root)
                self.assertTrue(errors, label)

    def test_identity_derived_public_fields_remain_strict_without_owner_ids(
        self,
    ) -> None:
        mutations = {
            "ranking rank": lambda page: page["sidebar"]["topBuyers"][0].__setitem__(
                "rank", 2
            ),
            "ranking invented owner": lambda page: page["sidebar"]["topSellers"][
                0
            ].__setitem__("displayName", "INVENTED REPORTING OWNER"),
            "ranking display value": lambda page: page["sidebar"]["topBuyers"][
                0
            ].__setitem__("displayValue", "$999B"),
            "ranking omitted despite transactions": lambda page: page[
                "sidebar"
            ].__setitem__("topBuyers", []),
            "rule distinct owner count": lambda page: page["sidebar"][
                "rule10b51"
            ].__setitem__("distinctOwnerGroupCount", 999),
            "rule marked value": lambda page: page["sidebar"]["rule10b51"].__setitem__(
                "planMarkedSalesValue", "999"
            ),
            "summary owner count": lambda page: page["summary"][
                "purchases"
            ].__setitem__("ownerGroupCount", 999),
            "summary owner count zero despite transactions": lambda page: page[
                "summary"
            ]["purchases"].__setitem__("ownerGroupCount", 0),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmpdir:
                public_root = self.write_tree(Path(tmpdir))
                relative = "securities/03770N101.json"
                page_path = public_root / relative
                page = json.loads(page_path.read_text())
                mutate(page)
                page_path.write_bytes(untrusted_public_json_bytes(page))
                update_manifest_entry(public_root, relative)

                errors = self.validate(public_root)
                self.assertTrue(errors, label)
                self.assertTrue(
                    any(
                        token in error
                        for error in errors
                        for token in ("owner count", "reconcile", "sidebar")
                    ),
                    errors,
                )

    def test_holding_sidebar_fields_have_closed_public_semantics(self) -> None:
        mutations = {
            "holding date": lambda item: item.__setitem__("asOfDate", "not-a-date"),
            "holding shares": lambda item: item.__setitem__("shares", "01.0"),
            "holding role": lambda item: item.__setitem__("roles", ["Other"]),
            "holding percentage": lambda item: item.__setitem__(
                "ownershipPercentage",
                "0.42",
            ),
        }
        publication = holding_publication_fixture()
        for label, mutate in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                write_insider_publication(
                    copy.deepcopy(publication), repository_root=root
                )
                public_root = root / "data/insiders/public"
                relative = "securities/03770N101.json"
                page_path = public_root / relative
                page = json.loads(page_path.read_text())
                groups = page["sidebar"]["latestReportedHoldings"]
                item = next(
                    candidate
                    for category in (
                        "officersAndDirectors",
                        "tenPercentOwnersAndEntities",
                    )
                    for candidate in groups[category]
                )
                mutate(item)
                page_path.write_bytes(untrusted_public_json_bytes(page))
                update_manifest_entry(public_root, relative)

                errors = self.validate(public_root)
                self.assertTrue(
                    any("latestReportedHoldings" in error for error in errors),
                    errors,
                )

    def test_resealed_detail_and_page_cannot_smuggle_text_through_transaction_label(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            accession = "0000000001-26-000001"
            detail_relative = f"filings/{accession}.json"
            detail_path = public_root / detail_relative
            detail = json.loads(detail_path.read_text())
            smuggled = "Purchase at 100 Main Street; phone 555-123-4567"
            detail["transactions"][0]["transactionLabel"] = smuggled
            detail_path.write_bytes(untrusted_public_json_bytes(detail))
            update_manifest_entry(public_root, detail_relative)

            page_relative = "securities/03770N101.json"
            page_path = public_root / page_relative
            page = json.loads(page_path.read_text())
            matching_items = [
                item
                for item in page["transactions"]["items"]
                if item["accessionNumber"] == accession
            ]
            self.assertEqual(1, len(matching_items))
            matching_items[0]["transactionLabel"] = smuggled
            page_path.write_bytes(untrusted_public_json_bytes(page))
            update_manifest_entry(public_root, page_relative)

            errors = self.validate(public_root)
            self.assertTrue(
                any("transaction classification" in error for error in errors),
                errors,
            )

    def test_approved_issuer_name_field_rejects_contact_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            relative = "filings/0000000001-26-000001.json"
            detail_path = public_root / relative
            detail = json.loads(detail_path.read_text())
            detail["issuer"]["nameAsFiled"] = "Synthetic Test Issuer 555-123-4567"
            detail_path.write_bytes(untrusted_public_json_bytes(detail))
            update_manifest_entry(public_root, relative)

            errors = self.validate(public_root)
            self.assertTrue(
                any("issuer nameAsFiled" in error for error in errors), errors
            )

    def test_private_field_and_unsafe_source_url_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            relative = "filings/0000000001-26-000001.json"
            detail_path = public_root / relative
            detail = json.loads(detail_path.read_text())
            detail["owners"][0]["restricted_address"] = {
                "street1": "PRIVATE SENTINEL STREET"
            }
            detail["source"]["documentUrl"] = "https://evil.example/ownership.xml"
            detail_path.write_text(json.dumps(detail, sort_keys=True) + "\n")
            update_manifest_entry(public_root, relative)

            errors = self.validate(public_root)
            self.assertTrue(any("forbidden" in error for error in errors), errors)
            self.assertTrue(
                any("SEC" in error or "source" in error for error in errors), errors
            )

    def test_supporting_detail_objects_are_strictly_allowlisted(self) -> None:
        mutations = {
            "source": lambda detail: detail["source"].__setitem__(
                "ownerAddress",
                "PRIVATE STREET",
            ),
            "issuer": lambda detail: detail["issuer"].__setitem__(
                "ownerAddress",
                "PRIVATE STREET",
            ),
            "lineage": lambda detail: detail.__setitem__(
                "lineage",
                {"ownerAddress": "PRIVATE STREET"},
            ),
            "amendment history": lambda detail: detail.__setitem__(
                "amendmentHistory",
                [{"ownerAddress": "PRIVATE STREET"}],
            ),
            "footnote link": lambda detail: detail.__setitem__(
                "fieldFootnoteLinks",
                [{"ownerAddress": "PRIVATE STREET"}],
            ),
            "transaction field footnotes": lambda detail: detail["transactions"][
                0
            ].__setitem__(
                "fieldFootnotes",
                {"owner_address": ["PRIVATE-STREET"]},
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmpdir:
                public_root = self.write_tree(Path(tmpdir))
                relative = "filings/0000000001-26-000001.json"
                detail_path = public_root / relative
                detail = json.loads(detail_path.read_text())
                mutate(detail)
                detail_path.write_bytes(untrusted_public_json_bytes(detail))
                update_manifest_entry(public_root, relative)

                errors = self.validate(public_root)
                self.assertTrue(errors, label)
                self.assertTrue(
                    any(
                        token in error
                        for error in errors
                        for token in (
                            "amendment",
                            "fields",
                            "footnote",
                            "issuer",
                            "lineage",
                            "public fields",
                            "source",
                        )
                    ),
                    errors,
                )

    def test_owner_identifiers_and_narrative_personal_fields_are_rejected(self) -> None:
        mutations = {
            "owner company-title smuggling": lambda detail: detail["owners"][
                0
            ].__setitem__(
                "companyTitle",
                "JOHN Q PUBLIC",
            ),
            "owner CIK": lambda detail: detail["owners"][0].__setitem__(
                "cik",
                "0000000002",
            ),
            "owner other text": lambda detail: detail["owners"][0].__setitem__(
                "otherText",
                "PRIVATE RELATIONSHIP NARRATIVE",
            ),
            "filing remarks": lambda detail: detail["filing"].__setitem__(
                "remarks",
                "PRIVATE FILING NARRATIVE",
            ),
            "transaction ownership narrative": lambda detail: detail["transactions"][
                0
            ].__setitem__(
                "natureOfOwnership",
                "PRIVATE FAMILY TRUST NARRATIVE",
            ),
            "footnote text": lambda detail: detail.__setitem__(
                "footnotes",
                [{"id": "F1", "text": "PRIVATE FOOTNOTE NARRATIVE"}],
            ),
            "signature": lambda detail: detail.__setitem__(
                "signatures",
                [{"name": "/s/ PRIVATE SIGNER", "date": "2026-06-01"}],
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmpdir:
                public_root = self.write_tree(Path(tmpdir))
                relative = "filings/0000000001-26-000001.json"
                detail_path = public_root / relative
                detail = json.loads(detail_path.read_text())
                mutate(detail)
                detail_path.write_bytes(untrusted_public_json_bytes(detail))
                update_manifest_entry(public_root, relative)

                errors = self.validate(public_root)
                self.assertTrue(errors, label)
                self.assertTrue(
                    any(
                        token in error
                        for error in errors
                        for token in ("forbidden", "owner", "filing", "public fields")
                    ),
                    errors,
                )

    def test_reporting_owner_cik_token_in_allowed_name_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            relative = "filings/0000000001-26-000001.json"
            detail_path = public_root / relative
            detail = json.loads(detail_path.read_text())
            detail["owners"][0]["nameAsFiled"] = "SYNTHETIC OWNER 0000000002"
            detail_path.write_bytes(untrusted_public_json_bytes(detail))
            update_manifest_entry(public_root, relative)

            errors = self.validate(public_root)
            self.assertTrue(
                any("owner 0 name" in error for error in errors),
                errors,
            )

    def test_reporting_owner_address_in_allowed_name_is_rejected(self) -> None:
        for unsafe_name in (
            "123 Main St",
            "45 Oak Rd",
            "12 First Ave",
            "PO Box 99",
            "P.O. Box 99",
            "123 Main Street, Unit 4",
            "123 Main Street, Suite 5",
            "SYNTHETIC OWNER 90210",
            "SYNTHETIC OWNER K1A 0B1",
            "URL: example.test",
            "URI: example.test",
            "Website: example.test",
            "Web: example.test",
            "Homepage: example.test",
            "Site: example.test",
            "Contact: example.test",
            "URL example.test",
            "Website example.test",
            "Web Site: example.test",
            "Home Page: example.test",
            "Internet: example.test",
            "URL - example.test",
            "Contact = example.test",
            "example.test",
            "example.com/path",
            "ftp://example.test",
            "ftp:example.test",
            "sftp://example.test",
            "192.0.2.1",
            "[2001:db8::1]",
            "Box 12",
            "Postal Box 12",
            "Mailbox 12",
            "Drawer 9",
            "Lock Box 12",
            "Post Box 12",
            "P.O.B. 12",
            "POB 12",
            "Rural Route 2 Box 5",
            "RR 2 Box 5",
            "HC 3 Box 10",
            "Route 2 Box 5",
            "Rte 2 Box 5",
            "General Delivery",
            "Private Bag 4",
            "Locked Bag 3",
            "Poste Restante",
            "C/O SYNTHETIC OWNER",
            "CARE-OF SYNTHETIC OWNER",
            "CARE.OF SYNTHETIC OWNER",
            "C O SYNTHETIC OWNER",
            "C.O. SYNTHETIC OWNER",
            "ATTN: SYNTHETIC OWNER",
            "ATTN SYNTHETIC OWNER",
            "ATTENTION SYNTHETIC OWNER",
        ):
            with (
                self.subTest(unsafe_name=unsafe_name),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                public_root = self.write_tree(Path(tmpdir))
                relative = "filings/0000000001-26-000001.json"
                detail_path = public_root / relative
                detail = json.loads(detail_path.read_text())
                detail["owners"][0]["nameAsFiled"] = unsafe_name
                detail_path.write_bytes(untrusted_public_json_bytes(detail))
                update_manifest_entry(public_root, relative)

                errors = self.validate(public_root)
                self.assertTrue(
                    any("owner 0 name" in error for error in errors),
                    errors,
                )

    def test_address_text_in_group_and_sidebar_names_is_rejected(self) -> None:
        mutations = {
            "filing owner group": (
                "filings/0000000001-26-000001.json",
                lambda payload: payload["ownerGroup"].__setitem__(
                    "displayName", "123 Main St"
                ),
            ),
            "filing transaction owner group": (
                "filings/0000000001-26-000001.json",
                lambda payload: payload["transactions"][0]["ownerGroup"].__setitem__(
                    "displayName", "45 Oak Rd"
                ),
            ),
            "security transaction owner group": (
                "securities/03770N101.json",
                lambda payload: payload["transactions"]["items"][0][
                    "ownerGroup"
                ].__setitem__("displayName", "12 First Ave"),
            ),
            "sidebar ranking": (
                "securities/03770N101.json",
                lambda payload: payload["sidebar"]["topBuyers"][0].__setitem__(
                    "displayName", "PO Box 99"
                ),
            ),
            "filing owner group URL label": (
                "filings/0000000001-26-000001.json",
                lambda payload: payload["ownerGroup"].__setitem__(
                    "displayName", "URL: example.test"
                ),
            ),
            "filing transaction website label": (
                "filings/0000000001-26-000001.json",
                lambda payload: payload["transactions"][0]["ownerGroup"].__setitem__(
                    "displayName", "Website: example.test"
                ),
            ),
            "security transaction contact label": (
                "securities/03770N101.json",
                lambda payload: payload["transactions"]["items"][0][
                    "ownerGroup"
                ].__setitem__("displayName", "Contact: example.test"),
            ),
            "sidebar rural route box": (
                "securities/03770N101.json",
                lambda payload: payload["sidebar"]["topBuyers"][0].__setitem__(
                    "displayName", "Rural Route 2 Box 5"
                ),
            ),
            "filing owner group bare domain": (
                "filings/0000000001-26-000001.json",
                lambda payload: payload["ownerGroup"].__setitem__(
                    "displayName", "example.test"
                ),
            ),
            "filing transaction non-http URL": (
                "filings/0000000001-26-000001.json",
                lambda payload: payload["transactions"][0]["ownerGroup"].__setitem__(
                    "displayName", "ftp://example.test"
                ),
            ),
            "security transaction care-of variant": (
                "securities/03770N101.json",
                lambda payload: payload["transactions"]["items"][0][
                    "ownerGroup"
                ].__setitem__("displayName", "CARE-OF SYNTHETIC OWNER"),
            ),
            "chart care-of abbreviation": (
                "securities/03770N101.json",
                lambda payload: payload["chartEvents"][0].__setitem__(
                    "ownerGroupDisplayName", "C.O. SYNTHETIC OWNER"
                ),
            ),
            "latest transaction attention label": (
                "securities/03770N101.json",
                lambda payload: payload["summary"]["latestMeaningfulTransaction"][
                    "ownerGroup"
                ].__setitem__("displayName", "ATTN SYNTHETIC OWNER"),
            ),
            "sidebar attention label": (
                "securities/03770N101.json",
                lambda payload: payload["sidebar"]["topBuyers"][0].__setitem__(
                    "displayName", "ATTENTION SYNTHETIC OWNER"
                ),
            ),
        }
        for label, (relative, mutate) in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmpdir:
                public_root = self.write_tree(Path(tmpdir))
                payload_path = public_root / relative
                payload = json.loads(payload_path.read_text())
                mutate(payload)
                payload_path.write_bytes(untrusted_public_json_bytes(payload))
                update_manifest_entry(public_root, relative)

                errors = self.validate(public_root)
                self.assertTrue(errors, label)
                self.assertTrue(
                    any("name" in error for error in errors),
                    errors,
                )

    def test_detail_allowed_fields_have_closed_semantics(self) -> None:
        mutations = {
            "non-UTC accepted timestamp": (
                "0000000001-26-000001",
                lambda detail: detail["filing"].__setitem__(
                    "acceptedAt", "2026-01-16T16:30:00-05:00"
                ),
            ),
            "inconsistent amendment form": (
                "0000000001-26-000001",
                lambda detail: detail["filing"].__setitem__("formType", "4/A"),
            ),
            "issuer symbol narrative": (
                "0000000001-26-000001",
                lambda detail: detail["issuer"].__setitem__(
                    "tradingSymbolAsFiled", "PRIVATE OWNER NARRATIVE"
                ),
            ),
            "owner name contact text": (
                "0000000001-26-000001",
                lambda detail: detail["owners"][0].__setitem__(
                    "nameAsFiled", "SYNTHETIC OWNER 555-123-4567"
                ),
            ),
            "invalid transaction date": (
                "0000000001-26-000001",
                lambda detail: detail["transactions"][0].__setitem__(
                    "transactionDate", "2026-99-99"
                ),
            ),
            "unbounded transaction plan state": (
                "0000000001-26-000001",
                lambda detail: detail["transactions"][0].__setitem__(
                    "planStatus", "PRIVATE PLAN NARRATIVE"
                ),
            ),
            "inconsistent transaction value method": (
                "0000000001-26-000001",
                lambda detail: detail["transactions"][0].__setitem__(
                    "valueMethod", "reported_total"
                ),
            ),
            "invalid holding date": (
                "0000000001-26-000002",
                lambda detail: detail["holdings"][0].__setitem__(
                    "asOfDate", "not-a-date"
                ),
            ),
        }
        for label, (accession, mutate) in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmpdir:
                public_root = self.write_tree(Path(tmpdir))
                relative = f"filings/{accession}.json"
                detail_path = public_root / relative
                detail = json.loads(detail_path.read_text())
                mutate(detail)
                detail_path.write_bytes(untrusted_public_json_bytes(detail))
                update_manifest_entry(public_root, relative)

                self.assertTrue(self.validate(public_root), label)

    def test_missing_detail_and_unexpected_file_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            (public_root / "filings/0000000001-26-000001.json").unlink()
            (public_root / "bulk-private-export.json").write_text("{}\n")

            errors = self.validate(public_root)
            self.assertTrue(any("missing" in error for error in errors), errors)
            self.assertTrue(any("unexpected" in error for error in errors), errors)

    def test_dangling_public_root_symlink_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            public_root = root / "data/insiders/public"
            public_root.parent.mkdir(parents=True)
            public_root.symlink_to(
                root / "missing-public-tree", target_is_directory=True
            )

            errors = self.validate(public_root)
            self.assertTrue(errors)
            self.assertTrue(any("opened safely" in error for error in errors), errors)

    def test_missing_optional_public_tree_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = Path(tmpdir) / "data/insiders/public"
            self.assertEqual([], self.validate(public_root))

    def test_detail_decimals_must_use_canonical_exact_serialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            relative = "filings/0000000001-26-000001.json"
            detail_path = public_root / relative
            detail = json.loads(detail_path.read_text())
            self.assertIsInstance(detail["transactions"][0]["shares"], str)
            detail["transactions"][0]["shares"] = "1.0"
            detail_path.write_bytes(canonical_public_json_bytes(detail))
            update_manifest_entry(public_root, relative)

            errors = self.validate(public_root)
            self.assertTrue(
                any("canonical exact decimal" in error for error in errors),
                errors,
            )

    def test_wrong_contract_noncanonical_decimal_and_symlink_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            public_root = self.write_tree(Path(tmpdir))
            relative = "securities/03770N101.json"
            page_path = public_root / relative
            page = json.loads(page_path.read_text())
            page["data_contract_version"] = 5
            page["transactions"]["items"][0]["value"] = 1.25
            page_path.write_text(json.dumps(page, sort_keys=True) + "\n")
            update_manifest_entry(public_root, relative)
            symlink = public_root / "filings/symlink.json"
            symlink.symlink_to(public_root / "manifest.json")

            errors = self.validate(public_root)
            self.assertTrue(any("contract" in error for error in errors), errors)
            self.assertTrue(
                any("decimal" in error or "JSON type" in error for error in errors),
                errors,
            )
            self.assertTrue(any("symlink" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
