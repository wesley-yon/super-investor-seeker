"""Secure, deterministic parser for SEC Section 16 ownership XML."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from typing import cast
from urllib.parse import urlsplit

from lxml import etree

from insider_contract import (
    INSIDER_CONTRACT_VERSION,
    MAX_RAW_XML_BYTES,
    MAX_WARNING_RECORDS,
    InsiderContractError,
    absolute_decimal_product,
    canonical_decimal_string,
    classify_transaction_code,
    is_known_section16_control_code,
    section16_source_row_key,
    validate_insider_filing,
)
from insider_schema import (
    MAX_UNKNOWN_RECORDS,
    OWNERSHIP_NAMESPACE,
    derive_unknown_element_records,
)
from security_identity import (
    normalize_sec_cik,
    section16_owner_group_key,
    section16_security_class_key,
)


INSIDER_PARSER_VERSION = "1.0.0"
MAX_XML_ELEMENTS = 100_000

_ACCESSION_RE = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_FORM_TYPES = frozenset({"3", "3/A", "4", "4/A", "5", "5/A"})
_BOOLEAN_ELEMENT_NAMES = frozenset({
    "aff10b5One",
    "equitySwapInvolved",
    "form3HoldingsReported",
    "form4TransactionsReported",
    "isDirector",
    "isOfficer",
    "isOther",
    "isTenPercentOwner",
    "noSecuritiesOwned",
    "notSubjectToSection16",
})

class InsiderParseError(ValueError):
    """Raised when ownership XML cannot be normalized safely."""


class UnsafeOwnershipXML(InsiderParseError):
    """Raised when XML requests an external or entity-based resource."""


class _RejectingResolver(etree.Resolver):
    def resolve(self, url: str, public_id: str, context: object) -> object:
        raise UnsafeOwnershipXML("ownership XML external resolution is disabled")


class _ElementLimitTarget:
    """Incrementally reject hostile XML before building a document tree."""

    def start(self, tag: str, attributes: dict[str, str]) -> None:
        del tag, attributes
        self.element_count += 1
        if self.element_count > MAX_XML_ELEMENTS:
            raise InsiderParseError("ownership XML contains too many elements")

    def end(self, tag: str) -> None:
        del tag

    def data(self, data: str) -> None:
        del data

    def doctype(
        self,
        name: str,
        public_id: str | None,
        system_id: str | None,
    ) -> None:
        del name, public_id, system_id
        raise UnsafeOwnershipXML("ownership XML DTDs and entities are disabled")

    def close(self) -> None:
        return None

    def __init__(self) -> None:
        self.element_count = 0


def _preflight_xml_element_limit(xml_bytes: bytes) -> None:
    """Validate XML syntax and bound element count without a DOM."""

    target = _ElementLimitTarget()
    parser = etree.XMLParser(
        target=target,
        resolve_entities=False,
        load_dtd=False,
        no_network=True,
        dtd_validation=False,
        attribute_defaults=False,
        recover=False,
        huge_tree=False,
        remove_blank_text=False,
    )
    parser.resolvers.add(_RejectingResolver())
    try:
        for offset in range(0, len(xml_bytes), 8_192):
            parser.feed(xml_bytes[offset : offset + 8_192])
        parser.close()
    except UnsafeOwnershipXML:
        raise
    except etree.XMLSyntaxError as error:
        raise InsiderParseError("ownership XML is not well formed") from error


def _local_name(element: etree._Element) -> str:
    return etree.QName(element.tag).localname


def _namespace_uri(element: etree._Element) -> str | None:
    return etree.QName(element.tag).namespace


def _direct_children(
    parent: etree._Element | None,
    local_name: str,
) -> list[etree._Element]:
    if parent is None:
        return []
    return [
        child
        for child in parent
        if (
            isinstance(child.tag, str)
            and _local_name(child) == local_name
            and _namespace_uri(child) == _namespace_uri(parent)
        )
    ]


def _one_child(
    parent: etree._Element | None,
    local_name: str,
    *,
    required: bool = False,
) -> etree._Element | None:
    matches = _direct_children(parent, local_name)
    if len(matches) > 1:
        raise InsiderParseError(f"duplicate {local_name} element")
    if required and not matches:
        raise InsiderParseError(f"missing {local_name} element")
    return matches[0] if matches else None


def _element_text(element: etree._Element | None) -> str | None:
    if element is None:
        return None
    text = "".join(
        [element.text or ""]
        + [child.tail or "" for child in element if isinstance(child.tag, str)]
    ).strip()
    return text or None


def _child_text(
    parent: etree._Element | None,
    local_name: str,
    *,
    required: bool = False,
) -> str | None:
    element = _one_child(parent, local_name, required=required)
    value = _element_text(element)
    if required and value is None:
        raise InsiderParseError(f"missing {local_name} value")
    return value


def _wrapped_text(
    parent: etree._Element | None,
    local_name: str,
    *,
    required: bool = False,
) -> str | None:
    wrapper = _one_child(parent, local_name, required=required)
    if wrapper is None:
        return None
    value = _child_text(wrapper, "value", required=required)
    return value


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true"}:
        return True
    if normalized in {"0", "false"}:
        return False
    return None


def _validate_date(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    try:
        date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise InsiderParseError(f"{field_name} must be an ISO date") from error
    return value


def _validate_datetime(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise InsiderParseError(f"{field_name} must be an ISO timestamp") from error
    return value


def _validate_sec_url(value: str, field_name: str) -> str:
    if type(value) is not str:
        raise InsiderParseError(f"{field_name} must be an allowlisted SEC URL")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise InsiderParseError(
            f"{field_name} must be an allowlisted SEC URL"
        ) from error
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"www.sec.gov", "sec.gov"}
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or not parsed.path.startswith("/Archives/")
        or parsed.query
        or parsed.fragment
    ):
        raise InsiderParseError(f"{field_name} must be an allowlisted SEC URL")
    return value


def _raw_element(element: etree._Element) -> dict[str, object]:
    return {
        "local_name": _local_name(element),
        "namespace_uri": _namespace_uri(element),
        "attributes": dict(sorted(element.attrib.items())),
        "text": element.text,
        "tail": element.tail,
        "children": [
            _raw_element(child)
            for child in element
            if isinstance(child.tag, str)
        ],
    }


def _source_path(element: etree._Element) -> str:
    parts: list[str] = []
    current: etree._Element | None = element
    while current is not None and isinstance(current.tag, str):
        name = _local_name(current)
        parent = current.getparent()
        index = 1
        if parent is not None:
            for sibling in parent:
                if sibling is current:
                    break
                if isinstance(sibling.tag, str) and _local_name(sibling) == name:
                    index += 1
        parts.append(f"{name}[{index}]")
        current = parent
    return "/" + "/".join(reversed(parts))


def _unknown_element_records(
    root: etree._Element,
) -> list[dict[str, object]]:
    try:
        return derive_unknown_element_records(_raw_element(root))
    except ValueError as error:
        raise InsiderParseError(str(error)) from error


def _parser_warnings(
    root: etree._Element,
    *,
    transactions: list[dict[str, object]],
    holdings: list[dict[str, object]],
    footnotes: list[dict[str, object]],
    field_footnote_links: list[dict[str, object]],
    unknown_elements: list[dict[str, object]],
) -> list[dict[str, object]]:
    warnings: list[dict[str, object]] = []

    def append_warning(warning: dict[str, object]) -> None:
        if len(warnings) >= MAX_WARNING_RECORDS:
            raise InsiderParseError("ownership XML contains too many parser warnings")
        warnings.append(warning)

    for record in unknown_elements:
        if record["kind"] == "unknown_element":
            append_warning({
                "code": "unknown_element",
                "source_path": record["source_path"],
                "local_name": record["local_name"],
            })
        elif record["kind"] == "unknown_attributes":
            append_warning({
                "code": "unknown_attribute",
                "source_path": record["source_path"],
                "local_name": record["local_name"],
            })
    for element in root.iter():
        if (
            not isinstance(element.tag, str)
            or _local_name(element) not in _BOOLEAN_ELEMENT_NAMES
            or _namespace_uri(element) != _namespace_uri(root)
        ):
            continue
        raw_value = _element_text(element)
        if raw_value is not None and raw_value.strip().lower() not in {
            "0",
            "1",
            "false",
            "true",
        }:
            append_warning({
                "code": "invalid_boolean",
                "source_path": _source_path(element),
                "raw_value": raw_value,
            })
    for row in transactions:
        if row["requires_review"]:
            append_warning({
                "code": "unknown_transaction_code",
                "source_path": row["source_path"],
                "raw_code": row["transaction_code"],
            })
    for row in transactions:
        field_sources = row["field_sources"]
        assert isinstance(field_sources, dict)
        for field_name in (
            "acquired_disposed_code",
            "direct_indirect_ownership",
            "transaction_timeliness",
        ):
            raw_code = row[field_name]
            if raw_code is None or is_known_section16_control_code(
                field_name,
                raw_code,
            ):
                continue
            source = field_sources[field_name]
            assert isinstance(source, dict)
            append_warning({
                "code": "unknown_control_code",
                "source_path": source["source_path"],
                "field_name": field_name,
                "raw_code": raw_code,
            })
    for row in holdings:
        field_name = "direct_indirect_ownership"
        raw_code = row[field_name]
        if raw_code is None or is_known_section16_control_code(
            field_name,
            raw_code,
        ):
            continue
        field_sources = row["field_sources"]
        assert isinstance(field_sources, dict)
        source = field_sources[field_name]
        assert isinstance(source, dict)
        append_warning({
            "code": "unknown_control_code",
            "source_path": source["source_path"],
            "field_name": field_name,
            "raw_code": raw_code,
        })
    known_footnote_ids = {footnote["id"] for footnote in footnotes}
    for link in field_footnote_links:
        if link["footnote_id"] not in known_footnote_ids:
            append_warning({
                "code": "unresolved_footnote_reference",
                "source_path": link["source_path"],
                "field_name": link["field_name"],
                "footnote_id": link["footnote_id"],
            })
    return warnings


def _external_metadata_source(value: str | None) -> dict[str, object]:
    return {
        "provenance": "external_call_metadata",
        "raw_value": value,
        "normalized_value": value,
    }


def _element_footnote_ids(element: etree._Element | None) -> list[str]:
    if element is None:
        return []
    references: list[str] = []
    for reference in _direct_children(element, "footnoteId"):
        footnote_id = reference.get("id")
        if not footnote_id:
            raise InsiderParseError("footnote reference is missing its id")
        references.append(footnote_id)
    return references


def _source_field(
    path: str,
    raw_value: str | None,
    footnote_ids: list[str] | None = None,
) -> dict[str, object]:
    return {
        "source_path": path,
        "raw_value": raw_value,
        "footnote_ids": list(footnote_ids or []),
    }


def _wrapper_footnote_ids(
    parent: etree._Element | None,
    local_name: str,
) -> list[str]:
    return _element_footnote_ids(_one_child(parent, local_name))


def _parse_footnotes(root: etree._Element) -> list[dict[str, object]]:
    container = _one_child(root, "footnotes")
    footnotes: list[dict[str, object]] = []
    seen: set[str] = set()
    for footnote_index, footnote in enumerate(
        _direct_children(container, "footnote"),
        start=1,
    ):
        footnote_id = footnote.get("id")
        if not footnote_id:
            raise InsiderParseError("footnote is missing its id")
        if footnote_id in seen:
            raise InsiderParseError(f"duplicate footnote id: {footnote_id}")
        seen.add(footnote_id)
        footnotes.append({
            "id": footnote_id,
            "text": "".join(footnote.itertext()),
            "source_path": (
                "/ownershipDocument/footnotes/"
                f"footnote[{footnote_index}]"
            ),
            "raw_footnote": _raw_element(footnote),
        })
    return footnotes


def _parse_owner(owner: etree._Element, owner_order: int) -> dict[str, object]:
    owner_id = _one_child(owner, "reportingOwnerId", required=True)
    relationship = _one_child(owner, "reportingOwnerRelationship")
    address = _one_child(owner, "reportingOwnerAddress")
    cik = normalize_sec_cik(_child_text(owner_id, "rptOwnerCik", required=True))
    name = _child_text(owner_id, "rptOwnerName", required=True)
    address_names = {
        "street1": "rptOwnerStreet1",
        "street2": "rptOwnerStreet2",
        "city": "rptOwnerCity",
        "state": "rptOwnerState",
        "zip_code": "rptOwnerZipCode",
        "state_description": "rptOwnerStateDescription",
    }
    restricted_address = {
        output_name: value
        for output_name, source_name in address_names.items()
        if (value := _child_text(address, source_name)) is not None
    }
    is_director = _parse_bool(_child_text(relationship, "isDirector"))
    is_officer = _parse_bool(_child_text(relationship, "isOfficer"))
    is_ten_percent_owner = _parse_bool(
        _child_text(relationship, "isTenPercentOwner")
    )
    is_other = _parse_bool(_child_text(relationship, "isOther"))
    officer_title = _child_text(relationship, "officerTitle")
    other_text = _child_text(relationship, "otherText")
    country = _child_text(owner, "reportingOwnerCountry")
    owner_path = f"/ownershipDocument/reportingOwner[{owner_order + 1}]"
    field_sources = {
        "cik": _source_field(
            f"{owner_path}/reportingOwnerId/rptOwnerCik",
            _child_text(owner_id, "rptOwnerCik"),
        ),
        "name_as_filed": _source_field(
            f"{owner_path}/reportingOwnerId/rptOwnerName", name
        ),
        "is_director": _source_field(
            f"{owner_path}/reportingOwnerRelationship/isDirector",
            _child_text(relationship, "isDirector"),
        ),
        "is_officer": _source_field(
            f"{owner_path}/reportingOwnerRelationship/isOfficer",
            _child_text(relationship, "isOfficer"),
        ),
        "is_ten_percent_owner": _source_field(
            f"{owner_path}/reportingOwnerRelationship/isTenPercentOwner",
            _child_text(relationship, "isTenPercentOwner"),
        ),
        "is_other": _source_field(
            f"{owner_path}/reportingOwnerRelationship/isOther",
            _child_text(relationship, "isOther"),
        ),
        "officer_title": _source_field(
            f"{owner_path}/reportingOwnerRelationship/officerTitle",
            officer_title,
        ),
        "other_text": _source_field(
            f"{owner_path}/reportingOwnerRelationship/otherText", other_text
        ),
        "country": _source_field(
            f"{owner_path}/reportingOwnerCountry", country
        ),
    }
    field_sources.update({
        f"restricted_address.{output_name}": _source_field(
            f"{owner_path}/reportingOwnerAddress/{source_name}",
            _child_text(address, source_name),
        )
        for output_name, source_name in address_names.items()
    })
    return {
        "cik": cik,
        "name_as_filed": name,
        "owner_order": owner_order,
        "is_director": is_director,
        "is_officer": is_officer,
        "is_ten_percent_owner": is_ten_percent_owner,
        "is_other": is_other,
        "officer_title": officer_title,
        "other_text": other_text,
        "country": country,
        "has_restricted_address_source": address is not None,
        "restricted_address": restricted_address,
        "field_sources": field_sources,
        "raw_owner": _raw_element(owner),
    }


def _parse_non_derivative_transaction(
    row: etree._Element,
    *,
    index: int,
    accession_number: str,
    issuer_cik: str,
    owner_group_key: str,
    plan_status: str,
) -> dict[str, object]:
    row_path = (
        "/ownershipDocument/nonDerivativeTable/"
        f"nonDerivativeTransaction[{index + 1}]"
    )
    coding = _one_child(row, "transactionCoding")
    amounts = _one_child(row, "transactionAmounts")
    post_amounts = _one_child(row, "postTransactionAmounts")
    ownership = _one_child(row, "ownershipNature")

    security_title = _wrapped_text(row, "securityTitle", required=True)
    transaction_date = _validate_date(
        _wrapped_text(row, "transactionDate", required=True),
        "transaction_date",
    )
    deemed_execution_date = _validate_date(
        _wrapped_text(row, "deemedExecutionDate"),
        "deemed_execution_date",
    )
    raw_code = _child_text(coding, "transactionCode")
    classification = classify_transaction_code(raw_code)
    raw_shares = _wrapped_text(amounts, "transactionShares")
    raw_price = _wrapped_text(amounts, "transactionPricePerShare")
    raw_reported_total = _wrapped_text(amounts, "transactionTotalValue")
    shares = canonical_decimal_string(raw_shares)
    price = canonical_decimal_string(raw_price)
    reported_total = canonical_decimal_string(raw_reported_total)
    calculated_value = (
        absolute_decimal_product(shares, price)
        if shares is not None and price is not None
        else None
    )
    transaction_value = reported_total or calculated_value
    raw_post_shares = _wrapped_text(
        post_amounts,
        "sharesOwnedFollowingTransaction",
    )
    raw_post_value = _wrapped_text(
        post_amounts,
        "valueOwnedFollowingTransaction",
    )
    raw_form_type = _child_text(coding, "transactionFormType")
    raw_equity_swap = _child_text(coding, "equitySwapInvolved")
    raw_timeliness = _wrapped_text(row, "transactionTimeliness")
    raw_acquired_disposed = _wrapped_text(
        amounts,
        "transactionAcquiredDisposedCode",
    )
    raw_direct_indirect = _wrapped_text(
        ownership,
        "directOrIndirectOwnership",
    )
    raw_nature = _wrapped_text(ownership, "natureOfOwnership")
    field_locations = {
        "security_title_as_filed": (row, "securityTitle", f"{row_path}/securityTitle/value"),
        "transaction_date": (row, "transactionDate", f"{row_path}/transactionDate/value"),
        "deemed_execution_date": (
            row,
            "deemedExecutionDate",
            f"{row_path}/deemedExecutionDate/value",
        ),
        "equity_swap_involved": (
            coding,
            "equitySwapInvolved",
            f"{row_path}/transactionCoding/equitySwapInvolved",
        ),
        "transaction_timeliness": (
            row,
            "transactionTimeliness",
            f"{row_path}/transactionTimeliness/value",
        ),
        "shares": (
            amounts,
            "transactionShares",
            f"{row_path}/transactionAmounts/transactionShares/value",
        ),
        "price_per_share": (
            amounts,
            "transactionPricePerShare",
            f"{row_path}/transactionAmounts/transactionPricePerShare/value",
        ),
        "reported_total_value": (
            amounts,
            "transactionTotalValue",
            f"{row_path}/transactionAmounts/transactionTotalValue/value",
        ),
        "acquired_disposed_code": (
            amounts,
            "transactionAcquiredDisposedCode",
            f"{row_path}/transactionAmounts/transactionAcquiredDisposedCode/value",
        ),
        "post_transaction_shares": (
            post_amounts,
            "sharesOwnedFollowingTransaction",
            f"{row_path}/postTransactionAmounts/sharesOwnedFollowingTransaction/value",
        ),
        "post_transaction_value": (
            post_amounts,
            "valueOwnedFollowingTransaction",
            f"{row_path}/postTransactionAmounts/valueOwnedFollowingTransaction/value",
        ),
        "direct_indirect_ownership": (
            ownership,
            "directOrIndirectOwnership",
            f"{row_path}/ownershipNature/directOrIndirectOwnership/value",
        ),
        "nature_of_ownership": (
            ownership,
            "natureOfOwnership",
            f"{row_path}/ownershipNature/natureOfOwnership/value",
        ),
    }
    field_raw_values = {
        "security_title_as_filed": security_title,
        "transaction_date": transaction_date,
        "deemed_execution_date": deemed_execution_date,
        "equity_swap_involved": raw_equity_swap,
        "transaction_timeliness": raw_timeliness,
        "shares": raw_shares,
        "price_per_share": raw_price,
        "reported_total_value": raw_reported_total,
        "acquired_disposed_code": raw_acquired_disposed,
        "post_transaction_shares": raw_post_shares,
        "post_transaction_value": raw_post_value,
        "direct_indirect_ownership": raw_direct_indirect,
        "nature_of_ownership": raw_nature,
    }
    field_footnotes = {
        field_name: ids
        for field_name, (parent, element_name, _) in field_locations.items()
        if (ids := _wrapper_footnote_ids(parent, element_name))
    }
    for field_name, element in (
        ("transaction_coding", coding),
        ("transaction_form_type", _one_child(coding, "transactionFormType")),
        ("transaction_code", _one_child(coding, "transactionCode")),
    ):
        if (ids := _element_footnote_ids(element)):
            field_footnotes[field_name] = ids
    field_sources = {
        field_name: _source_field(
            path,
            field_raw_values[field_name],
            field_footnotes.get(field_name),
        )
        for field_name, (_, _, path) in field_locations.items()
    }
    field_sources["transaction_coding"] = _source_field(
        f"{row_path}/transactionCoding",
        _element_text(coding),
        field_footnotes.get("transaction_coding"),
    )
    field_sources["transaction_form_type"] = _source_field(
        f"{row_path}/transactionCoding/transactionFormType",
        raw_form_type,
        field_footnotes.get("transaction_form_type"),
    )
    field_sources["transaction_code"] = _source_field(
        f"{row_path}/transactionCoding/transactionCode",
        raw_code,
        field_footnotes.get("transaction_code"),
    )
    source_table = "non_derivative"
    return {
        "row_key": section16_source_row_key(
            accession_number,
            "transaction",
            source_table,
            index,
        ),
        "accession_number": accession_number,
        "source_table": source_table,
        "source_row_index": index,
        "source_path": row_path,
        "owner_group_key": owner_group_key,
        "security_title_as_filed": security_title,
        "security_class_key": section16_security_class_key(
            issuer_cik,
            security_title,
            is_derivative=False,
        ),
        "normalized_security_id": None,
        "transaction_date": transaction_date,
        "deemed_execution_date": deemed_execution_date,
        "transaction_form_type": raw_form_type,
        "transaction_coding": None,
        "transaction_code": classification["raw_code"],
        "transaction_label": classification["label"],
        "normalized_category": classification["normalized_category"],
        "is_meaningful_ps": classification["is_meaningful_ps"],
        "requires_review": classification["requires_review"],
        "equity_swap_involved": _parse_bool(raw_equity_swap),
        "transaction_timeliness": raw_timeliness,
        "shares": shares,
        "price_per_share": price,
        "reported_total_value": reported_total,
        "calculated_value": calculated_value,
        "transaction_value": transaction_value,
        "value_method": (
            "reported_total"
            if reported_total is not None
            else "calculated_shares_times_price"
            if calculated_value is not None
            else "unavailable"
        ),
        "acquired_disposed_code": raw_acquired_disposed,
        "post_transaction_shares": canonical_decimal_string(raw_post_shares),
        "post_transaction_value": canonical_decimal_string(raw_post_value),
        "direct_indirect_ownership": raw_direct_indirect,
        "nature_of_ownership": raw_nature,
        "conversion_or_exercise_price": None,
        "exercise_date": None,
        "expiration_date": None,
        "underlying_security_title": None,
        "underlying_security_class_key": None,
        "underlying_security_id": None,
        "underlying_shares": None,
        "underlying_value": None,
        "plan_status": plan_status,
        "field_footnotes": field_footnotes,
        "field_sources": field_sources,
        "raw_row": _raw_element(row),
    }


def _parse_derivative_transaction(
    row: etree._Element,
    *,
    index: int,
    accession_number: str,
    issuer_cik: str,
    owner_group_key: str,
    plan_status: str,
) -> dict[str, object]:
    row_path = (
        "/ownershipDocument/derivativeTable/"
        f"derivativeTransaction[{index + 1}]"
    )
    coding = _one_child(row, "transactionCoding")
    amounts = _one_child(row, "transactionAmounts")
    post_amounts = _one_child(row, "postTransactionAmounts")
    ownership = _one_child(row, "ownershipNature")
    underlying = _one_child(row, "underlyingSecurity")

    security_title = _wrapped_text(row, "securityTitle", required=True)
    transaction_date = _validate_date(
        _wrapped_text(row, "transactionDate", required=True),
        "transaction_date",
    )
    deemed_execution_date = _validate_date(
        _wrapped_text(row, "deemedExecutionDate"),
        "deemed_execution_date",
    )
    raw_form_type = _child_text(coding, "transactionFormType")
    raw_code = _child_text(coding, "transactionCode")
    classification = classify_transaction_code(raw_code)
    raw_equity_swap = _child_text(coding, "equitySwapInvolved")
    raw_timeliness = _wrapped_text(row, "transactionTimeliness")
    raw_shares = _wrapped_text(amounts, "transactionShares")
    raw_price = _wrapped_text(amounts, "transactionPricePerShare")
    raw_reported_total = _wrapped_text(amounts, "transactionTotalValue")
    shares = canonical_decimal_string(raw_shares)
    price = canonical_decimal_string(raw_price)
    reported_total = canonical_decimal_string(raw_reported_total)
    calculated_value = (
        absolute_decimal_product(shares, price)
        if shares is not None and price is not None
        else None
    )
    transaction_value = reported_total or calculated_value
    raw_acquired_disposed = _wrapped_text(
        amounts,
        "transactionAcquiredDisposedCode",
    )
    raw_post_shares = _wrapped_text(
        post_amounts,
        "sharesOwnedFollowingTransaction",
    )
    raw_post_value = _wrapped_text(
        post_amounts,
        "valueOwnedFollowingTransaction",
    )
    raw_direct_indirect = _wrapped_text(
        ownership,
        "directOrIndirectOwnership",
    )
    raw_nature = _wrapped_text(ownership, "natureOfOwnership")
    raw_conversion_price = _wrapped_text(row, "conversionOrExercisePrice")
    exercise_date = _validate_date(
        _wrapped_text(row, "exerciseDate"),
        "exercise_date",
    )
    expiration_date = _validate_date(
        _wrapped_text(row, "expirationDate"),
        "expiration_date",
    )
    underlying_title = _wrapped_text(
        underlying,
        "underlyingSecurityTitle",
    )
    raw_underlying_shares = _wrapped_text(
        underlying,
        "underlyingSecurityShares",
    )
    raw_underlying_value = _wrapped_text(
        underlying,
        "underlyingSecurityValue",
    )
    field_locations = {
        "security_title_as_filed": (row, "securityTitle", f"{row_path}/securityTitle/value"),
        "transaction_date": (row, "transactionDate", f"{row_path}/transactionDate/value"),
        "deemed_execution_date": (row, "deemedExecutionDate", f"{row_path}/deemedExecutionDate/value"),
        "conversion_or_exercise_price": (row, "conversionOrExercisePrice", f"{row_path}/conversionOrExercisePrice/value"),
        "exercise_date": (row, "exerciseDate", f"{row_path}/exerciseDate/value"),
        "expiration_date": (row, "expirationDate", f"{row_path}/expirationDate/value"),
        "equity_swap_involved": (coding, "equitySwapInvolved", f"{row_path}/transactionCoding/equitySwapInvolved"),
        "transaction_timeliness": (row, "transactionTimeliness", f"{row_path}/transactionTimeliness/value"),
        "shares": (amounts, "transactionShares", f"{row_path}/transactionAmounts/transactionShares/value"),
        "price_per_share": (amounts, "transactionPricePerShare", f"{row_path}/transactionAmounts/transactionPricePerShare/value"),
        "reported_total_value": (amounts, "transactionTotalValue", f"{row_path}/transactionAmounts/transactionTotalValue/value"),
        "acquired_disposed_code": (amounts, "transactionAcquiredDisposedCode", f"{row_path}/transactionAmounts/transactionAcquiredDisposedCode/value"),
        "post_transaction_shares": (post_amounts, "sharesOwnedFollowingTransaction", f"{row_path}/postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
        "post_transaction_value": (post_amounts, "valueOwnedFollowingTransaction", f"{row_path}/postTransactionAmounts/valueOwnedFollowingTransaction/value"),
        "direct_indirect_ownership": (ownership, "directOrIndirectOwnership", f"{row_path}/ownershipNature/directOrIndirectOwnership/value"),
        "nature_of_ownership": (ownership, "natureOfOwnership", f"{row_path}/ownershipNature/natureOfOwnership/value"),
        "underlying_security_title": (underlying, "underlyingSecurityTitle", f"{row_path}/underlyingSecurity/underlyingSecurityTitle/value"),
        "underlying_shares": (underlying, "underlyingSecurityShares", f"{row_path}/underlyingSecurity/underlyingSecurityShares/value"),
        "underlying_value": (underlying, "underlyingSecurityValue", f"{row_path}/underlyingSecurity/underlyingSecurityValue/value"),
    }
    field_raw_values = {
        "security_title_as_filed": security_title,
        "transaction_date": transaction_date,
        "deemed_execution_date": deemed_execution_date,
        "conversion_or_exercise_price": raw_conversion_price,
        "exercise_date": exercise_date,
        "expiration_date": expiration_date,
        "equity_swap_involved": raw_equity_swap,
        "transaction_timeliness": raw_timeliness,
        "shares": raw_shares,
        "price_per_share": raw_price,
        "reported_total_value": raw_reported_total,
        "acquired_disposed_code": raw_acquired_disposed,
        "post_transaction_shares": raw_post_shares,
        "post_transaction_value": raw_post_value,
        "direct_indirect_ownership": raw_direct_indirect,
        "nature_of_ownership": raw_nature,
        "underlying_security_title": underlying_title,
        "underlying_shares": raw_underlying_shares,
        "underlying_value": raw_underlying_value,
    }
    field_footnotes = {
        field_name: ids
        for field_name, (parent, element_name, _) in field_locations.items()
        if (ids := _wrapper_footnote_ids(parent, element_name))
    }
    for field_name, element in (
        ("transaction_coding", coding),
        ("transaction_form_type", _one_child(coding, "transactionFormType")),
        ("transaction_code", _one_child(coding, "transactionCode")),
    ):
        if (ids := _element_footnote_ids(element)):
            field_footnotes[field_name] = ids
    field_sources = {
        field_name: _source_field(
            path,
            field_raw_values[field_name],
            field_footnotes.get(field_name),
        )
        for field_name, (_, _, path) in field_locations.items()
    }
    field_sources["transaction_coding"] = _source_field(
        f"{row_path}/transactionCoding",
        _element_text(coding),
        field_footnotes.get("transaction_coding"),
    )
    field_sources["transaction_form_type"] = _source_field(
        f"{row_path}/transactionCoding/transactionFormType",
        raw_form_type,
        field_footnotes.get("transaction_form_type"),
    )
    field_sources["transaction_code"] = _source_field(
        f"{row_path}/transactionCoding/transactionCode",
        raw_code,
        field_footnotes.get("transaction_code"),
    )
    source_table = "derivative"
    return {
        "row_key": section16_source_row_key(
            accession_number,
            "transaction",
            source_table,
            index,
        ),
        "accession_number": accession_number,
        "source_table": source_table,
        "source_row_index": index,
        "source_path": row_path,
        "owner_group_key": owner_group_key,
        "security_title_as_filed": security_title,
        "security_class_key": section16_security_class_key(
            issuer_cik,
            security_title,
            is_derivative=True,
        ),
        "normalized_security_id": None,
        "transaction_date": transaction_date,
        "deemed_execution_date": deemed_execution_date,
        "transaction_form_type": raw_form_type,
        "transaction_coding": None,
        "transaction_code": classification["raw_code"],
        "transaction_label": classification["label"],
        "normalized_category": classification["normalized_category"],
        "is_meaningful_ps": classification["is_meaningful_ps"],
        "requires_review": classification["requires_review"],
        "equity_swap_involved": _parse_bool(raw_equity_swap),
        "transaction_timeliness": raw_timeliness,
        "shares": shares,
        "price_per_share": price,
        "reported_total_value": reported_total,
        "calculated_value": calculated_value,
        "transaction_value": transaction_value,
        "value_method": (
            "reported_total"
            if reported_total is not None
            else "calculated_shares_times_price"
            if calculated_value is not None
            else "unavailable"
        ),
        "acquired_disposed_code": raw_acquired_disposed,
        "post_transaction_shares": canonical_decimal_string(raw_post_shares),
        "post_transaction_value": canonical_decimal_string(raw_post_value),
        "direct_indirect_ownership": raw_direct_indirect,
        "nature_of_ownership": raw_nature,
        "conversion_or_exercise_price": canonical_decimal_string(
            raw_conversion_price
        ),
        "exercise_date": exercise_date,
        "expiration_date": expiration_date,
        "underlying_security_title": underlying_title,
        "underlying_security_class_key": (
            section16_security_class_key(
                issuer_cik,
                underlying_title,
                is_derivative=False,
            )
            if underlying_title is not None
            else None
        ),
        "underlying_security_id": None,
        "underlying_shares": canonical_decimal_string(raw_underlying_shares),
        "underlying_value": canonical_decimal_string(raw_underlying_value),
        "plan_status": plan_status,
        "field_footnotes": field_footnotes,
        "field_sources": field_sources,
        "raw_row": _raw_element(row),
    }


def _parse_derivative_holding(
    row: etree._Element,
    *,
    index: int,
    accession_number: str,
    issuer_cik: str,
    owner_group_key: str,
) -> dict[str, object]:
    row_path = (
        "/ownershipDocument/derivativeTable/"
        f"derivativeHolding[{index + 1}]"
    )
    coding = _one_child(row, "transactionCoding")
    post_amounts = _one_child(row, "postTransactionAmounts")
    ownership = _one_child(row, "ownershipNature")
    underlying = _one_child(row, "underlyingSecurity")
    security_title = _wrapped_text(row, "securityTitle", required=True)
    raw_shares_owned = _wrapped_text(
        post_amounts,
        "sharesOwnedFollowingTransaction",
    )
    raw_value_owned = _wrapped_text(
        post_amounts,
        "valueOwnedFollowingTransaction",
    )
    raw_conversion_price = _wrapped_text(row, "conversionOrExercisePrice")
    exercise_date = _validate_date(
        _wrapped_text(row, "exerciseDate"),
        "exercise_date",
    )
    expiration_date = _validate_date(
        _wrapped_text(row, "expirationDate"),
        "expiration_date",
    )
    underlying_title = _wrapped_text(
        underlying,
        "underlyingSecurityTitle",
    )
    raw_underlying_shares = _wrapped_text(
        underlying,
        "underlyingSecurityShares",
    )
    raw_underlying_value = _wrapped_text(
        underlying,
        "underlyingSecurityValue",
    )
    raw_direct_indirect = _wrapped_text(
        ownership,
        "directOrIndirectOwnership",
    )
    raw_nature = _wrapped_text(ownership, "natureOfOwnership")
    raw_form_type = _child_text(coding, "transactionFormType")
    field_locations = {
        "security_title_as_filed": (row, "securityTitle", f"{row_path}/securityTitle/value"),
        "shares_owned": (post_amounts, "sharesOwnedFollowingTransaction", f"{row_path}/postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
        "value_owned": (post_amounts, "valueOwnedFollowingTransaction", f"{row_path}/postTransactionAmounts/valueOwnedFollowingTransaction/value"),
        "conversion_or_exercise_price": (row, "conversionOrExercisePrice", f"{row_path}/conversionOrExercisePrice/value"),
        "exercise_date": (row, "exerciseDate", f"{row_path}/exerciseDate/value"),
        "expiration_date": (row, "expirationDate", f"{row_path}/expirationDate/value"),
        "underlying_security_title": (underlying, "underlyingSecurityTitle", f"{row_path}/underlyingSecurity/underlyingSecurityTitle/value"),
        "underlying_shares": (underlying, "underlyingSecurityShares", f"{row_path}/underlyingSecurity/underlyingSecurityShares/value"),
        "underlying_value": (underlying, "underlyingSecurityValue", f"{row_path}/underlyingSecurity/underlyingSecurityValue/value"),
        "direct_indirect_ownership": (ownership, "directOrIndirectOwnership", f"{row_path}/ownershipNature/directOrIndirectOwnership/value"),
        "nature_of_ownership": (ownership, "natureOfOwnership", f"{row_path}/ownershipNature/natureOfOwnership/value"),
    }
    field_raw_values = {
        "security_title_as_filed": security_title,
        "shares_owned": raw_shares_owned,
        "value_owned": raw_value_owned,
        "conversion_or_exercise_price": raw_conversion_price,
        "exercise_date": exercise_date,
        "expiration_date": expiration_date,
        "underlying_security_title": underlying_title,
        "underlying_shares": raw_underlying_shares,
        "underlying_value": raw_underlying_value,
        "direct_indirect_ownership": raw_direct_indirect,
        "nature_of_ownership": raw_nature,
    }
    field_footnotes = {
        field_name: ids
        for field_name, (parent, element_name, _) in field_locations.items()
        if (ids := _wrapper_footnote_ids(parent, element_name))
    }
    field_sources = {
        field_name: _source_field(
            path,
            field_raw_values[field_name],
            field_footnotes.get(field_name),
        )
        for field_name, (_, _, path) in field_locations.items()
    }
    field_sources["transaction_form_type"] = _source_field(
        f"{row_path}/transactionCoding/transactionFormType",
        raw_form_type,
    )
    source_table = "derivative"
    return {
        "row_key": section16_source_row_key(
            accession_number,
            "holding",
            source_table,
            index,
        ),
        "accession_number": accession_number,
        "source_table": source_table,
        "source_row_index": index,
        "source_path": row_path,
        "owner_group_key": owner_group_key,
        "security_title_as_filed": security_title,
        "security_class_key": section16_security_class_key(
            issuer_cik,
            security_title,
            is_derivative=True,
        ),
        "normalized_security_id": None,
        "transaction_form_type": raw_form_type,
        "shares_owned": canonical_decimal_string(raw_shares_owned),
        "value_owned": canonical_decimal_string(raw_value_owned),
        "direct_indirect_ownership": raw_direct_indirect,
        "nature_of_ownership": raw_nature,
        "conversion_or_exercise_price": canonical_decimal_string(
            raw_conversion_price
        ),
        "exercise_date": exercise_date,
        "expiration_date": expiration_date,
        "underlying_security_title": underlying_title,
        "underlying_security_class_key": (
            section16_security_class_key(
                issuer_cik,
                underlying_title,
                is_derivative=False,
            )
            if underlying_title is not None
            else None
        ),
        "underlying_security_id": None,
        "underlying_shares": canonical_decimal_string(raw_underlying_shares),
        "underlying_value": canonical_decimal_string(raw_underlying_value),
        "field_footnotes": field_footnotes,
        "field_sources": field_sources,
        "raw_row": _raw_element(row),
    }


def _parse_non_derivative_holding(
    row: etree._Element,
    *,
    index: int,
    accession_number: str,
    issuer_cik: str,
    owner_group_key: str,
) -> dict[str, object]:
    row_path = (
        "/ownershipDocument/nonDerivativeTable/"
        f"nonDerivativeHolding[{index + 1}]"
    )
    coding = _one_child(row, "transactionCoding")
    post_amounts = _one_child(row, "postTransactionAmounts")
    ownership = _one_child(row, "ownershipNature")
    security_title = _wrapped_text(row, "securityTitle", required=True)
    raw_shares_owned = _wrapped_text(
        post_amounts,
        "sharesOwnedFollowingTransaction",
    )
    raw_value_owned = _wrapped_text(
        post_amounts,
        "valueOwnedFollowingTransaction",
    )
    raw_direct_indirect = _wrapped_text(
        ownership,
        "directOrIndirectOwnership",
    )
    raw_nature = _wrapped_text(ownership, "natureOfOwnership")
    raw_form_type = _child_text(coding, "transactionFormType")
    field_locations = {
        "security_title_as_filed": (
            row,
            "securityTitle",
            f"{row_path}/securityTitle/value",
        ),
        "shares_owned": (
            post_amounts,
            "sharesOwnedFollowingTransaction",
            f"{row_path}/postTransactionAmounts/"
            "sharesOwnedFollowingTransaction/value",
        ),
        "value_owned": (
            post_amounts,
            "valueOwnedFollowingTransaction",
            f"{row_path}/postTransactionAmounts/"
            "valueOwnedFollowingTransaction/value",
        ),
        "direct_indirect_ownership": (
            ownership,
            "directOrIndirectOwnership",
            f"{row_path}/ownershipNature/directOrIndirectOwnership/value",
        ),
        "nature_of_ownership": (
            ownership,
            "natureOfOwnership",
            f"{row_path}/ownershipNature/natureOfOwnership/value",
        ),
    }
    field_raw_values = {
        "security_title_as_filed": security_title,
        "shares_owned": raw_shares_owned,
        "value_owned": raw_value_owned,
        "direct_indirect_ownership": raw_direct_indirect,
        "nature_of_ownership": raw_nature,
    }
    field_footnotes = {
        field_name: ids
        for field_name, (parent, element_name, _) in field_locations.items()
        if (ids := _wrapper_footnote_ids(parent, element_name))
    }
    field_sources = {
        field_name: _source_field(
            path,
            field_raw_values[field_name],
            field_footnotes.get(field_name),
        )
        for field_name, (_, _, path) in field_locations.items()
    }
    field_sources["transaction_form_type"] = _source_field(
        f"{row_path}/transactionCoding/transactionFormType",
        raw_form_type,
    )
    source_table = "non_derivative"
    return {
        "row_key": section16_source_row_key(
            accession_number,
            "holding",
            source_table,
            index,
        ),
        "accession_number": accession_number,
        "source_table": source_table,
        "source_row_index": index,
        "source_path": row_path,
        "owner_group_key": owner_group_key,
        "security_title_as_filed": security_title,
        "security_class_key": section16_security_class_key(
            issuer_cik,
            security_title,
            is_derivative=False,
        ),
        "normalized_security_id": None,
        "transaction_form_type": raw_form_type,
        "shares_owned": canonical_decimal_string(raw_shares_owned),
        "value_owned": canonical_decimal_string(raw_value_owned),
        "direct_indirect_ownership": raw_direct_indirect,
        "nature_of_ownership": raw_nature,
        "conversion_or_exercise_price": None,
        "exercise_date": None,
        "expiration_date": None,
        "underlying_security_title": None,
        "underlying_security_class_key": None,
        "underlying_security_id": None,
        "underlying_shares": None,
        "underlying_value": None,
        "field_footnotes": field_footnotes,
        "field_sources": field_sources,
        "raw_row": _raw_element(row),
    }


def _secure_root(xml_bytes: bytes) -> etree._Element:
    if type(xml_bytes) is not bytes:
        raise TypeError("ownership XML must be bytes")
    if not xml_bytes:
        raise InsiderParseError("ownership XML is empty")
    if len(xml_bytes) > MAX_RAW_XML_BYTES:
        raise InsiderParseError("ownership XML exceeds the parser size limit")
    _preflight_xml_element_limit(xml_bytes)
    parser = etree.XMLParser(
        resolve_entities=False,
        load_dtd=False,
        no_network=True,
        dtd_validation=False,
        attribute_defaults=False,
        recover=False,
        huge_tree=False,
        remove_blank_text=False,
    )
    parser.resolvers.add(_RejectingResolver())
    try:
        root = etree.fromstring(xml_bytes, parser=parser)
    except UnsafeOwnershipXML:
        raise
    except etree.XMLSyntaxError as error:
        raise InsiderParseError("ownership XML is not well formed") from error
    document_info = root.getroottree().docinfo
    if (
        document_info.doctype
        or document_info.internalDTD is not None
        or document_info.externalDTD is not None
        or any(isinstance(element, etree._Entity) for element in root.iter())
    ):
        raise UnsafeOwnershipXML(
            "ownership XML DTDs and entities are disabled"
        )
    if (
        _local_name(root) != "ownershipDocument"
        or _namespace_uri(root) not in {None, OWNERSHIP_NAMESPACE}
    ):
        raise InsiderParseError("ownership XML root must be ownershipDocument")
    for element_count, _element in enumerate(root.iter(), start=1):
        if element_count > MAX_XML_ELEMENTS:
            raise InsiderParseError(
                "ownership XML contains too many elements"
            )
    return root


def raw_ownership_document(xml_bytes: bytes) -> dict[str, object]:
    """Return the hardened, contract-stable raw XML tree for storage binding."""

    return _raw_element(_secure_root(xml_bytes))


def _parse_ownership_xml_impl(
    xml_bytes: bytes,
    *,
    accession_number: str,
    filing_date: str,
    source_index_url: str,
    source_document_url: str,
    accepted_at: str | None = None,
) -> dict[str, object]:
    if type(accession_number) is not str:
        raise InsiderParseError("accession number is invalid")
    if type(filing_date) is not str:
        raise InsiderParseError("filing_date must be an ISO date")
    if accepted_at is not None and type(accepted_at) is not str:
        raise InsiderParseError("accepted_at must be an ISO timestamp")
    if not _ACCESSION_RE.fullmatch(accession_number):
        raise InsiderParseError("accession number is invalid")
    filing_date = _validate_date(filing_date, "filing_date")
    accepted_at = _validate_datetime(accepted_at, "accepted_at")
    source_index_url = _validate_sec_url(source_index_url, "source_index_url")
    source_document_url = _validate_sec_url(
        source_document_url,
        "source_document_url",
    )
    root = _secure_root(xml_bytes)

    form_type = _child_text(root, "documentType", required=True)
    if form_type not in _FORM_TYPES:
        raise InsiderParseError("documentType is not a supported ownership form")
    issuer = _one_child(root, "issuer", required=True)
    issuer_cik = normalize_sec_cik(
        _child_text(issuer, "issuerCik", required=True)
    )
    owners = [
        _parse_owner(owner, owner_order)
        for owner_order, owner in enumerate(_direct_children(root, "reportingOwner"))
    ]
    if not owners:
        raise InsiderParseError("ownership filing must contain a reporting owner")
    owner_group_key = section16_owner_group_key(
        owner["cik"] for owner in owners
    )
    aff10b5_one = _parse_bool(_child_text(root, "aff10b5One"))
    plan_status = (
        "filing_marked"
        if aff10b5_one is True
        else "not_marked"
        if aff10b5_one is False
        else "unknown"
    )

    transactions: list[dict[str, object]] = []
    non_derivative_table = _one_child(root, "nonDerivativeTable")
    for index, row in enumerate(
        _direct_children(non_derivative_table, "nonDerivativeTransaction")
    ):
        transactions.append(
            _parse_non_derivative_transaction(
                row,
                index=index,
                accession_number=accession_number,
                issuer_cik=issuer_cik,
                owner_group_key=owner_group_key,
                plan_status=plan_status,
            )
        )

    holdings: list[dict[str, object]] = []
    for index, row in enumerate(
        _direct_children(non_derivative_table, "nonDerivativeHolding")
    ):
        holdings.append(
            _parse_non_derivative_holding(
                row,
                index=index,
                accession_number=accession_number,
                issuer_cik=issuer_cik,
                owner_group_key=owner_group_key,
            )
        )
    derivative_table = _one_child(root, "derivativeTable")
    for index, row in enumerate(
        _direct_children(derivative_table, "derivativeTransaction")
    ):
        transactions.append(
            _parse_derivative_transaction(
                row,
                index=index,
                accession_number=accession_number,
                issuer_cik=issuer_cik,
                owner_group_key=owner_group_key,
                plan_status=plan_status,
            )
        )
    for index, row in enumerate(
        _direct_children(derivative_table, "derivativeHolding")
    ):
        holdings.append(
            _parse_derivative_holding(
                row,
                index=index,
                accession_number=accession_number,
                issuer_cik=issuer_cik,
                owner_group_key=owner_group_key,
            )
        )

    footnotes = _parse_footnotes(root)
    field_footnote_links = [
        {
            "entity_type": "transaction",
            "row_key": row["row_key"],
            "source_table": row["source_table"],
            "source_row_index": row["source_row_index"],
            "field_name": field_name,
            "footnote_id": footnote_id,
            "reference_order": reference_order,
            "source_path": row["field_sources"][field_name]["source_path"],
        }
        for row in transactions
        for field_name, footnote_ids in sorted(
            cast(dict[str, list[str]], row["field_footnotes"]).items()
        )
        for reference_order, footnote_id in enumerate(footnote_ids)
    ]
    field_footnote_links.extend(
        {
            "entity_type": "holding",
            "row_key": row["row_key"],
            "source_table": row["source_table"],
            "source_row_index": row["source_row_index"],
            "field_name": field_name,
            "footnote_id": footnote_id,
            "reference_order": reference_order,
            "source_path": row["field_sources"][field_name]["source_path"],
        }
        for row in holdings
        for field_name, footnote_ids in sorted(
            cast(dict[str, list[str]], row["field_footnotes"]).items()
        )
        for reference_order, footnote_id in enumerate(footnote_ids)
    )
    unknown_elements = _unknown_element_records(root)
    warnings = _parser_warnings(
        root,
        transactions=transactions,
        holdings=holdings,
        footnotes=footnotes,
        field_footnote_links=field_footnote_links,
        unknown_elements=unknown_elements,
    )

    signatures: list[dict[str, object]] = []
    for index, signature in enumerate(_direct_children(root, "ownerSignature")):
        signature_path = f"/ownershipDocument/ownerSignature[{index + 1}]"
        name = _child_text(signature, "signatureName", required=True)
        signed_at = _validate_date(
            _child_text(signature, "signatureDate", required=True),
            "signature_date",
        )
        signatures.append({
            "signature_order": index,
            "name": name,
            "date": signed_at,
            "source_path": signature_path,
            "field_sources": {
                "name": _source_field(
                    f"{signature_path}/signatureName", name
                ),
                "date": _source_field(
                    f"{signature_path}/signatureDate", signed_at
                ),
            },
            "raw_signature": _raw_element(signature),
        })
    original_submission_date = _validate_date(
        _child_text(root, "dateOfOriginalSubmission"),
        "original_submission_date",
    )
    is_amendment = form_type.endswith("/A")
    schema_version = _child_text(root, "schemaVersion")
    period_of_report = _validate_date(
        _child_text(root, "periodOfReport"), "period_of_report"
    )
    not_subject_to_section16 = _parse_bool(
        _child_text(root, "notSubjectToSection16")
    )
    no_securities_owned = _parse_bool(_child_text(root, "noSecuritiesOwned"))
    form3_holdings_reported = _parse_bool(
        _child_text(root, "form3HoldingsReported")
    )
    form4_transactions_reported = _parse_bool(
        _child_text(root, "form4TransactionsReported")
    )
    remarks = _child_text(root, "remarks")
    issuer_name = _child_text(issuer, "issuerName", required=True)
    issuer_symbol = _child_text(issuer, "issuerTradingSymbol")
    issuer_foreign_symbol = _child_text(issuer, "issuerTradingSymbolForeign")

    payload = {
        "insider_contract_version": INSIDER_CONTRACT_VERSION,
        "parser_version": INSIDER_PARSER_VERSION,
        "schema_version": schema_version,
        "accession_number": accession_number,
        "raw_sha256": hashlib.sha256(xml_bytes).hexdigest(),
        "raw_document": _raw_element(root),
        "field_sources": {
            "schema_version": _source_field(
                "/ownershipDocument/schemaVersion", schema_version
            ),
            "form_type": _source_field(
                "/ownershipDocument/documentType", form_type
            ),
            "original_submission_date": _source_field(
                "/ownershipDocument/dateOfOriginalSubmission",
                original_submission_date,
            ),
            "period_of_report": _source_field(
                "/ownershipDocument/periodOfReport", period_of_report
            ),
            "not_subject_to_section16": _source_field(
                "/ownershipDocument/notSubjectToSection16",
                _child_text(root, "notSubjectToSection16"),
            ),
            "no_securities_owned": _source_field(
                "/ownershipDocument/noSecuritiesOwned",
                _child_text(root, "noSecuritiesOwned"),
            ),
            "form3_holdings_reported": _source_field(
                "/ownershipDocument/form3HoldingsReported",
                _child_text(root, "form3HoldingsReported"),
            ),
            "form4_transactions_reported": _source_field(
                "/ownershipDocument/form4TransactionsReported",
                _child_text(root, "form4TransactionsReported"),
            ),
            "aff10b5_one": _source_field(
                "/ownershipDocument/aff10b5One",
                _child_text(root, "aff10b5One"),
            ),
            "remarks": _source_field("/ownershipDocument/remarks", remarks),
        },
        "source": {
            "index_url": source_index_url,
            "document_url": source_document_url,
            "field_sources": {
                "accession_number": _external_metadata_source(accession_number),
                "filing_date": _external_metadata_source(filing_date),
                "accepted_at": _external_metadata_source(accepted_at),
                "index_url": _external_metadata_source(source_index_url),
                "document_url": _external_metadata_source(source_document_url),
            },
        },
        "form_type": form_type,
        "base_form_type": form_type.split("/", 1)[0],
        "is_amendment": is_amendment,
        "original_submission_date": original_submission_date,
        "filing_date": filing_date,
        "accepted_at": accepted_at,
        "period_of_report": period_of_report,
        "not_subject_to_section16": not_subject_to_section16,
        "no_securities_owned": no_securities_owned,
        "form3_holdings_reported": form3_holdings_reported,
        "form4_transactions_reported": form4_transactions_reported,
        "aff10b5_one": aff10b5_one,
        "issuer": {
            "cik": issuer_cik,
            "name_as_filed": issuer_name,
            "trading_symbol_as_filed": issuer_symbol,
            "foreign_trading_symbol_as_filed": issuer_foreign_symbol,
            "field_sources": {
                "cik": _source_field(
                    "/ownershipDocument/issuer/issuerCik",
                    _child_text(issuer, "issuerCik"),
                ),
                "name_as_filed": _source_field(
                    "/ownershipDocument/issuer/issuerName", issuer_name
                ),
                "trading_symbol_as_filed": _source_field(
                    "/ownershipDocument/issuer/issuerTradingSymbol",
                    issuer_symbol,
                ),
                "foreign_trading_symbol_as_filed": _source_field(
                    "/ownershipDocument/issuer/issuerTradingSymbolForeign",
                    issuer_foreign_symbol,
                ),
            },
            "raw_issuer": _raw_element(issuer),
        },
        "owner_group_key": owner_group_key,
        "owners": owners,
        "transactions": transactions,
        "holdings": holdings,
        "footnotes": footnotes,
        "field_footnote_links": field_footnote_links,
        "signatures": signatures,
        "remarks": remarks,
        "amendment": {
            "original_submission_date": original_submission_date,
            "amends_accession_number": None,
            "match_confidence": "unresolved" if is_amendment else None,
            "resolution_status": (
                "unresolved_phase2" if is_amendment else "not_applicable"
            ),
        },
        "unknown_elements": unknown_elements,
        "warnings": warnings,
        "privacy": {
            "classification": "private_normalized_source",
            "contains_restricted_owner_addresses": any(
                owner["has_restricted_address_source"] for owner in owners
            ),
            "public_projection_allowed": False,
        },
    }
    try:
        return validate_insider_filing(payload)
    except InsiderContractError as error:
        raise InsiderParseError(
            f"normalized ownership contract is invalid: {error}"
        ) from error


def parse_ownership_xml(
    xml_bytes: bytes,
    *,
    accession_number: str,
    filing_date: str,
    source_index_url: str,
    source_document_url: str,
    accepted_at: str | None = None,
) -> dict[str, object]:
    """Normalize one immutable ownership XML document into private JSON data."""

    if type(xml_bytes) is not bytes:
        raise TypeError("ownership XML must be bytes")
    try:
        return _parse_ownership_xml_impl(
            xml_bytes,
            accession_number=accession_number,
            filing_date=filing_date,
            accepted_at=accepted_at,
            source_index_url=source_index_url,
            source_document_url=source_document_url,
        )
    except InsiderParseError:
        raise
    except ValueError as error:
        raise InsiderParseError(
            "ownership XML contains invalid normalized values"
        ) from error


__all__ = [
    "INSIDER_PARSER_VERSION",
    "MAX_RAW_XML_BYTES",
    "MAX_UNKNOWN_RECORDS",
    "MAX_WARNING_RECORDS",
    "MAX_XML_ELEMENTS",
    "InsiderParseError",
    "UnsafeOwnershipXML",
    "parse_ownership_xml",
    "raw_ownership_document",
]
