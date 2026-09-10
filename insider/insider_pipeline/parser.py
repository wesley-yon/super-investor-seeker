"""Loss-preserving, standard-library SEC ownership XML parser.

The normalized fields are conveniences, not replacements for source evidence.
``raw_fields`` retains all XML leaves and attributes; original XML is retained by
the collector. Reporting owners are never multiplied across transaction rows.
Amendments are independent filings and are not automatically netted or replaced.

Classification follows SEC Form 4/5 Instruction 8:
https://www.sec.gov/files/form4.pdf
https://www.sec.gov/files/form5.pdf
"""

from collections import Counter
from decimal import Decimal, InvalidOperation, localcontext
import json
import re
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET


PARSER_VERSION = "1.0.0"
MAX_XML_BYTES = 64 * 1024 * 1024
_DECIMAL = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\Z")
_FORMS = {"3", "3/A", "3A", "4", "4/A", "4A", "5", "5/A", "5A"}

# Values intentionally distinguish P/S from awards, exercises, and withholding.
TRANSACTION_CLASSIFICATIONS = {
    "P": "purchase",
    "S": "sale",
    "A": "award",
    "D": "issuer_disposition",
    "F": "tax_or_exercise_payment",
    "I": "discretionary_transaction",
    "M": "exercise_or_conversion",
    "C": "exercise_or_conversion",
    "X": "exercise_or_conversion",
    "O": "exercise_or_conversion",
    "E": "short_position_expiration",
    "H": "long_position_expiration_or_cancellation",
    "G": "gift",
    "L": "small_acquisition",
    "W": "inheritance",
    "Z": "voting_trust_transfer",
    "J": "other",
    "K": "equity_swap",
    "U": "change_of_control_tender",
    "V": "voluntarily_reported",
}

ROW_FIELDS = {
    "security_title": "securityTitle",
    "transaction_date": "transactionDate",
    "deemed_execution_date": "deemedExecutionDate",
    "transaction_form_type": "transactionCoding/transactionFormType",
    "transaction_code": "transactionCoding/transactionCode",
    "equity_swap_involved": "transactionCoding/equitySwapInvolved",
    "transaction_timeliness": "transactionTimeliness",
    "transaction_shares": "transactionAmounts/transactionShares",
    "transaction_price_per_share": "transactionAmounts/transactionPricePerShare",
    # Alternative derivative amount, not sale proceeds or shares times price.
    "reported_transaction_total_value": "transactionAmounts/transactionTotalValue",
    "acquired_disposed": "transactionAmounts/transactionAcquiredDisposedCode",
    "shares_owned_following": "postTransactionAmounts/sharesOwnedFollowingTransaction",
    "value_owned_following": "postTransactionAmounts/valueOwnedFollowingTransaction",
    "direct_or_indirect": "ownershipNature/directOrIndirectOwnership",
    "nature_of_ownership": "ownershipNature/natureOfOwnership",
    "conversion_or_exercise_price": "conversionOrExercisePrice",
    "exercise_date": "exerciseDate",
    "expiration_date": "expirationDate",
    "underlying_security_title": "underlyingSecurity/underlyingSecurityTitle",
    "underlying_security_shares": "underlyingSecurity/underlyingSecurityShares",
    "underlying_security_value": "underlyingSecurity/underlyingSecurityValue",
}

OWNER_FIELDS = {
    "owner_cik": "reportingOwnerId/rptOwnerCik",
    "owner_name": "reportingOwnerId/rptOwnerName",
    "street1": "reportingOwnerAddress/rptOwnerStreet1",
    "street2": "reportingOwnerAddress/rptOwnerStreet2",
    "city": "reportingOwnerAddress/rptOwnerCity",
    "state": "reportingOwnerAddress/rptOwnerState",
    "zip_code": "reportingOwnerAddress/rptOwnerZipCode",
    "state_description": "reportingOwnerAddress/rptOwnerStateDescription",
    "is_director": "reportingOwnerRelationship/isDirector",
    "is_officer": "reportingOwnerRelationship/isOfficer",
    "is_ten_percent_owner": "reportingOwnerRelationship/isTenPercentOwner",
    "is_other": "reportingOwnerRelationship/isOther",
    "officer_title": "reportingOwnerRelationship/officerTitle",
    "other_text": "reportingOwnerRelationship/otherText",
}

FILING_FIELDS = {
    "schema_version": "schemaVersion",
    "document_type": "documentType",
    "period_of_report": "periodOfReport",
    "date_of_original_submission": "dateOfOriginalSubmission",
    "issuer_cik": "issuer/issuerCik",
    "issuer_name": "issuer/issuerName",
    "issuer_trading_symbol": "issuer/issuerTradingSymbol",
    "no_securities_owned": "noSecuritiesOwned",
    "not_subject_to_section16": "notSubjectToSection16",
    "form3_holdings_reported": "form3HoldingsReported",
    "form4_transactions_reported": "form4TransactionsReported",
    "form5_holdings_reported": "form5HoldingsReported",
    "form5_transactions_reported": "form5TransactionsReported",
    # This checkbox applies to the filing. It is not copied to individual rows.
    "aff10b5_one": "aff10b5One",
    "remarks": "remarks",
}


class OwnershipXMLParseError(ValueError):
    """Unsafe, malformed, or unsupported ownership source document."""


class _SafeTreeBuilder(ET.TreeBuilder):
    def __init__(self) -> None:
        super().__init__()
        self._depth = 0

    def start(self, tag: str, attrs: dict) -> ET.Element:
        self._depth += 1
        if self._depth > 128:
            raise OwnershipXMLParseError("XML nesting exceeds the 128-level parser limit")
        return super().start(tag, attrs)

    def end(self, tag: str) -> ET.Element:
        result = super().end(tag)
        self._depth -= 1
        return result

    def doctype(self, name: str, pubid: str, system: str) -> None:
        raise OwnershipXMLParseError("DTD declarations are prohibited")


def _local_name(name: str) -> str:
    return name.rsplit("}", 1)[-1]


def _children(element: ET.Element, name: str) -> List[ET.Element]:
    return [child for child in element if _local_name(child.tag) == name]


def _find(element: ET.Element, path: str) -> Optional[ET.Element]:
    current = element
    for part in path.split("/"):
        matches = _children(current, part)
        if not matches:
            return None
        current = matches[0]
    return current


def _text(element: Optional[ET.Element]) -> str:
    return "" if element is None else "".join(element.itertext()).strip()


def _value(element: ET.Element, path: str) -> str:
    node = _find(element, path)
    if node is None:
        return ""
    value_node = _find(node, "value")
    return _text(value_node if value_node is not None else node)


def _refs(element: ET.Element) -> List[str]:
    return [child.attrib["id"] for child in element.iter()
            if _local_name(child.tag) == "footnoteId" and "id" in child.attrib]


def _unique(items: List[str]) -> List[str]:
    return list(dict.fromkeys(items))


def _capture(element: ET.Element) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    """Capture relative paths, preserving sibling order and footnote placement.

    Paths use source element names with an index only for repeated siblings.
    Repeated attributes colliding after namespace removal become value lists.
    Original text is not stripped here, so normalized numeric text never erases
    source precision or source whitespace. Non-whitespace mixed text and tails
    are also retained for future schema extensions.
    """
    fields: Dict[str, Any] = {}
    footnote_refs: Dict[str, List[str]] = {}

    def add(path: str, value: str) -> None:
        if path not in fields:
            fields[path] = value
        elif isinstance(fields[path], list):
            fields[path].append(value)
        else:
            fields[path] = [fields[path], value]

    def walk(node: ET.Element, path: str) -> None:
        for key, value in node.attrib.items():
            add((path + "/" if path else "") + "@" + _local_name(key), value)
        children = list(node)
        if not children:
            add(path or ".", node.text or "")
            return
        if node.text and node.text.strip():
            add((path + "/" if path else "") + "text()", node.text)
        counts = Counter(_local_name(child.tag) for child in children)
        seen: Counter = Counter()
        direct_refs = [child.attrib["id"] for child in children
                       if _local_name(child.tag) == "footnoteId" and "id" in child.attrib]
        if direct_refs:
            values = [child for child in children if _local_name(child.tag) == "value"]
            ref_path = (path + "/value") if values else (path or ".")
            footnote_refs[ref_path] = _unique(direct_refs)
        for child in children:
            name = _local_name(child.tag)
            seen[name] += 1
            child_name = name + ("[%d]" % seen[name] if counts[name] > 1 else "")
            child_path = (path + "/" if path else "") + child_name
            walk(child, child_path)
            if child.tail and child.tail.strip():
                add(child_path + "/tail()", child.tail)

    walk(element, "")
    return fields, footnote_refs


def _normalized_refs(element: ET.Element, field_paths: Dict[str, str]) -> Dict[str, List[str]]:
    result = {}
    for field, path in field_paths.items():
        node = _find(element, path)
        if node is not None:
            refs = _unique(_refs(node))
            if refs:
                result[field] = refs
    return result


def _numeric_product(shares: str, price: str, warnings: List[str]) -> str:
    """Return an exact source-precision product, or blank without imputation."""
    parsed = []
    for field, value in (("transaction_shares", shares), ("transaction_price_per_share", price)):
        if not value:
            continue
        if not _DECIMAL.fullmatch(value):
            warnings.append("INVALID_NUMERIC_VALUE:" + field)
            continue
        try:
            number = Decimal(value)
        except InvalidOperation:
            warnings.append("INVALID_NUMERIC_VALUE:" + field)
            continue
        if not number.is_finite() or number < 0:
            warnings.append("INVALID_NUMERIC_VALUE:" + field)
            continue
        parsed.append(number)
    if len(parsed) != 2:
        return ""
    with localcontext() as context:
        context.prec = max(28, sum(len(number.as_tuple().digits) for number in parsed) + 2)
        return format(parsed[0] * parsed[1], "f")


def _classification(code: str, warnings: List[str]) -> Tuple[str, str]:
    if code in TRANSACTION_CLASSIFICATIONS:
        return TRANSACTION_CLASSIFICATIONS[code], "open_or_private" if code in {"P", "S"} else ""
    if code:
        warnings.append("UNKNOWN_TRANSACTION_CODE:" + code)
    else:
        warnings.append("MISSING_TRANSACTION_CODE")
    return "unknown", ""


def parse_ownership_xml(data: bytes, accession: str, source_url: str, filing_date: str = "") -> dict:
    """Parse one Form 3/4/5 (including amendments) into source-linked records.

    Numeric and checkbox source values stay strings; ``is_amendment`` is a
    derived bool. Date strings are not silently repaired. ``transaction_value``
    is a calculated amount, not independently reported proceeds or cash flow.
    Missing and footnote-only prices remain blank, while reported zero is kept.
    """
    if not isinstance(data, bytes):
        raise TypeError("data must be bytes")
    if len(data) > MAX_XML_BYTES:
        raise OwnershipXMLParseError("XML exceeds the 64 MiB parser size limit")
    # Removing NULs catches UTF-16/32 declarations too. The parser target is a
    # second guard; neither external resolution nor custom entities is allowed.
    if re.search(br"<!\s*(?:DOCTYPE|ENTITY)\b", data.replace(b"\x00", b""), re.IGNORECASE):
        raise OwnershipXMLParseError("DTD and entity declarations are prohibited")
    try:
        root = ET.fromstring(data, parser=ET.XMLParser(target=_SafeTreeBuilder()))
    except (ET.ParseError, LookupError) as exc:
        raise OwnershipXMLParseError("Malformed ownership XML: " + str(exc)) from exc
    if _local_name(root.tag) != "ownershipDocument":
        raise OwnershipXMLParseError("Expected ownershipDocument root")
    form = _value(root, "documentType")
    if form not in _FORMS:
        raise OwnershipXMLParseError("Unsupported ownership document type: " + repr(form))

    base = {"accession": accession, "source_url": source_url, "filing_date": filing_date}
    filing = dict(base)
    filing.update({field: _value(root, path) for field, path in FILING_FIELDS.items()})
    filing.update({
        "parser_version": PARSER_VERSION,
        "is_amendment": form.endswith("A"),
        "warnings": [],
        "normalized_footnote_refs": _normalized_refs(root, FILING_FIELDS),
    })
    filing["raw_fields"], filing["footnote_refs"] = _capture(root)
    if not filing["issuer_cik"]:
        filing["warnings"].append("MISSING_ISSUER_CIK")

    owners = []
    for index, element in enumerate(_children(root, "reportingOwner"), 1):
        owner = dict(base)
        owner.update({field: _value(element, path) for field, path in OWNER_FIELDS.items()})
        owner.update({
            "owner_id": "%s:OWNER:%d" % (accession, index),
            "owner_number": index,
            "normalized_footnote_refs": _normalized_refs(element, OWNER_FIELDS),
            "footnote_ids": _unique(_refs(element)),
        })
        owner["raw_fields"], owner["footnote_refs"] = _capture(element)
        owners.append(owner)
    if not owners:
        filing["warnings"].append("MISSING_REPORTING_OWNER")
    owner_join = {
        "owners_json": json.dumps(owners, ensure_ascii=False, separators=(",", ":")),
        "owner_names": " | ".join(owner["owner_name"] for owner in owners),
        "owner_ciks": " | ".join(owner["owner_cik"] for owner in owners),
    }
    filing.update(owner_join)
    filing["reporting_owner_count"] = len(owners)

    transactions: List[dict] = []
    holdings: List[dict] = []
    for table_tag, table, prefix in (("nonDerivativeTable", "non_derivative", "ND"),
                                     ("derivativeTable", "derivative", "D")):
        for row_kind, suffix, destination in (("Transaction", "T", transactions),
                                               ("Holding", "H", holdings)):
            row_tag = table_tag.removesuffix("Table") + row_kind
            elements = [row for tab in _children(root, table_tag) for row in _children(tab, row_tag)]
            for index, element in enumerate(elements, 1):
                row = dict(base)
                row.update({field: _value(element, path) for field, path in ROW_FIELDS.items()})
                row.update(owner_join)
                row.update({
                    "row_id": "%s:%s-%s:%d" % (accession, prefix, suffix, index),
                    "table": table,
                    "row_type": "transaction" if suffix == "T" else "holding",
                    "row_number": index,
                    "document_type": form,
                    "is_amendment": filing["is_amendment"],
                    "issuer_cik": filing["issuer_cik"],
                    "issuer_name": filing["issuer_name"],
                    "issuer_trading_symbol": filing["issuer_trading_symbol"],
                    "period_of_report": filing["period_of_report"],
                    "warnings": [],
                    "normalized_footnote_refs": _normalized_refs(element, ROW_FIELDS),
                    "footnote_ids": _unique(_refs(element)),
                    "transaction_value": "",
                    "transaction_value_basis": "",
                    "classification": "holding" if suffix == "H" else "unknown",
                    "market_scope": "",
                })
                row["raw_fields"], row["footnote_refs"] = _capture(element)
                if suffix == "T":
                    row["classification"], row["market_scope"] = _classification(row["transaction_code"], row["warnings"])
                    if row["transaction_shares"] and row["reported_transaction_total_value"]:
                        row["warnings"].append("BOTH_TRANSACTION_SHARES_AND_TOTAL_VALUE_REPORTED")
                    row["transaction_value"] = _numeric_product(row["transaction_shares"], row["transaction_price_per_share"], row["warnings"])
                    if row["transaction_value"]:
                        row["transaction_value_basis"] = "reported_shares_times_reported_price"
                destination.append(row)

    footnotes = []
    for section in _children(root, "footnotes"):
        for element in _children(section, "footnote"):
            note = dict(base)
            note.update({"footnote_id": element.attrib.get("id", ""), "text": _text(element)})
            note["raw_fields"], note["footnote_refs"] = _capture(element)
            footnotes.append(note)
    note_ids = [note["footnote_id"] for note in footnotes]
    for note_id, count in Counter(note_ids).items():
        if count > 1:
            filing["warnings"].append("DUPLICATE_FOOTNOTE_ID:" + note_id)
    unresolved = sorted(set(_refs(root)) - set(note_ids))
    filing["warnings"].extend("UNRESOLVED_FOOTNOTE_ID:" + ref for ref in unresolved)
    if unresolved:
        for row in transactions + holdings:
            row["warnings"].extend("UNRESOLVED_FOOTNOTE_ID:" + ref for ref in row["footnote_ids"] if ref in unresolved)

    signatures = []
    for index, element in enumerate(_children(root, "ownerSignature"), 1):
        signature = dict(base)
        signature.update({
            "signature_number": index,
            "signature_name": _value(element, "signatureName"),
            "signature_date": _value(element, "signatureDate"),
        })
        signature["raw_fields"], signature["footnote_refs"] = _capture(element)
        signatures.append(signature)
    filing["transaction_count"] = len(transactions)
    filing["holding_count"] = len(holdings)
    filing["footnote_count"] = len(footnotes)
    filing["signature_count"] = len(signatures)
    return {"filing": filing, "owners": owners, "transactions": transactions,
            "holdings": holdings, "footnotes": footnotes, "signatures": signatures}
