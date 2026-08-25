"""Private, versioned JSON contract primitives for Section 16 filings."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any
from urllib.parse import urlsplit

from insider_schema import derive_unknown_element_records
from security_identity import (
    normalize_sec_cik,
    section16_owner_group_key,
    section16_security_class_key,
)


INSIDER_CONTRACT_VERSION = 1
MAX_RAW_XML_BYTES = 10_000_000
MAX_NORMALIZED_JSON_BYTES = 100_000_000
MAX_WARNING_RECORDS = 10_000
MAX_DECIMAL_LEXICAL_CHARS = 1024
MAX_CANONICAL_DECIMAL_CHARS = 4096

_DECIMAL_LEXICAL_RE = re.compile(
    r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)"
)
_ACCESSION_RE = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_CIK_RE = re.compile(r"[0-9]{10}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_PARSER_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_ARCHIVE_PATH_SEGMENT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}")
_OWNERSHIP_DOCUMENT_FILENAME_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\.xml"
)
_ISO_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_ISO_UTC_TIMESTAMP_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z"
)
_SOURCE_PATH_COMPONENT_RE = re.compile(
    r"(?P<local_name>[^/\[\]]+)(?:\[(?P<index>[1-9][0-9]*)\])?"
)
_FORM_TYPES = frozenset({"3", "3/A", "4", "4/A", "5", "5/A"})
_CONTROL_CODE_VALUES = {
    "acquired_disposed_code": frozenset({"A", "D"}),
    "direct_indirect_ownership": frozenset({"D", "I"}),
    "transaction_timeliness": frozenset({"E", "L"}),
}
_REQUIRED_ROOT_FIELDS = frozenset({
    "accepted_at",
    "accession_number",
    "aff10b5_one",
    "amendment",
    "base_form_type",
    "field_footnote_links",
    "field_sources",
    "filing_date",
    "footnotes",
    "form3_holdings_reported",
    "form4_transactions_reported",
    "form_type",
    "holdings",
    "insider_contract_version",
    "is_amendment",
    "issuer",
    "no_securities_owned",
    "not_subject_to_section16",
    "original_submission_date",
    "owner_group_key",
    "owners",
    "parser_version",
    "period_of_report",
    "privacy",
    "raw_sha256",
    "raw_document",
    "remarks",
    "schema_version",
    "signatures",
    "source",
    "transactions",
    "unknown_elements",
    "warnings",
})
_COMMON_ROW_FIELDS = frozenset({
    "accession_number",
    "direct_indirect_ownership",
    "field_footnotes",
    "field_sources",
    "nature_of_ownership",
    "normalized_security_id",
    "owner_group_key",
    "raw_row",
    "row_key",
    "security_class_key",
    "security_title_as_filed",
    "source_path",
    "source_row_index",
    "source_table",
    "underlying_security_class_key",
    "underlying_security_id",
    "underlying_security_title",
    "underlying_shares",
    "underlying_value",
})
_TRANSACTION_ROW_FIELDS = _COMMON_ROW_FIELDS | frozenset({
    "acquired_disposed_code",
    "calculated_value",
    "conversion_or_exercise_price",
    "deemed_execution_date",
    "equity_swap_involved",
    "exercise_date",
    "expiration_date",
    "is_meaningful_ps",
    "normalized_category",
    "plan_status",
    "post_transaction_shares",
    "post_transaction_value",
    "price_per_share",
    "reported_total_value",
    "requires_review",
    "shares",
    "transaction_code",
    "transaction_coding",
    "transaction_date",
    "transaction_form_type",
    "transaction_label",
    "transaction_timeliness",
    "transaction_value",
    "value_method",
})
_HOLDING_ROW_FIELDS = _COMMON_ROW_FIELDS | frozenset({
    "conversion_or_exercise_price",
    "exercise_date",
    "expiration_date",
    "shares_owned",
    "transaction_form_type",
    "value_owned",
})
_NON_DERIVATIVE_TRANSACTION_SOURCES = frozenset({
    "acquired_disposed_code",
    "deemed_execution_date",
    "direct_indirect_ownership",
    "equity_swap_involved",
    "nature_of_ownership",
    "post_transaction_shares",
    "post_transaction_value",
    "price_per_share",
    "reported_total_value",
    "security_title_as_filed",
    "shares",
    "transaction_code",
    "transaction_coding",
    "transaction_date",
    "transaction_form_type",
    "transaction_timeliness",
})
_DERIVATIVE_TRANSACTION_SOURCES = (
    _NON_DERIVATIVE_TRANSACTION_SOURCES
    | frozenset({
        "conversion_or_exercise_price",
        "exercise_date",
        "expiration_date",
        "underlying_security_title",
        "underlying_shares",
        "underlying_value",
    })
)
_NON_DERIVATIVE_HOLDING_SOURCES = frozenset({
    "direct_indirect_ownership",
    "nature_of_ownership",
    "security_title_as_filed",
    "shares_owned",
    "transaction_form_type",
    "value_owned",
})
_DERIVATIVE_HOLDING_SOURCES = (
    _NON_DERIVATIVE_HOLDING_SOURCES
    | frozenset({
        "conversion_or_exercise_price",
        "exercise_date",
        "expiration_date",
        "underlying_security_title",
        "underlying_shares",
        "underlying_value",
    })
)
_REQUIRED_FIELD_SOURCES = {
    ("transactions", "non_derivative"): _NON_DERIVATIVE_TRANSACTION_SOURCES,
    ("transactions", "derivative"): _DERIVATIVE_TRANSACTION_SOURCES,
    ("holdings", "non_derivative"): _NON_DERIVATIVE_HOLDING_SOURCES,
    ("holdings", "derivative"): _DERIVATIVE_HOLDING_SOURCES,
}
_FIELD_SOURCE_SUFFIXES = {
    "acquired_disposed_code": (
        "transactionAmounts/transactionAcquiredDisposedCode/value"
    ),
    "conversion_or_exercise_price": "conversionOrExercisePrice/value",
    "deemed_execution_date": "deemedExecutionDate/value",
    "direct_indirect_ownership": (
        "ownershipNature/directOrIndirectOwnership/value"
    ),
    "equity_swap_involved": "transactionCoding/equitySwapInvolved",
    "exercise_date": "exerciseDate/value",
    "expiration_date": "expirationDate/value",
    "nature_of_ownership": "ownershipNature/natureOfOwnership/value",
    "post_transaction_shares": (
        "postTransactionAmounts/sharesOwnedFollowingTransaction/value"
    ),
    "post_transaction_value": (
        "postTransactionAmounts/valueOwnedFollowingTransaction/value"
    ),
    "price_per_share": "transactionAmounts/transactionPricePerShare/value",
    "reported_total_value": "transactionAmounts/transactionTotalValue/value",
    "security_title_as_filed": "securityTitle/value",
    "shares": "transactionAmounts/transactionShares/value",
    "shares_owned": (
        "postTransactionAmounts/sharesOwnedFollowingTransaction/value"
    ),
    "transaction_code": "transactionCoding/transactionCode",
    "transaction_coding": "transactionCoding",
    "transaction_date": "transactionDate/value",
    "transaction_form_type": "transactionCoding/transactionFormType",
    "transaction_timeliness": "transactionTimeliness/value",
    "underlying_security_title": (
        "underlyingSecurity/underlyingSecurityTitle/value"
    ),
    "underlying_shares": "underlyingSecurity/underlyingSecurityShares/value",
    "underlying_value": "underlyingSecurity/underlyingSecurityValue/value",
    "value_owned": (
        "postTransactionAmounts/valueOwnedFollowingTransaction/value"
    ),
}

_TRANSACTION_CODES = {
    "A": ("Award / Grant", "compensation_acquisition", False),
    "C": ("Conversion", "derivative_conversion", False),
    "D": ("Disposition to Issuer", "issuer_disposition", False),
    "E": ("Short Derivative Expiration", "derivative_expiration", False),
    "F": ("Tax / Exercise Withholding", "tax_or_exercise_withholding", False),
    "G": ("Gift", "gift", False),
    "H": ("Long Derivative Expiration", "derivative_expiration", False),
    "I": ("Discretionary Transaction", "discretionary_plan", False),
    "J": ("Other", "other", False),
    "L": ("Small Acquisition", "small_acquisition", False),
    "M": ("Exercise / Conversion", "derivative_exercise", False),
    "O": ("Out-of-Money Exercise", "derivative_exercise", False),
    "P": ("Purchase", "purchase", True),
    "S": ("Sale", "sale", True),
    "U": ("Tender / Change of Control", "change_of_control", False),
    "W": ("Will / Descent / Distribution", "inheritance", False),
    "X": ("In/At-the-Money Exercise", "derivative_exercise", False),
    "Z": ("Voting Trust Transfer", "voting_trust", False),
}

_DECIMAL_FIELD_NAMES = frozenset({
    "calculated_value",
    "conversion_or_exercise_price",
    "post_transaction_shares",
    "post_transaction_value",
    "price_per_share",
    "reported_total_value",
    "shares",
    "shares_owned",
    "transaction_value",
    "underlying_shares",
    "underlying_value",
    "value_owned",
})
_TRISTATE_FIELD_NAMES = frozenset({
    "aff10b5_one",
    "equity_swap_involved",
    "form3_holdings_reported",
    "form4_transactions_reported",
    "is_director",
    "is_officer",
    "is_other",
    "is_ten_percent_owner",
    "no_securities_owned",
    "not_subject_to_section16",
})
_SOURCE_DECIMAL_FIELD_NAMES = _DECIMAL_FIELD_NAMES - {
    "calculated_value",
    "transaction_value",
}


class InsiderContractError(ValueError):
    """Raised when normalized insider data violates the private contract."""


def _require_string(
    value: object,
    path: str,
    *,
    nullable: bool = False,
) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or not value:
        suffix = " or null" if nullable else ""
        raise InsiderContractError(f"{path} must be a non-empty string{suffix}")
    return value


def _require_iso_date(
    value: object,
    path: str,
    *,
    nullable: bool = False,
) -> str | None:
    text = _require_string(value, path, nullable=nullable)
    if text is None:
        return None
    if not _ISO_DATE_RE.fullmatch(text):
        raise InsiderContractError(f"{path} must be an ISO date")
    try:
        date.fromisoformat(text)
    except ValueError as error:
        raise InsiderContractError(f"{path} must be an ISO date") from error
    return text


def _require_iso_utc_timestamp(
    value: object,
    path: str,
    *,
    nullable: bool = False,
) -> str | None:
    text = _require_string(value, path, nullable=nullable)
    if text is None:
        return None
    if not _ISO_UTC_TIMESTAMP_RE.fullmatch(text):
        raise InsiderContractError(f"{path} must be an ISO timestamp in UTC")
    try:
        datetime.fromisoformat(f"{text[:-1]}+00:00")
    except ValueError as error:
        raise InsiderContractError(
            f"{path} must be an ISO timestamp in UTC"
        ) from error
    return text


def _validate_nested_temporal_fields(payload: dict[str, Any]) -> None:
    for signature_index, signature in enumerate(payload["signatures"]):
        if isinstance(signature, dict) and "date" in signature:
            _require_iso_date(
                signature["date"],
                f"signatures[{signature_index}].date",
            )

    for collection_name in ("transactions", "holdings"):
        for row_index, row in enumerate(payload[collection_name]):
            if not isinstance(row, dict):
                continue
            path = f"{collection_name}[{row_index}]"
            if collection_name == "transactions" and "transaction_date" in row:
                _require_iso_date(
                    row["transaction_date"],
                    f"{path}.transaction_date",
                )
            for field_name in (
                "deemed_execution_date",
                "exercise_date",
                "expiration_date",
            ):
                if field_name in row:
                    _require_iso_date(
                        row[field_name],
                        f"{path}.{field_name}",
                        nullable=True,
                    )


def _validate_sec_url(value: object, path: str) -> None:
    url = _require_string(value, path)
    assert url is not None
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise InsiderContractError(f"{path} must be an allowlisted SEC URL") from error
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
        raise InsiderContractError(f"{path} must be an allowlisted SEC URL")


def _validate_sec_accession_url(
    value: object,
    path: str,
    accession_number: str,
    *,
    allow_nested: bool,
) -> str:
    _validate_sec_url(value, path)
    assert isinstance(value, str)
    compact_accession = accession_number.replace("-", "")
    path_components = urlsplit(value).path.split("/")
    document_components = path_components[6:] if len(path_components) >= 7 else []
    if (
        (len(path_components) != 7 if not allow_nested else len(path_components) < 7)
        or path_components[1:4] != ["Archives", "edgar", "data"]
        or not path_components[4].isdigit()
        or path_components[5] != compact_accession
        or not document_components
        or any(
            not _ARCHIVE_PATH_SEGMENT_RE.fullmatch(component)
            for component in document_components
        )
        or (
            allow_nested
            and not _OWNERSHIP_DOCUMENT_FILENAME_RE.fullmatch(document_components[-1])
        )
    ):
        raise InsiderContractError(
            f"{path} must match the filing accession_number archive path"
        )
    try:
        return normalize_sec_cik(path_components[4])
    except ValueError as error:
        raise InsiderContractError(
            f"{path} archive issuer CIK is invalid"
        ) from error


def _require_source_path(value: object, path: str) -> str:
    source_path = _require_string(value, path)
    assert source_path is not None
    if not (
        source_path.startswith("/ownershipDocument/")
        or source_path.startswith("/ownershipDocument[")
    ):
        raise InsiderContractError(f"{path} must be an absolute ownership path")
    return source_path


def _validate_raw_element(value: object, path: str) -> None:
    pending: list[tuple[object, str]] = [(value, path)]
    required = {
        "attributes",
        "children",
        "local_name",
        "namespace_uri",
        "tail",
        "text",
    }
    while pending:
        element, element_path = pending.pop()
        if not isinstance(element, dict) or not required.issubset(element):
            raise InsiderContractError(
                f"{element_path} must be a structured raw XML element"
            )
        _require_string(element["local_name"], f"{element_path}.local_name")
        _require_string(
            element["namespace_uri"],
            f"{element_path}.namespace_uri",
            nullable=True,
        )
        for field in ("text", "tail"):
            if element[field] is not None and type(element[field]) is not str:
                raise InsiderContractError(
                    f"{element_path}.{field} must be a string or null"
                )
        attributes = element["attributes"]
        if not isinstance(attributes, dict) or any(
            type(key) is not str or type(item) is not str
            for key, item in attributes.items()
        ):
            raise InsiderContractError(
                f"{element_path}.attributes must contain strings"
            )
        children = element["children"]
        if not isinstance(children, list):
            raise InsiderContractError(
                f"{element_path}.children must be a list"
            )
        pending.extend(
            (child, f"{element_path}.children[{index}]")
            for index, child in enumerate(children)
        )


def _raw_element_text(element: dict[str, Any]) -> str | None:
    text = element["text"]
    assert text is None or isinstance(text, str)
    fragments = [text or ""]
    children = element["children"]
    assert isinstance(children, list)
    for child in children:
        assert isinstance(child, dict)
        tail = child["tail"]
        assert tail is None or isinstance(tail, str)
        fragments.append(tail or "")
    value = "".join(fragments).strip()
    return value or None


def _raw_element_itertext(element: dict[str, Any]) -> str:
    text = element["text"]
    assert text is None or isinstance(text, str)
    fragments = [text or ""]
    children = element["children"]
    assert isinstance(children, list)
    for child in children:
        assert isinstance(child, dict)
        fragments.append(_raw_element_itertext(child))
        tail = child["tail"]
        assert tail is None or isinstance(tail, str)
        fragments.append(tail or "")
    return "".join(fragments)


def _raw_element_at_source_path(
    raw_root: dict[str, Any],
    root_source_path: str,
    source_path: str,
) -> dict[str, Any] | None:
    if source_path == root_source_path:
        return raw_root
    prefix = f"{root_source_path}/"
    if not source_path.startswith(prefix):
        raise InsiderContractError("raw source path is inconsistent")
    relative_path = source_path[len(prefix) :]
    if not relative_path:
        raise InsiderContractError("raw source path is inconsistent")

    current = raw_root
    for component in relative_path.split("/"):
        match = _SOURCE_PATH_COMPONENT_RE.fullmatch(component)
        if match is None:
            raise InsiderContractError("raw source path is invalid")
        local_name = match.group("local_name")
        explicit_index = match.group("index")
        child_index = int(explicit_index or "1") - 1
        children = current["children"]
        assert isinstance(children, list)
        matches = [
            child
            for child in children
            if (
                isinstance(child, dict)
                and child.get("local_name") == local_name
                and child.get("namespace_uri") == current.get("namespace_uri")
            )
        ]
        if explicit_index is None and len(matches) > 1:
            raise InsiderContractError("raw source path is ambiguous")
        if child_index >= len(matches):
            return None
        current = matches[child_index]
    return current


def _validate_raw_document_subtree(
    payload: dict[str, Any],
    source_path: str,
    raw_subtree: dict[str, Any],
    label: str,
) -> None:
    raw_document = payload["raw_document"]
    assert isinstance(raw_document, dict)
    expected = _raw_element_at_source_path(
        raw_document,
        "/ownershipDocument",
        source_path,
    )
    if expected is None or raw_subtree != expected:
        raise InsiderContractError(
            f"{label} raw lineage does not match raw_document"
        )


def _raw_field_source_value(
    raw_row: dict[str, Any],
    row_source_path: str,
    field_source_path: str,
) -> str | None:
    current = _raw_element_at_source_path(
        raw_row,
        row_source_path,
        field_source_path,
    )
    return _raw_element_text(current) if current is not None else None


def _raw_field_footnote_ids(
    raw_row: dict[str, Any],
    row_source_path: str,
    field_source_path: str,
) -> list[str]:
    reference_path = (
        field_source_path[: -len("/value")]
        if field_source_path.endswith("/value")
        else field_source_path
    )
    element = _raw_element_at_source_path(
        raw_row,
        row_source_path,
        reference_path,
    )
    if element is None:
        return []
    children = element["children"]
    assert isinstance(children, list)
    references: list[str] = []
    for child in children:
        if (
            not isinstance(child, dict)
            or child.get("local_name") != "footnoteId"
            or child.get("namespace_uri") != element.get("namespace_uri")
        ):
            continue
        attributes = child.get("attributes")
        footnote_id = attributes.get("id") if isinstance(attributes, dict) else None
        if type(footnote_id) is not str or not footnote_id:
            raise InsiderContractError("raw footnote reference is missing its id")
        references.append(footnote_id)
    return references


def _validate_filing_header_sources(payload: dict[str, Any]) -> None:
    raw_document = payload["raw_document"]
    assert isinstance(raw_document, dict)
    _validate_scalar_field_sources(
        payload,
        raw_document,
        "/ownershipDocument",
        {
            "schema_version": ("schemaVersion", None),
            "form_type": ("documentType", None),
            "original_submission_date": ("dateOfOriginalSubmission", None),
            "period_of_report": ("periodOfReport", None),
            "not_subject_to_section16": ("notSubjectToSection16", "bool"),
            "no_securities_owned": ("noSecuritiesOwned", "bool"),
            "form3_holdings_reported": ("form3HoldingsReported", "bool"),
            "form4_transactions_reported": ("form4TransactionsReported", "bool"),
            "aff10b5_one": ("aff10b5One", "bool"),
            "remarks": ("remarks", None),
        },
        "filing",
    )


def _lineage_bool(raw_value: str | None) -> bool | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true"}:
        return True
    if normalized in {"0", "false"}:
        return False
    return None


def _lineage_cik(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    try:
        return normalize_sec_cik(raw_value)
    except ValueError as error:
        raise InsiderContractError("field source CIK is invalid") from error


def _validate_scalar_field_sources(
    container: dict[str, Any],
    raw_element: dict[str, Any],
    base_path: str,
    specifications: dict[str, tuple[str, object]],
    label: str,
) -> None:
    field_sources = container.get("field_sources")
    if not isinstance(field_sources, dict):
        raise InsiderContractError(f"{label} field_sources must be an object")
    missing = sorted(set(specifications) - set(field_sources))
    if missing:
        raise InsiderContractError(f"{label} is missing field sources: {missing}")
    for field_name, (suffix, normalizer) in specifications.items():
        source = field_sources.get(field_name)
        if not isinstance(source, dict):
            raise InsiderContractError(f"{label} field source is invalid")
        expected_path = f"{base_path}/{suffix}"
        source_path = _require_source_path(
            source.get("source_path"),
            f"{label} field source path",
        )
        if source_path != expected_path:
            raise InsiderContractError(f"{label} field source path is inconsistent")
        raw_value = source.get("raw_value")
        if raw_value is not None and type(raw_value) is not str:
            raise InsiderContractError(f"{label} field raw value is invalid")
        if raw_value != _raw_field_source_value(
            raw_element,
            base_path,
            source_path,
        ):
            raise InsiderContractError(
                f"{label} field source does not match its raw lineage"
            )
        if normalizer == "bool":
            normalized = _lineage_bool(raw_value)
        elif normalizer == "cik":
            normalized = _lineage_cik(raw_value)
        else:
            normalized = raw_value
        if container.get(field_name) != normalized:
            raise InsiderContractError(
                f"{label} field source does not match its normalized value"
            )


def _validate_external_metadata_sources(payload: dict[str, Any]) -> None:
    source = payload["source"]
    assert isinstance(source, dict)
    field_sources = source.get("field_sources")
    if not isinstance(field_sources, dict):
        raise InsiderContractError("source external metadata provenance is required")
    expected = {
        "accession_number": payload["accession_number"],
        "filing_date": payload["filing_date"],
        "accepted_at": payload["accepted_at"],
        "index_url": source["index_url"],
        "document_url": source["document_url"],
    }
    for field_name, normalized_value in expected.items():
        record = field_sources.get(field_name)
        if (
            not isinstance(record, dict)
            or record.get("provenance") != "external_call_metadata"
            or record.get("raw_value") != normalized_value
            or record.get("normalized_value") != normalized_value
        ):
            raise InsiderContractError(
                "source external metadata provenance is invalid"
            )


def _validate_root_shape(payload: dict[str, Any]) -> str:
    missing = sorted(_REQUIRED_ROOT_FIELDS - set(payload))
    if missing:
        raise InsiderContractError(
            f"insider filing is missing required fields: {missing}"
        )
    if (
        type(payload["insider_contract_version"]) is not int
        or payload["insider_contract_version"] != INSIDER_CONTRACT_VERSION
    ):
        raise InsiderContractError("insider contract version is unsupported")
    parser_version = payload["parser_version"]
    if (
        type(parser_version) is not str
        or not _PARSER_VERSION_RE.fullmatch(parser_version)
    ):
        raise InsiderContractError("parser_version is invalid")
    accession = payload["accession_number"]
    if type(accession) is not str or not _ACCESSION_RE.fullmatch(accession):
        raise InsiderContractError("accession_number is invalid")
    raw_sha256 = payload["raw_sha256"]
    if type(raw_sha256) is not str or not _SHA256_RE.fullmatch(raw_sha256):
        raise InsiderContractError("raw_sha256 is invalid")
    _require_string(payload["schema_version"], "schema_version")
    _require_iso_date(payload["filing_date"], "filing_date")
    _require_iso_utc_timestamp(
        payload["accepted_at"],
        "accepted_at",
        nullable=True,
    )
    _require_iso_date(
        payload["original_submission_date"],
        "original_submission_date",
        nullable=True,
    )
    _require_iso_date(
        payload["period_of_report"],
        "period_of_report",
        nullable=True,
    )

    source = payload["source"]
    if not isinstance(source, dict):
        raise InsiderContractError("source must be an object")
    source_index_cik = _validate_sec_accession_url(
        source.get("index_url"),
        "source.index_url",
        accession,
        allow_nested=False,
    )
    source_document_cik = _validate_sec_accession_url(
        source.get("document_url"),
        "source.document_url",
        accession,
        allow_nested=True,
    )
    _validate_external_metadata_sources(payload)

    raw_document = payload.get("raw_document")
    _validate_raw_element(raw_document, "filing.raw_document")
    assert isinstance(raw_document, dict)
    if raw_document["local_name"] != "ownershipDocument":
        raise InsiderContractError("filing raw_document lineage is invalid")

    form_type = payload["form_type"]
    if form_type not in _FORM_TYPES:
        raise InsiderContractError("form_type is unsupported")
    if payload["base_form_type"] != form_type.split("/", 1)[0]:
        raise InsiderContractError("base_form_type does not match form_type")
    if type(payload["is_amendment"]) is not bool:
        raise InsiderContractError("is_amendment must be a boolean")
    if payload["is_amendment"] != form_type.endswith("/A"):
        raise InsiderContractError("is_amendment does not match form_type")

    _require_string(payload["remarks"], "remarks", nullable=True)

    issuer = payload["issuer"]
    if not isinstance(issuer, dict):
        raise InsiderContractError("issuer must be an object")
    issuer_fields = {
        "cik",
        "foreign_trading_symbol_as_filed",
        "name_as_filed",
        "trading_symbol_as_filed",
    }
    if not issuer_fields.issubset(issuer):
        raise InsiderContractError("issuer is missing required fields")
    issuer_cik = issuer.get("cik")
    if (
        type(issuer_cik) is not str
        or not _CIK_RE.fullmatch(issuer_cik)
        or issuer_cik == "0000000000"
    ):
        raise InsiderContractError("issuer.cik is invalid")
    if source_index_cik != issuer_cik:
        raise InsiderContractError(
            "source index URL archive CIK does not match issuer CIK"
        )
    _require_string(issuer.get("name_as_filed"), "issuer.name_as_filed")
    for field in (
        "trading_symbol_as_filed",
        "foreign_trading_symbol_as_filed",
    ):
        _require_string(issuer.get(field), f"issuer.{field}", nullable=True)
    raw_issuer = issuer.get("raw_issuer")
    _validate_raw_element(raw_issuer, "issuer.raw_issuer")
    assert isinstance(raw_issuer, dict)
    if raw_issuer["local_name"] != "issuer":
        raise InsiderContractError("issuer raw_issuer lineage is invalid")
    _validate_raw_document_subtree(
        payload,
        "/ownershipDocument/issuer",
        raw_issuer,
        "issuer",
    )
    _validate_scalar_field_sources(
        issuer,
        raw_issuer,
        "/ownershipDocument/issuer",
        {
            "cik": ("issuerCik", "cik"),
            "name_as_filed": ("issuerName", None),
            "trading_symbol_as_filed": ("issuerTradingSymbol", None),
            "foreign_trading_symbol_as_filed": (
                "issuerTradingSymbolForeign",
                None,
            ),
        },
        "issuer",
    )

    owner_group_key = payload["owner_group_key"]
    if (
        type(owner_group_key) is not str
        or not _SHA256_RE.fullmatch(owner_group_key)
    ):
        raise InsiderContractError("owner_group_key is invalid")
    owners = payload["owners"]
    if not isinstance(owners, list) or not owners:
        raise InsiderContractError("owners must be a non-empty list")
    for field in (
        "transactions",
        "holdings",
        "footnotes",
        "field_footnote_links",
        "signatures",
        "unknown_elements",
        "warnings",
    ):
        if not isinstance(payload[field], list):
            raise InsiderContractError(f"{field} must be a list")
    if len(payload["warnings"]) > MAX_WARNING_RECORDS:
        raise InsiderContractError("private filing has too many parser warnings")
    if not isinstance(payload["amendment"], dict):
        raise InsiderContractError("amendment must be an object")
    _validate_nested_temporal_fields(payload)
    return source_document_cik


def _validate_owner_and_signature_shapes(payload: dict[str, Any]) -> frozenset[str]:
    owner_ciks: list[str] = []
    for owner_order, owner in enumerate(payload["owners"]):
        if not isinstance(owner, dict):
            raise InsiderContractError("owners must contain objects")
        owner_fields = {
            "cik",
            "country",
            "has_restricted_address_source",
            "is_director",
            "is_officer",
            "is_other",
            "is_ten_percent_owner",
            "name_as_filed",
            "officer_title",
            "other_text",
            "owner_order",
            "field_sources",
            "raw_owner",
            "restricted_address",
        }
        if not owner_fields.issubset(owner):
            raise InsiderContractError("owner is missing required fields")
        cik = owner.get("cik")
        if (
            type(cik) is not str
            or not _CIK_RE.fullmatch(cik)
            or cik == "0000000000"
        ):
            raise InsiderContractError("owner cik is invalid")
        owner_ciks.append(cik)
        if owner.get("owner_order") != owner_order or type(
            owner.get("owner_order")
        ) is not int:
            raise InsiderContractError("owner order is invalid")
        _require_string(owner.get("name_as_filed"), "owner name_as_filed")
        for field in ("officer_title", "other_text", "country"):
            _require_string(owner.get(field), f"owner {field}", nullable=True)
        if type(owner.get("has_restricted_address_source")) is not bool:
            raise InsiderContractError(
                "owner address-source classification must be boolean"
            )
        if owner["has_restricted_address_source"] != _raw_tree_contains(
            owner.get("raw_owner"),
            "reportingOwnerAddress",
        ):
            raise InsiderContractError(
                "owner address-source classification does not match raw lineage"
            )
        restricted_address = owner.get("restricted_address")
        if not isinstance(restricted_address, dict) or any(
            type(key) is not str or type(value) is not str
            for key, value in restricted_address.items()
        ):
            raise InsiderContractError("owner restricted_address is invalid")
        _validate_raw_element(owner.get("raw_owner"), "owner.raw_owner")
        if owner["raw_owner"]["local_name"] != "reportingOwner":
            raise InsiderContractError("owner raw_owner lineage is invalid")
        _validate_raw_document_subtree(
            payload,
            f"/ownershipDocument/reportingOwner[{owner_order + 1}]",
            owner["raw_owner"],
            "owner",
        )
        _validate_scalar_field_sources(
            owner,
            owner["raw_owner"],
            f"/ownershipDocument/reportingOwner[{owner_order + 1}]",
            {
                "cik": ("reportingOwnerId/rptOwnerCik", "cik"),
                "name_as_filed": ("reportingOwnerId/rptOwnerName", None),
                "is_director": (
                    "reportingOwnerRelationship/isDirector",
                    "bool",
                ),
                "is_officer": (
                    "reportingOwnerRelationship/isOfficer",
                    "bool",
                ),
                "is_ten_percent_owner": (
                    "reportingOwnerRelationship/isTenPercentOwner",
                    "bool",
                ),
                "is_other": ("reportingOwnerRelationship/isOther", "bool"),
                "officer_title": (
                    "reportingOwnerRelationship/officerTitle",
                    None,
                ),
                "other_text": ("reportingOwnerRelationship/otherText", None),
                "country": ("reportingOwnerCountry", None),
            },
            "owner",
        )
        address_sources = {
            "street1": "rptOwnerStreet1",
            "street2": "rptOwnerStreet2",
            "city": "rptOwnerCity",
            "state": "rptOwnerState",
            "zip_code": "rptOwnerZipCode",
            "state_description": "rptOwnerStateDescription",
        }
        for field_name, source_name in address_sources.items():
            source = owner["field_sources"].get(
                f"restricted_address.{field_name}"
            )
            expected_path = (
                f"/ownershipDocument/reportingOwner[{owner_order + 1}]/"
                f"reportingOwnerAddress/{source_name}"
            )
            if (
                not isinstance(source, dict)
                or source.get("source_path") != expected_path
                or source.get("raw_value")
                != restricted_address.get(field_name)
                or source.get("raw_value")
                != _raw_field_source_value(
                    owner["raw_owner"],
                    f"/ownershipDocument/reportingOwner[{owner_order + 1}]",
                    expected_path,
                )
            ):
                raise InsiderContractError(
                    "owner field source does not match its raw lineage"
                )
    if len(owner_ciks) != len(set(owner_ciks)):
        raise InsiderContractError("owner CIKs must be unique within a filing")
    if section16_owner_group_key(owner_ciks) != payload["owner_group_key"]:
        raise InsiderContractError("owner_group_key does not match owners")

    for signature_order, signature in enumerate(payload["signatures"]):
        if not isinstance(signature, dict):
            raise InsiderContractError("signatures must contain objects")
        if signature.get("signature_order") != signature_order or type(
            signature.get("signature_order")
        ) is not int:
            raise InsiderContractError("signature order is invalid")
        for field in ("name", "date"):
            _require_string(
                signature.get(field),
                f"signature {field}",
            )
        signature_path = _require_source_path(
            signature.get("source_path"),
            "signature source_path",
        )
        if signature_path != (
            f"/ownershipDocument/ownerSignature[{signature_order + 1}]"
        ):
            raise InsiderContractError("signature source_path is inconsistent")
        raw_signature = signature.get("raw_signature")
        _validate_raw_element(raw_signature, "signature.raw_signature")
        assert isinstance(raw_signature, dict)
        if raw_signature["local_name"] != "ownerSignature":
            raise InsiderContractError("signature raw_signature lineage is invalid")
        _validate_raw_document_subtree(
            payload,
            signature_path,
            raw_signature,
            "signature",
        )
        _validate_scalar_field_sources(
            signature,
            raw_signature,
            signature_path,
            {
                "name": ("signatureName", None),
                "date": ("signatureDate", None),
            },
            "signature",
        )
    return frozenset(owner_ciks)


def _validate_amendment_metadata(payload: dict[str, Any]) -> None:
    amendment = payload["amendment"]
    expected_keys = {
        "amends_accession_number",
        "match_confidence",
        "original_submission_date",
        "resolution_status",
    }
    if set(amendment) != expected_keys:
        raise InsiderContractError("amendment metadata fields are invalid")
    if amendment["original_submission_date"] != payload["original_submission_date"]:
        raise InsiderContractError(
            "amendment original submission date does not match filing"
        )
    if amendment["amends_accession_number"] is not None:
        raise InsiderContractError(
            "Phase 2 amendment metadata cannot resolve an accession"
        )
    if payload["is_amendment"]:
        if (
            amendment["match_confidence"] != "unresolved"
            or amendment["resolution_status"] != "unresolved_phase2"
        ):
            raise InsiderContractError(
                "Phase 2 amendment metadata must remain unresolved"
            )
    elif (
        amendment["match_confidence"] is not None
        or amendment["resolution_status"] != "not_applicable"
    ):
        raise InsiderContractError(
            "non-amendment metadata must be not applicable"
        )


def _raw_unknown_element_records(payload: dict[str, Any]) -> list[dict[str, object]]:
    raw_document = payload["raw_document"]
    assert isinstance(raw_document, dict)
    try:
        return derive_unknown_element_records(raw_document)
    except ValueError as error:
        raise InsiderContractError("raw unknown element telemetry is invalid") from error


def _validate_unknowns_and_warnings(payload: dict[str, Any]) -> None:
    unknown_fields = {
        "attributes",
        "kind",
        "local_name",
        "namespace_uri",
        "raw_fragment",
        "source_path",
        "text",
    }
    for record in payload["unknown_elements"]:
        if not isinstance(record, dict) or not unknown_fields.issubset(record):
            raise InsiderContractError("unknown element traceability is invalid")
        if record["kind"] not in {"unknown_element", "unknown_attributes"}:
            raise InsiderContractError("unknown element kind is invalid")
        _require_source_path(record["source_path"], "unknown element source_path")
        _require_string(record["local_name"], "unknown element local_name")
        _require_string(
            record["namespace_uri"],
            "unknown element namespace_uri",
            nullable=True,
        )
        if not isinstance(record["attributes"], dict) or any(
            type(key) is not str or type(value) is not str
            for key, value in record["attributes"].items()
        ):
            raise InsiderContractError("unknown element attributes are invalid")
        if type(record["text"]) is not str:
            raise InsiderContractError("unknown element text is invalid")
        _require_string(record["raw_fragment"], "unknown element raw_fragment")

    if payload["unknown_elements"] != _raw_unknown_element_records(payload):
        raise InsiderContractError(
            "unknown element telemetry does not match raw_document"
        )

    warning_fields = {
        "unknown_element": {"local_name"},
        "unknown_attribute": {"local_name"},
        "invalid_boolean": {"raw_value"},
        "unknown_control_code": {"field_name", "raw_code"},
        "unknown_transaction_code": {"raw_code"},
        "unresolved_footnote_reference": {"field_name", "footnote_id"},
    }
    for warning in payload["warnings"]:
        if not isinstance(warning, dict):
            raise InsiderContractError("parser warning must be an object")
        code = warning.get("code")
        if code not in warning_fields:
            raise InsiderContractError("parser warning code is invalid")
        if "source_path" not in warning or not warning_fields[code].issubset(
            warning
        ):
            raise InsiderContractError("parser warning traceability is invalid")
        _require_source_path(warning["source_path"], "warning source_path")
        for field in warning_fields[code]:
            if warning[field] is not None and type(warning[field]) is not str:
                raise InsiderContractError(
                    f"parser warning {field} must be a string or null"
                )


def _validate_required_warnings(payload: dict[str, Any]) -> None:
    actual: set[tuple[object, ...]] = set()
    for warning in payload["warnings"]:
        code = warning["code"]
        if code in {"unknown_element", "unknown_attribute"}:
            actual.add((
                code,
                warning["source_path"],
                warning["local_name"],
            ))
        elif code == "invalid_boolean":
            actual.add((
                code,
                warning["source_path"],
                warning["raw_value"],
            ))
        elif code == "unknown_control_code":
            actual.add((
                code,
                warning["source_path"],
                warning["field_name"],
                warning["raw_code"],
            ))
        elif code == "unknown_transaction_code":
            actual.add((
                code,
                warning["source_path"],
                warning["raw_code"],
            ))
        elif code == "unresolved_footnote_reference":
            actual.add((
                code,
                warning["source_path"],
                warning["field_name"],
                warning["footnote_id"],
            ))

    required: set[tuple[object, ...]] = set()
    for record in _raw_unknown_element_records(payload):
        code = (
            "unknown_element"
            if record["kind"] == "unknown_element"
            else "unknown_attribute"
        )
        required.add((code, record["source_path"], record["local_name"]))
    required.update(
        (
            "unknown_transaction_code",
            row["source_path"],
            row["transaction_code"],
        )
        for row in payload["transactions"]
        if row["requires_review"]
    )
    raw_document = payload["raw_document"]
    root_namespace = raw_document["namespace_uri"]
    pending = [(raw_document, "/ownershipDocument[1]")]
    boolean_element_names = {
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
    }
    while pending:
        element, source_path = pending.pop()
        if (
            element["namespace_uri"] == root_namespace
            and element["local_name"] in boolean_element_names
        ):
            raw_value = _raw_element_text(element)
            if (
                raw_value is not None
                and raw_value.strip().lower()
                not in {"0", "1", "false", "true"}
            ):
                required.add((
                    "invalid_boolean",
                    source_path,
                    raw_value,
                ))
        sibling_counts: dict[str, int] = {}
        children_with_paths: list[tuple[dict[str, Any], str]] = []
        for child in element["children"]:
            local_name = child["local_name"]
            sibling_counts[local_name] = sibling_counts.get(local_name, 0) + 1
            children_with_paths.append((
                child,
                f"{source_path}/{local_name}[{sibling_counts[local_name]}]",
            ))
        pending.extend(reversed(children_with_paths))
    for row in payload["transactions"]:
        for field_name in _CONTROL_CODE_VALUES:
            raw_code = row[field_name]
            if raw_code is None or is_known_section16_control_code(
                field_name,
                raw_code,
            ):
                continue
            source = row["field_sources"][field_name]
            required.add((
                "unknown_control_code",
                source["source_path"],
                field_name,
                raw_code,
            ))
    for row in payload["holdings"]:
        field_name = "direct_indirect_ownership"
        raw_code = row[field_name]
        if raw_code is None or is_known_section16_control_code(
            field_name,
            raw_code,
        ):
            continue
        source = row["field_sources"][field_name]
        required.add((
            "unknown_control_code",
            source["source_path"],
            field_name,
            raw_code,
        ))
    known_footnote_ids = {footnote["id"] for footnote in payload["footnotes"]}
    required.update(
        (
            "unresolved_footnote_reference",
            link["source_path"],
            link["field_name"],
            link["footnote_id"],
        )
        for link in payload["field_footnote_links"]
        if link["footnote_id"] not in known_footnote_ids
    )
    if not required.issubset(actual):
        raise InsiderContractError(
            "private filing is missing required parser warning telemetry"
        )


def canonical_decimal_string(value: object | None) -> str | None:
    """Return an exact, non-exponent decimal string, preserving ``None``."""

    if value is None:
        return None
    if isinstance(value, (bool, float)):
        raise InsiderContractError("exact decimals cannot be booleans or floats")
    text = str(value).strip()
    if not text:
        raise InsiderContractError("decimal value cannot be blank")
    if not isinstance(value, Decimal):
        if len(text) > MAX_DECIMAL_LEXICAL_CHARS:
            raise InsiderContractError("decimal lexical value is too long")
        if not _DECIMAL_LEXICAL_RE.fullmatch(text):
            raise InsiderContractError("decimal lexical value is invalid")
    try:
        decimal_value = Decimal(text)
    except InvalidOperation as error:
        raise InsiderContractError(f"invalid decimal value: {text!r}") from error
    if not decimal_value.is_finite():
        raise InsiderContractError("decimal value must be finite")
    if decimal_value == 0:
        return "0"
    decimal_tuple = decimal_value.as_tuple()
    digit_count = len(decimal_tuple.digits)
    exponent = decimal_tuple.exponent
    sign_count = int(decimal_tuple.sign)
    if exponent >= 0:
        expanded_length = sign_count + digit_count + exponent
    elif digit_count + exponent > 0:
        expanded_length = sign_count + digit_count + 1
    else:
        expanded_length = sign_count + 2 - exponent
    if expanded_length > MAX_CANONICAL_DECIMAL_CHARS:
        raise InsiderContractError("canonical decimal value is too long")
    canonical = format(decimal_value, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    return canonical


def multiply_decimal_strings_exact(left: str, right: str) -> str:
    """Multiply canonical decimal strings without ambient-context rounding."""

    left_decimal = Decimal(left)
    right_decimal = Decimal(right)
    precision = (
        len(left_decimal.as_tuple().digits)
        + len(right_decimal.as_tuple().digits)
    )
    with localcontext() as context:
        context.prec = max(1, precision)
        product = left_decimal * right_decimal
    result = canonical_decimal_string(product)
    assert result is not None
    return result


def absolute_decimal_product(left: str, right: str) -> str:
    """Return the exact absolute product required for transaction values."""

    product = multiply_decimal_strings_exact(left, right)
    return product.removeprefix("-")


def classify_transaction_code(code: object | None) -> dict[str, object]:
    """Classify an exact SEC code while retaining unknown future values."""

    raw_code = str(code).strip() if code is not None else None
    raw_code = raw_code or None
    known = _TRANSACTION_CODES.get(raw_code)
    if known is None:
        return {
            "raw_code": raw_code,
            "label": f"UNKNOWN ({raw_code})" if raw_code else "UNKNOWN",
            "normalized_category": "unknown",
            "is_meaningful_ps": False,
            "requires_review": True,
        }
    label, category, is_meaningful = known
    return {
        "raw_code": raw_code,
        "label": label,
        "normalized_category": category,
        "is_meaningful_ps": is_meaningful,
        "requires_review": False,
    }


def is_known_section16_control_code(field_name: object, code: object) -> bool:
    """Return whether a non-null Section 16 control code is in its known domain."""

    if type(field_name) is not str or type(code) is not str:
        return False
    return code in _CONTROL_CODE_VALUES.get(field_name, frozenset())


def section16_source_row_key(
    accession_number: object,
    entity_type: object,
    source_table: object,
    source_row_index: object,
) -> str:
    """Return the stable identity of one exact source row within a filing."""

    if (
        type(accession_number) is not str
        or not _ACCESSION_RE.fullmatch(accession_number)
        or entity_type not in {"transaction", "holding"}
        or source_table not in {"non_derivative", "derivative"}
        or type(source_row_index) is not int
        or source_row_index < 0
    ):
        raise InsiderContractError("source row identity is invalid")
    assert isinstance(accession_number, str)
    assert isinstance(entity_type, str)
    assert isinstance(source_table, str)
    digest = hashlib.sha256(b"section16-source-row-v1\0")
    digest.update(accession_number.encode("ascii"))
    for component in (
        entity_type,
        source_table,
        str(source_row_index),
    ):
        digest.update(b"\0")
        digest.update(component.encode("ascii"))
    return digest.hexdigest()


def _validate_json_value(value: object, path: str) -> None:
    if value is None or type(value) in {str, bool, int}:
        return
    if isinstance(value, (float, Decimal)):
        raise InsiderContractError(
            f"{path} contains a non-JSON exact numeric value"
        )
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if type(key) is not str:
                raise InsiderContractError(f"{path} contains a non-string key")
            _validate_json_value(item, f"{path}.{key}")
        return
    raise InsiderContractError(
        f"{path} contains unsupported value type {type(value).__name__}"
    )


def _validate_decimal_fields(payload: dict[str, Any]) -> None:
    for collection_name in ("transactions", "holdings"):
        for index, row in enumerate(payload[collection_name]):
            if not isinstance(row, dict):
                continue
            for field_name in _DECIMAL_FIELD_NAMES & row.keys():
                item = row[field_name]
                item_path = (
                    f"filing.{collection_name}[{index}].{field_name}"
                )
                if item is None:
                    continue
                if type(item) is not str:
                    raise InsiderContractError(
                        f"{item_path} must be a canonical decimal string or null"
                    )
                try:
                    canonical = canonical_decimal_string(item)
                except InsiderContractError as error:
                    raise InsiderContractError(
                        f"{item_path} must be a canonical decimal string or null"
                    ) from error
                if canonical != item:
                    raise InsiderContractError(
                        f"{item_path} must be a canonical decimal string or null"
                    )


def _normalized_field_source_value(
    field_name: str,
    raw_value: str | None,
) -> object:
    if field_name in _SOURCE_DECIMAL_FIELD_NAMES:
        return canonical_decimal_string(raw_value)
    if field_name == "equity_swap_involved":
        if raw_value is None:
            return None
        normalized = raw_value.strip().lower()
        if normalized in {"1", "true"}:
            return True
        if normalized in {"0", "false"}:
            return False
        return None
    return raw_value


def _validate_source_rows(payload: dict[str, Any]) -> None:
    accession = payload.get("accession_number")
    owner_group_key = payload.get("owner_group_key")
    for entity_name in ("transactions", "holdings"):
        rows = payload.get(entity_name)
        if not isinstance(rows, list):
            raise InsiderContractError(f"{entity_name} must be a list")
        seen_sources: set[tuple[object, object]] = set()
        seen_row_keys: set[object] = set()
        for row in rows:
            if not isinstance(row, dict):
                raise InsiderContractError(f"{entity_name} rows must be objects")
            row_key = row.get("row_key")
            if type(row_key) is not str or not _SHA256_RE.fullmatch(row_key):
                raise InsiderContractError(
                    f"{entity_name} row_key is invalid"
                )
            source_table = row.get("source_table")
            if source_table not in {"non_derivative", "derivative"}:
                raise InsiderContractError(
                    f"{entity_name} source_table is invalid"
                )
            source_row_index = row.get("source_row_index")
            if type(source_row_index) is not int or source_row_index < 0:
                raise InsiderContractError(
                    f"{entity_name} source_row_index is invalid"
                )
            source_path = _require_source_path(
                row.get("source_path"),
                f"{entity_name} source_path",
            )
            source_prefix = (
                "nonDerivative"
                if source_table == "non_derivative"
                else "derivative"
            )
            row_suffix = (
                "Transaction"
                if entity_name == "transactions"
                else "Holding"
            )
            expected_source_path = (
                f"/ownershipDocument/{source_prefix}Table/"
                f"{source_prefix}{row_suffix}[{source_row_index + 1}]"
            )
            if source_path != expected_source_path:
                raise InsiderContractError(
                    f"{entity_name} row source_path is inconsistent"
                )
            entity_type = entity_name.removesuffix("s")
            if row_key != section16_source_row_key(
                accession,
                entity_type,
                source_table,
                source_row_index,
            ):
                raise InsiderContractError(
                    f"{entity_name} row_key does not match source identity"
                )
            source_identity = (
                source_table,
                source_row_index,
            )
            if source_identity in seen_sources:
                entity_label = entity_name.removesuffix("s")
                raise InsiderContractError(
                    f"duplicate {entity_label} source row"
                )
            seen_sources.add(source_identity)
            if row_key in seen_row_keys:
                raise InsiderContractError(f"duplicate {entity_name} row key")
            seen_row_keys.add(row_key)
            if row.get("accession_number") != accession:
                raise InsiderContractError(
                    f"{entity_name} row accession does not match filing"
                )
            if row.get("owner_group_key") != owner_group_key:
                raise InsiderContractError(
                    f"{entity_name} row owner group does not match filing"
                )
            field_sources = row.get("field_sources")
            if not isinstance(field_sources, dict):
                raise InsiderContractError(
                    f"{entity_name} row field_sources must be an object"
                )
            missing_sources = sorted(
                _REQUIRED_FIELD_SOURCES[(entity_name, source_table)]
                - set(field_sources)
            )
            if missing_sources:
                raise InsiderContractError(
                    f"{entity_name} row is missing field sources: "
                    f"{missing_sources}"
                )
            raw_row = row.get("raw_row")
            _validate_raw_element(
                raw_row,
                f"{entity_name} row raw lineage",
            )
            assert isinstance(raw_row, dict)
            for field_name, source in field_sources.items():
                if type(field_name) is not str or not isinstance(source, dict):
                    raise InsiderContractError(
                        f"{entity_name} row field source is invalid"
                    )
                if field_name not in row:
                    raise InsiderContractError(
                        f"{entity_name} row field source has no normalized field"
                    )
                field_source_path = _require_source_path(
                    source.get("source_path"),
                    f"{entity_name} row field source path",
                )
                expected_suffix = _FIELD_SOURCE_SUFFIXES.get(field_name)
                if (
                    expected_suffix is None
                    or field_source_path != f"{source_path}/{expected_suffix}"
                ):
                    raise InsiderContractError(
                        f"{entity_name} row field source path is inconsistent"
                    )
                if "raw_value" not in source or (
                    source["raw_value"] is not None
                    and type(source["raw_value"]) is not str
                ):
                    raise InsiderContractError(
                        f"{entity_name} row field raw value is invalid"
                    )
                raw_value = source["raw_value"]
                if row[field_name] != _normalized_field_source_value(
                    field_name,
                    raw_value,
                ):
                    raise InsiderContractError(
                        f"{entity_name} row field source does not match "
                        "its normalized value"
                    )
                if raw_value != _raw_field_source_value(
                    raw_row,
                    source_path,
                    field_source_path,
                ):
                    raise InsiderContractError(
                        f"{entity_name} row field source does not match "
                        "its raw lineage"
                    )
                footnote_ids = source.get("footnote_ids")
                if not isinstance(footnote_ids, list) or any(
                    type(footnote_id) is not str
                    for footnote_id in footnote_ids
                ):
                    raise InsiderContractError(
                        f"{entity_name} row field footnotes are invalid"
                    )
                if footnote_ids != _raw_field_footnote_ids(
                    raw_row,
                    source_path,
                    field_source_path,
                ):
                    raise InsiderContractError(
                        f"{entity_name} row field footnotes do not match "
                        "raw lineage"
                    )
            expected_raw_name = f"{source_prefix}{row_suffix}"
            if raw_row["local_name"] != expected_raw_name:
                raise InsiderContractError(
                    f"{entity_name} row raw lineage is inconsistent"
                )
            _validate_raw_document_subtree(
                payload,
                source_path,
                raw_row,
                f"{entity_name} row",
            )


def _validate_row_contract(payload: dict[str, Any]) -> None:
    issuer_cik = payload["issuer"]["cik"]
    for collection_name, required_fields in (
        ("transactions", _TRANSACTION_ROW_FIELDS),
        ("holdings", _HOLDING_ROW_FIELDS),
    ):
        for index, row in enumerate(payload[collection_name]):
            missing = sorted(required_fields - set(row))
            if missing:
                raise InsiderContractError(
                    f"{collection_name} row is missing required fields: {missing}"
                )
            path = f"{collection_name}[{index}]"
            title = _require_string(
                row["security_title_as_filed"],
                f"{path}.security_title_as_filed",
            )
            assert title is not None
            expected_class_key = section16_security_class_key(
                issuer_cik,
                title,
                is_derivative=row["source_table"] == "derivative",
            )
            if row["security_class_key"] != expected_class_key:
                raise InsiderContractError(
                    f"{path}.security_class_key does not match its title"
                )
            for field in (
                "direct_indirect_ownership",
                "nature_of_ownership",
                "normalized_security_id",
                "underlying_security_id",
                "underlying_security_title",
            ):
                _require_string(row[field], f"{path}.{field}", nullable=True)
            underlying_title = row["underlying_security_title"]
            expected_underlying_key = (
                section16_security_class_key(
                    issuer_cik,
                    underlying_title,
                    is_derivative=False,
                )
                if underlying_title is not None
                else None
            )
            if row["underlying_security_class_key"] != expected_underlying_key:
                raise InsiderContractError(
                    f"{path}.underlying_security_class_key is invalid"
                )
            if not isinstance(row["field_footnotes"], dict):
                raise InsiderContractError(
                    f"{path}.field_footnotes must be an object"
                )

            if collection_name == "holdings":
                for field in (
                    "transaction_form_type",
                    "exercise_date",
                    "expiration_date",
                ):
                    _require_string(row[field], f"{path}.{field}", nullable=True)
                continue

            _require_string(row["transaction_date"], f"{path}.transaction_date")
            for field in (
                "acquired_disposed_code",
                "deemed_execution_date",
                "exercise_date",
                "expiration_date",
                "transaction_code",
                "transaction_form_type",
                "transaction_timeliness",
            ):
                _require_string(row[field], f"{path}.{field}", nullable=True)
            classification = classify_transaction_code(row["transaction_code"])
            for field, expected_field in (
                ("transaction_label", "label"),
                ("normalized_category", "normalized_category"),
                ("is_meaningful_ps", "is_meaningful_ps"),
                ("requires_review", "requires_review"),
            ):
                if row[field] != classification[expected_field] or type(
                    row[field]
                ) is not type(classification[expected_field]):
                    raise InsiderContractError(
                        f"{path}.{field} does not match transaction_code"
                    )
            expected_plan_status = (
                "filing_marked"
                if payload["aff10b5_one"] is True
                else "not_marked"
                if payload["aff10b5_one"] is False
                else "unknown"
            )
            if row["plan_status"] != expected_plan_status:
                raise InsiderContractError(
                    f"{path}.plan_status does not match filing"
                )
            if row["value_method"] not in {
                "reported_total",
                "calculated_shares_times_price",
                "unavailable",
            }:
                raise InsiderContractError(f"{path}.value_method is invalid")
            expected_calculated = (
                absolute_decimal_product(row["shares"], row["price_per_share"])
                if row["shares"] is not None and row["price_per_share"] is not None
                else None
            )
            if row["calculated_value"] != expected_calculated:
                raise InsiderContractError(
                    f"{path}.calculated_value does not match shares and price"
                )
            expected_value = row["reported_total_value"] or expected_calculated
            expected_method = (
                "reported_total"
                if row["reported_total_value"] is not None
                else "calculated_shares_times_price"
                if expected_calculated is not None
                else "unavailable"
            )
            if (
                row["transaction_value"] != expected_value
                or row["value_method"] != expected_method
            ):
                raise InsiderContractError(
                    f"{path} transaction value semantics are inconsistent"
                )


def _validate_tristate_fields(payload: dict[str, Any]) -> None:
    containers: list[tuple[str, dict[str, Any]]] = [("filing", payload)]
    containers.extend(
        (f"filing.owners[{index}]", owner)
        for index, owner in enumerate(payload["owners"])
        if isinstance(owner, dict)
    )
    containers.extend(
        (f"filing.transactions[{index}]", row)
        for index, row in enumerate(payload["transactions"])
        if isinstance(row, dict)
    )
    amendment = payload.get("amendment")
    if isinstance(amendment, dict):
        containers.append(("filing.amendment", amendment))
    for path, container in containers:
        for key in _TRISTATE_FIELD_NAMES & container.keys():
            item = container[key]
            if item is not None and type(item) is not bool:
                raise InsiderContractError(
                    f"{path}.{key} must be a tri-state boolean"
                )


def _validate_footnote_links(payload: dict[str, Any]) -> None:
    footnotes = payload.get("footnotes")
    if not isinstance(footnotes, list):
        raise InsiderContractError("footnotes must be a list")
    footnote_ids: list[object] = []
    for footnote_index, footnote in enumerate(footnotes, start=1):
        if (
            not isinstance(footnote, dict)
            or type(footnote.get("id")) is not str
            or not footnote["id"]
            or type(footnote.get("text")) is not str
        ):
            raise InsiderContractError("footnotes must have string IDs")
        source_path = _require_source_path(
            footnote.get("source_path"),
            "footnote source_path",
        )
        expected_path = (
            f"/ownershipDocument/footnotes/footnote[{footnote_index}]"
        )
        if source_path != expected_path:
            raise InsiderContractError("footnote source_path is inconsistent")
        raw_footnote = footnote.get("raw_footnote")
        _validate_raw_element(raw_footnote, "footnote.raw_footnote")
        assert isinstance(raw_footnote, dict)
        if raw_footnote["local_name"] != "footnote":
            raise InsiderContractError("footnote raw lineage is invalid")
        _validate_raw_document_subtree(
            payload,
            source_path,
            raw_footnote,
            "footnote",
        )
        if (
            raw_footnote["attributes"].get("id") != footnote["id"]
            or _raw_element_itertext(raw_footnote) != footnote["text"]
        ):
            raise InsiderContractError(
                "footnote definition does not match raw lineage"
            )
        footnote_ids.append(footnote["id"])
    if len(footnote_ids) != len(set(footnote_ids)):
        raise InsiderContractError("footnote IDs must be unique within a filing")

    expected: list[tuple[object, ...]] = []
    for entity_type, collection_name in (
        ("transaction", "transactions"),
        ("holding", "holdings"),
    ):
        for row in payload[collection_name]:
            references = row.get("field_footnotes")
            if not isinstance(references, dict):
                raise InsiderContractError("row field_footnotes must be an object")
            expected_references = {
                field_name: source["footnote_ids"]
                for field_name, source in row["field_sources"].items()
                if source["footnote_ids"]
            }
            if references != expected_references:
                raise InsiderContractError(
                    "row field footnotes do not match raw lineage"
                )
            for field_name in sorted(references):
                referenced_ids = references[field_name]
                if type(field_name) is not str or not isinstance(
                    referenced_ids,
                    list,
                ):
                    raise InsiderContractError(
                        "row footnote references must be ordered ID lists"
                    )
                if any(type(footnote_id) is not str for footnote_id in referenced_ids):
                    raise InsiderContractError(
                        "row footnote references must contain string IDs"
                    )
                source = row["field_sources"].get(field_name)
                if (
                    not isinstance(source, dict)
                    or source.get("footnote_ids") != referenced_ids
                ):
                    raise InsiderContractError(
                        "row footnote references do not match field sources"
                    )
                for reference_order, footnote_id in enumerate(referenced_ids):
                    expected.append((
                        entity_type,
                        row.get("row_key"),
                        row.get("source_table"),
                        row.get("source_row_index"),
                        field_name,
                        footnote_id,
                        reference_order,
                        source.get("source_path"),
                    ))

    links = payload.get("field_footnote_links")
    if not isinstance(links, list):
        raise InsiderContractError("field_footnote_links must be a list")
    actual: list[tuple[object, ...]] = []
    for link in links:
        if not isinstance(link, dict):
            raise InsiderContractError("field footnote links must be objects")
        actual.append((
            link.get("entity_type"),
            link.get("row_key"),
            link.get("source_table"),
            link.get("source_row_index"),
            link.get("field_name"),
            link.get("footnote_id"),
            link.get("reference_order"),
            link.get("source_path"),
        ))
    if actual != expected:
        raise InsiderContractError(
            "field footnote links do not match row references"
        )


def _validate_private_boundary(payload: dict[str, Any]) -> None:
    privacy = payload.get("privacy")
    if not isinstance(privacy, dict):
        raise InsiderContractError("private filing privacy metadata is required")
    if privacy.get("classification") != "private_normalized_source":
        raise InsiderContractError("private filing classification is invalid")
    if privacy.get("public_projection_allowed") is not False:
        raise InsiderContractError("private filing cannot be public")
    if type(privacy.get("contains_restricted_owner_addresses")) is not bool:
        raise InsiderContractError(
            "private filing address classification must be boolean"
        )
    contains_address_data = any(
        isinstance(owner, dict)
        and (
            owner.get("has_restricted_address_source") is True
            or bool(owner.get("restricted_address"))
            or _raw_tree_contains(
                owner.get("raw_owner"),
                "reportingOwnerAddress",
            )
        )
        for owner in payload["owners"]
    )
    if privacy["contains_restricted_owner_addresses"] != contains_address_data:
        raise InsiderContractError(
            "private filing address classification does not match owner data"
        )


def _raw_tree_contains(value: object, local_name: str) -> bool:
    if not isinstance(value, dict):
        return False
    if value.get("local_name") == local_name:
        return True
    children = value.get("children")
    return isinstance(children, list) and any(
        _raw_tree_contains(child, local_name) for child in children
    )


def validate_insider_filing(payload: object) -> dict[str, Any]:
    """Validate the stable root and JSON-safe types of a private filing."""

    if not isinstance(payload, dict):
        raise InsiderContractError("insider filing must be a JSON object")
    source_document_cik = _validate_root_shape(payload)
    _validate_tristate_fields(payload)
    _validate_filing_header_sources(payload)
    owner_ciks = _validate_owner_and_signature_shapes(payload)
    issuer = payload["issuer"]
    assert isinstance(issuer, dict)
    issuer_cik = issuer["cik"]
    assert isinstance(issuer_cik, str)
    if source_document_cik not in {issuer_cik, *owner_ciks}:
        raise InsiderContractError(
            "source document URL archive CIK does not match issuer CIK or owner CIK"
        )
    _validate_amendment_metadata(payload)
    _validate_unknowns_and_warnings(payload)
    _validate_decimal_fields(payload)
    _validate_source_rows(payload)
    _validate_row_contract(payload)
    _validate_footnote_links(payload)
    _validate_required_warnings(payload)
    _validate_private_boundary(payload)
    _validate_json_value(payload, "filing")
    return payload


def canonical_insider_json_bytes(payload: object) -> bytes:
    """Serialize one validated private filing deterministically as UTF-8."""

    validated = validate_insider_filing(payload)
    rendered = json.dumps(
        validated,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return (rendered + "\n").encode("utf-8")


__all__ = [
    "INSIDER_CONTRACT_VERSION",
    "MAX_NORMALIZED_JSON_BYTES",
    "MAX_RAW_XML_BYTES",
    "MAX_WARNING_RECORDS",
    "InsiderContractError",
    "absolute_decimal_product",
    "canonical_insider_json_bytes",
    "canonical_decimal_string",
    "classify_transaction_code",
    "is_known_section16_control_code",
    "multiply_decimal_strings_exact",
    "section16_source_row_key",
    "validate_insider_filing",
]
