"""Pure fixed-scope ServiceNow publication-policy candidate validation."""

from __future__ import annotations

import hashlib
import re
import unicodedata

from insider_publication import (
    InsiderPublicationError,
    validate_public_security_metadata,
)
from insider_storage import (
    MAX_INSIDER_STATE_COLLECTION,
    InsiderStorageError,
    canonical_insider_state_json_bytes,
    validate_issuer_state_payload,
    validate_publication_policy_payload,
)
from security_identity import (
    is_canonical_security_identifier,
    parse_stock_lookup_id,
    stock_lookup_id,
)


SERVICENOW_CIK = "0001373715"
_PUBLIC_CIK_TOKEN_RE = re.compile(r"(?<![0-9])[0-9]{10}(?![0-9])")
_PUBLIC_PRIVATE_CORRELATOR_RE = re.compile(
    r"(?<![A-F0-9])[A-F0-9]{64}(?![A-F0-9])",
    re.IGNORECASE,
)


class ServiceNowPublicationPolicyError(ValueError):
    """Raised when a ServiceNow policy candidate cannot be approved safely."""


def _fail(label: str) -> None:
    raise ServiceNowPublicationPolicyError(label)


def _validated_mapping(metadata: object) -> dict[str, object]:
    try:
        validated = validate_public_security_metadata(metadata)
    except InsiderPublicationError as error:
        raise ServiceNowPublicationPolicyError("public security metadata") from error
    stock_id = validated["stockId"]
    assert isinstance(stock_id, str)
    base, instrument_type = parse_stock_lookup_id(stock_id)
    if (
        not is_canonical_security_identifier(base)
        or stock_lookup_id(base, instrument_type) != stock_id
    ):
        _fail("public security metadata")
    for value in validated.values():
        if type(value) is str and (
            value != unicodedata.normalize("NFKC", value)
            or any(unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in value)
            or _PUBLIC_CIK_TOKEN_RE.search(value) is not None
            or _PUBLIC_PRIVATE_CORRELATOR_RE.search(value) is not None
        ):
            _fail("public security metadata")
    return validated


def _validate_servicenow_policy_candidate(policy: object) -> dict[str, object]:
    try:
        validated = validate_publication_policy_payload(policy)
    except InsiderStorageError as error:
        raise ServiceNowPublicationPolicyError("publication policy") from error
    issuers = validated["issuers"]
    if not isinstance(issuers, list) or len(issuers) != 1:
        _fail("publication policy")
    assert isinstance(issuers, list)
    issuer = issuers[0]
    if type(issuer) is not dict or issuer.get("issuer_cik") != SERVICENOW_CIK:
        _fail("publication policy")
    assert isinstance(issuer, dict)
    security_mappings = issuer.get("security_mappings")
    if not isinstance(security_mappings, dict) or not security_mappings:
        _fail("publication policy")
    assert isinstance(security_mappings, dict)

    mapped_stock_ids: set[str] = set()
    mappings: dict[str, object] = {}
    for class_key in sorted(security_mappings):
        metadata = _validated_mapping(security_mappings[class_key])
        stock_id = metadata["stockId"]
        assert isinstance(stock_id, str)
        if stock_id in mapped_stock_ids:
            _fail("public security identity")
        mapped_stock_ids.add(stock_id)
        mappings[class_key] = metadata
    return {
        "contract_version": 1,
        "issuers": [
            {
                "issuer_cik": SERVICENOW_CIK,
                "security_mappings": mappings,
            }
        ],
    }


def build_servicenow_publication_policy(
    *,
    issuer_state: object,
    mapping_spec: object,
    public_index: object,
) -> dict[str, object]:
    """Return one exact, complete ServiceNow policy or raise before I/O."""

    try:
        state = validate_issuer_state_payload(issuer_state)
    except InsiderStorageError as error:
        raise ServiceNowPublicationPolicyError("issuer state") from error
    if state["issuer_cik"] != SERVICENOW_CIK:
        _fail("issuer CIK")
    if state["unresolved_ambiguities"]:
        _fail("issuer unresolved ambiguities")

    accessions = state["accessions"]
    class_rows = state["security_classes"]
    if not isinstance(accessions, list) or not accessions:
        _fail("issuer accessions")
    if not isinstance(class_rows, list) or not class_rows:
        _fail("issuer security classes")
    assert isinstance(class_rows, list)
    class_keys: list[str] = []
    for row in class_rows:
        if type(row) is not dict or type(row.get("security_class_key")) is not str:
            _fail("issuer security classes")
        class_keys.append(row["security_class_key"])

    if (
        not isinstance(mapping_spec, dict)
        or not mapping_spec
        or len(mapping_spec) > MAX_INSIDER_STATE_COLLECTION
        or set(mapping_spec) != set(class_keys)
    ):
        _fail("security mapping keys")
    if not isinstance(public_index, dict):
        _fail("public index")

    mappings: dict[str, object] = {}
    for class_key in sorted(class_keys):
        metadata = _validated_mapping(mapping_spec[class_key])
        stock_id = metadata["stockId"]
        assert isinstance(stock_id, str)
        if stock_id not in public_index:
            _fail("public security identity")
        indexed_metadata = _validated_mapping(public_index[stock_id])
        if indexed_metadata != metadata:
            _fail("public security identity")
        mappings[class_key] = metadata

    candidate = {
        "contract_version": 1,
        "issuers": [
            {
                "issuer_cik": SERVICENOW_CIK,
                "security_mappings": mappings,
            }
        ],
    }
    return _validate_servicenow_policy_candidate(candidate)


def publication_policy_sha256(policy: object) -> str:
    """Validate and hash one exact fixed-scope policy candidate."""

    validated = _validate_servicenow_policy_candidate(policy)
    return hashlib.sha256(canonical_insider_state_json_bytes(validated)).hexdigest()


__all__ = [
    "SERVICENOW_CIK",
    "ServiceNowPublicationPolicyError",
    "build_servicenow_publication_policy",
    "publication_policy_sha256",
]
