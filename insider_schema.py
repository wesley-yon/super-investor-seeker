"""Shared Section 16 schema-location and unknown-telemetry primitives."""

from __future__ import annotations

from html import escape
from typing import Any

MAX_UNKNOWN_RECORDS = 2_000
OWNERSHIP_NAMESPACE = "http://www.sec.gov/edgar/document/ownership"

_KNOWN_ELEMENT_NAMES = frozenset({
    "aff10b5One", "conversionOrExercisePrice", "dateOfOriginalSubmission",
    "deemedExecutionDate", "derivativeHolding", "derivativeTable",
    "derivativeTransaction", "directOrIndirectOwnership", "documentType",
    "equitySwapInvolved", "exerciseDate", "expirationDate", "footnote",
    "footnoteId", "footnotes", "form3HoldingsReported",
    "form4TransactionsReported", "isDirector", "isOfficer", "isOther",
    "isTenPercentOwner", "issuer", "issuerCik", "issuerName",
    "issuerTradingSymbol", "issuerTradingSymbolForeign", "natureOfOwnership",
    "noSecuritiesOwned", "nonDerivativeHolding", "nonDerivativeTable",
    "nonDerivativeTransaction", "notSubjectToSection16", "officerTitle",
    "otherText", "ownerSignature", "ownershipDocument", "ownershipNature",
    "periodOfReport", "postTransactionAmounts", "remarks", "reportingOwner",
    "reportingOwnerAddress", "reportingOwnerCountry", "reportingOwnerId",
    "reportingOwnerRelationship", "rptOwnerCik", "rptOwnerCity", "rptOwnerName",
    "rptOwnerState", "rptOwnerStateDescription", "rptOwnerStreet1",
    "rptOwnerStreet2", "rptOwnerZipCode", "schemaVersion", "securityTitle",
    "sharesOwnedFollowingTransaction", "signatureDate", "signatureName",
    "transactionAcquiredDisposedCode", "transactionAmounts", "transactionCode",
    "transactionCoding", "transactionDate", "transactionFormType",
    "transactionPricePerShare", "transactionShares", "transactionTimeliness",
    "transactionTotalValue", "underlyingSecurity", "underlyingSecurityShares",
    "underlyingSecurityTitle", "underlyingSecurityValue", "value",
    "valueOwnedFollowingTransaction",
})
_ROOT_CHILD_NAMES = frozenset({
    "aff10b5One", "dateOfOriginalSubmission", "derivativeTable", "documentType",
    "footnotes", "form3HoldingsReported", "form4TransactionsReported", "issuer",
    "noSecuritiesOwned", "nonDerivativeTable", "notSubjectToSection16",
    "ownerSignature", "periodOfReport", "remarks", "reportingOwner", "schemaVersion",
})
_ALLOWED_CHILD_NAMES = {
    "derivativeTable": frozenset({"derivativeHolding", "derivativeTransaction"}),
    "footnotes": frozenset({"footnote"}),
    "issuer": frozenset({
        "issuerCik", "issuerName", "issuerTradingSymbol", "issuerTradingSymbolForeign",
    }),
    "nonDerivativeTable": frozenset({"nonDerivativeHolding", "nonDerivativeTransaction"}),
    "ownerSignature": frozenset({"signatureDate", "signatureName"}),
    "ownershipDocument": _ROOT_CHILD_NAMES,
    "ownershipNature": frozenset({"directOrIndirectOwnership", "natureOfOwnership"}),
    "postTransactionAmounts": frozenset({
        "sharesOwnedFollowingTransaction", "valueOwnedFollowingTransaction",
    }),
    "reportingOwner": frozenset({
        "reportingOwnerAddress", "reportingOwnerCountry", "reportingOwnerId",
        "reportingOwnerRelationship",
    }),
    "reportingOwnerAddress": frozenset({
        "rptOwnerCity", "rptOwnerState", "rptOwnerStateDescription", "rptOwnerStreet1",
        "rptOwnerStreet2", "rptOwnerZipCode",
    }),
    "reportingOwnerId": frozenset({"rptOwnerCik", "rptOwnerName"}),
    "reportingOwnerRelationship": frozenset({
        "isDirector", "isOfficer", "isOther", "isTenPercentOwner", "officerTitle",
        "otherText",
    }),
    "transactionAmounts": frozenset({
        "transactionAcquiredDisposedCode", "transactionPricePerShare", "transactionShares",
        "transactionTotalValue",
    }),
    "transactionCoding": frozenset({
        "equitySwapInvolved", "footnoteId", "transactionCode", "transactionFormType",
    }),
    "underlyingSecurity": frozenset({
        "underlyingSecurityShares", "underlyingSecurityTitle", "underlyingSecurityValue",
    }),
}
_ROW_CHILD_NAMES = frozenset({
    "conversionOrExercisePrice", "deemedExecutionDate", "exerciseDate", "expirationDate",
    "ownershipNature", "postTransactionAmounts", "securityTitle", "transactionAmounts",
    "transactionCoding", "transactionDate", "transactionTimeliness", "underlyingSecurity",
})
_VALUE_WRAPPER_NAMES = frozenset({
    "conversionOrExercisePrice", "deemedExecutionDate", "directOrIndirectOwnership",
    "exerciseDate", "expirationDate", "natureOfOwnership", "securityTitle",
    "sharesOwnedFollowingTransaction", "transactionAcquiredDisposedCode", "transactionCode",
    "transactionDate", "transactionPricePerShare", "transactionShares",
    "transactionTimeliness", "transactionTotalValue", "transactionFormType",
    "underlyingSecurityShares", "underlyingSecurityTitle", "underlyingSecurityValue",
    "valueOwnedFollowingTransaction",
})
_ROW_NAMES = frozenset({
    "derivativeHolding", "derivativeTransaction", "nonDerivativeHolding",
    "nonDerivativeTransaction",
})


def _children(element: dict[str, Any]) -> list[dict[str, Any]]:
    children = element["children"]
    if not isinstance(children, list) or any(not isinstance(child, dict) for child in children):
        raise ValueError("raw XML element children are invalid")
    return children


def _namespace_name(name: str) -> tuple[str | None, str]:
    if name.startswith("{"):
        namespace, separator, local_name = name[1:].partition("}")
        if separator and namespace and local_name:
            return namespace, local_name
    return None, name


def _is_known_element_location(
    element: dict[str, Any],
    *,
    parent: dict[str, Any] | None,
    root: dict[str, Any],
    document_namespace: str | None,
) -> bool:
    local_name = element["local_name"]
    namespace_uri = element["namespace_uri"]
    if (
        not isinstance(local_name, str)
        or local_name not in _KNOWN_ELEMENT_NAMES
        or namespace_uri != document_namespace
    ):
        return False
    if element is root:
        return local_name == "ownershipDocument"
    if parent is None or parent.get("namespace_uri") != document_namespace:
        return False
    parent_name = parent.get("local_name")
    if parent_name in _ROW_NAMES:
        return local_name in _ROW_CHILD_NAMES
    if parent_name in _VALUE_WRAPPER_NAMES:
        return local_name in {"footnoteId", "value"}
    return local_name in _ALLOWED_CHILD_NAMES.get(parent_name, frozenset())


def _raw_itertext(element: dict[str, Any]) -> str:
    text = element["text"]
    if text is not None and not isinstance(text, str):
        raise ValueError("raw XML element text is invalid")
    fragments = [text or ""]
    for child in _children(element):
        fragments.append(_raw_itertext(child))
        tail = child["tail"]
        if tail is not None and not isinstance(tail, str):
            raise ValueError("raw XML element tail is invalid")
        fragments.append(tail or "")
    return "".join(fragments)


def _namespace_map(element: dict[str, Any]) -> dict[str, str]:
    namespaces: set[str] = set()
    attribute_namespaces: set[str] = set()
    pending = [element]
    while pending:
        current = pending.pop()
        namespace_uri = current.get("namespace_uri")
        if namespace_uri is not None:
            if not isinstance(namespace_uri, str):
                raise ValueError("raw XML element namespace is invalid")
            namespaces.add(namespace_uri)
        attributes = current.get("attributes")
        if not isinstance(attributes, dict):
            raise ValueError("raw XML element attributes are invalid")
        for name in attributes:
            if not isinstance(name, str):
                raise ValueError("raw XML attribute name is invalid")
            namespace_uri, _ = _namespace_name(name)
            if namespace_uri is not None:
                namespaces.add(namespace_uri)
                attribute_namespaces.add(namespace_uri)
        pending.extend(_children(current))
    root_namespace = element.get("namespace_uri")
    prefix_namespaces = sorted(
        namespace
        for namespace in namespaces
        if namespace != root_namespace or namespace in attribute_namespaces
    )
    return {
        namespace: f"ns{index}"
        for index, namespace in enumerate(prefix_namespaces)
    }


def canonical_raw_fragment(element: dict[str, Any]) -> str:
    """Serialize a raw XML subtree with deterministic namespace and attribute order."""

    root_namespace = element.get("namespace_uri")
    if root_namespace is not None and not isinstance(root_namespace, str):
        raise ValueError("raw XML element namespace is invalid")
    prefixes = _namespace_map(element)

    def qualified_name(
        namespace_uri: str | None,
        local_name: str,
        *,
        attribute: bool = False,
    ) -> str:
        if namespace_uri is None:
            return local_name
        if namespace_uri == root_namespace and not attribute:
            return local_name
        return f"{prefixes[namespace_uri]}:{local_name}"

    def render(
        current: dict[str, Any],
        *,
        include_tail: bool,
        active_default_namespace: str | None,
    ) -> str:
        namespace_uri = current.get("namespace_uri")
        local_name = current.get("local_name")
        if not isinstance(namespace_uri, (str, type(None))) or not isinstance(local_name, str):
            raise ValueError("raw XML element name is invalid")
        attributes = current.get("attributes")
        if not isinstance(attributes, dict) or any(
            not isinstance(name, str) or not isinstance(value, str)
            for name, value in attributes.items()
        ):
            raise ValueError("raw XML element attributes are invalid")
        rendered_attributes: list[tuple[tuple[str, str], str]] = []
        for name, value in attributes.items():
            attribute_namespace, attribute_name = _namespace_name(name)
            rendered_attributes.append((
                (attribute_namespace or "", attribute_name),
                f' {qualified_name(attribute_namespace, attribute_name, attribute=True)}="{escape(value, quote=True)}"',
            ))
        default_namespace = active_default_namespace
        if namespace_uri in {root_namespace, None}:
            default_namespace = namespace_uri
        if default_namespace != active_default_namespace:
            rendered_attributes.append((
                ("", "xmlns"),
                f' xmlns="{escape(default_namespace or "", quote=True)}"',
            ))
        if current is element:
            rendered_attributes.extend(
                ((namespace, "xmlns"), f' xmlns:{prefix}="{escape(namespace, quote=True)}"')
                for namespace, prefix in sorted(prefixes.items())
            )
        attributes_text = "".join(value for _, value in sorted(rendered_attributes))
        tag = qualified_name(namespace_uri, local_name)
        text = current.get("text")
        if text is not None and not isinstance(text, str):
            raise ValueError("raw XML element text is invalid")
        children = _children(current)
        if text is None and not children:
            rendered = f"<{tag}{attributes_text}/>"
        else:
            rendered = f'<{tag}{attributes_text}>{escape(text or "")}' + "".join(
                render(
                    child,
                    include_tail=True,
                    active_default_namespace=default_namespace,
                )
                for child in children
            ) + f"</{tag}>"
        tail = current.get("tail")
        if tail is not None and not isinstance(tail, str):
            raise ValueError("raw XML element tail is invalid")
        return rendered + (escape(tail or "") if include_tail else "")

    return render(
        element,
        include_tail=False,
        active_default_namespace=None,
    )


def derive_unknown_element_records(raw_root: dict[str, Any]) -> list[dict[str, object]]:
    """Derive ordered unknown Section 16 XML telemetry from a raw document tree."""

    root_name = raw_root.get("local_name")
    document_namespace = raw_root.get("namespace_uri")
    if not isinstance(root_name, str) or not isinstance(document_namespace, (str, type(None))):
        raise ValueError("raw XML root is invalid")
    records: list[dict[str, object]] = []
    pending: list[tuple[dict[str, Any], dict[str, Any] | None, bool, str]] = [
        (raw_root, None, False, f"/{root_name}[1]")
    ]
    while pending:
        element, parent, inside_unknown_subtree, path = pending.pop()
        local_name = element.get("local_name")
        namespace_uri = element.get("namespace_uri")
        attributes = element.get("attributes")
        if not isinstance(local_name, str) or not isinstance(namespace_uri, (str, type(None))):
            raise ValueError("raw XML element name is invalid")
        if not isinstance(attributes, dict) or any(
            not isinstance(name, str) or not isinstance(value, str)
            for name, value in attributes.items()
        ):
            raise ValueError("raw XML element attributes are invalid")
        children = _children(element)
        is_unknown = not _is_known_element_location(
            element,
            parent=parent,
            root=raw_root,
            document_namespace=document_namespace,
        )
        captures_unknown_attributes = False
        if is_unknown and not inside_unknown_subtree:
            if len(records) >= MAX_UNKNOWN_RECORDS:
                raise ValueError("ownership XML contains too many unknown elements")
            records.append({
                "kind": "unknown_element",
                "source_path": path,
                "namespace_uri": namespace_uri,
                "local_name": local_name,
                "attributes": dict(sorted(attributes.items())),
                "text": _raw_itertext(element),
                "raw_fragment": canonical_raw_fragment(element),
            })
        elif not inside_unknown_subtree:
            allowed_attributes = {"id"} if local_name in {"footnote", "footnoteId"} else set()
            unknown_attributes = {
                name: value
                for name, value in sorted(attributes.items())
                if not (
                    _namespace_name(name)[0] is None
                    and _namespace_name(name)[1] in allowed_attributes
                )
            }
            if unknown_attributes:
                if len(records) >= MAX_UNKNOWN_RECORDS:
                    raise ValueError("ownership XML contains too many unknown elements")
                captures_unknown_attributes = True
                records.append({
                    "kind": "unknown_attributes",
                    "source_path": path,
                    "namespace_uri": namespace_uri,
                    "local_name": local_name,
                    "attributes": unknown_attributes,
                    "text": _raw_itertext(element),
                    "raw_fragment": canonical_raw_fragment(element),
                })
        child_inside_unknown = inside_unknown_subtree or is_unknown or captures_unknown_attributes
        counts: dict[str, int] = {}
        children_with_paths: list[tuple[dict[str, Any], dict[str, Any], bool, str]] = []
        for child in children:
            child_name = child.get("local_name")
            if not isinstance(child_name, str):
                raise ValueError("raw XML child name is invalid")
            counts[child_name] = counts.get(child_name, 0) + 1
            children_with_paths.append((
                child,
                element,
                child_inside_unknown,
                f"{path}/{child_name}[{counts[child_name]}]",
            ))
        pending.extend(reversed(children_with_paths))
    return records


__all__ = [
    "MAX_UNKNOWN_RECORDS",
    "OWNERSHIP_NAMESPACE",
    "canonical_raw_fragment",
    "derive_unknown_element_records",
]
