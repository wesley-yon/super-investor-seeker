"""Canonical exact-decimal metrics for bounded public insider projections.

The browser may format or lightly filter published rows, but this module is the
financial source of truth.  It accepts a deliberately small *private in-memory*
calculation contract from :mod:`insider_publication`.  Private grouping and row
lineage are stripped before any result crosses the public boundary.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from functools import cmp_to_key
from typing import Iterable, Mapping
from urllib.parse import urlsplit


MAX_PUBLIC_TRANSACTION_ROWS = 5_000
MAX_PUBLIC_HOLDING_ROWS = 5_000
MAX_SEARCH_CHARS = 100
_CURSOR_RE = re.compile(r"v1\.([0-9a-f]{16})\.([0-9]+)")
_ACCESSION_RE = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_DECIMAL_RE = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?")
_ALLOWED_RANGES = frozenset({"6m", "1y", "2y", "5y", "all"})
_ALLOWED_TRANSACTION_SCOPES = frozenset({"ps", "all"})
_ALLOWED_OWNER_SCOPES = frozenset({"all", "officers-directors", "ten-percent"})
_ALLOWED_PLAN_FILTERS = frozenset({"all", "10b5-1", "not-10b5-1", "unknown"})
_ALLOWED_SORTS = frozenset(
    {"tradeDate", "value", "shares", "holdingsAfter", "percentChange"}
)
_ALLOWED_ORDERS = frozenset({"asc", "desc"})
_ALLOWED_LIMITS = frozenset({25, 50, 100})
_ALLOWED_PLAN_STATES = frozenset({"filing_marked", "not_marked", "unknown"})
_ROLE_ORDER = {
    "Officer": 0,
    "Director": 1,
    "TenPercentOwner": 2,
    "Other": 3,
}
_ALLOWED_ROLES = frozenset(_ROLE_ORDER)
_ALLOWED_OWNERSHIP_FILTERS = frozenset({"all", "direct", "indirect"})
_ALLOWED_FORMS = frozenset({"all", "3", "3/A", "4", "4/A", "5", "5/A"})
_ALLOWED_QUERY_KEYS = frozenset(
    {
        "amendedOnly",
        "cursor",
        "end",
        "formType",
        "includeTenPercentOwners",
        "lateOnly",
        "limit",
        "minimumValue",
        "order",
        "ownerScope",
        "ownership",
        "plan",
        "range",
        "search",
        "securityScope",
        "sort",
        "start",
        "transactionScope",
    }
)


class InsiderMetricsError(ValueError):
    """Raised when a public row, query, or canonical metric is invalid."""


@dataclass(frozen=True, slots=True)
class _Query:
    range: str
    transaction_scope: str
    owner_scope: str
    include_ten_percent_owners: bool
    plan: str
    security_scope: str
    start: str | None
    end: str
    search: str
    sort: str
    order: str
    limit: int
    cursor: str | None
    minimum_value: str | None
    form_type: str
    late_only: bool
    amended_only: bool
    ownership: str

    def public_dict(self) -> dict[str, object]:
        return {
            "range": self.range,
            "transactionScope": self.transaction_scope,
            "ownerScope": self.owner_scope,
            "includeTenPercentOwners": self.include_ten_percent_owners,
            "plan": self.plan,
            "securityScope": self.security_scope,
            "start": self.start,
            "end": self.end,
            "search": self.search,
            "sort": self.sort,
            "order": self.order,
            "limit": self.limit,
            "minimumValue": self.minimum_value,
            "formType": self.form_type,
            "lateOnly": self.late_only,
            "amendedOnly": self.amended_only,
            "ownership": self.ownership,
        }


def _fail(label: str) -> InsiderMetricsError:
    return InsiderMetricsError(f"insider metric input is invalid: {label}")


def _safe_string(
    value: object,
    label: str,
    *,
    nullable: bool = False,
    maximum: int = 512,
) -> str | None:
    if value is None and nullable:
        return None
    if (
        type(value) is not str
        or not value
        or len(value) > maximum
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise _fail(label)
    return value


def _iso_date(value: object, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or not _DATE_RE.fullmatch(value):
        raise _fail(label)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise _fail(label) from error
    if parsed.isoformat() != value:
        raise _fail(label)
    return value


def _timestamp(value: object, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or not value or len(value) > 40:
        raise _fail(label)
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        raise _fail(label) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _fail(label)
    return value


def _decimal(
    value: object,
    label: str,
    *,
    nullable: bool = False,
    nonnegative: bool = True,
) -> Decimal | None:
    if value is None and nullable:
        return None
    if type(value) is not str or not _DECIMAL_RE.fullmatch(value):
        raise _fail(label)
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise _fail(label) from error
    if not parsed.is_finite() or (nonnegative and parsed < 0):
        raise _fail(label)
    if _decimal_text(parsed) != value:
        raise _fail(f"{label} canonical decimal")
    return parsed


def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise _fail("derived decimal")
    if value == 0:
        return "0"
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _ratio(numerator: Decimal, denominator: Decimal) -> str:
    if denominator <= 0:
        raise _fail("ratio denominator")
    with localcontext() as context:
        context.prec = 80
        value = numerator / denominator
        quantum = Decimal("0.0000000001")
        return _decimal_text(value.quantize(quantum))


def _shift_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 + months
    year, month_zero = divmod(month_index, 12)
    month = month_zero + 1
    if month == 12:
        first_next = date(year + 1, 1, 1)
    else:
        first_next = date(year, month + 1, 1)
    last_day = (first_next - date.resolution).day
    return date(year, month, min(value.day, last_day))


def _parse_query(
    query: Mapping[str, object] | None,
    *,
    security_id: str,
    as_of: str,
) -> _Query:
    raw = {} if query is None else dict(query)
    unknown = sorted(set(raw) - _ALLOWED_QUERY_KEYS)
    if unknown:
        raise _fail(f"query fields {unknown}")
    _timestamp(as_of, "asOf")
    as_of_text = as_of[:-1] + "+00:00" if as_of.endswith("Z") else as_of
    end_date = datetime.fromisoformat(as_of_text).astimezone(timezone.utc).date()

    range_value = raw.get("range", "1y")
    transaction_scope = raw.get("transactionScope", "ps")
    owner_scope = raw.get("ownerScope", "all")
    plan = raw.get("plan", "all")
    sort = raw.get("sort", "tradeDate")
    order = raw.get("order", "desc")
    form_type = raw.get("formType", "all")
    ownership = raw.get("ownership", "all")
    if range_value not in _ALLOWED_RANGES:
        raise _fail("range")
    if transaction_scope not in _ALLOWED_TRANSACTION_SCOPES:
        raise _fail("transactionScope")
    if owner_scope not in _ALLOWED_OWNER_SCOPES:
        raise _fail("ownerScope")
    if plan not in _ALLOWED_PLAN_FILTERS:
        raise _fail("plan")
    if sort not in _ALLOWED_SORTS:
        raise _fail("sort")
    if order not in _ALLOWED_ORDERS:
        raise _fail("order")
    if form_type not in _ALLOWED_FORMS:
        raise _fail("formType")
    if ownership not in _ALLOWED_OWNERSHIP_FILTERS:
        raise _fail("ownership")

    include_ten_percent = raw.get("includeTenPercentOwners", True)
    late_only = raw.get("lateOnly", False)
    amended_only = raw.get("amendedOnly", False)
    for label, value in (
        ("includeTenPercentOwners", include_ten_percent),
        ("lateOnly", late_only),
        ("amendedOnly", amended_only),
    ):
        if type(value) is not bool:
            raise _fail(label)

    limit = raw.get("limit", 25)
    if type(limit) is not int or type(limit) is bool or limit not in _ALLOWED_LIMITS:
        raise _fail("limit")

    search = raw.get("search", "")
    if (
        type(search) is not str
        or len(search) > MAX_SEARCH_CHARS
        or any(ord(character) < 32 or ord(character) == 127 for character in search)
    ):
        raise _fail("search")
    search = search.strip()

    explicit_start = raw.get("start")
    explicit_end = raw.get("end")
    if explicit_start is not None:
        explicit_start = _iso_date(explicit_start, "start")
    if explicit_end is not None:
        explicit_end = _iso_date(explicit_end, "end")
    canonical_end = explicit_end or end_date.isoformat()
    if explicit_start is not None:
        canonical_start = explicit_start
    elif range_value == "all":
        canonical_start = None
    else:
        months = {"6m": -6, "1y": -12, "2y": -24, "5y": -60}[range_value]
        canonical_start = _shift_months(
            date.fromisoformat(canonical_end), months
        ).isoformat()
    if canonical_start is not None and canonical_start > canonical_end:
        raise _fail("date range")

    security_scope = raw.get("securityScope", security_id)
    if security_scope != security_id:
        raise _fail("securityScope")

    minimum_value = raw.get("minimumValue")
    if minimum_value is not None:
        minimum = _decimal(minimum_value, "minimumValue")
        assert minimum is not None
        minimum_value = _decimal_text(minimum)

    cursor = raw.get("cursor")
    if cursor is not None and (
        type(cursor) is not str or _CURSOR_RE.fullmatch(cursor) is None
    ):
        raise _fail("cursor")

    return _Query(
        range=range_value,
        transaction_scope=transaction_scope,
        owner_scope=owner_scope,
        include_ten_percent_owners=include_ten_percent,
        plan=plan,
        security_scope=security_id,
        start=canonical_start,
        end=canonical_end,
        search=search,
        sort=sort,
        order=order,
        limit=limit,
        cursor=cursor,
        minimum_value=minimum_value,
        form_type=form_type,
        late_only=late_only,
        amended_only=amended_only,
        ownership=ownership,
    )


def _validate_sec_url(value: object) -> str:
    url = _safe_string(value, "secDocumentUrl", maximum=512)
    assert isinstance(url, str)
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise _fail("secDocumentUrl") from error
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.sec.gov"
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/Archives/edgar/data/")
    ):
        raise _fail("secDocumentUrl")
    return url


def _validate_owner(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _fail("ownerGroup")
    required = {
        "displayName",
        "isJoint",
        "ownerCount",
        "primaryTitle",
        "roles",
    }
    if set(value) != required:
        raise _fail("ownerGroup fields")
    display_name = _safe_string(value["displayName"], "owner displayName", maximum=256)
    title = _safe_string(
        value["primaryTitle"],
        "owner primaryTitle",
        nullable=True,
        maximum=256,
    )
    owner_count = value["ownerCount"]
    is_joint = value["isJoint"]
    roles = value["roles"]
    if (
        type(owner_count) is not int
        or type(owner_count) is bool
        or not 1 <= owner_count <= 100
        or type(is_joint) is not bool
        or is_joint != (owner_count > 1)
        or not isinstance(roles, list)
        or not roles
        or len(roles) > len(_ALLOWED_ROLES)
        or any(type(role) is not str or role not in _ALLOWED_ROLES for role in roles)
        or len(roles) != len(set(roles))
    ):
        raise _fail("ownerGroup values")
    return {
        "displayName": display_name,
        "ownerCount": owner_count,
        "roles": sorted(roles, key=_ROLE_ORDER.__getitem__),
        "primaryTitle": title,
        "isJoint": is_joint,
    }


def _owner_identity(record: Mapping[str, object]) -> str:
    """Return the validated private owner-group identity used only in memory."""

    owner_group_key = record.get("privateOwnerGroupKey")
    if (
        type(owner_group_key) is not str
        or _SHA256_RE.fullmatch(owner_group_key) is None
    ):
        raise _fail("privateOwnerGroupKey")
    return owner_group_key


def _validate_transaction_row(value: object, *, security_id: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _fail("transaction row")
    required = {
        "acceptedAt",
        "accessionNumber",
        "acquiredDisposedCode",
        "deemedExecutionDate",
        "directIndirectOwnership",
        "filingDate",
        "privateFootnoteIds",
        "formType",
        "isAmended",
        "isSuperseded",
        "normalizedCategory",
        "ownerGroup",
        "privateOwnerGroupKey",
        "planStatus",
        "postTransactionShares",
        "priceIsWeightedAverage",
        "pricePerShare",
        "privateRowKey",
        "secDocumentUrl",
        "securityId",
        "shares",
        "privateSourceRowIndex",
        "privateSourceTable",
        "transactionCode",
        "transactionDate",
        "transactionLabel",
        "transactionTimeliness",
        "value",
        "valueMethod",
    }
    optional = {"privateDisplayGroupKeyOverride"}
    if set(value) - required - optional or required - set(value):
        raise _fail("transaction row fields")
    row = dict(value)
    display_group_override = row.get("privateDisplayGroupKeyOverride")
    if display_group_override is not None and (
        type(display_group_override) is not str
        or _SHA256_RE.fullmatch(display_group_override) is None
    ):
        raise _fail("privateDisplayGroupKeyOverride")
    row_key = _safe_string(row["privateRowKey"], "privateRowKey", maximum=256)
    accession = row["accessionNumber"]
    if type(accession) is not str or not _ACCESSION_RE.fullmatch(accession):
        raise _fail("accessionNumber")
    if row["securityId"] != security_id:
        raise _fail("securityId")
    _safe_string(row["securityId"], "securityId", maximum=128)
    owner = _validate_owner(row["ownerGroup"])
    owner_group_key = row["privateOwnerGroupKey"]
    if (
        type(owner_group_key) is not str
        or _SHA256_RE.fullmatch(owner_group_key) is None
    ):
        raise _fail("privateOwnerGroupKey")
    source_table = row["privateSourceTable"]
    if source_table not in {"non_derivative", "derivative"}:
        raise _fail("privateSourceTable")
    source_index = row["privateSourceRowIndex"]
    if type(source_index) is not int or type(source_index) is bool or source_index < 0:
        raise _fail("privateSourceRowIndex")
    transaction_date = _iso_date(row["transactionDate"], "transactionDate")
    deemed = _iso_date(row["deemedExecutionDate"], "deemedExecutionDate", nullable=True)
    transaction_code = _safe_string(
        row["transactionCode"],
        "transactionCode",
        nullable=True,
        maximum=8,
    )
    _safe_string(row["transactionLabel"], "transactionLabel", maximum=128)
    _safe_string(row["normalizedCategory"], "normalizedCategory", maximum=128)
    acquired_disposed = row["acquiredDisposedCode"]
    if acquired_disposed not in {"A", "D", None}:
        raise _fail("acquiredDisposedCode")
    shares = _decimal(row["shares"], "shares", nullable=True)
    price = _decimal(row["pricePerShare"], "pricePerShare", nullable=True)
    reported_value = _decimal(row["value"], "value", nullable=True)
    post_shares = _decimal(
        row["postTransactionShares"],
        "postTransactionShares",
        nullable=True,
    )
    if type(row["priceIsWeightedAverage"]) is not bool:
        raise _fail("priceIsWeightedAverage")
    value_method = row["valueMethod"]
    if value_method not in {
        "reported_total",
        "calculated_shares_times_price",
        "unavailable",
    }:
        raise _fail("valueMethod")
    if (reported_value is None) != (value_method == "unavailable"):
        raise _fail("valueMethod availability")
    direct_indirect = row["directIndirectOwnership"]
    if direct_indirect not in {"D", "I", None}:
        raise _fail("directIndirectOwnership")
    plan_status = row["planStatus"]
    if plan_status not in _ALLOWED_PLAN_STATES:
        raise _fail("planStatus")
    timeliness = _safe_string(
        row["transactionTimeliness"],
        "transactionTimeliness",
        nullable=True,
        maximum=16,
    )
    for label in ("isAmended", "isSuperseded"):
        if type(row[label]) is not bool:
            raise _fail(label)
    if row["formType"] not in _ALLOWED_FORMS - {"all"}:
        raise _fail("formType")
    filing_date = _iso_date(row["filingDate"], "filingDate")
    accepted = _timestamp(row["acceptedAt"], "acceptedAt", nullable=True)
    sec_url = _validate_sec_url(row["secDocumentUrl"])
    footnote_ids = row["privateFootnoteIds"]
    if (
        not isinstance(footnote_ids, list)
        or len(footnote_ids) > 100
        or any(
            type(item) is not str or not item or len(item) > 128 or item != item.strip()
            for item in footnote_ids
        )
        or len(footnote_ids) != len(set(footnote_ids))
    ):
        raise _fail("privateFootnoteIds")

    return {
        **row,
        "privateRowKey": row_key,
        "ownerGroup": owner,
        "transactionDate": transaction_date,
        "deemedExecutionDate": deemed,
        "transactionCode": transaction_code,
        "shares": None if shares is None else _decimal_text(shares),
        "pricePerShare": None if price is None else _decimal_text(price),
        "value": None if reported_value is None else _decimal_text(reported_value),
        "postTransactionShares": (
            None if post_shares is None else _decimal_text(post_shares)
        ),
        "transactionTimeliness": timeliness,
        "filingDate": filing_date,
        "acceptedAt": accepted,
        "secDocumentUrl": sec_url,
        "privateFootnoteIds": sorted(footnote_ids),
    }


def _bounded_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    security_id: str,
) -> list[dict[str, object]]:
    if isinstance(rows, (str, bytes, Mapping)):
        raise _fail("transaction rows")
    try:
        iterator = iter(rows)
    except TypeError as error:
        raise _fail("transaction rows") from error
    result: list[dict[str, object]] = []
    row_keys: set[str] = set()
    for raw in iterator:
        if len(result) >= MAX_PUBLIC_TRANSACTION_ROWS:
            raise _fail("transaction row limit")
        row = _validate_transaction_row(raw, security_id=security_id)
        row_key = row["privateRowKey"]
        assert isinstance(row_key, str)
        if row_key in row_keys:
            raise _fail("duplicate privateRowKey")
        row_keys.add(row_key)
        result.append(row)
    return result


def _display_group_material(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "accessionNumber": row["accessionNumber"],
        "privateOwnerGroupKey": row["privateOwnerGroupKey"],
        "transactionDate": row["transactionDate"],
        "securityId": row["securityId"],
        "privateSourceTable": row["privateSourceTable"],
        "transactionCode": row["transactionCode"],
        "acquiredDisposedCode": row["acquiredDisposedCode"],
        "directIndirectOwnership": row["directIndirectOwnership"],
        "pricePerShare": row["pricePerShare"],
        "privateFootnoteIds": row["privateFootnoteIds"],
    }


def _display_group_key(row: Mapping[str, object]) -> str:
    override = row.get("privateDisplayGroupKeyOverride")
    if override is not None:
        assert isinstance(override, str)
        return override
    payload = json.dumps(
        _display_group_material(row),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(b"sis-insider-display-group-v1\0" + payload).hexdigest()


def _sum_optional(values: list[str | None], *, require_all: bool) -> str | None:
    parsed = [Decimal(value) for value in values if value is not None]
    if not parsed or (require_all and len(parsed) != len(values)):
        return None
    return _decimal_text(sum(parsed, Decimal(0)))


def _percent_change(group_rows: list[dict[str, object]]) -> tuple[str, str | None]:
    if len(group_rows) != 1:
        return "unavailable", None
    row = group_rows[0]
    if row["transactionCode"] not in {"P", "S"}:
        return "unavailable", None
    shares_text = row["shares"]
    post_text = row["postTransactionShares"]
    acquired_disposed = row["acquiredDisposedCode"]
    if (
        not isinstance(shares_text, str)
        or not isinstance(post_text, str)
        or acquired_disposed not in {"A", "D"}
        or row["directIndirectOwnership"] not in {"D", "I"}
    ):
        return "unavailable", None
    shares = Decimal(shares_text)
    post = Decimal(post_text)
    prior = post - shares if acquired_disposed == "A" else post + shares
    if prior < 0:
        return "unavailable", None
    if prior == 0:
        if acquired_disposed == "A" and shares > 0 and post > 0:
            return "new", None
        return "unavailable", None
    numerator = shares if acquired_disposed == "A" else -shares
    return "known", _ratio(numerator, prior)


def _group_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    buckets: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        if row["isSuperseded"]:
            continue
        buckets.setdefault(_display_group_key(row), []).append(row)

    groups: list[dict[str, object]] = []
    for key in sorted(buckets):
        bucket = sorted(
            buckets[key],
            key=lambda row: (
                row["privateSourceTable"],
                row["privateSourceRowIndex"],
                row["privateRowKey"],
            ),
        )
        first = bucket[0]
        invariant_fields = (
            "acceptedAt",
            "accessionNumber",
            "acquiredDisposedCode",
            "directIndirectOwnership",
            "filingDate",
            "formType",
            "isAmended",
            "normalizedCategory",
            "ownerGroup",
            "privateOwnerGroupKey",
            "planStatus",
            "pricePerShare",
            "secDocumentUrl",
            "securityId",
            "privateSourceTable",
            "transactionCode",
            "transactionDate",
            "transactionLabel",
            "transactionTimeliness",
            "valueMethod",
        )
        if any(
            any(row[field] != first[field] for field in invariant_fields)
            for row in bucket[1:]
        ):
            raise _fail("display group invariants")
        percent_state, percent_change = _percent_change(bucket)
        values = [row["value"] for row in bucket]
        shares = [row["shares"] for row in bucket]
        post_values = {row["postTransactionShares"] for row in bucket}
        group_value = _sum_optional(values, require_all=False)
        all_values_known = all(value is not None for value in values)
        group = {
            **{field: first[field] for field in invariant_fields},
            "_privateDisplayGroupKey": key,
            "_privateSourceRowKeys": [row["privateRowKey"] for row in bucket],
            "transactionLegCount": len(bucket),
            "shares": _sum_optional(shares, require_all=True),
            "value": group_value,
            "valueCoverage": (
                "complete"
                if all_values_known
                else "partial"
                if group_value is not None
                else "unavailable"
            ),
            "postTransactionShares": (
                next(iter(post_values)) if len(post_values) == 1 else None
            ),
            "priceIsWeightedAverage": any(
                row["priceIsWeightedAverage"] for row in bucket
            ),
            "percentChange": percent_change,
            "percentChangeState": percent_state,
            "isSuperseded": False,
            "privateFootnoteIds": first["privateFootnoteIds"],
        }
        groups.append(group)
    return groups


def _roles(group: Mapping[str, object]) -> set[str]:
    owner = group["ownerGroup"]
    assert isinstance(owner, dict)
    roles = owner["roles"]
    assert isinstance(roles, list)
    return set(roles)


def _matches_query(group: Mapping[str, object], query: _Query) -> bool:
    transaction_date = group["transactionDate"]
    assert isinstance(transaction_date, str)
    if query.start is not None and transaction_date < query.start:
        return False
    if transaction_date > query.end:
        return False
    if query.transaction_scope == "ps" and group["transactionCode"] not in {"P", "S"}:
        return False
    roles = _roles(group)
    if query.owner_scope == "officers-directors" and not roles & {
        "Officer",
        "Director",
    }:
        return False
    if query.owner_scope == "ten-percent" and "TenPercentOwner" not in roles:
        return False
    if (
        not query.include_ten_percent_owners
        and "TenPercentOwner" in roles
        and not roles & {"Officer", "Director"}
    ):
        return False
    if query.plan == "10b5-1" and group["planStatus"] != "filing_marked":
        return False
    if query.plan == "not-10b5-1" and group["planStatus"] != "not_marked":
        return False
    if query.plan == "unknown" and group["planStatus"] != "unknown":
        return False
    if query.form_type != "all" and group["formType"] != query.form_type:
        return False
    if query.late_only and group["transactionTimeliness"] != "L":
        return False
    if query.amended_only and group["isAmended"] is not True:
        return False
    if query.ownership == "direct" and group["directIndirectOwnership"] != "D":
        return False
    if query.ownership == "indirect" and group["directIndirectOwnership"] != "I":
        return False
    if query.minimum_value is not None:
        value = group["value"]
        if value is None or Decimal(value) < Decimal(query.minimum_value):
            return False
    if query.search:
        owner = group["ownerGroup"]
        assert isinstance(owner, dict)
        haystack = " ".join(
            str(value or "")
            for value in (
                owner["displayName"],
                owner["primaryTitle"],
                group["accessionNumber"],
            )
        ).casefold()
        if query.search.casefold() not in haystack:
            return False
    return True


def _compare_optional_decimal(left: object, right: object) -> int:
    if left is None and right is None:
        return 0
    if left is None:
        return 1
    if right is None:
        return -1
    left_decimal = Decimal(str(left))
    right_decimal = Decimal(str(right))
    return (left_decimal > right_decimal) - (left_decimal < right_decimal)


def _sort_groups(
    groups: list[dict[str, object]], query: _Query
) -> list[dict[str, object]]:
    field = {
        "tradeDate": "transactionDate",
        "value": "value",
        "shares": "shares",
        "holdingsAfter": "postTransactionShares",
        "percentChange": "percentChange",
    }[query.sort]

    def compare(left: Mapping[str, object], right: Mapping[str, object]) -> int:
        if query.sort == "tradeDate":
            primary = (left[field] > right[field]) - (left[field] < right[field])
        else:
            primary = _compare_optional_decimal(left[field], right[field])
            if left[field] is None or right[field] is None:
                if primary:
                    return primary
        if primary:
            return primary if query.order == "asc" else -primary
        left_tie = (
            left["transactionDate"],
            left["acceptedAt"] or "",
            left["accessionNumber"],
            left["_privateDisplayGroupKey"],
        )
        right_tie = (
            right["transactionDate"],
            right["acceptedAt"] or "",
            right["accessionNumber"],
            right["_privateDisplayGroupKey"],
        )
        return -1 if left_tie > right_tie else 1 if left_tie < right_tie else 0

    return sorted(groups, key=cmp_to_key(compare))


def _code_summary(groups: list[dict[str, object]], code: str) -> dict[str, object]:
    selected = [group for group in groups if group["transactionCode"] == code]
    known_values = [
        Decimal(group["value"]) for group in selected if group["value"] is not None
    ]
    owner_keys = {_owner_identity(group) for group in selected}
    missing = sum(group["valueCoverage"] != "complete" for group in selected)
    result: dict[str, object] = {
        "value": _decimal_text(sum(known_values, Decimal(0))),
        "displayValue": _compact_money(sum(known_values, Decimal(0)))
        if known_values
        else "—",
        "transactionCount": len(selected),
        "ownerGroupCount": len(owner_keys),
        "knownValueCount": len(selected) - missing,
        "missingValueCount": missing,
    }
    if code == "S":
        known_plan = [
            group
            for group in selected
            if group["planStatus"] in {"filing_marked", "not_marked"}
            and group["value"] is not None
        ]
        denominator = sum(
            (Decimal(group["value"]) for group in known_plan),
            Decimal(0),
        )
        numerator = sum(
            (
                Decimal(group["value"])
                for group in known_plan
                if group["planStatus"] == "filing_marked"
            ),
            Decimal(0),
        )
        result["planMarkedKnownValuePercentage"] = (
            _ratio(numerator, denominator) if denominator > 0 else None
        )
        result["unknownPlanStatusCount"] = sum(
            group["planStatus"] == "unknown" for group in selected
        )
    return result


def _compact_money(value: Decimal) -> str:
    negative = value < 0
    absolute = -value if negative else value
    suffix = ""
    divisor = Decimal(1)
    for threshold, candidate_divisor, candidate_suffix in (
        (Decimal("1000000000000"), Decimal("1000000000000"), "T"),
        (Decimal("1000000000"), Decimal("1000000000"), "B"),
        (Decimal("1000000"), Decimal("1000000"), "M"),
        (Decimal("1000"), Decimal("1000"), "K"),
    ):
        if absolute >= threshold:
            divisor = candidate_divisor
            suffix = candidate_suffix
            break
    with localcontext() as context:
        context.prec = 80
        scaled = absolute / divisor
        places = (
            Decimal("0.01")
            if scaled < 10
            else Decimal("0.1")
            if scaled < 100
            else Decimal("1")
        )
        rendered = _decimal_text(scaled.quantize(places))
    return f"{'-' if negative else ''}${rendered}{suffix}"


def _latest_meaningful(groups: list[dict[str, object]]) -> dict[str, object] | None:
    qualifying = [group for group in groups if group["transactionCode"] in {"P", "S"}]
    if not qualifying:
        return None
    return max(
        qualifying,
        key=lambda group: (
            group["transactionDate"],
            group["acceptedAt"] or "",
            group["accessionNumber"],
            group["_privateDisplayGroupKey"],
        ),
    )


def _rank_owners(groups: list[dict[str, object]], code: str) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for group in groups:
        if group["transactionCode"] != code:
            continue
        owner = group["ownerGroup"]
        assert isinstance(owner, dict)
        key = _owner_identity(group)
        bucket = buckets.setdefault(
            key,
            {
                "displayName": owner["displayName"],
                "incompleteCount": 0,
                "knownValues": [],
                "planRows": [],
                "roleLabel": owner["primaryTitle"],
                "sortIdentity": key,
                "transactionCount": 0,
            },
        )
        bucket["transactionCount"] += 1
        if group["valueCoverage"] != "complete":
            bucket["incompleteCount"] += 1
        if group["value"] is not None:
            bucket["knownValues"].append(Decimal(group["value"]))
        if code == "S":
            bucket["planRows"].append(group)

    rankings: list[dict[str, object]] = []
    for bucket in buckets.values():
        known_values = bucket.pop("knownValues")
        plan_rows = bucket.pop("planRows")
        assert isinstance(known_values, list)
        assert isinstance(plan_rows, list)
        value = sum(known_values, Decimal(0)) if known_values else None
        item = {
            **bucket,
            "value": None if value is None else _decimal_text(value),
            "displayValue": "—" if value is None else _compact_money(value),
            "planMarkedKnownValuePercentage": None,
        }
        if code == "S":
            known_plan_rows = [
                group
                for group in plan_rows
                if group["planStatus"] in {"filing_marked", "not_marked"}
                and group["value"] is not None
            ]
            denominator = sum(
                (Decimal(group["value"]) for group in known_plan_rows),
                Decimal(0),
            )
            numerator = sum(
                (
                    Decimal(group["value"])
                    for group in known_plan_rows
                    if group["planStatus"] == "filing_marked"
                ),
                Decimal(0),
            )
            item["planMarkedKnownValuePercentage"] = (
                _ratio(numerator, denominator) if denominator > 0 else None
            )
        rankings.append(item)
    rankings.sort(
        key=lambda item: (
            item["value"] is None,
            -(Decimal(item["value"]) if item["value"] is not None else Decimal(0)),
            str(item["displayName"]).casefold(),
            item["sortIdentity"],
        )
    )
    return [
        {
            **{key: value for key, value in item.items() if key != "sortIdentity"},
            "rank": index,
        }
        for index, item in enumerate(rankings[:5], start=1)
    ]


def _validate_holding_rows(
    holdings: Iterable[Mapping[str, object]] | None,
    *,
    security_id: str,
) -> list[dict[str, object]]:
    if holdings is None:
        return []
    if isinstance(holdings, (str, bytes, Mapping)):
        raise _fail("holding rows")
    result: list[dict[str, object]] = []
    seen: set[tuple[str, str, str, str | None]] = set()
    for raw in holdings:
        if len(result) >= MAX_PUBLIC_HOLDING_ROWS or not isinstance(raw, dict):
            raise _fail("holding rows")
        required = {
            "asOfDate",
            "directIndirectOwnership",
            "ownerGroup",
            "privateOwnerGroupKey",
            "securityId",
            "shares",
        }
        if set(raw) != required or raw["securityId"] != security_id:
            raise _fail("holding row fields")
        owner = _validate_owner(raw["ownerGroup"])
        owner_group_key = raw["privateOwnerGroupKey"]
        if (
            type(owner_group_key) is not str
            or _SHA256_RE.fullmatch(owner_group_key) is None
        ):
            raise _fail("holding ownerGroupKey")
        as_of_date = _iso_date(raw["asOfDate"], "holding asOfDate")
        ownership = raw["directIndirectOwnership"]
        if ownership not in {"D", "I"}:
            raise _fail("holding ownership")
        shares = _decimal(raw["shares"], "holding shares", nullable=True)
        key = (owner_group_key, ownership, "", as_of_date)
        if key in seen:
            raise _fail("duplicate holding row")
        seen.add(key)
        result.append(
            {
                "asOfDate": as_of_date,
                "directIndirectOwnership": ownership,
                "ownerGroup": owner,
                "privateOwnerGroupKey": owner_group_key,
                "securityId": security_id,
                "shares": None if shares is None else _decimal_text(shares),
            }
        )
    return result


def _latest_holdings(
    holdings: list[dict[str, object]],
) -> dict[str, list[dict[str, object]]]:
    latest_date: dict[str, str] = {}
    for row in holdings:
        owner = row["ownerGroup"]
        assert isinstance(owner, dict)
        key = _owner_identity(row)
        latest_date[key] = max(latest_date.get(key, ""), str(row["asOfDate"]))
    totals: dict[str, dict[str, object]] = {}
    for row in holdings:
        owner = row["ownerGroup"]
        assert isinstance(owner, dict)
        key = _owner_identity(row)
        if row["asOfDate"] != latest_date[key]:
            continue
        item = totals.setdefault(
            key,
            {
                "asOfDate": row["asOfDate"],
                "displayName": owner["displayName"],
                "known": True,
                "roleLabel": owner["primaryTitle"],
                "roles": owner["roles"],
                "sharesDecimal": Decimal(0),
            },
        )
        if row["shares"] is None:
            item["known"] = False
        else:
            item["sharesDecimal"] += Decimal(row["shares"])
    output: list[dict[str, object]] = []
    for item in totals.values():
        shares_decimal = item.pop("sharesDecimal")
        known = item.pop("known")
        output.append(
            {
                **item,
                "shares": _decimal_text(shares_decimal) if known else None,
                "ownershipPercentage": None,
            }
        )
    output.sort(
        key=lambda item: (
            item["shares"] is None,
            -(Decimal(item["shares"]) if item["shares"] is not None else Decimal(0)),
            str(item["displayName"]).casefold(),
        )
    )
    return {
        "officersAndDirectors": [
            item for item in output if set(item["roles"]) & {"Officer", "Director"}
        ][:5],
        "tenPercentOwnersAndEntities": [
            item for item in output if "TenPercentOwner" in set(item["roles"])
        ][:5],
    }


def _quality_context(
    value: Mapping[str, object] | None,
    *,
    groups: list[dict[str, object]],
    as_of: str,
) -> dict[str, object]:
    raw = {} if value is None else dict(value)
    allowed = {
        "freshnessMaxAgeSeconds",
        "latestSuccessfulSyncAt",
        "unmappedSecurityRowCount",
        "unresolvedAmendmentCount",
    }
    if set(raw) - allowed:
        raise _fail("data quality fields")
    unresolved = raw.get("unresolvedAmendmentCount", 0)
    unmapped = raw.get("unmappedSecurityRowCount", 0)
    for label, item in (
        ("unresolvedAmendmentCount", unresolved),
        ("unmappedSecurityRowCount", unmapped),
    ):
        if type(item) is not int or type(item) is bool or item < 0:
            raise _fail(label)
    freshness_max_age_seconds = raw.get("freshnessMaxAgeSeconds", 129_600)
    if (
        type(freshness_max_age_seconds) is not int
        or type(freshness_max_age_seconds) is bool
        or not 1 <= freshness_max_age_seconds <= 604_800
    ):
        raise _fail("freshnessMaxAgeSeconds")

    as_of_text = as_of[:-1] + "+00:00" if as_of.endswith("Z") else as_of
    as_of_time = datetime.fromisoformat(as_of_text).astimezone(timezone.utc)
    successful_sync = raw.get("latestSuccessfulSyncAt")
    freshness_status = "unknown"
    if successful_sync is not None:
        _timestamp(successful_sync, "latestSuccessfulSyncAt")
        assert isinstance(successful_sync, str)
        sync_text = (
            successful_sync[:-1] + "+00:00"
            if successful_sync.endswith("Z")
            else successful_sync
        )
        sync_time = datetime.fromisoformat(sync_text).astimezone(timezone.utc)
        if sync_time > as_of_time:
            raise _fail("latestSuccessfulSyncAt after asOf")
        age_seconds = (as_of_time - sync_time).total_seconds()
        freshness_status = (
            "current" if age_seconds <= freshness_max_age_seconds else "stale"
        )
    accepted_values = [
        str(group["acceptedAt"]) for group in groups if group["acceptedAt"] is not None
    ]
    return {
        "unresolvedAmendmentCount": unresolved,
        "unmappedSecurityRowCount": unmapped,
        "latestSuccessfulSyncAt": successful_sync,
        "latestSecAcceptedAt": max(accepted_values) if accepted_values else None,
        "freshnessMaxAgeSeconds": freshness_max_age_seconds,
        "freshnessStatus": freshness_status,
    }


def _cursor_digest(query: _Query, groups: list[dict[str, object]]) -> str:
    material = {
        "filters": query.public_dict(),
        "groups": [group["_privateDisplayGroupKey"] for group in groups],
    }
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(b"sis-insider-cursor-v1\0" + encoded).hexdigest()[:16]


def _public_transaction_group(group: Mapping[str, object]) -> dict[str, object]:
    """Strip private grouping material before serializing a canonical group."""

    private_fields = {
        "_privateDisplayGroupKey",
        "_privateSourceRowKeys",
        "privateDisplayGroupKeyOverride",
        "privateFootnoteIds",
        "privateOwnerGroupKey",
        "privateSourceTable",
    }
    return {key: value for key, value in group.items() if key not in private_fields}


def build_insider_metric_projection(
    rows: Iterable[Mapping[str, object]],
    *,
    security_id: str,
    as_of: str,
    query: Mapping[str, object] | None = None,
    holdings: Iterable[Mapping[str, object]] | None = None,
    quality: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build one exact, deterministic page/query projection.

    Every page region derives from the same filtered display-group sequence.
    The returned cursor is bound to both the normalized query and that sequence,
    so stale or cross-query cursors fail closed rather than skipping data.
    """

    canonical_security_id = _safe_string(security_id, "securityId", maximum=128)
    assert isinstance(canonical_security_id, str)
    canonical_as_of = _timestamp(as_of, "asOf")
    assert isinstance(canonical_as_of, str)
    parsed_query = _parse_query(
        query,
        security_id=canonical_security_id,
        as_of=canonical_as_of,
    )
    validated_rows = _bounded_rows(rows, security_id=canonical_security_id)
    all_groups = _group_rows(validated_rows)
    filtered = [group for group in all_groups if _matches_query(group, parsed_query)]
    ordered = _sort_groups(filtered, parsed_query)
    digest = _cursor_digest(parsed_query, ordered)
    offset = 0
    if parsed_query.cursor is not None:
        match = _CURSOR_RE.fullmatch(parsed_query.cursor)
        assert match is not None
        if match.group(1) != digest:
            raise _fail("cursor query binding")
        offset = int(match.group(2))
        if offset <= 0 or offset >= len(ordered):
            raise _fail("cursor offset")
    page = ordered[offset : offset + parsed_query.limit]
    next_offset = offset + len(page)
    next_cursor = f"v1.{digest}.{next_offset}" if next_offset < len(ordered) else None

    purchases = _code_summary(ordered, "P")
    sales = _code_summary(ordered, "S")
    purchase_value = Decimal(str(purchases["value"]))
    sale_value = Decimal(str(sales["value"]))
    net_value = purchase_value - sale_value
    if purchase_value > 0:
        ratio_state = "ratio"
        ratio_value = _ratio(sale_value, purchase_value)
    elif sale_value > 0:
        ratio_state = "sales_only"
        ratio_value = None
    else:
        ratio_state = "no_valued_activity"
        ratio_value = None
    direction = (
        "net_reported_buying"
        if net_value > 0
        else "net_reported_selling"
        if net_value < 0
        else "balanced_reported_activity"
    )
    latest = _latest_meaningful(ordered)

    marked_sales = [
        group
        for group in ordered
        if group["transactionCode"] == "S" and group["planStatus"] == "filing_marked"
    ]
    marked_known_values = [
        Decimal(group["value"]) for group in marked_sales if group["value"] is not None
    ]
    marked_value = sum(marked_known_values, Decimal(0))
    holding_rows = _validate_holding_rows(
        holdings,
        security_id=canonical_security_id,
    )
    quality_context = _quality_context(
        quality,
        groups=all_groups,
        as_of=canonical_as_of,
    )
    missing_value_count = sum(
        group["transactionCode"] in {"P", "S"} and group["valueCoverage"] != "complete"
        for group in ordered
    )
    unknown_plan_count = sum(
        group["transactionCode"] == "S" and group["planStatus"] == "unknown"
        for group in ordered
    )
    partial = any(
        (
            missing_value_count,
            unknown_plan_count,
            quality_context["unresolvedAmendmentCount"],
            quality_context["unmappedSecurityRowCount"],
        )
    )

    chart_events = [
        {
            "transactionDate": group["transactionDate"],
            "ownerGroupDisplayName": group["ownerGroup"]["displayName"],
            "roleLabel": group["ownerGroup"]["primaryTitle"],
            "code": group["transactionCode"],
            "category": group["normalizedCategory"],
            "marker": (
                "triangle-up-filled"
                if group["transactionCode"] == "P"
                else "triangle-down-outline"
                if group["transactionCode"] == "S"
                and group["planStatus"] == "filing_marked"
                else "triangle-down-filled"
                if group["transactionCode"] == "S"
                else "circle-neutral"
            ),
            "shares": group["shares"],
            "pricePerShare": group["pricePerShare"],
            "value": group["value"],
            "postTransactionShares": group["postTransactionShares"],
            "formType": group["formType"],
            "filingDate": group["filingDate"],
            "planStatus": group["planStatus"],
            "accessionNumber": group["accessionNumber"],
        }
        for group in ordered
    ]

    return {
        "asOf": canonical_as_of,
        "filters": parsed_query.public_dict(),
        "summary": {
            "window": "12m" if parsed_query.range == "1y" else "filtered",
            "purchases": purchases,
            "sales": sales,
            "netPS": {
                "value": _decimal_text(net_value),
                "displayValue": _compact_money(net_value),
                "direction": direction,
                "ratioState": ratio_state,
                "salesToPurchasesRatio": ratio_value,
                "missingValueCount": missing_value_count,
            },
            "latestMeaningfulTransaction": (
                None if latest is None else _public_transaction_group(latest)
            ),
        },
        "priceSeries": [],
        "chartEvents": chart_events,
        "transactions": {
            "items": [_public_transaction_group(group) for group in page],
            "nextCursor": next_cursor,
            "total": len(ordered),
            "totalApproximate": len(ordered),
        },
        "sidebar": {
            "window": "12m" if parsed_query.range == "1y" else "filtered",
            "topBuyers": _rank_owners(ordered, "P"),
            "topSellers": _rank_owners(ordered, "S"),
            "latestReportedHoldings": _latest_holdings(holding_rows),
            "rule10b51": {
                "planMarkedSalesValue": _decimal_text(marked_value),
                "planMarkedSalesDisplayValue": (
                    _compact_money(marked_value) if marked_known_values else "—"
                ),
                "distinctOwnerGroupCount": len(
                    {_owner_identity(group) for group in marked_sales}
                ),
                "missingValueCount": sum(
                    group["valueCoverage"] != "complete" for group in marked_sales
                ),
                "latestPlanAdoptionDate": None,
            },
        },
        "dataFreshness": {
            "latestSecAcceptedAt": quality_context["latestSecAcceptedAt"],
            "latestSuccessfulSecSyncAt": quality_context["latestSuccessfulSyncAt"],
            "latestPriceDate": None,
            "priceDataStatus": "not_integrated_phase4",
            "secFreshnessThresholdSeconds": quality_context["freshnessMaxAgeSeconds"],
            "status": quality_context["freshnessStatus"],
        },
        "dataQuality": {
            "partial": partial,
            "missingValueTransactionCount": missing_value_count,
            "unknownPlanStatusSaleCount": unknown_plan_count,
            "unresolvedAmendmentCount": quality_context["unresolvedAmendmentCount"],
            "unmappedSecurityRowCount": quality_context["unmappedSecurityRowCount"],
            "priceCoverageStart": None,
            "priceCoverageEnd": None,
            "latestSecAcceptedAt": quality_context["latestSecAcceptedAt"],
            "latestSuccessfulSyncAt": quality_context["latestSuccessfulSyncAt"],
        },
    }


__all__ = [
    "MAX_PUBLIC_HOLDING_ROWS",
    "MAX_PUBLIC_TRANSACTION_ROWS",
    "InsiderMetricsError",
    "build_insider_metric_projection",
]
