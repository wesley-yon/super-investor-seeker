#!/usr/bin/env python3
"""Plan one fixed-scope ServiceNow publication policy without publishing it."""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import math
import os
from pathlib import Path
import re
import stat
import statistics
import sys
from typing import Never, cast
import unicodedata

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_contract import DATA_CONTRACT_VERSION  # noqa: E402
from insider_publication_policy import (  # noqa: E402
    SERVICENOW_CIK,
    build_servicenow_publication_policy,
    publication_policy_sha256,
)
from insider_storage import (  # noqa: E402
    MAX_INSIDER_STATE_BYTES,
    MAX_INSIDER_STATE_COLLECTION,
    InsiderStateStore,
    canonical_insider_state_json_bytes,
)
from security_identity import (  # noqa: E402
    VALID_INSTRUMENT_TYPES,
    is_canonical_security_identifier,
    stock_file_stem,
    stock_lookup_id,
)

_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_OUTPUT_FLAGS = (
    os.O_WRONLY
    | os.O_CREAT
    | os.O_EXCL
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_FILE_READ_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_NONBLOCK", 0)
)
MAX_MAPPING_SPEC_BYTES = MAX_INSIDER_STATE_BYTES
MAX_PUBLIC_INDEX_BYTES = 50_000_000
MAX_PUBLIC_STOCK_BYTES = 50_000_000
MAX_POLICY_CANDIDATE_BYTES = MAX_INSIDER_STATE_BYTES
MAX_PUBLIC_INDEX_ROWS = 100_000
_PUBLIC_INDEX_KEYS = {
    "data_contract_version",
    "fund_data_revision",
    "funds",
    "last_updated",
    "proven_split_adjustments",
    "tickers",
    "total_filers",
    "total_tickers",
}
_PUBLIC_TICKER_KEYS = {
    "stock_id",
    "cusip",
    "ticker",
    "issuer",
    "instrument_type",
    "holder_count",
    "current_holder_count",
    "last_seen",
}
_PUBLIC_STOCK_BASE_KEYS = {
    "stock_id",
    "cusip",
    "ticker",
    "issuer",
    "instrument_type",
    "holders",
}
_PUBLIC_EQUITY_STOCK_KEYS = _PUBLIC_STOCK_BASE_KEYS | {"split_adjustments"}
_PUBLIC_FUND_BASE_KEYS = {"cik", "name", "q"}
_PUBLIC_FUND_WITHHELD_KEYS = {
    "status",
    "latest_withheld_report_date",
    "withheld_reason",
}
_PUBLIC_FUND_OPTIONAL_KEYS = _PUBLIC_FUND_WITHHELD_KEYS | {"unverified_report_dates"}
_PUBLIC_HOLDER_KEYS = {"cik", "name", "history"}
_PUBLIC_HISTORY_BASE_KEYS = {"date", "shares", "value", "pct_of_fund"}
_PUBLIC_HISTORY_KEYS_WITH_IMPUTED = _PUBLIC_HISTORY_BASE_KEYS | {"shares_imputed"}
_SPLIT_ADJUSTMENT_KEYS = {
    "from_report_date",
    "to_report_date",
    "factor",
    "proven",
    "support",
    "observations",
}
_SPLIT_FACTORS = {0.1, 0.2, 0.25, 0.33333333, 0.5, 2, 3, 4, 5, 10}
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_REPORT_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_UTC_TIMESTAMP_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
)


def _is_exact_public_text(value: object, *, maximum: int) -> bool:
    return (
        type(value) is str
        and bool(value)
        and len(value) <= maximum
        and value == value.strip()
        and value == unicodedata.normalize("NFKC", value)
        and not any(
            unicodedata.category(character) in {"Cc", "Cf", "Cs"} for character in value
        )
    )


class InsiderPublicationPolicyPlanningError(ValueError):
    """Raised when a private policy candidate cannot be planned safely."""


class _PlanningArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        del message
        raise InsiderPublicationPolicyPlanningError("configuration")


def _parse_arguments(argv: list[str]) -> argparse.Namespace:
    if type(argv) is not list or any(type(value) is not str for value in argv):
        raise InsiderPublicationPolicyPlanningError("configuration")
    parser = _PlanningArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--repository-root", required=True)
    parser.add_argument("--issuer-cik", required=True)
    parser.add_argument("--review-directory", required=True)
    parser.add_argument("--mapping-spec", required=True)
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args(argv)
    if arguments.issuer_cik != SERVICENOW_CIK:
        raise InsiderPublicationPolicyPlanningError("configuration")
    return arguments


def _metadata_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _is_owner_only_regular(metadata: os.stat_result) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == 0o600
        and metadata.st_nlink == 1
    )


def _single_component(name: str, label: str) -> str:
    if (
        type(name) is not str
        or not _is_exact_public_text(name, maximum=255)
        or len(name.encode("utf-8")) > 255
        or name in {".", ".."}
        or "/" in name
        or "\\" in name
        or "\0" in name
    ):
        raise InsiderPublicationPolicyPlanningError(label)
    return name


def _open_owner_only_directory(path: Path) -> int:
    try:
        before = os.stat(path, follow_symlinks=False)
        descriptor = os.open(path, _DIRECTORY_FLAGS)
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError("review directory") from error
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(before.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or _metadata_identity(before) != _metadata_identity(opened)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != 0o700
        ):
            raise InsiderPublicationPolicyPlanningError("review directory")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_directory(path: Path, label: str) -> int:
    try:
        before = os.stat(path, follow_symlinks=False)
        descriptor = os.open(path, _DIRECTORY_FLAGS)
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError(label) from error
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(before.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or _metadata_identity(before) != _metadata_identity(opened)
        ):
            raise InsiderPublicationPolicyPlanningError(label)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_directory_at(
    parent_descriptor: int,
    name: str,
    label: str,
) -> int:
    filename = _single_component(name, label)
    try:
        before = os.stat(
            filename,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        descriptor = os.open(
            filename,
            _DIRECTORY_FLAGS,
            dir_fd=parent_descriptor,
        )
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError(label) from error
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(before.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or _metadata_identity(before) != _metadata_identity(opened)
        ):
            raise InsiderPublicationPolicyPlanningError(label)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _require_directory_path_identity(
    path: Path,
    descriptor: int,
    label: str,
    *,
    owner_only: bool = False,
) -> None:
    try:
        named = os.stat(path, follow_symlinks=False)
        opened = os.fstat(descriptor)
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError(label) from error
    if (
        not stat.S_ISDIR(named.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or _metadata_identity(named) != _metadata_identity(opened)
        or (owner_only and opened.st_uid != os.geteuid())
        or (owner_only and stat.S_IMODE(opened.st_mode) != 0o700)
    ):
        raise InsiderPublicationPolicyPlanningError(label)


def _require_directory_identity_at(
    parent_descriptor: int,
    name: str,
    descriptor: int,
    label: str,
) -> None:
    try:
        named = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        opened = os.fstat(descriptor)
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError(label) from error
    if (
        not stat.S_ISDIR(named.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or _metadata_identity(named) != _metadata_identity(opened)
    ):
        raise InsiderPublicationPolicyPlanningError(label)


def _read_bounded_regular_file_at(
    directory_descriptor: int,
    name: str,
    *,
    maximum: int,
    label: str,
    owner_only: bool,
) -> tuple[bytes, tuple[int, int, int, int, int]]:
    filename = _single_component(name, label)
    try:
        before = os.stat(
            filename,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        descriptor = os.open(
            filename,
            _FILE_READ_FLAGS,
            dir_fd=directory_descriptor,
        )
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError(label) from error
    try:
        opened = os.fstat(descriptor)
        identity = _metadata_identity(opened)
        if (
            not stat.S_ISREG(before.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or _metadata_identity(before) != identity
            or opened.st_size > maximum
            or (owner_only and not _is_owner_only_regular(opened))
        ):
            raise InsiderPublicationPolicyPlanningError(label)
        chunks: list[bytes] = []
        total = 0
        while True:
            block = os.read(
                descriptor,
                min(1024 * 1024, maximum + 1 - total),
            )
            if not block:
                break
            chunks.append(block)
            total += len(block)
            if total > maximum:
                raise InsiderPublicationPolicyPlanningError(label)
        after = os.fstat(descriptor)
        named_after = os.stat(
            filename,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        if (
            _metadata_identity(after) != identity
            or _metadata_identity(named_after) != identity
            or total != opened.st_size
        ):
            raise InsiderPublicationPolicyPlanningError(label)
        return b"".join(chunks), identity
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError(label) from error
    finally:
        os.close(descriptor)


def _require_named_identity(
    directory_descriptor: int,
    name: str,
    expected: tuple[int, int, int, int, int],
    label: str,
    *,
    owner_only: bool = False,
) -> None:
    try:
        current = os.stat(
            name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError(label) from error
    if (
        not stat.S_ISREG(current.st_mode)
        or _metadata_identity(current) != expected
        or (owner_only and not _is_owner_only_regular(current))
    ):
        raise InsiderPublicationPolicyPlanningError(label)


def _strict_json_bytes(rendered: bytes, label: str) -> object:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(
            rendered.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("nonfinite JSON")
            ),
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        ValueError,
    ) as error:
        raise InsiderPublicationPolicyPlanningError(label) from error


def _write_all(descriptor: int, payload: bytes) -> None:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(descriptor, remaining)
        if written < 1:
            raise InsiderPublicationPolicyPlanningError("candidate write")
        remaining = remaining[written:]


def _read_canonical_issuer_state(
    state_store: InsiderStateStore,
) -> dict[str, object]:
    try:
        return state_store.read_canonical(f"issuers/{SERVICENOW_CIK}")
    except (OSError, TypeError, ValueError) as error:
        raise InsiderPublicationPolicyPlanningError("issuer state") from error


def _security_class_keys(issuer_state: object) -> tuple[str, ...]:
    if not isinstance(issuer_state, dict):
        raise InsiderPublicationPolicyPlanningError("issuer state")
    classes = issuer_state.get("security_classes")
    if not isinstance(classes, list):
        raise InsiderPublicationPolicyPlanningError("issuer state")
    keys: list[str] = []
    for row in classes:
        if not isinstance(row, dict) or type(row.get("security_class_key")) is not str:
            raise InsiderPublicationPolicyPlanningError("issuer state")
        keys.append(row["security_class_key"])
    return tuple(keys)


def _report_quarter_code(value: object) -> int | None:
    if type(value) is not str or _REPORT_DATE_RE.fullmatch(value) is None:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    quarter = {
        (3, 31): 1,
        (6, 30): 2,
        (9, 30): 3,
        (12, 31): 4,
    }.get((parsed.month, parsed.day))
    if quarter is None or parsed.isoformat() != value:
        return None
    return parsed.year * 10 + quarter


def _is_finite_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value))
    except (OverflowError, TypeError, ValueError):
        return False


def _validate_public_funds(funds: list[object]) -> dict[int, dict[str, object]]:
    indexed: dict[int, dict[str, object]] = {}
    ordered_names: list[str] = []
    for row in funds:
        if type(row) is not dict:
            raise InsiderPublicationPolicyPlanningError("public index")
        keys = set(row)
        withheld_keys = keys & _PUBLIC_FUND_WITHHELD_KEYS
        if (
            not _PUBLIC_FUND_BASE_KEYS.issubset(keys)
            or keys - (_PUBLIC_FUND_BASE_KEYS | _PUBLIC_FUND_OPTIONAL_KEYS)
            or withheld_keys not in (set(), _PUBLIC_FUND_WITHHELD_KEYS)
        ):
            raise InsiderPublicationPolicyPlanningError("public index")
        cik = row["cik"]
        name = row["name"]
        quarters = row["q"]
        if (
            type(cik) is not int
            or cik <= 0
            or cik in indexed
            or not _is_exact_public_text(name, maximum=512)
            or type(quarters) is not list
            or len(quarters) > 4
            or any(
                type(code) is not int
                or code // 10 not in range(1000, 10_000)
                or code % 10 not in {1, 2, 3, 4}
                for code in quarters
            )
            or quarters != sorted(set(quarters), reverse=True)
        ):
            raise InsiderPublicationPolicyPlanningError("public index")
        if "unverified_report_dates" in row:
            unverified = row["unverified_report_dates"]
            if (
                type(unverified) is not list
                or not unverified
                or len(unverified) > 4
                or any(_report_quarter_code(value) is None for value in unverified)
                or unverified != sorted(set(unverified), reverse=True)
                or any(
                    _report_quarter_code(value) not in quarters for value in unverified
                )
            ):
                raise InsiderPublicationPolicyPlanningError("public index")
        if withheld_keys:
            if (
                row["status"] != "WITHHELD"
                or _report_quarter_code(row["latest_withheld_report_date"]) is None
                or not _is_exact_public_text(row["withheld_reason"], maximum=4096)
            ):
                raise InsiderPublicationPolicyPlanningError("public index")
        indexed[cik] = row
        ordered_names.append(name.upper())
    if ordered_names != sorted(ordered_names):
        raise InsiderPublicationPolicyPlanningError("public index")
    return indexed


def _validate_split_adjustments(
    payload: object,
    *,
    allow_empty: bool,
    label: str,
) -> list[dict[str, object]]:
    if (
        type(payload) is not list
        or (not allow_empty and not payload)
        or len(payload) > MAX_PUBLIC_INDEX_ROWS
    ):
        raise InsiderPublicationPolicyPlanningError(label)
    validated: list[dict[str, object]] = []
    periods: list[tuple[str, str]] = []
    for row in payload:
        if type(row) is not dict or set(row) != _SPLIT_ADJUSTMENT_KEYS:
            raise InsiderPublicationPolicyPlanningError(label)
        from_date = row["from_report_date"]
        to_date = row["to_report_date"]
        from_code = _report_quarter_code(from_date)
        to_code = _report_quarter_code(to_date)
        factor = row["factor"]
        support = row["support"]
        observations = row["observations"]
        if (
            from_code is None
            or to_code is None
            or (to_code // 10) * 4 + (to_code % 10)
            != (from_code // 10) * 4 + (from_code % 10) + 1
            or not _is_finite_number(factor)
            or factor not in _SPLIT_FACTORS
            or row["proven"] is not True
            or type(support) is not int
            or type(observations) is not int
            or support < 20
            or observations < support
            or support / observations < 0.55
        ):
            raise InsiderPublicationPolicyPlanningError(label)
        validated.append(row)
        periods.append((from_date, to_date))
    if periods != sorted(set(periods)):
        raise InsiderPublicationPolicyPlanningError(label)
    return validated


def _validate_global_split_adjustments(
    payload: dict[str, object],
) -> dict[str, list[dict[str, object]]]:
    if list(payload) != sorted(payload):
        raise InsiderPublicationPolicyPlanningError("public index")
    validated: dict[str, list[dict[str, object]]] = {}
    total = 0
    for stock_id, rows in payload.items():
        if (
            type(stock_id) is not str
            or not is_canonical_security_identifier(stock_id)
            or stock_lookup_id(stock_id, "EQUITY") != stock_id
        ):
            raise InsiderPublicationPolicyPlanningError("public index")
        validated_rows = _validate_split_adjustments(
            rows,
            allow_empty=False,
            label="public index",
        )
        total += len(validated_rows)
        if total > MAX_PUBLIC_INDEX_ROWS:
            raise InsiderPublicationPolicyPlanningError("public index")
        validated[stock_id] = validated_rows
    return validated


def _validate_public_holders(
    payload: list[object],
    funds: dict[int, dict[str, object]],
) -> list[dict[str, object]]:
    if len(payload) > MAX_PUBLIC_INDEX_ROWS:
        raise InsiderPublicationPolicyPlanningError("public stock")
    validated: list[dict[str, object]] = []
    seen_ciks: set[int] = set()
    latest_values: list[float] = []
    history_count = 0
    for holder in payload:
        if type(holder) is not dict or set(holder) != _PUBLIC_HOLDER_KEYS:
            raise InsiderPublicationPolicyPlanningError("public stock")
        cik = holder["cik"]
        name = holder["name"]
        history = holder["history"]
        if (
            type(cik) is not int
            or cik <= 0
            or cik in seen_ciks
            or cik not in funds
            or not _is_exact_public_text(name, maximum=512)
            or funds[cik]["name"] != name
            or type(history) is not list
            or not history
            or len(history) > MAX_PUBLIC_INDEX_ROWS
        ):
            raise InsiderPublicationPolicyPlanningError("public stock")
        dates: list[str] = []
        validated_history: list[dict[str, object]] = []
        for row in history:
            if type(row) is not dict or set(row) not in (
                _PUBLIC_HISTORY_BASE_KEYS,
                _PUBLIC_HISTORY_KEYS_WITH_IMPUTED,
            ):
                raise InsiderPublicationPolicyPlanningError("public stock")
            report_date = row["date"]
            if (
                _report_quarter_code(report_date) is None
                or not all(
                    _is_finite_number(row[field])
                    for field in ("shares", "value", "pct_of_fund")
                )
                or ("shares_imputed" in row and row["shares_imputed"] is not True)
            ):
                raise InsiderPublicationPolicyPlanningError("public stock")
            dates.append(report_date)
            validated_history.append(row)
        if dates != sorted(set(dates), reverse=True):
            raise InsiderPublicationPolicyPlanningError("public stock")
        history_count += len(validated_history)
        if history_count > MAX_PUBLIC_INDEX_ROWS:
            raise InsiderPublicationPolicyPlanningError("public stock")
        seen_ciks.add(cik)
        latest_values.append(float(cast(int | float, validated_history[0]["value"])))
        validated.append(holder)
    if latest_values != sorted(latest_values, reverse=True):
        raise InsiderPublicationPolicyPlanningError("public stock")
    return validated


def _current_public_fund_quarters(
    funds: dict[int, dict[str, object]],
) -> dict[int, int]:
    latest_counts: dict[int, int] = {}
    for fund in funds.values():
        if fund.get("status") == "WITHHELD":
            continue
        quarters = fund["q"]
        assert isinstance(quarters, list)
        if not quarters:
            continue
        latest = quarters[0]
        assert isinstance(latest, int)
        latest_counts[latest] = latest_counts.get(latest, 0) + 1
    if not latest_counts:
        return {}
    baseline = max(
        latest_counts,
        key=lambda quarter: (latest_counts[quarter], quarter),
    )
    current: dict[int, int] = {}
    for cik, fund in funds.items():
        if fund.get("status") == "WITHHELD":
            continue
        quarters = fund["q"]
        assert isinstance(quarters, list)
        if quarters and isinstance(quarters[0], int) and quarters[0] >= baseline:
            current[cik] = quarters[0]
    return current


def _require_stock_index_metadata_match(
    row: dict[str, object],
    holders: list[dict[str, object]],
    funds: dict[int, dict[str, object]],
) -> None:
    current_fund_quarters = _current_public_fund_quarters(funds)
    last_seen = ""
    current_holder_count = 0
    for holder in holders:
        cik = holder["cik"]
        history = holder["history"]
        assert isinstance(cik, int)
        assert isinstance(history, list)
        current_quarter = current_fund_quarters.get(cik)
        holder_is_current = False
        for observation in history:
            assert isinstance(observation, dict)
            report_date = observation["date"]
            assert isinstance(report_date, str)
            last_seen = max(last_seen, report_date)
            if _report_quarter_code(report_date) == current_quarter:
                holder_is_current = True
        if holder_is_current:
            current_holder_count += 1
    expected = {
        "holder_count": len(holders),
        "current_holder_count": current_holder_count,
        "last_seen": last_seen,
    }
    if any(row[field] != value for field, value in expected.items()):
        raise InsiderPublicationPolicyPlanningError("public stock")


def _infer_proven_split_adjustments(
    holders: list[dict[str, object]],
) -> list[dict[str, object]]:
    histories: list[dict[str, dict[str, object]]] = []
    all_dates: set[str] = set()
    for holder in holders:
        history_rows = holder["history"]
        assert isinstance(history_rows, list)
        by_date = {row["date"]: row for row in history_rows}
        histories.append(by_date)
        all_dates.update(by_date)

    ordered_dates = sorted(all_dates)
    candidate_factors = (0.1, 0.2, 0.25, 1 / 3, 0.5, 2, 3, 4, 5, 10)
    adjustments: list[dict[str, object]] = []
    for previous_date, current_date in zip(ordered_dates, ordered_dates[1:]):
        previous_code = _report_quarter_code(previous_date)
        current_code = _report_quarter_code(current_date)
        assert previous_code is not None and current_code is not None
        previous_ordinal = (previous_code // 10) * 4 + (previous_code % 10)
        current_ordinal = (current_code // 10) * 4 + (current_code % 10)
        if current_ordinal != previous_ordinal + 1:
            continue
        observations: list[tuple[float, float]] = []
        for history in histories:
            previous = history.get(previous_date)
            current = history.get(current_date)
            if previous is None or current is None:
                continue
            if previous.get("shares_imputed") or current.get("shares_imputed"):
                continue
            numbers = [
                float(cast(int | float, row[field]))
                for row, field in (
                    (previous, "shares"),
                    (current, "shares"),
                    (previous, "value"),
                    (current, "value"),
                )
            ]
            if any(number <= 0 for number in numbers):
                continue
            previous_shares, current_shares, previous_value, current_value = numbers
            try:
                share_ratio = current_shares / previous_shares
                previous_price = previous_value / previous_shares
                current_price = current_value / current_shares
                price_ratio = current_price / previous_price
            except (OverflowError, ZeroDivisionError):
                continue
            if (
                not math.isfinite(share_ratio)
                or not math.isfinite(price_ratio)
                or share_ratio <= 0
                or price_ratio <= 0
            ):
                continue
            observations.append((share_ratio, price_ratio))
        if len(observations) < 20:
            continue
        supported: list[tuple[int, float, list[float]]] = []
        for factor in candidate_factors:
            price_ratios = [
                price_ratio
                for share_ratio, price_ratio in observations
                if abs(share_ratio - factor) / factor <= 0.10
            ]
            supported.append((len(price_ratios), factor, price_ratios))
        supported.sort(key=lambda row: (row[0], row[1]), reverse=True)
        support, factor, price_ratios = supported[0]
        if support < 20 or support / len(observations) < 0.55:
            continue
        expected_price_ratio = 1 / factor
        median_price_ratio = statistics.median(price_ratios)
        if abs(median_price_ratio - expected_price_ratio) / expected_price_ratio > 0.40:
            continue
        adjustments.append(
            {
                "from_report_date": previous_date,
                "to_report_date": current_date,
                "factor": round(factor, 8),
                "proven": True,
                "support": support,
                "observations": len(observations),
            }
        )
    return adjustments


def _validate_public_index(
    payload: object,
) -> tuple[
    dict[str, dict[str, object]],
    dict[int, dict[str, object]],
    dict[str, list[dict[str, object]]],
]:
    if type(payload) is not dict or set(payload) != _PUBLIC_INDEX_KEYS:
        raise InsiderPublicationPolicyPlanningError("public index")
    if (
        type(payload["data_contract_version"]) is not int
        or payload["data_contract_version"] != DATA_CONTRACT_VERSION
    ):
        raise InsiderPublicationPolicyPlanningError("public index")
    revision = payload["fund_data_revision"]
    if type(revision) is not str or _SHA256_RE.fullmatch(revision) is None:
        raise InsiderPublicationPolicyPlanningError("public index")
    timestamp = payload["last_updated"]
    if type(timestamp) is not str or _UTC_TIMESTAMP_RE.fullmatch(timestamp) is None:
        raise InsiderPublicationPolicyPlanningError("public index")
    try:
        parsed_timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise InsiderPublicationPolicyPlanningError("public index") from error
    if parsed_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") != timestamp:
        raise InsiderPublicationPolicyPlanningError("public index")
    funds = payload["funds"]
    adjustments = payload["proven_split_adjustments"]
    rows = payload["tickers"]
    if (
        type(funds) is not list
        or type(adjustments) is not dict
        or type(rows) is not list
        or len(funds) > MAX_PUBLIC_INDEX_ROWS
        or len(adjustments) > MAX_PUBLIC_INDEX_ROWS
        or len(rows) > MAX_PUBLIC_INDEX_ROWS
        or type(payload["total_filers"]) is not int
        or payload["total_filers"] != len(funds)
        or type(payload["total_tickers"]) is not int
        or payload["total_tickers"] != len(rows)
    ):
        raise InsiderPublicationPolicyPlanningError("public index")

    indexed: dict[str, dict[str, object]] = {}
    ordered: list[tuple[str, str]] = []
    stems: set[str] = set()
    for row in rows:
        if type(row) is not dict or set(row) != _PUBLIC_TICKER_KEYS:
            raise InsiderPublicationPolicyPlanningError("public index")
        stock_id = row["stock_id"]
        cusip = row["cusip"]
        ticker = row["ticker"]
        issuer = row["issuer"]
        instrument_type = row["instrument_type"]
        if (
            type(stock_id) is not str
            or not stock_id
            or len(stock_id) > 200
            or not is_canonical_security_identifier(cusip)
            or len(cusip) > 160
            or not _is_exact_public_text(ticker, maximum=512)
            or not _is_exact_public_text(issuer, maximum=512)
            or type(instrument_type) is not str
            or instrument_type not in VALID_INSTRUMENT_TYPES
        ):
            raise InsiderPublicationPolicyPlanningError("public index")
        try:
            expected_stock_id = stock_lookup_id(cusip, instrument_type)
            stem = stock_file_stem(stock_id)
        except (TypeError, ValueError) as error:
            raise InsiderPublicationPolicyPlanningError("public index") from error
        if (
            stock_id != expected_stock_id
            or not stem
            or stock_id in indexed
            or stem in stems
        ):
            raise InsiderPublicationPolicyPlanningError("public index")
        holder_count = row["holder_count"]
        current_holder_count = row["current_holder_count"]
        if (
            type(holder_count) is not int
            or holder_count < 0
            or type(current_holder_count) is not int
            or current_holder_count < 0
            or current_holder_count > holder_count
        ):
            raise InsiderPublicationPolicyPlanningError("public index")
        last_seen = row["last_seen"]
        if type(last_seen) is not str:
            raise InsiderPublicationPolicyPlanningError("public index")
        try:
            report_date = date.fromisoformat(last_seen)
        except ValueError as error:
            raise InsiderPublicationPolicyPlanningError("public index") from error
        if report_date.isoformat() != last_seen or (
            report_date.month,
            report_date.day,
        ) not in {(3, 31), (6, 30), (9, 30), (12, 31)}:
            raise InsiderPublicationPolicyPlanningError("public index")
        indexed[stock_id] = row
        stems.add(stem)
        ordered.append((ticker.upper(), stock_id))
    if ordered != sorted(ordered):
        raise InsiderPublicationPolicyPlanningError("public index")
    validated_funds = _validate_public_funds(funds)
    validated_adjustments = _validate_global_split_adjustments(adjustments)
    return indexed, validated_funds, validated_adjustments


def _validate_public_stock(
    payload: object,
    row: dict[str, object],
    funds: dict[int, dict[str, object]],
    global_adjustments: dict[str, list[dict[str, object]]],
) -> None:
    if type(payload) is not dict:
        raise InsiderPublicationPolicyPlanningError("public stock")
    keys = set(payload)
    equity = row.get("instrument_type") == "EQUITY"
    equity_keys_valid = (
        keys == _PUBLIC_STOCK_BASE_KEYS or keys == _PUBLIC_EQUITY_STOCK_KEYS
    )
    if (
        (equity and not equity_keys_valid)
        or (not equity and keys != _PUBLIC_STOCK_BASE_KEYS)
        or type(payload["holders"]) is not list
        or (
            "split_adjustments" in payload
            and type(payload["split_adjustments"]) is not list
        )
    ):
        raise InsiderPublicationPolicyPlanningError("public stock")
    for field in ("stock_id", "cusip", "ticker", "issuer", "instrument_type"):
        if payload[field] != row[field]:
            raise InsiderPublicationPolicyPlanningError("public stock")
    holders = _validate_public_holders(payload["holders"], funds)
    _require_stock_index_metadata_match(row, holders, funds)
    stock_id = row["stock_id"]
    assert isinstance(stock_id, str)
    if equity:
        emitted_adjustments = _validate_split_adjustments(
            payload.get("split_adjustments", []),
            allow_empty=True,
            label="public stock",
        )
        recomputed_adjustments = _infer_proven_split_adjustments(holders)
        if (
            emitted_adjustments != recomputed_adjustments
            or global_adjustments.get(stock_id, []) != recomputed_adjustments
        ):
            raise InsiderPublicationPolicyPlanningError("public stock")
    elif stock_id in global_adjustments:
        raise InsiderPublicationPolicyPlanningError("public stock")


class _RestoredPublicIndexSnapshot:
    __slots__ = (
        "public_index",
        "repository_path",
        "repository_descriptor",
        "data_descriptor",
        "stocks_descriptor",
        "file_anchors",
    )

    def __init__(
        self,
        *,
        public_index: dict[str, object],
        repository_path: Path,
        repository_descriptor: int,
        data_descriptor: int,
        stocks_descriptor: int,
        file_anchors: tuple[tuple[int, str, tuple[int, int, int, int, int], str], ...],
    ) -> None:
        self.public_index = public_index
        self.repository_path = repository_path
        self.repository_descriptor = repository_descriptor
        self.data_descriptor = data_descriptor
        self.stocks_descriptor = stocks_descriptor
        self.file_anchors = file_anchors

    def validate(self) -> None:
        _require_directory_path_identity(
            self.repository_path,
            self.repository_descriptor,
            "repository root",
        )
        _require_directory_identity_at(
            self.repository_descriptor,
            "data",
            self.data_descriptor,
            "public data directory",
        )
        _require_directory_identity_at(
            self.data_descriptor,
            "stocks",
            self.stocks_descriptor,
            "public stocks directory",
        )
        for descriptor, name, identity, label in self.file_anchors:
            _require_named_identity(descriptor, name, identity, label)

    def close(self) -> None:
        os.close(self.stocks_descriptor)
        os.close(self.data_descriptor)
        os.close(self.repository_descriptor)


def _open_restored_public_index(
    repository_root: Path,
    mapping_spec: object,
) -> _RestoredPublicIndexSnapshot:
    if (
        type(mapping_spec) is not dict
        or not mapping_spec
        or len(mapping_spec) > MAX_INSIDER_STATE_COLLECTION
    ):
        raise InsiderPublicationPolicyPlanningError("mapping specification")
    repository_descriptor = _open_directory(repository_root, "repository root")
    data_descriptor = -1
    stocks_descriptor = -1
    success = False
    try:
        data_descriptor = _open_directory_at(
            repository_descriptor,
            "data",
            "public data directory",
        )
        stocks_descriptor = _open_directory_at(
            data_descriptor,
            "stocks",
            "public stocks directory",
        )
        index_rendered, index_identity = _read_bounded_regular_file_at(
            data_descriptor,
            "index.json",
            maximum=MAX_PUBLIC_INDEX_BYTES,
            label="public index",
            owner_only=False,
        )
        indexed_rows, validated_funds, validated_adjustments = _validate_public_index(
            _strict_json_bytes(index_rendered, "public index")
        )
        public_index: dict[str, object] = {}
        anchors: list[tuple[int, str, tuple[int, int, int, int, int], str]] = [
            (data_descriptor, "index.json", index_identity, "public index")
        ]
        seen_stock_ids: set[str] = set()
        for metadata in mapping_spec.values():
            if type(metadata) is not dict or type(metadata.get("stockId")) is not str:
                raise InsiderPublicationPolicyPlanningError("mapping specification")
            stock_id = metadata["stockId"]
            row = indexed_rows.get(stock_id)
            if row is None:
                raise InsiderPublicationPolicyPlanningError("public index")
            if stock_id not in seen_stock_ids:
                stock_name = f"{stock_file_stem(stock_id)}.json"
                stock_rendered, stock_identity = _read_bounded_regular_file_at(
                    stocks_descriptor,
                    stock_name,
                    maximum=MAX_PUBLIC_STOCK_BYTES,
                    label="public stock",
                    owner_only=False,
                )
                _validate_public_stock(
                    _strict_json_bytes(stock_rendered, "public stock"),
                    row,
                    validated_funds,
                    validated_adjustments,
                )
                anchors.append(
                    (
                        stocks_descriptor,
                        stock_name,
                        stock_identity,
                        "public stock",
                    )
                )
                seen_stock_ids.add(stock_id)
            public_index[stock_id] = {
                "stockId": row["stock_id"],
                "fileStem": stock_file_stem(stock_id),
                "ticker": row["ticker"],
                "companyName": row["issuer"],
                "securityType": metadata.get("securityType"),
                "securityTypeLabel": metadata.get("securityTypeLabel"),
                "cusip": row["cusip"],
                "primary": metadata.get("primary"),
            }
        snapshot = _RestoredPublicIndexSnapshot(
            public_index=public_index,
            repository_path=repository_root,
            repository_descriptor=repository_descriptor,
            data_descriptor=data_descriptor,
            stocks_descriptor=stocks_descriptor,
            file_anchors=tuple(anchors),
        )
        success = True
        return snapshot
    finally:
        if not success:
            if stocks_descriptor >= 0:
                os.close(stocks_descriptor)
            if data_descriptor >= 0:
                os.close(data_descriptor)
            os.close(repository_descriptor)


def _write_candidate(
    review_descriptor: int,
    output_filename: str,
    rendered: bytes,
) -> tuple[int, int, int, int, int]:
    try:
        output_descriptor = os.open(
            output_filename,
            _OUTPUT_FLAGS,
            0o600,
            dir_fd=review_descriptor,
        )
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError("candidate output") from error
    try:
        try:
            os.fchmod(output_descriptor, 0o600)
            metadata = os.fstat(output_descriptor)
            if not _is_owner_only_regular(metadata):
                raise InsiderPublicationPolicyPlanningError("candidate output")
            _write_all(output_descriptor, rendered)
            metadata = os.fstat(output_descriptor)
            if not _is_owner_only_regular(metadata):
                raise InsiderPublicationPolicyPlanningError("candidate output")
            written_identity = _metadata_identity(metadata)
            os.fsync(output_descriptor)
            metadata = os.fstat(output_descriptor)
            if (
                not _is_owner_only_regular(metadata)
                or _metadata_identity(metadata) != written_identity
            ):
                raise InsiderPublicationPolicyPlanningError("candidate output")
            output_identity = _metadata_identity(metadata)
        except OSError as error:
            raise InsiderPublicationPolicyPlanningError("candidate output") from error
    finally:
        os.close(output_descriptor)
    _require_named_identity(
        review_descriptor,
        output_filename,
        output_identity,
        "candidate output",
        owner_only=True,
    )
    try:
        os.fsync(review_descriptor)
    except OSError as error:
        raise InsiderPublicationPolicyPlanningError("candidate output") from error
    return output_identity


def plan_servicenow_publication_policy(
    *,
    repository_root: Path,
    issuer_cik: str,
    review_directory: Path,
    mapping_spec_name: str,
    output_name: str,
) -> dict[str, object]:
    """Write one exact owner-reviewed candidate and return bounded metadata."""

    if type(issuer_cik) is not str or issuer_cik != SERVICENOW_CIK:
        raise InsiderPublicationPolicyPlanningError("issuer CIK")
    mapping_filename = _single_component(mapping_spec_name, "mapping specification")
    output_filename = _single_component(output_name, "candidate output")
    root = Path(repository_root)
    review_path = Path(review_directory)
    review_descriptor = _open_owner_only_directory(review_path)
    try:
        mapping_rendered, mapping_identity = _read_bounded_regular_file_at(
            review_descriptor,
            mapping_filename,
            maximum=MAX_MAPPING_SPEC_BYTES,
            label="mapping specification",
            owner_only=True,
        )
        mapping_spec = _strict_json_bytes(
            mapping_rendered,
            "mapping specification",
        )
        state_store = InsiderStateStore(root)
        issuer_state = _read_canonical_issuer_state(state_store)
        initial_generation = issuer_state.get("generation_digest")
        initial_classes = _security_class_keys(issuer_state)
        public_snapshot = _open_restored_public_index(root, mapping_spec)
        try:
            candidate = build_servicenow_publication_policy(
                issuer_state=issuer_state,
                mapping_spec=mapping_spec,
                public_index=public_snapshot.public_index,
            )
            final_state = _read_canonical_issuer_state(state_store)
            if (
                final_state.get("generation_digest") != initial_generation
                or _security_class_keys(final_state) != initial_classes
            ):
                raise InsiderPublicationPolicyPlanningError("issuer state changed")
            rendered = canonical_insider_state_json_bytes(candidate)
            if len(rendered) > MAX_POLICY_CANDIDATE_BYTES:
                raise InsiderPublicationPolicyPlanningError("candidate output")
            public_snapshot.validate()
            _require_directory_path_identity(
                review_path,
                review_descriptor,
                "review directory",
                owner_only=True,
            )
            _require_named_identity(
                review_descriptor,
                mapping_filename,
                mapping_identity,
                "mapping specification",
                owner_only=True,
            )
            candidate_identity = _write_candidate(
                review_descriptor,
                output_filename,
                rendered,
            )
            public_snapshot.validate()
            _require_named_identity(
                review_descriptor,
                mapping_filename,
                mapping_identity,
                "mapping specification",
                owner_only=True,
            )
            _require_directory_path_identity(
                review_path,
                review_descriptor,
                "review directory",
                owner_only=True,
            )
            completed_state = _read_canonical_issuer_state(state_store)
            if (
                completed_state.get("generation_digest") != initial_generation
                or _security_class_keys(completed_state) != initial_classes
            ):
                raise InsiderPublicationPolicyPlanningError("issuer state changed")
            public_snapshot.validate()
            _require_named_identity(
                review_descriptor,
                mapping_filename,
                mapping_identity,
                "mapping specification",
                owner_only=True,
            )
            _require_directory_path_identity(
                review_path,
                review_descriptor,
                "review directory",
                owner_only=True,
            )
            _require_named_identity(
                review_descriptor,
                output_filename,
                candidate_identity,
                "candidate output",
                owner_only=True,
            )
        finally:
            public_snapshot.close()
    finally:
        os.close(review_descriptor)

    generation_digest = issuer_state["generation_digest"]
    assert isinstance(generation_digest, str)
    return {
        "candidate_policy_sha256": publication_policy_sha256(candidate),
        "issuer_cik": SERVICENOW_CIK,
        "issuer_generation_digest": generation_digest,
        "security_class_count": len(initial_classes),
    }


def main(argv: list[str] | None = None) -> int:
    """Run the fixed-scope planner with bounded, non-sensitive output."""

    raw_arguments = list(sys.argv[1:]) if argv is None else argv
    try:
        arguments = _parse_arguments(raw_arguments)
    except (InsiderPublicationPolicyPlanningError, TypeError, ValueError):
        sys.stderr.write("private insider policy planning configuration is invalid\n")
        return 2

    try:
        result = plan_servicenow_publication_policy(
            repository_root=Path(arguments.repository_root),
            issuer_cik=arguments.issuer_cik,
            review_directory=Path(arguments.review_directory),
            mapping_spec_name=arguments.mapping_spec,
            output_name=arguments.output,
        )
    except Exception:
        sys.stderr.write("private insider policy planning failed\n")
        return 1

    sys.stdout.write(
        json.dumps(
            result,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    )
    return 0


__all__ = [
    "InsiderPublicationPolicyPlanningError",
    "main",
    "plan_servicenow_publication_policy",
]


if __name__ == "__main__":
    raise SystemExit(main())
