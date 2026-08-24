from __future__ import annotations

import copy
import hashlib
import json
import unittest

from insider_metrics import (
    InsiderMetricsError,
    build_insider_metric_projection,
)


AS_OF = "2026-06-30T20:45:00Z"
SECURITY_ID = "03770N101"
OWNER_A = "a" * 64
OWNER_B = "b" * 64
OWNER_C = "c" * 64


def owner_group(
    key: str,
    name: str,
    *,
    roles: tuple[str, ...] = ("Officer",),
    title: str = "CFO",
    owner_count: int = 1,
) -> dict[str, object]:
    if len(key) != 64:
        raise ValueError("test owner key must be 64 characters")
    return {
        "displayName": name,
        "ownerCount": owner_count,
        "roles": list(roles),
        "primaryTitle": title,
        "isJoint": owner_count > 1,
    }


def transaction_row(
    row_key: str,
    *,
    owner: dict[str, object] | None = None,
    owner_key: str | None = None,
    accession: str = "0000000001-26-000001",
    transaction_date: str = "2026-06-01",
    accepted_at: str = "2026-06-02T16:00:00Z",
    code: str | None = "P",
    shares: str | None = "10",
    price: str | None = "10",
    value: str | None = "100",
    post_shares: str | None = "110",
    acquired_disposed: str | None = "A",
    plan_status: str = "not_marked",
    source_row_index: int = 0,
    is_superseded: bool = False,
    is_amended: bool = False,
    direct_indirect: str | None = "D",
    footnote_ids: tuple[str, ...] = (),
    form_type: str = "4",
    timeliness: str | None = None,
    security_id: str = SECURITY_ID,
) -> dict[str, object]:
    category = {"P": "purchase", "S": "sale"}.get(code, "other")
    label = {"P": "PURCHASE (P)", "S": "SALE (S)"}.get(
        code,
        f"OTHER ({code or '?'})",
    )
    owner_value = dict(owner or owner_group(OWNER_A, "Alex Example"))
    private_owner_key = (
        owner_key
        or hashlib.sha256(
            b"sis-filing-owner-v1\0" + accession.encode("ascii")
        ).hexdigest()
    )
    return {
        "privateRowKey": row_key,
        "accessionNumber": accession,
        "privateOwnerGroupKey": private_owner_key,
        "ownerGroup": owner_value,
        "securityId": security_id,
        "privateSourceTable": "non_derivative",
        "privateSourceRowIndex": source_row_index,
        "transactionDate": transaction_date,
        "deemedExecutionDate": None,
        "transactionCode": code,
        "transactionLabel": label,
        "normalizedCategory": category,
        "acquiredDisposedCode": acquired_disposed,
        "shares": shares,
        "pricePerShare": price,
        "priceIsWeightedAverage": False,
        "value": value,
        "valueMethod": (
            "calculated_shares_times_price" if value is not None else "unavailable"
        ),
        "postTransactionShares": post_shares,
        "directIndirectOwnership": direct_indirect,
        "planStatus": plan_status,
        "transactionTimeliness": timeliness,
        "isAmended": is_amended,
        "isSuperseded": is_superseded,
        "formType": form_type,
        "filingDate": accepted_at[:10],
        "acceptedAt": accepted_at,
        "secDocumentUrl": (
            "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/ownership.xml"
        ),
        "privateFootnoteIds": list(footnote_ids),
    }


class InsiderMetricProjectionTests(unittest.TestCase):
    def test_default_metrics_are_exact_grouped_and_reconciled(self) -> None:
        joint = owner_group(
            OWNER_A,
            "Alex and Blair Example",
            roles=("Officer", "Director"),
            owner_count=2,
        )
        rows = [
            transaction_row(
                "p-one",
                owner=joint,
                shares="10",
                value="100",
                post_shares="130",
                source_row_index=0,
            ),
            transaction_row(
                "p-two",
                owner=joint,
                shares="20",
                value="200",
                post_shares="130",
                source_row_index=1,
            ),
            transaction_row(
                "p-missing",
                owner=owner_group(OWNER_B, "Casey Example", title="CEO"),
                transaction_date="2026-05-01",
                shares=None,
                price=None,
                value=None,
                post_shares=None,
                source_row_index=0,
                accession="0000000001-26-000002",
            ),
            transaction_row(
                "s-marked",
                owner=owner_group(OWNER_C, "Devon Example", roles=("Director",)),
                accession="0000000001-26-000003",
                transaction_date="2026-04-01",
                code="S",
                acquired_disposed="D",
                shares="90",
                value="900",
                post_shares="10",
                plan_status="filing_marked",
            ),
            transaction_row(
                "s-unmarked",
                accession="0000000001-26-000004",
                transaction_date="2026-03-01",
                code="S",
                acquired_disposed="D",
                shares="10",
                value="100",
                post_shares="90",
                plan_status="not_marked",
            ),
            transaction_row(
                "s-unknown-plan",
                accession="0000000001-26-000005",
                transaction_date="2026-02-01",
                code="S",
                acquired_disposed="D",
                shares="5",
                value="50",
                post_shares="95",
                plan_status="unknown",
            ),
            transaction_row(
                "outside-window",
                accession="0000000001-25-000001",
                transaction_date="2025-06-29",
                value="9999",
            ),
            transaction_row(
                "other-code",
                accession="0000000001-26-000006",
                transaction_date="2026-06-10",
                code="A",
                acquired_disposed="A",
                value="5000",
            ),
            transaction_row(
                "superseded-sale",
                accession="0000000001-26-000007",
                transaction_date="2026-06-20",
                code="S",
                acquired_disposed="D",
                value="7777",
                is_superseded=True,
            ),
        ]

        result = build_insider_metric_projection(
            rows,
            security_id=SECURITY_ID,
            as_of=AS_OF,
        )

        self.assertEqual("2025-06-30", result["filters"]["start"])
        self.assertEqual("2026-06-30", result["filters"]["end"])
        purchases = result["summary"]["purchases"]
        sales = result["summary"]["sales"]
        net = result["summary"]["netPS"]
        self.assertEqual(
            {
                "value": "300",
                "transactionCount": 2,
                "ownerGroupCount": 2,
                "knownValueCount": 1,
                "missingValueCount": 1,
            },
            {
                key: purchases[key]
                for key in (
                    "value",
                    "transactionCount",
                    "ownerGroupCount",
                    "knownValueCount",
                    "missingValueCount",
                )
            },
        )
        self.assertEqual("1050", sales["value"])
        self.assertEqual(3, sales["transactionCount"])
        self.assertEqual("0.9", sales["planMarkedKnownValuePercentage"])
        self.assertEqual(1, sales["unknownPlanStatusCount"])
        self.assertEqual("-750", net["value"])
        self.assertEqual("net_reported_selling", net["direction"])
        self.assertEqual("ratio", net["ratioState"])
        self.assertEqual("3.5", net["salesToPurchasesRatio"])

        latest = result["summary"]["latestMeaningfulTransaction"]
        self.assertEqual("P", latest["transactionCode"])
        self.assertEqual(2, latest["ownerGroup"]["ownerCount"])
        self.assertEqual("300", latest["value"])
        self.assertEqual(2, latest["transactionLegCount"])

        table_items = result["transactions"]["items"]
        chart_events = result["chartEvents"]
        self.assertEqual(5, result["transactions"]["total"])
        self.assertEqual(
            {
                (item["accessionNumber"], item["transactionDate"], item["value"])
                for item in table_items
            },
            {
                (item["accessionNumber"], item["transactionDate"], item["value"])
                for item in chart_events
            },
        )
        self.assertEqual(
            "300",
            result["sidebar"]["topBuyers"][0]["value"],
        )
        self.assertEqual(
            "900",
            result["sidebar"]["topSellers"][0]["value"],
        )
        self.assertEqual(
            "900",
            result["sidebar"]["rule10b51"]["planMarkedSalesValue"],
        )
        self.assertEqual(
            1,
            result["sidebar"]["rule10b51"]["distinctOwnerGroupCount"],
        )
        self.assertEqual(1, result["dataQuality"]["missingValueTransactionCount"])
        self.assertEqual(1, result["dataQuality"]["unknownPlanStatusSaleCount"])
        self.assertTrue(result["dataQuality"]["partial"])

    def test_owner_roles_use_stable_company_semantic_order(self) -> None:
        row = transaction_row(
            "role-order",
            owner=owner_group(
                OWNER_A,
                "Alex Example",
                roles=("Other", "TenPercentOwner", "Director", "Officer"),
            ),
        )

        result = build_insider_metric_projection(
            [row],
            security_id=SECURITY_ID,
            as_of=AS_OF,
        )

        self.assertEqual(
            ["Officer", "Director", "TenPercentOwner", "Other"],
            result["transactions"]["items"][0]["ownerGroup"]["roles"],
        )

    def test_identically_displayed_owner_groups_remain_distinct_without_key_leak(
        self,
    ) -> None:
        rows = [
            transaction_row(
                "same-display-a",
                owner=owner_group(OWNER_A, "Same Person", title="CFO"),
                owner_key=OWNER_A,
                value="100",
            ),
            transaction_row(
                "same-display-b",
                owner=owner_group(OWNER_B, "Same Person", title="CFO"),
                owner_key=OWNER_B,
                accession="0000000001-26-000002",
                value="200",
            ),
        ]

        result = build_insider_metric_projection(
            rows,
            security_id=SECURITY_ID,
            as_of=AS_OF,
        )

        self.assertEqual(2, result["summary"]["purchases"]["ownerGroupCount"])
        self.assertEqual(2, len(result["sidebar"]["topBuyers"]))
        rendered = json.dumps(result, sort_keys=True)
        for forbidden in (
            "displayGroupKey",
            "footnoteIds",
            "ownerGroupKey",
            "rowKey",
            "sourceRowIndex",
            "sourceRowKeys",
            "sourceTable",
        ):
            self.assertNotIn(forbidden, rendered)
        self.assertNotIn(OWNER_A, rendered)
        self.assertNotIn(OWNER_B, rendered)

    def test_filters_and_sorting_apply_before_every_projection(self) -> None:
        rows = [
            transaction_row(
                "officer-purchase",
                owner=owner_group(OWNER_A, "Alpha Officer", roles=("Officer",)),
                transaction_date="2026-05-01",
                value="100",
            ),
            transaction_row(
                "director-sale",
                owner=owner_group(OWNER_B, "Beta Director", roles=("Director",)),
                accession="0000000001-26-000002",
                transaction_date="2026-05-02",
                code="S",
                acquired_disposed="D",
                plan_status="filing_marked",
                value="300",
            ),
            transaction_row(
                "ten-percent-sale",
                owner=owner_group(
                    OWNER_C,
                    "Gamma Capital",
                    roles=("TenPercentOwner", "Other"),
                    title="10% Owner",
                ),
                accession="0000000001-26-000003",
                transaction_date="2026-05-03",
                code="S",
                acquired_disposed="D",
                plan_status="filing_marked",
                value="200",
            ),
            transaction_row(
                "other-officer",
                owner=owner_group(OWNER_A, "Alpha Officer", roles=("Officer",)),
                accession="0000000001-26-000004",
                transaction_date="2026-05-04",
                code="A",
                value="1000",
            ),
        ]

        result = build_insider_metric_projection(
            rows,
            security_id=SECURITY_ID,
            as_of=AS_OF,
            query={
                "transactionScope": "all",
                "ownerScope": "officers-directors",
                "plan": "10b5-1",
                "search": "beta",
                "start": "2026-05-01",
                "end": "2026-05-31",
                "sort": "value",
                "order": "asc",
            },
        )

        self.assertEqual(1, result["transactions"]["total"])
        self.assertEqual(
            "0000000001-26-000002",
            result["transactions"]["items"][0]["accessionNumber"],
        )
        self.assertEqual("0", result["summary"]["purchases"]["value"])
        self.assertEqual("300", result["summary"]["sales"]["value"])
        self.assertEqual(
            result["transactions"]["items"][0]["accessionNumber"],
            result["chartEvents"][0]["accessionNumber"],
        )
        self.assertEqual(
            "Beta Director",
            result["sidebar"]["topSellers"][0]["displayName"],
        )
        self.assertNotIn("ownerGroupKey", result["sidebar"]["topSellers"][0])

        without_standalone_ten_percent = build_insider_metric_projection(
            rows,
            security_id=SECURITY_ID,
            as_of=AS_OF,
            query={"includeTenPercentOwners": False},
        )
        owner_names = {
            item["ownerGroup"]["displayName"]
            for item in without_standalone_ten_percent["transactions"]["items"]
        }
        self.assertNotIn("Gamma Capital", owner_names)
        self.assertIn("Alpha Officer", owner_names)
        self.assertIn("Beta Director", owner_names)

    def test_cursor_is_stable_query_bound_and_lossless(self) -> None:
        rows = [
            transaction_row(
                f"row-{index:02d}",
                accession=f"0000000001-26-{index + 1:06d}",
                transaction_date=f"2026-05-{(index % 28) + 1:02d}",
                source_row_index=index,
                value=str(index + 1),
            )
            for index in range(26)
        ]

        first = build_insider_metric_projection(
            rows,
            security_id=SECURITY_ID,
            as_of=AS_OF,
            query={"limit": 25},
        )
        first_again = build_insider_metric_projection(
            reversed(rows),
            security_id=SECURITY_ID,
            as_of=AS_OF,
            query={"limit": 25},
        )
        self.assertEqual(first, first_again)
        self.assertEqual(25, len(first["transactions"]["items"]))
        cursor = first["transactions"]["nextCursor"]
        self.assertRegex(cursor, r"^v1\.[0-9a-f]{16}\.25$")

        second = build_insider_metric_projection(
            rows,
            security_id=SECURITY_ID,
            as_of=AS_OF,
            query={"limit": 25, "cursor": cursor},
        )
        self.assertEqual(1, len(second["transactions"]["items"]))
        self.assertIsNone(second["transactions"]["nextCursor"])
        self.assertEqual(
            26,
            len(
                {
                    item["accessionNumber"]
                    for item in (
                        first["transactions"]["items"] + second["transactions"]["items"]
                    )
                }
            ),
        )

        with self.assertRaisesRegex(InsiderMetricsError, "cursor"):
            build_insider_metric_projection(
                rows,
                security_id=SECURITY_ID,
                as_of=AS_OF,
                query={
                    "limit": 25,
                    "cursor": cursor,
                    "ownerScope": "officers-directors",
                },
            )

    def test_percent_change_states_are_never_inferred_from_ambiguous_rows(self) -> None:
        rows = [
            transaction_row(
                "purchase",
                shares="20",
                post_shares="120",
                value="200",
            ),
            transaction_row(
                "sale",
                accession="0000000001-26-000002",
                transaction_date="2026-05-01",
                code="S",
                acquired_disposed="D",
                shares="15",
                post_shares="85",
                value="150",
            ),
            transaction_row(
                "new-position",
                accession="0000000001-26-000003",
                transaction_date="2026-04-01",
                shares="10",
                post_shares="10",
                value="100",
            ),
            transaction_row(
                "ambiguous-a",
                accession="0000000001-26-000004",
                transaction_date="2026-03-01",
                shares="5",
                post_shares="20",
                value="50",
                source_row_index=0,
            ),
            transaction_row(
                "ambiguous-b",
                accession="0000000001-26-000004",
                transaction_date="2026-03-01",
                shares="5",
                post_shares="20",
                value="50",
                source_row_index=1,
            ),
        ]

        result = build_insider_metric_projection(
            rows,
            security_id=SECURITY_ID,
            as_of=AS_OF,
        )
        by_accession = {
            item["accessionNumber"]: item for item in result["transactions"]["items"]
        }
        self.assertEqual("0.2", by_accession["0000000001-26-000001"]["percentChange"])
        self.assertEqual(
            "known",
            by_accession["0000000001-26-000001"]["percentChangeState"],
        )
        self.assertEqual("-0.15", by_accession["0000000001-26-000002"]["percentChange"])
        self.assertEqual(
            "new", by_accession["0000000001-26-000003"]["percentChangeState"]
        )
        self.assertIsNone(by_accession["0000000001-26-000003"]["percentChange"])
        ambiguous = by_accession["0000000001-26-000004"]
        self.assertEqual(2, ambiguous["transactionLegCount"])
        self.assertEqual("unavailable", ambiguous["percentChangeState"])
        self.assertIsNone(ambiguous["percentChange"])

    def test_freshness_status_uses_an_explicit_bounded_threshold(self) -> None:
        row = transaction_row("freshness")
        for sync_at, expected_status in (
            ("2026-06-30T20:00:00Z", "current"),
            ("2026-06-30T18:00:00Z", "stale"),
        ):
            with self.subTest(sync_at=sync_at):
                result = build_insider_metric_projection(
                    [row],
                    security_id=SECURITY_ID,
                    as_of=AS_OF,
                    quality={
                        "freshnessMaxAgeSeconds": 3600,
                        "latestSuccessfulSyncAt": sync_at,
                    },
                )
                self.assertEqual(expected_status, result["dataFreshness"]["status"])
                self.assertEqual(
                    3600,
                    result["dataFreshness"]["secFreshnessThresholdSeconds"],
                )

        unknown = build_insider_metric_projection(
            [row],
            security_id=SECURITY_ID,
            as_of=AS_OF,
            quality={"freshnessMaxAgeSeconds": 3600},
        )
        self.assertEqual("unknown", unknown["dataFreshness"]["status"])

    def test_freshness_rejects_future_malformed_and_unbounded_policy_inputs(
        self,
    ) -> None:
        row = transaction_row("freshness-invalid")
        invalid_quality = (
            {
                "freshnessMaxAgeSeconds": 3600,
                "latestSuccessfulSyncAt": "2026-06-30T21:00:00Z",
            },
            {
                "freshnessMaxAgeSeconds": 3600,
                "latestSuccessfulSyncAt": "not-a-timestamp",
            },
            {"freshnessMaxAgeSeconds": 0},
            {"freshnessMaxAgeSeconds": 604801},
        )
        for quality in invalid_quality:
            with self.subTest(quality=quality), self.assertRaises(InsiderMetricsError):
                build_insider_metric_projection(
                    [row],
                    security_id=SECURITY_ID,
                    as_of=AS_OF,
                    quality=quality,
                )

    def test_invalid_unknown_or_inexact_inputs_fail_closed(self) -> None:
        invalid_rows = []
        for field, value in (
            ("value", 100.0),
            ("planStatus", None),
            ("isSuperseded", 0),
            ("securityId", "not the page security"),
        ):
            row = transaction_row("bad")
            row[field] = value
            invalid_rows.append((field, row))

        for field, row in invalid_rows:
            with self.subTest(field=field):
                with self.assertRaises(InsiderMetricsError):
                    build_insider_metric_projection(
                        [row],
                        security_id=SECURITY_ID,
                        as_of=AS_OF,
                    )

        duplicate = transaction_row("duplicate")
        with self.assertRaisesRegex(InsiderMetricsError, "privateRowKey"):
            build_insider_metric_projection(
                [duplicate, copy.deepcopy(duplicate)],
                security_id=SECURITY_ID,
                as_of=AS_OF,
            )


if __name__ == "__main__":
    unittest.main()
