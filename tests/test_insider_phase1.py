import hashlib
import json
import struct
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX_HTML = ROOT / "index.html"


class InsiderPhase1SemanticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = INDEX_HTML.read_text(encoding="utf-8")

    def test_preview_flag_is_local_and_default_off(self) -> None:
        start = self.html.index("function insiderPreviewEnabled(")
        end = self.html.index("// ---------- URL routing ----------", start)
        completed = subprocess.run(
            [
                "node",
                "-e",
                self.html[start:end]
                + """
                const cases = {
                  localOptIn: insiderPreviewEnabled({
                    hostname: "localhost",
                    search: "?insiderPreview=fixture",
                  }),
                  loopbackOptIn: insiderPreviewEnabled({
                    hostname: "127.0.0.1",
                    search: "?insiderPreview=fixture",
                  }),
                  localDefault: insiderPreviewEnabled({
                    hostname: "localhost",
                    search: "",
                  }),
                  deployedOptIn: insiderPreviewEnabled({
                    hostname: "13f.wesleyyon.com",
                    search: "?insiderPreview=fixture",
                  }),
                  wrongValue: insiderPreviewEnabled({
                    hostname: "localhost",
                    search: "?insiderPreview=true",
                  }),
                };
                console.log(JSON.stringify(cases));
                """,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            {
                "localOptIn": True,
                "loopbackOptIn": True,
                "localDefault": False,
                "deployedOptIn": False,
                "wrongValue": False,
            },
            json.loads(completed.stdout),
        )

    def test_fixture_path_requires_exact_canonical_apge_security_identity(self) -> None:
        preview_start = self.html.index("function insiderPreviewEnabled(")
        preview_end = self.html.index(
            "// ---------- URL routing ----------", preview_start
        )
        fixture_constant_start = self.html.index("const INSIDER_FIXTURE_PATH =")
        fixture_constant_end = self.html.index("\n", fixture_constant_start) + 1
        helper_start = self.html.index("function insiderFixturePathForStock(")
        helper_end = self.html.index("function insiderQueryState(", helper_start)
        completed = subprocess.run(
            [
                "node",
                "-e",
                self.html[preview_start:preview_end]
                + self.html[fixture_constant_start:fixture_constant_end]
                + self.html[helper_start:helper_end]
                + """
                global.location = {
                  hostname: "localhost",
                  search: "?insiderPreview=fixture",
                };
                const cases = {
                  exact: insiderFixturePathForStock("03770n101", {
                    stock_id: "03770N101",
                    cusip: "03770N101",
                    ticker: "APGE",
                  }),
                  tickerAliasOnly: insiderFixturePathForStock("APGE", {
                    ticker: "APGE",
                    issuer: "Apogee Therapeutics",
                  }),
                  wrongCanonicalIdentityWithApgeTicker: insiderFixturePathForStock("99999X999", {
                    stock_id: "99999X999",
                    cusip: "99999X999",
                    ticker: "APGE",
                  }),
                  conflictingMetadataWithApgeTicker: insiderFixturePathForStock("03770N101", {
                    stock_id: "99999X999",
                    cusip: "99999X999",
                    ticker: "APGE",
                  }),
                  nameAliasOnly: insiderFixturePathForStock("APOGEE THERAPEUTICS", {
                    issuer: "Apogee Therapeutics",
                  }),
                };
                global.location = { hostname: "localhost", search: "" };
                cases.defaultOff = insiderFixturePathForStock("03770N101", {
                  stock_id: "03770N101", cusip: "03770N101",
                });
                global.location = {
                  hostname: "example.test",
                  search: "?insiderPreview=fixture",
                };
                cases.nonLoopback = insiderFixturePathForStock("03770N101", {
                  stock_id: "03770N101", cusip: "03770N101",
                });
                console.log(JSON.stringify(cases));
                """,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            {
                "exact": "13f-insider-activity-prd/fixtures/apge-insider-activity.example.json",
                "tickerAliasOnly": None,
                "wrongCanonicalIdentityWithApgeTicker": None,
                "conflictingMetadataWithApgeTicker": None,
                "nameAliasOnly": None,
                "defaultOff": None,
                "nonLoopback": None,
            },
            json.loads(completed.stdout),
        )

    def test_live_insider_security_paths_are_canonical_and_bounded(self) -> None:
        start = self.html.index("const safeTicker =")
        end = self.html.index("function holdingHistoryKey(", start)
        completed = subprocess.run(
            [
                "node",
                "-e",
                self.html[start:end]
                + """
                const available = typeof insiderSecurityPathForStock === "function";
                const cases = available ? {
                  equity: insiderSecurityPathForStock("03770n101"),
                  derivative: insiderSecurityPathForStock("29273v100|call"),
                  dotTicker: insiderSecurityPathForStock("BRK.B"),
                  traversal: insiderSecurityPathForStock("../03770N101"),
                  unknownInstrument: insiderSecurityPathForStock("03770N101|SWAP"),
                  overlong: insiderSecurityPathForStock("A".repeat(161)),
                } : {};
                console.log(JSON.stringify({ available, cases }));
                """,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            {
                "available": True,
                "cases": {
                    "equity": ("data/insiders/public/securities/03770N101.json"),
                    "derivative": (
                        "data/insiders/public/securities/29273V100__CALL.json"
                    ),
                    "dotTicker": "data/insiders/public/securities/BRK.B.json",
                    "traversal": None,
                    "unknownInstrument": None,
                    "overlong": None,
                },
            },
            json.loads(completed.stdout),
        )

    def test_stock_subroutes_are_live_without_enabling_fixture_preview(self) -> None:
        start = self.html.index("function parseAppRoute(")
        end = self.html.index("function wireUrlRouting(", start)
        completed = subprocess.run(
            [
                "node",
                "-e",
                self.html[start:end]
                + """
                console.log(JSON.stringify({
                  holders: parseAppRoute("#stock/03770N101", true),
                  insiders: parseAppRoute(
                    "#stock/03770N101/insiders", true
                  ),
                  reporting: parseAppRoute(
                    "#stock/03770N101/reporting-insiders", true
                  ),
                  liveWithoutFixture: parseAppRoute(
                    "#stock/03770N101/insiders", false
                  ),
                  encoded: parseAppRoute("#stock/ABC%7CNOTE/insiders", false),
                  invalid: parseAppRoute("#stock/03770N101/not-a-view", false),
                }));
                """,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            {
                "holders": {
                    "kind": "stock",
                    "id": "03770N101",
                    "view": "holders",
                },
                "insiders": {
                    "kind": "stock",
                    "id": "03770N101",
                    "view": "insiders",
                },
                "reporting": {
                    "kind": "stock",
                    "id": "03770N101",
                    "view": "reporting-insiders",
                },
                "liveWithoutFixture": {
                    "kind": "stock",
                    "id": "03770N101",
                    "view": "insiders",
                },
                "encoded": {
                    "kind": "stock",
                    "id": "ABC|NOTE",
                    "view": "insiders",
                },
                "invalid": None,
            },
            json.loads(completed.stdout),
        )

    def test_stock_url_writer_preserves_live_insider_subroutes(self) -> None:
        preview_start = self.html.index("function insiderPreviewEnabled(")
        preview_end = self.html.index(
            "// ---------- URL routing ----------", preview_start
        )
        routing_start = self.html.index("const INSIDER_VIEW_QUERY_KEYS =")
        routing_end = self.html.index(
            "// ---------- curated browse lookups ----------", routing_start
        )
        completed = subprocess.run(
            [
                "node",
                "-e",
                self.html[preview_start:preview_end]
                + self.html[routing_start:routing_end]
                + """
                const writes = [];
                global.location = {
                  hostname: "13f.wesleyyon.com",
                  pathname: "/",
                  search: "",
                  hash: "",
                };
                global.history = {
                  pushState: (_state, _title, url) => writes.push(url),
                };
                setUrl("stock", "03770N101", "insiders");
                setUrl("stock", "ABC|NOTE", "reporting-insiders");
                setUrl("stock", "03770N101", "holders");
                console.log(JSON.stringify(writes));
                """,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            [
                "/#stock/03770N101/insiders",
                "/#stock/ABC%7CNOTE/reporting-insiders",
                "/#stock/03770N101",
            ],
            json.loads(completed.stdout),
        )

    def test_fixture_summary_markup_keeps_disclaimer_and_display_values(
        self,
    ) -> None:
        esc_start = self.html.index("const esc =")
        esc_end = self.html.index("function sec13fFilingsUrl", esc_start)
        start = self.html.index("// ---------- insider fixture UI ----------")
        end = self.html.index("// ---------- insider fixture loading ----------", start)
        helper_start = self.html.index("function exactWholeDisplay(", end)
        helper_end = self.html.index(
            "function insiderFixturePathForStock(", helper_start
        )
        completed = subprocess.run(
            [
                "node",
                "-e",
                self.html[esc_start:esc_end]
                + self.html[start:end]
                + self.html[helper_start:helper_end]
                + """
                let currentInsiderPayloadMode = "fixture";
                const fixture = JSON.parse(require("fs").readFileSync(
                  "13f-insider-activity-prd/fixtures/"
                    + "apge-insider-activity.example.json",
                  "utf8"
                ));
                const markup = insiderSummaryMarkup(fixture, "03770N101");
                console.log(JSON.stringify({
                  notice: markup.includes(fixture._fixtureNotice),
                  purchase: markup.includes("$1.81M"),
                  sale: markup.includes("$9.42M"),
                  net: markup.includes("-$7.61M"),
                  latest: markup.includes("Jane H. Smith"),
                  navigation: markup.includes(
                    '<nav class="security-tabs" aria-label="Security views">'
                  ),
                  current: markup.includes(
                    'aria-current="page">Insider Activity'
                  ),
                  tablist: markup.includes('role="tablist"'),
                  tab: markup.includes('role="tab"'),
                  selected: markup.includes('aria-selected='),
                }));
                """,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            {
                "notice": True,
                "purchase": True,
                "sale": True,
                "net": True,
                "latest": True,
                "navigation": True,
                "current": True,
                "tablist": False,
                "tab": False,
                "selected": False,
            },
            json.loads(completed.stdout),
        )

    def test_source_mockup_comparison_manifest_is_exact_and_auditable(
        self,
    ) -> None:
        manifest_path = (
            ROOT
            / "13f-insider-activity-prd/reference"
            / "phase1-implementation-comparison.json"
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(1, manifest["contract_version"])
        for key in ("source_mockup", "implementation_baseline"):
            item = manifest[key]
            artifact = ROOT / item["path"]
            payload = artifact.read_bytes()
            self.assertEqual(item["sha256"], hashlib.sha256(payload).hexdigest())
            self.assertEqual(b"\x89PNG\r\n\x1a\n", payload[:8])
            self.assertEqual(
                [item["width"], item["height"]],
                list(struct.unpack(">II", payload[16:24])),
            )
        self.assertEqual(
            {
                "fixture_disclaimer",
                "simplified_fixture_chart_and_markers",
                "fewer_visible_fixture_rows",
            },
            {item["id"] for item in manifest["accepted_differences"]},
        )

    def test_complete_fixture_markup_has_chart_table_rail_and_drawer_hooks(
        self,
    ) -> None:
        required_contracts = (
            'id="insiderPriceChart"',
            'aria-label="Share price and insider transaction chart"',
            'id="insiderTransactionTable"',
            '<button type="button" class="filing-detail-button"',
            'data-insider-accession="${esc(accession)}"',
            'class="insider-summary-rail"',
            'id="insiderDrawer"',
            'role="dialog"',
            "function openInsiderFiling(",
            "function closeInsiderDrawer(",
        )
        for contract in required_contracts:
            with self.subTest(contract=contract):
                self.assertIn(contract, self.html)
        self.assertNotRegex(
            self.html,
            r'<tr class="insider-row"[^>]*(?:tabindex|data-insider-accession)',
        )

    def test_plan_filter_is_url_backed_and_conservative(self) -> None:
        helper_start = self.html.index("function validFixtureIsoDate(")
        helper_end = self.html.index("function setInsiderQuery(", helper_start)
        filter_start = self.html.index("function filteredInsiderTransactions(")
        filter_end = self.html.index("function insiderChartMarkup(", filter_start)
        completed = subprocess.run(
            [
                "node",
                "-e",
                """
                const fixture = JSON.parse(require("fs").readFileSync(
                  "13f-insider-activity-prd/fixtures/"
                    + "apge-insider-activity.example.json",
                  "utf8"
                ));
                """
                + self.html[helper_start:helper_end]
                + self.html[filter_start:filter_end]
                + """
                let currentInsiderPayloadMode = "fixture";
                global.location = {
                  search: "?plan=10b5-1&transactionScope=ps"
                };
                const markedState = insiderQueryState(fixture);
                const marked = filteredInsiderTransactions(
                  fixture, markedState
                );
                global.location = {
                  search: "?plan=not-10b5-1&transactionScope=ps"
                };
                const unmarkedState = insiderQueryState(fixture);
                const unmarked = filteredInsiderTransactions(
                  fixture, unmarkedState
                );
                console.log(JSON.stringify({
                  markedPlan: markedState.plan,
                  marked: marked.map(row => row.id),
                  unmarkedPlan: unmarkedState.plan,
                  unmarked: unmarked.map(row => row.id),
                }));
                """,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            {
                "markedPlan": "10b5-1",
                "marked": ["txn_fixture_2", "txn_fixture_4"],
                "unmarkedPlan": "not-10b5-1",
                "unmarked": [
                    "txn_fixture_1",
                    "txn_fixture_3",
                    "txn_fixture_5",
                ],
            },
            json.loads(completed.stdout),
        )

    def test_date_filters_are_editable_and_url_backed(self) -> None:
        for contract in (
            'id="insiderStartDate" type="date"',
            'aria-label="Start date"',
            (
                "setInsiderQuery({start:this.value,end:"
                "document.getElementById('insiderEndDate').value})"
            ),
            'id="insiderEndDate" type="date"',
            'aria-label="End date"',
            (
                "setInsiderQuery({start:document.getElementById("
                "'insiderStartDate').value,end:this.value})"
            ),
        ):
            with self.subTest(contract=contract):
                self.assertIn(contract, self.html)

    def test_opted_in_holders_view_has_fixture_safe_cross_link(self) -> None:
        self.assertIn("Insider Activity — 90D", self.html)
        self.assertIn("Illustrative fixture preview · not real filing data", self.html)
        self.assertIn("View insider activity →", self.html)

    def test_data_derived_navigation_uses_delegated_actions(self) -> None:
        for inline_handler in (
            'onclick="loadStock(',
            'onclick="closeGlobalSearch(); loadStock(',
            'onclick="loadFund(',
            'onclick="closeGlobalSearch(); loadFund(',
        ):
            with self.subTest(inline_handler=inline_handler):
                self.assertNotIn(inline_handler, self.html)
        for delegated_contract in (
            "function wireDataActions()",
            'data-action="load-stock" data-stock-id="${esc(lookupId)}"',
            'data-action="load-fund" data-fund-cik="${esc(f.cik)}"',
            'data-action="load-fund" data-fund-cik="${esc(h.cik)}"',
            "CIK ${esc(f.cik)}",
        ):
            with self.subTest(delegated_contract=delegated_contract):
                self.assertIn(delegated_contract, self.html)

    def test_plan_records_as_built_private_paths_and_resolved_phase1_scope(
        self,
    ) -> None:
        plan = (ROOT / "IMPLEMENTATION_PLAN.md").read_text(encoding="utf-8")

        self.assertIn(
            "data/insiders/private/accessions/<accession>/...",
            plan,
        )
        self.assertIn("data/insiders/private/state/...", plan)
        self.assertIn("Phase 1 is local-only and default-off.", plan)
        self.assertIn("Reporting Insiders is implemented fixture scope.", plan)
        for stale_path in (
            "data/insiders/filings/<accession>.json",
            "data/insiders/raw/<accession>.xml",
            "data/insiders/issuers/<issuer-cik>.json",
            "data/insider_pipeline_state.json",
        ):
            with self.subTest(stale_path=stale_path):
                self.assertNotIn(stale_path, plan)
        self.assertNotIn("Decide whether Phase 1 is local/test-only", plan)
        self.assertNotIn(
            "Confirm whether the first UI shows a complete fixture subview", plan
        )

    def test_visual_runner_is_pinned_and_ci_gated(self) -> None:
        package = json.loads((ROOT / "package.json").read_text())
        lock = json.loads((ROOT / "package-lock.json").read_text())
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        artifact_builder = (ROOT / "scripts/build_pages_artifact.py").read_text()

        self.assertEqual("1.58.2", package["devDependencies"]["@playwright/test"])
        self.assertEqual(
            "1.58.2",
            lock["packages"]["node_modules/@playwright/test"]["version"],
        )
        self.assertIn("npm ci --ignore-scripts", workflow)
        self.assertIn("npx playwright install --with-deps chromium", workflow)
        self.assertIn("npm run test:visual", workflow)
        self.assertNotIn("13f-insider-activity-prd", artifact_builder)


if __name__ == "__main__":
    unittest.main()
