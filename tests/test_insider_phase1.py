import json
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

    def test_stock_subroutes_are_flag_gated_and_preserve_identity(self) -> None:
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
                  gated: parseAppRoute(
                    "#stock/03770N101/insiders", false
                  ),
                  encoded: parseAppRoute("#stock/ABC%7CNOTE/insiders", true),
                  invalid: parseAppRoute("#stock/03770N101/not-a-view", true),
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
                "gated": {
                    "kind": "stock",
                    "id": "03770N101",
                    "view": "holders",
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
                  tablist: markup.includes('role="tablist"'),
                  selected: markup.includes(
                    'aria-selected="true">Insider Activity'
                  ),
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
                "tablist": True,
                "selected": True,
            },
            json.loads(completed.stdout),
        )

    def test_complete_fixture_markup_has_chart_table_rail_and_drawer_hooks(
        self,
    ) -> None:
        required_contracts = (
            'id="insiderPriceChart"',
            'aria-label="Share price and insider transaction chart"',
            'id="insiderTransactionTable"',
            'data-insider-accession="${esc(accession)}"',
            'class="insider-summary-rail"',
            'id="insiderDrawer"',
            'role="dialog"',
            'function openInsiderFiling(',
            'function closeInsiderDrawer(',
        )
        for contract in required_contracts:
            with self.subTest(contract=contract):
                self.assertIn(contract, self.html)

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

    def test_visual_runner_is_pinned_and_ci_gated(self) -> None:
        package = json.loads((ROOT / "package.json").read_text())
        lock = json.loads((ROOT / "package-lock.json").read_text())
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        artifact_builder = (ROOT / "scripts/build_pages_artifact.py").read_text()

        self.assertEqual(
            "1.58.2", package["devDependencies"]["@playwright/test"]
        )
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
