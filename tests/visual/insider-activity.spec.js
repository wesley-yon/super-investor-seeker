const { test, expect } = require("@playwright/test");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "../..");

const fundsIndex = {
  data_contract_version: 5,
  last_updated: "2026-06-30T20:45:00-04:00",
  funds: [{ cik: 1, name: "Fixture Fund", q: [20262, 20261] }],
};

const securityLabels = {
  data_contract_version: 5,
  labels: { "03770N101": "APGE" },
  kinds: { "03770N101": "COMMON" },
  product_names: {},
  fund_identities: [],
};

const stock = {
  stock_id: "03770N101",
  cusip: "03770N101",
  ticker: "APGE",
  issuer: "Apogee Therapeutics",
  instrument_type: "EQUITY",
  holders: [],
};

const fixturePath = path.join(
  root,
  "13f-insider-activity-prd/fixtures/apge-insider-activity.example.json"
);
const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));

async function installDeterministicRoutes(page) {
  await page.route("**/*", async (route) => {
    const url = new URL(route.request().url());
    if (url.origin !== "http://127.0.0.1:4173") {
      await route.abort("blockedbyclient");
      return;
    }
    const bodies = {
      "/data/funds-index.json": fundsIndex,
      "/data/security_labels.json": securityLabels,
      "/data/stocks/03770N101.json": stock,
    };
    if (bodies[url.pathname]) {
      await route.fulfill({ json: bodies[url.pathname] });
      return;
    }
    if (url.pathname === "/data/index.json") {
      await route.fulfill({
        json: { ...fundsIndex, tickers: [stock] },
      });
      return;
    }
    await route.continue();
  });
}

test.beforeEach(async ({ page }) => {
  await installDeterministicRoutes(page);
});

test("local APGE preview renders fixture summary", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );

  await expect(page.getByText("Fixture preview — not real filing data.")).toBeVisible();
  await expect(page.getByText("$1.81M")).toBeVisible();
  await expect(page.getByRole("tab", { name: "Insider Activity" }))
    .toHaveAttribute("aria-selected", "true");
});

test("preview remains default-off and preserves holders", async ({ page }) => {
  await page.goto("/#stock/03770N101/insiders");
  await expect(
    page.locator(".fund-panel-title").filter({ hasText: "Current Holders" })
  ).toBeVisible();
  await expect(page.getByText("Fixture preview — not real filing data."))
    .toHaveCount(0);
  await expect(page.getByRole("tab", { name: "Insider Activity" }))
    .toHaveCount(0);
});

test("filters, sorting, empty state, and URL state stay synchronized", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  await page.getByRole("button", { name: "Officers & Directors" }).click();
  await expect(page).toHaveURL(/ownerScope=officers-directors/);
  const transactionTable = page.locator("#insiderTransactionTable");
  await expect(transactionTable.locator("tbody tr.insider-row")).toHaveCount(4);
  await expect(
    transactionTable.getByText("Apogee Ventures LP", { exact: true })
  ).toHaveCount(0);
  await expect(
    transactionTable.getByText("Jane H. Smith", { exact: true })
  ).toBeVisible();
  await page.getByLabel("Rule 10b5-1 filter").selectOption("10b5-1");
  await expect(page).toHaveURL(/plan=10b5-1/);
  await expect(page.locator("#insiderTransactionTable tbody tr.insider-row"))
    .toHaveCount(2);
  await page.getByLabel("Search insiders").fill("No Such Insider");
  await expect(page).toHaveURL(/search=No(?:\+|%20)Such(?:\+|%20)Insider/);
  await expect(page.getByText("No transactions match these filters."))
    .toBeVisible();
  await page.getByLabel("Clear insider search").click();
  await page.getByLabel("Sort by Value").click();
  await expect(page).toHaveURL(/sort=value/);
});

test("active filters reconcile cards and rail with visible transactions", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  await page.getByLabel("Rule 10b5-1 filter").selectOption("10b5-1");

  const purchases = page.locator(".insider-kpi").filter({
    hasText: "Reported Purchases (P)",
  });
  const sales = page.locator(".insider-kpi").filter({
    hasText: "Reported Sales (S)",
  });
  const net = page.locator(".insider-kpi").filter({
    hasText: "Net P/S Activity",
  });
  await expect(purchases).toContainText("Filtered");
  await expect(purchases).toContainText("$0");
  await expect(purchases).toContainText("0 transactions · 0 insiders");
  await expect(sales).toContainText("$1.41M");
  await expect(sales).toContainText("2 transactions · 2 insiders");
  await expect(sales).toContainText("100% plan-marked");
  await expect(net).toContainText("-$1.41M");

  const buyerRail = page.locator(".insider-rail-section").filter({
    hasText: "Top Buyers by Value",
  });
  const sellerRail = page.locator(".insider-rail-section").filter({
    hasText: "Top Sellers by Value",
  });
  const planRail = page.locator(".insider-rail-section").filter({
    hasText: "Rule 10b5-1 Activity",
  });
  await expect(buyerRail).toContainText("No data");
  await expect(sellerRail).toContainText("Robert L. Brown");
  await expect(sellerRail).toContainText("Michael T. Lee");
  await expect(sellerRail).not.toContainText("Apogee Ventures LP");
  await expect(planRail).toContainText("$1.41M");
  await expect(planRail).toContainText("Distinct Insiders2");
});

test("filtered missing values remain unavailable rather than zero", async ({ page }) => {
  const missingFixture = structuredClone(fixture);
  for (const item of missingFixture.transactions.items) {
    if (item.planStatus !== "filing_marked") continue;
    item.value = null;
    item.valueDisplay = null;
    item.pricePerShare = null;
    item.priceDisplay = null;
  }
  await page.route("**/apge-insider-activity.example.json", route =>
    route.fulfill({ json: missingFixture })
  );
  await page.goto(
    "/?insiderPreview=fixture&plan=10b5-1#stock/03770N101/insiders"
  );

  const sales = page.locator(".insider-kpi").filter({
    hasText: "Reported Sales (S)",
  });
  const net = page.locator(".insider-kpi").filter({
    hasText: "Net P/S Activity",
  });
  const planRail = page.locator(".insider-rail-section").filter({
    hasText: "Rule 10b5-1 Activity",
  });
  await expect(sales.locator(".insider-kpi-value")).toHaveText("—");
  await expect(sales).toContainText("2 values unavailable");
  await expect(net.locator(".insider-kpi-value")).toHaveText("—");
  await expect(planRail).toContainText("Plan-Marked Sales—");
});

test("filtered net remains unavailable when either P/S value is missing", async ({ page }) => {
  const partialFixture = structuredClone(fixture);
  const purchase = {
    ...partialFixture.transactions.items[0],
    value: null,
    valueDisplay: null,
    pricePerShare: null,
    priceDisplay: null,
  };
  const sale = {
    ...partialFixture.transactions.items[1],
    value: "100.00",
    valueDisplay: "$100.00",
  };
  partialFixture.transactions.items = [purchase, sale];
  await page.route("**/apge-insider-activity.example.json", route =>
    route.fulfill({ json: partialFixture })
  );
  await page.goto(
    "/?insiderPreview=fixture&transactionScope=all#stock/03770N101/insiders"
  );

  const purchases = page.locator(".insider-kpi").filter({
    hasText: "Reported Purchases (P)",
  });
  const sales = page.locator(".insider-kpi").filter({
    hasText: "Reported Sales (S)",
  });
  const net = page.locator(".insider-kpi").filter({
    hasText: "Net P/S Activity",
  });
  await expect(purchases.locator(".insider-kpi-value")).toHaveText("—");
  await expect(sales.locator(".insider-kpi-value")).toHaveText("$100");
  await expect(net.locator(".insider-kpi-value")).toHaveText("—");
  await expect(net).toContainText("1 value unavailable");
  await expect(net).toContainText("Value unavailable");
});

test("chart range drives the table, cards, and rail until dates override it", async ({ page }) => {
  const rangeFixture = structuredClone(fixture);
  rangeFixture.transactions.items.push({
    ...rangeFixture.transactions.items[0],
    id: "txn_fixture_outside_range",
    displayGroupKey: "fixture-outside-range",
    transactionDate: "2025-07-01",
    ownerGroup: {
      ...rangeFixture.transactions.items[0].ownerGroup,
      key: "owner_outside_range",
      displayName: "Outside Range Insider",
    },
    value: "9000000.00",
    valueDisplay: "$9,000,000.00",
    accessionNumber: "0000000000-25-000099",
  });
  await page.route("**/apge-insider-activity.example.json", route =>
    route.fulfill({ json: rangeFixture })
  );

  await page.goto(
    "/?insiderPreview=fixture&range=6m#stock/03770N101/insiders"
  );
  await expect(page.getByText("Fixture preview — not real filing data.")).toBeVisible();
  const rangeState = await page.evaluate(() => {
    const state = insiderQueryState(currentInsiderFixture);
    return {
      range: state.range,
      window: insiderEffectiveDateWindow(currentInsiderFixture, state),
      rowIds: filteredInsiderTransactions(currentInsiderFixture, state)
        .map(item => item.id),
    };
  });
  expect(rangeState).toEqual({
    range: "6m",
    window: { start: "2025-12-30", end: "2026-06-30", explicit: false },
    rowIds: [
      "txn_fixture_1",
      "txn_fixture_2",
      "txn_fixture_3",
      "txn_fixture_4",
      "txn_fixture_5",
    ],
  });
  const purchases = page.locator(".insider-kpi").filter({
    hasText: "Reported Purchases (P)",
  });
  const buyerRail = page.locator(".insider-rail-section").filter({
    hasText: "Top Buyers by Value",
  });
  await expect(page.locator("#insiderTransactionTable"))
    .not.toContainText("Outside Range Insider");
  await expect(purchases.locator(".insider-kpi-value")).toHaveText("$1.12M");
  await expect(buyerRail).not.toContainText("Outside Range Insider");

  await page.goto(
    "/?insiderPreview=fixture&range=6m&start=2025-07-01&end=2025-07-01"
      + "#stock/03770N101/insiders"
  );
  await expect(page.locator("#insiderTransactionTable"))
    .toContainText("Outside Range Insider");
  await expect(page.locator("#insiderTransactionTable")).not.toContainText("Jane H. Smith");
  await expect(purchases.locator(".insider-kpi-value")).toHaveText("$9M");
  await expect(buyerRail).toContainText("Outside Range Insider");
});

test("filing drawer traps focus, closes with Escape, and restores focus", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  const row = page.locator("#insiderTransactionTable tbody tr.insider-row").first();
  await row.focus();
  await page.keyboard.press("Enter");
  const drawer = page.getByRole("dialog", { name: /Form 4 Filing Detail/ });
  await expect(drawer).toBeVisible();
  await expect(page).toHaveURL(/filing=0000000000-26-000001/);
  await expect(page.getByRole("button", { name: "Close filing detail" }))
    .toBeFocused();
  await page.keyboard.press("Shift+Tab");
  await expect(drawer.locator("a").last()).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(drawer).toHaveCount(0);
  await expect(page).not.toHaveURL(/filing=/);
  await expect(row).toBeFocused();
});

test("chart marker opens detail by keyboard and source link is separate", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  const marker = page.locator("#insiderPriceChart [role=button]").first();
  await marker.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog", { name: /Form 4 Filing Detail/ }))
    .toBeVisible();
  await page.keyboard.press("Escape");
  await expect(marker).toBeFocused();

  const secLink = page.locator("#insiderTransactionTable a[target=_blank]").first();
  await expect(secLink).toHaveAttribute("rel", "noopener noreferrer");
  await expect(secLink).toHaveAttribute("href", /^https:\/\/www\.sec\.gov\//);
});

test("fixture source links reject non-SEC URLs", async ({ page }) => {
  const unsafeFixture = structuredClone(fixture);
  for (const item of unsafeFixture.transactions.items) {
    item.secDocumentUrl = "javascript:alert('fixture')";
  }
  await page.route("**/apge-insider-activity.example.json", route =>
    route.fulfill({ json: unsafeFixture })
  );
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );

  await expect(page.locator("#insiderTransactionTable .filing-cell a"))
    .toHaveCount(0);
  await page.locator("#insiderTransactionTable tbody tr.insider-row").first().click();
  const drawer = page.getByRole("dialog", { name: /Form 4 Filing Detail/ });
  await expect(drawer.locator("a.drawer-source")).toHaveCount(0);
});

test("fixture accessions cannot inject inline code", async ({ page }) => {
  const unsafeFixture = structuredClone(fixture);
  unsafeFixture.transactions.items[0].accessionNumber =
    "');window.__fixtureExecuted=true;//";
  await page.route("**/apge-insider-activity.example.json", route =>
    route.fulfill({ json: unsafeFixture })
  );
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );

  const row = page.locator("#insiderTransactionTable tbody tr.insider-row").filter({
    hasText: "Jane H. Smith",
  });
  await expect(row).not.toHaveAttribute("onclick");
  await row.click();
  await expect.poll(() => page.evaluate(() => Boolean(window.__fixtureExecuted)))
    .toBe(false);
  await expect(page.getByRole("dialog", { name: /Form 4 Filing Detail/ }))
    .toHaveCount(0);
});

test("reporting-insiders is a routed fixture subview", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  await page.getByRole("tab", { name: "Reporting Insiders" }).click();
  await expect(page).toHaveURL(/#stock\/03770N101\/reporting-insiders$/);
  await expect(page.getByRole("tab", { name: "Reporting Insiders" }))
    .toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#reportingInsidersTable")).toBeVisible();
  await expect(page.getByText("Latest New Relationship")).toBeVisible();
  await expect(
    page.getByRole("columnheader", { name: "Plan-Marked Sale %" })
  ).toBeVisible();
  const janeRow = page.locator("#reportingInsidersTable tbody tr").filter({
    hasText: "Jane H. Smith",
  });
  await expect(janeRow).toContainText("$1.05M");
  await expect(janeRow).toContainText("$517K");
  await expect(janeRow).toContainText("100%");
});

test("leaving the insider tab clears filters but Back restores them", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture&ownerScope=officers-directors&plan=10b5-1"
      + "&search=Jane#stock/03770N101/insiders"
  );
  await page.getByRole("tab", { name: "Institutional Holders" }).click();
  await expect(page).toHaveURL(/\?insiderPreview=fixture#stock\/03770N101$/);
  await expect(page).not.toHaveURL(/ownerScope=|plan=|search=/);

  await page.goBack();
  await expect(page).toHaveURL(/#stock\/03770N101\/insiders$/);
  await expect(page).toHaveURL(/ownerScope=officers-directors/);
  await expect(page).toHaveURL(/plan=10b5-1/);
  await expect(page).toHaveURL(/search=Jane/);
});

test("All Transactions renders a neutral marker for other codes", async ({ page }) => {
  const fixtureWithOther = structuredClone(fixture);
  fixtureWithOther.transactions.items.push({
    ...fixtureWithOther.transactions.items[0],
    id: "txn_fixture_other",
    displayGroupKey: "fixture-other-award",
    transactionDate: "2026-05-10",
    transactionCode: "A",
    transactionLabel: "AWARD (A)",
    normalizedCategory: "other",
    planStatus: "not_marked",
    accessionNumber: "0000000000-26-000099",
  });
  await page.route("**/apge-insider-activity.example.json", route =>
    route.fulfill({ json: fixtureWithOther })
  );
  await page.goto(
    "/?insiderPreview=fixture&transactionScope=all"
      + "#stock/03770N101/insiders"
  );
  const otherMarker = page.locator("#insiderPriceChart circle.chart-marker.other");
  await expect(otherMarker).toHaveCount(1);
  await expect(otherMarker).toHaveAttribute("fill", "#898b87");
});

test("other-only filtered results keep net P/S at zero", async ({ page }) => {
  const otherOnlyFixture = structuredClone(fixture);
  otherOnlyFixture.transactions.items = [{
    ...otherOnlyFixture.transactions.items[0],
    id: "txn_fixture_other_only",
    displayGroupKey: "fixture-other-only-award",
    transactionDate: "2026-05-10",
    transactionCode: "A",
    transactionLabel: "AWARD (A)",
    normalizedCategory: "other",
    planStatus: "not_marked",
    accessionNumber: "0000000000-26-000098",
  }];
  await page.route("**/apge-insider-activity.example.json", route =>
    route.fulfill({ json: otherOnlyFixture })
  );
  await page.goto(
    "/?insiderPreview=fixture&transactionScope=all"
      + "#stock/03770N101/insiders"
  );

  const net = page.locator(".insider-kpi").filter({
    hasText: "Net P/S Activity",
  });
  await expect(net.locator(".insider-kpi-value")).toHaveText("$0");
  await expect(net).toContainText("Balanced");
});

test("loading and fetch-error states preserve the stock shell", async ({ page }) => {
  let releaseFixture;
  await page.route("**/apge-insider-activity.example.json", async (route) => {
    await new Promise(resolve => { releaseFixture = resolve; });
    await route.fulfill({ json: fixture });
  });
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  await expect(page.locator("[aria-busy=true] .skeleton-chart")).toBeVisible();
  releaseFixture();
  await expect(page.getByText("$1.81M")).toBeVisible();

  const errorPage = await page.context().newPage();
  await installDeterministicRoutes(errorPage);
  await errorPage.route("**/apge-insider-activity.example.json", route =>
    route.fulfill({ status: 503, body: "unavailable" })
  );
  await errorPage.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  await expect(errorPage.getByRole("alert")).toContainText(
    "Insider preview couldn’t load"
  );
  await expect(errorPage.getByText("Apogee Therapeutics")).toBeVisible();
});

for (const viewport of [
  { width: 1621, height: 970 },
  { width: 1440, height: 900 },
  { width: 1024, height: 768 },
  { width: 768, height: 1024 },
  { width: 390, height: 844 },
]) {
  test(`deterministic responsive screenshot ${viewport.width}x${viewport.height}`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.goto(
      "/?insiderPreview=fixture#stock/03770N101/insiders"
    );
    await expect(page.locator("#insiderTransactionTable")).toBeVisible();
    const layout = await page.locator(".insider-layout").evaluate(element => {
      const style = getComputedStyle(element);
      const main = element.querySelector(".insider-main").getBoundingClientRect();
      const rail = element.querySelector(".insider-summary-rail").getBoundingClientRect();
      return {
        columns: style.gridTemplateColumns.split(" ").length,
        mainWidth: main.width,
        railWidth: rail.width,
        overflow: document.documentElement.scrollWidth - innerWidth,
      };
    });
    expect(layout.overflow).toBeLessThanOrEqual(1);
    if (viewport.width >= 1280) {
      expect(layout.columns).toBe(2);
      expect(layout.railWidth).toBeGreaterThanOrEqual(315);
      expect(layout.railWidth).toBeLessThanOrEqual(345);
      expect(layout.mainWidth).toBeGreaterThan(750);
    } else {
      expect(layout.columns).toBe(1);
    }
    if (viewport.width <= 520) {
      const tabBounds = await page.getByRole("tab").evaluateAll(tabs =>
        tabs.map(tab => {
          const box = tab.getBoundingClientRect();
          const textRange = document.createRange();
          textRange.selectNodeContents(tab);
          const textBox = textRange.getBoundingClientRect();
          return {
            left: box.left,
            right: box.right,
            textLeft: textBox.left,
            textRight: textBox.right,
            clientWidth: tab.clientWidth,
            scrollWidth: tab.scrollWidth,
            whiteSpace: getComputedStyle(tab).whiteSpace,
          };
        })
      );
      for (const box of tabBounds) {
        expect(box.left).toBeGreaterThanOrEqual(0);
        expect(box.right).toBeLessThanOrEqual(viewport.width);
        expect(box.textLeft).toBeGreaterThanOrEqual(box.left);
        expect(box.textRight).toBeLessThanOrEqual(box.right);
        expect(box.scrollWidth).toBeLessThanOrEqual(box.clientWidth + 1);
        expect(box.whiteSpace).toBe("normal");
      }
    }
    await page.evaluate(() => document.fonts.ready);
    await page.evaluate(() => new Promise(resolve =>
      requestAnimationFrame(() => requestAnimationFrame(resolve))
    ));
    await expect(page).toHaveScreenshot(
      `insider-${viewport.width}x${viewport.height}.png`,
      {
        animations: "disabled",
        caret: "hide",
        maxDiffPixelRatio: 0.03,
        threshold: 0.25,
      }
    );
  });
}
