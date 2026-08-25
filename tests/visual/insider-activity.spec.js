const { test, expect } = require("@playwright/test");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "../..");
// The checked-in baselines are reviewed on macOS. Linux Chromium rasterizes
// the same glyphs differently; layout assertions below stay strict while this
// narrow runner allowance absorbs the inspected glyph-edge noise in CI.
const maxVisualDiffPixelRatio = process.platform === "linux" ? 0.05 : 0.03;

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

const fund = {
  data_contract_version: 5,
  cik: 1,
  name: "Fixture Fund",
  quarters: [],
};

const fixturePath = path.join(
  root,
  "13f-insider-activity-prd/fixtures/apge-insider-activity.example.json"
);
const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const liveSecurityPath = path.join(
  root,
  "tests/fixtures/phase5-live-security.json"
);
const liveFilingPath = path.join(
  root,
  "tests/fixtures/phase5-live-filing.json"
);
const liveSecurity = JSON.parse(fs.readFileSync(liveSecurityPath, "utf8"));
const liveFiling = JSON.parse(fs.readFileSync(liveFilingPath, "utf8"));
const liveAccession = liveFiling.accessionNumber;
const complexLiveSecurityPath = path.join(
  root,
  "tests/fixtures/phase5-live-complex-security.json"
);
const complexLiveFilingPath = path.join(
  root,
  "tests/fixtures/phase5-live-complex-filing.json"
);
const complexLiveSecurity = JSON.parse(
  fs.readFileSync(complexLiveSecurityPath, "utf8")
);
const complexLiveFiling = JSON.parse(
  fs.readFileSync(complexLiveFilingPath, "utf8")
);
const complexLiveAccession = complexLiveFiling.accessionNumber;

async function installDeterministicRoutes(page) {
  await page.route("**/*", async (route) => {
    const url = new URL(route.request().url());
    if (url.origin !== "http://127.0.0.1:4173") {
      await route.abort("blockedbyclient");
      return;
    }
    const bodies = {
      "/data/funds-index.json": fundsIndex,
      "/data/funds/1.json": fund,
      "/data/security_labels.json": securityLabels,
      "/data/stocks/03770N101.json": stock,
      "/data/insiders/public/securities/03770N101.json": liveSecurity,
    };
    if (url.pathname === `/data/insiders/public/filings/${liveAccession}.json`) {
      await route.fulfill({
        body: fs.readFileSync(liveFilingPath),
        contentType: "application/json",
      });
      return;
    }
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

async function delayFirstLiveSecurityResponse(page) {
  let releaseFirst;
  let markStarted;
  let markFinished;
  let requestCount = 0;
  const started = new Promise(resolve => { markStarted = resolve; });
  const finished = new Promise(resolve => { markFinished = resolve; });
  await page.route(
    "**/data/insiders/public/securities/03770N101.json",
    async (route) => {
      requestCount += 1;
      const first = requestCount === 1;
      if (first) {
        markStarted();
        await new Promise(resolve => { releaseFirst = resolve; });
      }
      await route.fulfill({
        body: fs.readFileSync(liveSecurityPath),
        contentType: "application/json",
      });
      if (first) markFinished();
    }
  );
  return {
    started,
    finished,
    release: () => releaseFirst(),
  };
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
  await expect(page.getByRole("button", { name: "Insider Activity" }))
    .toHaveAttribute("aria-current", "page");
});

test("rendered delegated data actions are native keyboard controls", async ({
  page,
}) => {
  await page.goto("/");
  await page.evaluate(() => {
    idx = {
      funds: [{ cik: 1, name: "Fixture Fund", q: [20262, 20261] }],
      tickers: [{
        stock_id: "03770N101",
        cusip: "03770N101",
        ticker: "APGE",
        issuer: "Apogee Therapeutics",
        instrument_type: "EQUITY",
      }],
    };
    globalSearch("FIXTURE");
    app().insertAdjacentHTML("beforeend", `
      <table><tbody id="fundTbody"></tbody></table><div id="fundFoot"></div>
      <table><tbody id="stockTbody"></tbody></table><div id="stockFoot"></div>
    `);
    curFundRows = [{
      cusip: "03770N101", ticker: "APGE", issuer: "Apogee Therapeutics",
      instrument_type: "EQUITY", pct: 1, value: 100, shares: 10,
      ch: null, prevPct: 0, sparkData: [],
    }];
    curStockRows = [{
      cik: 1, name: "Fixture Fund", value: 100, shares: 10,
      pctOfFund: 1, ch: null, sparkData: [], asOfDate: "2026-06-30",
    }];
    renderFundTbody();
    renderStockTbody();
  });

  const controls = page.locator(
    ".gsearch-item, #fundTbody [data-action], #stockTbody [data-action]"
  );
  await expect(controls).toHaveCount(3);
  for (let index = 0; index < 3; index += 1) {
    const control = controls.nth(index);
    expect(await control.evaluate(node => ({
      tag: node.tagName,
      type: node.getAttribute("type"),
      tabIndex: node.tabIndex,
    }))).toEqual({ tag: "BUTTON", type: "button", tabIndex: 0 });
  }

  await page.evaluate(() => {
    window.__nativeActionAudit = [];
    loadFund = cik => window.__nativeActionAudit.push(["fund", cik]);
    loadStock = stockId => window.__nativeActionAudit.push(["stock", stockId]);
  });
  await controls.nth(0).focus();
  await page.keyboard.press("Enter");
  await page.evaluate(() => globalSearch("APGE"));
  await page.locator(".gsearch-item").first().focus();
  await page.keyboard.press(" ");
  await page.locator("#fundTbody [data-action]").focus();
  await page.keyboard.press("Enter");
  await page.locator("#stockTbody [data-action]").focus();
  await page.keyboard.press(" ");
  await page.evaluate(() => {
    fundIndexByCik = new Map([["1", {
      cik: 1, name: "Fixture Fund", q: [20262, 20261],
    }]]);
    currentReportingQuarter = 20262;
    renderStock({
      stock_id: "03770N101",
      cusip: "03770N101",
      ticker: "APGE",
      issuer: "Apogee Therapeutics",
      instrument_type: "EQUITY",
      holders: [{
        cik: 1,
        name: "Fixture Fund",
        history: [
          { date: "2026-06-30", shares: 10, value: 100, pct_of_fund: 1 },
          { date: "2026-03-31", shares: 9, value: 90, pct_of_fund: 1 },
        ],
      }],
    }, idx.tickers[0]);
  });
  const summaryControl = page.locator(".stock-summary-name[data-action]").first();
  expect(await page.locator(".stock-summary-name[data-action]").count()).toBeGreaterThan(0);
  expect(await summaryControl.evaluate(node => ({
    tag: node.tagName,
    type: node.getAttribute("type"),
    tabIndex: node.tabIndex,
  }))).toEqual({ tag: "BUTTON", type: "button", tabIndex: 0 });
  await summaryControl.focus();
  await page.keyboard.press("Enter");
  await expect.poll(() => page.evaluate(() => window.__nativeActionAudit)).toEqual([
    ["fund", "1"],
    ["stock", "03770N101"],
    ["stock", "03770N101"],
    ["fund", "1"],
    ["fund", "1"],
  ]);
});

test("browser rejects APGE ticker aliases for the fixture path", async ({ page }) => {
  await page.goto("/?insiderPreview=fixture");
  await expect(page).toHaveURL(/insiderPreview=fixture/);
  await expect(page.evaluate(() => ({
    exact: insiderFixturePathForStock("03770N101", {
      stock_id: "03770N101", cusip: "03770N101", ticker: "APGE",
    }),
    tickerAlias: insiderFixturePathForStock("APGE", {
      ticker: "APGE", issuer: "Apogee Therapeutics",
    }),
    wrongSecurityWithApgeTicker: insiderFixturePathForStock("99999X999", {
      stock_id: "99999X999", cusip: "99999X999", ticker: "APGE",
    }),
  }))).resolves.toEqual({
    exact: "13f-insider-activity-prd/fixtures/apge-insider-activity.example.json",
    tickerAlias: null,
    wrongSecurityWithApgeTicker: null,
  });
});

test("security views use ordinary button keyboard navigation", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );

  const navigation = page.getByRole("navigation", { name: "Security views" });
  const holders = navigation.getByRole("button", {
    name: "Institutional Holders",
  });
  const insiders = navigation.getByRole("button", { name: "Insider Activity" });
  const reporting = navigation.getByRole("button", {
    name: "Reporting Insiders",
  });

  await expect(insiders).toHaveAttribute("aria-current", "page");
  await holders.focus();
  await page.keyboard.press("Tab");
  await expect(insiders).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(reporting).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/#stock\/03770N101\/reporting-insiders$/);
  await expect(reporting).toHaveAttribute("aria-current", "page");
});

test("production insider route loads the validated live payload only", async ({
  page,
}) => {
  const requested = [];
  page.on("request", request => requested.push(new URL(request.url()).pathname));
  await page.goto("/#stock/03770N101/insiders");

  await expect(page.getByRole("heading", { name: "Synthetic Test Issuer" }))
    .toBeVisible();
  await expect(page.getByRole("button", { name: "Insider Activity" }))
    .toHaveAttribute("aria-current", "page");
  await expect(page.getByText("$1.27K").first()).toBeVisible();
  await expect(page.getByText("Fixture preview — not real filing data."))
    .toHaveCount(0);
  expect(requested.some(value => value.endsWith(
    "/apge-insider-activity.example.json"
  ))).toBe(false);
  expect(requested.some(value => value.endsWith(
    "/data/insiders/public/securities/03770N101.json"
  ))).toBe(true);
});

test("methodology dialog distinguishes validated live data from fixture preview", async ({
  page,
}) => {
  await page.goto("/#stock/03770N101/insiders");
  await expect(page.getByRole("heading", { name: "Synthetic Test Issuer" }))
    .toBeVisible();
  await page.getByRole("button", { name: "Learn more" }).click();

  let dialog = page.getByRole("dialog", {
    name: "Insider Activity Methodology",
  });
  await expect(dialog).toBeVisible();
  await expect(dialog.locator(".drawer-meta"))
    .toHaveText("Validated public filing data");
  await expect(dialog).toContainText("Forms 3, 4, and 5");
  await expect(dialog).toContainText("transaction-only");
  await expect(dialog).toContainText(
    "name as filed and company relationship/title"
  );
  await expect(dialog).not.toContainText("Phase 1 fixture preview");
  await expect(dialog).not.toContainText("illustrative");
  await expect(dialog).not.toContainText("APGE");
  await dialog.getByRole("button", { name: "Close methodology" }).click();

  await page.goto("/?insiderPreview=fixture#stock/03770N101/insiders");
  await expect(page.getByText("Fixture preview — not real filing data."))
    .toBeVisible();
  await page.getByRole("button", { name: "Learn more" }).click();
  dialog = page.getByRole("dialog", {
    name: "Insider Activity Methodology",
  });
  await expect(dialog.locator(".drawer-meta"))
    .toHaveText("Phase 1 fixture preview");
  await expect(dialog).toContainText("Fixture limitation");
  await expect(dialog).toContainText("illustrative");
  await expect(dialog).toContainText("APGE filing evidence");
});

test("live view formats validated rows and uses an honest transaction-only timeline", async ({
  page,
}) => {
  await page.goto("/#stock/03770N101/insiders");

  await expect(page.getByRole("button", { name: "1Y" }))
    .toHaveAttribute("aria-pressed", "true");
  await expect(page.getByRole("button", { name: "P/S Only" }))
    .toHaveAttribute("aria-pressed", "true");
  const latest = page.locator(".insider-kpi.latest");
  await expect(latest).toContainText("Purchased 123.45 shares");
  await expect(latest).toContainText("$1.27K");
  const net = page.locator(".insider-kpi").filter({
    hasText: "Net P/S Activity",
  });
  await expect(net).toContainText("Net reported buying");
  const row = page.locator("#insiderTransactionTable tbody tr.insider-row").first();
  await expect(row).toContainText("$10.25");
  await expect(row).toContainText("$1.27K");
  await expect(row).toContainText("14.08%");
  await expect(page.getByRole("heading", {
    name: "Insider Transaction Timeline",
  })).toBeVisible();
  await expect(page.getByText(
    "Daily share-price history is not integrated; markers use only prices reported in each SEC transaction."
  )).toBeVisible();
  await expect(page.locator("#insiderPriceChart .chart-price-line")).toHaveCount(0);
  await expect(page.locator(".chart-legend")).not.toContainText("Price");
});

test("live transaction table uses bounded URL-backed client pagination", async ({
  page,
}) => {
  await page.goto(
    "/?range=all&transactionScope=all#stock/03770N101/insiders"
  );
  await expect(page.getByRole("heading", { name: "Synthetic Test Issuer" }))
    .toBeVisible();
  await expect(page.locator("#insiderTransactionTable tbody tr.insider-row"))
    .toHaveCount(1);
  await page.evaluate(() => {
    const item = currentInsiderFixture.transactions.items[0];
    currentInsiderFixture.transactions.items = Array.from(
      { length: 205 },
      (_, index) => ({
        ...item,
        shares: String(index + 1),
        postTransactionShares: String(1_000 + index),
      })
    );
    currentInsiderFixture.transactions.total = 205;
    currentInsiderFixture.transactions.totalApproximate = 205;
    currentInsiderFixture.staticPagination.itemCount = 205;
    renderCurrentInsiderView();
  });

  const rows = page.locator("#insiderTransactionTable tbody tr.insider-row");
  const pagination = page.getByRole("navigation", {
    name: "Insider transaction pages",
  });
  await expect(rows).toHaveCount(100);
  await expect(pagination).toContainText("1–100 of 205");
  await pagination.getByRole("button", { name: "Next page" }).click();
  await expect(page).toHaveURL(/page=2/);
  await expect(rows).toHaveCount(100);
  await expect(pagination).toContainText("101–200 of 205");
  await pagination.getByRole("button", { name: "Next page" }).click();
  await expect(page).toHaveURL(/page=3/);
  await expect(rows).toHaveCount(5);
  await expect(pagination).toContainText("201–205 of 205");
  await pagination.getByRole("button", { name: "Previous page" }).click();
  await expect(page).toHaveURL(/page=2/);
  await page.getByRole("button", { name: "Officers & Directors" }).click();
  await expect(page).not.toHaveURL(/page=/);
});

test("out-of-range insider page is replaced with its canonical effective page", async ({
  page,
}) => {
  await page.goto(
    "/?range=all&transactionScope=all#stock/03770N101/insiders"
  );
  await expect(page.getByRole("heading", { name: "Synthetic Test Issuer" }))
    .toBeVisible();
  await page.evaluate(() => {
    const expandTransactions = payload => {
      const item = payload.transactions.items[0];
      payload.transactions.items = Array.from(
        { length: 205 },
        (_, index) => ({
          ...item,
          shares: String(index + 1),
          postTransactionShares: String(1_000 + index),
        })
      );
      payload.transactions.total = 205;
      payload.transactions.totalApproximate = 205;
      payload.staticPagination.itemCount = 205;
    };
    const stableLivePayload = structuredClone(currentLiveInsiderPayload);
    expandTransactions(stableLivePayload);
    globalThis.__phase5CanonicalPagePayload = stableLivePayload;
    loadLiveInsiderSecurityPayload = async () => structuredClone(
      globalThis.__phase5CanonicalPagePayload
    );
    expandTransactions(currentInsiderFixture);
    history.replaceState(
      {},
      "",
      "/?range=all&transactionScope=all&page=5000#stock/03770N101/insiders"
    );
    renderCurrentInsiderView();
  });

  const rows = page.locator("#insiderTransactionTable tbody tr.insider-row");
  const pagination = page.getByRole("navigation", {
    name: "Insider transaction pages",
  });
  await expect(page).toHaveURL(/page=3/);
  await expect(page).not.toHaveURL(/page=5000/);
  await expect(rows).toHaveCount(5);
  await expect(pagination).toContainText("201–205 of 205");

  await pagination.getByRole("button", { name: "Previous page" }).click();
  await expect(page).toHaveURL(/page=2/);
  await expect(rows).toHaveCount(100);
  await expect(pagination).toContainText("101–200 of 205");
  await page.goBack();
  await expect(page).toHaveURL(/page=3/);
  await expect(rows).toHaveCount(5);
  await expect(pagination).toContainText("201–205 of 205");
  await page.goForward();
  await expect(page).toHaveURL(/page=2/);
  await expect(rows).toHaveCount(100);
  await expect(pagination).toContainText("101–200 of 205");
});

test("bounded live contract scale keeps timeline and table DOM finite", async ({
  page,
}) => {
  await page.goto(
    "/?range=all&transactionScope=all#stock/03770N101/insiders"
  );
  await expect(page.getByRole("heading", { name: "Synthetic Test Issuer" }))
    .toBeVisible();
  const renderMilliseconds = await page.evaluate(() => {
    const item = currentInsiderFixture.transactions.items[0];
    currentInsiderFixture.transactions.items = Array.from(
      { length: 5_000 },
      (_, index) => ({
        ...item,
        shares: String(index + 1),
        postTransactionShares: String(10_000 + index),
      })
    );
    currentInsiderFixture.transactions.total = 5_000;
    currentInsiderFixture.transactions.totalApproximate = 5_000;
    currentInsiderFixture.staticPagination.itemCount = 5_000;
    const started = performance.now();
    renderCurrentInsiderView();
    return performance.now() - started;
  });

  expect(renderMilliseconds).toBeLessThan(3_000);
  await expect(page.locator("#insiderTransactionTable tbody tr.insider-row"))
    .toHaveCount(100);
  await expect(page.getByRole("navigation", {
    name: "Insider transaction pages",
  })).toContainText("1–100 of 5000");
  const timeline = page.locator("#insiderPriceChart");
  await expect(timeline).toHaveAttribute("role", "img");
  await expect(timeline).toHaveAttribute("data-event-count", "5000");
  await expect(timeline.locator(".chart-event-series")).toHaveCount(1);
  await expect(timeline.locator(".chart-event-series"))
    .toHaveAttribute("data-event-count", "5000");
  await expect(timeline.locator("[tabindex], [role=button]")).toHaveCount(0);
  expect(await timeline.locator(".chart-axis").count()).toBeLessThanOrEqual(7);
  expect(await timeline.locator("*").count()).toBeLessThan(40);
});

test("live 404 renders an explicit empty state without fixture fallback", async ({
  page,
}) => {
  const requested = [];
  page.on("request", request => requested.push(new URL(request.url()).pathname));
  await page.route(
    "**/data/insiders/public/securities/03770N101.json",
    route => route.fulfill({ status: 404, body: "not found" })
  );
  await page.goto("/#stock/03770N101/reporting-insiders");

  const empty = page.getByRole("status");
  await expect(empty).toContainText("No published insider activity");
  await expect(empty).toContainText(
    "No validated public Forms 3, 4, or 5 payload is available for this security."
  );
  await expect(page.getByRole("button", { name: "Reporting Insiders" }))
    .toHaveAttribute("aria-current", "page");
  await expect(page.getByRole("alert")).toHaveCount(0);
  await expect(page.getByText("Fixture preview — not real filing data."))
    .toHaveCount(0);
  expect(requested.some(value => value.endsWith(
    "/apge-insider-activity.example.json"
  ))).toBe(false);
});

test("malformed live payload fails generically without exposing contract details", async ({
  page,
}) => {
  const malformed = structuredClone(liveSecurity);
  malformed.payloadType = "private_insider_corpus";
  await page.route(
    "**/data/insiders/public/securities/03770N101.json",
    route => route.fulfill({ json: malformed })
  );
  await page.goto("/#stock/03770N101/insiders");

  const alert = page.getByRole("alert");
  await expect(alert).toContainText("Insider activity couldn’t load");
  await expect(alert).toContainText(
    "Validated public insider activity is temporarily unavailable."
  );
  await expect(alert.getByRole("button", { name: "Try again" })).toBeVisible();
  await expect(page.locator("body")).not.toContainText("private_insider_corpus");
  await expect(page.locator("body")).not.toContainText(
    "Published insider activity contract is invalid"
  );
  await expect(page.getByText("Fixture preview — not real filing data."))
    .toHaveCount(0);
});

test("live filing drawer loads only digest-bound public filing detail", async ({
  page,
}) => {
  const requested = [];
  page.on("request", request => requested.push(new URL(request.url()).pathname));
  await page.goto("/#stock/03770N101/insiders");

  const detailButton = page.locator(
    "#insiderTransactionTable .filing-detail-button"
  ).first();
  await detailButton.focus();
  await expect(detailButton).toBeFocused();
  await page.keyboard.press("Enter");
  const drawer = page.getByRole("dialog", { name: /Form 4 Filing Detail/ });
  await expect(drawer).toBeVisible();
  await expect(drawer).toContainText("SYNTHETIC OWNER ALPHA");
  await expect(drawer).toContainText("Director");
  await expect(drawer).toContainText("Purchase");
  await expect(drawer).toContainText("123.45");
  await expect(drawer).toContainText("$10.25");
  await expect(drawer).toContainText("$1.27K");
  await expect(drawer).toContainText(
    "Filing narratives, addresses, owner CIKs, signatures, and raw source are intentionally omitted."
  );
  await expect(drawer.getByText("Footnotes & Remarks")).toHaveCount(0);
  await expect(drawer.getByText("Data Lineage")).toHaveCount(0);
  await expect(drawer.locator("a.drawer-source")).toHaveCount(2);
  await expect(drawer.locator("a.drawer-source").first()).toHaveAttribute(
    "rel",
    "noopener noreferrer"
  );
  await expect(page).toHaveURL(new RegExp(`filing=${liveAccession}`));
  expect(requested.some(value => value.endsWith(
    `/data/insiders/public/filings/${liveAccession}.json`
  ))).toBe(true);
  await expect(drawer.getByRole("button", { name: "Close filing detail" }))
    .toBeFocused();
  await page.keyboard.press("Escape");
  await expect(drawer).toHaveCount(0);
  await expect(detailButton).toBeFocused();
  await expect(page).not.toHaveURL(/filing=/);
});

test("complex live filing preserves joint, null, indirect, and derivative semantics", async ({
  page,
}) => {
  await page.route(
    "**/data/insiders/public/securities/03770N101.json",
    route => route.fulfill({ json: complexLiveSecurity })
  );
  await page.route(
    `**/data/insiders/public/filings/${complexLiveAccession}.json`,
    route => route.fulfill({
      body: fs.readFileSync(complexLiveFilingPath),
      contentType: "application/json",
    })
  );
  await page.goto(
    "/?range=all&transactionScope=all#stock/03770N101/insiders"
  );

  const rows = page.locator("#insiderTransactionTable tbody tr.insider-row");
  await expect(rows).toHaveCount(2);
  const weightedRow = rows.filter({ hasText: "$5.03K" });
  await expect(weightedRow).toContainText(
    "SYNTHETIC OWNER BETA / SYNTHETIC TEST ENTITY"
  );
  await expect(weightedRow.locator("td").nth(4)).toContainText("$20.125");
  await expect(weightedRow.locator("td").nth(4)).toHaveAttribute(
    "title",
    "Reported weighted-average price"
  );
  await expect(weightedRow.locator("td").nth(6)).toHaveAttribute(
    "title",
    "Indirect ownership"
  );

  const missingRow = rows.filter({ hasText: "5.00000001" });
  await expect(missingRow.locator("td").nth(4)).toHaveText("—");
  await expect(missingRow.locator("td").nth(5)).toHaveText("—");

  await weightedRow.locator(".filing-detail-button").click();
  const drawer = page.getByRole("dialog", { name: /Form 4 Filing Detail/ });
  await expect(drawer).toContainText("SYNTHETIC OWNER BETA");
  await expect(drawer).toContainText("SYNTHETIC TEST ENTITY");
  await expect(drawer).toContainText("Exercise / Conversion");
  await expect(drawer).toContainText("weighted average");
  await expect(drawer).toContainText("Indirect");
  await expect(drawer).toContainText("Derivative holding");
  await expect(drawer).toContainText("Underlying 03770N101 · 42.125 shares");
  await expect(drawer).toContainText("Exercise price $4.25");
  await expect(drawer).not.toContainText("0000000002");
  await expect(drawer).not.toContainText("ownerGroupKey");
  await expect(drawer).not.toContainText("PRIVATE STREET");
});

test("live reporting-insiders view groups only privacy-safe published names", async ({
  page,
}) => {
  await page.goto("/#stock/03770N101/reporting-insiders");

  await expect(page.getByRole("button", { name: "Reporting Insiders" }))
    .toHaveAttribute("aria-current", "page");
  await expect(page.getByRole("heading", { name: "Reporting Insiders" }))
    .toBeVisible();
  await expect(page.getByText("SYNTHETIC OWNER ALPHA", { exact: true }))
    .toBeVisible();
  await expect(page.locator("#reportingInsidersTable")).toContainText("Director");
  await expect(page.locator("#reportingInsidersTable")).toContainText("$1.27K");
  await expect(page.getByText(
    "Owners are grouped only by identical published names in this security view; no private or cross-filing owner identifier is exposed."
  )).toBeVisible();
  await expect(page.getByText("Fixture preview — not real filing data."))
    .toHaveCount(0);
  await expect(page.locator("body")).not.toContainText("Fixture unavailable");
  await expect(page.locator("body")).not.toContainText("0000000002");
});

test("native logo and JSON-derived fund cards activate with Enter and Space", async ({
  page,
}) => {
  await page.goto("/");
  await page.evaluate(() => {
    _popularFundsCache = [{ cik: 1, name: "Fixture Fund" }];
    renderFundsHome();
  });
  const logo = page.getByRole("button", { name: "13F Super Investor Seeker" });
  const card = page.getByRole("button", { name: /Fixture Fund.*CIK 1/ });

  await expect(card).toBeVisible();
  await card.focus();
  await expect(card).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/#fund\/1$/);
  await expect(page.getByText("No quarter data available for this fund yet.")).toBeVisible();

  await logo.focus();
  await expect(logo).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/$/);
  await expect(card).toBeVisible();
  await page.getByRole("button", { name: /Fixture Fund.*CIK 1/ }).click();
  await expect(page).toHaveURL(/#fund\/1$/);
  await expect(page.getByText("No quarter data available for this fund yet.")).toBeVisible();
  await page.getByRole("button", { name: "13F Super Investor Seeker" }).click();
  await expect(page).toHaveURL(/\/$/);

  await page.goto("/");
  await page.evaluate(() => {
    _popularFundsCache = [{ cik: 1, name: "Fixture Fund" }];
    renderFundsHome();
    wireDataActions();
  });
  const spaceCard = page.getByRole("button", { name: /Fixture Fund.*CIK 1/ });
  await expect(spaceCard).toBeVisible();
  await spaceCard.focus();
  await expect(spaceCard).toBeFocused();
  await spaceCard.press(" ");
  await expect(page).toHaveURL(/#fund\/1$/);
});

test("renderFund rejects malformed total values without DOM injection", async ({
  page,
}) => {
  await page.goto("/");
  const rendered = await page.evaluate(() => {
    window.__fundValueXss = false;
    const invalidValues = [
      "<img src=x onerror=window.__fundValueXss=true>",
      "123", "12x", NaN, Infinity, -Infinity,
    ];
    return invalidValues.map(total_value => {
      renderFund({
        cik: 1,
        name: "Fixture Fund",
        quarters: [{
          report_date: "2026-06-30",
          filing_date: "2026-08-14",
          total_value,
          holdings: [],
        }],
      });
      return {
        display: document.querySelector(".fund-stat-value").textContent,
        images: document.querySelectorAll(".fund-stat-value img").length,
      };
    });
  });

  expect(rendered).toEqual([
    { display: "—", images: 0 },
    { display: "—", images: 0 },
    { display: "—", images: 0 },
    { display: "—", images: 0 },
    { display: "—", images: 0 },
    { display: "—", images: 0 },
  ]);
  await expect.poll(() => page.evaluate(() => window.__fundValueXss)).toBe(false);
});

test("deployed hosts ignore fixture opt-in without requesting fixture data", async ({
  page,
}) => {
  let fixtureRequests = 0;
  const deployedOrigin = "http://13f.wesleyyon.com:4173";
  await page.route(`${deployedOrigin}/**`, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("apge-insider-activity.example.json")) {
      fixtureRequests += 1;
      await route.abort("blockedbyclient");
      return;
    }
    const jsonBodies = {
      "/data/funds-index.json": fundsIndex,
      "/data/security_labels.json": securityLabels,
      "/data/stocks/03770N101.json": stock,
      "/data/index.json": { ...fundsIndex, tickers: [stock] },
    };
    if (url.pathname.endsWith(".json.gz")) {
      await route.fulfill({ status: 404, body: "not found" });
      return;
    }
    if (
      url.pathname === "/data/insiders/public/securities/03770N101.json"
    ) {
      await route.fulfill({ status: 404, body: "not found" });
      return;
    }
    if (jsonBodies[url.pathname]) {
      await route.fulfill({ json: jsonBodies[url.pathname] });
      return;
    }
    const files = {
      "/": ["index.html", "text/html"],
      "/index.html": ["index.html", "text/html"],
      "/site-data-loader.js": [
        "site-data-loader.js",
        "application/javascript",
      ],
    };
    if (files[url.pathname]) {
      const [relativePath, contentType] = files[url.pathname];
      await route.fulfill({
        body: fs.readFileSync(path.join(root, relativePath)),
        contentType,
      });
      return;
    }
    await route.abort("blockedbyclient");
  });

  await page.goto(
    `${deployedOrigin}/?insiderPreview=fixture#stock/03770N101/insiders`
  );
  await expect(
    page.getByRole("heading", { name: "No published insider activity" })
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "Insider Activity" }))
    .toHaveAttribute("aria-current", "page");
  await expect(page.getByText("Fixture preview — not real filing data."))
    .toHaveCount(0);
  expect(fixtureRequests).toBe(0);
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

test("insider transaction sort headers expose native keyboard state", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  const valueSort = () => page.locator("#insiderTransactionTable")
    .getByRole("button", { name: "Sort by Value" });
  const valueHeader = () => valueSort().locator("xpath=..");

  await expect(valueSort()).toHaveAttribute("type", "button");
  await expect(valueHeader()).toHaveAttribute("aria-sort", "none");

  await valueSort().focus();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/sort=value/);
  await expect(valueHeader()).toHaveAttribute("aria-sort", "descending");

  await valueSort().focus();
  await page.keyboard.press(" ");
  await expect(page).toHaveURL(/order=asc/);
  await expect(valueHeader()).toHaveAttribute("aria-sort", "ascending");
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
  const detailButton = page.locator(
    "#insiderTransactionTable .filing-detail-button"
  ).first();
  await expect(detailButton).toHaveAttribute("type", "button");
  await detailButton.focus();
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
  await expect(detailButton).toBeFocused();
});

test("public sort and load-error actions use native keyboard controls", async ({ page }) => {
  await page.goto("/");
  await page.evaluate(() => {
    app().innerHTML = `
      <table id="fundTable"><thead><tr>
        ${sortableHeader("onFundSort", "value", "Value", "right")}
      </tr></thead><tbody id="fundTbody"></tbody></table>
      <div id="fundFoot"></div>
      <table id="stockTable"><thead><tr>
        ${sortableHeader("onStockSort", "shares", "Shares", "right")}
      </tr></thead><tbody id="stockTbody"></tbody></table>
      <div id="stockFoot"></div>`;
    curFundRows = [];
    curStockRows = [];
  });

  const fundSortButton = page.getByRole("button", { name: "Sort by Value" });
  await fundSortButton.focus();
  await page.keyboard.press("Enter");
  await expect.poll(() => page.evaluate(() => fundSort.col)).toBe("value");

  const stockSortButton = page.getByRole("button", { name: "Sort by Shares" });
  await stockSortButton.focus();
  await page.keyboard.press(" ");
  await expect.poll(() => page.evaluate(() => stockSort.col)).toBe("shares");

  await page.evaluate(() => {
    history.replaceState({}, "", `${location.pathname}${location.search}#fund/1`);
    showLoadError("Missing fixture", "data/funds/1.json");
  });
  const back = page.getByRole("button", { name: "Back to search" });
  await back.focus();
  await page.keyboard.press(" ");
  await expect(page).toHaveURL(/\/$/);
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
  await page.locator("#insiderTransactionTable .filing-detail-button").first().click();
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
  await expect(row.locator(".filing-detail-button")).toHaveCount(0);
  await expect.poll(() => page.evaluate(() => Boolean(window.__fixtureExecuted)))
    .toBe(false);
  await expect(page.getByRole("dialog", { name: /Form 4 Filing Detail/ }))
    .toHaveCount(0);
});

test("malicious stock IDs cannot execute through Phase 1 controls", async ({
  page,
}) => {
  const stockId = "x');window.__inlineXss=true;//";
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  await expect(page.getByText("Fixture preview — not real filing data.")).toBeVisible();
  await page.evaluate(id => {
    window.loadStock = () => {};
    window.__inlineXss = false;
    currentInsiderStockId = id;
    renderInsiderActivity(currentInsiderFixture, id);
  }, stockId);
  await page.getByRole("button", { name: "Reporting Insiders" }).click();
  const tabExecuted = await page.evaluate(() => window.__inlineXss);

  await page.evaluate(id => {
    window.__inlineXss = false;
    renderStock(
      {
        stock_id: id,
        cusip: id,
        ticker: "XSS",
        issuer: "XSS Test Security",
        instrument_type: "EQUITY",
        holders: [],
      },
      { stock_id: id, cusip: id, ticker: "XSS", issuer: "XSS Test Security" }
    );
  }, stockId);
  await page.getByRole("button", { name: "View insider activity →" }).click();
  const crossLinkExecuted = await page.evaluate(() => window.__inlineXss);

  await page.evaluate(id => {
    window.__inlineXss = false;
    currentInsiderStockId = id;
    renderInsiderActivity(currentInsiderFixture, id);
  }, stockId);
  await page.locator(".insider-rail-section")
    .filter({ hasText: "Largest Latest-Reported Holdings" })
    .getByRole("button", { name: "View all" })
    .click();
  const railExecuted = await page.evaluate(() => window.__inlineXss);

  await page.evaluate(id => {
    window.__inlineXss = false;
    renderInsiderError(
      { ticker: "XSS", issuer: "XSS Test Security", cusip: id },
      id,
      "insiders",
      "test error"
    );
  }, stockId);
  await page.getByRole("button", { name: "Try again" }).click();
  const errorExecuted = await page.evaluate(() => window.__inlineXss);

  expect({ tabExecuted, crossLinkExecuted, railExecuted, errorExecuted }).toEqual({
    tabExecuted: false,
    crossLinkExecuted: false,
    railExecuted: false,
    errorExecuted: false,
  });
});

test("live holder page cross-links to a published 90-day insider view", async ({ page }) => {
  await page.goto("/#stock/03770N101");

  const crossLink = page.getByRole("region", {
    name: "Insider Activity preview link",
  });
  await expect(crossLink).toContainText("Published Forms 3, 4, and 5");
  await expect(crossLink).toContainText("90-day view");
  await expect(crossLink).not.toContainText("Illustrative fixture preview");
  await crossLink.getByRole("button", { name: "View insider activity →" }).click();
  await expect(page).toHaveURL(
    /\?range=90d#stock\/03770N101\/insiders$/
  );
  await expect(page.getByRole("heading", { name: "Synthetic Test Issuer" }))
    .toBeVisible();
  await expect(page.getByRole("button", { name: "90D" }))
    .toHaveAttribute("aria-pressed", "true");
});

test("holders insider cross-link retains its valid routed action", async ({ page }) => {
  await page.goto("/?insiderPreview=fixture#stock/03770N101");
  await page.getByRole("button", { name: "View insider activity →" }).click();
  await expect(page).toHaveURL(
    /\?insiderPreview=fixture&start=2026-04-01&end=2026-06-30&range=6m#stock\/03770N101\/insiders$/
  );
  await expect(page.getByText("Fixture preview — not real filing data.")).toBeVisible();
});

test("delegated stock actions reject forged IDs and preserve canonical routes", async ({
  page,
}) => {
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  await expect(page.getByText("Fixture preview — not real filing data.")).toBeVisible();
  const baseline = await page.evaluate(() => {
    window.__delegatedRealLoadStock = window.loadStock;
    window.__delegatedRealFetch = window.fetch;
    window.__delegatedActionAudit = { calls: [], fetches: [] };
    window.loadStock = (...args) => {
      window.__delegatedActionAudit.calls.push(args);
    };
    window.fetch = (...args) => {
      window.__delegatedActionAudit.fetches.push(String(args[0]));
      return window.__delegatedRealFetch(...args);
    };
    document.body.insertAdjacentHTML("beforeend", `
      <button id="forgedLoadStock" data-action="load-stock" data-stock-id="FORGED999">forged load</button>
      <button id="forgedInsiderPreview" data-action="holders-insider-preview" data-stock-id="FORGED999">forged preview</button>
    `);
    return location.href;
  });

  await page.locator("#forgedLoadStock").click();
  await page.locator("#forgedInsiderPreview").click();
  expect(await page.evaluate(() => ({
    href: location.href,
    ...window.__delegatedActionAudit,
  }))).toEqual({ href: baseline, calls: [], fetches: [] });

  await page.evaluate(() => {
    window.loadStock = window.__delegatedRealLoadStock;
    window.fetch = window.__delegatedRealFetch;
    document.body.insertAdjacentHTML("beforeend", `
      <button id="canonicalLoadStock" data-action="load-stock" data-stock-id="  03770n101  ">canonical load</button>
      <button id="invalidStockView" data-action="load-stock-view" data-stock-id="03770N101" data-stock-view="not-a-view">invalid view</button>
      ${["holders", "insiders", "reporting-insiders"].map(view =>
        `<button id="stockView-${view}" data-action="load-stock-view" data-stock-id="03770n101" data-stock-view="${view}">${view}</button>`
      ).join("")}
    `);
  });

  await page.locator("#canonicalLoadStock").click();
  await expect(page).toHaveURL(/#stock\/03770N101$/);
  for (const view of ["holders", "insiders", "reporting-insiders"]) {
    await page.locator(`#stockView-${view}`).click();
    const suffix = view === "holders" ? "" : `/${view}`;
    await expect(page).toHaveURL(new RegExp(`#stock/03770N101${suffix}$`));
  }

  const invalidBaseline = await page.evaluate(() => {
    window.__delegatedActionAudit = { calls: [], fetches: [] };
    window.__delegatedRealLoadStock = window.loadStock;
    window.__delegatedRealFetch = window.fetch;
    window.loadStock = (...args) => {
      window.__delegatedActionAudit.calls.push(args);
    };
    window.fetch = (...args) => {
      window.__delegatedActionAudit.fetches.push(String(args[0]));
      return window.__delegatedRealFetch(...args);
    };
    return location.href;
  });
  await page.locator("#invalidStockView").click();
  expect(await page.evaluate(() => ({
    href: location.href,
    ...window.__delegatedActionAudit,
  }))).toEqual({ href: invalidBaseline, calls: [], fetches: [] });
});

test("JSON-derived stock and fund identifiers cannot execute in navigation controls", async ({
  page,
}) => {
  const stockId = "x');window.__inlineXss=true;//";
  const fundCik = "1);window.__inlineXss=true;//";
  await page.goto(
    "/?insiderPreview=fixture#stock/03770N101/insiders"
  );
  await expect(page.getByText("Fixture preview — not real filing data.")).toBeVisible();
  await page.evaluate(({ maliciousStockId, maliciousFundCik }) => {
    window.loadStock = () => {};
    window.loadFund = () => {};
    window.__inlineXss = false;
    idx = {
      funds: [{ cik: maliciousFundCik, name: "EVIL FUND" }],
      tickers: [{
        stock_id: maliciousStockId,
        ticker: "EVIL",
        issuer: "Evil Security",
        cusip: "EVILCUSIP",
        instrument_type: "EQUITY",
      }],
    };
    globalSearch("EVIL");
  }, { maliciousStockId: stockId, maliciousFundCik: fundCik });
  await page.locator(".gsearch-item").filter({ hasText: "EVIL" }).first().click();
  const globalStockExecuted = await page.evaluate(() => window.__inlineXss);

  await page.evaluate(({ maliciousFundCik }) => {
    window.__inlineXss = false;
    globalSearch("EVIL");
  }, { maliciousFundCik: fundCik });
  await page.locator(".gsearch-item").filter({ hasText: "EVIL FUND" }).click();
  const globalFundExecuted = await page.evaluate(() => window.__inlineXss);

  await page.evaluate(maliciousFundCik => {
    _popularFundsCache = [{ cik: maliciousFundCik, name: "EVIL FUND" }];
    renderFundsHome();
  }, fundCik);
  await expect(page.locator(".popular-cik img")).toHaveCount(0);
  await expect(page.locator(".popular-cik")).toContainText(fundCik);

  await page.evaluate(maliciousStockId => {
    window.__inlineXss = false;
    stockLookupId = () => maliciousStockId;
    app().innerHTML = "<table><tbody id=\"fundTbody\"></tbody></table>"
      + "<div id=\"fundFoot\"></div>";
    curFundRows = [{
      cusip: maliciousStockId,
      ticker: "EVIL",
      issuer: "Evil Security",
      holding_type: "EQUITY",
      pct: 0,
      value: 0,
      shares: 0,
      ch: null,
      prevPct: 0,
      sparkData: [],
    }];
    renderFundTbody();
  }, stockId);
  await page.locator("#fundTbody .security-label-cell").click();
  const lookupIdExecuted = await page.evaluate(() => window.__inlineXss);

  expect({ globalStockExecuted, globalFundExecuted, lookupIdExecuted }).toEqual({
    globalStockExecuted: false,
    globalFundExecuted: false,
    lookupIdExecuted: false,
  });
});

test("reporting-insiders is a routed fixture subview", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture&page=3#stock/03770N101/insiders"
  );
  await page.getByRole("button", { name: "Reporting Insiders" }).click();
  await expect(page).toHaveURL(/#stock\/03770N101\/reporting-insiders$/);
  await expect(page).not.toHaveURL(/page=/);
  await expect(page.getByRole("button", { name: "Reporting Insiders" }))
    .toHaveAttribute("aria-current", "page");
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

test("leaving the insider view clears filters but Back restores them", async ({ page }) => {
  await page.goto(
    "/?insiderPreview=fixture&ownerScope=officers-directors&plan=10b5-1"
      + "&search=Jane&page=3#stock/03770N101/insiders"
  );
  await page.getByRole("button", { name: "Institutional Holders" }).click();
  await expect(page).toHaveURL(/\?insiderPreview=fixture#stock\/03770N101$/);
  await expect(page).not.toHaveURL(/ownerScope=|plan=|search=|page=/);

  await page.goBack();
  await expect(page).toHaveURL(/#stock\/03770N101\/insiders$/);
  await expect(page).toHaveURL(/ownerScope=officers-directors/);
  await expect(page).toHaveURL(/plan=10b5-1/);
  await expect(page).toHaveURL(/search=Jane/);
  await expect(page).toHaveURL(/page=3/);
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

test("stale live response cannot overwrite reporting-insiders", async ({ page }) => {
  const delayed = await delayFirstLiveSecurityResponse(page);
  await page.goto("/#stock/03770N101/insiders");
  await delayed.started;

  await page.getByRole("button", { name: "Reporting Insiders" }).click();
  await expect(page).toHaveURL(/#stock\/03770N101\/reporting-insiders$/);
  await expect(page.locator("#reportingInsidersTable")).toBeVisible();

  delayed.release();
  await delayed.finished;
  await page.evaluate(() => new Promise(resolve =>
    requestAnimationFrame(() => requestAnimationFrame(resolve))
  ));

  await expect(page).toHaveURL(/#stock\/03770N101\/reporting-insiders$/);
  await expect(page.getByRole("button", { name: "Reporting Insiders" }))
    .toHaveAttribute("aria-current", "page");
  await expect(page.locator("#reportingInsidersTable")).toBeVisible();
  await expect(page.locator("#insiderTransactionTable")).toHaveCount(0);
});

test("stale live response cannot overwrite institutional holders", async ({ page }) => {
  const delayed = await delayFirstLiveSecurityResponse(page);
  await page.goto("/#stock/03770N101/insiders");
  await delayed.started;

  await page.getByRole("button", { name: "Institutional Holders" }).click();
  await expect(page).toHaveURL(/#stock\/03770N101$/);
  await expect(page.locator("#stockTable")).toBeVisible();

  delayed.release();
  await delayed.finished;
  await page.evaluate(() => new Promise(resolve =>
    requestAnimationFrame(() => requestAnimationFrame(resolve))
  ));

  await expect(page).toHaveURL(/#stock\/03770N101$/);
  await expect(page.locator("#stockTable")).toBeVisible();
  await expect(page.locator("#insiderTransactionTable")).toHaveCount(0);
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

const insiderViewports = [
  { width: 1621, height: 970 },
  { width: 1440, height: 900 },
  { width: 1024, height: 768 },
  { width: 768, height: 1024 },
  { width: 390, height: 844 },
];

for (const viewport of insiderViewports) {
  test(`live responsive layout ${viewport.width}x${viewport.height}`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.goto("/#stock/03770N101/insiders");
    await expect(page.getByRole("heading", { name: "Synthetic Test Issuer" }))
      .toBeVisible();
    await expect(page.locator("#insiderTransactionTable")).toBeVisible();
    await expect(page.getByRole("heading", {
      name: "Insider Transaction Timeline",
    })).toBeVisible();
    await expect(page.getByText("Fixture preview — not real filing data."))
      .toHaveCount(0);
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
  });
}

for (const viewport of insiderViewports) {
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
      const tabBounds = await page.locator(".security-tabs .security-tab").evaluateAll(tabs =>
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
        maxDiffPixelRatio: maxVisualDiffPixelRatio,
        threshold: 0.25,
      }
    );
  });
}
