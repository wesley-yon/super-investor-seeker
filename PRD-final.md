# Super Investor Seeker — Product Requirements Document

## Vision

A free, public web app where anyone can search and browse institutional fund holdings from **all** SEC 13F filings. Users can search by fund name or stock ticker across the current universe of roughly 9,400 institutional 13F-HR filers. The app shows portfolio breakdowns, position changes over time, historical trends, and cross-fund ownership analysis.

This is a WhaleWisdom-style product — not limited to a curated list of investors, but covering every 13F filer.

**UI implementation:** The live site lives in `index.html`. It was originally bootstrapped from a `super-investor-seeker.html` prototype (dark theme, Chart.js charts, SVG sparklines, compact data tables), which has since been removed now that `index.html` is the canonical design. Any future UI rework should match what `index.html` currently ships — see the Design section below for the locked-in rules.

---

## Architecture

### Static Public Site, Private Durable Dataset

```
┌──────────────────────┐   authenticated snapshot   ┌────────────────────────┐
│ GitHub Actions       │ <────────────────────────> │ private data repository│
│ pipeline + validators│                            │ full corpus + caches   │
└──────────┬───────────┘                            └────────────────────────┘
           │ exact validated, bounded Pages artifact
           v
┌──────────────────────┐
│ GitHub Pages         │  public indexes plus individually compressed
│ static HTML/JS/CSS   │  fund and stock payloads loaded on demand
└──────────────────────┘
```

**Why this split:** The website remains static and inexpensive, with no server
or database to operate. The complete generated corpus, pipeline state,
registries, reports, and operational caches are not stored in the public code
repository. If an update or validation fails, the last validated snapshot and
live Pages deployment remain unchanged.

### How It Works

1. **GitHub Actions** restores the newest authenticated private snapshot.
2. Weekday maintenance runs inspect SEC EDGAR throughout the filing day; a
   separate weekly pass fully refreshes the CUSIP/OpenFIGI registry.
3. The pipeline discovers all 13F filers, fetches new filings, rebuilds derived
   data, and runs complete corpus validation plus regression tests.
4. Changed data is published as a new private, content-addressed snapshot.
5. Pages restores that exact snapshot, rebuilds a bounded public artifact, and
   refuses stale code or dataset inputs before deployment.
6. Finalization retains the active snapshot plus one fallback and immediately
   removes the temporary public Pages bulk artifact.
7. The browser loads public indexes and individual compressed fund or stock
   payloads on demand. Those public payloads can be enumerated or scraped; the
   private source archive is not exposed as a persistent bulk download.

### Key Numbers

| Metric | Value |
|--------|-------|
| Searchable 13F filers | ~9,400 |
| API calls per filer per quarter | ~2-3 (submissions + index + XML) |
| Time to fetch 1 quarter for all filers | ~55-80 minutes |
| Full backfill or replay | resumable across runs; each hosted ingestion pass has a 210-minute cooperative budget |
| Normal incremental workflow | roughly 40-70 minutes including full rebuild and validation |
| Complete private dataset | approximately 4.6GB before compression |
| Private snapshot archive | approximately 540MB |
| Public code repository | code and tests only; generated data is forbidden |
| Rollback retention | active snapshot plus one validated fallback |

---

## Data Pipeline (`pipeline.py`)

### Discovery: Finding All 13F Filers

SEC publishes quarterly index files that list every filing submitted that quarter:

```
https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.idx
```

This single file contains every 13F-HR filing for that quarter — company name, CIK, accession number, and filing date. One download gives us the complete universe of filers. No need to maintain a list manually.

### Data Fetching Strategy

**Initial backfill:**
1. Download the company.idx for the most recent 4 quarters
2. Parse to extract all unique CIKs that filed 13F-HR
3. For each CIK × quarter, fetch the filing index and holdings XML
4. Checkpoint durable progress before the hosted workflow's cooperative ingestion cutoff
5. Resume from the private snapshot on later runs until the backfill is complete

**Ongoing incremental updates:**
1. Download the company.idx for the current quarter
2. Replay recently accepted filings so late index publication does not create a blind spot
3. Fetch only filings absent from durable pipeline state
4. Preserve verified historical report dates already in the private snapshot
5. Rebuild registries and browser-facing data, then run full validation before publication

### Resume Capability

The pipeline must track what has been fetched. Strategy:

- Maintain `data/pipeline_state.json` inside the private snapshot, listing all
  accession numbers already processed
- On each run, skip any filing already in the state file
- Restore that state transactionally at the start of each hosted run
- This means an interrupted run can resume without losing progress or
  publishing an inconsistent snapshot

### Four-Quarter Discovery and Display Window

The site shows the newest 4 quarters for charts and comparisons. On each
incremental run, the pipeline:

1. Searches the configured recent SEC filing quarters (4 by default)
2. Fetches new filings and amendments found there or in recent-feed replay
3. Replaces matching report dates but never prunes unrelated verified fund history
4. Regenerates stock-level aggregations and browser payloads
5. Limits fund-page charts and comparisons to the newest 4 stored quarters

### Output Structure

```
data/                     # Private snapshot input; ignored by Git
  index.json              # Master search index: all fund names + CIKs + all tickers
  pipeline_state.json     # Durable processed-filing and retry state
  funds/
    1067983.json          # Berkshire Hathaway — retained verified history
    1336528.json          # Pershing Square
    ...                   # ~9,400 fund files
  stocks/
    037833100.json        # AAPL equity, keyed by canonical security identity
    29273V100__CALL.json  # Option family kept separate from its underlying
    ...                   # tens of thousands of stock/security files
```

Only the three browser indexes and individually gzip-compressed files from
`data/funds/` and `data/stocks/` enter the Pages artifact. Pipeline state,
health reports, registries, and operational caches remain private.

### Fund JSON Format (`data/funds/{cik}.json`)

```json
{
  "cik": 1067983,
  "name": "BERKSHIRE HATHAWAY INC",
  "quarters": [
    {
      "report_date": "2026-03-31",
      "filing_date": "2026-05-15",
      "total_value": 263095703570,
      "num_holdings": 29,
      "holdings": [
        {
          "ticker": "AAPL",
          "issuer": "Apple Inc.",
          "cusip": "037833100",
          "class": "COM",
          "value": 57843260493,
          "shares": 227917808,
          "holding_type": "EQUITY"
        }
      ]
    }
  ]
}
```

All `value` and `total_value` fields are normalized dollars, never SEC
thousands. The website computes derived fields (% of portfolio, QoQ changes,
sparkline data) client-side from this raw data.

### Stock JSON Format (`data/stocks/{stock_id}.json`)

```json
{
  "stock_id": "037833100",
  "cusip": "037833100",
  "ticker": "AAPL",
  "issuer": "Apple Inc.",
  "instrument_type": "EQUITY",
  "holders": [
    {
      "cik": 1067983,
      "name": "BERKSHIRE HATHAWAY INC",
      "history": [
        { "date": "2026-03-31", "shares": 227917808, "value": 57843260493, "pct_of_fund": 21.986 },
        { "date": "2025-12-31", "shares": 227917808, "value": 61961735283, "pct_of_fund": 22.601 }
      ]
    }
  ]
}
```

Stock files are built by cross-referencing all fund holdings after the fund files are generated. **Stock files and `index.json` must be regenerated at the end of every pipeline run**, even if the run only processed a subset of filers. This ensures stock-level data stays consistent with the latest fund data.

### Index JSON Format (`data/index.json`)

```json
{
  "funds": [
    { "cik": 1067983, "name": "BERKSHIRE HATHAWAY INC" },
    { "cik": 1336528, "name": "PERSHING SQUARE CAPITAL MANAGEMENT LP" }
  ],
  "tickers": [
    {
      "stock_id": "037833100",
      "cusip": "037833100",
      "ticker": "AAPL",
      "issuer": "Apple Inc.",
      "instrument_type": "EQUITY",
      "last_seen": "2026-06-30",
      "current_holder_count": 1000,
      "holder_count": 1400
    }
  ],
  "last_updated": "2026-08-05T21:51:45Z",
  "total_filers": 9410,
  "total_tickers": 35255
}
```

This browser index is loaded once when the site opens. JavaScript searches it
locally; individual fund and security payloads remain on-demand.

### CUSIP-to-Ticker Mapping

13F filings report holdings by CUSIP, not ticker. The pipeline needs to map CUSIPs to tickers. Approach:

1. Combine retained filing evidence, reviewed overrides, SEC company and fund
   metadata, and prior validated registry state.
2. Resolve missing or suspicious current CUSIPs through OpenFIGI. Weekday
   updates tolerate transient vendor outages; the weekly authenticated full
   refresh fails closed if any batch is incomplete or malformed.
3. Store the operational map in `.cache/cusip_map.json` and the display registry
   in both private snapshot copies used for recovery and publication.
4. Holdings without a safe ticker retain their CUSIP-based `stock_id`, so they
   remain distinct, searchable, and displayable without inventing a symbol.

### SEC EDGAR Rate Limiting

- Maximum **8 requests per second** sustained (SEC caps at 10; 8 leaves headroom)
- **Retry with exponential backoff** (2s, 4s, 8s, 16s, max 60s) on 403, 429, 503
- **User-Agent header required:** must contain a real contact email, format `"YourName your@email.com"`. SEC blocks requests without valid contact info. In production we read this from the `SEC_USER_AGENT` env var, which is populated from a GitHub Actions repo secret — never hard-coded into the workflow file.
- Accept-Encoding: gzip, deflate
- **Concurrency:** a thread-safe rate limiter lets a small pool of workers (8
  by default) issue overlapping requests while still respecting the 8 req/sec
  cap. This absorbs network round-trip latency without increasing the aggregate
  SEC request rate.

### Key SEC EDGAR Endpoints

| Purpose | URL |
|---|---|
| Quarterly filing index | `https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/company.idx` |
| Company submissions | `https://data.sec.gov/submissions/CIK{cik_padded_10}.json` |
| Filing document index | `https://data.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/index.json` |
| Filing document | `https://data.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/{filename}` |
| Company tickers | `https://www.sec.gov/files/company_tickers.json` |

---

## Website (`index.html`)

### Tech Stack

- Single `index.html` file with inline CSS and JavaScript — no frontend compilation, npm, or framework
- **Chart.js** from CDN for stacked bar charts
- **SVG sparklines** rendered inline (see prototype)
- Data loaded via `fetch()` from `data/*.json` files
- Client-side search against the index.json

### Features

**Fund Search & Browse:**
- Search bar with autocomplete — searches across all roughly 9,400 filer names
- Type-ahead results appear as you type
- Click any fund to see their portfolio

**Fund Portfolio View:**
- Stats: 13F Equity Value (not "Portfolio Value"), number of positions, top holding
- Note explaining that 13F excludes cash, T-bills, private holdings, and operating businesses
- Stacked bar chart: portfolio composition over time (top 5 holdings + "Other")
- Holdings table columns in order: rank, ticker, company, 4-quarter sparkline trend, % of portfolio, previous % of portfolio, % change (QoQ badge), value, shares
- QoQ changes computed client-side by comparing current quarter to prior quarter in the JSON
- Sparklines computed from the quarterly history in the JSON
- Click any ticker → jump to stock lookup

**Stock Search & Browse:**
- Search bar — searches across all tickers and company names
- Click any ticker to see holders

**Stock Detail View:**
- Stats: number of institutional holders, total held value, total shares, largest holder
- Holders table: fund name, shares, value, % of that fund's portfolio, QoQ change, sparkline
- Click any fund name → jump to that fund's portfolio

**Cross-Linking:**
- Fund view tickers are clickable → stock detail
- Stock view fund names are clickable → fund portfolio
- Tab navigation always visible (Fund Lookup / Stock Lookup)

### Design

The design is already implemented in `index.html`. The rules below are the locked-in decisions (originally made against a `super-investor-seeker.html` prototype, now removed) that any future rework must preserve:
- Fund names only — no "Manager" or "Person" column. The production data has no source for person names across roughly 9,400 filers.
- 4-quarter sparklines and 4-quarter charts (not 8, as the prototype used).

Design rules to preserve:
- Dark theme: `#06080d` background, `#0d1017` surface, `#111620` cards, `#1c2333` borders
- Accent: `#4e8cff`, Green: `#34d399`, Red: `#f87171`, Gold: `#fbbf24`
- Fonts: JetBrains Mono (data), Source Sans 3 (text) — both from Google Fonts CDN
- Color-coded QoQ badges: gold NEW, green ↑X%, red ↓X%, gray —
- SVG sparklines: green bars = shares increased, red = decreased

### Data Loading Strategy

```javascript
// site-data-loader.js transparently maps detail fetches to .json.gz.
// On page load, load the browser-facing search index.
const index = await fetch('data/index.json').then(r => r.json());

// Detail payloads are individually gzip-compressed in the Pages artifact.
const fund = await fetch(`data/funds/${cik}.json`).then(r => r.json());

const stock = await fetch(`data/stocks/${securityPath}.json`).then(r => r.json());
```

The loader first requests the published `.json.gz` payload and decompresses it
in the browser, with a compatibility fallback for local development. Search and
detail data therefore load on demand; ordinary visitors do not download the
complete corpus up front.

---

## GitHub Actions Workflows

### `update-data.yml`

- Runs repeatedly during the Monday-Friday 7am-6pm America/New_York filing
  window and supports manual dispatch.
- Uses a shared `data-maintenance` concurrency group so Update and the weekly
  CUSIP refresh cannot mutate snapshot state concurrently.
- Checks out live `main`, authenticates to the private data repository with a
  short-lived GitHub App token, and transactionally restores the newest
  validated snapshot.
- Runs the quarterly pipeline, recent-filing replay, registry rebuild, complete
  corpus validator, and regression tests.
- Mints a fresh write-scoped token only immediately before publication.
- If the content digest is unchanged, reuses the active release. Otherwise it
  publishes a draft release, round-trips its manifest and archive, then marks it
  public and passes the exact release identity to Pages.
- Never commits generated data to the public repository.

### `refresh-cusip-registry.yml`

- Performs the weekly full OpenFIGI/CUSIP refresh under the same maintenance
  lock and private-snapshot contract.
- Requires a configured API key and fails publication if the authenticated full
  refresh cannot complete reliably.
- Validates and deploys changed derived data through the same exact-target Pages
  workflow.

### `deploy-pages.yml`

- Accepts an exact code SHA, private release tag, and dataset digest from a
  publisher; manual dispatch can also select a retained rollback release.
- A scheduled recovery pass repairs interrupted finalization and removes any
  orphaned Pages artifact.
- Restores and validates the exact private snapshot, builds only the explicit
  public allowlist, rejects stale public or private inputs, and deploys through
  GitHub Actions Pages.
- Records the successful deployment identity in the private release, retains
  the active release plus one fallback, and deletes every temporary public
  `github-pages` artifact.

### `test.yml` and schedule keepalive

- CI compiles entry points, rejects generated private paths in the current tree
  and Git history, runs the loader test, and executes the complete Python suite.
- Publishing workflows repeat the regression gates against their actual code
  and restored data rather than depending on a parallel CI result.
- A tiny, off-main heartbeat branch provides repository activity so GitHub does
  not disable schedules after 60 quiet days. It does not alter `main`, trigger
  deployment, or add meaningful clone weight.

Required repository configuration:

- `SEC_USER_AGENT`, `OPENFIGI_API_KEY`, and `DATA_ARCHIVE_APP_PRIVATE_KEY`
  repository secrets.
- `DATA_ARCHIVE_APP_CLIENT_ID` repository variable.
- GitHub App access restricted to the private data repository, with read access
  for restore and a separately minted write token for publication.
- GitHub Pages publishing source set to **GitHub Actions**.

---

## GitHub Pages Configuration

- Publishing source: GitHub Actions, not a branch directory.
- Static entry points: `.nojekyll`, `CNAME`, `index.html`, and
  `site-data-loader.js`.
- Public data allowlist: the three browser indexes plus individual compressed
  fund and stock payloads.
- Custom domain: `https://13f.wesleyyon.com/`, fronted by Cloudflare.
- Direct scripted reads may receive a Cloudflare challenge. Individual payloads
  are nevertheless public and should be treated as scrapeable.

---

## Change and Verification Order

### 1. Data pipeline changes

Preserve these `pipeline.py` requirements:
1. Downloads SEC quarterly index files to discover all 13F filers
2. Fetches and parses 13F-HR XML holdings for each filer
3. Maps CUSIPs to tickers
4. Outputs fund JSON, stock JSON, and browser indexes into private `data/`
5. Tracks processed, retry, amendment, identity, and health state for safe resume
6. Has proper rate limiting and retry logic
7. Accepts `--all` (all filers), `--cik XXXXXXX` (single filer), `--quarters N` (limit quarters)

**Verification:** Use fixture-backed unit tests first. For an authorized corpus
check, restore a private snapshot and run the narrow command plus
`validate_data.py`; never commit the resulting `data/` or `.cache/` paths.

Full-corpus publication must occur through the hosted workflow so snapshot
identity, validation, retention, and deployment gates are exercised together.

### 2. Static website changes

Build `index.html` that:
1. Loads `data/index.json` on startup for search
2. Renders fund lookup, fund portfolio, stock lookup, stock detail views
3. Matches the prototype design exactly
4. Computes QoQ changes and sparklines client-side from the quarterly data

**Verification:** Build a local Pages artifact from an authenticated snapshot,
serve that artifact, exercise fund and stock routes, and run the loader and
frontend regression tests.

### 3. Automation or publication changes

1. Run the complete local suite and workflow-resilience tests.
2. Confirm neither `data/` nor `.cache/` is tracked or present in Git history.
3. Publish the code change to `main` only after the local gates pass.
4. Require hosted Test, Update/Refresh, exact Pages deployment, finalization,
   rollback retention, and artifact cleanup to succeed.
5. Reconcile the private manifest and deployment marker, then verify the live
   site through a browser-capable path.

---

## Prompting Tips for Claude Code

### 1. Change one layer at a time
"Change the data pipeline only. Preserve the private snapshot contract and run
the complete regression suite before publishing." Then, separately: "Change
the website and verify it against a locally built Pages artifact."

### 2. No server. No database. No framework.
"The website is a single index.html file. It loads JSON via fetch(). No Python
backend, React, npm, or frontend compilation. Deployment still builds and
validates a bounded Pages artifact from the authenticated private snapshot."

### 3. The pipeline discovers filers from SEC index files
"Do NOT hardcode a list of filers. The pipeline downloads SEC's quarterly company.idx file to discover ALL 13F filers automatically."

### 4. Resume capability is critical
"The pipeline must track processed filings in pipeline_state.json inside the
private snapshot. If interrupted, the next run restores that authenticated
snapshot and picks up where it left off."

### 5. Reference the shipped design
"Match the design in `index.html` — same dark theme, same tables, same Chart.js charts, same sparklines. (The original `super-investor-seeker.html` prototype was removed once `index.html` became canonical.)"
