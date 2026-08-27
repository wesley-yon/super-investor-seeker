# Super Investor Seeker Architecture

## Vision

A free, public web app where anyone can search and browse institutional fund
holdings from **all** SEC 13F filings. Users can search by fund name or stock
ticker across the discovered filer universe. The app shows portfolio
breakdowns, position changes over time, historical trends, and cross-fund
ownership analysis.

This is a WhaleWisdom-style product — not limited to a curated list of investors, but covering every 13F filer.

**UI implementation:** The live site and canonical design live in `index.html`.
Any future UI rework must preserve the rules in the Design section below.

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
│ GitHub Pages         │  public indexes plus individually compressed fund,
│ static HTML/JS/CSS   │  stock, and optional validated insider payloads
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
4. Bounded Section 16 maintenance may update private normalized records and
   checkpoints. A separate manual, default-off gate may then rebuild the entire
   reviewed public-insider policy corpus; merge, push, and scheduled events
   cannot select that materialization gate.
5. Changed data is published as a new private, content-addressed snapshot.
6. Pages restores that exact snapshot, rebuilds a bounded public artifact, and
   refuses stale code or dataset inputs before deployment.
7. Finalization verifies the active snapshot and deployment marker, preserves
   prior private releases and tags for explicit manual reconciliation, and
   immediately removes only the current run's temporary public Pages artifact.
8. The browser loads public indexes and individual compressed fund or stock
   payloads on demand. When a validated insider generation is present, the
   insider routes load its bounded per-security payload and digest-bound filing
   details from the same origin. The private source archive is never exposed as
   a persistent bulk download.

### Live Insider Browser Boundary

The `insiders` and `reporting-insiders` stock subroutes are static-site views;
they do not introduce an API server. `site-data-loader.js` transparently maps
the exact admitted public insider `.json` paths to their packaged `.json.gz`
files. `index.html` then applies a closed contract-1 validator before rendering.
A 404 is a public-data empty state. Any other fetch, byte-limit, UTF-8, shape,
identity, or reconciliation failure is a generic error and cannot fall through
to the local illustrative fixture.

A filing drawer request must originate from exactly one filing reference in the
currently validated security payload. The accession, relative same-origin path,
declared bytes, and SHA-256 digest must all match before the detail is rendered.
Only the privacy-screened name-as-filed and company relationship/title already
admitted by `insider_publication.py` can reach owner display surfaces; there is
no browser owner catalog, private identifier, or cross-filing identity lookup.

Static security payloads contain the complete bounded canonical row set. Browser
filters and sorting do not redefine financial values, and the table exposes at
most 100 rows per URL-backed client page while cards, rail, and timeline use the
entire filtered set. Because no daily price/split/currency provider contract is
approved, the live chart is transaction-only and uses only a transaction's
reported price when present. It performs no browser-to-provider calls and draws
no daily price line. Public materialization remains a separate manual, default-off
operation; loading these public files cannot trigger it.

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
    ...                   # one file per discovered fund
  stocks/
    037833100.json        # AAPL equity, keyed by canonical security identity
    29273V100__CALL.json  # Option family kept separate from its underlying
    ...                   # one file per public security identity
  insiders/
    private/              # Raw/normalized ownership records and durable state
      accessions/...      # Immutable source plus parser-versioned derivations
      state/
        publication-policy-v1.json  # Reviewed public issuer/class allowlist
    public/               # Generated only by the private-to-public boundary
      manifest.json
      securities/*.json
      filings/*.json
```

Only the three browser indexes, individually gzip-compressed files from
`data/funds/` and `data/stocks/`, and the optional validated insider manifest
plus compressed `securities/` and `filings/` projections enter the Pages
artifact. Pipeline state, insider private records, health reports, registries,
and operational caches remain private.

### Section 16 Publication Policy and Materialization

`approved-issuers-v1` authorizes bounded private ingestion; it does not by
itself authorize an issuer for the public website. The separate private
`publication-policy-v1` state is a reviewed, versioned allowlist of public
issuer CIKs and exact security-class mappings. Its issuer set may be a strict
subset of ingestion-approved issuers. Every materialization rebuilds all policy
issuers—not merely the issuer touched by the immediately preceding maintenance
run—so one atomic replacement is always a complete policy corpus.

Private snapshots created before Section 16 may lack both authority documents.
The manual-only
`.github/workflows/initialize-empty-private-insider-authority.yml` workflow is
the sole genesis boundary for that state. It binds exact current `main` and the
newest private dataset, accepts only missing or exact empty authority roots,
and publishes a private replacement only after proving that both roots are the
canonical empty contracts and that the bounded public tree is unchanged. A
partial exact-empty genesis is safely repairable; malformed or nonempty state
fails before a missing counterpart is created. The empty policy represents
durable deny-all state, and the offline materializer independently rejects a
policy with no reviewed issuer rows.

The fixed-scope `.github/workflows/approve-servicenow-insider-ingestion.yml`
authorizes only ServiceNow CIK `0001373715` for future private ingestion. It
pins the newest private snapshot by exact dataset ID, calls
`scripts/approve_insider_issuer.py` under the maintenance/publication locks,
and uses the state store's compare-and-swap revision. The resulting snapshot is
private-only: `publication-policy-v1` and the bounded public artifact must both
remain unchanged, and the workflow has no public materializer or Pages job.
Consequently, ingestion approval cannot by itself publish ServiceNow records.

`scripts/publish_insider_activity.py` is an offline production adapter around
the Phase 4 projection library. It accepts only a fixed repository root and a
bounded `incremental`, `backfill`, or `reparse` maintenance identity. Before any
public write it requires an exact completed checkpoint. Because the v1
incremental state stores issuer scope only in queued accessions, the adapter
rejects an empty incremental completion as unbound even though ingestion may
retain it as a valid no-op. The adapter opens every referenced
canonical normalized record by the exact SHA-256 recorded in issuer state
without invoking the filing-index or ownership-XML parsers, re-derives and
exactly reconciles issuer state, verifies complete policy mappings, applies
explicit canonical UTC freshness timestamps, and enforces aggregate ceilings of
15,000 normalized filings and 250 MB of canonical normalized input before
building every policy issuer in memory. It then invokes the journaled
`write_insider_publication` boundary once. Any missing, stale, unapproved,
ambiguous, incomplete, noncanonical, or unmapped input fails before the public
tree is replaced.

The hosted entry point is deliberately manual and default-off. A
`workflow_dispatch` must select a bounded insider maintenance mode and separately
set `publish_insider_publication=true` with exact timestamps. The materializer
runs after private checkpoint validation and before generated-data validation,
full tests, snapshot publication, and Pages selection. It performs no SEC or
OpenFIGI calls and cannot silently broaden or initiate a backfill or reparse.

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
  "last_updated": "2026-01-01T12:00:00Z",
  "total_filers": 2,
  "total_tickers": 1
}
```

The values above are illustrative. This browser index is loaded once when the
site opens. JavaScript searches it locally; individual fund and security
payloads remain on-demand.

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

- Single `index.html` file with inline CSS and JavaScript — no frontend compilation or framework
- Native HTML/SVG/CSS charts and sparklines; no runtime chart package or CDN script
- npm is test-only: the pinned Playwright harness never enters the Pages artifact
- Data loaded via `fetch()` from `data/*.json` files
- Client-side search against the index.json

### Features

**Fund Search & Browse:**
- Search bar with autocomplete — searches across all indexed filer names
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

The design is implemented in `index.html`. Any future rework must preserve:
- Fund names only — no "Manager" or "Person" column. The production data has
  no source for person names across the complete filer universe.
- 4-quarter sparklines and 4-quarter charts.

Design rules to preserve:
- Canonical light theme: `#f4f0e8` page background, `#f7f3eb` surface, `#fcfaf5` cards, and `#d6cfc3` taupe borders
- Accent: `#006b4f`, Green: `#007342`, Red: `#97281f`, Gold: `#744620`; use the corresponding light semantic fills rather than a dark-theme palette
- Fonts: Newsreader/Georgia for editorial headings and Source Sans 3 for text/data — both loaded from Google Fonts in `index.html`
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

The loader also recognizes only the exact same-origin insider security and
filing path grammar. Phase 5 uses those paths for the live `insiders` and
`reporting-insiders` subviews, then applies the closed browser contract before
rendering. The illustrative fixture adapter remains loopback-only, explicit,
and default-off; live fetch or validation failures cannot fall through to it.

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
- Supports bounded, separately authorized insider maintenance. Scheduled
  ingestion is repository-variable gated; public insider materialization is
  manual-only, default-off, timestamp-bound, and requires a completed matching
  checkpoint plus the private reviewed publication policy.
- Runs the offline public-insider materializer, when explicitly selected, after
  private checkpoint validation and before all public validation and tests.
  Merge, push, and scheduled runs cannot select this step.
- Mints a fresh write-scoped token only immediately before publication.
- If the content digest is unchanged, reuses the active release. Otherwise it
  publishes a draft release, round-trips its manifest and archive, then marks it
  public and passes the exact release identity to Pages. It retains the draft's
  immutable release ID and, before every tag-addressed upload or publication
  retry, re-resolves that ID, exact owned metadata and asset state, current
  `main`, the restored base, and the locally packed asset digests.
- GitHub release mutations do not expose a conditional update predicate. These
  checks narrow the client-side race window; private write authority therefore
  remains confined to the serialized workflow publisher rather than treating a
  non-cooperating external writer as part of the supported CAS boundary.
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
- A scheduled recovery pass repairs interrupted finalization and removes the
  current run's temporary Pages artifact.
- Restores and validates the exact private snapshot, builds only the explicit
  public allowlist, rejects stale public or private inputs, and deploys through
  GitHub Actions Pages.
- Records the successful deployment identity in the private release and deletes
  the current run's temporary public `github-pages` artifact. Private releases,
  drafts, and dataset tags are preserved for explicit manual reconciliation:
  GitHub does not provide a conditional/versioned release or tag deletion API,
  so automatic retention cleanup could delete a record changed after a
  client-side ownership check.

### `test.yml` and schedule keepalive

- CI uses Python 3.11 and Node 22, installs only hash-locked Python and
  package-lock-pinned browser-test dependencies, compiles entry points, rejects
  generated private paths in the current tree and Git history, runs the loader
  and Playwright suites, and executes the complete Python suite.
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
- Public data allowlist: the three browser indexes, individual compressed fund
  and stock payloads, and—when present—the validated insider manifest plus its
  exact compressed security and filing topology.
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
identity, validation, conservative history preservation, and deployment gates
are exercised together.

### 2. Static website changes

Build `index.html` that:
1. Loads `data/index.json` on startup for search
2. Renders fund lookup, fund portfolio, stock lookup, stock detail views
3. Preserves the design rules above
4. Computes QoQ changes and sparklines client-side from the quarterly data

**Verification:** Build a local Pages artifact from an authenticated snapshot,
serve that artifact, exercise fund and stock routes, and run the loader and
frontend regression tests.

### 3. Automation or publication changes

1. Run the complete local suite and workflow-resilience tests.
2. Confirm neither `data/` nor `.cache/` is tracked or present in Git history.
3. Publish the code change to `main` only after the local gates pass.
4. Require hosted Test, Update/Refresh, exact Pages deployment, finalization,
   rollback preservation, and artifact cleanup to succeed.
5. Reconcile the private manifest and deployment marker, then verify the live
   site through a browser-capable path.
