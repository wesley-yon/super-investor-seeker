# Super Investor Seeker Architecture

## Vision

A free, public web app where anyone can search and browse institutional fund
holdings from **all** SEC 13F filings. Users can search by fund name or stock
ticker across the discovered filer universe. The app shows portfolio
breakdowns, position changes over time, historical trends, and cross-fund
ownership analysis.

This is a WhaleWisdom-style product — not limited to a curated list of investors, but covering every 13F filer.

**UI implementation:** The live markup and canonical design live in `index.html`; application behavior lives in `app.js`.
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
   separate overnight pass deterministically rebuilds the SEC security master.
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

## Data Pipeline (`pipeline.py`)

`sec_http.py` shares redirect handling and per-instance pacing while each SEC
source retains its own admission and retry policy. `atomic_files.py` shares
sibling-file replacement for durable JSON writers; serializers, directory
durability, and interruption cleanup remain explicit at each call site.

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
  "last_updated": "2026-01-01T12:00:00Z",
  "total_filers": 2,
  "total_tickers": 1
}
```

The values above are illustrative. This browser index is loaded once when the
site opens. JavaScript searches it locally; individual fund and security
payloads remain on-demand.

### CUSIP-to-Ticker Mapping

13F filings report holdings by CUSIP, not ticker. CUSIP therefore remains the
canonical security identity; ticker is dated display metadata that is published
only when official evidence meets a fail-closed mapping rule.

1. The quarterly official Section 13(f) securities list defines the reportable
   universe, issuer description, class description, and lifecycle status.
2. SEC-published fails-to-deliver archives provide direct, settlement-dated
   CUSIP-symbol observations. A recent repeated pair is the primary mapping
   signal, but a security is absent when it has no positive fail balance. The
   historical loader accepts the SEC's quarterly bundles beginning with the
   first actual observation on March 22, 2004 through June 2009 and the
   half-month archives published from July 2009 onward. Those files use
   disjoint date ownership: A owns calendar days 1-14 and B owns day 15 through
   month-end. The cutover audit verified that boundary across all 411 available
   half-month archives and 23,458,560 raw rows without an exception or overlap.
   A clean discovery must be period-continuous through the latest mature
   boundary. Every parsed
   settlement date must belong to its URL period except for one machine-checked
   source anomaly: the `2004q1` ZIP also contains April 1, and its complete
   normalized April 1 row multiset is an exact duplicate of the April 1 rows in
   `2004q2`. Both archive inventories retain the same count and SHA-256 boundary
   proof, independent of the active CUSIP filter. Only after those proofs match
   is the Q1 copy discarded, leaving Q2 as the sole owner and preventing double
   counting. February, dates before March 22, any other Q1 spillover, or a
   missing or unequal proof fails closed. Every later quarterly bundle through
   2009 Q2 must cover all three calendar months, and every text member in a
   quarterly bundle is parsed.
3. SEC company-ticker and fund-series files validate current symbols. For
   titleless fund metadata, a unique current SEC CIK/series/class identity
   corroborates the repeated exact FTD CUSIP-symbol pair. The checksummed fund
   identity is retained in `symbol_validation_fund_identity`; an active official
   13F issuer must agree with filer identity and its trust brand must occur in
   each recent FTD description. Missing or competing fund identities cannot
   supply this proof. Company-name normalization handles recognized trailing
   share descriptions without fuzzy issuer matching. Structured
   Schedules 13D/G prove an exact CUSIP-to-CIK/class relationship, and a
   periodic filing's same-class inline XBRL context can complete the bridge to a
   symbol and exchange. Repeated 13F filer descriptions may classify an
   instrument but cannot by themselves override ambiguity.
4. Each public registry entry carries `mapping_status`, `ticker_source`, and
   `ticker_as_of`. `resolved` requires a ticker, one of the approved SEC source
   enums, and a canonical evidence date. `unresolved`, `ambiguous`,
   `no_listed_symbol`, and `malformed_as_filed` publish no ticker or ticker
   provenance.
5. Durable private state lives in `.cache/sec_security_master.json` and
   `.cache/sec_source_state.json`. FTD archive entries retain checksummed
   inventory metadata and an explicit `boundary_date_proofs` list (empty for
   ordinary archives) rather than per-security rows; a compact per-CUSIP
   timeline stores symbol-set intervals, aggregate counts, and a first boundary
   plus the last 32 exact date witnesses. The mapping policy is capped at the
   exactly retained 31-day window. The two newest archives remain as a
   reversible mutable tail, while older checksum mutation fails closed. One
   append-only CUSIP filter log and per-archive high-water marks avoid a full
   universe copy per archive, and operational ZIP parsing filters rows as a
   stream. Raw source hashes, fetch cursors, conflicts, and candidate evidence
   remain private; browser data receives only the fail-closed result.
6. Holdings without a safe ticker retain their CUSIP-based `stock_id`, so notes,
   warrants, preferred shares, and unresolved common equity remain distinct and
   displayable without inheriting an issuer's common-stock symbol.

The only approved `ticker_source` values are `sec_ftd` and `sec_ixbrl`.
`sec_13f_list`, `sec_company_tickers`, `sec_fund_series`,
`sec_schedule_13dg`, and `sec_13f_filer_consensus` are corroborating SEC
metadata sources for identity, issuer, class, kind, labels, or candidate
validation; none can publish a ticker alone. Weekday runs reuse the verified
SEC master and leave new identities tickerless pending evidence maintenance.
The overnight workflow uses `--refresh-security-master` to fetch new or changed
SEC inputs and runs the complete deterministic registry/provenance audit.
`--rebuild-security-master` is reserved for the one-time legacy cutover or an
explicit clean rebuild. The overnight workflow exposes that clean path as the
manual `rebuild_security_master` input;
it stages against empty source/master files and promotes the pair only after
the full acceptance audit passes.

Clean EDGAR exception discovery is transactionally resumable without rewriting
the large pair per network batch. Each 100-CUSIP batch becomes one atomic,
SHA-256 hash-chained journal file containing only normalized discovery
diagnostics and exact filing evidence for those observed securities. On resume,
the candidate identities and evidence fingerprints must match the journal's
contiguous prefix. A stale, torn, or tampered prefix is discarded and safely
refetched before publication. After every batch succeeds, the pipeline performs
one source-state copy, one deterministic master rebuild and audit, and one
source-state-first paired write. Hosted jobs cache only the manifest and compact
filing-scoped journals; the official-list-bearing staged source state and master
remain process-local.

Before the first clean rebuild, the pipeline reconstructs the immutable
`reported_issuer`, `reported_class`, `reported_cusip`, optional
`reported_figi`, accession, and report-date evidence in retained holdings from
the SEC's quarterly Form 13F data sets. The index is restricted to exact
retained accessions and CIK/report-date targets. Where the bulk history is not
sufficient, exact SEC Archives accession XML (or structurally provable legacy
filing text) supplies the fallback; unresolved rows remain unchanged and block
a complete cutover rather than being synthesized from canonical metadata.
The hosted cutover performs a resume-aware free-space preflight, streams target
collection one fund at a time, and checkpoints completed quarterly ZIPs into a
private plan-addressed SQLite candidate. Exact-accession fallbacks are inserted
before that candidate's single finalization, avoiding a second full SQLite copy.
The checkpoint key binds normalized source URLs, exact target scope, and the
parser/schema contract; landing-page markup alone cannot invalidate useful
progress. The complete logical post-backfill corpus is verified before writes,
then each fund is atomically replaced one at a time. A killed prefix is
idempotent on retry and cannot be published because validation and snapshot
publication are later fail-closed steps. Every retained non-placeholder holding
is covered by a canonical quarter-level `reported_identity_sources` witness
containing the exact accession, report date, SEC URL, and SHA-256 checksum; the
post-apply audit binds those compact references to the same SQLite evidence
before the disposable index can be removed.
The immutable holding identity fields are covered by composition-hash protocol
v3; the compact source list is independently schema-validated,
accession-bound, and checked for complete holding coverage at publication. The
cutover freezes a source-neutral public mapping projection before any
rewrite, compares it with the SEC-only result afterward, and blocks on any
change to retained fund/quarter/holding counts, values, or position identity.
The local difference report is excluded from snapshots and workflow artifact
uploads and is never an input to resolution.

After the direct FTD pass, automatic EDGAR exception discovery examines only
current official-list gaps, identities first reported during the trailing six
months, recent exact-FTD/conflict records, due iXBRL revalidations, and retryable
prior failures. Historical corpus-only gaps remain tickerless. Work is ordered
with due revalidations, changed fingerprints, and current ambiguities first,
then bounded to 50 CUSIPs per incremental run or 250 during a clean rebuild.
Terminal decisions fall out so an exceptional backlog drains deterministically.
Discovery verifies the exact CUSIP in a structured
Schedule 13D/G and then requires compatible same-class periodic-filing evidence
before emitting `sec_ixbrl`. Exact official-list row changes reopen a terminal
result, but routine quarterly URL/checksum/period churn does not. Accepted
source hashes and evidence, terminal no-symbol decisions, and retryable
diagnostics are embedded in
`.cache/sec_source_state.json`; there is no separate manual exception queue or
third security-mapping cache.

The publication audit also rejects plausibly decoded but truncated current
symbol feeds or official lists. It applies conservative clean-build population
floors, title or fund-series/class identity sanity, a latest-completed-quarter
check for the official list, and bounded feed/list/resolved-mapping regressions
against the last verified master. Thus a small valid JSON response cannot erase
a previously verified ticker while leaving the FTD coverage ratio unchanged.
The official-list parser treats its normalized five-field rows as set
membership because SEC-generated files can contain byte-identical repeats.
Only complete duplicates are collapsed; rows sharing a CUSIP but differing in
issuer, class, status, or option marker remain separate conflict evidence.

Private snapshot contract v2 requires only the two SEC security-master cache
files above. During the migration release, restore also accepts contract v1,
verifies every archived byte, extracts only shared SEC lookup cache members,
and discards registry and other unprovenanced private cache members.
A v1 restore must complete a security-master refresh or rebuild before it can
publish a v2 replacement snapshot; new snapshots are never packed as v1.
Clean-build checkpoints live only under dedicated ignored `.cache` work paths.
The maintenance workflows cache those nonpublishable paths after each clean
attempt, keyed by restored dataset, code revision, operating system, and
parser-contract version. This includes a completed bulk index, so a failure in
later SEC-master or derived-output work does not repeat the all-history pass.
After the cache save succeeds, the runner removes the bulk state and all owned
SQLite generations before validation and snapshot packing; unknown neighboring
files are never deleted. Local clean commands retain the same resumable work set
until the caller explicitly invokes `cleanup_13f_bulk_working_set()`. Thus the
fresh-run disk peak contains the restored corpus, one filtered SQLite candidate,
one persistent SEC-master candidate, and only one fund rewrite temporary. It
does not contain a second bulk generation, a second fund corpus, or a snapshot
archive. The hosted preflight reserves 8 GiB for that peak, subtracts the size
of an integrity-checked partial SQLite generation on resume, and preserves a
1 GiB minimum free-space floor.

### SEC EDGAR Rate Limiting

- Maximum **8 requests per second** sustained (SEC caps at 10; 8 leaves headroom)
- **Retry with bounded exponential backoff** (respecting `Retry-After`) on
  connection/timeout failures and HTTP 403, 429, 500, 502, 503, and 504;
  authentication, schema, and other permanent HTTP errors fail immediately
- **User-Agent header required:** must contain a real contact email, format `"YourName your@email.com"`. SEC blocks requests without valid contact info. In production we read this from the `SEC_USER_AGENT` env var, which is populated from a GitHub Actions repo secret — never hard-coded into the workflow file.
- Accept-Encoding: gzip, deflate
- **Concurrency:** a thread-safe rate limiter supports overlapping ingestion
  callers elsewhere in the pipeline while still respecting the 8 req/sec cap.
  The bounded EDGAR exception queue itself is processed sequentially so its
  request fan-out and checkpoints remain deterministic.

### Key SEC EDGAR Endpoints

| Purpose | URL |
|---|---|
| Quarterly filing index | `https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/company.idx` |
| Company submissions | `https://data.sec.gov/submissions/CIK{cik_padded_10}.json` |
| Filing document index | `https://data.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/index.json` |
| Filing document | `https://data.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/{filename}` |
| Company tickers | `https://www.sec.gov/files/company_tickers.json` |
| Company tickers with exchange | `https://www.sec.gov/files/company_tickers_exchange.json` |
| Mutual-fund series/class tickers | `https://www.sec.gov/files/company_tickers_mf.json` |
| Fails-to-deliver archive index | `https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data` |
| Official Section 13(f) list index | `https://www.sec.gov/rules-regulations/staff-guidance/official-list-section-13f-securities` |
| Quarterly Form 13F data sets | `https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets` |

---

## Website (`index.html` and `app.js`)

The HTML policy blocks inline event handlers, objects, base-URL changes, and form
submissions. Full script restrictions are prepared in `cloudflare/csp-worker.mjs`
for a response-header CSP compatible with Cloudflare Bot Fight Mode nonces. The
Worker is optional owner deployment; see `CSP-OWNER-SETUP.md` for staged rollout
and `SECURITY-OWNER-SETUP.md` for required GitHub environment protection.

### Tech Stack

- Static `index.html` with inline CSS, external `app.js`, and the compressed-data loader; no frontend compilation, npm, or framework. Native links/buttons and delegated events avoid inline JavaScript handlers.
- Inline **SVG sparklines and miniature line/bar charts**, plus a CSS concentration donut
- Data loaded via `fetch()` from `data/*.json` files
- Client-side search using `funds-index.json` and the lazily loaded `index.json`

### Features

**Fund Search & Browse:**
- Search bar with autocomplete — searches across all indexed filer names
- Type-ahead results appear as you type
- Click any fund to see their portfolio

**Fund Portfolio View:**
- Stats: 13F Equity Value (not "Portfolio Value"), number of positions, top holding, and top-five concentration
- Note explaining that 13F excludes cash, T-bills, private holdings, and operating businesses
- Miniature charts show four-quarter total-value and position-count histories; a donut shows current top-five concentration
- Holdings table columns in order: rank, security, company, % of portfolio, value, shares, change vs prior, previous % of portfolio, four-quarter trend, and security type
- QoQ changes computed client-side by comparing current quarter to prior quarter in the JSON
- Sparklines computed from the quarterly history in the JSON
- Click any ticker → jump to stock lookup

**Stock Search & Browse:**
- The unified search matches ticker symbols and verified fund-product names; ordinary company descriptions are displayed with results
- Search results focus on common equities, ticker-based funds, and ETNs; options and other security types remain reachable through fund holdings
- Click any ticker to see holders

**Stock Detail View:**
- Stats: current institutional holders, total held value, total exact shares, and largest holder
- Holders table: rank, fund name, value, shares, % of that fund's portfolio, change vs prior, four-quarter trend, and source date
- Separate panels retain latest-filing exits, stale records, and withheld records without including them in current totals
- Click any fund name → jump to that fund's portfolio

**Cross-Linking:**
- Fund view tickers are clickable → stock detail
- Stock view fund names are clickable → fund portfolio
- Unified search remains available on detail pages; the logo and Back button return home, and hash routes support browser history

### Design

The design is implemented in `index.html`. Any future rework must preserve:
- Fund names only — no "Manager" or "Person" column. The production data has
  no source for person names across the complete filer universe.
- 4-quarter sparklines and 4-quarter charts.

Design rules to preserve:
- Analyst's Notebook light theme: `#f4f0e8` background, `#f7f3eb` surface, `#fcfaf5` cards, and `#d6cfc3` borders
- Accent: `#006b4f`, green: `#007342`, red: `#97281f`, gold: `#744620`
- Fonts: Newsreader (headings) and Source Sans 3 (text and tabular numbers), loaded from Google Fonts
- Color-coded QoQ badges: green NEW/increases, red reductions/exits, and muted unchanged or unavailable values
- SVG sparklines: green bars = shares increased, red = decreased

### Data Loading Strategy

```javascript
// site-data-loader.js transparently maps detail fetches to .json.gz.
// On page load, fetch the fund bootstrap and required security metadata.
const funds = await fetch('data/funds-index.json').then(r => r.json());
const labels = await fetch('data/security_labels.json').then(r => r.json());

// Warm the larger ticker search index after the initial view is ready.
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
- Uses a shared `data-maintenance` concurrency group so Update and the overnight
  security-master rebuild cannot mutate snapshot state concurrently.
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

- Performs a daily deterministic SEC security-master refresh at 04:23 UTC and
  complete registry/provenance audit under the same maintenance lock and
  private-snapshot contract.
- A legacy restore reconstructs and verifies every retained immutable holding
  against SEC Form 13F bulk data and exact accession filings before rebuilding
  mappings and verifies the source-neutral cutover difference report. That
  local report is excluded from snapshots and workflow artifact uploads.
  Contract-v2 overnight runs
  reuse the verified corpus and discover only changed SEC security-master
  sources, so the all-history 13F download remains a one-time migration unless
  an operator explicitly selects the workflow's `rebuild_security_master`
  input (or runs the clean CLI rebuild in an equivalent hosted environment).
- Discovers FTD archives by URL and checksum and runs bounded automatic EDGAR
  exception discovery from the checkpointed source state.
- Requires only the declared SEC user agent and fails publication if required
  SEC source inputs cannot be verified or the resulting mappings violate the
  provenance contract.
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
  and Git history, runs the loader and CSP Worker tests, and executes the complete Python suite.
- Publishing workflows repeat the regression gates against their actual code
  and restored data rather than depending on a parallel CI result.
- A tiny, off-main heartbeat branch provides repository activity so GitHub does
  not disable schedules after 60 quiet days. It does not alter `main`, trigger
  deployment, or add meaningful clone weight.

Required repository configuration:

- `SEC_USER_AGENT` and `DATA_ARCHIVE_APP_PRIVATE_KEY` repository secrets.
- `DATA_ARCHIVE_APP_CLIENT_ID` repository variable.
- GitHub App access restricted to the private data repository, with read access
  for restore and a separately minted write token for publication.
- GitHub Pages publishing source set to **GitHub Actions**.

---

## GitHub Pages Configuration

- Publishing source: GitHub Actions, not a branch directory.
- Static entry points: `.nojekyll`, `CNAME`, `index.html`, `app.js`, and
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
   rollback retention, and artifact cleanup to succeed.
5. Reconcile the private manifest and deployment marker, then verify the live
   site through a browser-capable path.

### Quantity evidence independent of security resolution

`quantity_estimation.py` implements a separate derived-quantity policy. Its
six-process scans collect only explicit, positive, SEC-provenanced reported
quantities; estimated and unknown rows never vote in peer pricing. The target
filer is excluded, prices are grouped by exact CUSIP, instrument, quarter, and
unit, and at least three filers must support an agreed median. Frozen receipts
retain the complete screening input digest and a compact exact median witness.
Validation reproduces that witness rather than recalculating a changing median
from the current database.

A previously saved quarter-end price receipt has priority for a verified USD equity listing and
exact quarter-end trading session. It binds the dated SEC identity, source
listing, response checksum, price, volume, and corporate-action conversion.
Option receipts use underlying-share quantities while preserving CALL/PUT
identity. A quote cannot supply a debt principal price or resolve a ticker.

A quantity plan preflights every affected file checksum and holding before any
fund write. Only derived quantity fields may change, and the reported-economic
projection must remain identical. Evidence is saved before references to it;
fund writes are atomic individually, and the operation is safely repeatable.
A process interruption can leave mixed generations that require reapplying the
policy before publication. Private snapshot restoration includes both
quantity caches in its rollback transaction without weakening the required SEC
master/source-state pair. `scripts/quantity_policy.py` provides local receipt migration,
plan, and apply commands; the scheduled pipeline uses saved market
receipts, then peer evidence or an explicit unknown quantity.

`saved_price_migration.py` only upgrades archived storage receipts. It has no
network or import capability. The complete original receipt remains privately
hash-bound. Restore performs the upgrade in staging before installing its
transaction; local maintenance preflights all affected files and retains both
receipt generations until every dependent annotation is rewritten. An
interrupted local migration can be rerun without recomputing any quantity.
