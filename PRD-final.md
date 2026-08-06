# Super Investor Seeker — Product Requirements Document

## Vision

A free, public web app where anyone can search and browse institutional fund holdings from **all** SEC 13F filings. Users can search by fund name or stock ticker across the full universe of ~5,000+ institutional managers that file 13F-HR forms. The app shows portfolio breakdowns, position changes over time, historical trends, and cross-fund ownership analysis.

This is a WhaleWisdom-style product — not limited to a curated list of investors, but covering every 13F filer.

**UI implementation:** The live site lives in `index.html`. It was originally bootstrapped from a `super-investor-seeker.html` prototype (dark theme, Chart.js charts, SVG sparklines, compact data tables), which has since been removed now that `index.html` is the canonical design. Any future UI rework should match what `index.html` currently ships — see the Design section below for the locked-in rules.

---

## Architecture

### Fully Static — No Server, No Database, $0/Month

```
┌─────────────────────────┐       ┌────────────────┐       ┌─────────────────┐
│  GitHub Actions          │ ───>  │  JSON files    │ <──── │  GitHub Pages   │
│  (scheduled pipeline)    │ write │  in /data      │ read  │  (static site)  │
│  Fetches ALL 13F filers  │ +     │  ~5,000 fund   │ via   │  HTML/JS/CSS    │
│  from SEC EDGAR          │ commit│  files + index │ fetch │  loads on demand │
└─────────────────────────┘       └────────────────┘       └─────────────────┘
```

**Why static:** No server to crash, no database to manage, no hosting bills, nothing to maintain. If the pipeline fails, old data stays up. All data is visible as files in the repo.

### How It Works

1. **GitHub Actions** runs a Python pipeline on a weekly schedule (and can be triggered manually)
2. The pipeline downloads SEC EDGAR's quarterly index to discover **all** 13F filers
3. For each filer, it fetches their holdings XML and parses it into structured JSON
4. JSON files are committed back to the repo
5. **GitHub Pages** serves the static HTML/JS site, which loads JSON files on demand via `fetch()`
6. The site has a search index (~500KB) that enables instant client-side search across all ~5,000 fund names

### Key Numbers

| Metric | Value |
|--------|-------|
| Active 13F filers per quarter | ~5,000-6,000 |
| API calls per filer per quarter | ~2-3 (submissions + index + XML) |
| Time to fetch 1 quarter for all filers | ~55-80 minutes |
| Time to backfill 4 quarters | ~3.5-5 hours (fits in a single pipeline run) |
| Weekly update (new filings only) | ~5-15 minutes |
| Total data size | ~60-100MB of JSON |
| GitHub Pages limit | 1GB (we're well under) |
| GitHub Actions free minutes/month | 2,000 (first month uses ~300-360 for backfill, then ~60/month ongoing) |
| Monthly cost | $0 |

---

## Data Pipeline (`pipeline.py`)

### Discovery: Finding All 13F Filers

SEC publishes quarterly index files that list every filing submitted that quarter:

```
https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.idx
```

This single file contains every 13F-HR filing for that quarter — company name, CIK, accession number, and filing date. One download gives us the complete universe of filers. No need to maintain a list manually.

### Data Fetching Strategy

**Initial backfill (first run):**
1. Download the company.idx for the most recent 4 quarters
2. Parse to extract all unique CIKs that filed 13F-HR
3. For each CIK × quarter, fetch the filing index and holdings XML
4. This takes ~3.5-5 hours and fits within GitHub Actions' 6-hour limit in a single run
5. The pipeline tracks what's already been fetched and resumes if interrupted

**Ongoing weekly updates:**
1. Download the company.idx for the current quarter
2. Check which filings are new (not already in the JSON files)
3. Fetch only new filings — typically a few hundred per week during filing season, near zero otherwise
4. Drop any holdings data older than 4 quarters (rolling window)
5. Takes 5-15 minutes

### Resume Capability

The pipeline must track what has been fetched. Strategy:

- Maintain a `data/pipeline_state.json` file listing all accession numbers already processed
- On each run, skip any filing already in the state file
- This means the pipeline can be interrupted and restarted safely

### Rolling 4-Quarter Window

The site only shows 4 quarters of data. On each weekly run, the pipeline:

1. Determines the 4 most recent reporting quarters (e.g. Q1'25, Q2'25, Q3'25, Q4'25)
2. Fetches any new filings for those quarters
3. Removes holdings data from quarters older than the window from each fund JSON file
4. Removes stale accession numbers from pipeline_state.json
5. This keeps the data size stable over time and prevents unbounded growth

### Output Structure

```
data/
  index.json              # Master search index: all fund names + CIKs + all tickers
  pipeline_state.json     # Tracks which filings have been processed
  funds/
    1067983.json          # Berkshire Hathaway — 4 quarters of holdings
    1336528.json          # Pershing Square
    ...                   # ~5,000 fund files
  stocks/
    AAPL.json             # Every fund holding AAPL, with per-quarter history
    AMZN.json
    ...                   # ~5,000-10,000 stock files
```

### Fund JSON Format (`data/funds/{cik}.json`)

```json
{
  "cik": 1067983,
  "name": "BERKSHIRE HATHAWAY INC",
  "quarters": [
    {
      "report_date": "2025-12-31",
      "filing_date": "2026-02-17",
      "total_value": 274160087,
      "num_holdings": 42,
      "holdings": [
        {
          "ticker": "AAPL",
          "issuer": "APPLE INC",
          "cusip": "037833100",
          "class": "COM",
          "value": 61960000,
          "shares": 227920000
        }
      ]
    }
  ]
}
```

The website computes derived fields (% of portfolio, QoQ changes, sparkline data) client-side from this raw data. This keeps the JSON files simple and the pipeline fast.

### Stock JSON Format (`data/stocks/{TICKER}.json`)

```json
{
  "ticker": "AAPL",
  "issuer": "APPLE INC",
  "holders": [
    {
      "cik": 1067983,
      "name": "BERKSHIRE HATHAWAY INC",
      "history": [
        { "date": "2025-12-31", "shares": 227920000, "value": 61960000 },
        { "date": "2025-09-30", "shares": 238210000, "value": 65432000 }
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
  "tickers": ["AAPL", "AMZN", "AXP"],
  "last_updated": "2026-04-07T06:00:00Z",
  "total_filers": 5234,
  "total_tickers": 8912
}
```

This file is ~500KB and loaded once when the site opens. JavaScript searches it instantly on the client side — no server needed.

### CUSIP-to-Ticker Mapping

13F filings report holdings by CUSIP, not ticker. The pipeline needs to map CUSIPs to tickers. Approach:

1. Build a CUSIP → ticker lookup from the 13F data itself — many filers list recognizable issuer names (e.g. "APPLE INC") alongside CUSIPs, and the same CUSIP appears across thousands of filings. Cross-referencing issuer names against `https://www.sec.gov/files/company_tickers.json` (which maps company names to tickers) enables matching.
2. Store the mapping in `data/cusip_map.json` and grow it over time as more filings are processed
3. Holdings with unmapped CUSIPs keep the CUSIP as an identifier — they're still searchable and displayable

### SEC EDGAR Rate Limiting

- Maximum **8 requests per second** sustained (SEC caps at 10; 8 leaves headroom)
- **Retry with exponential backoff** (2s, 4s, 8s, 16s, max 60s) on 403, 429, 503
- **User-Agent header required:** must contain a real contact email, format `"YourName your@email.com"`. SEC blocks requests without valid contact info. In production we read this from the `SEC_USER_AGENT` env var, which is populated from a GitHub Actions repo secret — never hard-coded into the workflow file.
- Accept-Encoding: gzip, deflate
- **Concurrency:** a thread-safe rate limiter lets a small pool of workers (4) issue overlapping requests while still respecting the 8 req/sec cap. This absorbs network round-trip latency — without workers the latency dominates and we can't hit the rate cap in practice.

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

- Single `index.html` file with inline CSS and JavaScript — no build step, no npm, no framework
- **Chart.js** from CDN for stacked bar charts
- **SVG sparklines** rendered inline (see prototype)
- Data loaded via `fetch()` from `data/*.json` files
- Client-side search against the index.json

### Features

**Fund Search & Browse:**
- Search bar with autocomplete — searches across all ~5,000 fund names
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
- Fund names only — no "Manager" or "Person" column. The production data has no source for person names across ~5,000 filers.
- 4-quarter sparklines and 4-quarter charts (not 8, as the prototype used).

Design rules to preserve:
- Dark theme: `#06080d` background, `#0d1017` surface, `#111620` cards, `#1c2333` borders
- Accent: `#4e8cff`, Green: `#34d399`, Red: `#f87171`, Gold: `#fbbf24`
- Fonts: JetBrains Mono (data), Source Sans 3 (text) — both from Google Fonts CDN
- Color-coded QoQ badges: gold NEW, green ↑X%, red ↓X%, gray —
- SVG sparklines: green bars = shares increased, red = decreased

### Data Loading Strategy

```javascript
// On page load — load the search index (one file, ~500KB)
const index = await fetch('data/index.json').then(r => r.json());
// Now search works instantly across all ~5,000 fund names

// When user clicks a fund — load that fund's data (one file, ~5-50KB)
const fund = await fetch(`data/funds/${cik}.json`).then(r => r.json());

// When user clicks a stock — load that stock's data (one file, ~2-20KB)
const stock = await fetch(`data/stocks/${ticker}.json`).then(r => r.json());
```

This means the initial page load is fast (~500KB), and individual fund/stock data loads on demand. No need to download everything upfront.

---

## GitHub Actions Workflow

### `.github/workflows/update-data.yml`

```yaml
name: Update 13F Data

on:
  schedule:
    - cron: '0 6 * * 6'      # Every Saturday at 6am UTC (2am ET)
  workflow_dispatch:            # Manual trigger button on GitHub

permissions:
  contents: write               # so the job can push data/ commits back

jobs:
  update:
    runs-on: ubuntu-latest
    timeout-minutes: 340        # outer safety; pipeline step has its own 300
    steps:
      - uses: actions/checkout@v5
        with:
          fetch-depth: 0        # full history for clean diff / commit

      - uses: actions/setup-python@v6
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Verify SEC_USER_AGENT secret is configured
        env:
          SEC_USER_AGENT: ${{ secrets.SEC_USER_AGENT }}
        run: |
          if [ -z "$SEC_USER_AGENT" ]; then
            echo "::error::SEC_USER_AGENT secret is not set."
            exit 1
          fi

      - name: Run pipeline
        timeout-minutes: 300    # 5h — leaves 40min for the commit step
        env:
          SEC_USER_AGENT: ${{ secrets.SEC_USER_AGENT }}
        run: python pipeline.py --all

      - name: Commit and push data
        if: always()            # run even on pipeline timeout / failure
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/
          git diff --staged --quiet || git commit -m "Update 13F data $(date -u +%Y-%m-%d)"
          git push
```

Key details:
- `workflow_dispatch` lets you click "Run workflow" on GitHub to trigger manually
- The pipeline has resume capability, so if it times out at 5.5 hours, the next run picks up where it left off
- The `git diff --staged --quiet ||` only commits if data changed
- Committing keeps the repo active (prevents GitHub from auto-disabling the schedule after 60 days of inactivity)
- After the initial backfill, weekly runs take 5-15 minutes

---

## GitHub Pages Configuration

- Serve from the `main` branch, root directory (`/`)
- Add a `.nojekyll` file to the repo root (prevents Jekyll processing)
- The site URL will be `https://USERNAME.github.io/super-investor-seeker`
- Custom domain can be added later via repo Settings → Pages → Custom Domains

---

## Implementation Order

### Phase 1: Data Pipeline

Build `pipeline.py` that:
1. Downloads SEC quarterly index files to discover all 13F filers
2. Fetches and parses 13F-HR XML holdings for each filer
3. Maps CUSIPs to tickers
4. Outputs fund JSON, stock JSON, and index.json into `data/`
5. Tracks state in `pipeline_state.json` for resume capability
6. Has proper rate limiting and retry logic
7. Accepts `--all` (all filers), `--cik XXXXXXX` (single filer), `--quarters N` (limit quarters)

**Test:** Run `python3 pipeline.py --cik 1067983 --quarters 2` and verify `data/funds/1067983.json` has real Berkshire data for 2 quarters.

**Then:** Run `python3 pipeline.py --all --quarters 1` to fetch the most recent quarter for all ~5,000 filers. This verifies the full pipeline works end-to-end before deploying.

### Phase 2: Static Website

Build `index.html` that:
1. Loads `data/index.json` on startup for search
2. Renders fund lookup, fund portfolio, stock lookup, stock detail views
3. Matches the prototype design exactly
4. Computes QoQ changes and sparklines client-side from the quarterly data

**Test:** Serve locally with `python3 -m http.server 8000` and browse the site.

### Phase 3: GitHub Actions + Pages

1. Create `.github/workflows/update-data.yml`
2. Create `.nojekyll`
3. Push to GitHub, enable Pages, enable Actions write permissions
4. Trigger manual run to backfill data
5. Verify the live site works

---

## Prompting Tips for Claude Code

### 1. Build one phase at a time
"Build Phase 1 — the data pipeline only. See the PRD for architecture."
Then test it. Then: "Build Phase 2 — the website."

### 2. No server. No database. No framework.
"The website is a single index.html file. It loads JSON via fetch(). No Python backend, no React, no npm, no build step."

### 3. The pipeline discovers filers from SEC index files
"Do NOT hardcode a list of filers. The pipeline downloads SEC's quarterly company.idx file to discover ALL 13F filers automatically."

### 4. Resume capability is critical
"The pipeline must track processed filings in pipeline_state.json. If interrupted, the next run picks up where it left off."

### 5. Reference the shipped design
"Match the design in `index.html` — same dark theme, same tables, same Chart.js charts, same sparklines. (The original `super-investor-seeker.html` prototype was removed once `index.html` became canonical.)"
