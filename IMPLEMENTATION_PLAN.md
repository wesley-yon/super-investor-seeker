# Insider Activity Implementation Plan

## Scope and discovery basis

This document originated as the Phase 0 repository-discovery deliverable. The plan remains the design record; factual stack and verification notes below have been updated after the authorized Phase 1–3 implementation so they do not misdescribe the current checkout. The illustrative appendices remain non-authoritative.

## Implementation status

Phases 1–3 are **completed as-built history** in this checkout: Phase 1 delivered the local-only, default-off illustrative fixture UI and rendered-browser coverage; Phase 2 delivered the validated Section 16 contract/parser foundations; and Phase 3 delivered ingestion, backfill, snapshot, and workflow integration. Phase 1 is local-only and default-off. Reporting Insiders is implemented fixture scope. The detailed Phase 1–3 sections below record that delivered scope rather than authorize new work. Phases 4–5 remain **future work**; they are not implied by the fixture preview or current private-pipeline implementation.

Discovery was intentionally bounded to the required package documents and images plus targeted checks of the actual application, data pipeline, identity helpers, tests, and workflows. `13f-insider-activity-prd/CODEX_START_HERE.md` was the first deliberate repository-file read. The package README and `13f-insider-activity-prd/13F_INSIDER_ACTIVITY_PRD.md` were then read in full, and both `13f-insider-activity-prd/reference/current-holder-page.png` and `13f-insider-activity-prd/reference/insider-activity-mockup.png` were inspected at original resolution. `/tmp/codex-phase0-audit-hints.md` was treated as untrusted; the findings below are based on spot-checks of the named repository files.

The current checkout observed during discovery is branch `docs/insider-activity-phase-0` at commit `491b05b6efc7a5890370d4891314ed0da4679491`.

Critical fixture disclaimer: **every APGE insider name, transaction, value, and date in both `13f-insider-activity-prd/fixtures/apge-insider-activity.example.json` and `13f-insider-activity-prd/reference/insider-activity-mockup.png` is illustrative UI data, not real APGE filing evidence or production truth.** The package README and PRD explicitly require production values to come from normalized SEC filings.

## Existing stack and versions

| Concern | Repository reality | Exact evidence |
|---|---|---|
| Frontend framework | None. The application remains one static HTML document with inline CSS and vanilla JavaScript; there is no frontend compilation. `package.json` exists only for the pinned Playwright test harness. | `index.html`; `package.json`; `ARCHITECTURE.md` Website section |
| Language/runtime | Browser JavaScript, Python 3.11+, and Node 22 for tests. The Python minimum is machine-readable and CI pins both runtimes. | `pyproject.toml`; `.github/workflows/test.yml` |
| Python dependencies | Runtime ranges are `requests>=2.31.0,<3` and `lxml>=6.1.0,<7`; development adds `ruff==0.16.3`. Automation installs hash-locked transitive sets. | `pyproject.toml`; `requirements*.txt`; `requirements*.lock` |
| Package manager | `pip --require-hashes` with checked-in Python lockfiles; npm uses the checked-in lockfile only for the Playwright test harness. | `README.md`; `requirements*.lock`; `package-lock.json` |
| Router | Hand-written hash routing. Supported routes are `#fund/<CIK>` and `#stock/<stockId>`; the local fixture preview additionally uses closed `insiders` and `reporting-insiders` stock subviews. Delegated stock actions canonicalize an ID and resolve it against the loaded search index before routing. `history.pushState` plus `hashchange`/`popstate` is used. | `index.html`: `wireUrlRouting`, `routeFromHash`, `setUrl`, `resolveDelegatedStockId` |
| Backend/API framework | None. GitHub Pages serves static files; browser data access is `fetch`. There is no application server or runtime API. | `ARCHITECTURE.md`; `site-data-loader.js`; `scripts/build_pages_artifact.py` |
| Database/ORM | None. There is no PostgreSQL, SQLite, ORM, migration system, or schema registry. Durable state is JSON in ignored `data/` and `.cache/`, carried in authenticated private snapshots. | `ARCHITECTURE.md`; `pipeline.py`; `scripts/data_snapshot.py`; `.gitignore` |
| Styling | Inline CSS in `index.html`; no component library or separate design-token file. | `index.html` |
| Charting | No chart package is loaded at runtime. Visualizations are local HTML/SVG/CSS functions (`spark`, `miniLine`, `miniBars`, `miniDonut`); the architecture document now states that explicitly. | `index.html`; `ARCHITECTURE.md` |
| Tests | Python `unittest`, Node's built-in `node:test`, pinned Playwright, Ruff 0.16.3, and Python compilation. | `.github/workflows/test.yml`; `package.json`; `tests/` |
| Visual/accessibility testing | Pinned Playwright exercises rendered routes, five viewports, keyboard behavior, focus, responsive layout, and deterministic screenshots. Static semantic tests remain as a second layer. | `playwright.config.js`; `tests/visual/`; `tests/test_insider_phase1.py` |
| Deployment | A deterministic, allowlisted GitHub Pages artifact built by Python and deployed by GitHub Actions. | `scripts/build_pages_artifact.py`; `.github/workflows/deploy-pages.yml` |

The illustrative `13f-insider-activity-prd/appendices/database-schema.sql` is therefore a logical reference, not an installed PostgreSQL schema. Likewise, `13f-insider-activity-prd/appendices/insider-activity-types.ts` is a contract reference, not evidence of TypeScript or a TypeScript build. Their concepts must be represented in validated Python/JSON and vanilla JavaScript.

## Routing, page components, and styles

### Current routes and implemented route adaptation

`index.html` parses `#fund/<id>` and `#stock/<id>`, with the local fixture preview also accepting the closed `insiders` and `reporting-insiders` stock subviews. Stock IDs are canonical public security identities, not necessarily ticker strings. Delegated controls canonicalize their `data-stock-id` and require a resolved entry from the loaded search index before they can invoke stock loading. `stockFilePath` maps an already-resolved equity ID to `data/stocks/<safe-id>.json` and a non-equity ID to `data/stocks/<safe-id>__<INSTRUMENT_TYPE>.json`.

The implementation preserves that route and uses view suffixes rather than inventing `/security/:ticker` paths:

- Institutional holders: `#stock/<encoded-stock-id>` (existing canonical URL)
- Insider activity: `#stock/<encoded-stock-id>/insiders`
- Reporting insiders: `#stock/<encoded-stock-id>/reporting-insiders`

The fixture filter state uses the real query string because `setUrl` preserves `location.search`, for example `?range=1y&transactionScope=ps&ownerScope=all#stock/03770N101/insiders`. The route retains the stock ID as the stable security key, and a route-backed fixture filing drawer uses an `accession` query parameter. The exact hash grammar is covered by semantic and rendered tests.

### Existing security page and reusable implementation

There are no separate component files. The relevant implementation is co-located in `index.html`:

- Global shell and search: `.hdr`, `.logo`, `.gsearch-wrap`, `wireGlobalSearch`, and the global back button.
- Stock loading/routing: `stockFilePath`, `loadStock`, `renderStock`, `routeFromHash`, `wireUrlRouting`, and `setUrl`.
- Security identity header: the `.stock-title`, `.ticker-mark`, `.stock-title-line`, `.stock-meta`, and `.ht-tag` markup generated by `renderStock`.
- Cards: `.stock-stat-grid`, `.stock-stat`, `.stock-stat-label`, `.stock-stat-value`, `.stock-largest`, and the local `miniLine`/`miniBars`/icon helpers.
- Tables and controls: `.fund-panel`, `.stock-panel`, `.fund-panel-head`, `.holdings-tools`, `.holdings-search`, `.tbl-wrap`, `.stock-table-footer`, `.page-btn`, `sortableHeader`, `renderStockTbody`, and stock pagination/sort functions.
- Summary rail: `.stock-sidebar`, `.side-panel`, `.side-title`, `.stock-summary-head`, `.stock-summary-row`, `.summary-action`, and `.summary-line`, with local `summaryRows`/`summarySection` closures in `renderStock`.
- Small trends: `spark`, `miniLine`, `miniBars`, and `miniDonut`; these establish the existing hand-rendered visualization approach but are not sufficient alone for the required daily price/event chart.
- Responsive rules: existing `@media` rules at 1350, 1200, 1100, 768, and 520 px, plus `prefers-reduced-motion` handling.

The effective production design tokens are the later light-theme `:root` block in `index.html`: `--bg:#f4f0e8`, `--sf:#f7f3eb`, `--cd:#fcfaf5`, `--cd2:#f9f6ef`, `--bd:#d6cfc3`, `--bd2:#bcb3a5`, `--tx:#201f1b`, `--mt:#5f5b53`, `--ac:#006b4f`, `--gn:#007342`, `--rd:#97281f`, `--gd:#744620`, `--ac-soft:#e1e8e2`, `--gn-soft:#e1eee5`, `--rd-soft:#f3e2de`, and `--warm-hover:#eee9df`. Typography is `--serif:'Newsreader', ... Georgia, serif` and `--sans:'Source Sans 3', ... sans-serif`; both are loaded from Google Fonts in `index.html`. The earlier dark tokens and the dark-theme wording in `ARCHITECTURE.md` are superseded by the later CSS and conflict with both supplied images. Implementation should use the effective light tokens and update architecture prose only in a later authorized phase.

Visual interpretation of the supplied assets:

- `current-holder-page.png` closely matches the actual light CSS and `renderStock` structure: editorial identity header, thin taupe borders, four summary cells, dense holder table, and one continuous narrow ownership-summary rail.
- `insider-activity-mockup.png` preserves that shell but adds a three-tab row, methodology banner, four insider KPI cells, a quiet line chart with shaped event markers, filter bar, transaction table, footer freshness, and insider-ranked rail.
- The mockup is 1621 x 970 and the PRD also requires 1440 x 900 plus responsive baselines. Production components and real font metrics take precedence where they conflict with the mockup.

## Persistence, data contracts, and identifiers

### Persistence reality

`pipeline.py` writes JSON atomically through `_atomic_write_json` using a sibling temporary file, `fsync`, and `os.replace`. Durable generated state lives under ignored paths including `data/pipeline_state.json`, `data/funds/`, `data/stocks/`, and operational `.cache/` registries. `scripts/data_snapshot.py` transactionally packs, verifies, restores, and replaces the private snapshot; `README.md` states the latest two validated snapshots are retained for rollback. `scripts/build_pages_artifact.py` publishes only an explicit static allowlist: `.nojekyll`, `CNAME`, `index.html`, `site-data-loader.js`, three browser indexes, and compressed `data/funds/` and `data/stocks/` payloads.

There is no row-query database or cursor-capable server API. The insider logical model must initially become immutable, normalized, versioned JSON records in private snapshot storage, plus bounded per-security public projections. Canonical metrics should be calculated in Python with `decimal.Decimal` and serialized as exact decimal strings; browser code should format them and must not become the financial source of truth. `null` must remain unknown throughout Python, JSON, and UI.

The existing `DATA_CONTRACT_VERSION` is `5` in `data_contract.py`. New public insider payloads require a deliberate contract-version change and matching validator/frontend handshake, not an ad hoc unversioned shape.

### Existing identifiers

- Fund identity is SEC filer CIK (`data/funds/<cik>.json`, `#fund/<cik>`).
- Public security identity is normalized security identifier plus instrument type. `security_identity.py` sets `SECURITY_IDENTITY_VERSION = 1`; `stock_lookup_id`, `parse_stock_lookup_id`, `stock_file_stem`, and `stock_filename` preserve CALL/PUT/NOTE separation. Equity filenames omit the type suffix; non-equities append `__<TYPE>`.
- Current stock JSON carries `stock_id`, `cusip`, `ticker`, `issuer`, `instrument_type`, and holders. `data/index.json` carries the same search identity plus freshness/count metadata.
- CUSIP/security ID is canonical for the present 13F product; ticker and issuer are descriptive and may be absent or remapped. The insider model additionally needs issuer CIK as the durable Section 16 issuer key and must map the as-filed security class to the existing stock ID without merging distinct classes.
- Filing accession number is already the idempotency key for 13F pipeline state and should be the immutable public identifier for insider filing detail. Reporting-owner CIK and a sorted-CIK owner-group hash should be added for insider ownership identity and joint-filer counting.

As built in Phase 2, the issuer-CIK-to-stock-ID mapping is validated with provenance; ticker is not used as the durable Section 16 join.

## Price-data integration reality

There is no external daily market-price provider, OHLC/split schema, or daily price public payload. `pipeline.py:load_peer_value_unit_prices` derives same-security implied price observations (`value / shares`) from generated 13F history only to help validate historical 13F value units. That is not a market-price integration and cannot satisfy the PRD chart.

The implementation must therefore pause before selecting a provider. The required provider must support reproducible daily unadjusted close or adjusted close plus split metadata, historical ticker/security mapping, currency, source timestamp, caching, rate limits, licensing compatible with a publicly enumerable static site, and deterministic server-side ingestion. Price fetching belongs in Python/GitHub Actions, never browser-to-provider calls. If no acceptable provider is approved, the PRD-sanctioned transaction-only timeline and explicit partial-price state should ship rather than fabricated or basis-mismatched prices.

## SEC ingestion and background workflow reality

`pipeline.py` is a 13F-specific batch application, not a general job framework. It discovers quarterly 13F-HR/13F-HR/A filings, fetches SEC submission/index/primary documents, parses with `lxml`, reduces amendment chains, regenerates JSON, and checkpoints accessions/quarantine state. Its process-global `RateLimitedSession` uses `requests.Session`, `SEC_USER_AGENT`, gzip/deflate, a shared thread lock, 8 requests/second spacing, six attempts, exponential backoff capped at 60 seconds, and a 30-second timeout.

`scripts/refresh_recent_13f_filings.py` supplements quarterly indexes with the SEC current-filings Atom feed, groups pending filings by CIK, replays them, and checkpoints interruptions. `.github/workflows/update-data.yml` runs on weekday filing-window cron (`23 11-23 * * 1-5`) and manual dispatch, requires a secret `SEC_USER_AGENT`, restores a private snapshot, runs the main/recent pipelines and tests, then publishes the validated snapshot. `.github/workflows/deploy-pages.yml` restores that exact snapshot, validates it, builds/audits the bounded Pages artifact, and deploys through `actions/upload-pages-artifact@v5` and `actions/deploy-pages@v5`.

Reusable conventions are the shared SEC session/rate limiter, accession checkpointing/quarantine, atomic JSON writes, immutable snapshot publication, exact artifact audit, and existing workflow gates. Missing infrastructure includes ownership-form discovery/parsing, quarterly insider dataset backfill, immutable raw ownership XML retention, parser-version storage/reparse tooling, insider amendment matching, owner/security normalization, and insider-specific telemetry. The current 13F parser does not preserve raw filings indefinitely; insider work must add that private immutable source store rather than claiming the existing pipeline already has one.

## Tests and visual QA

Current CI in `.github/workflows/test.yml` uses Python 3.11 and Node 22, installs `requirements-dev.lock` with `pip --require-hashes`, runs `ruff check .`, compiles Python, runs the Node loader suite, installs npm dependencies with `npm ci --ignore-scripts`, runs the pinned Playwright suite, and runs `python -m unittest discover -s tests -v`. Relevant patterns include:

- `tests/test_frontend_semantics.py`: frontend semantic/data behavior checks against `index.html` using Python and Node subprocesses.
- `tests/test_site_data_loader.mjs`: same-origin `.json.gz` loader tests with `node:test`.
- `tests/test_data_contract.py`, `tests/test_pages_artifact.py`, `tests/test_workflow_resilience.py`, and `tests/test_data_snapshot.py`: public contract, allowlist, workflow, privacy, and snapshot invariants.
- Existing frozen SEC 13F fixtures under `tests/fixtures/sec_filing_oracle/` demonstrate the fixture/oracle convention, but they are not Section 16 fixtures.

The Phase 1 implementation established a pinned Playwright browser harness and deterministic coverage at 1621x970, 1440x900, 1024x768, 768x1024, and 390x844. The immutable source mockup and implementation screenshot are compared through an explicit accepted-difference manifest rather than an uncited visual claim.

## As-built history and future plan mapped to PRD Phases 1-5

The mapping below follows the five phases in `CODEX_START_HERE.md`. The longer PRD later separates frontend integration and hardening into additional phases; here those tasks are folded into Phase 5 as the user requested. Phases 1–3 are recorded as-built; only Phase 4 onward describes future proposals subject to phase review.

### Phase 1 - completed as-built history: UI against the illustrative fixture

- `index.html` extended the existing hash router and added the security navigation, default-off local fixture gate, methodology banner, KPI row, chart/timeline renderer, filters, transaction table, filing drawer, right rail, responsive/loading/empty/error states, focus management, and search placeholder. The implementation reused the existing header, identity, panel, table, token, typography, and responsive conventions through focused vanilla-JS render/helpers rather than introducing React or TypeScript.
- `13f-insider-activity-prd/fixtures/apge-insider-activity.example.json` remains a read-only development and QA input. Its claims are never treated as APGE evidence, and the Pages artifact allowlist excludes the fixture package.
- `tests/test_frontend_semantics.py` and `tests/test_insider_phase1.py` cover routes, navigation semantics, URL filters, null rendering, drawer focus, source-link separation, the P/S default, and the production-host fixture boundary.
- `tests/test_site_data_loader.mjs` retained the existing compressed-data path contract because Phase 1 introduced no public insider payload family.
- The pinned `tests/visual/` Playwright suite captures the five PRD viewport sizes and records the accepted source-mockup differences in a deterministic comparison manifest.
- `scripts/build_pages_artifact.py`, `tests/test_pages_artifact.py`, and `tests/test_workflow_resilience.py` preserve the narrow positive allowlist and prove that the local fixture/private insider tree cannot enter the public artifact.

Phase 1 stop: the feature remains off by default, the existing holders view is unchanged, the static insider view matches the reference at desktop sizes, responsive and accessible states work, and all displayed APGE content is visibly/structurally fixture-only.

### Phase 2 - completed as-built history: JSON data model and ownership parser

- `insider_contract.py`: implements the versioned Python/JSON contract, enums, exact-decimal serialization, tri-state fields, data-quality metadata, and validation entry points. It replaces the illustrative TypeScript contract with a repository-native contract.
- `insider_parser.py`: securely parses Forms 3, 4, 5 and amendments with installed `lxml`; disables entity/network expansion; and retains owners, non-derivative and derivative rows, holdings, signatures, remarks, all raw codes/ownership fields, footnotes and field links, schema version, source URLs, accession, accepted timestamps, Rule 10b5-1 tri-state, and parser version.
- `security_identity.py` added narrowly tested Section 16 security-class mapping hooks and issuer-CIK associations while preserving `SECURITY_IDENTITY_VERSION = 1` behavior for existing 13F identities.
- `data_contract.py` remains at contract version 5 because Phases 1–3 add no live public insider payload shape.
- Private snapshot storage uses `data/insiders/private/accessions/<accession>/...` for each immutable accession's source and derived records and `data/insiders/private/state/...` for pipeline state. Raw accessions are immutable; reparses create parser-versioned derived records rather than overwriting source evidence.
- `scripts/data_snapshot.py` includes, hashes, validates, restores, and protects the private insider state/raw directories while Git and the public artifact continue to exclude them.
- `tests/fixtures/insider_filings/` contains frozen, documented SEC XML fixtures with source provenance and no live-network dependency.
- `tests/test_insider_contract.py`, `tests/test_insider_parser.py`, `tests/test_insider_amendments.py`, and `tests/test_insider_security_identity.py` cover exact decimals, nulls, unknown codes/elements, footnote links, joint filers, raw preservation, amendments, and class separation.

Phase 2 stop: representative 3/4/5 and amendment fixtures parse deterministically and idempotently, raw source is immutable, decimal precision and null semantics are proven, and every normalized value remains traceable to accession/source row/footnotes.

### Phase 3 - completed as-built history: ingestion and background automation

- `insider_pipeline.py`: reuses `pipeline.py` HTTP/session, retry, logging, atomic-write, quarantine, and checkpoint concepts without mixing ownership-form semantics into 13F reducers. It adds bulk quarterly dataset backfill, raw XML reconciliation, idempotent accession processing, amendment resolution, owner-group keys, parser-version reprocessing, immutable cache lookup, and issuer aggregate invalidation.
- `scripts/refresh_recent_insider_filings.py` discovers incremental Forms 3/4/5 and amendments from approved SEC feeds, fetches only server-side, and shares the declared User-Agent and one process-wide SEC limiter.
- `scripts/backfill_insider_transactions.py` performs bounded, resumable quarterly SEC Insider Transactions imports with source-quarter metadata and raw-XML reconciliation.
- `pipeline.py` exposes the shared SEC client/rate-limiter and hardened atomic writer while preserving existing 13F behavior and tests; no independent per-worker limiter was introduced.
- `.github/workflows/update-data.yml` contains bounded insider incremental/backfill/reparse steps, cooperative deadlines, snapshot restore/checkpoint/publication, telemetry, and fail-safe behavior that leaves last-known public data visible while retaining `SEC_USER_AGENT` validation.
- `scripts/data_snapshot.py` and `scripts/publish_private_snapshot.sh` include the new durable inputs in the authenticated, content-addressed snapshot lifecycle without weakening released publication reconciliation and serialization.
- `tests/test_insider_ingestion.py`, `tests/test_insider_backfill.py`, and `tests/test_workflow_resilience.py` cover retry/quarantine, repeated-accession idempotency, original/amendment sequencing, interruption recovery, raw-cache reuse, global throttling, and workflow ordering.

Phase 3 stop: a test issuer backfills and incrementally updates without duplicates; raw XML is never fetched in browser code or overwritten; retries, rate limiting, checkpoints, and reparse operations are observable and recoverable.

### Phase 4 - future: static page payloads and canonical metrics

- Proposed `insider_metrics.py`: compute P/S-only summary, plan-known denominators, net/ratio states, latest meaningful event, percent-change eligibility, owner rankings, latest-reported holdings, display groups, amendment exclusions, and data-quality counts with `Decimal`; count joint-filed rows once at company level.
- Proposed `insider_publication.py` or a focused generation section in `insider_pipeline.py`: emit a bounded per-security page payload and per-accession detail payload from the same normalized rows. Likely public paths are `data/insiders/public/securities/<stock-file-stem>.json` and `data/insiders/public/filings/<accession>.json.gz`; they remain distinct from the implemented private tree.
- `validate_data.py`: validate insider public/private referential integrity, safe SEC URLs, decimal strings, tri-state fields, current amendment version, owner-group/display-group uniqueness, price basis, freshness, and reconciliation across summary/chart/table/sidebar.
- `site-data-loader.js`: extend its same-origin path matcher only for approved public insider payload paths; retain compressed-first and local raw fallback behavior.
- `scripts/build_pages_artifact.py`: explicitly allow and deterministically compress only the minimum public insider projections. Never publish raw XML, addresses, parser errors, private state, or bulk archives.
- `tests/test_data_contract.py`, `tests/test_pages_artifact.py`, `tests/test_public_data_status.py`, and proposed `tests/test_insider_metrics.py`/`tests/test_insider_publication.py`: enforce exact values, filters, stable cursors or static pagination semantics, reconciliation, privacy, and bounded artifact behavior.

There can be no live `/api/...` endpoint in the current architecture. The PRD API response becomes a versioned static per-security JSON contract. Filing detail becomes an on-demand static compressed payload. Filtering/pagination can be client-side for a bounded issuer payload; if payload measurements prove that unsafe, generation should create deterministic query/page shards. Introducing an API server/database would be an architectural project requiring separate authorization, not an incidental Phase 4 choice.

Phase 4 stop: all UI regions reconcile to one normalized source set, values are exact strings, missing/unknown states are explicit, public output is bounded and privacy-audited, and the static substitute for API/cursor behavior meets measured payload limits.

### Phase 5 - future: live integration and hardening

- `index.html`: replace the fixture adapter with static live payload loading while preserving feature flag, skeletons, partial/error states, URL state, drawer deep links, accessible interactions, existing holder route, and the compact 90-day institutional-page cross-link.
- Proposed server-side price module (name chosen after provider approval), insider publication code, and public payloads: ingest/cache daily prices and split metadata, align transaction dates/bases, and expose price freshness. Fall back to transaction-only timelines when coverage is absent.
- `scripts/build_pages_artifact.py`, `.github/workflows/deploy-pages.yml`, and `.github/workflows/update-data.yml`: run generation, validation, visual/accessibility checks, exact snapshot publication, artifact audits, and deploy gates without widening public exposure.
- Existing test files plus the proposed insider parser/metrics/ingestion/frontend/visual suites: cover several issuers, amendments, joint owners, null prices, derivatives, indirect ownership, split basis, plan-status nulls, keyboard/drawer behavior, performance, and all five viewports.
- `README.md` and `ARCHITECTURE.md`: maintain the implemented backfill, incremental sync, reparse/recovery, runtime/dependency, JSON-contract, and visual-test tooling documentation as later phases add price/timeline behavior.

Phase 5 stop: live normalized SEC data replaces fixture data, the price/timeline state is honest and basis-correct, existing holders remain unregressed, accessibility/performance/visual checks pass, and operations/recovery are documented.

## Possible dependencies, with reasons

No dependency should be added until its phase is approved and a repository-native alternative has been evaluated.

1. **No parser dependency is presently needed.** Installed `lxml>=6.1.0,<7` can parse XML securely if configured to disable external entities/network access; `requests>=2.31.0,<3` and the existing rate-limited session cover HTTP. Python `decimal.Decimal`, `hashlib`, `json`, `gzip`, and date libraries cover exact values, keys, serialization, and archives.
2. **Prefer a native SVG chart first.** The current site already renders SVG/CSS mini charts and has no installed chart library. A vanilla SVG price line plus shaped transaction markers avoids creating an npm toolchain or adding a large CDN dependency. Add a chart library only if keyboard event selection, responsive tooltips, and performance cannot be met; any candidate needs pinned version, local/allowlisted delivery, bundle/license review, and custom marker support.
3. **Rendered visual/accessibility harness is installed for tests only.** Playwright is pinned in `package-lock.json` and CI uses Node 22 plus `npm ci --ignore-scripts`; it does not enter the public artifact or application runtime. The suite covers deterministic screenshots, focus, keyboard behavior, and responsive rendering.
4. **Daily market-price provider/library is unresolved.** This is primarily a data-source/licensing decision, not necessarily a Python package. Prefer direct server-side HTTP through existing `requests` and a documented provider contract. Do not introduce a second runtime service or browser API key.
5. **No database/ORM/TypeScript dependency.** JSON snapshot/publication is the installed persistence architecture. PostgreSQL, an ORM, or TypeScript would be an unauthorized architecture replacement for this scope.

## Open assumptions, conflicts, and decisions required

The remaining decisions are Phase 4+ work; resolved Phase 1–3 choices are retained above as as-built history.

1. The current search index has no insider owner records. A future owner-search feature requires an approved bounded public owner index and privacy/size review.
2. There is no daily price provider or split/currency contract. Select and approve one, or explicitly ship transaction-only timelines with partial-data messaging.
3. The PRD asks for cursor pagination, but a static page has no server cursor. Measure per-issuer insider payload sizes and choose client pagination or deterministic generated shards.
4. Current public stock values are JavaScript `Number`s and some derived 13F calculations use floats, but insider canonical financial values must be Python `Decimal` strings. The new feature should not silently broaden into rewriting existing 13F arithmetic.
5. Market capitalization, sector, and shares-outstanding denominators shown in the mockup do not exist in the inspected current stock contract. Omit them unless a sourced, dated integration is approved; never infer or use mockup values.

## Historical Phase 0 exit

Repository discovery established the original plan without replacing the stack. Phase 1 was subsequently completed as a default-off vanilla-JS illustrative fixture UI grounded in the effective `index.html` design system and protected by static artifact/privacy constraints; Phases 2 and 3 then completed the contract/parser and ingestion foundations. Future work begins with separately authorized Phase 4 proposals.
