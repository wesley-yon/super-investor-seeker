# Codex Start Here — Insider Activity Feature

You are implementing the Insider Activity feature described in `13F_INSIDER_ACTIVITY_PRD.md` inside the existing 13F Super Investor Seeker repository.

## Non-negotiable rules

1. Inspect the repository before editing.
2. Preserve the existing framework, router, database, ORM, design system, search shell, price provider, test framework, and deployment workflow.
3. Reuse current security-page components and CSS tokens.
4. Do not rebuild the application shell.
5. Do not call SEC endpoints from browser code.
6. Do not use JavaScript floating-point arithmetic as the canonical representation of financial values.
7. Treat `null` as unknown, never as zero or false.
8. Count joint-filed transaction rows once at the company level.
9. Keep all raw filings immutable and version amendments rather than overwriting originals.
10. Use only P- and S-coded transactions in default purchase/sale metrics.
11. Preserve every raw transaction code, filing accession, source URL, footnote, and ownership field.
12. Keep the feature behind a flag until parser, data, and visual QA pass.

## First task: repository discovery

Before implementing the feature, create `IMPLEMENTATION_PLAN.md` in the repository with:

- Framework and version.
- Router and relevant routes.
- Existing security page component paths.
- Existing design-token and shared-card/table component paths.
- Database and ORM.
- Existing issuer/security identifiers.
- Existing price-data integration.
- Existing SEC ingestion or background-job infrastructure.
- Chart library, if any.
- Test and visual-regression tools.
- Proposed file-by-file implementation plan mapped to PRD phases.
- Dependencies that may be needed and why.
- Open assumptions or conflicts.

Do not introduce new dependencies until this audit is complete.

## Implementation order

### Phase 1 — UI against fixture

- Add the new tabs.
- Build the complete page using `fixtures/apge-insider-activity.example.json`.
- Match `reference/insider-activity-mockup.png` at 1621 × 970 and 1440 × 900.
- Reuse current header, typography, cards, borders, colors, table, and spacing.
- Add responsive behavior, drawer, filters, URL state, loading, empty, and error states.
- Add visual-regression coverage.

### Phase 2 — Data model and parser

- Adapt the logical model in `appendices/database-schema.sql` to the current ORM.
- Parse Forms 3, 4, 5, and amendments.
- Parse owners, non-derivative and derivative rows, holdings, footnotes, signatures, Rule 10b5-1 flag, and amendments.
- Store raw source and parser version.
- Add frozen SEC XML fixtures and unit tests.

### Phase 3 — Ingestion

- Add historical bulk backfill and incremental filing ingestion.
- Add a global SEC rate limiter and declared User-Agent.
- Make all upserts idempotent.
- Add retries, immutable caching, telemetry, and admin reparse capability.

### Phase 4 — API and metrics

- Implement canonical server-side metrics.
- Build page and filing-detail endpoints.
- Add cursor pagination and query filters.
- Return data-quality and freshness metadata.
- Reconcile summary, chart, table, and sidebar to the same normalized rows.

### Phase 5 — Integration and hardening

- Replace fixture adapter with live API.
- Test several issuers with different filing patterns.
- Test amendments, joint filers, missing prices, derivatives, indirect ownership, and stock splits.
- Run accessibility, performance, and visual-regression checks.
- Document backfill, incremental sync, and recovery procedures.

## Visual target

The page should look like a native extension of the existing site:

- Warm ivory background.
- Off-white cards.
- Thin taupe borders.
- Forest-green links and purchase accents.
- Muted coral sale accents.
- Editorial serif headings.
- Compact sans-serif labels and data.
- Minimal shadows.
- Quiet price line with shaped transaction markers.
- Main content plus narrow ranked-summary rail.

Do not make it look like a dark trading terminal or a generic SaaS dashboard.

## Completion response

At the end of each phase, report:

- Files changed.
- Migrations added.
- Tests added and results.
- Screenshots or visual-regression outputs.
- Remaining assumptions.
- Known data-quality limitations.
- Exact next phase.
