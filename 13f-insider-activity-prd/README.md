# 13F Insider Activity — Codex Build Package

This package contains a build-ready PRD and reference assets for adding insider-transaction tracking to `13f.wesleyyon.com`.

## Start here

1. Open `CODEX_START_HERE.md`.
2. Read `13F_INSIDER_ACTIVITY_PRD.md` in full.
3. Inspect both images in `reference/`.
4. Give Codex access to the existing repository and ask it to complete **Phase 0 — Repository discovery** before making broad architectural changes.

## Files

- `13F_INSIDER_ACTIVITY_PRD.md` — complete product, design, data, API, ingestion, testing, and acceptance specification.
- `CODEX_START_HERE.md` — concise coding-agent execution brief.
- `reference/current-holder-page.png` — current site visual reference.
- `reference/insider-activity-mockup.png` — desired insider page visual target.
- `fixtures/apge-insider-activity.example.json` — illustrative frontend/API fixture; not real APGE filing data.
- `appendices/database-schema.sql` — illustrative PostgreSQL logical schema.
- `appendices/insider-activity-types.ts` — illustrative TypeScript contracts.
- `appendices/transaction-code-map.csv` — SEC transaction-code mapping for product labels.

## Important

The fixture and mockup use illustrative names and values. Production data must come from the SEC ingestion pipeline described in the PRD.
