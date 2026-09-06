# Code cleanup audit — September 6, 2026

Baseline: `47d81ec` (the fetched `origin/main` when this audit started).
Scope: repository cleanup preserving product behavior, including Python, the
static website, tests, operational scripts, workflow entry points, and
architecture documentation. One operational argument-handling defect found
during verification was also corrected, as described below.

## Product and contracts

Super Investor Seeker is a public, searchable view of institutional holdings
across the SEC 13F filer universe. Visitors can find a fund's portfolio, inspect
a security's institutional owners, follow links between them, and compare the
latest four reported quarters. Older verified history remains in the dataset.

| Area | Active implementation | Contract to preserve |
| --- | --- | --- |
| Filing ingestion | `pipeline.py`, `scripts/refresh_recent_13f_filings.py` | Discover filings, compose amendments, preserve reported identity and normalized dollar values, checkpoint retries and interrupted work. |
| Security identity | `sec_security_master.py`, `sec_edgar_evidence.py`, `security_identity.py` | Keep CUSIP and instrument type distinct; publish tickers only with accepted SEC evidence. |
| Historical verification | `sec_13f_bulk_backfill.py`, `sec_13f_accession_discovery.py`, `composition_integrity.py`, `security_master_migration.py` | Verify historical sources and hashes, resume interrupted rebuilds, preserve holdings and economic values through migration. |
| Quantity and data quality | `quantity_estimation.py`, `value_units.py`, `quarter_health.py`, `validate_data.py`, `incremental_validation.py` | Keep estimates separate from reported quantities; preserve unit, identity, provenance, and publication checks. |
| Browser | `index.html`, `site-data-loader.js` | Preserve search, fund/security routing, tables, charts, missing-quarter handling, and compressed payload loading. |
| Operations | `.github/workflows/`, `scripts/data_snapshot.py`, artifact/publication helpers | Restore private state, validate before publication, deploy the exact dataset, retain rollback state, and clean temporary public artifacts. |

## Removed backend vestiges

- `find_ambiguous_ticker_cusips`: no callers remain; active exact-identity and
  ticker-conflict checks are separate and unchanged.
- `build_zero_share_price_reference_maps` and its two private helpers: an unused
  price-median path predating the active evidence-bound quantity policy.
- `load_sec_security_details`: an unused CUSIP-only projection of the master.
- `refresh_security_master_incremental`: an unused wrapper; scheduled and
  explicit refresh commands use the retained refresh implementation.
- `save_cusip_map`: already performed no persistence. Removed its 18 internal
  calls across the pipeline and recent-feed script, including exception
  handlers that could never report a real map-write failure. SEC mappings
  continue to persist through the provenance-bearing security-master pair.
- `_INFOTABLE_OPTIONAL_COLUMNS` and
  `_archive_targets_for_unmatched_verification`: unreferenced private backfill
  definitions. Optional SEC fields and accession fallback remain supported.

Repository-wide text and Python syntax-tree reference checks established the
unused definitions' lack of callers, including imports and string references.
An independent syntax-tree comparison confirmed the remaining pipeline code
is identical after removing these definitions and no-op calls. All 23 actual
`save_state` calls, their locks, and their ordering are preserved. Tests retain
state-checkpoint assertions and direct in-memory mapping assertions; only
mocked persistence of the no-op function was removed.

## Website cleanup

Removed 342 CSS declarations superseded by unconditional declarations for the
same selectors in the current theme. The active theme itself is byte-identical.
Also removed an unused cache method, five unread metadata copies in the private
holding-history accumulator, a summary-rendering branch whose callers always
selected the other branch, and an orphaned comment about a removed stock list.
Public row shapes and all live rendering functions remain available.
The main HTML file is 8,254 bytes smaller; no runtime speedup is claimed.

## Operational defect corrected

`scripts/refresh_recent_13f_filings.py` previously ignored command-line
arguments. A `--help` smoke check therefore started actual feed ingestion in
the isolated audit checkout. That run completed locally; its six generated
files were removed. The original dataset and remote publication were untouched.

The script now parses arguments before entering ingestion: help exits
successfully, unknown arguments fail without running, and the supported
no-argument invocation preserves the existing behavior and exit status. Both
workflow invocations use that no-argument form. Two regression tests cover
these boundaries without network access.

## Retained deliberately

Historical SEC parsers, composition-hash versions, snapshot-v1 restore,
unpublished legacy-index adoption, migration comparisons, repair tools, and
clean-rebuild checkpoints still have active compatibility or recovery uses.
Every operational script has a workflow, documented CLI, or supported tool
role. The candidate benchmark workflow is current as of this audit. Exported
parser APIs, compatibility imports, accepted CLI options, and the browser's
local JSON fallback remain available.

Architecture documentation now reflects the configured daily 04:23 UTC SEC
maintenance, weekday reuse of the verified master, and the existing light
theme, charts, search, and navigation. Workflow configuration was not changed.

## Verification and limits

- Baseline: 972 Python tests ran; 971 passed and one private-dataset integration
  test was skipped in the isolated checkout. The Node loader test passed.
- Final: 974 Python tests ran; 973 passed with the same private-dataset test
  skipped. The Node loader test, configured Ruff checks, Python compilation,
  CLI help smoke checks, and whitespace checks passed.
- Browser fixture comparisons matched the original's DOM, computed styles,
  and full-page pixels for home, fund, and stock views at seven viewport widths
  (21 comparisons). Search focus and hover styles also matched. Network assets
  were blocked for deterministic comparison; the active theme was independently
  verified byte-for-byte and every removed CSS declaration was checked against
  its unconditional replacement using the browser's CSS parser.
- Read-only checks against the existing local dataset passed for contract-v5
  indexes and all 45,847 public registry entries and corresponding security
  labels. This is a metadata/provenance check, not full-corpus revalidation.
- Existing generated data, private caches, dependency versions, and workflows
  were unchanged. No private snapshot was restored into the cleanup checkout;
  the temporary feed output described above was removed. No snapshot
  publication or deployment was run.

This establishes the scope of the cleanup and its regression evidence. It is
not a fresh attestation of every historical holding or remote deployment.
