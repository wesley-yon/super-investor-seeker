# Code cleanup audit — September 6, 2026

Baseline: `47d81ec` (fetched `origin/main` when the audit started).
Scope: both cleanup passes across Python, the static website, tests,
operational scripts, workflow entry points, and architecture documentation.
Changes are local; no private snapshot publication or deployment was run.

## Product and preserved contracts

Super Investor Seeker is a public, searchable view of institutional holdings
across the SEC 13F filer universe. Visitors can inspect fund portfolios and
institutional owners, navigate between them, and compare the latest four
reported quarters. Older verified history remains in the dataset.

| Area | Active implementation | Preserved contract |
| --- | --- | --- |
| Ingestion | `pipeline.py`, recent-feed script | Discover filings, compose amendments, retain history, checkpoint retries and interruptions. |
| Identity | SEC master, EDGAR evidence, `security_identity.py` | Keep CUSIP and instrument type distinct; publish tickers only with accepted SEC evidence. |
| Historical verification | Bulk backfill, accession discovery, composition and migration modules | Verify source hashes, resume rebuilds, preserve reported holdings and economic values. |
| Data quality | Quantity, value-unit, quarter-health, and validation modules | Normalize dollar values; separate estimated quantities; preserve independent publication checks. |
| Browser | `index.html`, `site-data-loader.js` | Preserve search, routes, tables, charts, filing states, and compressed payload loading. |
| Operations | Workflows, snapshots, artifact/publication helpers | Restore private state, validate before publishing, deploy the exact dataset, retain rollback state. |

## Backend simplification

- Removed the legacy mutable CUSIP map from ingestion, replay, amendment and
  identity repairs, and retry paths. It was copied and updated but never used
  to assign holding tickers or persisted as authority. The retained
  `update_holding_tickers` applies the same exact, type-keyed master lookup.
  Removed the map loader, resolver, no-op saver, and their argument plumbing.
- Removed unused registry refresh options, a test-only compatibility branch,
  and an optional company-title fallback. Every repository caller supplied
  empty/default company data; shipped paths already use verified master titles.
  Active master-refresh flags and registry publication gates remain in place.
- Removed unused ambiguity, price-reference, master-projection, incremental
  wrapper, prior-unit wrapper, and private backfill helpers. Repository-wide
  text and syntax-tree checks found no remaining callers or CLI dependencies.
- Consolidated three bounded redirect loops into `sec_http.py` and shared the
  identical per-instance request pacer. URL admission, redirect scope, status
  handling, retries, exception types, and the master's process-wide pacing
  remain source-specific.
- Consolidated five durable JSON writers through `atomic_files.py`. Callers
  retain their serializers, symlink checks, private file modes, directory-sync
  requirements, and interruption cleanup. The shared path also closes a file
  descriptor if opening its text stream fails.
- Included both shared modules in frozen-rebuild code hashes. Incremental
  validation already fingerprints every root Python module.

All real state checkpoints and their ordering remain; only the lock used
solely to copy the unused map was removed. An independent call-site review
checked all 57 direct calls to the ten changed APIs. Tests retain economic,
identity, retry, and checkpoint assertions; map-only expectations were removed.

## Website simplification

Shared JSON fetching, security-text normalization, sort toggling, change
labels, statistic cards, and identity cells replace repeated implementations.
Current, stale, and withheld holder projections share their common field
mapping. Removed a redundant holder sort, write-only row/history fields, an
unused cache method, and an always-unselected rendering branch.

Removed superseded CSS declarations, including matching media-rule overrides.
The active theme and compressed loader remain byte-identical. Relative to the
original baseline, `index.html` is 13,166 bytes smaller. No runtime speedup is
claimed.

## Operational defect corrected

The recent-feed script previously ignored command-line arguments. A `--help`
smoke check therefore started ingestion in the isolated audit checkout. That
run completed locally; its six generated files were removed. The original
dataset and remote publication were untouched.

The script now parses arguments first: help exits successfully, unknown
arguments fail without ingestion, and the supported no-argument invocation
preserves behavior and exit status. Both workflow invocations use that form.
Two regression tests cover these boundaries without network access.

## Retained deliberately

Historical SEC parsers, composition-hash versions, snapshot-v1 restore,
legacy-index adoption, migration comparisons, repair tools, and clean-rebuild
checkpoints still serve compatibility or recovery roles. Independent validator
calculations remain independent. Writers with different durability contracts
and network retry policies remain separate where sharing would add complexity.

Operational scripts and the candidate-verification workflow still have active
roles. GitHub retry policy is already centralized; extracting the remaining
small shell wrappers would add publication dependencies with little benefit.
Accepted CLI options and local JSON fallback remain. Internal Python helper
signatures were simplified; undocumented external importers would need the new
signatures. Workflow configuration and dependency versions were not changed.

## Verification and limits

- Baseline: 972 Python tests ran; 971 passed and one private-dataset integration
  test was skipped. Final: 991 ran; 990 passed with the same expected skip.
- Node loader tests, configured Ruff checks, Python compilation, CLI help
  checks, and whitespace checks passed.
- Twenty-four offline old/new ticker-update comparisons produced identical
  holdings across mixed instruments, mapping statuses, and missing siblings.
  The SEC master was not mutated.
- Three hundred thirty-two simulated old/new HTTP traces matched outcomes,
  errors, request URLs/headers/timeouts, status checks, and sleeps. Added
  regressions preserve source-specific retry and pacing differences.
- Atomic writer tests cover exact JSON bytes, private modes, symlink rejection,
  interrupted cleanup, descriptor-open failure, and directory-sync failures.
- Browser comparisons cover home/fund/stock at seven widths (21 cases), search
  focus/hover, six filing-state views, and 38 sort/filter/pagination operations.
  Network assets are blocked for deterministic comparisons; this is fixture
  coverage, not an exhaustive browser or live-data certification.
- Read-only original-dataset checks passed for contract-v5 indexes and all
  45,847 registry entries and matching labels. This was metadata/provenance
  validation, not full-corpus revalidation. No original data or caches changed.

Production code has 1,071 lines removed and 495 added: **576 fewer lines net**,
including both new shared modules. Tests add 302 lines net; documentation adds
130. Across all files, the repository is **144 lines smaller**. These counts
cover both passes; they do not represent a measured performance improvement.
